import json
import time
import uuid
from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from django.core.management.base import BaseCommand
from django.conf import settings
from billing_app.models import Payment, ProcessedEvent

class Command(BaseCommand):
    help = 'Consume order events and publish payment events'

    def get_latest_offset(self, consumer, message):
        _, high_offset = consumer.get_watermark_offsets(
            TopicPartition(message.topic(), message.partition()),
            timeout=10,
        )
        return high_offset - 1

    def handle(self, *args, **options):
        print(f"Billing: connecting to {settings.KAFKA_BOOTSTRAP_SERVERS}")

        kafka_config = {
            'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': settings.KAFKA_API_KEY,
            'sasl.password': settings.KAFKA_API_SECRET,
        }
        consumer_config = {
            **kafka_config,
            'group.id': 'billing-service-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        }

        consumer = Consumer(consumer_config)
        producer = Producer(kafka_config)
        consumer.subscribe(['orders'])

        print('Billing: listening to orders topic')
        try:
            while True:
                message = consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    print(f'Billing: consumer error from orders: {message.error()}', flush=True)
                    continue

                raw_value = message.value()
                try:
                    event = json.loads(raw_value.decode('utf-8'))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    print(
                        'Billing: invalid message from orders '
                        f'partition={message.partition()} offset={message.offset()} error={exc}',
                        flush=True,
                    )
                    consumer.commit(message=message, asynchronous=False)
                    continue

                print(
                    'Billing: read message from orders '
                    f'partition={message.partition()} offset={message.offset()} '
                    f'value={json.dumps(event)}',
                    flush=True,
                )
                latest_offset = self.get_latest_offset(consumer, message)
                if message.offset() < latest_offset:
                    print(
                        'Billing: read orders message is not the latest '
                        f'partition={message.partition()} offset={message.offset()} '
                        f'latest_offset={latest_offset}; waiting for latest message',
                        flush=True,
                    )
                    consumer.commit(message=message, asynchronous=False)
                    continue

                print(
                    'Billing: successfully read latest message from orders '
                    f'partition={message.partition()} offset={message.offset()} '
                    f'value={json.dumps(event)}',
                    flush=True,
                )

                event_id = event.get('event_id')
                if not event_id or event.get('event_type') != 'OrderCreated':
                    consumer.commit(message=message, asynchronous=False)
                    continue
                if ProcessedEvent.objects.filter(event_id=event_id).exists():
                    print(f'Billing: duplicate OrderCreated {event_id} ignored')
                    consumer.commit(message=message, asynchronous=False)
                    continue
                payment_event_id = str(uuid.uuid4())
                payment, _ = Payment.objects.get_or_create(
                    order_id=event['order_id'],
                    defaults={
                        'amount': event['total_amount'],
                        'status': 'PAID',
                        'payment_method': 'card',
                        'event_id': payment_event_id,
                    },
                )
                payment_event = {
                    'event_id': payment.event_id,
                    'event_type': 'PaymentProcessed',
                    'order_id': payment.order_id,
                    'product_id': event['product_id'],
                    'quantity': event['quantity'],
                    'amount': float(payment.amount),
                    'status': payment.status,
                    'payment_method': payment.payment_method,
                    'customer_email': event.get('customer_email'),
                }
                payment_event_json = json.dumps(payment_event)
                delivery_errors = []

                def delivery_report(error, produced_message):
                    if error is not None:
                        delivery_errors.append(error)
                        print(
                            'Billing: failed to publish message to payments '
                            f'error={error} value={payment_event_json}',
                            flush=True,
                        )
                        return
                    print(
                        'Billing: successfully published message to payments '
                        f'partition={produced_message.partition()} '
                        f'offset={produced_message.offset()} '
                        f'value={payment_event_json}',
                        flush=True,
                    )

                producer.produce(
                    'payments',
                    value=payment_event_json.encode('utf-8'),
                    callback=delivery_report,
                )
                remaining_messages = producer.flush(10)
                if remaining_messages:
                    raise TimeoutError('Billing: timed out publishing message to payments')
                if delivery_errors:
                    raise KafkaException(delivery_errors[0])

                ProcessedEvent.objects.get_or_create(event_id=event_id)
                consumer.commit(message=message, asynchronous=False)
                print(f"Billing: processed payment for order {payment.order_id}")
                time.sleep(0.5)
        except KeyboardInterrupt:
            print('Billing: consumer stopped', flush=True)
        finally:
            producer.flush(10)
            consumer.close()
