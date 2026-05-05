import json
import time
import uuid
from confluent_kafka import Consumer, KafkaException, Producer
from django.conf import settings
from django.core.management.base import BaseCommand
from shipping_app.models import Shipment, ProcessedEvent


class Command(BaseCommand):
    help = 'Consume inventory events and generate shipments'

    def publish_event(self, producer, topic, event):
        event_json = json.dumps(event)
        delivery_errors = []

        def delivery_report(error, produced_message):
            if error is not None:
                delivery_errors.append(error)
                print(
                    f'Shipping: failed to publish message to {topic} '
                    f'error={error} value={event_json}',
                    flush=True,
                )
                return
            print(
                f'Shipping: successfully published message to {topic} '
                f'partition={produced_message.partition()} '
                f'offset={produced_message.offset()} '
                f'value={event_json}',
                flush=True,
            )

        producer.produce(
            topic,
            value=event_json.encode('utf-8'),
            callback=delivery_report,
        )
        remaining_messages = producer.flush(10)
        if remaining_messages:
            raise TimeoutError(f'Shipping: timed out publishing message to {topic}')
        if delivery_errors:
            raise KafkaException(delivery_errors[0])

    def handle(self, *args, **options):
        print(f"Shipping: connecting to {settings.KAFKA_BOOTSTRAP_SERVERS}")

        kafka_config = {
            'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': settings.KAFKA_API_KEY,
            'sasl.password': settings.KAFKA_API_SECRET,
        }
        consumer_config = {
            **kafka_config,
            'group.id': 'shipping-service-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        }

        consumer = Consumer(consumer_config)
        producer = Producer(kafka_config)
        consumer.subscribe(['inventory'])

        print('Shipping: listening to inventory topic')
        try:
            while True:
                message = consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    print(f'Shipping: consumer error from inventory: {message.error()}', flush=True)
                    continue

                raw_value = message.value()
                try:
                    event = json.loads(raw_value.decode('utf-8'))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    print(
                        'Shipping: invalid message from inventory '
                        f'partition={message.partition()} offset={message.offset()} error={exc}',
                        flush=True,
                    )
                    consumer.commit(message=message, asynchronous=False)
                    continue

                print(
                    'Shipping: read message from inventory '
                    f'partition={message.partition()} offset={message.offset()} '
                    f'value={json.dumps(event)}',
                    flush=True,
                )

                event_id = event.get('event_id')
                if not event_id:
                    consumer.commit(message=message, asynchronous=False)
                    continue
                if ProcessedEvent.objects.filter(event_id=event_id).exists():
                    print(f'Shipping: duplicate inventory event {event_id} ignored')
                    consumer.commit(message=message, asynchronous=False)
                    continue
                if event.get('event_type') == 'StockReservationFailed':
                    print(
                        'Shipping: stock reservation failed; shipment will not be created '
                        f'order_id={event.get("order_id")} reason={event.get("reason")}',
                        flush=True,
                    )
                    ProcessedEvent.objects.get_or_create(event_id=event_id)
                    consumer.commit(message=message, asynchronous=False)
                    continue
                if event.get('event_type') != 'StockReserved':
                    ProcessedEvent.objects.get_or_create(event_id=event_id)
                    consumer.commit(message=message, asynchronous=False)
                    continue

                required_fields = ['order_id', 'product_id', 'quantity']
                missing_fields = [field for field in required_fields if field not in event]
                if missing_fields:
                    print(
                        'Shipping: StockReserved missing fields '
                        f'fields={missing_fields} value={json.dumps(event)}',
                        flush=True,
                    )
                    consumer.commit(message=message, asynchronous=False)
                    continue

                shipment, created = Shipment.objects.get_or_create(
                    order_id=event['order_id'],
                    defaults={
                        'shipment_id': f'SHIP-{uuid.uuid4().hex[:8]}',
                        'product_id': event['product_id'],
                        'quantity': event['quantity'],
                        'status': 'READY',
                    },
                )
                if created:
                    print(f'Shipping: created shipment for order {shipment.order_id}', flush=True)
                else:
                    print(f'Shipping: shipment already exists for order {shipment.order_id}', flush=True)

                shipment_event = {
                    'event_id': str(uuid.uuid4()),
                    'event_type': 'ShipmentCreated',
                    'inventory_event_id': event_id,
                    'order_id': shipment.order_id,
                    'shipment_id': shipment.shipment_id,
                    'product_id': shipment.product_id,
                    'quantity': shipment.quantity,
                    'status': shipment.status,
                    'customer_email': event.get('customer_email'),
                }
                self.publish_event(producer, 'shipments', shipment_event)
                ProcessedEvent.objects.get_or_create(event_id=event_id)
                consumer.commit(message=message, asynchronous=False)
                time.sleep(0.5)
        except KeyboardInterrupt:
            print('Shipping: consumer stopped', flush=True)
        finally:
            producer.flush(10)
            consumer.close()
