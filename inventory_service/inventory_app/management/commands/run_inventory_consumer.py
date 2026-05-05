import json
import time
import uuid
from confluent_kafka import Consumer, KafkaException, Producer
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from inventory_app.models import Stock, Reservation, ProcessedEvent


class Command(BaseCommand):
    help = 'Consume payment events, reserve stock, and publish inventory events'

    def publish_event(self, producer, topic, event):
        event_json = json.dumps(event)
        delivery_errors = []

        def delivery_report(error, produced_message):
            if error is not None:
                delivery_errors.append(error)
                print(
                    f'Inventory: failed to publish message to {topic} '
                    f'error={error} value={event_json}',
                    flush=True,
                )
                return
            print(
                f'Inventory: successfully published message to {topic} '
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
            raise TimeoutError(f'Inventory: timed out publishing message to {topic}')
        if delivery_errors:
            raise KafkaException(delivery_errors[0])

    def build_failure_event(self, payment_event, reason):
        return {
            'event_id': str(uuid.uuid4()),
            'event_type': 'StockReservationFailed',
            'payment_event_id': payment_event['event_id'],
            'order_id': payment_event.get('order_id'),
            'product_id': payment_event.get('product_id'),
            'quantity': payment_event.get('quantity'),
            'status': 'FAILED',
            'reason': reason,
            'customer_email': payment_event.get('customer_email'),
        }

    def reserve_stock(self, payment_event):
        order_id = payment_event['order_id']
        product_id = payment_event['product_id']
        quantity = int(payment_event['quantity'])

        with transaction.atomic():
            reservation = Reservation.objects.filter(order_id=order_id).first()
            if reservation:
                print(f'Inventory: stock already reserved for order {order_id}', flush=True)
                return reservation, None

            stock = Stock.objects.select_for_update().filter(product_id=product_id).first()
            if not stock:
                return None, f'product {product_id} not found in stock'
            if stock.available < quantity:
                return None, f'insufficient stock for product {product_id}'

            stock.available -= quantity
            stock.reserved += quantity
            stock.save()
            reservation = Reservation.objects.create(
                order_id=order_id,
                product_id=product_id,
                quantity=quantity,
            )
            return reservation, None

    def handle(self, *args, **options):
        print(f"Inventory: connecting to {settings.KAFKA_BOOTSTRAP_SERVERS}")

        kafka_config = {
            'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': settings.KAFKA_API_KEY,
            'sasl.password': settings.KAFKA_API_SECRET,
        }
        consumer_config = {
            **kafka_config,
            'group.id': 'inventory-service-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        }

        consumer = Consumer(consumer_config)
        producer = Producer(kafka_config)
        consumer.subscribe(['payments'])

        print('Inventory: listening to payments topic')
        try:
            while True:
                message = consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    print(f'Inventory: consumer error from payments: {message.error()}', flush=True)
                    continue

                raw_value = message.value()
                try:
                    event = json.loads(raw_value.decode('utf-8'))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    print(
                        'Inventory: invalid message from payments '
                        f'partition={message.partition()} offset={message.offset()} error={exc}',
                        flush=True,
                    )
                    consumer.commit(message=message, asynchronous=False)
                    continue

                print(
                    'Inventory: read message from payments '
                    f'partition={message.partition()} offset={message.offset()} '
                    f'value={json.dumps(event)}',
                    flush=True,
                )

                event_id = event.get('event_id')
                if not event_id or event.get('event_type') != 'PaymentProcessed':
                    consumer.commit(message=message, asynchronous=False)
                    continue
                if ProcessedEvent.objects.filter(event_id=event_id).exists():
                    print(f'Inventory: duplicate PaymentProcessed {event_id} ignored')
                    consumer.commit(message=message, asynchronous=False)
                    continue

                required_fields = ['order_id', 'product_id', 'quantity']
                missing_fields = [field for field in required_fields if field not in event]
                if missing_fields:
                    print(
                        'Inventory: PaymentProcessed missing fields '
                        f'fields={missing_fields} value={json.dumps(event)}',
                        flush=True,
                    )
                    consumer.commit(message=message, asynchronous=False)
                    continue

                reservation, failure_reason = self.reserve_stock(event)
                if failure_reason:
                    inventory_event = self.build_failure_event(event, failure_reason)
                    print(
                        f'Inventory: stock reservation failed for order {event["order_id"]}: '
                        f'{failure_reason}',
                        flush=True,
                    )
                else:
                    inventory_event = {
                        'event_id': str(uuid.uuid4()),
                        'event_type': 'StockReserved',
                        'payment_event_id': event_id,
                        'order_id': reservation.order_id,
                        'product_id': reservation.product_id,
                        'quantity': reservation.quantity,
                        'status': 'RESERVED',
                        'customer_email': event.get('customer_email'),
                    }
                    print(
                        'Inventory: reserved stock '
                        f'order_id={reservation.order_id} '
                        f'product_id={reservation.product_id} '
                        f'quantity={reservation.quantity}',
                        flush=True,
                    )

                self.publish_event(producer, 'inventory', inventory_event)
                ProcessedEvent.objects.get_or_create(event_id=event_id)
                consumer.commit(message=message, asynchronous=False)
                time.sleep(0.5)
        except KeyboardInterrupt:
            print('Inventory: consumer stopped', flush=True)
        finally:
            producer.flush(10)
            consumer.close()
