import json
import time
import uuid
from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from django.core.management.base import BaseCommand
from django.conf import settings
from shipping_app.models import Reservation, Shipment, PendingPayment, ProcessedEvent

class Command(BaseCommand):
    help = 'Consume payment events and generate shipments'

    def get_latest_offset(self, consumer, message):
        _, high_offset = consumer.get_watermark_offsets(
            TopicPartition(message.topic(), message.partition()),
            timeout=10,
        )
        return high_offset - 1

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
        consumer.subscribe(['payments'])

        print('Shipping: listening to payments topic')
        try:
            while True:
                message = consumer.poll(1.0)
                
                if message is None:
                    self.process_pending(producer)
                    continue
                if message.error():
                    print(f'Shipping: consumer error from payments: {message.error()}', flush=True)
                    continue

                raw_value = message.value()
                try:
                    event = json.loads(raw_value.decode('utf-8'))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    print(
                        'Shipping: invalid message from payments '
                        f'partition={message.partition()} offset={message.offset()} error={exc}',
                        flush=True,
                    )
                    consumer.commit(message=message, asynchronous=False)
                    continue

                print(
                    'Shipping: read message from payments '
                    f'partition={message.partition()} offset={message.offset()} '
                    f'value={json.dumps(event)}',
                    flush=True,
                )
                latest_offset = self.get_latest_offset(consumer, message)
                if message.offset() < latest_offset:
                    print(
                        'Shipping: read payments message is not the latest '
                        f'partition={message.partition()} offset={message.offset()} '
                        f'latest_offset={latest_offset}; waiting for latest message',
                        flush=True,
                    )
                    consumer.commit(message=message, asynchronous=False)
                    continue

                print(
                    'Shipping: successfully read latest message from payments '
                    f'partition={message.partition()} offset={message.offset()} '
                    f'value={json.dumps(event)}',
                    flush=True,
                )

                event_id = event.get('event_id')
                if not event_id or event.get('event_type') != 'PaymentProcessed':
                    consumer.commit(message=message, asynchronous=False)
                    continue
                if ProcessedEvent.objects.filter(event_id=event_id).exists():
                    print(f'Shipping: duplicate PaymentProcessed {event_id} ignored')
                    consumer.commit(message=message, asynchronous=False)
                    continue
                
                order_id = event['order_id']
                pending, created = PendingPayment.objects.get_or_create(
                    order_id=order_id,
                    defaults={
                        'amount': event['amount'],
                        'status': 'WAITING_FOR_STOCK',
                        'event_id': event_id,
                    }
                )
                if not created:
                    print(f'Shipping: payment for order {order_id} already pending')
                
                print(f'Shipping: payment received for order {order_id}, checking reservation')
                self.try_create_shipment(order_id, producer)
                
                ProcessedEvent.objects.get_or_create(event_id=event_id)
                consumer.commit(message=message, asynchronous=False)
                
                self.process_pending(producer)
                time.sleep(0.5)
        except KeyboardInterrupt:
            print('Shipping: consumer stopped', flush=True)
        finally:
            producer.flush(10)
            consumer.close()

    def process_pending(self, producer):
        for pending in PendingPayment.objects.filter(status='WAITING_FOR_STOCK'):
            self.try_create_shipment(pending.order_id, producer)

    def try_create_shipment(self, order_id, producer):
        reservation = Reservation.objects.filter(order_id=order_id).first()
        if not reservation:
            print(f'Shipping: no reservation yet for order {order_id}, waiting')
            return
        if Shipment.objects.filter(order_id=order_id).exists():
            print(f'Shipping: shipment already created for order {order_id}')
            PendingPayment.objects.filter(order_id=order_id).update(status='SHIPPED')
            return
            
        shipment_id = f'SHIP-{uuid.uuid4().hex[:8]}'
        shipment = Shipment.objects.create(
            order_id=order_id,
            shipment_id=shipment_id,
            product_id=reservation.product_id,
            quantity=reservation.quantity,
            status='READY'
        )
        
        shipment_event = {
            'event_id': str(uuid.uuid4()),
            'event_type': 'ShipmentCreated',
            'order_id': shipment.order_id,
            'shipment_id': shipment.shipment_id,
            'product_id': shipment.product_id,
            'quantity': shipment.quantity,
            'status': shipment.status,
        }
        
        shipment_event_json = json.dumps(shipment_event)
        delivery_errors = []

        def delivery_report(error, produced_message):
            if error is not None:
                delivery_errors.append(error)
                print(
                    'Shipping: failed to publish message to shipments '
                    f'error={error} value={shipment_event_json}',
                    flush=True,
                )
                return
            print(
                'Shipping: successfully published message to shipments '
                f'partition={produced_message.partition()} '
                f'offset={produced_message.offset()} '
                f'value={shipment_event_json}',
                flush=True,
            )

        producer.produce(
            'shipments',
            value=shipment_event_json.encode('utf-8'),
            callback=delivery_report,
        )
        remaining_messages = producer.flush(10)
        if remaining_messages:
            raise TimeoutError('Shipping: timed out publishing message to shipments')
        if delivery_errors:
            raise KafkaException(delivery_errors[0])

        PendingPayment.objects.filter(order_id=order_id).update(status='SHIPPED')
        print(f'Shipping: created shipment for order {order_id}')
