import json
import time
import uuid
from kafka import KafkaConsumer, KafkaProducer
from django.core.management.base import BaseCommand
from django.conf import settings
from shipping_app.models import Reservation, Shipment, PendingPayment, ProcessedEvent
class Command(BaseCommand):
    help = 'Consume payment events and generate shipments'
    def handle(self, *args, **options):
        consumer = KafkaConsumer(
            'payments',
            bootstrap_servers=[settings.KAFKA_BOOTSTRAP_SERVERS],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='shipping-service-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        )
        producer = KafkaProducer(
            bootstrap_servers=[settings.KAFKA_BOOTSTRAP_SERVERS],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )
        print('Shipping: listening to payments topic')
        while True:
            for message in consumer.poll(timeout_ms=1000).values():
                for record in message:
                    event = record.value
                    self.process_payment_event(event, producer)
            self.process_pending(producer)
            time.sleep(2)
    def process_payment_event(self, event, producer):
        event_id = event.get('event_id')
        if not event_id or event.get('event_type') != 'PaymentProcessed':
            return
        if ProcessedEvent.objects.filter(event_id=event_id).exists():
            print(f'Shipping: duplicate PaymentProcessed {event_id} ignored')
            return
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
        ProcessedEvent.objects.create(event_id=event_id)
        print(f'Shipping: payment received for order {order_id}, checking reservation')
        self.try_create_shipment(order_id, producer)
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
            return
        shipment_id = f'SHIP-{uuid.uuid4().hex[:8]}'
        shipment = Shipment.objects.create(
            order_id=order_id,
            shipment_id=shipment_id,
            product_id=reservation.product_id,
            quantity=reservation.quantity,
            status='READY'
        )
        producer.send('shipments', value={
            'event_id': str(uuid.uuid4()),
            'event_type': 'ShipmentCreated',
            'order_id': shipment.order_id,
            'shipment_id': shipment.shipment_id,
            'product_id': shipment.product_id,
            'quantity': shipment.quantity,
            'status': shipment.status,
        })
        producer.flush()
        PendingPayment.objects.filter(order_id=order_id).update(status='SHIPPED')
        print(f'Shipping: created shipment for order {order_id}')
