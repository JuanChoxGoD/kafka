import json
import time
import uuid
from kafka import KafkaConsumer, KafkaProducer
from django.core.management.base import BaseCommand
from django.conf import settings
from inventory_app.models import Stock, Reservation, ProcessedEvent
class Command(BaseCommand):
    help = 'Consume order events and reserve stock'
    def handle(self, *args, **options):
        consumer = KafkaConsumer(
            'orders',
            bootstrap_servers=[settings.KAFKA_BOOTSTRAP_SERVERS],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='inventory-service-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        )
        producer = KafkaProducer(
            bootstrap_servers=[settings.KAFKA_BOOTSTRAP_SERVERS],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )
        print('Inventory: listening to orders topic')
        for message in consumer:
            event = message.value
            event_id = event.get('event_id')
            if not event_id or event.get('event_type') != 'OrderCreated':
                continue
            if ProcessedEvent.objects.filter(event_id=event_id).exists():
                print(f'Inventory: duplicate OrderCreated {event_id} ignored')
                continue
            product_id = event['product_id']
            quantity = event['quantity']
            stock = Stock.objects.filter(product_id=product_id).first()
            if not stock:
                print(f'Inventory: product {product_id} not found in stock')
                continue
            if stock.available < quantity:
                print(f'Inventory: insufficient stock for order {event["order_id"]} product {product_id}')
                continue
            stock.available -= quantity
            stock.reserved += quantity
            stock.save()
            Reservation.objects.create(order_id=event['order_id'], product_id=product_id, quantity=quantity)
            ProcessedEvent.objects.create(event_id=event_id)
            inventory_event_id = str(uuid.uuid4())
            inventory_event = {
                'event_id': inventory_event_id,
                'event_type': 'StockReserved',
                'order_id': event['order_id'],
                'product_id': product_id,
                'quantity': quantity,
                'status': 'RESERVED',
            }
            producer.send('shipments', value=inventory_event)
            producer.flush()
            print(f'Inventory: reserved {quantity} units for order {event["order_id"]} and published StockReserved')
            time.sleep(0.5)
