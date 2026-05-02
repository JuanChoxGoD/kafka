import json
import time
import uuid
from kafka import KafkaConsumer, KafkaProducer
from django.core.management.base import BaseCommand
from django.conf import settings
from billing_app.models import Payment, ProcessedEvent
class Command(BaseCommand):
    help = 'Consume order events and publish payment events'
    def handle(self, *args, **options):
        consumer = KafkaConsumer(
            'orders',
            bootstrap_servers=[settings.KAFKA_BOOTSTRAP_SERVERS],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='billing-service-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        )
        producer = KafkaProducer(
            bootstrap_servers=[settings.KAFKA_BOOTSTRAP_SERVERS],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )
        print('Billing: listening to orders topic')
        for message in consumer:
            event = message.value
            event_id = event.get('event_id')
            if not event_id or event.get('event_type') != 'OrderCreated':
                continue
            if ProcessedEvent.objects.filter(event_id=event_id).exists():
                print(f'Billing: duplicate OrderCreated {event_id} ignored')
                continue
            payment_event_id = str(uuid.uuid4())
            payment = Payment.objects.create(
                order_id=event['order_id'],
                amount=event['total_amount'],
                status='PAID',
                payment_method='card',
                event_id=payment_event_id,
            )
            ProcessedEvent.objects.create(event_id=event_id)
            payment_event = {
                'event_id': payment_event_id,
                'event_type': 'PaymentProcessed',
                'order_id': payment.order_id,
                'amount': float(payment.amount),
                'status': payment.status,
                'payment_method': payment.payment_method,
            }
            producer.send('payments', value=payment_event)
            producer.flush()
            print(f"Billing: processed payment for order {payment.order_id}")
            time.sleep(0.5)
