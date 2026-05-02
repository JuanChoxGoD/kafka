import json
import time
from kafka import KafkaConsumer
from django.core.mail import send_mail
from django.core.management.base import BaseCommand
from django.conf import settings
from notifications.models import Customer, Order, ProcessedEvent
class Command(BaseCommand):
    help = 'Consume all service events and notify users'
    def handle(self, *args, **options):
        consumer = KafkaConsumer(
            'orders',
            'payments',
            'shipments',
            bootstrap_servers=[settings.KAFKA_BOOTSTRAP_SERVERS],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='notification-service-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        )
        print('Notification: listening to orders, payments, shipments topics')
        for message in consumer:
            event = message.value
            event_id = event.get('event_id')
            if not event_id or ProcessedEvent.objects.filter(event_id=event_id).exists():
                if event_id:
                    print(f'Notification: duplicate event {event_id} ignored')
                continue
            ProcessedEvent.objects.create(event_id=event_id)
            self.notify(event)
            time.sleep(0.5)
    def get_customer_email(self, order_id, event):
        email = event.get('customer_email')
        if email:
            return email
        try:
            order = Order.objects.select_related('customer').get(id=order_id)
            return order.customer.email
        except Order.DoesNotExist:
            return None
    def notify(self, event):
        order_id = event.get('order_id')
        email = self.get_customer_email(order_id, event)
        if not email:
            print(f'Notification: no email found for order {order_id}, event {event.get("event_type")} skipped')
            return
        event_type = event.get('event_type')
        if event_type == 'OrderCreated':
            subject = f'Orden {order_id} creada'
            message = f"Tu orden {order_id} ha sido creada correctamente. Monto: {event.get('total_amount')}"
        elif event_type == 'PaymentProcessed':
            subject = f'Pago recibido para orden {order_id}'
            message = f"Tu pago de {event.get('amount')} ha sido procesado con estado {event.get('status')}.
Método: {event.get('payment_method')}"
        elif event_type == 'StockReserved':
            subject = f'Stock reservado para orden {order_id}'
            message = f"Se ha reservado stock para el producto {event.get('product_id')} (cantidad {event.get('quantity')})."
        elif event_type == 'ShipmentCreated':
            subject = f'Envío generado para orden {order_id}'
            message = f"Tu envío {event.get('shipment_id')} ha sido creado para la orden {order_id}."
        else:
            subject = f'Evento {event_type} recibido para orden {order_id}'
            message = f"Se recibió el evento {event_type} para la orden {order_id}."
        try:
            send_mail(
                subject,
                message,
                settings.DEFAULT_FROM_EMAIL,
                [email],
                fail_silently=False,
            )
            print(f'Notification: email sent to {email} for event {event_type} order {order_id}')
        except Exception as exc:
            print(f'Notification: failed to send email to {email} for event {event_type}: {exc}')
