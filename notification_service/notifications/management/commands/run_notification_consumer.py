import json
import time
from kafka import KafkaConsumer
from django.conf import settings
from django.core.mail import EmailMessage
from django.core.management.base import BaseCommand
from notifications.models import Order, ProcessedEvent


class Command(BaseCommand):
    help = 'Consume all service events and notify users'
    TOPICS = ['orders', 'payments', 'inventory', 'shipments']

    def handle(self, *args, **options):
        consumer = KafkaConsumer(
            *self.TOPICS,
            bootstrap_servers=[settings.KAFKA_BOOTSTRAP_SERVERS],
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id='notification-service-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            security_protocol='SASL_SSL',
            sasl_mechanism='PLAIN',
            sasl_plain_username=settings.KAFKA_API_KEY,
            sasl_plain_password=settings.KAFKA_API_SECRET,
        )
        print(
            'Notification: email config '
            f'host={settings.EMAIL_HOST} port={settings.EMAIL_PORT} '
            f'user={settings.EMAIL_HOST_USER} from={settings.DEFAULT_FROM_EMAIL} '
            f'use_tls={settings.EMAIL_USE_TLS} password_configured={bool(settings.EMAIL_HOST_PASSWORD)}',
            flush=True,
        )
        if not settings.EMAIL_HOST_PASSWORD:
            print('Notification: SMTP_PASSWORD is empty; SendGrid email delivery will fail', flush=True)

        print(f'Notification: listening to {", ".join(self.TOPICS)} topics')
        for message in consumer:
            event = message.value
            print(
                'Notification: read message '
                f'topic={message.topic} partition={message.partition} offset={message.offset} '
                f'value={json.dumps(event)}',
                flush=True,
            )
            if message.topic in ('inventory', 'shipments'):
                print(
                    f'Notification: read message from {message.topic} '
                    f'partition={message.partition} offset={message.offset} '
                    f'value={json.dumps(event)}',
                    flush=True,
                )
            event_id = event.get('event_id')
            if not event_id or ProcessedEvent.objects.filter(event_id=event_id).exists():
                if event_id:
                    print(f'Notification: duplicate event {event_id} ignored')
                consumer.commit()
                continue
            if self.notify(event):
                ProcessedEvent.objects.create(event_id=event_id)
                consumer.commit()
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
            return True

        event_type = event.get('event_type')
        if event_type == 'OrderCreated':
            subject = f'Orden {order_id} creada'
            message = f"Tu orden {order_id} ha sido creada correctamente. Monto: {event.get('total_amount')}"
        elif event_type == 'PaymentProcessed':
            subject = f'Pago recibido para orden {order_id}'
            message = (
                f"Tu pago de {event.get('amount')} ha sido procesado con estado {event.get('status')}.\n"
                f"Metodo: {event.get('payment_method')}"
            )
        elif event_type == 'StockReserved':
            subject = f'Stock reservado para orden {order_id}'
            message = (
                f"Se ha reservado stock para el producto {event.get('product_id')} "
                f"(cantidad {event.get('quantity')})."
            )
        elif event_type == 'StockReservationFailed':
            subject = f'Stock no disponible para orden {order_id}'
            message = (
                f"No fue posible reservar stock para el producto {event.get('product_id')} "
                f"(cantidad {event.get('quantity')}). Motivo: {event.get('reason')}."
            )
        elif event_type == 'ShipmentCreated':
            subject = f'Envio generado para orden {order_id}'
            message = f"Tu envio {event.get('shipment_id')} ha sido creado para la orden {order_id}."
        else:
            subject = f'Evento {event_type} recibido para orden {order_id}'
            message = f"Se recibio el evento {event_type} para la orden {order_id}."

        try:
            print(
                'Notification: attempting to send email '
                f'to={email} from={settings.DEFAULT_FROM_EMAIL} '
                f'event={event_type} order={order_id} subject={subject}',
                flush=True,
            )
            email_message = EmailMessage(
                subject,
                message,
                settings.DEFAULT_FROM_EMAIL,
                [email],
            )
            sent_count = email_message.send(fail_silently=False)
            print(
                'Notification: email send finished '
                f'to={email} event={event_type} order={order_id} sent_count={sent_count}',
                flush=True,
            )
            if sent_count != 1:
                return False
            return True
        except Exception as exc:
            print(
                'Notification: failed to send email '
                f'to={email} event={event_type} error_type={type(exc).__name__} error={exc!r}',
                flush=True,
            )
            return False
