import json
import uuid
from kafka import KafkaProducer
from django.conf import settings
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from .models import Customer, Product, Order
from .serializers import OrderCreateSerializer
class OrderCreateView(APIView):
    def post(self, request):
        serializer = OrderCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        customer_id = serializer.validated_data['customer_id']
        product_id = serializer.validated_data['product_id']
        quantity = serializer.validated_data['quantity']
        try:
            customer = Customer.objects.get(pk=customer_id)
            product = Product.objects.get(pk=product_id)
        except (Customer.DoesNotExist, Product.DoesNotExist):
            return Response({'detail': 'Customer or product not found.'}, status=status.HTTP_400_BAD_REQUEST)
        total_amount = product.unit_price * quantity
        event_id = str(uuid.uuid4())
        order = Order.objects.create(
            customer=customer,
            product=product,
            quantity=quantity,
            total_amount=total_amount,
            event_id=event_id,
        )
        event = {
            'event_id': event_id,
            'event_type': 'OrderCreated',
            'order_id': order.id,
            'customer_id': customer.id,
            'product_id': product.id,
            'quantity': quantity,
            'total_amount': float(total_amount),
            'customer_email': customer.email,
        }
        producer = KafkaProducer(
            bootstrap_servers=[settings.KAFKA_BOOTSTRAP_SERVERS],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            security_protocol='SASL_SSL',
            sasl_mechanism='PLAIN',
            sasl_plain_username=settings.KAFKA_API_KEY,
            sasl_plain_password=settings.KAFKA_API_SECRET,
        )
        producer.send('orders', value=event)
        producer.flush()
        print(f"Ordering: published OrderCreated for order {order.id}")
        return Response({'order_id': order.id, 'event_id': event_id, 'status': 'created'}, status=status.HTTP_201_CREATED)
