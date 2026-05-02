from rest_framework import serializers
class OrderCreateSerializer(serializers.Serializer):
    customer_id = serializers.IntegerField()
    product_id = serializers.IntegerField()
    quantity = serializers.IntegerField(min_value=1)
class OrderSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    customer_id = serializers.IntegerField()
    product_id = serializers.IntegerField()
    quantity = serializers.IntegerField()
    total_amount = serializers.DecimalField(max_digits=12, decimal_places=2)
    status = serializers.CharField()
    event_id = serializers.CharField()
