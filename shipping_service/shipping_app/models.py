from django.db import models
class Reservation(models.Model):
    order_id = models.IntegerField(unique=True)
    product_id = models.IntegerField()
    quantity = models.IntegerField()
    class Meta:
        db_table = 'stock_reservations'
        managed = False
class Shipment(models.Model):
    order_id = models.IntegerField(unique=True)
    shipment_id = models.CharField(max_length=100, unique=True)
    product_id = models.IntegerField()
    quantity = models.IntegerField()
    status = models.CharField(max_length=50)
    shipped_at = models.DateTimeField(auto_now_add=True)
    class Meta:
        db_table = 'shipments'
class PendingPayment(models.Model):
    order_id = models.IntegerField(unique=True)
    amount = models.DecimalField(max_digits=12, decimal_places=2)
    status = models.CharField(max_length=30, default='WAITING_FOR_STOCK')
    event_id = models.CharField(max_length=128, unique=True)
    created_at = models.DateTimeField(auto_now_add=True)
    class Meta:
        db_table = 'pending_payments'
class ProcessedEvent(models.Model):
    event_id = models.CharField(max_length=128, primary_key=True)
    created_at = models.DateTimeField(auto_now_add=True)
    class Meta:
        db_table = 'processed_events'
