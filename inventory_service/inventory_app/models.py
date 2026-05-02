from django.db import models
class Stock(models.Model):
    product_id = models.IntegerField(primary_key=True)
    available = models.IntegerField()
    reserved = models.IntegerField()
    class Meta:
        db_table = 'stock'
        managed = False
class Reservation(models.Model):
    order_id = models.IntegerField(unique=True)
    product_id = models.IntegerField()
    quantity = models.IntegerField()
    reserved_at = models.DateTimeField(auto_now_add=True)
    class Meta:
        db_table = 'stock_reservations'
        managed = False
class ProcessedEvent(models.Model):
    event_id = models.CharField(max_length=128, primary_key=True)
    created_at = models.DateTimeField(auto_now_add=True)
    class Meta:
        db_table = 'processed_events'
        managed = False
