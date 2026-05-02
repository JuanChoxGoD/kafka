from django.db import models
class Payment(models.Model):
    order_id = models.IntegerField(unique=True)
    amount = models.DecimalField(max_digits=12, decimal_places=2)
    status = models.CharField(max_length=20)
    payment_method = models.CharField(max_length=50)
    event_id = models.CharField(max_length=128, unique=True)
    processed_at = models.DateTimeField(auto_now_add=True)
    class Meta:
        db_table = 'payments'
class ProcessedEvent(models.Model):
    event_id = models.CharField(max_length=128, primary_key=True)
    created_at = models.DateTimeField(auto_now_add=True)
    class Meta:
        db_table = 'processed_events'
