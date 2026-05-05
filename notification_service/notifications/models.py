from django.db import models
class Customer(models.Model):
    id = models.IntegerField(primary_key=True)
    name = models.CharField(max_length=200)
    email = models.EmailField()
    class Meta:
        db_table = 'customers'
        managed = False
class Order(models.Model):
    id = models.AutoField(primary_key=True, db_column='id')
    customer = models.ForeignKey(Customer, db_column='customer_id', on_delete=models.PROTECT)
    class Meta:
        db_table = 'orders'
        managed = False
class ProcessedEvent(models.Model):
    event_id = models.CharField(max_length=128, primary_key=True)
    created_at = models.DateTimeField(auto_now_add=True)
    class Meta:
        db_table = 'notification_processed_events'
