from django.db import models
class Customer(models.Model):
    id = models.IntegerField(primary_key=True)
    name = models.CharField(max_length=200)
    email = models.EmailField()
    class Meta:
        db_table = 'customers'
class Product(models.Model):
    id = models.IntegerField(primary_key=True)
    name = models.CharField(max_length=200)
    sku = models.CharField(max_length=100)
    unit_price = models.DecimalField(max_digits=12, decimal_places=2)
    class Meta:
        db_table = 'products'
class Order(models.Model):
    STATUS_CHOICES = [
        ('CREATED', 'Created'),
        ('PAID', 'Paid'),
        ('SHIPPED', 'Shipped'),
    ]
    customer = models.ForeignKey(Customer, on_delete=models.PROTECT)
    product = models.ForeignKey(Product, on_delete=models.PROTECT)
    quantity = models.IntegerField()
    total_amount = models.DecimalField(max_digits=12, decimal_places=2)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='CREATED')
    event_id = models.CharField(max_length=128, unique=True)
    created_at = models.DateTimeField(auto_now_add=True)
    class Meta:
        db_table = 'orders'
