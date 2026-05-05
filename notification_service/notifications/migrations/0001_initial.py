from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.SeparateDatabaseAndState(
            database_operations=[
                migrations.RunSQL(
                    sql="""
                    CREATE TABLE IF NOT EXISTS notification_processed_events (
                        event_id varchar(128) PRIMARY KEY,
                        created_at timestamp with time zone NOT NULL DEFAULT now()
                    );
                    """,
                    reverse_sql=migrations.RunSQL.noop,
                ),
            ],
            state_operations=[
                migrations.CreateModel(
                    name='Customer',
                    fields=[
                        ('id', models.IntegerField(primary_key=True, serialize=False)),
                        ('name', models.CharField(max_length=200)),
                        ('email', models.EmailField(max_length=254)),
                    ],
                    options={
                        'db_table': 'customers',
                        'managed': False,
                    },
                ),
                migrations.CreateModel(
                    name='Order',
                    fields=[
                        ('id', models.AutoField(db_column='id', primary_key=True, serialize=False)),
                        (
                            'customer',
                            models.ForeignKey(
                                db_column='customer_id',
                                on_delete=models.PROTECT,
                                to='notifications.customer',
                            ),
                        ),
                    ],
                    options={
                        'db_table': 'orders',
                        'managed': False,
                    },
                ),
                migrations.CreateModel(
                    name='ProcessedEvent',
                    fields=[
                        ('event_id', models.CharField(max_length=128, primary_key=True, serialize=False)),
                        ('created_at', models.DateTimeField(auto_now_add=True)),
                    ],
                    options={
                        'db_table': 'notification_processed_events',
                    },
                ),
            ],
        ),
    ]
