#!/bin/bash

python manage.py migrate --noinput

python manage.py run_inventory_consumer > /proc/1/fd/1 2>&1 &
CONSUMER_PID=$!
echo "Inventory consumer started with PID $CONSUMER_PID"

exec gunicorn inventory.wsgi:application \
    --bind 0.0.0.0:8080 \
    --workers 2 \
    --timeout 120
