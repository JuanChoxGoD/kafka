#!/bin/bash
nohup python manage.py run_billing_consumer > /proc/1/fd/1 2>&1 &
exec gunicorn billing.wsgi:application \
    --bind 0.0.0.0:8080 \
    --workers 2 \
    --timeout 120
