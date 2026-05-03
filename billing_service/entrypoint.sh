#!/bin/bash
python manage.py run_billing_consumer &
exec gunicorn billing.wsgi:application \
    --bind 0.0.0.0:8080 \
    --workers 2 \
    --timeout 120
