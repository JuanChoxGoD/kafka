#!/bin/bash
python manage.py run_shipping_consumer &
exec gunicorn shipping.wsgi:application \
    --bind 0.0.0.0:8080 \
    --workers 2 \
    --timeout 120
