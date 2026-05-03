#!/bin/bash
python manage.py run_inventory_consumer &
exec gunicorn inventory.wsgi:application \
    --bind 0.0.0.0:8080 \
    --workers 2 \
    --timeout 120
