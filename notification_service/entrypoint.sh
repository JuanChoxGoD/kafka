#!/bin/bash
python manage.py run_notification_consumer &
exec gunicorn notification.wsgi:application \
    --bind 0.0.0.0:8080 \
    --workers 2 \
    --timeout 120
