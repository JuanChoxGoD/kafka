#!/bin/bash
set -e

echo "Running migrations..."
python manage.py migrate

echo "Starting billing consumer in background..."
python manage.py run_billing_consumer &

echo "Starting Gunicorn on port 8080..."
exec gunicorn billing.wsgi.application \
    --bind 0.0.0.0:8080 \
    --workers 2 \
    --timeout 120
