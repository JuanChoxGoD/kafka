import os
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent
SECRET_KEY = 'shipping-secret-key'
DEBUG = True
ALLOWED_HOSTS = ['*']
INSTALLED_APPS = [
    'django.contrib.contenttypes',
    'shipping_app',
]
MIDDLEWARE = [
    'django.middleware.common.CommonMiddleware',
]
ROOT_URLCONF = 'shipping.urls'
TEMPLATES = []
WSGI_APPLICATION = 'shipping.wsgi.application'
POSTGRES_HOST = os.environ.get('POSTGRES_HOST')
if os.environ.get('K_SERVICE') and POSTGRES_HOST in (None, '', 'postgres'):
    POSTGRES_HOST = '/cloudsql/project-e15b8e8c-0432-43ea-ac0:us-central1:db-logistica'
POSTGRES_HOST = POSTGRES_HOST or 'postgres'
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': os.environ.get('POSTGRES_DB', 'logistics_db'),
        'USER': os.environ.get('POSTGRES_USER', 'postgres'),
        'PASSWORD': os.environ.get('POSTGRES_PASSWORD', 'postgres'),
        'HOST': POSTGRES_HOST,
        'PORT': os.environ.get('POSTGRES_PORT', '5432'),
    }
}
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_API_KEY = os.environ.get('KAFKA_API_KEY')
KAFKA_API_SECRET = os.environ.get('KAFKA_API_SECRET')
