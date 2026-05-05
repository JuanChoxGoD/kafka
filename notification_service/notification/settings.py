import os
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent
SECRET_KEY = 'notification-secret-key'
DEBUG = True
ALLOWED_HOSTS = ['*']
INSTALLED_APPS = [
    'django.contrib.contenttypes',
    'notifications',
]
MIDDLEWARE = [
    'django.middleware.common.CommonMiddleware',
]
ROOT_URLCONF = 'notification.urls'
TEMPLATES = []
WSGI_APPLICATION = 'notification.wsgi.application'
POSTGRES_HOST = os.environ.get('POSTGRES_HOST')
if os.environ.get('K_SERVICE') and POSTGRES_HOST in (None, '', 'postgres'):
    POSTGRES_HOST = '/cloudsql/project-e15b8e8c-0432-43ea-ac0:us-central1:db-comercial'
POSTGRES_HOST = POSTGRES_HOST or 'postgres'
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': os.environ.get('POSTGRES_DB', 'commercial_db'),
        'USER': os.environ.get('POSTGRES_USER', 'postgres'),
        'PASSWORD': os.environ.get('POSTGRES_PASSWORD', 'postgres'),
        'HOST': POSTGRES_HOST,
        'PORT': os.environ.get('POSTGRES_PORT', '5432'),
    }
}
EMAIL_BACKEND = 'django.core.mail.backends.smtp.EmailBackend'
EMAIL_HOST = os.environ.get('SMTP_HOST', 'smtp.sendgrid.net')
EMAIL_PORT = int(os.environ.get('SMTP_PORT', '587'))
EMAIL_HOST_USER = os.environ.get('SMTP_USER', 'apikey')
EMAIL_HOST_PASSWORD = os.environ.get('SMTP_PASSWORD') or os.environ.get('SENDGRID_API_KEY', '')
EMAIL_USE_TLS = os.environ.get('SMTP_USE_TLS', 'True').lower() in ('true', '1', 'yes')
EMAIL_USE_SSL = os.environ.get('SMTP_USE_SSL', 'False').lower() in ('true', '1', 'yes')
DEFAULT_FROM_EMAIL = os.environ.get('EMAIL_FROM') or os.environ.get('SENDGRID_FROM_EMAIL', 'storresp37@gmail.com')
EMAIL_TIMEOUT = int(os.environ.get('EMAIL_TIMEOUT', '20'))
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_API_KEY = os.environ.get('KAFKA_API_KEY')
KAFKA_API_SECRET = os.environ.get('KAFKA_API_SECRET')
