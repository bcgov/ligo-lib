
LINK_DB_USER={{ customconfig['db_user'] }}
LINK_DB_HOST={{ customconfig['db_host'] }}
LINK_DB_PORT={{ customconfig['db_port'] }}
LINK_DB_SERVICE= {{ customconfig['db_service'] }}
LINK_DB_PASSWORD=

#LINK_DB_HOST=http://postgresql2-linking-project-dev-2-1-0.10.0.75.2.xip.io
#LINK_DB_PASSWORD=postgres
#LINK_DB_SERVICE=postgresql2

IN_DOCKER=1
C_FORCE_ROOT={{ customconfig['celery_as_root'] }}

CELERY_BROKER_URL={{ customconfig['celery_broker_url'] }}
# General settings
DJANGO_ADMIN_URL=
#DJANGO_SETTINGS_MODULE=config.settings.local
DJANGO_SETTINGS_MODULE={{ customconfig['django_settings'] }}
#DJANGO_SECRET_KEY=e(q8huau(-+qz6oi9!k62#_+t61n(*7daz7vmv2439ns1+2=g^
DJANGO_SECRET_KEY={{ customconfig['django_secret_key'] }}
#DJANGO_ALLOWED_HOSTS=.localhost
DJANGO_ALLOWED_HOSTS={{ customconfig['django_allowed_host'] }}

# AWS Settings
DJANGO_AWS_ACCESS_KEY_ID=
DJANGO_AWS_SECRET_ACCESS_KEY=
DJANGO_AWS_STORAGE_BUCKET_NAME=

# Used with email
DJANGO_MAILGUN_API_KEY=
DJANGO_SERVER_EMAIL=
MAILGUN_SENDER_DOMAIN=

#If we want to see confirmation email in file then
#django.core.mail.backends.filebased.EmailBackend is an appropriate
#value for DJANGO_EMAIL_BACKEND
#If we want to see confirmation email on console then
#django.core.mail.backends.console.EmailBackend is an appropriate
#value for DJANGO_EMAIL_BACKEND
DJANGO_EMAIL_BACKEND = {{ customconfig['django_email_backend'] }}
#Sample value for directory under which the emails would be saved
#DJANGO_EMAIL_FILE_PATH=/tmp/django-email-dev
DJANGO_EMAIL_FILE_PATH={{ customconfig['django_email_file_path'] }}

# Security! Better to use DNS for this task, but you can use redirect
DJANGO_SECURE_SSL_REDIRECT=False

# django-allauth
DJANGO_ACCOUNT_ALLOW_REGISTRATION=True
# Sentry
DJANGO_SENTRY_DSN=
