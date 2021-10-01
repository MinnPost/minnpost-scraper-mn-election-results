web: gunicorn app:app --log-file=-
worker: celery worker --app=tasks.app