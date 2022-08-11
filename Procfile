web: gunicorn app:app --log-file=-
worker: celery -A src.worker:celery worker --loglevel=INFO
beat: celery -A src.worker:celery beat --loglevel=INFO