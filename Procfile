web: gunicorn app:app --log-file=-
worker: celery -A src.worker:celery worker --purge --loglevel=INFO
beat: celery -A src.worker:celery beat -S redbeat.RedBeatScheduler --loglevel=INFO