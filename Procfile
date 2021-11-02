#worker: celery -A app.celery worker --without-heartbeat --without-gossip --without-mingle --beat --loglevel=INFO
#worker: celery worker -A celery_worker.celery --beat --loglevel=info
web: gunicorn app:app --log-file=-
worker: celery -A src.worker:celery worker --loglevel=INFO
beat: celery -A src.worker:celery beat --loglevel=INFO