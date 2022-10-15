web: gunicorn app:app --log-file=-
worker: celery -A src.worker:celery worker --without-gossip --without-mingle --without-heartbeat --loglevel=INFO
beat: celery -A src.worker:celery beat --without-gossip --without-mingle --without-heartbeat --loglevel=INFO