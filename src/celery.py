from celery import Celery
from flask import Flask

from src import extensions

def configure_celery(app):
    TaskBase = extensions.celery.Task

    class ContextTask(TaskBase):
        abstract = True

        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)

    extensions.celery.conf.update(
        broker_url=app.config['CELERY_BROKER_URL'],
        result_backend=app.config['RESULT_BACKEND'],
        redbeat_redis_url = app.config["REDBEAT_REDIS_URL"]
    )

    extensions.celery.Task = ContextTask
    return extensions.celery
