import logging
from flask import Blueprint
from project import create_app, ext_celery

app = create_app()
celery = ext_celery.celery

LOG = logging.getLogger(__name__)

@app.route("/")
def hello_world():
    LOG.debug("Call health ok")
    return "Hello, World!"
