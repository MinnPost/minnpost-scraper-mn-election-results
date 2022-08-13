import os
from flask import Flask, jsonify, request, current_app

from src.extensions import register_extensions

from config import Config

def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)

    register_extensions(app)

    #from src.errors import bp as errors_bp
    #app.register_blueprint(errors_bp)

    from src.scraper import bp as scraper_bp
    app.register_blueprint(scraper_bp, url_prefix='/scraper')

    from src.api import bp as api_bp
    app.register_blueprint(api_bp, url_prefix='/api')

    return app

def create_worker_app(config_class=Config):
    """Minimal App without routes for celery worker."""
    app = Flask(__name__)
    app.config.from_object(config_class)

    register_extensions(app, worker=True)

    return app


from src import models
