import logging
import os
from flask import Flask, jsonify, request, current_app
from flask_celeryext import FlaskCeleryExt
from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy

from src.cache import cache

from config import Config


db = SQLAlchemy()
migrate = Migrate(compare_type=True)

def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)

    db.init_app(app)
    migrate.init_app(app, db)
    cache.init_app(app)

    #app.task_queue = rq.Queue('microblog-tasks', connection=app.redis)

    #from src.errors import bp as errors_bp
    #app.register_blueprint(errors_bp)

    from src.scraper import bp as scraper_bp
    app.register_blueprint(scraper_bp, url_prefix='/scraper')

    from src.api import bp as api_bp
    app.register_blueprint(api_bp, url_prefix='/api')

    #from src.main import bp as main_bp
    #app.register_blueprint(main_bp)

    return app


from src import models
