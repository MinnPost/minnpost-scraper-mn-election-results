from celery import Celery
from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy

from src.cache import cache
from src.logger import ScraperLogger

db = SQLAlchemy()
celery = Celery()
migrate = Migrate(compare_type=True)


def register_extensions(app, worker=False):

    db.init_app(app)
    migrate.init_app(app, db)
    cache.init_app(app)

    # load celery config
    celery.config_from_object(app.config)

    app.log = ScraperLogger('scraper_results').logger

    if not worker:
        # register celery irrelevant extensions
        pass
