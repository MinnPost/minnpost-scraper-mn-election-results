import os
from pathlib import Path

basedir = os.path.abspath(os.path.dirname(__file__))

class Config(object):
    BASE_DIR = Path(__file__).parent.parent
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'you-will-never-guess'
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL', '').replace(
        'postgres://', 'postgresql://') or \
        'sqlite:///' + os.path.join(basedir, 'app.db')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_POOL_SIZE=int(os.environ.get("SQLALCHEMY_POOL_SIZE"))
    SQLALCHEMY_POOL_TIMEOUT=int(os.environ.get("SQLALCHEMY_POOL_TIMEOUT"))
    CELERY_BROKER_URL = os.environ.get("CELERY_BROKER_URL", "redis://127.0.0.1:6379/0")
    RESULT_BACKEND = os.environ.get("RESULT_BACKEND", "redis://127.0.0.1:6379/0")
    CACHE_TYPE = os.environ.get("CACHE_TYPE", "redis")
    CACHE_REDIS_HOST = os.environ.get("CACHE_REDIS_HOST", "redis")
    CACHE_REDIS_PORT = os.environ.get("CACHE_REDIS_PORT", 6379)
    CACHE_REDIS_DB = os.environ.get("CACHE_REDIS_DB", 0)
    CACHE_REDIS_URL = os.environ.get("CACHE_REDIS_URL", "redis://127.0.0.1:6379/0")
    CACHE_DEFAULT_TIMEOUT = os.environ.get("CACHE_DEFAULT_TIMEOUT", 500)
    QUERY_LIST_CACHE_KEY = os.environ.get("QUERY_LIST_CACHE_KEY", "")
