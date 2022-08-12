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
    SQLALCHEMY_ECHO=os.environ.get("SQLALCHEMY_ECHO", "false")
    if SQLALCHEMY_ECHO == "true":
        SQLALCHEMY_ECHO = True
    else:
        SQLALCHEMY_ECHO = False

    # by default, use the REDIS_URL for all the redis urls.
    # but they could also each have distinct values in the environment settings.
    REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")
    CELERY_BROKER_URL = os.environ.get("CELERY_BROKER_URL", REDIS_URL)
    RESULT_BACKEND = os.environ.get("RESULT_BACKEND", REDIS_URL)
    #CELERY_RESULT_BACKEND = RESULT_BACKEND # this is deprecated but heroku still seems to like it.
    REDBEAT_REDIS_URL = os.environ.get("REDBEAT_REDIS_URL", REDIS_URL)
    CACHE_REDIS_URL = os.environ.get("CACHE_REDIS_URL", REDIS_URL)

    CACHE_TYPE = os.environ.get("CACHE_TYPE", "RedisCache")
    CACHE_DEFAULT_TIMEOUT = os.environ.get("CACHE_DEFAULT_TIMEOUT", 500)
    QUERY_LIST_CACHE_KEY = os.environ.get("QUERY_LIST_CACHE_KEY", "")
    DEFAULT_SCRAPE_FREQUENCY = int(os.environ.get("DEFAULT_SCRAPE_FREQUENCY", 3600))
    ELECTION_DAY_RESULT_SCRAPE_FREQUENCY = int(os.environ.get("ELECTION_DAY_RESULT_SCRAPE_FREQUENCY", 60))
    ELECTION_DAY_RESULT_HOURS_START = os.environ.get("ELECTION_DAY_RESULT_HOURS_START", "")
    ELECTION_DAY_RESULT_HOURS_END = os.environ.get("ELECTION_DAY_RESULT_HOURS_END", "")
    PARSER_API_CACHE_TIMEOUT = os.environ.get("PARSER_API_CACHE_TIMEOUT", 500)
    PARSER_API_KEY = os.environ.get("PARSER_API_KEY", "")
    AUTHORIZE_API_URL = os.environ.get("AUTHORIZE_API_URL", "")
    PARSER_API_URL = os.environ.get("PARSER_API_URL", "")
    OVERWRITE_API_URL = os.environ.get("OVERWRITE_API_URL", "")
    PARSER_STORE_IN_S3 = os.environ.get("PARSER_STORE_IN_S3", "")
    PARSER_BYPASS_API_CACHE = os.environ.get("PARSER_BYPASS_API_CACHE", "false")
