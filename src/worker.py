from celery import Celery
from celery.schedules import crontab

from . import create_worker_app
#from src.tasks.long_task import log
#from src.tasks.long_task import reverse_messages
from src.scraper.areas import scrape_areas
from src.scraper.contests import scrape_contests
from src.scraper.meta import scrape_meta
from src.scraper.questions import scrape_questions
from src.scraper.results import scrape_results

def create_celery(app):
    celery = Celery(
        app.import_name,
        backend=app.config["RESULT_BACKEND"],
        broker=app.config["CELERY_BROKER_URL"],
    )
    celery.conf.update(app.config)
    TaskBase = celery.Task

    class ContextTask(TaskBase):
        abstract = True

        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)

    celery.Task = ContextTask
    return celery


flask_app = create_worker_app()
celery = create_celery(flask_app)


@celery.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    # Calls tasks at the desired frequency

    # daily tasks
    sender.add_periodic_task(86400.0, scrape_areas, name="scrape areas every day")
    sender.add_periodic_task(86400.0, scrape_contests, name="scrape contests every day")
    sender.add_periodic_task(86400.0, scrape_meta, name="scrape meta every day")
    sender.add_periodic_task(86400.0, scrape_questions, name="scrape questions every day")
    sender.add_periodic_task(86400.0, scrape_results, name="scrape results every day")

    # Calls reverse_messages every 10 seconds.
    #sender.add_periodic_task(10.0, reverse_messages, name="reverse every 10")

    # Calls log('Logging Stuff') every 30 seconds
    #sender.add_periodic_task(30.0, log.s(("Logging Stuff")), name="Log every 30")

    # Executes every Monday morning at 7:30 a.m.
    #sender.add_periodic_task(
    #    crontab(hour=7, minute=30, day_of_week=1), log.s("Monday morning log!"),
    #)
