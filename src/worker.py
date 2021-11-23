from celery import Celery
from celery.schedules import schedule

from redbeat import RedBeatSchedulerEntry as Entry

from . import create_worker_app
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
        redbeat_redis_url = app.config["REDBEAT_REDIS_URL"],
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


default_interval = schedule(run_every=flask_app.config["DEFAULT_SCRAPE_FREQUENCY"])  # seconds

# daily tasks
areas_entry = Entry('scrape_areas_task', 'src.scraper.areas.scrape_areas', default_interval, app=celery)
areas_entry.save()

contests_entry = Entry('scrape_contests_task', 'src.scraper.contests.scrape_contests', default_interval, app=celery)
contests_entry.save()

meta_entry = Entry('scrape_meta_task', 'src.scraper.meta.scrape_meta', default_interval, app=celery)
meta_entry.save()

questions_entry = Entry('scrape_questions_task', 'src.scraper.questions.scrape_questions', default_interval, app=celery)
questions_entry.save()

results_entry = Entry('scrape_results_task', 'src.scraper.results.scrape_results', default_interval, app=celery)
results_entry.save()
