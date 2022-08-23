from celery import Celery
from celery.schedules import schedule
from celery.signals import worker_ready

from redbeat import RedBeatSchedulerEntry as Entry

from . import create_worker_app
from src.celery import configure_celery
from src.scraper.areas import scrape_areas
from src.scraper.contests import scrape_contests
from src.scraper.elections import scrape_elections
from src.scraper.questions import scrape_questions
from src.scraper.results import scrape_results

flask_app = create_worker_app()
celery = configure_celery(flask_app)

default_interval = schedule(run_every=flask_app.config["DEFAULT_SCRAPE_FREQUENCY"])  # seconds


# run tasks on startup
@worker_ready.connect
def at_start(sender, **kwargs):
    """Run tasks at startup"""
    with sender.app.connection() as conn:
        sender.app.send_task("src.scraper.areas.scrape_areas", app=celery, connection=conn)
        sender.app.send_task("src.scraper.elections.scrape_elections", app=celery, connection=conn)
        sender.app.send_task("src.scraper.contests.scrape_contests", app=celery, connection=conn)
        sender.app.send_task("src.scraper.questions.scrape_questions", app=celery, connection=conn)
        sender.app.send_task("src.scraper.results.scrape_results", app=celery, connection=conn)


# daily tasks
areas_entry = Entry('scrape_areas_task', 'src.scraper.areas.scrape_areas', default_interval, app=celery)
areas_entry.save()

contests_entry = Entry('scrape_contests_task', 'src.scraper.contests.scrape_contests', default_interval, app=celery)
contests_entry.save()

elections_entry = Entry('scrape_elections_task', 'src.scraper.elections.scrape_elections', default_interval, app=celery)
elections_entry.save()

questions_entry = Entry('scrape_questions_task', 'src.scraper.questions.scrape_questions', default_interval, app=celery)
questions_entry.save()

results_entry = Entry('scrape_results_task', 'src.scraper.results.scrape_results', default_interval, app=celery)
results_entry.save()
