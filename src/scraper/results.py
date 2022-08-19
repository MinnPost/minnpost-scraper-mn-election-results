import json
from datetime import datetime
from datetime import timedelta
import pytz
from datetimerange import DateTimeRange
from redbeat import RedBeatSchedulerEntry as Entry
from celery.schedules import schedule
from flask import jsonify, current_app, request
from src.extensions import db
from src.extensions import celery
from src.storage import Storage
from src.models import Result
from src.scraper import bp

newest_election = None
election = None

@celery.task(bind=True)
def scrape_results(self, election_id = None):
    storage      = Storage()
    result       = Result()
    class_name   = Result.get_classname()
    election     = result.set_election(election_id)
    if election is None:
        return

    sources      = result.read_sources()
    election_key = result.set_election_key(election.id)
    if election_key not in sources:
        return

    # set up count for results
    inserted_count = 0
    updated_count = 0
    deleted_count = 0
    parsed_count = 0
    supplemented_count = 0
    group_count = 0

    for group in sources[election_key]:
        source = sources[election_key][group]
        group_count = group_count + 1

        if 'type' in source and source['type'] == 'results':
            # handle parsed results
            rows = result.parse_election(source, election)
            for row in rows:
                parsed = result.parser(row, group, election.id)

                result = Result()
                result.from_dict(parsed, new=True)

                db.session.merge(result)
                inserted_count = inserted_count + 1
                parsed_count = parsed_count + 1
            # commit parsed rows
            db.session.commit()

    # Handle post processing actions. this only needs to happen once, not for every group.
    supplemental = result.post_processing('results', election.id)
    #meta = Meta()
    for supplemental_result in supplemental:
        rows = supplemental_result['rows']
        action = supplemental_result['action']
        if action is not None and rows != []:
            for row in rows:
                if row is not []:
                    if action == 'insert' or action == 'update':
                        db.session.merge(row)
                        if action == 'insert':
                            inserted_count = inserted_count + 1
                        elif action == 'update':
                            updated_count = updated_count + 1
                        supplemented_count = supplemented_count + 1
                    elif action == 'delete':
                        db.session.delete(row)
                        deleted_count = deleted_count + 1
                    #elif action == 'meta':
                    #    parsed = row # it's already in the format we need to save it
                    #    meta = Meta()
                    #    meta.from_dict(parsed, new=True)
                    #    db.session.merge(meta)
    # commit supplemental rows
    db.session.commit()

    result = {
        "sources": group_count,
        "inserted": inserted_count,
        "updated": updated_count,
        "deleted": deleted_count,
        "parsed": parsed_count,
        "supplemented": supplemented_count,
        "cache": storage.clear_group(class_name),
        "status": "completed"
    }
    current_app.log.debug(result)

    now = datetime.now(pytz.timezone('America/Chicago'))
    #offset = now.strftime('%z')

    entry_key = 'scrape_results_task'
    prefixed_entry_key = 'redbeat:' + entry_key
    interval = schedule(run_every=current_app.config["DEFAULT_SCRAPE_FREQUENCY"])

    if current_app.config["ELECTION_DAY_RESULT_HOURS_START"] != "" and current_app.config["ELECTION_DAY_RESULT_HOURS_END"] != "":
        time_range = DateTimeRange(current_app.config["ELECTION_DAY_RESULT_HOURS_START"], current_app.config["ELECTION_DAY_RESULT_HOURS_END"])
        now_formatted = now.isoformat()
        if now_formatted in time_range:
            current_app.log.info("this is during election result hours")
            interval = schedule(run_every=current_app.config["ELECTION_DAY_RESULT_SCRAPE_FREQUENCY"])
            try:
                e = Entry.from_key(prefixed_entry_key)
                if e.schedule != interval:
                    current_app.log.info(f"the current schedule is {e.schedule}. change to {interval}.")
                    e = Entry(entry_key, 'src.scraper.results.scrape_results', interval, app=celery)
                    e.save()
                else:
                    current_app.log.info(f"the current schedule is already {interval}")
            except Exception as err:
                current_app.log.info("create the election result hours task")
                e = Entry(entry_key, 'src.scraper.results.scrape_results', interval, app=celery)
                e.save()
        else:
            current_app.log.info("this is not during election result hours")
            try:
                e = Entry.from_key(prefixed_entry_key)
                if e.schedule != interval:
                    current_app.log.info(f"the current schedule is {e.schedule}. reset to {interval}.")
                    e = Entry(entry_key, 'src.scraper.results.scrape_results', interval, app=celery)
                    e.save()
                else:
                    current_app.log.info(f"the current schedule is already {interval}")
            except Exception as err:
                current_app.log.info("this task does not exist. create it.")
                e = Entry(entry_key, 'src.scraper.results.scrape_results', interval, app=celery)
                e.save()
    return json.dumps(result)


@bp.route("/results/")
def results_index():
    """Add a new result scrape task and start running it after 10 seconds."""
    if request.is_json:
        # JSON request
        request_json = request.get_json()
        election_id  = request_json.get('election_id')
    elif request.method == 'POST':
        # form request
        election_id = request.form.get('election_id', None)
    else:
        # GET request
        election_id = request.values.get('election_id', None)
    eta = datetime.utcnow() + timedelta(seconds=10)
    task = scrape_results.apply_async(args=[election_id], eta=eta)
    return (
        jsonify(
            json.loads(task.get(propagate=False))
        ),
        202,
    )
