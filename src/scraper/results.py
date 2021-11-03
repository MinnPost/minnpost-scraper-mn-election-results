import json
from datetime import datetime
from datetime import timedelta
import pytz
from datetimerange import DateTimeRange
from flask import jsonify, current_app
from src.extensions import db
from src.extensions import celery
from src.cache import clear_multiple_keys
from src.models import Result, Meta
from src.scraper import bp

newest_election = None
election = None

@celery.task(bind=True)
def scrape_results(self):
    result = Result()
    sources = result.read_sources()
    election = result.set_election()

    if election not in sources:
        return

    # Get metadata about election
    election_meta = result.set_election_metadata()
    inserted_count = 0
    updated_count = 0
    deleted_count = 0
    parsed_count = 0
    supplemented_count = 0
    group_count = 0

    for group in sources[election]:
        source = sources[election][group]
        group_count = group_count + 1

        if 'type' in source and source['type'] == 'results':
            # handle parsed results
            rows = result.parse_election(source, election_meta)
            for row in rows:
                parsed = result.parser(row, group)

                result = Result()
                result.from_dict(parsed, new=True)

                db.session.merge(result)
                inserted_count = inserted_count + 1
                parsed_count = parsed_count + 1
            # commit parsed rows
            db.session.commit()

    # Handle post processing actions. this only needs to happen once, not for every group.
    supplemental = result.post_processing('results')
    meta = Meta()
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
                    elif action == 'meta':
                        parsed = row # it's already in the format we need to save it
                        meta = Meta()
                        meta.from_dict(parsed, new=True)
                        db.session.merge(meta)
    # commit supplemental rows
    db.session.commit()

    result = {
        "sources": group_count,
        "inserted": inserted_count,
        "updated": updated_count,
        "deleted": deleted_count,
        "parsed": parsed_count,
        "supplemented": supplemented_count,
        "cache": clear_multiple_keys(current_app.config['QUERY_LIST_CACHE_KEY']),
        "status": "completed"
    }
    current_app.log.info(result)

    now = datetime.now(pytz.timezone('America/Chicago'))
    offset = now.strftime('%z')

    if current_app.config["ELECTION_DAY_RESULT_HOURS_START"] != "" and current_app.config["ELECTION_DAY_RESULT_HOURS_END"] != "":
        time_range = DateTimeRange(current_app.config["ELECTION_DAY_RESULT_HOURS_START"], current_app.config["ELECTION_DAY_RESULT_HOURS_END"])
        now_formatted = now.isoformat()
        if now_formatted in time_range:
            print("this is during election result hours")
            return "this is during election result hours"
        else:
            print("this is not during election result hours")
            return "this is not during election result hours"

    return json.dumps(result)


@bp.route("/results")
def results_index():
    """Add a new result scrape task and start running it after 10 seconds."""
    eta = datetime.utcnow() + timedelta(seconds=10)
    task = scrape_results.apply_async(eta=eta)
    return (
        jsonify(
            json.loads(task.get(propagate=False))
        ),
        202,
    )
