import json
from datetime import datetime
from datetime import timedelta
from flask import jsonify, current_app
from src.extensions import db
from src.extensions import celery
from src.storage import Storage
from src.models import Election
from src.scraper import bp

@celery.task(bind=True)
def scrape_elections(self, args={}):
    storage     = Storage()
    election    = Election()
    class_name  = Election.get_classname()
    sources     = election.read_sources()
    contests    = args.get('contests', None)
    results     = args.get('results', None)
    election_id = None

    inserted_count = 0
    parsed_count = 0

    if contests is not None:
        election_id = contests['election_id']
    elif results is not None:
        election_id = results['election_id']

    if election_id == None:
        for key in sources:
            row = sources[key]
            parsed = election.parser(row, key)
            election.from_dict(parsed, new=True)

            db.session.merge(election)
            inserted_count = inserted_count + 1
            parsed_count = parsed_count + 1
    else:
        key = election.set_election_key(election_id)
        row = sources[key]
        parsed = election.parser(row, key)
        election.from_dict(parsed, new=True)

        db.session.merge(election)
        inserted_count = inserted_count + 1
        parsed_count = parsed_count + 1

    # commit parsed rows
    db.session.commit()

    if args == []:
        result = {
            "inserted": inserted_count,
            "parsed": parsed_count,
            "cache": storage.clear_group(class_name),
            "status": "completed"
        }
    else:
        result = {
            "elections": {
                "inserted": inserted_count,
                "parsed": parsed_count,
                "cache": storage.clear_group(class_name),
                "status": "completed"
            }
        }
        if contests is not None:
            result['contests'] = contests
        elif results is not None:
            result['results'] = results
    #current_app.log.debug(result)
    return json.dumps(result)


@bp.route("/elections/")
def elections_index():
    """Add a new election scrape task and start running it after 10 seconds."""
    eta = datetime.utcnow() + timedelta(seconds=10)
    task = scrape_elections.apply_async(eta=eta)
    return (
        jsonify(
            json.loads(task.get(propagate=False))
        ),
        202,
    )
