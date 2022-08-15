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
def scrape_elections(self):
    storage    = Storage()
    election   = Election()
    class_name = Election.get_classname()
    sources    = election.read_sources()
    election   = election.set_election()

    if election not in sources:
        return

    # Get metadata about election
    election_meta = election.set_election_metadata()
    inserted_count = 0
    parsed_count = 0
    group_count = 0

    for group in sources[election]:
        source = sources[election][group]
        group_count = group_count + 1

        if 'type' in source and source['type'] == 'areas':
            # handle parsed areas
            rows = election.parse_election(source, election_meta)

            for row in rows:
                parsed = election.parser(row, group)

                election = Election()
                election.from_dict(parsed, new=True)

                db.session.merge(election)
                inserted_count = inserted_count + 1
                parsed_count = parsed_count + 1

            # commit parsed rows
            db.session.commit()
    
    result = {
        "sources": group_count,
        "inserted": inserted_count,
        "parsed": parsed_count,
        "cache": storage.clear_group(class_name),
        "status": "completed"
    }
    current_app.log.info(result)
    return json.dumps(result)


@bp.route("/elections")
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
