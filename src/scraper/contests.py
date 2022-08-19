import json
from datetime import datetime
from datetime import timedelta
from flask import jsonify, current_app
from src.extensions import db
from src.extensions import celery
from src.storage import Storage
from src.models import Contest
from src.scraper import bp

@celery.task(bind=True)
def scrape_contests(self):
    storage    = Storage()
    contest    = Contest()
    class_name = Contest.get_classname()
    sources    = contest.read_sources()
    election   = contest.set_election()
    election_key = election.election_key

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
            # handle parsed contests
            rows = contest.parse_election(source, election)
            for row in rows:
                parsed = contest.parser(row, group, election, source)

                contest = Contest()
                contest.from_dict(parsed, new=True)

                db.session.merge(contest)
                inserted_count = inserted_count + 1
                parsed_count = parsed_count + 1
            # commit parsed rows
            db.session.commit()
            
    # Handle post processing actions. this only needs to happen once, not for every group.
    supplemental = contest.post_processing('contests', election.id)
    #meta = Meta()
    for supplemental_contest in supplemental:
        rows = supplemental_contest['rows']
        action = supplemental_contest['action']
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
    return json.dumps(result)


@bp.route("/contests")
def contests_index():
    """Add a new contest scrape task and start running it after 10 seconds."""
    eta = datetime.utcnow() + timedelta(seconds=10)
    task = scrape_contests.apply_async(eta=eta)
    return (
        jsonify(
            json.loads(task.get(propagate=False))
        ),
        202,
    )
