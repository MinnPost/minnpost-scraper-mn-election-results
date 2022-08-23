import json
from datetime import datetime
from datetime import timedelta
from flask import current_app, request, Response
from src.extensions import db
from src.extensions import celery
from src.storage import Storage
from src.models import Contest
from src.scraper import bp
from src.scraper import elections
from celery import chain

@celery.task(bind=True)
def scrape_contests(self, election_id = None):
    storage      = Storage()
    contest      = Contest()
    class_name   = Contest.get_classname()
    election     = contest.set_election(election_id)
    if election is None:
        return

    sources      = contest.read_sources()
    election_key = contest.set_election_key(election.id)
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
            parsed_election = contest.parse_election(source, election)
            rows = parsed_election['rows']
            updated = parsed_election['updated']
            for row in rows:
                parsed = contest.parser(row, group, election, source, updated)

                contest = Contest()
                contest.from_dict(parsed, new=True)

                db.session.merge(contest)
                inserted_count = inserted_count + 1
                parsed_count = parsed_count + 1
            # commit parsed rows
            db.session.commit()
            
    # Handle post processing actions. this only needs to happen once, not for every group.
    supplemental = contest.post_processing('contests', election.id)
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
    # commit supplemental rows
    db.session.commit()

    result = {
        "contests" : {
            "sources": group_count,
            "inserted": inserted_count,
            "updated": updated_count,
            "deleted": deleted_count,
            "parsed": parsed_count,
            "supplemented": supplemented_count,
            "cache": storage.clear_group(class_name),
            "status": "completed"
        }
    }
    #current_app.log.debug(result)
    return result


@bp.route("/contests/")
def contests_index():
    """Add a new contest scrape task and start running it after 10 seconds."""
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
    res = chain(scrape_contests.s() | elections.scrape_elections.s()).apply_async(args=[election_id], eta=eta)
    output = res.get()

    # set up the response and return it
    mime = 'application/json'
    ctype = 'application/json; charset=UTF-8'

    res = Response(response = output, status = 200, mimetype = mime)
    res.headers['Content-Type'] = ctype
    res.headers['Connection'] = 'keep-alive'
    res.headers.add("Access-Control-Allow-Origin", "*")
    return res
