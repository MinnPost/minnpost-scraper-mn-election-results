import json
from datetime import datetime
from datetime import timedelta
from flask import jsonify, current_app, request
from src.extensions import db
from src.extensions import celery
from src.storage import Storage
from src.models import Question
from src.scraper import bp
from src.scraper import elections
from celery import chain

@celery.task(bind=True)
def scrape_questions(self, election_id = None):
    storage      = Storage()
    question     = Question()
    class_name   = Question.get_classname()
    election     = question.set_election(election_id)
    if election is None:
        return

    sources      = question.read_sources()
    election_key = question.set_election_key(election.id)
    if election_key not in sources:
        return

    # set up count for results
    inserted_count = 0
    parsed_count = 0
    group_count = 0

    for group in sources[election_key]:
        source = sources[election_key][group]
        group_count = group_count + 1

        if 'type' in source and source['type'] == 'questions':

            parsed_election = question.parse_election(source, election)
            rows = parsed_election['rows']
            updated = parsed_election['updated']

            for row in rows:
                parsed = question.parser(row, group, election.id, updated)

                question = Question()
                question.from_dict(parsed, new=True)

                db.session.merge(question)
                inserted_count = inserted_count + 1
                parsed_count = parsed_count + 1
            # commit parsed rows
            db.session.commit()

    result = {
        "election_id": election.id,
        "sources": group_count,
        "inserted": inserted_count,
        "parsed": parsed_count,
        "cache": storage.clear_group(class_name),
        "status": "completed"
    }
    #current_app.log.debug(result)
    return json.dumps(result)


@celery.task(bind=True)
def scrape_questions_chain(self, election_id = None):
    eta = datetime.utcnow() + timedelta(seconds=10)
    res = chain(scrape_questions.s() | elections.scrape_elections.s()).apply_async(args=[election_id], eta=eta)
    return res


@bp.route("/questions/")
def questions_index():
    """Add a new question scrape task and start running it after 10 seconds."""
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
    task = scrape_questions.apply_async(args=[election_id], eta=eta)
    return (
        jsonify(
            json.loads(task.get(propagate=False))
        ),
        202,
    )
