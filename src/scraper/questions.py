import os
from flask import jsonify, request, current_app
from src.extensions import db
from src.cache import clear_multiple_keys
from src.models import Question
from src.scraper import bp
#from src.api.auth import token_auth
#from src.api.errors import bad_request

newest_election = None
election = None

@bp.route('/questions')
def scrape_questions():
    question = Question()
    sources = question.read_sources()
    election = question.set_election()

    if election not in sources:
        return

    # Get metadata about election
    election_meta = question.set_election_metadata()
    inserted_count = 0
    parsed_count = 0
    group_count = 0

    for group in sources[election]:
        source = sources[election][group]
        group_count = group_count + 1

        if 'type' in source and source['type'] == 'questions':

            rows = question.parse_election(source, election_meta)

            for row in rows:
                parsed = question.parser(row, group)

                question = Question()
                question.from_dict(parsed, new=True)

                db.session.merge(question)
                inserted_count = inserted_count + 1
                parsed_count = parsed_count + 1
            # commit parsed rows
            db.session.commit()

    result = "Elections scanned: %s. Rows inserted: %s. Parsed rows: %s" % (str(group_count), str(inserted_count), str(parsed_count))
    cache_result = clear_multiple_keys(current_app.config['QUERY_LIST_CACHE_KEY'])

    result = result + cache_result
    current_app.log.info(result)
    return result
