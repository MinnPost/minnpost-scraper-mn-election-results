import os
from flask import jsonify, request, current_app
from src import db
from src.cache import clear_multiple_keys
from src.models import Contest, Meta
from src.scraper import bp
#from src.api.auth import token_auth
#from src.api.errors import bad_request

newest_election = None
election = None

@bp.route('/contests')
def scrape_contests():
    contest = Contest()
    sources = contest.read_sources()
    election = contest.set_election()

    if election not in sources:
        return

    # Get metadata about election
    election_meta = contest.set_election_metadata()
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
            # handle parsed contests
            rows = contest.parse_election(source, election_meta)
            for row in rows:
                parsed = contest.parser(row, group, source)

                contest = Contest()
                contest.from_dict(parsed, new=True)

                db.session.merge(contest)
                inserted_count = inserted_count + 1
                parsed_count = parsed_count + 1
            # commit parsed rows
            db.session.commit()
            
    # Handle post processing actions. this only needs to happen once, not for every group.
    supplemental = contest.post_processing('contests')
    meta = Meta()
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
                    elif action == 'meta':
                        parsed = row # it's already in the format we need to save it
                        meta = Meta()
                        meta.from_dict(parsed, new=True)
                        db.session.merge(meta)
    # commit supplemental rows
    db.session.commit()

    result = "Elections scanned: %s. Rows inserted: %s; Rows updated: %s; Rows deleted: %s. Parsed rows: %s Supplemental rows: %s" % (str(group_count), str(inserted_count), str(updated_count), str(deleted_count), str(parsed_count), str(supplemented_count))
    cache_result = clear_multiple_keys(current_app.config['QUERY_LIST_CACHE_KEY'])
    result = result + cache_result
    current_app.log.info(result)
    return result
