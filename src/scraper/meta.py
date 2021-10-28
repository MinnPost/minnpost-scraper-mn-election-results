import os
from flask import jsonify, request, current_app
from src import db
from src.cache import clear_multiple_keys
from src.models import Meta
from src.scraper import bp
#from src.api.auth import token_auth
#from src.api.errors import bad_request

newest_election = None
election = None

@bp.route('/meta')
def scrape_meta():
    meta = Meta()
    sources = meta.read_sources()
    election = meta.set_election()

    if election not in sources:
        return

    # set row counts
    inserted_count = 0
    parsed_count = 0
    group_count = 0

    for group in sources[election]:
        group_count = group_count + 1
        
        if 'meta' in sources[election]:
            rows = sources[election]['meta']

            for m in rows:
                row = rows[m]
                parsed = meta.parser(m, row)
                meta = Meta()
                meta.from_dict(parsed, new=True)

                db.session.merge(meta)
                inserted_count = inserted_count + 1
                parsed_count = parsed_count + 1
            # commit parsed rows
            db.session.commit()

    result = "Elections scanned: %s. Rows inserted: %s. Parsed rows: %s" % (str(group_count), str(inserted_count), str(parsed_count))
    cache_result = clear_multiple_keys(current_app.config['QUERY_LIST_CACHE_KEY'])

    result = result + cache_result
    current_app.log.info(result)
    return result
