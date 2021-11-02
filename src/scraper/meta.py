import json
from datetime import datetime
from datetime import timedelta
from flask import jsonify, current_app
from src.extensions import db
from src.extensions import celery
from src.cache import clear_multiple_keys
from src.models import Meta
from src.scraper import bp

newest_election = None
election = None

@celery.task(bind=True)
def scrape_meta(self):
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

    result = {
        "sources": group_count,
        "inserted": inserted_count,
        "parsed": parsed_count,
        "cache": clear_multiple_keys(current_app.config['QUERY_LIST_CACHE_KEY']),
        "status": "completed"
    }
    current_app.log.info(result)
    return json.dumps(result)


@bp.route("/meta")
def meta_index():
    """Add a new meta scrape task and start running it after 10 seconds."""
    eta = datetime.utcnow() + timedelta(seconds=10)
    task = scrape_meta.apply_async(eta=eta)
    return (
        jsonify(
            json.loads(task.get(propagate=False))
        ),
        202,
    )
