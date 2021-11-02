import json
from datetime import datetime
from datetime import timedelta
from flask import jsonify, current_app
from src.extensions import db
from src.extensions import celery
from src.cache import clear_multiple_keys
from src.models import Area
from src.scraper import bp

newest_election = None
election = None

@celery.task(bind=True)
def scrape_areas(self):
    area = Area()
    sources = area.read_sources()
    election = area.set_election()

    if election not in sources:
        return

    # Get metadata about election
    election_meta = area.set_election_metadata()
    inserted_count = 0
    parsed_count = 0
    group_count = 0

    for group in sources[election]:
        source = sources[election][group]
        group_count = group_count + 1

        if 'type' in source and source['type'] == 'areas':
            # handle parsed areas
            rows = area.parse_election(source, election_meta)

            for row in rows:
                parsed = area.parser(row, group)

                area = Area()
                area.from_dict(parsed, new=True)

                db.session.merge(area)
                inserted_count = inserted_count + 1
                parsed_count = parsed_count + 1

                #self.update_state(
                #    state="PROGRESS", meta={"current": row, "total": parsed_count, "status": "scanning"}
                #)

            # commit parsed rows
            db.session.commit()
    
    #result = "sources: %s. Rows inserted: %s. Parsed rows: %s" % (str(group_count), str(inserted_count), str(parsed_count))
    result = {
        "sources": group_count,
        "inserted": inserted_count,
        "parsed": parsed_count,
        "cache": clear_multiple_keys(current_app.config['QUERY_LIST_CACHE_KEY']),
        "status": "completed"
    }
    #cache_result = clear_multiple_keys(current_app.config['QUERY_LIST_CACHE_KEY'])

    #result = result + cache_result
    current_app.log.info(result)
    return json.dumps(result)


#def on_raw_message(body):
#    print(body)


@bp.route("/areas")
def areas_index():
    """Add a new area scrape task and start running it after 10 seconds."""
    eta = datetime.utcnow() + timedelta(seconds=10)
    task = scrape_areas.apply_async(eta=eta)
    #print(task.get(on_message=on_raw_message, propagate=False))
    return (
        jsonify(
            #{"message": task.get(on_message=on_raw_message, propagate=False)}
            #json.loads(task.get(on_message=on_raw_message, propagate=False))
            json.loads(task.get(propagate=False))
        ),
        202,
    )
