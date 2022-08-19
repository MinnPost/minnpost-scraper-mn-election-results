import json
from datetime import datetime
from datetime import timedelta
from flask import jsonify, current_app, request
from src.extensions import db
from src.extensions import celery
from src.storage import Storage
from src.models import Area
from src.scraper import bp

newest_election = None
election = None

@celery.task(bind=True)
def scrape_areas(self, election_id = None):
    storage      = Storage()
    area         = Area()
    class_name   = Area.get_classname()
    election     = area.set_election(election_id)
    if election is None:
        return
        
    election_key = area.set_election_key(election.id)
    sources      = area.read_sources()
    if election_key not in sources:
        return

    # set up count for results
    inserted_count = 0
    parsed_count = 0
    group_count = 0

    for group in sources[election_key]:
        source = sources[election_key][group]
        group_count = group_count + 1

        if 'type' in source and source['type'] == 'areas':
            # handle parsed areas
            parsed_election = area.parse_election(source, election)
            rows = parsed_election['rows']
            updated = parsed_election['updated']

            for row in rows:
                parsed = area.parser(row, group, election.id, updated)

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
        "cache": storage.clear_group(class_name),
        "status": "completed"
    }
    #cache_result = clear_multiple_keys(current_app.config['QUERY_LIST_CACHE_KEY'])

    #result = result + cache_result
    current_app.log.debug(result)
    return json.dumps(result)


#def on_raw_message(body):
#    print(body)


@bp.route("/areas/")
def areas_index():
    """Add a new area scrape task and start running it after 10 seconds."""
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
    task = scrape_areas.apply_async(args=[election_id], eta=eta)
    #print(task.get(on_message=on_raw_message, propagate=False))
    return (
        jsonify(
            #{"message": task.get(on_message=on_raw_message, propagate=False)}
            #json.loads(task.get(on_message=on_raw_message, propagate=False))
            json.loads(task.get(propagate=False))
        ),
        202,
    )
