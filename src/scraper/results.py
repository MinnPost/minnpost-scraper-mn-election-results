from datetime import datetime
from datetime import timedelta
from dateutil import parser
import pytz
from datetimerange import DateTimeRange
from redbeat import RedBeatSchedulerEntry as Entry
from celery.schedules import schedule
from flask import current_app, request, Response
from src.extensions import db
from src.extensions import celery
from src.storage import Storage
from src.models import Result, Contest
from src.scraper import bp
from src.scraper import elections
from celery import chain
from sqlalchemy import exc
from src.electionresultwindow import ElectionResultWindow

@celery.task(bind=True)
def scrape_results(self, election_id = None):
    storage      = Storage()
    result       = Result()
    contest      = Contest()
    class_name   = Result.get_classname()
    election     = result.set_election(election_id)
    if election is None:
        return

    sources      = result.read_sources()
    election_key = result.set_election_key(election.id)
    if election_key not in sources:
        return

    # set up count for results
    inserted_count = 0
    updated_count = 0 # this number is unreliable except for spreadsheet rows
    deleted_count = 0
    parsed_count = 0
    supplemented_count = 0
    group_count = 0

    for group in sources[election_key]:
        source = sources[election_key][group]
        group_count = group_count + 1

        if 'type' in source and source['type'] == 'results':
            # handle parsed results
            parsed_election = result.parse_election(source, election)
            rows = parsed_election['rows']
            updated = parsed_election['updated']
            for row in rows:
                parsed = result.parser(row, group, election.id, updated)
                # handle the contest info related to the results
                parsed_contest_result = contest.parser_results(parsed, row, group, election, source, updated)

                if parsed_contest_result:
                    contest_result = Contest()
                    contest_result.from_dict(parsed_contest_result, new=True)

                    db.session.merge(contest_result)

                result = Result()
                result.from_dict(parsed, new=True)

                db.session.merge(result)
                
                inserted_count = inserted_count + 1
                parsed_count = parsed_count + 1
            # commit parsed rows
            try:
                db.session.commit()
            except exc.IntegrityError as e:
                reason = e.message
                current_app.log.debug(reason)

                if "Duplicate entry" in reason:
                    current_app.log.debug("%s already in results table." % e.params[0])
                    db.session.rollback()

    # Handle post processing actions. this only needs to happen once, not for every group.
    supplemental_contests = contest.post_processing('contests', election.id)
    for supplemental_contest in supplemental_contests:
        rows = supplemental_contest['rows']
        action = supplemental_contest['action']
        if action is not None and rows != []:
            for row in rows:
                if row is not []:
                    if action == 'insert' or action == 'update':
                        db.session.merge(row)
                        if action == 'insert':
                            current_app.log.info('Could not find match for contest from spreadsheet. Trying to create one, which is unexpected. %s' % row)
                    elif action == 'delete':
                        db.session.delete(row)
    supplemental_results = result.post_processing('results', election.id)
    for supplemental_result in supplemental_results:
        rows = supplemental_result['rows']
        action = supplemental_result['action']
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

    # clear both result and contest caches here since we're scraping both
    deleted_cache = {
        "results": storage.clear_group(class_name, election.id),
        "contests": storage.clear_group(class_name, election.id)
    }

    result = {
        "results" : {
            "election_id": election.id,
            "sources": group_count,
            "inserted": inserted_count,
            "updated": updated_count,
            "deleted": deleted_count,
            "parsed": parsed_count,
            "supplemented": supplemented_count,
            "cache": deleted_cache,
            "status": "completed"
        }
    }
    #current_app.log.debug(result)

    entry_key              = 'scrape_results_chain_task'
    prefixed_entry_key     = 'redbeat:' + entry_key
    election_result_window = ElectionResultWindow(election)
    election_result_window = election_result_window.check_election_result_window()
    interval_value         = election_result_window["interval"]
    debug_message          = election_result_window["debug_message"]
    interval               = schedule(run_every=interval_value)

    # create and/or run the result task based on the current interval value.
    try:
        e = Entry.from_key(prefixed_entry_key)
        if e.schedule != interval:
            #debug_message += f" The current schedule is {e.schedule}. Change to {interval}."
            #current_app.log.debug(debug_message)
            #e = Entry(entry_key, 'src.scraper.results.scrape_results', interval, app=celery)
            e = Entry(entry_key, 'src.scraper.results.scrape_results_chain', interval, app=celery)
            e.save()
        #else:
            #debug_message += f" The current schedule is already {interval}."
            #current_app.log.debug(debug_message)
    except Exception as err:
        #debug_message += f" The configured election result task does not exist; create it."
        #current_app.log.debug(debug_message)
        #e = Entry(entry_key, 'src.scraper.results.scrape_results', interval, app=celery)
        e = Entry(entry_key, 'src.scraper.results.scrape_results_chain', interval, app=celery)
        e.save()

    return result


@celery.task(bind=True)
def scrape_results_chain(self, election_id = None):
    eta = datetime.utcnow() + timedelta(seconds=10)
    res = chain(scrape_results.s() | elections.scrape_elections.s()).apply_async(args=[election_id], eta=eta)
    return res


@bp.route("/results/")
def results_index():
    """Add a new result scrape task and start running it after 10 seconds."""
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
    res = chain(scrape_results.s() | elections.scrape_elections.s()).apply_async(args=[election_id], eta=eta)
    output = res.get()

    # set up the response and return it
    mime = 'application/json'
    ctype = 'application/json; charset=UTF-8'

    res = Response(response = output, status = 200, mimetype = mime)
    res.headers['Content-Type'] = ctype
    res.headers['Connection'] = 'keep-alive'
    res.headers.add("Access-Control-Allow-Origin", "*")
    return res
