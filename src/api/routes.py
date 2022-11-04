import ciso8601
import pytz
from datetime import datetime
from flask import request, Response, current_app
from sqlalchemy import text
from sqlalchemy import exc
from sqlalchemy import any_
from sqlalchemy.orm import contains_eager
from natsort import natsorted
from src.extensions import db
from src.storage import Storage
from src.models import Area, Contest, Election, Question, Result, ScraperModel
from src.api import bp
from src.api.errors import bad_request

@bp.route('/query/', methods=['GET', 'POST'])
def query():
    request.args = request.args.to_dict()
    request.args["display_cache_data"] = "false"
    storage       = Storage(request.args)
    output        = None
    example_query = 'SELECT * FROM contests WHERE title LIKE \'%governor%\'';

    scraper_model     = ScraperModel()
    sql = request.args.get('q', None)
    display_cache_data = request.args.get('display_cache_data', None)
    callback = request.args.get('callback')

    election_id = request.args.get('election_id', None)
    election    = scraper_model.set_election(election_id)

    if election is None:
        return 'Welcome to the election scraper server. Use a URL like: <a href="/query/?q=%s">/query/?q=%s</a>' % (example_query, example_query);

    # set up sql query as cache list key
    cache_list_key = current_app.config['QUERY_LIST_CACHE_KEY']
    cache_key = sql

    # the meta query for the old dashboard. ideally we will remove this.
    if sql.lower() == "select * from meta":
        election_model = Election()
        # check for cached data and set the output, if it exists
        cached_output = storage.get(cache_key)
        if cached_output is not None:
            #current_app.log.debug('found cached result for legacy meta key: %s' % cache_key)
            output = cached_output
        else:
            #current_app.log.debug('did not find cached result for legacy meta key: %s' % cache_list_key)
            output = election_model.legacy_meta_output(election.id)
            output = storage.save(cache_key, output, cache_list_key, election)

    # verify/format the sql. use it for the cache key value.
    sql = scraper_model.format_sql(sql, election.id)
    cache_key = sql
    if sql == "":
        return 'Welcome to the election scraper server. Use a URL like: <a href="/query/?q=%s">/query/?q=%s</a>' % (example_query, example_query);

    if output == None:
        # check for cached data and set the output, if it exists
        cached_output = storage.get(cache_key)
        if cached_output is not None:
            #current_app.log.debug('found cached result for key: %s' % cache_key)
            output = cached_output
        else:
            #current_app.log.debug('did not find cached result for key: %s' % cache_key)
            # run the query
            try:
                query_result = db.session.execute(sql)
            except exc.SQLAlchemyError:
                query_result = {}
                pass
            
            query_result = natsorted(query_result)
            # set the cache and the output from the query result
            output = {}
            if display_cache_data == "true":
                output["data"] = {}
            for count, row in enumerate(query_result):
                d = dict(row)
                if 'updated' in d:
                    if not isinstance(d['updated'], int):
                        d['updated'] = datetime.timestamp(d['updated'])
                if 'key' in d and d['key'] == 'updated' and d['type'] == 'int':
                    if not isinstance(d['value'], int):
                        date_object = ciso8601.parse_datetime(d['value'])
                        d['value'] = datetime.timestamp(date_object)
                if 'value' in d:
                    if d['value'] == "true":
                        d['value'] = True
                    elif d['value'] == "false":
                        d['value'] = False
                if display_cache_data == "true":
                    output["data"][count] = d
                else:
                    output[count] = d
            if display_cache_data == "true":
                output["generated"] = datetime.now(pytz.timezone(current_app.config["TIMEZONE"]))

            output = storage.save(cache_key, output, cache_list_key, election)
    
    # set up the response and return it
    mime = 'application/json'
    ctype = 'application/json; charset=UTF-8'

    if callback is not None:
        output = '%s(%s);' % (callback, output)
        mime = 'text/javascript'
        ctype = 'text/javascript; charset=UTF-8'

    #response = 

    res = Response(response = output, status = 200, mimetype = mime)
    res.headers['Content-Type'] = ctype
    res.headers['Connection'] = 'keep-alive'
    res.headers.add("Access-Control-Allow-Origin", "*")
    return res


@bp.route('/areas/', methods=['GET', 'POST'])
def areas():
    request.args = request.args.to_dict()
    request.args["display_cache_data"] = "true"
    area_model     = Area()
    storage        = Storage(request.args)
    class_name     = Area.get_classname()
    query_result   = None
    if request.is_json:
        # JSON request
        request_json = request.get_json()
        area_id      = request_json.get('area_id')
        areas_group  = request_json.get('areas_group')
        county_name  = request_json.get('county_name')
        election_id  = request_json.get('election_id')
        limit        = request_json.get('limit')
        offset       = request_json.get('offset')
    elif request.method == 'POST':
        # form request
        area_id     = request.form.get('area_id', None)
        areas_group = request.form.get('areas_group', None)
        county_name = request.form.get('county_name', None)
        election_id = request.form.get('election_id', None)
        limit       = request.form.get('limit', None)
        offset      = request.form.get('offset', None)
    else:
        # GET request
        area_id     = request.values.get('area_id', None)
        areas_group = request.values.get('areas_group', None)
        county_name = request.values.get('county_name', None)
        election_id = request.values.get('election_id', None)
        limit       = request.values.get('limit', None)
        offset      = request.values.get('offset', None)

    # set cache key
    if area_id is not None:
        cache_key_name  = "area_id"
        cache_key_value = area_id
    elif areas_group is not None:
        cache_key_name  = "areas_group"
        cache_key_value = areas_group
    elif county_name is not None:
        cache_key_name  = "county_name"
        cache_key_value = county_name
    else:
        cache_key_name = "all_areas"
        cache_key_value = ""

    # add election to cache key, even if it's None
    election = area_model.set_election(election_id)
    if election is None:
        return
    cache_key_name = cache_key_name + "-" + cache_key_value + "-election-" + election.id

    if limit is not None:
        cache_key_name += "limit-" + limit
    if offset is not None:
        cache_key_name += "offset-" + offset

    # check for cached data and set the output, if it exists
    cached_output = storage.get(cache_key_name)
    if cached_output is not None:
        output = cached_output
    else:
        # run the queries
        if area_id is not None:
            try:
                query_result = Area.query.filter_by(id=area_id, election_id=election.id).offset(offset).limit(limit).all()
            except exc.SQLAlchemyError:
                pass
        elif areas_group is not None:
            try:
                query_result = Area.query.filter_by(areas_group=areas_group, election_id=election.id).offset(offset).limit(limit).all()
            except exc.SQLAlchemyError:
                pass
        elif county_name is not None:
            try:
                query_result = Area.query.filter_by(county_name=county_name, election_id=election.id).offset(offset).limit(limit).all()
            except exc.SQLAlchemyError:
                pass
        else:
            try:
                query_result = Area.query.filter_by(election_id=election.id).offset(offset).limit(limit).all()
            except exc.SQLAlchemyError:
                pass

        query_result = natsorted(query_result, key=str)
        # set the cache and the output from the query result
        output = area_model.output_for_cache(query_result, request.args)
        output = storage.save(cache_key_name, output, class_name, election)
    
    # set up the response and return it
    mime = 'application/json'
    ctype = 'application/json; charset=UTF-8'

    res = Response(response = output, status = 200, mimetype = mime)
    res.headers['Content-Type'] = ctype
    res.headers['Connection'] = 'keep-alive'
    res.headers.add("Access-Control-Allow-Origin", "*")
    return res


@bp.route('/boundaries/', methods=['GET', 'POST'])
def boundaries():
    request.args = request.args.to_dict()
    request.args["display_cache_data"] = "true"
    contest_model  = Contest()
    storage        = Storage(request.args)
    class_name     = Contest.get_classname()
    query_result   = None
    if request.is_json:
        # JSON request
        request_json = request.get_json()
        title        = request_json.get('title')
        contest_id   = request_json.get('contest_id')
        contest_ids  = request_json.get('contest_ids')
        election_id  = request_json.get('election_id')
        limit        = request_json.get('limit')
        offset       = request_json.get('offset')
    elif request.method == 'POST':
        # form request
        title       = request.form.get('title', None)
        contest_id  = request.form.get('contest_id', None)
        contest_ids = request.form.get('contest_ids', [])
        election_id = request.form.get('election_id', None)
        limit       = request.form.get('limit', None)
        offset      = request.form.get('offset', None)
    else:
        # GET request
        title       = request.values.get('title', None)
        contest_id  = request.values.get('contest_id', None)
        contest_ids = request.values.get('contest_ids', [])
        election_id = request.values.get('election_id', None)
        limit       = request.values.get('limit', None)
        offset      = request.values.get('offset', None)

    # if the contest_ids value is provided on the url, it'll be a string and we need to make it a list
    if isinstance(contest_ids, str):
        contest_ids = contest_ids.split(',')

    # set cache key
    if contest_id is not None:
        cache_key_name  = "contest_id"
        cache_key_value = contest_id
    elif title is not None:
        cache_key_name  = "title"
        search = "%{}%".format(title)
        cache_key_value = title
    elif len(contest_ids):
        cache_key_name  = "contest_ids"
        cache_key_value = ','.join(contest_ids)
    else:
        cache_key_name = "all_contests"
        cache_key_value = ""

    # add election to cache key, even if it's None
    election = contest_model.set_election(election_id)
    if election is None:
        return
    cache_key_name = cache_key_name + "-" + cache_key_value + "-election-" + election.id

    if limit is not None:
        cache_key_name += "limit-" + limit
    if offset is not None:
        cache_key_name += "offset-" + offset
    
    # check for cached data and set the output, if it exists
    #cached_output = storage.get(cache_key_name)
    #if cached_output is not None:
    #    output = cached_output
    #else:
    # run the queries
    if contest_id is not None:
        try:
            query_result = Contest.query.filter(Contest.id == contest_id, Contest.election_id == election.id, Contest.boundary.isnot(None)).offset(offset).limit(limit).all()
        except exc.SQLAlchemyError:
            pass
    elif title is not None:
        try:
            query_result = Contest.query.filter(Contest.title.ilike(search), Contest.election_id == election.id, Contest.boundary.isnot(None)).offset(offset).limit(limit).all()
        except exc.SQLAlchemyError:
            pass
    elif len(contest_ids):
        try:
            query_result = Contest.query.filter(Contest.id.ilike(any_(contest_ids)), Contest.election_id == election.id, Contest.boundary.isnot(None)).offset(offset).limit(limit).all()
        except exc.SQLAlchemyError:
            pass
    else:
        try:
            query_result = Contest.query.filter(Contest.election_id == election.id, Contest.boundary.isnot(None)).offset(offset).limit(limit).all()
        except exc.SQLAlchemyError:
            pass

    query_result = natsorted(query_result, key=lambda x: x.title)
    # set the cache and the output from the query result
    #output = contest_model.output_for_cache(query_result, request.args)
    #output = storage.save(cache_key_name, output, class_name, election)
    output = contest_model.check_boundary(query_result)

    # set up the response and return it
    mime = 'application/json'
    ctype = 'application/json; charset=UTF-8'

    res = Response(response = output, status = 200, mimetype = mime)
    res.headers['Content-Type'] = ctype
    res.headers['Connection'] = 'keep-alive'
    res.headers.add("Access-Control-Allow-Origin", "*")
    return res


@bp.route('/contests/', methods=['GET', 'POST'])
def contests():
    request.args = request.args.to_dict()
    request.args["display_cache_data"] = "true"
    contest_model  = Contest()
    storage        = Storage(request.args)
    class_name     = Contest.get_classname()
    query_result   = None
    if request.is_json:
        # JSON request
        request_json  = request.get_json()
        title         = request_json.get('title')
        scope         = request_json.get('scope')
        results_group = request_json.get('results_group')
        contest_id    = request_json.get('contest_id')
        contest_ids   = request_json.get('contest_ids')
        election_id   = request_json.get('election_id')
        address       = request_json.get('address')
        coordinates   = request_json.get('coordinates')
        limit         = request_json.get('limit')
        offset        = request_json.get('offset')
    elif request.method == 'POST':
        # form request
        title         = request.form.get('title', None)
        scope         = request.form.get('scope', None)
        results_group = request.form.get('results_group', None)
        contest_id    = request.form.get('contest_id', None)
        contest_ids   = request.form.get('contest_ids', [])
        election_id   = request.form.get('election_id', None)
        address       = request.form.get('address', None)
        coordinates   = request.form.get('coordinates', None)
        limit         = request.form.get('limit', None)
        offset        = request.form.get('offset', None)
    else:
        # GET request
        title         = request.values.get('title', None)
        scope         = request.values.get('scope', None)
        results_group = request.values.get('results_group', None)
        contest_id    = request.values.get('contest_id', None)
        contest_ids   = request.values.get('contest_ids', [])
        election_id   = request.values.get('election_id', None)
        address       = request.values.get('address', None)
        coordinates   = request.values.get('coordinates', None)
        limit         = request.values.get('limit', None)
        offset        = request.values.get('offset', None)

    # if the contest_ids value is provided on the url, it'll be a string and we need to make it a list
    if isinstance(contest_ids, str):
        contest_ids = contest_ids.split(',')

    # set cache key
    if contest_id is not None:
        cache_key_name  = "contest_id"
        cache_key_value = contest_id
    elif title is not None:
        cache_key_name  = "title"
        search = "%{}%".format(title)
        cache_key_value = title
    elif scope is not None:
        cache_key_name  = "scope"
        cache_key_value = scope
    elif results_group is not None:
        cache_key_name  = "results_group"
        cache_key_value = results_group
    elif len(contest_ids):
        cache_key_name  = "contest_ids"
        cache_key_value = ','.join(contest_ids)
    elif address is not None:
        cache_key_name  = "boundary"
        boundaries = contest_model.address_to_boundaries(address)
        cache_key_value = ','.join(boundaries)
    elif coordinates is not None:
        cache_key_name    = "coordinates"
        coordinates_array = coordinates.split(',')
        boundaries        = contest_model.latitude_longitude_to_boundaries(coordinates_array[0], coordinates_array[1])
        cache_key_value   = ','.join(boundaries)
    else:
        cache_key_name = "all_contests"
        cache_key_value = ""

    # add election to cache key, even if it's None
    election = contest_model.set_election(election_id)
    if election is None:
        return
    cache_key_name = cache_key_name + "-" + cache_key_value + "-election-" + election.id

    if limit is not None:
        cache_key_name += "limit-" + limit
    if offset is not None:
        cache_key_name += "offset-" + offset
    
    # check for cached data and set the output, if it exists
    cached_output = storage.get(cache_key_name)
    if cached_output is not None:
        output = cached_output
    else:
        order_naturally = True
        # run the queries
        if contest_id is not None:
            order_naturally = False
            try:
                query_result = Contest.query.filter_by(id=contest_id, election_id=election.id).offset(offset).limit(limit).all()
            except exc.SQLAlchemyError:
                pass
        elif title is not None:
            try:
                query_result = Contest.query.filter(Contest.title.ilike(search), Contest.election_id == election.id).offset(offset).limit(limit).all()
            except exc.SQLAlchemyError:
                pass
        elif scope is not None:
            try:
                query_result = Contest.query.filter_by(scope=scope, election_id= election.id).offset(offset).limit(limit).all()
            except exc.SQLAlchemyError:
                pass
        elif results_group is not None:
            try:
                query_result = Contest.query.filter_by(results_group=results_group, election_id= election.id).offset(offset).limit(limit).all()
            except exc.SQLAlchemyError:
                pass
        elif len(contest_ids):
            order_naturally = False
            try:
                query_result = Contest.query.filter(Contest.id.ilike(any_(contest_ids)), Contest.election_id == election.id).offset(offset).limit(limit).all()
            except exc.SQLAlchemyError:
                pass
            if query_result is not None:
                contest_map = {t.id: t for t in query_result}
                query_result = [contest_map[n] for n in contest_ids]
        elif address is not None and len(boundaries):
            try:
                query_result = Contest.query.filter(Contest.boundary.ilike(any_(boundaries)), Contest.election_id == election.id).offset(offset).limit(limit).all()
            except exc.SQLAlchemyError:
                pass
        elif coordinates is not None and len(boundaries):
            try:
                query_result = Contest.query.filter(Contest.boundary.ilike(any_(boundaries)), Contest.election_id == election.id).offset(offset).limit(limit).all()
            except exc.SQLAlchemyError:
                pass
        else:
            try:
                query_result = Contest.query.filter_by(election_id=election.id).offset(offset).limit(limit).all()
            except exc.SQLAlchemyError:
                pass
        if query_result is not None and order_naturally is True:
            query_result = natsorted(query_result, key=lambda x: x.title)
        # set the cache and the output from the query result
        output = contest_model.output_for_cache(query_result, request.args)
        output = storage.save(cache_key_name, output, class_name, election)

    # set up the response and return it
    mime = 'application/json'
    ctype = 'application/json; charset=UTF-8'

    res = Response(response = output, status = 200, mimetype = mime)
    res.headers['Content-Type'] = ctype
    res.headers['Connection'] = 'keep-alive'
    res.headers.add("Access-Control-Allow-Origin", "*")
    return res


@bp.route('/contests-with-results/', methods=['GET', 'POST'])
def contests_with_results():
    request.args = request.args.to_dict()
    request.args["display_cache_data"] = "true"
    contest_model  = Contest()
    storage        = Storage(request.args)
    class_name     = Contest.get_classname()
    query_result   = None
    if request.is_json:
        # JSON request
        request_json  = request.get_json()
        title         = request_json.get('title')
        scope         = request_json.get('scope')
        results_group = request_json.get('results_group')
        contest_id    = request_json.get('contest_id')
        contest_ids   = request_json.get('contest_ids')
        election_id   = request_json.get('election_id')
        address       = request_json.get('address')
        coordinates   = request_json.get('coordinates')
        limit         = request_json.get('limit')
        offset        = request_json.get('offset')
    elif request.method == 'POST':
        # form request
        title         = request.form.get('title', None)
        scope         = request.form.get('scope', None)
        results_group = request.form.get('results_group', None)
        contest_id    = request.form.get('contest_id', None)
        contest_ids   = request.form.get('contest_ids', [])
        election_id   = request.form.get('election_id', None)
        address       = request.form.get('address', None)
        coordinates   = request.form.get('coordinates', None)
        limit         = request.form.get('limit', None)
        offset        = request.form.get('offset', None)
    else:
        # GET request
        title         = request.values.get('title', None)
        scope         = request.values.get('scope', None)
        results_group = request.values.get('results_group', None)
        contest_id    = request.values.get('contest_id', None)
        contest_ids   = request.values.get('contest_ids', [])
        election_id   = request.values.get('election_id', None)
        address       = request.values.get('address', None)
        coordinates   = request.values.get('coordinates', None)
        limit         = request.values.get('limit', None)
        offset        = request.values.get('offset', None)

    # if the contest_ids value is provided on the url, it'll be a string and we need to make it a list
    if isinstance(contest_ids, str):
        contest_ids = contest_ids.split(',')

    # set cache key
    if contest_id is not None:
        cache_key_name  = "contest_id"
        cache_key_value = contest_id
    elif title is not None:
        cache_key_name  = "title"
        search = "%{}%".format(title)
        cache_key_value = title
    elif scope is not None:
        cache_key_name  = "scope"
        cache_key_value = scope
    elif results_group is not None:
        cache_key_name  = "results_group"
        cache_key_value = results_group
    elif len(contest_ids):
        cache_key_name  = "contest_ids"
        cache_key_value = ','.join(contest_ids)
    elif address is not None:
        cache_key_name  = "boundary"
        boundaries = contest_model.address_to_boundaries(address)
        cache_key_value = ','.join(boundaries)
    elif coordinates is not None:
        cache_key_name    = "coordinates"
        coordinates_array = coordinates.split(',')
        boundaries        = contest_model.latitude_longitude_to_boundaries(coordinates_array[0], coordinates_array[1])
        cache_key_value   = ','.join(boundaries)
    else:
        cache_key_name = "all_contests"
        cache_key_value = ""

    # add election to cache key, even if it's None
    election = contest_model.set_election(election_id)
    if election is None:
        return
    cache_key_name = cache_key_name + "-" + cache_key_value + "-with-results-election-" + election.id

    if limit is not None:
        cache_key_name += "limit-" + limit
    if offset is not None:
        cache_key_name += "offset-" + offset
    
    # check for cached data and set the output, if it exists
    cached_output = storage.get(cache_key_name)
    if cached_output is not None:
        output = cached_output
    else:
        order_naturally = True
        # run the queries
        if contest_id is not None:
            try:
                order_naturally = False
                query_result = Contest.query.join(Result, Contest.results).filter(Contest.id == contest_id, Contest.election_id == election.id, Result.election_id == election.id).options(contains_eager(Contest.results)).offset(offset).limit(limit).all()
            except exc.SQLAlchemyError:
                pass
        elif title is not None:
            try:
                query_result = Contest.query.join(Result, Contest.results).filter(Contest.title.ilike(search), Contest.election_id == election.id, Result.election_id == election.id).options(contains_eager(Contest.results)).offset(offset).limit(limit).all()
            except exc.SQLAlchemyError:
                pass
        elif scope is not None:
            try:
                query_result = Contest.query.join(Result, Contest.results).filter(Contest.scope == scope, Contest.election_id == election.id, Result.election_id == election.id).options(contains_eager(Contest.results)).offset(offset).limit(limit).all()
            except exc.SQLAlchemyError:
                pass
        elif results_group is not None:
            try:
                query_result = Contest.query.join(Result, Contest.results).filter(Contest.results_group == results_group, Contest.election_id == election.id, Result.election_id == election.id).options(contains_eager(Contest.results)).offset(offset).limit(limit).all()
            except exc.SQLAlchemyError:
                pass
        elif len(contest_ids):
            order_naturally = False
            try:
                query_result = Contest.query.join(Result, Contest.results).filter(Contest.id.ilike(any_(contest_ids)), Contest.election_id == election.id, Result.election_id == election.id).options(contains_eager(Contest.results)).offset(offset).limit(limit).all()
            except exc.SQLAlchemyError:
                pass
            if query_result is not None:
                contest_map = {t.id: t for t in query_result}
                query_result = [contest_map[n] for n in contest_ids]
        elif address is not None and len(boundaries):
            try:
                query_result = Contest.query.join(Result, Contest.results).filter(Contest.boundary.ilike(any_(boundaries)), Contest.election_id == election.id, Result.election_id == election.id).options(contains_eager(Contest.results)).offset(offset).limit(limit).all()
            except exc.SQLAlchemyError:
                pass
        elif coordinates is not None and len(boundaries):
            try:
                query_result = Contest.query.join(Result, Contest.results).filter(Contest.boundary.ilike(any_(boundaries)), Contest.election_id == election.id, Result.election_id == election.id).options(contains_eager(Contest.results)).offset(offset).limit(limit).all()
            except exc.SQLAlchemyError:
                pass
        else:
            try:
                query_result = Contest.query.join(Result, Contest.results).filter(Contest.election_id == election.id, Result.election_id == election.id).options(contains_eager(Contest.results)).offset(offset).limit(limit).all()
            except exc.SQLAlchemyError:
                pass
        if query_result is not None and order_naturally is True:
            query_result = natsorted(query_result, key=lambda x: x.title)
        # set the cache and the output from the query result
        output = contest_model.output_for_cache(query_result, request.args, False, 'results')
        output = storage.save(cache_key_name, output, class_name, election)

    # set up the response and return it
    mime = 'application/json'
    ctype = 'application/json; charset=UTF-8'

    res = Response(response = output, status = 200, mimetype = mime)
    res.headers['Content-Type'] = ctype
    res.headers['Connection'] = 'keep-alive'
    res.headers.add("Access-Control-Allow-Origin", "*")
    return res


@bp.route('/elections/', methods=['GET', 'POST'])
def elections():
    request.args = request.args.to_dict()
    request.args["display_cache_data"] = "true"
    election_model       = Election()
    storage              = Storage(request.args)
    class_name           = Election.get_classname()
    query_result         = None
    returning_single_row = False
    if request.is_json:
        # JSON request
        request_json  = request.get_json()
        election_id   = request_json.get('election_id')
        election_date = request_json.get('election_date')
        limit         = request_json.get('limit')
        offset        = request_json.get('offset')
    elif request.method == 'POST':
        # form request
        election_id   = request.form.get('election_id', None)
        election_date = request.form.get('election_date', None)
        limit         = request.form.get('limit', None)
        offset        = request.form.get('offset', None)
    else:
        # GET request
        election_id   = request.values.get('election_id', None)
        election_date = request.values.get('election_date', None)
        limit         = request.values.get('limit', None)
        offset        = request.values.get('offset', None)

    # set cache key
    if election_id is not None:
        cache_key_name  = "election_id"
        cache_key_value = election_id
    elif election_date is not None:
        cache_key_name  = "election_date"
        cache_key_value = election_date
    else:
        cache_key_name = "all_elections"
        cache_key_value = ""

    cache_key_name = cache_key_name + "-" + cache_key_value

    if limit is not None:
        cache_key_name += "limit-" + limit
    if offset is not None:
        cache_key_name += "offset-" + offset

    # check for cached data and set the output, if it exists
    cached_output = storage.get(cache_key_name)
    if cached_output is not None:
        output = cached_output
    else:
        # run the queries
        if election_id is not None:
            try:
                query_result = Election.query.filter_by(id=election_id).offset(offset).limit(limit).all()
            except exc.SQLAlchemyError:
                pass
        elif election_date is not None:
            try:
                query_result = Election.query.filter_by(date=election_date).offset(offset).limit(limit).all()
            except exc.SQLAlchemyError:
                pass
        else:
            try:
                query_result = Election.query.order_by(Election.election_datetime.desc()).offset(offset).limit(limit).all()
            except exc.SQLAlchemyError:
                pass

        if query_result is not None:
            query_result = natsorted(query_result, key=str)
        # set the cache and the output from the query result
        output = election_model.output_for_cache(query_result, request.args, returning_single_row)
        if len(query_result):
            election = query_result[0]
        else:
            election = cache_key_value
        #current_app.log.info('the scraped value is %s and the generated value is %s.' % (output["data"][0]["scraped"], output["generated"]))
        output = storage.save(cache_key_name, output, class_name, election)

    # set up the response and return it
    mime = 'application/json'
    ctype = 'application/json; charset=UTF-8'

    res = Response(response = output, status = 200, mimetype = mime)
    res.headers['Content-Type'] = ctype
    res.headers['Connection'] = 'keep-alive'
    res.headers.add("Access-Control-Allow-Origin", "*")
    return res


@bp.route('/questions/', methods=['GET', 'POST'])
def questions():
    request.args = request.args.to_dict()
    request.args["display_cache_data"] = "true"
    question_model = Question()
    storage        = Storage(request.args)
    class_name     = Question.get_classname()
    query_result   = None
    if request.is_json:
        # JSON request
        request_json = request.get_json()
        question_id  = request_json.get('question_id')
        contest_id   = request_json.get('contest_id')
        election_id  = request_json.get('election_id')
        limit        = request_json.get('limit')
        offset       = request_json.get('offset')
    elif request.method == 'POST':
        # form request
        question_id = request.form.get('question_id', None)
        contest_id  = request.form.get('contest_id', None)
        election_id = request.form.get('election_id', None)
        limit       = request.form.get('limit', None)
        offset      = request.form.get('offset', None)
    else:
        # GET request
        question_id = request.values.get('question_id', None)
        contest_id  = request.values.get('contest_id', None)
        election_id = request.values.get('election_id', None)
        limit       = request.values.get('limit', None)
        offset      = request.values.get('offset', None)

    # set cache key
    if question_id is not None:
        cache_key_name  = "question_id"
        cache_key_value = question_id
    elif contest_id is not None:
        cache_key_name  = "contest_id"
        cache_key_value = contest_id
    else:
        cache_key_name = "all_questions"
        cache_key_value = ""

    # add election to cache key, even if it's None
    election = question_model.set_election(election_id)
    if election is None:
        return
    cache_key_name = cache_key_name + "-" + cache_key_value + "-election-" + election.id

    if limit is not None:
        cache_key_name += "limit-" + limit
    if offset is not None:
        cache_key_name += "offset-" + offset

    # check for cached data and set the output, if it exists
    cached_output = storage.get(cache_key_name)
    cached_output = None
    if cached_output is not None:
        output = cached_output
    else:
        # run the queries
        if question_id is not None:
            try:
                query_result = Question.query.filter_by(id=question_id, election_id=election.id).offset(offset).limit(limit).all()
            except exc.SQLAlchemyError:
                pass
        elif contest_id is not None:
            try:
                query_result = Question.query.filter_by(contest_id=contest_id, election_id=election.id).offset(offset).limit(limit).all()
            except exc.SQLAlchemyError:
                pass
        else:
            try:
                query_result = Question.query.filter_by(election_id=election.id).offset(offset).limit(limit).all()
            except exc.SQLAlchemyError:
                pass

        query_result = natsorted(query_result, key=lambda x: x.title)
        # set the cache and the output from the query result
        output = question_model.output_for_cache(query_result, request.args)
        output = storage.save(cache_key_name, output, class_name, election)

    # set up the response and return it
    mime = 'application/json'
    ctype = 'application/json; charset=UTF-8'

    res = Response(response = output, status = 200, mimetype = mime)
    res.headers['Content-Type'] = ctype
    res.headers['Connection'] = 'keep-alive'
    res.headers.add("Access-Control-Allow-Origin", "*")
    return res


@bp.route('/results/', methods=['GET', 'POST'])
def results():
    request.args = request.args.to_dict()
    request.args["display_cache_data"] = "true"
    result_model   = Result()
    storage        = Storage(request.args)
    class_name     = Result.get_classname()
    query_result   = None
    if request.is_json:
        # JSON request
        request_json = request.get_json()
        result_id    = request_json.get('result_id')
        contest_id   = request_json.get('contest_id')
        election_id  = request_json.get('election_id')
        limit        = request_json.get('limit')
        offset       = request_json.get('offset')
    elif request.method == 'POST':
        # form request
        result_id   = request.form.get('result_id', None)
        contest_id  = request.form.get('contest_id', None)
        election_id = request.form.get('election_id', None)
        limit       = request.form.get('limit', None)
        offset      = request.form.get('offset', None)
    else:
        # GET request
        result_id   = request.values.get('result_id', None)
        contest_id  = request.values.get('contest_id', None)
        election_id = request.values.get('election_id', None)
        limit       = request.values.get('limit', None)
        offset      = request.values.get('offset', None)

    # set cache key
    if result_id is not None:
        cache_key_name  = "result_id"
        cache_key_value = result_id
    elif contest_id is not None:
        cache_key_name  = "contest_id"
        cache_key_value = contest_id
    else:
        cache_key_name = "all_results"
        cache_key_value = ""

    # add election to cache key, even if it's None
    election = result_model.set_election(election_id)
    if election is None:
        return
    cache_key_name = cache_key_name + "-" + cache_key_value + "-election-" + election.id
    
    if limit is not None:
        cache_key_name += "limit-" + limit
    if offset is not None:
        cache_key_name += "offset-" + offset

    # check for cached data and set the output, if it exists
    cached_output = storage.get(cache_key_name)
    if cached_output is not None:
        output = cached_output
    else:
        # run the queries
        if result_id is not None:
            try:
                query_result = Result.query.filter_by(id=result_id, election_id=election.id).offset(offset).limit(limit).all()
            except exc.SQLAlchemyError:
                pass
        elif contest_id is not None:
            try:
                query_result = Result.query.filter_by(contest_id=contest_id, election_id=election.id).offset(offset).limit(limit).all()
            except exc.SQLAlchemyError:
                pass
        else:
            try:
                query_result = Result.query.filter_by(election_id=election.id).offset(offset).limit(limit).all()
            except exc.SQLAlchemyError:
                pass
        
        query_result = natsorted(query_result, key=lambda x: x.office_name)
        # set the cache and the output from the query result
        output = result_model.output_for_cache(query_result, request.args)
        output = storage.save(cache_key_name, output, class_name, election)

    # set up the response and return it
    mime = 'application/json'
    ctype = 'application/json; charset=UTF-8'

    res = Response(response = output, status = 200, mimetype = mime)
    res.headers['Content-Type'] = ctype
    res.headers['Connection'] = 'keep-alive'
    res.headers.add("Access-Control-Allow-Origin", "*")
    return res
