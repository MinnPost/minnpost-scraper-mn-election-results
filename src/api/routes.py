import ciso8601
from datetime import datetime
from flask import jsonify, request, Response, current_app
from sqlalchemy import text
from sqlalchemy import exc
from sqlalchemy import any_
from src.extensions import db
from src.storage import Storage
from src.models import Area, Contest, Election, Question, Result, ScraperModel
from src.api import bp
from src.api.errors import bad_request

@bp.route('/query/', methods=['GET', 'POST'])
def query():
    request.args = request.args.to_dict()
    request.args["display_cache_data"] = "false"
    storage        = Storage(request.args)

    scraper_model     = ScraperModel()
    sql = request.args.get('q', None)
    display_cache_data = request.args.get('display_cache_data', None)
    callback = request.args.get('callback')

    election_key = request.args.get('election_key', None)
    election     = scraper_model.set_election(election_key)
    election_id  = election.id

    sql = scraper_model.format_sql(sql, election_id)
    if sql == "":
        example_query = 'SELECT * FROM contests WHERE title LIKE \'%governor%\'';
        return 'Welcome to the election scraper server. Use a URL like: <a href="/query/?q=%s">/query/?q=%s</a>' % (example_query, example_query);

    # check for cached data and set the output, if it exists
    cache_list_key = current_app.config['QUERY_LIST_CACHE_KEY']
    cache_key = sql
    cached_output = storage.get(cache_key)
    if cached_output is not None:
        current_app.log.info('found cached result for key: %s' % cache_key)
        output = cached_output
    else:
        current_app.log.info('did not find cached result for key: %s' % cache_list_key)
        # run the query
        try:
            query_result = db.session.execute(sql)
        except exc.SQLAlchemyError:
            query_result = {}
            pass
        
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
            output["generated"] = datetime.now()

        output = storage.save(cache_key, output, cache_list_key)
    
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
        request_json     = request.get_json()
        area_id          = request_json.get('area_id')
        areas_group      = request_json.get('areas_group')
        election_key     = request_json.get('election_key')
    elif request.method == 'POST':
        # form request
        area_id      = request.form.get('area_id', None)
        areas_group  = request.form.get('areas_group', None)
        election_key = request.form.get('election_key', None)
    else:
        # GET request
        area_id      = request.values.get('area_id', None)
        areas_group  = request.values.get('areas_group', None)
        election_key = request.values.get('election_key', None)

    # set cache key
    if area_id is not None:
        cache_key_name  = "area_id"
    elif areas_group is not None:
        cache_key_name  = "areas_group"
    else:
        cache_key_name = "all_areas"

    # add election to cache key, even if it's None
    election       = area_model.set_election(election_key)
    election_id    = election.id
    cache_key_name = cache_key_name + "-election-" + election_id

    # check for cached data and set the output, if it exists
    cached_output = storage.get(cache_key_name)
    if cached_output is not None:
        output = cached_output
    else:
        # run the queries
        if area_id is not None:
            try:
                query_result = Area.query.filter_by(id=area_id, election_id=election_id).all()
            except exc.SQLAlchemyError:
                pass
        elif areas_group is not None:
            try:
                query_result = Area.query.filter_by(areas_group=areas_group, election_id=election_id).all()
            except exc.SQLAlchemyError:
                pass
        else:
            try:
                query_result = Area.query.filter_by(election_id=election_id).all()
            except exc.SQLAlchemyError:
                pass

        # set the cache and the output from the query result
        output = area_model.output_for_cache(query_result, request.args)
        output = storage.save(cache_key_name, output, class_name)
    
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
        request_json     = request.get_json()
        title            = request_json.get('title')
        contest_id       = request_json.get('contest_id')
        contest_ids      = request_json.get('contest_ids')
        election_key     = request_json.get('election_key')
    elif request.method == 'POST':
        # form request
        title        = request.form.get('title', None)
        contest_id   = request.form.get('contest_id', None)
        contest_ids  = request.form.get('contest_ids', [])
        election_key = request.form.get('election_key', None)
    else:
        # GET request
        title        = request.values.get('title', None)
        contest_id   = request.values.get('contest_id', None)
        contest_ids  = request.values.get('contest_ids', [])
        election_key = request.values.get('election_key', None)

    # if the contest_ids value is provided on the url, it'll be a string and we need to make it a list
    if isinstance(contest_ids, str):
        contest_ids = contest_ids.split(',')

    # set cache key
    if contest_id is not None:
        cache_key_name  = "contest_id"
    elif title is not None:
        cache_key_name  = "title"
        search = "%{}%".format(title)
    elif len(contest_ids):
        cache_key_name  = "contest_ids"
    else:
        cache_key_name = "all_contests"

    # add election to cache key, even if it's None
    election       = contest_model.set_election(election_key)
    election_id    = election.id
    cache_key_name = cache_key_name + "-election-" + election_id
    
    # check for cached data and set the output, if it exists
    cached_output = storage.get(cache_key_name)
    if cached_output is not None:
        output = cached_output
    else:
        # run the queries
        if contest_id is not None:
            try:
                query_result = Contest.query.join(Result.contests).filter_by(id=contest_id, election_id=election_id).all()
            except exc.SQLAlchemyError:
                pass
        elif title is not None:
            try:
                query_result = Contest.query.join(Result.contests).filter(Contest.title.ilike(search), Contest.election_id == election_id).all()
            except exc.SQLAlchemyError:
                pass
        elif len(contest_ids):
            try:
                query_result = Contest.query.join(Result.contests).filter(Contest.id.ilike(any_(contest_ids)), Contest.election_id == election_id).all()
            except exc.SQLAlchemyError:
                pass
        else:
            try:
                query_result = Contest.query.filter_by(election_id=election_id).all()
            except exc.SQLAlchemyError:
                pass
        
        # set the cache and the output from the query result
        output = contest_model.output_for_cache(query_result, request.args)
        output = storage.save(cache_key_name, output, class_name)

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
    elif request.method == 'POST':
        # form request
        election_id   = request.form.get('election_id', None)
        election_date = request.form.get('election_date', None)
    else:
        # GET request
        election_id   = request.values.get('election_id', None)
        election_date = request.values.get('election_date', None)

    # set cache key
    if election_id is not None:
        cache_key_name  = "election_id"
    elif election_date is not None:
        cache_key_name  = "election_date"
    else:
        cache_key_name = "all_elections"

    # check for cached data and set the output, if it exists
    cached_output = storage.get(cache_key_name)
    if cached_output is not None:
        output = cached_output
    else:
        # run the queries
        if election_id is not None:
            try:
                query_result = Election.query.filter_by(id=election_id).all()
            except exc.SQLAlchemyError:
                pass
        elif election_date is not None:
            try:
                query_result = Election.query.filter_by(election_date=election_date).all()
            except exc.SQLAlchemyError:
                pass
        else:
            try:
                query_result = Election.query.order_by(Election.election_datetime.desc()).all()
            except exc.SQLAlchemyError:
                pass

        # set the cache and the output from the query result
        output = election_model.output_for_cache(query_result, request.args, returning_single_row)
        output = storage.save(cache_key_name, output, class_name)

    # set up the response and return it
    mime = 'application/json'
    ctype = 'application/json; charset=UTF-8'

    res = Response(response = output, status = 200, mimetype = mime)
    res.headers['Content-Type'] = ctype
    res.headers['Connection'] = 'keep-alive'
    res.headers.add("Access-Control-Allow-Origin", "*")
    return res


@bp.route('/meta/', methods=['GET'])
def meta():
    request.args = request.args.to_dict()
    request.args["display_cache_data"] = "true"
    election_model        = Election()
    storage              = Storage(request.args)
    class_name           = Election.get_classname()
    query_result         = None
    returning_single_row = False
    if request.is_json:
        # JSON request
        request_json  = request.get_json()
        election_id   = request_json.get('election_id')
        election_date = request_json.get('election_date')
        key           = request_json.get('key', None)
    elif request.method == 'POST':
        # form request
        election_id = request.form.get('election_id', None)
        election_date = request.form.get('election_date', None)
        key           = request.form.get('key', None)
    else:
        # GET request
        election_id = request.values.get('election_id', None)
        election_date = request.values.get('election_date', None)
        key           = request.values.get('key', None)

    # set cache key
    if election_id is not None:
        cache_key_name  = "election_id"
    elif election_date is not None:
        cache_key_name  = "election_date"
    elif key is not None:
        cache_key_name  = "election_meta_key"
    else:
        cache_key_name = "all_elections"

    # check for cached data and set the output, if it exists
    cached_output = storage.get(cache_key_name)
    if cached_output is not None:
        output = cached_output
    else:
        # run the queries
        if election_id is not None:
            try:
                query_result = Election.query.filter_by(id=election_id).all()
            except exc.SQLAlchemyError:
                pass
        elif election_date is not None:
            try:
                query_result = Election.query.filter_by(election_date=election_date).all()
            except exc.SQLAlchemyError:
                pass
        elif key is not None:
            try:
                query_result = Election.query.order_by(Election.election_datetime.desc()).first()
                returning_single_row = True
            except exc.SQLAlchemyError:
                pass
        else:
            try:
                query_result = Election.query.order_by(Election.election_datetime.desc()).all()
            except exc.SQLAlchemyError:
                pass

        # set the cache and the output from the query result
        output = election_model.output_for_cache(query_result, request.args, returning_single_row)
        output = storage.save(cache_key_name, output, class_name)
    
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
        election_key = request_json.get('election_key')
    elif request.method == 'POST':
        # form request
        question_id = request.form.get('question_id', None)
        contest_id = request.form.get('contest_id', None)
        election_key = request.form.get('election_key', None)
    else:
        # GET request
        question_id = request.values.get('question_id', None)
        contest_id = request.values.get('contest_id', None)
        election_key = request.values.get('election_key', None)

    # set cache key
    if question_id is not None:
        cache_key_name  = "question_id"
    elif contest_id is not None:
        cache_key_name  = "contest_id"
    else:
        cache_key_name = "all_questions"

    # add election to cache key, even if it's None
    election       = question_model.set_election(election_key)
    election_id    = election.id
    cache_key_name = cache_key_name + "-election-" + election_id

    # check for cached data and set the output, if it exists
    cached_output = storage.get(cache_key_name)
    if cached_output is not None:
        output = cached_output
    else:
        # run the queries
        if question_id is not None:
            try:
                query_result = Question.query.filter_by(id=question_id, election_id=election_id).all()
            except exc.SQLAlchemyError:
                pass
        elif contest_id is not None:
            try:
                query_result = Question.query.filter_by(contest_id=contest_id, election_id=election_id).all()
            except exc.SQLAlchemyError:
                pass
        else:
            try:
                query_result = Question.query.filter_by(election_id=election_id).all()
            except exc.SQLAlchemyError:
                pass

        # set the cache and the output from the query result
        output = question_model.output_for_cache(query_result, request.args)
        output = storage.save(cache_key_name, output, class_name)

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
        election_key = request_json.get('election_key')
    elif request.method == 'POST':
        # form request
        result_id    = request.form.get('result_id', None)
        contest_id   = request.form.get('contest_id', None)
        election_key = request.form.get('election_key', None)
    else:
        # GET request
        result_id    = request.values.get('result_id', None)
        contest_id   = request.values.get('contest_id', None)
        election_key = request.values.get('election_key', None)

    # set cache key
    if result_id is not None:
        cache_key_name  = "result_id"
    elif contest_id is not None:
        cache_key_name  = "contest_id"
    else:
        cache_key_name = "all_results"

    # add election to cache key, even if it's None
    election       = result_model.set_election(election_key)
    election_id    = election.id
    cache_key_name = cache_key_name + "-election-" + election_id

    # check for cached data and set the output, if it exists
    cached_output = storage.get(cache_key_name)
    if cached_output is not None:
        output = cached_output
    else:
        # run the queries
        if result_id is not None:
            try:
                query_result = Result.query.filter_by(id=result_id, election_id=election_id).all()
            except exc.SQLAlchemyError:
                pass
        elif contest_id is not None:
            try:
                query_result = Result.query.filter_by(contest_id=contest_id, election_id=election_id).all()
            except exc.SQLAlchemyError:
                pass
        else:
            try:
                query_result = Result.query.filter_by(election_id=election_id).all()
            except exc.SQLAlchemyError:
                pass
        
        # set the cache and the output from the query result
        output = result_model.output_for_cache(query_result, request.args)
        output = storage.save(cache_key_name, output, class_name)

    # set up the response and return it
    mime = 'application/json'
    ctype = 'application/json; charset=UTF-8'

    res = Response(response = output, status = 200, mimetype = mime)
    res.headers['Content-Type'] = ctype
    res.headers['Connection'] = 'keep-alive'
    res.headers.add("Access-Control-Allow-Origin", "*")
    return res
