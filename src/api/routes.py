import ciso8601
import sqlparse
from datetime import datetime
from flask import jsonify, request, Response, current_app
from sqlalchemy import text
from sqlalchemy import exc
from sqlalchemy import any_
from src.extensions import db
from src.storage import Storage
from src.models import Area, Contest, Meta, Question, Result
from src.api import bp
from src.api.errors import bad_request

@bp.route('/query/', methods=['GET', 'POST'])
def query():
    request.args = request.args.to_dict()
    request.args["display_cache_data"] = "false"
    storage        = Storage(request.args)
    sql = request.args.get('q', None)
    display_cache_data = request.args.get('display_cache_data', None)
    parsed = sqlparse.parse(sql)[0]
    callback = request.args.get('callback')
    example_query = 'SELECT * FROM contests WHERE title LIKE \'%governor%\'';
    sqltype = parsed.get_type()
    if sqltype != 'SELECT' or parsed in ['', None]:
        return 'Welcome to the election scraper server. Use a URL like: <a href="/query/?q=%s">/query/?q=%s</a>' % (example_query, example_query);

    # make the query case insensitive
    sql = sql.replace(" LIKE ", " ILIKE ")

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
    elif request.method == 'POST':
        # form request
        area_id = request.form.get('area_id', None)
        areas_group = request.form.get('areas_group', None)
    else:
        # GET request
        area_id = request.values.get('area_id', None)
        areas_group = request.values.get('areas_group', None)

    # set cache key
    if area_id is not None:
        cache_key_name  = "area_id"
    elif areas_group is not None:
        cache_key_name  = "areas_group"
    else:
        cache_key_name = "all_areas"

    # check for cached data and set the output, if it exists
    cached_output = storage.get(cache_key_name)
    if cached_output is not None:
        output = cached_output
    else:
        # run the queries
        if area_id is not None:
            try:
                query_result = Area.query.filter_by(id=area_id).all()
            except exc.SQLAlchemyError:
                pass
        elif areas_group is not None:
            try:
                query_result = Area.query.filter_by(areas_group=areas_group).all()
            except exc.SQLAlchemyError:
                pass
        else:
            try:
                query_result = Area.query.all()
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
    elif request.method == 'POST':
        # form request
        title = request.form.get('title', None)
        contest_id = request.form.get('contest_id', None)
        contest_ids = request.form.get('contest_ids', [])
    else:
        # GET request
        title = request.values.get('title', None)
        contest_id = request.values.get('contest_id', None)
        contest_ids = request.values.get('contest_ids', [])

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
    
    # check for cached data and set the output, if it exists
    cached_output = storage.get(cache_key_name)
    if cached_output is not None:
        output = cached_output
    else:
        # run the queries
        if contest_id is not None:
            try:
                query_result = Contest.query.join(Result.contests).filter_by(id=contest_id).all()
            except exc.SQLAlchemyError:
                pass
        elif title is not None:
            try:
                query_result = Contest.query.join(Result.contests).filter(Contest.title.ilike(search)).all()
            except exc.SQLAlchemyError:
                pass
        elif len(contest_ids):
            try:
                query_result = Contest.query.join(Result.contests).filter(Contest.id.ilike(any_(contest_ids))).all()
            except exc.SQLAlchemyError:
                pass
        else:
            try:
                query_result = Contest.query.all()
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


@bp.route('/meta/', methods=['GET'])
def meta():
    request.args = request.args.to_dict()
    request.args["display_cache_data"] = "true"
    meta_model     = Meta()
    storage        = Storage(request.args)
    class_name     = Meta.get_classname()
    key            = request.values.get('key', None)
    query_result   = None

    # set cache key
    cache_key_name = "all_meta"
    if key is not None:
        cache_key_name = '{}-{}'.format("meta_key", key).lower()

    # check for cached data and set the output, if it exists
    cached_output = storage.get(cache_key_name)
    if cached_output is not None:
        output = cached_output
    else:
        # run the queries
        if key is not None:
            try:
                query_result = Meta.query.filter_by(key=key).all()
            except exc.SQLAlchemyError:
                pass
        else:
            try:
                query_result = Meta.query.all()
            except exc.SQLAlchemyError:
                pass
        # set the cache and the output from the query result
        output = meta_model.output_for_cache(query_result)
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
    elif request.method == 'POST':
        # form request
        question_id = request.form.get('question_id', None)
        contest_id = request.form.get('contest_id', None)
    else:
        # GET request
        question_id = request.values.get('question_id', None)
        contest_id = request.values.get('contest_id', None)

    # set cache key
    if question_id is not None:
        cache_key_name  = "question_id"
    elif contest_id is not None:
        cache_key_name  = "contest_id"
    else:
        cache_key_name = "all_questions"

    # check for cached data and set the output, if it exists
    cached_output = storage.get(cache_key_name)
    if cached_output is not None:
        output = cached_output
    else:
        # run the queries
        if question_id is not None:
            try:
                query_result = Question.query.filter_by(id=question_id).all()
            except exc.SQLAlchemyError:
                pass
        elif contest_id is not None:
            try:
                query_result = Question.query.filter_by(contest_id=contest_id).all()
            except exc.SQLAlchemyError:
                pass
        else:
            try:
                query_result = Question.query.all()
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
        result_id  = request_json.get('result_id')
        contest_id = request_json.get('contest_id')
    elif request.method == 'POST':
        # form request
        result_id  = request.form.get('result_id', None)
        contest_id = request.form.get('contest_id', None)
    else:
        # GET request
        result_id  = request.values.get('result_id', None)
        contest_id = request.values.get('contest_id', None)

    # set cache key
    if result_id is not None:
        cache_key_name  = "result_id"
    elif contest_id is not None:
        cache_key_name  = "contest_id"
    else:
        cache_key_name = "all_results"

    # check for cached data and set the output, if it exists
    cached_output = storage.get(cache_key_name)
    if cached_output is not None:
        output = cached_output
    else:
        # run the queries
        if result_id is not None:
            try:
                query_result = Result.query.filter_by(id=result_id).all()
            except exc.SQLAlchemyError:
                pass
        elif contest_id is not None:
            try:
                query_result = Result.query.filter_by(contest_id=contest_id).all()
            except exc.SQLAlchemyError:
                pass
        else:
            try:
                query_result = Result.query.all()
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
