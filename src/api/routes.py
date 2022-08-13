import json
import hashlib
import ciso8601
import sqlparse
from datetime import datetime
from flask import jsonify, request, Response, current_app
from sqlalchemy import text
from sqlalchemy import exc
from sqlalchemy import any_
from src.extensions import cache, db
from src.storage import Storage
from src.models import Area, Contest, Meta, Question, Result
from src.api import bp
from src.api.errors import bad_request

@bp.route('/query/', methods=['GET', 'POST'])
def query():
    storage        = Storage(request.args)
    sql = request.args.get('q', None)
    display_cache_data = request.args.get('display_cache_data', None)
    parsed = sqlparse.parse(sql)[0]
    callback = request.args.get('callback')
    example_query = 'SELECT * FROM contests WHERE title LIKE \'%governor%\'';
    sqltype = parsed.get_type()
    if sqltype != 'SELECT' or parsed in ['', None]:
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
    return res


@bp.route('/areas/', methods=['GET'])
@cache.cached(timeout=30, query_string=True)
def areas():
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

    data = []
    if area_id is not None:
        try:
            areas = Area.query.filter_by(id=area_id).all()
            if areas is None:
                return data
        except exc.SQLAlchemyError:
            pass
    elif areas_group is not None:
        try:
            areas = Area.query.filter_by(areas_group=areas_group).all()
            if areas is None:
                return data
        except exc.SQLAlchemyError:
            pass
    else:
        try:
            areas = Area.query.all()
            if areas is None:
                return data
        except exc.SQLAlchemyError:
            pass

    data = [Area.row2dict(area) for area in areas]
    output = json.dumps(data)
    mime = 'application/json'
    ctype = 'application/json; charset=UTF-8'

    res = Response(response = output, status = 200, mimetype = mime)
    res.headers['Content-Type'] = ctype
    res.headers['Connection'] = 'keep-alive'
    return res


@bp.route('/contests/', methods=['GET', 'POST'])
def contests():
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
    
    data = []
    cache_key_name   = ""
    cache_key_value  = ""
    if contest_id is not None:
        try:
            cache_key_name  = "contest_id"
            cache_key_value = contest_id
            cache_key = hashlib.md5((cache_key_name + cache_key_value).encode('utf-8')).hexdigest()
            cached_output = cache.get(cache_key)
            if cached_output is not None:
                current_app.log.info('found cached result for key: %s' % cache_key_name)
                contests = cached_output
            else:
                current_app.log.info('did not find cached result for key: %s' % cache_key_name)
                contests = Contest.query.join(Result.contests).filter_by(id=contest_id).all()
                cached_output = cache.set(cache_key, contests)
            if contests is None:
                return data
        except exc.SQLAlchemyError:
            pass
    elif title is not None:
        try:
            search = "%{}%".format(title)
            cache_key_name  = "title"
            cache_key_value = search
            cache_key = hashlib.md5((cache_key_name + cache_key_value).encode('utf-8')).hexdigest()
            cached_output = cache.get(cache_key)
            if cached_output is not None:
                current_app.log.info('found cached result for key: %s' % cache_key_name)
                contests = cached_output
            else:
                current_app.log.info('did not find cached result for key: %s' % cache_key_name)
                contests = Contest.query.join(Result.contests).filter(Contest.title.ilike(search)).all()
                cached_output = cache.set(cache_key, contests)
            if contests is None:
                return data
        except exc.SQLAlchemyError:
            pass
    elif len(contest_ids):
        try:
            cache_key_name  = "contest_ids"
            cache_key_value = contest_ids
            cache_key = hashlib.md5((cache_key_name + str(cache_key_value)).encode('utf-8')).hexdigest()
            cached_output = cache.get(cache_key)
            if cached_output is not None:
                current_app.log.info('found cached result for key: %s' % cache_key_name)
                contests = cached_output
            else:
                current_app.log.info('did not find cached result for key: %s' % cache_key_name)
                contests = Contest.query.join(Result.contests).filter(Contest.id.ilike(any_(contest_ids))).all()
                cached_output = cache.set(cache_key, contests)
            if contests is None:
                return data
        except exc.SQLAlchemyError:
            pass
    else:
        try:
            cache_key_name = "all_contests"
            cache_key = hashlib.md5((cache_key_name).encode('utf-8')).hexdigest()
            cached_output = cache.get(cache_key)
            if cached_output is not None:
                current_app.log.info('found cached result for key: %s' % cache_key_name)
                contests = cached_output
            else:
                current_app.log.info('did not find cached result for key: %s' % cache_key_name)
                contests = Contest.query.all()
                cached_output = cache.set(cache_key, contests)
            if contests is None:
                return data
        except exc.SQLAlchemyError:
            pass

    data = [Contest.row2dict(contest) for contest in contests]
    output = json.dumps(data)
    mime = 'application/json'
    ctype = 'application/json; charset=UTF-8'

    res = Response(response = output, status = 200, mimetype = mime)
    res.headers['Content-Type'] = ctype
    res.headers['Connection'] = 'keep-alive'
    return res


@bp.route('/meta/', methods=['GET'])
def meta():
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
    return res


@bp.route('/questions/', methods=['GET'])
@cache.cached(timeout=30, query_string=True)
def questions():
    # GET request
    question_id = request.values.get('question_id', None)
    contest_id = request.values.get('contest_id', None)
    
    data = []
    if question_id is not None:
        try:
            questions = Question.query.filter_by(id=question_id).all()
            if questions is None:
                return data
        except exc.SQLAlchemyError:
            pass
    elif contest_id is not None:
        try:
            questions = Question.query.filter_by(contest_id=contest_id).all()
            if questions is None:
                return data
        except exc.SQLAlchemyError:
            pass
    else:
        try:
            questions = Question.query.all()
            if questions is None:
                return data
        except exc.SQLAlchemyError:
            pass

    data = [Question.row2dict(question) for question in questions]
    output = json.dumps(data)
    mime = 'application/json'
    ctype = 'application/json; charset=UTF-8'

    res = Response(response = output, status = 200, mimetype = mime)
    res.headers['Content-Type'] = ctype
    res.headers['Connection'] = 'keep-alive'
    return res


@bp.route('/results/', methods=['GET'])
@cache.cached(timeout=30, query_string=True)
def results():
    # GET request
    result_id  = request.values.get('result_id', None)
    contest_id = request.values.get('contest_id', None)
    
    data = []
    if result_id is not None:
        try:
            results = Result.query.filter_by(id=result_id).all()
            if results is None:
                return data
        except exc.SQLAlchemyError:
            pass
    elif contest_id is not None:
        try:
            results = Result.query.filter_by(contest_id=contest_id).all()
            if results is None:
                return data
        except exc.SQLAlchemyError:
            pass
    else:
        try:
            results = Result.query.all()
            if results is None:
                return data
        except exc.SQLAlchemyError:
            pass

    data = [Result.row2dict(result) for result in results]
    output = json.dumps(data)
    mime = 'application/json'
    ctype = 'application/json; charset=UTF-8'

    res = Response(response = output, status = 200, mimetype = mime)
    res.headers['Content-Type'] = ctype
    res.headers['Connection'] = 'keep-alive'
    return res

