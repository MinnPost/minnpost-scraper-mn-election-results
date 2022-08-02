import json
import hashlib
import ciso8601
from datetime import datetime
from flask import jsonify, request, Response, current_app
from sqlalchemy import text
from sqlalchemy import exc
from src.extensions import cache, db
from src.models import Area, Contest, Meta, Question, Result
from src.api import bp
from src.api.errors import bad_request

import sqlparse

@bp.route('/query/', methods=['GET', 'POST'])
def query():
    sql = request.args.get('q', None)
    parsed = sqlparse.parse(sql)[0]
    cb = request.args.get('callback')
    example_query = 'SELECT * FROM contests WHERE title LIKE \'%governor%\'';
    sqltype = parsed.get_type()
    if sqltype != 'SELECT' or parsed in ['', None]:
        return 'Hi, welcome to the election scraper local server. Use a URL like: <a href="/query/?q=%s">/query/?q=%s</a>' % (example_query, example_query);

    cache_list_key = current_app.config['QUERY_LIST_CACHE_KEY']
    all_cache_keys = cache.get(cache_list_key)
    if all_cache_keys is None:
        all_cache_keys = []

    cache_key = hashlib.md5(sql.encode('utf-8')).hexdigest()

    cached_output = cache.get(cache_key)
    if cached_output is not None:
        current_app.log.info('found cached result for key: %s' % cache_key)
        output = cached_output
    else:
        current_app.log.info('did not find cached result for key: %s' % cache_key)
        data = []
        try:
            results = db.session.execute(sql)    
            if results is None:
                return data
            for row in results:
                d = dict(row)
                if 'updated' in d:
                    if not isinstance(d['updated'], int):
                        d['updated'] = datetime.timestamp(d['updated'])
                if 'key' in d and d['key'] == 'updated' and d['type'] == 'int':
                    if not isinstance(d['value'], int):
                        date_object = ciso8601.parse_datetime(d['value'])
                        d['value'] = datetime.timestamp(date_object)
                data.append(d)
        except exc.SQLAlchemyError:
            pass
        output = json.dumps(data)
        cached_output = cache.set(cache_key, output)
        all_cache_keys.append(cache_key)
        cached_keys_updated = cache.set(cache_list_key, all_cache_keys)
        if cached_output == True and cached_keys_updated == True:
            current_app.log.info('create cached result for key: %s and list of keys: %s' % (cache_key, all_cache_keys))
        else:
            current_app.log.info('failed to cache data for supplied key')
    
    mime = 'application/json'
    ctype = 'application/json; charset=UTF-8'

    if cb is not None:
        output = '%s(%s);' % (cb, output)
        mime = 'text/javascript'
        ctype = 'text/javascript; charset=UTF-8'

    res = Response(response = output, status = 200, mimetype = mime)
    res.headers['Content-Type'] = ctype
    res.headers['Connection'] = 'keep-alive'
    return res


@bp.route('/areas/', methods=['GET'])
@cache.cached(timeout=30, query_string=True)
def areas():
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
@cache.cached(timeout=30, query_string=True)
def contests():
    if request.method == 'POST':
        # these are legacy
        title = request.values.get('title', None)
        contest_id = request.values.get('contest_id', None)
    else:
        title = request.values.get('title', None)
        contest_id = request.values.get('contest_id', None)
    
    data = []
    if contest_id is not None:
        try:
            contests = Contest.query.join(Result.contests).filter_by(id=contest_id).all()
            if contests is None:
                return data
        except exc.SQLAlchemyError:
            pass
    elif title is not None:
        try:
            contests = Contest.query.filter_by(title=title).all()
            if contests is None:
                return data
        except exc.SQLAlchemyError:
            pass
    else:
        try:
            contests = Contest.query.all()
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
@cache.cached(timeout=30, query_string=True)
def meta():
    key = request.values.get('key', None)
    data = []
    if key is not None:
        try:
            meta = Meta.query.filter_by(key=key).all()
            if meta is None:
                return data
        except exc.SQLAlchemyError:
            pass
    else:
        try:
            meta = Meta.query.all()
            if meta is None:
                return data
        except exc.SQLAlchemyError:
            pass
    
    data = [Meta.row2dict(metaItem) for metaItem in meta]
    output = json.dumps(data)
    mime = 'application/json'
    ctype = 'application/json; charset=UTF-8'

    res = Response(response = output, status = 200, mimetype = mime)
    res.headers['Content-Type'] = ctype
    res.headers['Connection'] = 'keep-alive'
    return res


@bp.route('/questions/', methods=['GET'])
@cache.cached(timeout=30, query_string=True)
def questions():
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


@bp.route('/results/', methods=['GET', 'POST'])
@cache.cached(timeout=30, query_string=True)
def results():
    if request.method == 'POST':
        # these are legacy
        result_id = request.values.get('result_id', None)
        contest_id = request.values.get('contest_id', None)
    else:
        result_id = request.values.get('result_id', None)
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

