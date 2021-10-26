import json
import hashlib
from datetime import datetime
from flask import jsonify, request, Response, current_app
from sqlalchemy import text
from sqlalchemy import exc
from src import cache, db
from src.models import Area, Contest, Meta, Question, Result
from src.api import bp
from src.api.errors import bad_request

#import sqlparse
from mo_sql_parsing import parse

@bp.route('/query/', methods=['GET', 'POST'])
def query():
    sql = request.args.get('q', None)
    cb = request.args.get('callback')
    example_query = 'SELECT * FROM contests WHERE title LIKE \'%governor%\'';
    #current_app.log.info(f"the query is {example_query}")
    cache_list_key = current_app.config['QUERY_LIST_CACHE_KEY']
    all_cache_keys = cache.get(cache_list_key)
    if all_cache_keys is None:
        all_cache_keys = []

    try:
        parsed = parse(sql)
    except Exception:
        return 'Hi, welcome to the election scraper local server. Use a URL like: <a href="/query/?q=%s">/query/?q=%s</a>' % (example_query, example_query);

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

@bp.route('/results/', methods=['GET', 'POST'])
def results():
    if request.method == 'POST':
        title = request.values.get('title', None)
        result_id = request.values.get('result_id', None)
        contest_id = request.values.get('contest_id', None)
    else:
        # You probably don't have args at this route with GET
        # method, but if you do, you can access them like so:
        title = request.args.get('title', None)
        result_id = request.args.get('result_id', None)
        contest_id = request.args.get('contest_id', None)
    if 'title' == None and 'result_id' == None and 'contest_id' == None:
        return bad_request('POST request must include title, result id, or contest id')        
    
    if result_id is not None:
        data = []
        try:
            results = Result.query.filter_by(result_id=result_id).all()
            if results is None:
                return data
            for row in results:
                #d = dict(row)
                d = row
                if 'updated' in d:
                    if not isinstance(d['updated'], int):
                        d['updated'] = datetime.timestamp(d['updated'])
                data.append(d)
        except exc.SQLAlchemyError:
            pass
        output = json.dumps(data)
        mime = 'application/json'
        ctype = 'application/json; charset=UTF-8'

        res = Response(response = output, status = 200, mimetype = mime)
        res.headers['Content-Type'] = ctype
        res.headers['Connection'] = 'keep-alive'
        return res