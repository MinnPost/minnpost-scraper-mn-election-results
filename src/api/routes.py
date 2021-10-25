import json
from datetime import datetime
from flask import jsonify, request, Response, url_for, abort
from sqlalchemy import text
from sqlalchemy import exc
from src import cache, db, ScraperLogger
from src.models import Area, Contest, Meta, Question, Result
from src.api import bp
from src.api.errors import bad_request

log = ScraperLogger('scraper_results').logger

@bp.route('/query/', methods=['GET', 'POST'])
def query():
    sql = request.args.get('q', None)
    cb = request.args.get('callback')
    example_query = 'SELECT * FROM contests WHERE title LIKE \'%governor%\'';

    if sql in ['', None]:
        return 'Hi, welcome to the election scraper local server. Use a URL like: <a href="/?q=%s">/?q=%s</a>' % (example_query, example_query);
    #output = json.dumps(sql)
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