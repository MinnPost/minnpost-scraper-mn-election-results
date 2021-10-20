import json
from datetime import datetime
from flask import jsonify, request, Response, url_for, abort
from sqlalchemy import text
from sqlalchemy import exc
from src import db
from src.models import Area, Contest, Meta, Question, Result
from src.api import bp
from src.api.errors import bad_request

@bp.route('/query/', methods=['GET', 'POST'])
def query():
    sql = request.args.get('q', None)
    cb = request.args.get('callback')
    example_query = 'SELECT * FROM contests WHERE title LIKE \'%governor%\'';

    if sql in ['', None]:
        return 'Hi, welcome to the election scraper local server. Use a URL like: <a href="/?q=%s">/?q=%s</a>' % (example_query, example_query);
    #output = json.dumps(sql)
    data = []
    print(sql)
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