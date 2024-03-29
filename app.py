from src import create_app, cli
from src.extensions import db
from src.celery import configure_celery
from src.models import Area, Contest, Election, Question, Result

app = create_app()
configure_celery(app)

cli.register(app)

@app.shell_context_processor
def make_shell_context():
    return {'db': db, 'Area': Area, 'Contest': Contest, 'Election': Election, 'Question': Question, 'Result': Result}