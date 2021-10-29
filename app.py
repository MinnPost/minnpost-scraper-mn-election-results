from src import create_app, db, cli, ext_celery
from src.models import Area, Contest, Meta, Question, Result

app = create_app()
celery = ext_celery.celery
cli.register(app)

@app.shell_context_processor
def make_shell_context():
    return {'db': db, 'Area': Area, 'Contest': Contest, 'Meta': Meta, 'Question': Question, 'Result': Result}