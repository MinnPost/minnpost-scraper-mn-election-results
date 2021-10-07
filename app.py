from app import create_app, db, cli
from app.models import Area, Contest, Meta, Question, Result

app = create_app()
cli.register(app)

@app.shell_context_processor
def make_shell_context():
    return {'db': db, 'Area': Area, 'Contest': Contest, 'Meta': Meta, 'Question': Question, 'Result': Result}