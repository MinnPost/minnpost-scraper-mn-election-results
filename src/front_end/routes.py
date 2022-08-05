import random
from flask import current_app, send_from_directory
from src.extensions import cache
from src.front_end import bp

@bp.route("/")
def base():
    return send_from_directory('../client/public', 'index.html')

# Path for all the static files (compiled JS/CSS, etc.)
@bp.route("/<path:path>")
def home(path):
    return send_from_directory('../client/public', path)

@bp.route("/rand")
def hello():
    return str(random.randint(0, 100))
