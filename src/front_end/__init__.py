from flask import Blueprint

bp = Blueprint('front-end', __name__)

from src.front_end import routes
