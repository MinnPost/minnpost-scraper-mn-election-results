from flask import Blueprint

bp = Blueprint('scraper', __name__)

from src.scraper import areas, contests, elections, questions, results
