import os
import json
import re
import csv
import urllib.request
import requests
#import unicodecsv
import pytz
import time
import datetime
from datetime import timedelta
from flask import current_app
from src.extensions import db

from src.boundaries import Boundaries
from sqlglot import exp, parse_one, errors

from sqlalchemy import text, inspect, func, select
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert
from sqlalchemy.ext.hybrid import hybrid_property

scraper_sources_inline = None


class Message(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    text = db.Column(db.Unicode(200), nullable=False)


class ScraperModel(object):

    nonpartisan_parties = ['NP', 'WI', 'N P']


    def __init__(self, group_type = None):
        """
        Constructor
        """


    @classmethod
    def get_classname(cls):
        return cls.__name__


    def row2dict(self, row, child_name=''):
        #return {
        #    c.name: str(getattr(row, c.name))
        #    for c in row.__table__.columns
        #}
        dictfromrow = {
            c.key: getattr(row, c.key)
            for c in inspect(row).mapper.column_attrs
        }
        if hasattr(row, child_name):
            dictfromrow[child_name] = [self.row2dict(item) for item in getattr(row, child_name)]

            try:
                dictfromrow[child_name] = self.sort_children(child_name, dictfromrow[child_name], row)
            except AttributeError:
                pass         

        return dictfromrow

    
    def output_for_cache(self, query_result, args = {}, single_row=False, child_name=''):
        output = {}
        if query_result is None:
            return output
        if single_row == False:
            data = [self.row2dict(item, child_name) for item in query_result]
        else:
            data = self.row2dict(query_result, child_name)
        if "display_cache_data" in args and args["display_cache_data"] == "true":
            output["data"] = data
            output["generated"] = datetime.datetime.now(pytz.timezone(current_app.config["TIMEZONE"]))
        else:
            output = data
        return output

    
    def read_sources(self):
        """
        Read the scraper_sources.json file.
        """
        if scraper_sources_inline is not None:
            self.sources = json.loads(scraper_sources_inline)
        else:
            #sources_file = current_app.config['SOURCES_FILE']
            sources_file = os.path.join(current_app.root_path, '../scraper_sources.json')
            data = open(sources_file)
            self.sources = json.load(data)

        return self.sources


    def set_election(self, election_id = None):
        # priority:
        # 1. url or other function argument for election
        # 2. config level override
        # 3. newest election in the elections table
        election = None
        if election_id == None:
            election_id = current_app.config["ELECTION_DATE_OVERRIDE"]

        # if there is an election id value from anywhere
        if election_id is not None and election_id != "":
            if not election_id.startswith('id-'):
                election_id = 'id-' + election_id
            election = Election.query.filter_by(id=election_id).first()
        
        if election == None:
            election = Election.query.order_by(Election.election_datetime.desc()).first()

        sources      = self.read_sources()
        election_key = ''.join(election.id.split('id-', 3))
        if election_key not in sources:
            return None

        #current_app.log.debug('Set election to: %s' % election)
        return election


    def format_sql(self, sql, election_id = None):
        try:
            expression_tree = parse_one(sql).assert_is(exp.Select)
        except errors.ParseError:
            return ""

        alias = ""
        aliases = []
        if "areas AS a" in sql:
            alias = "a"
            aliases.append("a")
        if "contests AS c" in sql:
            alias = "c"
            aliases.append("c")
        if "elections AS e" in sql:
            alias = "e"
            aliases.append("e")
        if "questions AS q" in sql:
            alias = "q"
            aliases.append("q")
        if "results AS r" in sql:
            alias = "r"
            aliases.append("r")

        election_id_field = "election_id"
        if " from elections" in sql.lower():
            election_id_field = "id"
        #if alias != "":
        #    alias = alias + '.'
        #if election_id is not None:
        #    expression_tree = expression_tree.where(f"{alias}{election_id_field}='{election_id}'")
        if aliases:
            for alias in aliases:
                if alias != "":
                    alias = alias + '.'
                if election_id is not None:
                    expression_tree = expression_tree.where(f"{alias}{election_id_field}='{election_id}'")
        
        # make the query case insensitive
        sql = expression_tree.sql().replace(" LIKE ", " ILIKE ")
        #current_app.log.debug('sql is %s' % sql)
        return sql


    def set_election_key(self, election_id):
        key = election_id
        if election_id.startswith('id-'):
            key = election_id.replace("id-", "", 1)
        return key


    def parse_election(self, source, election = {}):

        # Ensure we have a valid parser for this type
        parser_method = getattr(self, "parser", None)
        if callable(parser_method):
            # Check if election has base_url
            source['url'] = election.base_url + source['url'] if election.base_url else source['url']

            # Get data from URL
            try:
                # Ballot questions spreadsheet requires latin-1 encoding
                #rows = unicodecsv.reader(scraped.splitlines(), delimiter=';', quotechar='|', encoding='latin-1')
                response = urllib.request.urlopen(source['url'])
                lines = [l.decode('latin-1') for l in response.readlines()]
                rows = csv.reader(lines, delimiter=';')
                election_source_data = {}
                election_source_data["rows"] = rows
                headers = dict(response.getheaders())
                if "Last-Modified" in headers:
                    election_source_data["updated"] = headers['Last-Modified']
                else:
                    #current_app.log.debug('headers is %s ' % headers)
                    election_source_data["updated"] = None
                return election_source_data
            except Exception as err:
                current_app.log.error('[%s] Error when trying to read URL and parse CSV: %s' % (source['type'], source['url']))
                raise


    def from_dict(self, data, new=False):
        for field in data:
            setattr(self, field, data[field])


    def post_processing(self, type, election_id = None):

        # Handle any supplemental data
        spreadsheet_result = self.supplement_connect('supplemental_' + type, election_id)
        spreadsheet_rows   = None
        election = self.set_election(election_id)
        if election is None:
            #current_app.log.debug('Election missing in the %s post processing: %s' % type)
            return spreadsheet_rows
        if "rows" in spreadsheet_result:
            #current_app.log.debug('Valid spreadsheet rows result. Spreadsheet result is %s ' % spreadsheet_result)
            spreadsheet_rows = spreadsheet_result['rows']
        updated = None
        if "updated" in spreadsheet_result:
            updated = spreadsheet_result["updated"]
        supplemented_rows = []
        insert_rows = {'action': 'insert', 'rows': []}
        update_rows = {'action': 'update', 'rows': []}
        delete_rows = {'action': 'delete', 'rows': []}
        if spreadsheet_rows is None:
            return supplemented_rows

        # for each row in the spreadsheet
        for spreadsheet_row in spreadsheet_rows:
            supplement_row = self.supplement_row(spreadsheet_row, election.id, updated)
            if 'rows' in supplement_row:
                if supplement_row['action'] == 'insert' and supplement_row['rows'] not in insert_rows['rows']:
                    insert_rows['rows'] = list(set(insert_rows['rows'] + supplement_row['rows']))
                elif supplement_row['action'] == 'update' and supplement_row['rows'] not in update_rows['rows']:
                    update_rows['rows'] = list(set(update_rows['rows'] + supplement_row['rows']))
                elif supplement_row['action'] == 'delete' and supplement_row['rows'] not in delete_rows['rows']:
                    #delete_rows['rows'] = list(set(insert_rows['rows'] + supplement_row['rows'])) # seems like this is wrong
                    delete_rows['rows'] = list(set(delete_rows['rows'] + supplement_row['rows']))
        if insert_rows not in supplemented_rows:
            supplemented_rows.append(insert_rows)
        if update_rows not in supplemented_rows:
            supplemented_rows.append(update_rows)
        if delete_rows not in supplemented_rows:
            supplemented_rows.append(delete_rows)
        return supplemented_rows


    def supplement_connect(self, source, election_id = None):
        """
        Connect to supplemental source (Google spreadsheets) given set.
        """
        sources = self.read_sources()
        election = self.set_election(election_id)
        election_key = self.set_election_key(election.id)

        data = {}
        result_json = None
        supplemental_output = {}

        if election_key not in sources:
            current_app.log.error('Election missing in sources: %s' % election_key)
            return supplemental_output

        if source not in sources[election_key]:
            # this just means there isn't a supplemental spreadsheet for that source
            #current_app.log.debug('Source missing in the %s election: %s' % (election_key, source))
            return supplemental_output

        s = sources[election_key][source]
        spreadsheet_id = s["spreadsheet_id"]
        worksheet_id = str(s["worksheet_id"])
        cache_timeout = int(current_app.config["PARSER_API_CACHE_TIMEOUT"])
        parser_store_in_s3 = current_app.config["PARSER_STORE_IN_S3"]
        parser_bypass_cache = current_app.config["PARSER_BYPASS_API_CACHE"]
        if spreadsheet_id is not None:
            parser_api_key = current_app.config["PARSER_API_KEY"]
            authorize_url = current_app.config["AUTHORIZE_API_URL"]
            url = current_app.config["PARSER_API_URL"]
            if authorize_url != "" and parser_api_key != "" and url != "":
                token_params = {
                    "api_key": parser_api_key
                }
                token_headers = {'Content-Type': 'application/json'}
                token_result = requests.post(authorize_url, data=json.dumps(token_params), headers=token_headers)
                try:
                    token_json = token_result.json()
                except Exception as e:
                    current_app.log.error('Error in token request for spreadsheet ID %s. Error is: %s' % (spreadsheet_id, e))
                    return supplemental_output
                if "token" in token_json:
                    token = token_json["token"]
                    authorized_headers = {"Authorization": f"Bearer {token}"}
                    result = requests.get(f"{url}?spreadsheet_id={spreadsheet_id}&worksheet_keys={worksheet_id}&external_use_s3={parser_store_in_s3}&bypass_cache={parser_bypass_cache}", headers=authorized_headers)
                    result_json = result.json()
                else:
                    current_app.log.error('Error in token authorize. Token result is: %s' % token_json)
                    result_json = None
                    return supplemental_output
        if result_json is not None and worksheet_id in result_json:
            data["rows"] = result_json[worksheet_id]

            # set metadata and send the customized json output to the api
            if "generated" in result_json:
                data["generated"] = result_json["generated"]
            data["customized"] = datetime.datetime.now(pytz.timezone(current_app.config["TIMEZONE"]))
            if cache_timeout != 0:
                data["cache_timeout"] = data["customized"] + timedelta(seconds=int(cache_timeout))
            else:
                data["cache_timeout"] = 0
            output = json.dumps(data, default=str)
            
        if result_json is not None and "customized" not in result_json or parser_store_in_s3 == "true":
            overwrite_url = current_app.config["OVERWRITE_API_URL"]
            params = {
                "spreadsheet_id": spreadsheet_id,
                "worksheet_keys": [worksheet_id],
                "output": output,
                "cache_timeout": cache_timeout,
                "bypass_cache": "true",
                "external_use_s3": parser_store_in_s3
            }

            headers = {'Content-Type': 'application/json'}
            if authorized_headers:
                headers = headers | authorized_headers
            result = requests.post(overwrite_url, data=json.dumps(params), headers=headers)
            result_json = result.json()

            if result_json is not None and "rows" in result_json:
                #output = json.dumps(result_json, default=str)
                supplemental_output["rows"] = result_json["rows"]
                if "generated" in result_json:
                    supplemental_output["updated"] = result_json["generated"]
                if "customized" in data:
                    supplemental_output["updated"] = data["customized"]

        return supplemental_output


class Area(ScraperModel, db.Model):

    __tablename__ = "areas"

    id = db.Column(db.String(255), autoincrement=False, nullable=False)
    election_id = db.Column(db.String(255), db.ForeignKey('elections.id'), autoincrement=False, nullable=False, server_default='')
    areas_group = db.Column(db.String(255))
    county_id = db.Column(db.String(255))
    county_name = db.Column(db.String(255))
    ward_id = db.Column(db.String(255))
    precinct_id = db.Column(db.String(255))
    precinct_name = db.Column(db.String(255))
    state_senate_id = db.Column(db.String(255))
    state_house_id = db.Column(db.String(255))
    county_commissioner_id = db.Column(db.String(255))
    district_court_id = db.Column(db.String(255))
    soil_water_id = db.Column(db.String(255))
    school_district_id = db.Column(db.String(255))
    school_district_name = db.Column(db.String(255))
    mcd_id = db.Column(db.String(255))
    precincts = db.Column(db.String(255))
    name = db.Column(db.String(255))
    updated = db.Column(db.DateTime(timezone=True), default=db.func.current_timestamp())

    __table_args__ = (
        db.PrimaryKeyConstraint(
            'id', 'election_id', name='election_area_id'
        ),
    )


    def __init__(self, **kwargs):
        self.id = kwargs.get('id')
        self.election_id = kwargs.get('election_id')
        self.areas_group = kwargs.get('areas_group')
        self.county_id = kwargs.get('county_id')
        self.county_name = kwargs.get('county_name')
        self.ward_id = kwargs.get('ward_id')
        self.precinct_id = kwargs.get('precinct_id')
        self.precinct_name = kwargs.get('precinct_name')
        self.state_senate_id = kwargs.get('state_senate_id')
        self.state_house_id = kwargs.get('state_house_id')
        self.county_commissioner_id = kwargs.get('county_commissioner_id')
        self.district_court_id = kwargs.get('district_court_id')
        self.soil_water_id = kwargs.get('soil_water_id')
        self.school_district_id = kwargs.get('school_district_id')
        self.school_district_name = kwargs.get('school_district_name')
        self.mcd_id = kwargs.get('mcd_id')
        self.name = kwargs.get('name')


    def __repr__(self):
        return '<Area {}>'.format(self.id)


    def __repr__(self):
        return '<Area {}>'.format(self.id)


    def parser(self, row, group, election_id, updated = None):

        # General data
        parsed = {
            'area_id': group + '-',
            'election_id': election_id,
            'areas_group': group,
            'county_id': None,
            'county_name': None,
            'ward_id': None,
            'precinct_id': None,
            'precinct_name': '',
            'state_senate_id': None,
            'state_house_id': None,
            'county_commissioner_id': None,
            'district_court_id': None,
            'soil_water_id': None,
            'school_district_id': None,
            'school_district_name': '',
            'mcd_id': None,
            'precincts': None,
            'name': ''
        }

        if group == 'municipalities':
            parsed['area_id'] = parsed['area_id'] + row[0] + '-' + row[2]
            parsed['county_id'] = row[0]
            parsed['county_name'] = row[1]
            parsed['mcd_id'] = "{0:05d}".format(int(row[2])) #enforce 5 digit
            parsed['name'] = row[1] # I'm not sure why this doesn't use row[3]? Keeping the old version for now though.

        if group == 'counties':
            parsed['area_id'] = parsed['area_id'] + row[0]
            parsed['county_id'] = row[0]
            parsed['county_name'] = row[1]
            parsed['precincts'] = row[2]

        if group == 'precincts':
            parsed['area_id'] = parsed['area_id'] + row[0] + '-' + row[1]
            parsed['county_id'] = row[0]
            parsed['precinct_id'] = row[1]
            parsed['precinct_name'] = row[2]
            parsed['state_senate_id'] = row[3]
            parsed['state_house_id'] = row[4]
            parsed['county_commissioner_id'] = row[5]
            parsed['district_court_id'] = row[6]
            parsed['soil_water_id'] = row[7]
            parsed['mcd_id'] = row[8]

        if group == 'school_districts':
            parsed['area_id'] = parsed['area_id'] + row[0]
            parsed['school_district_id'] = row[0]
            parsed['school_district_name'] = row[1]
            parsed['county_id'] = row[2]
            parsed['county_name'] = row[3]

        if updated is not None:
            parsed['updated'] = updated

        parsed['id'] = parsed['area_id']

        return parsed


class Election(ScraperModel, db.Model):

    __tablename__ = "elections"

    id = db.Column(db.String(255), primary_key=True, autoincrement=False, nullable=False)
    base_url = db.Column(db.String(255))
    date = db.Column(db.String(255))
    primary = db.Column(db.Boolean())
    updated = db.Column(db.DateTime(timezone=True), default=db.func.current_timestamp())
    scraped = db.Column(db.DateTime(timezone=True), default=db.func.current_timestamp(), onupdate=db.func.current_timestamp())

    areas = db.relationship('Area', backref=__tablename__, lazy=True)
    contests = db.relationship('Contest', backref=__tablename__, lazy=True)
    questions = db.relationship('Question', backref=__tablename__, lazy=True)
    results = db.relationship('Result', backref=__tablename__, lazy=True)


    def __init__(self, **kwargs):
        self.id = kwargs.get('id')
        self.base_url = kwargs.get('base_url')
        self.date = kwargs.get('date')
        self.primary = kwargs.get('primary')
        self.updated = kwargs.get('updated')


    def __repr__(self):
        return '<Election {}>'.format(self.id)


    @hybrid_property
    def election_datetime(self):
        return datetime.datetime.strptime(self.date, '%Y-%m-%d')


    @election_datetime.expression
    def election_datetime(self):
        return func.to_date(self.date, "YYYY-MM-DD")


    @hybrid_property
    def election_key(self):
        fixed = ''.join(self.id.split('id-', 3))
        return fixed


    @election_key.expression
    def election_key(self):
        return func.substr(self.id, func.strpos(self.id,'id-'))


    @hybrid_property
    def contest_count(self):
        #return len(self.contests)   # @note: use when non-dynamic relationship
        return self.contests.count()# @note: use when dynamic relationship


    @contest_count.expression
    def contest_count(cls):
        return (select([func.count(Contest.id)]).
                where(Contest.election_id == cls.id).
                label("contest_count")
                )


    def output_for_cache(self, query_result, args = {}, single_row=False):
        output = {}
        if query_result is None:
            return output
        if single_row == False:
            data = []
            for item in query_result:
                election = self.row2dict(item)
                if "contest_count" not in election:
                    election["contest_count"] = len(item.contests)
                data.append(election)
        else:
            data = self.row2dict(query_result)
            if "contest_count" not in data:
                data["contest_count"] = len(query_result.contests)
        if "display_cache_data" in args and args["display_cache_data"] == "true":
            output["data"] = data
            output["generated"] = datetime.datetime.now(pytz.timezone(current_app.config["TIMEZONE"]))
        else:
            output = data
        return output


    def parser(self, row, key):
        """
        Parser for election scraping.
        """

        election_meta = row["meta"] if 'meta' in row else {}

        # base url is stored in the meta
        base_url = election_meta['base_url'] if 'base_url' in election_meta else ""

        # election date is stored in the meta
        date = election_meta['date'] if 'date' in election_meta else ""

        # Create ids.
        election_id = 'id-' + key

        # Primary is not designated in any way, but we can make some initial
        # guesses. All contests in an election are considered primary, but
        # non-partisan ones only mean there is more than one seat available.
        primary = election_meta['primary'] if 'primary' in election_meta else False

        # try to set the updated value based on the most recently updated result
        try:
            query_result = Result.query.filter_by(election_id=election_id).order_by(Result.updated.desc()).first()
        except Exception as e:
            query_result = {}
            pass
        
        updated = db.func.current_timestamp()
        if query_result is not None and query_result.updated:
            updated = query_result.updated

        parsed = {
            'id': election_id,
            'base_url': base_url,
            'date': date,
            'primary': primary,
            'updated': updated
        }

        # Return election record
        return parsed


    def legacy_meta_output(self, election_id):
        query_result = self.query.filter_by(id=election_id).first()
        election = self.row2dict(query_result)
        data = []
        for key in election:
            row = {}
            row["key"] = key
            if key == "updated":
                row["value"] = time.mktime(election[key].timetuple())
            else:
                row["value"] = election[key]
            row["type"] = type(election[key]).__name__
            data.append(row)
        #current_app.log.debug(data)
        return data


class Contest(ScraperModel, db.Model):

    __tablename__ = "contests"

    #list of county names
    mn_counties = ["Aitkin", "Anoka", "Becker", "Beltrami", "Benton", "Big Stone", "Blue Earth", "Brown", "Carlton", "Carver", "Cass", "Chippewa", "Chisago", "Clay", "Clearwater", "Cook", "Cottonwood", "Crow Wing", "Dakota", "Dodge", "Douglas", "Faribault", "Fillmore", "Freeborn", "Goodhue", "Grant", "Hennepin", "Houston", "Hubbard", "Isanti", "Itasca", "Jackson", "Kanabec", "Kandiyohi", "Kittson", "Koochiching", "Lac qui Parle", "Lake", "Lake of the Woods", "Le Sueur", "Lincoln", "Lyon", "McLeod", "Mahnomen", "Marshall", "Martin", "Meeker", "Mille Lacs", "Morrison", "Mower", "Murray", "Nicollet", "Nobles", "Norman", "Olmsted", "Otter Tail", "Pennington", "Pine", "Pipestone", "Polk", "Pope", "Ramsey", "Red Lake", "Redwood", "Renville", "Rice", "Rock", "Roseau", "Saint Louis", "Scott", "Sherburne", "Sibley", "Stearns", "Steele", "Stevens", "Swift", "Todd", "Traverse", "Wabasha", "Wadena", "Waseca", "Washington", "Watonwan", "Wilkin", "Winona", "Wright", "Yellow Medicine"]

    id = db.Column(db.String(255), autoincrement=False, nullable=False)
    election_id = db.Column(db.String(255), db.ForeignKey('elections.id'), autoincrement=False, nullable=False, server_default='')
    office_id = db.Column(db.String(255))
    results_group = db.Column(db.String(255))
    office_name = db.Column(db.String(255))
    district_code = db.Column(db.String(255))
    state = db.Column(db.String(255))
    county_id = db.Column(db.String(255))
    precinct_id = db.Column(db.String(255))
    precincts_reporting = db.Column(db.BigInteger())
    total_effected_precincts = db.Column(db.BigInteger())
    total_votes_for_office = db.Column(db.BigInteger())
    seats = db.Column(db.BigInteger())
    ranked_choice = db.Column(db.Boolean())
    primary = db.Column(db.Boolean())
    scope = db.Column(db.String(255))
    title = db.Column(db.String(255))
    boundary = db.Column(db.String(510))
    partisan = db.Column(db.Boolean())
    question_body = db.Column(db.Text)
    sub_title = db.Column(db.String(255))
    incumbent_party = db.Column(db.String(255))
    percent_needed = db.Column(db.Float())
    called = db.Column(db.Boolean())
    updated = db.Column(db.DateTime(timezone=True), default=db.func.current_timestamp())

    results = db.relationship('Result', backref=__tablename__, lazy=True)

    __table_args__ = (
        db.PrimaryKeyConstraint(
            'id', 'election_id', name='election_contest_id'
        ),
    )


    def __init__(self, **kwargs):
        self.id = kwargs.get('id')
        self.election_id = kwargs.get('election_id')
        self.office_id = kwargs.get('office_id')
        self.results_group = kwargs.get('results_group')
        self.office_name = kwargs.get('office_name')
        self.district_code = kwargs.get('district_code')
        self.state = kwargs.get('state')
        self.county_id = kwargs.get('county_id')
        self.precinct_id = kwargs.get('precinct_id')
        self.precincts_reporting = kwargs.get('precincts_reporting')
        self.total_effected_precincts = kwargs.get('total_effected_precincts')
        self.total_votes_for_office = kwargs.get('total_votes_for_office')
        self.seats = kwargs.get('seats')
        self.ranked_choice = kwargs.get('ranked_choice')
        self.primary = kwargs.get('primary')
        self.scope = kwargs.get('scope')
        self.title = kwargs.get('title')
        self.boundary = kwargs.get('boundary')
        self.partisan = kwargs.get('partisan')
        self.question_body = kwargs.get('question_body')
        self.sub_title = kwargs.get('sub_title')
        self.incumbent_party = kwargs.get('incumbent_party')
        self.percent_needed = kwargs.get('percent_needed') # this is a null default because currently it only gets populated by the spreadsheet data
        self.called = kwargs.get('called')


    def __repr__(self):
        return '<Contest {}>'.format(self.id)


    def parser(self, row, group, election, source, updated = None):
        """
        Parser for contest scraping.
        """

        # SSD1 is Minneapolis and ISD1 is Aitkin, though they have the same
        # numbers and therefor make the same ID
        mpls_ssd = re.compile(r'.*\(SSD #1\).*', re.IGNORECASE).match(row[4])
        if mpls_ssd is not None:
            row[5] = '1-1'

        # Create ids.
        # id-State-County-Precinct-District-Office
        base_id = 'id-' + row[0] + '-' + row[1] + '-' + row[2] + '-' + row[5] + '-' + row[3]

        # Office refers to office name and office id as assigned by SoS, but
        # contest ID is a more specific id as office id's are not unique across
        # all results
        contest_id = base_id
        office_id = row[3]

        # For ranked choice voting, we want to a consistent contest id, as the
        # office_id is different for each set of choices.
        #
        # It seems that the office id is incremented by 1 starting at 1 so
        # we use the first
        ranked_choice = re.compile(r'.*(first|second|third|\w*th) choice.*', re.IGNORECASE).match(row[4])
        if ranked_choice is not None:
            office_id = ''.join(row[3].split())[:-1] + '1'
            contest_id = 'id-' + row[0] + '-' + row[1] + '-' + row[2] + '-' + row[5] + '-' + office_id

        # The only way to know if there are multiple seats is look at the office
        # name which has "(Elect X)" in it.
        re_seats = re.compile(r'.*\(elect ([0-9]+)\).*', re.IGNORECASE)
        matched_seats = re_seats.match(row[4])

        # Primary is not designated in any way, but we can make some initial
        # guesses. All contests in an election are considered primary, but
        # non-partisan ones only mean there is more than one seat available.
        primary = election.primary if election.primary else False

        re_question = re.compile(r'.*question.*', re.IGNORECASE)
        matched_question = re_question.match(row[4])
        primary = False if matched_question is not None else primary
        
        office_name = row[4]
        county_id = row[1]
        scope = source['contest_scope'] if 'contest_scope' in source else None
        district_code = row[5]
        title = self.generate_title(office_name, county_id, row, scope, district_code)

        parsed = {
            'id': contest_id,
            'contest_id': contest_id,
            'election_id': election.id,
            'office_id': office_id,
            'results_group': group,
            'office_name': office_name,
            'district_code': district_code,
            'state': row[0],
            'county_id': county_id,
            'precinct_id': row[2],
            'precincts_reporting': int(row[11]),
            'total_effected_precincts': int(row[12]),
            'total_votes_for_office': int(row[15]),
            'seats': int(matched_seats.group(1)) if matched_seats is not None else 1,
            'ranked_choice': ranked_choice is not None,
            'primary': primary,
            'scope': scope,
            'title': title
        }

        # set fields that aren't directly in the data
        boundaries         = Boundaries(Area)
        parsed['boundary'] = boundaries.find_boundary(parsed)
        parsed = self.set_question_fields(parsed)
        parsed['partisan'] = self.set_partisanship(parsed)
        parsed['seats'] = self.set_seats(parsed)

        if updated is not None:
            parsed['updated'] = updated

        # Return contest record
        return parsed


    def parser_results(self, result, row, group, election, source, updated):
        """
        Parser for limited contest scraping during results.
        """

        parsed = {}
        # Check for existing contest rows
        if result['contest_id']:
            contest = Contest.query.filter_by(id=result['contest_id'], election_id=election.id).first()
            if contest != None:
                parsed = self.row2dict(contest)
                parsed['precincts_reporting'] = int(row[11])
                parsed['total_effected_precincts'] = int(row[12])
                parsed['total_votes_for_office'] = int(row[15])
            #else:
                #current_app.log.info('Could not find matching contest for contest ID %s. Trying to create one, which is unexpected.' % result['contest_id'])
                #parsed = self.parser(row, group, election, source, updated)

            if parsed and updated is not None:
                parsed['updated'] = updated

        # Return parsed contest record
        return parsed


    def generate_title(self, office_name, county_id, row, scope = None, district_code = None):
        # Title and search term
        title = office_name
        title = re.compile(r'(\(elect [0-9]+\))', re.IGNORECASE).sub('', title)
        title = re.compile(r'((first|second|third|\w*th) choice)', re.IGNORECASE).sub('', title)

        # Look for non-ISD parenthesis which should be place names
        re_place = re.compile(r'.*\(([^#]*)\).*', re.IGNORECASE).match(title)
        title = re.compile(r'(\([^#]*\))', re.IGNORECASE).sub('', title)
        if re_place is not None:
            title = re_place.group(1) + ' ' + title
        title = title.rstrip()

        # Add county name to county commissioner contest titles
        if 'County Commissioner' in title and county_id:
            county_index = int(county_id) - 1
            title = self.mn_counties[county_index] + " " + title

        # Add county name to county sheriff contest titles
        if 'County Sheriff' in title and county_id:
            county_index = int(county_id) - 1
            title = self.mn_counties[county_index] + " " + title

        #Add county name to county questions
        if 'COUNTY QUESTION' in title and county_id:
            county_index = int(county_id) - 1
            title = self.mn_counties[county_index].upper() + " " + title
        
        #Add school district names to school district contests
        #with special handling for the SSD1 vs ISD1 issue
        if scope == "school" and district_code != None:
            if district_code == '0001':
                title = title[0:-1] + " - Aitkin)"
            elif district_code == '1-1':
                title = title[0:-1] + " - Minneapolis)"
            else:
                area_model = Area()
                try:
                    query_result = Area.query.filter_by(school_district_id=district_code, election_id=row['election_id']).all()
                    # set the output
                    output = area_model.output_for_cache(query_result, {})
                except Exception:
                    output = {}
                    pass
                for a in output:
                    if a['school_district_id']:
                        title = title[0:-1] + " - " + a['school_district_name'].title() + ")"

        return title


    def check_boundary(self, query_result):
        contests_count = 0
        boundaries_found = 0
        boundaries_not_found = []
        for item in query_result:
            contest = self.row2dict(item)
            contests_count = contests_count + 1
            for boundary in contest['boundary'].split(','):
                boundary_not_found = {}
                boundaries   = Boundaries(Area)
                boundary_url = boundaries.get_boundary_by_query(boundary)
                if boundary_url != "":
                    boundaries_found = boundaries_found + 1
                else:
                    boundary_not_found['contest'] = contest['title']
                    boundary_not_found['boundary'] = boundary
                    boundaries_not_found.append(boundary_not_found)
        output = {
            "contests_count": contests_count,
            "boundaries_found": boundaries_found,
            "boundaries_not_found": boundaries_not_found
        }
        return json.dumps(output)


    def set_question_fields(self, parsed_row):
        # Get question data
        try:
            questions = Question.query.all(contest_id=parsed_row['contest_id'], election_id=parsed_row['election_id'])
        except:
            questions = []
        
        # Assign the fields
        for q in questions:
            parsed_row['question_body'] = q.question_body
            parsed_row['sub_title'] = q.sub_title

        return parsed_row
    
    
    def set_partisanship(self, parsed_row):
        # Determine partisanship for contests for other processing. We need to look
        # at all the candidates to know if the contest is nonpartisan or not.
        #results = db.engine.execute("select result_id from results where contest_id = '%s' and party_id not in ('%s')" % (parsed_row['contest_id'], "', '".join(self.nonpartisan_parties)))
        results = Result.query.filter(
            Result.contest_id == parsed_row['contest_id'],
            Result.party_id.notin_(self.nonpartisan_parties)
        ).first()
        #users = session.query(Post).filter(not_(Post.tags.name.in_(['dont', 'want', these'])))
        if results is not None:
            partisan = True
        else:
            partisan = False
        return partisan
    
    
    def set_seats(self, parsed_row):
        # For non-partisan primaries, the general rule is that there are twice
        # as many winners as there are seats available for the general election.
        # Unfortunately we can't determine this from the existing value
        # otherwise, it will just grow.
        seats = parsed_row['seats']
        if parsed_row['primary'] and parsed_row['partisan'] is False:
            re_seats = re.compile(r'.*\(elect ([0-9]+)\).*', re.IGNORECASE)
            matched_seats = re_seats.match(parsed_row['office_name'])
            seats = matched_seats.group(1) if matched_seats is not None else 1
            seats = int(seats) * 2
        return seats
    
    
    def supplement_row(self, spreadsheet_row, election_id=None, updated=None):

        if isinstance(spreadsheet_row, (bytes, bytearray)):
            try:
                spreadsheet_row = json.loads(spreadsheet_row)
                #current_app.log.debug('Spreadsheet row: %s ' % spreadsheet_row)
            except Exception:
                #current_app.log.debug('Failed to load contest json into a dict. The json data is %s ' % spreadsheet_row)
                supplemented_row = {}
                return supplemented_row

        # parse/format the row
        spreadsheet_row = self.set_db_fields_from_spreadsheet(spreadsheet_row, election_id)

        supplemented_row = {}

        # Check for existing contest rows
        results = Contest.query.filter_by(id=spreadsheet_row['id'], election_id=election_id).all()

        # If valid data
        if spreadsheet_row['id'] is not None:
            # there are rows in the database to update or delete
            if results != None and results != []:
                update_results = []
                # for each matching row in the database to that spreadsheet row
                for matching_result in results:
                    for field in spreadsheet_row:
                        if spreadsheet_row[field] is not None and spreadsheet_row[field] != '':
                            matching_result.field = spreadsheet_row[field]
                    if election_id is not None:
                        matching_result.election_id = election_id
                    if updated is not None:
                        matching_result.updated = updated
                    if matching_result not in update_results:
                        update_results.append(matching_result)
                row_result = {
                    'action': 'update',
                    'rows': update_results
                }
                supplemented_row = row_result
            else:
                # make rows to insert
                insert_rows = []
                # Add new row, make sure to mark the row as supplemental
                new_contest = {}
                for field in spreadsheet_row:
                    if field == 'id':
                        # if the id gets renamed, use contest_id here.
                        new_contest['id'] = spreadsheet_row[field]
                    elif spreadsheet_row[field] is not None and spreadsheet_row[field] != '':
                        new_contest[field] = spreadsheet_row[field]
                if election_id is not None:
                    new_contest['election_id'] = election_id
                if updated is not None:
                    new_contest['updated'] = updated
                new_contest['results_group'] = 'supplemental_results'
                contest_model = Contest(**new_contest)
                if contest_model not in insert_rows:
                    insert_rows.append(contest_model)
                row_result = {
                    'action': 'insert',
                    'rows': insert_rows
                }
                supplemented_row = row_result
        return supplemented_row


    # this handles the key names and value formats for the database if they are different in the spreadsheet
    def set_db_fields_from_spreadsheet(self, spreadsheet_row, election_id=None):
        spreadsheet_row['id'] = str(spreadsheet_row['id'])
        if election_id is not None:
            spreadsheet_row['election_id'] = election_id
        if "incumbent_party" not in spreadsheet_row:
            spreadsheet_row['incumbent_party'] = spreadsheet_row.get('incumbent.party', "")
        if "question_help" not in spreadsheet_row:
            spreadsheet_row['question_help'] = spreadsheet_row.get('question.help', "")
        if "question_body" not in spreadsheet_row:
            spreadsheet_row['question_body'] = spreadsheet_row.get('question.body', "")
        if "precincts_reporting" not in spreadsheet_row:
            spreadsheet_row['precincts_reporting'] = spreadsheet_row.get('precincts.reporting', 0)
        if "percent_needed" not in spreadsheet_row:
            spreadsheet_row['percent_needed'] = spreadsheet_row.get('percent.needed', 0)
        return spreadsheet_row


    def sort_children(self, child_name, item, parent):
        if child_name == "results":
            item = sorted(
                item,
                key=lambda x: (-x['votes_candidate'], x['candidate'])
            )
            if parent.primary == True:
                item = sorted(
                    item,
                    key=lambda x: x['party_id']
                )
        return item


class Question(ScraperModel, db.Model):

    __tablename__ = "questions"

    id = db.Column(db.String(255), nullable=False)
    contest_id = db.Column(db.String(255))
    election_id = db.Column(db.String(255), db.ForeignKey('elections.id'), nullable=False, server_default='')
    title = db.Column(db.String(255))
    sub_title = db.Column(db.String(255))
    question_body = db.Column(db.Text)
    updated = db.Column(db.DateTime(timezone=True), default=db.func.current_timestamp())

    __table_args__ = (
        db.PrimaryKeyConstraint(
            'id', 'election_id', name='election_question_id'
        ),
    )


    def __init__(self, **kwargs):
        super(Question, self).__init__(**kwargs)


    def __repr__(self):
        return '<Question {}>'.format(self.id)


    def parser(self, row, group, election_id, updated = None):

        """
        Parser for ballot questions data.  Note that for whatever reason there
        are duplicates in the MN SoS data source.

        County ID
        Office Code
        MCD code, if applicable (using FIPS statewide unique codes, not county MCDs)
        School District Numbe, if applicable
        Ballot Question Number
        Question Title
        Question Body
        """
        combined_id = 'id-' + row[0] + '-' + row[1] + '-' + row[2] + '-' + row[3]

        # We have to do some hackery to get the right contest ID
        # County
        # 0 - - - 1
        # id-MN-38---0421

        # City question
        # ^0 - - 2 - 1
        #id-MN---43000-1131

        # School
        # ^0 - - 3 - 1
        # id-MN---110-5031

        # SSD1 is Minneapolis and ISD1 is Aitkin, though they have the same
        # numbers and therefor make the same ID
        mpls_ssd = re.compile(r'.*\(SSD #1\).*', re.IGNORECASE).match(row[4])
        if mpls_ssd is not None:
            row[3] = '1-1'

        contest_id = 'id-MN-' + row[0] + '-' + row[3] + '-' + row[2] + '-' + row[1]
        if row[2] is not None and row[2] != '':
            contest_id = 'id-MN---' + row[2] + '-' + row[1]
        if row[3] is not None and row[3] != '':
            contest_id = 'id-MN---' + row[3] + '-' + row[1]

        #Clean random formatting problems in question text
        question_body = row[6].replace("^", "").strip()
        question_body = question_body.replace("&ldquo",'"')
        question_body = question_body.replace("&ldquo",'"')

        # Make row
        parsed = {
            'id': combined_id,
            'question_id': combined_id,
            'contest_id': contest_id,
            'election_id': election_id,
            'title': row[4],
            'sub_title': row[5].title(),
            'question_body': question_body
        }

        if updated is not None:
            parsed['updated'] = updated

        return parsed


class Result(ScraperModel, db.Model):

    __tablename__ = "results"

    id = db.Column(db.String(255), primary_key=True, nullable=False, autoincrement=False)
    contest_id = db.Column(db.String(255), db.ForeignKey('contests.id'), nullable=False)
    election_id = db.Column(db.String(255), db.ForeignKey('elections.id'), primary_key=True, autoincrement=False, nullable=False, server_default='')
    results_group = db.Column(db.String(255))
    office_name = db.Column(db.String(255))
    candidate_id = db.Column(db.String(255))
    candidate = db.Column(db.String(255))
    suffix = db.Column(db.String(255))
    incumbent_code = db.Column(db.String(255))
    party_id = db.Column(db.String(255))
    votes_candidate = db.Column(db.BigInteger())
    percentage = db.Column(db.Float())
    ranked_choice_place = db.Column(db.BigInteger())
    updated = db.Column(db.DateTime(timezone=True), default=db.func.current_timestamp())

    __table_args__ = (
        db.PrimaryKeyConstraint(
            'id', 'election_id', name='election_result_id'
        ),
    )

    def __init__(self, **kwargs):
        self.id = kwargs.get('id')
        self.contest_id = kwargs.get('contest_id')
        self.election_id = kwargs.get('election_id')
        self.results_group = kwargs.get('results_group')
        self.office_name = kwargs.get('office_name')
        self.candidate_id = kwargs.get('candidate_id')
        self.candidate = kwargs.get('candidate')
        self.suffix = kwargs.get('suffix')
        self.incumbent_code = kwargs.get('incumbent_code')
        self.party_id = kwargs.get('party_id')
        self.votes_candidate = kwargs.get('votes_candidate')
        self.percentage = kwargs.get('percentage')
        self.ranked_choice_place = kwargs.get('ranked_choice_place')


    def __repr__(self):
        return '<Result {}>'.format(self.id)


    def parser(self, row, group, election_id, updated = None):
        """
        Parser for results type scraping.
        """
        ranked_choice_translations = { 'first': 1, 'second': 2, 'third': 3, 'fourth': 4, 'fifth': 5, 'sixth': 6, 'seventh': 7, 'eighth': 8, 'nineth': 9, 'tenth': 10, 'final': 100 }
        ranked_choice_place = None

        # SSD1 is Minneapolis and ISD1 is Aitkin, though they have the same
        # numbers and therefor make the same ID
        mpls_ssd = re.compile(r'.*\(SSD #1\).*', re.IGNORECASE).match(row[4])
        if mpls_ssd is not None:
            row[5] = '1-1'

        # Create ids.
        # id-State-County-Precinct-District-Office
        base_id = 'id-' + row[0] + '-' + row[1] + '-' + row[2] + '-' + row[5] + '-' + row[3]
        # id-BASE-Candidate
        row_id = base_id + '-' + row[6]

        # Office refers to office name and office id as assigned by SoS, but
        # contest ID is a more specific id as office id's are not unique across
        # all results
        contest_id = base_id
        office_id = row[3]

        # For ranked choice voting, we want to a consistent contest id, as the
        # office_id is different for each set of choices.
        #
        # It seems that the office id is incremented by 1 starting at 1 so
        # we use the first
        ranked_choice = re.compile(r'.*(first|second|third|\w*th) choice.*', re.IGNORECASE).match(row[4])
        if ranked_choice is not None:
            office_id = ''.join(row[3].split())[:-1] + '1'
            contest_id = 'id-' + row[0] + '-' + row[1] + '-' + row[2] + '-' + row[5] + '-' + office_id

            # Determine which "choice" this is
            for c in ranked_choice_translations:
                ranked_choice_choice = re.compile(r'.*%s.*' % c, re.IGNORECASE).match(row[4])
                if ranked_choice_choice is not None:
                    ranked_choice_place = ranked_choice_translations[c]

        parsed = {
            'id': row_id,
            'result_id': row_id,
            'election_id': election_id,
            'results_group': group,
            'office_name': row[4],
            'candidate_id': row[6],
            'candidate': row[7].replace('WRITE-IN**', 'WRITE-IN'),
            'suffix': row[8],
            'incumbent_code': row[9],
            'party_id': row[10],
            'votes_candidate': int(row[13]),
            'percentage': float(row[14]),
            'ranked_choice_place': int(ranked_choice_place) if ranked_choice_place is not None else 0,
            'contest_id': contest_id
        }

        if updated is not None:
            parsed['updated'] = updated

        # Return results record for the database
        return parsed


    def supplement_row(self, spreadsheet_row, election_id=None, updated=None):

        if isinstance(spreadsheet_row, (bytes, bytearray)):
            try:
                spreadsheet_row = json.loads(spreadsheet_row)
                #current_app.log.debug('Spreadsheet row: %s ' % spreadsheet_row)
            except Exception:
                #current_app.log.debug('Failed to load result json into a dict. The json data is %s ' % spreadsheet_row)
                supplemented_row = {}
                return supplemented_row

        # parse/format the row
        spreadsheet_row = self.set_db_fields_from_spreadsheet(spreadsheet_row, election_id)
        supplemented_row = {}

        # Check for existing result rows
        results = Result.query.filter_by(id=spreadsheet_row['id'], election_id=election_id).all()

        # If valid data
        if spreadsheet_row['id'] is not None and spreadsheet_row['contest_id'] is not None and spreadsheet_row['candidate_id'] is not None:
            # there are rows in the database to update or delete
            if results != None and results != []:
                # these rows can be updated
                if (spreadsheet_row['votes_candidate'] >= 0) and spreadsheet_row['enabled'] is True:
                    update_results = []
                    # for each matching row in the database to that spreadsheet row
                    for matching_result in results:
                        matching_result.percentage = spreadsheet_row['percentage']
                        matching_result.votes_candidate = spreadsheet_row['votes_candidate']
                        matching_result.ranked_choice_place = spreadsheet_row['ranked_choice_place']
                        if election_id is not None:
                            matching_result.election_id = election_id
                        if updated is not None:
                            matching_result.updated = updated
                        if matching_result not in update_results:
                            update_results.append(matching_result)
                    row_result = {
                        'action': 'update',
                        'rows': update_results
                    }
                    supplemented_row = row_result
                elif spreadsheet_row['enabled'] is False and results[0].results_group:
                    # these rows can be deleted
                    delete_result = {
                        'action': 'delete',
                        'rows': results
                    }
                    supplemented_row = delete_result
            elif (spreadsheet_row['votes_candidate'] >= 0) and spreadsheet_row['enabled'] is True:
                # these rows don't have a match. they should be inserted.
                insert_rows = []
                # Add new row, make sure to mark the row as supplemental
                spreadsheet_row['results_group'] = 'supplemental_results'
                insert_result = spreadsheet_row
                if election_id is not None:
                    insert_result['election_id'] = election_id
                if updated is not None:
                    insert_result['updated'] = updated
                result_model = Result(**insert_result)
                if result_model not in insert_rows:
                    insert_rows.append(result_model)
                row_result = {
                    'action': 'insert',
                    'rows': insert_rows
                }
                supplemented_row = row_result

        return supplemented_row


    # this handles the key names and value formats for the database if they are different in the spreadsheet
    def set_db_fields_from_spreadsheet(self, spreadsheet_row, election_id=None):
        spreadsheet_row['id'] = str(spreadsheet_row['id'])
        if election_id is not None:
            spreadsheet_row['election_id'] = election_id
        if "contest_id" not in spreadsheet_row:
            spreadsheet_row['contest_id'] = str(spreadsheet_row.get('contest.id', None))
        if "candidate_id" not in spreadsheet_row:
            spreadsheet_row['candidate_id'] = str(spreadsheet_row.get('candidate.id', None))
        if "office_name" not in spreadsheet_row:
            spreadsheet_row['office_name'] = spreadsheet_row.get('office.name', None)
        spreadsheet_row['percentage'] = spreadsheet_row.get('percentage', None)
        if "votes_candidate" not in spreadsheet_row:
            spreadsheet_row['votes_candidate'] = spreadsheet_row.get('votes.candidate', 0)
        if "ranked_choice_place" not in spreadsheet_row:
            spreadsheet_row['ranked_choice_place'] = spreadsheet_row.get('ranked.choice.place', None)
        spreadsheet_row['enabled'] = bool(spreadsheet_row.get('enabled', False))
        return spreadsheet_row
