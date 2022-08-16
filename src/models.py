import os
import json
import re
import csv
import urllib.request
import requests
#import unicodecsv
import calendar
import datetime
from datetime import timedelta
from flask import current_app, request
from src.extensions import db

from sqlalchemy import text, inspect
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert

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

        # this is where scraperwiki was creating and connecting to its database
        # we do this in the imported sql file instead

        self.read_sources()


    @classmethod
    def get_classname(cls):
        return cls.__name__


    def row2dict(self, row):
        #return {
        #    c.name: str(getattr(row, c.name))
        #    for c in row.__table__.columns
        #}
        return {
            c.key: getattr(row, c.key)
            for c in inspect(row).mapper.column_attrs
        }

    
    def output_for_cache(self, query_result, args = {}):
        output = {}
        if query_result is None:
            return output
        data = [self.row2dict(item) for item in query_result]
        if args["display_cache_data"] == "true":
            output["data"] = data
            output["generated"] = datetime.datetime.now()
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


    def set_election(self, election = None):
        election = current_app.config["ELECTION_DATE_OVERRIDE"]
        if election == None:
            # Get the newest set
            newest = 0
            for s in self.sources:
                newest = int(s) if int(s) > newest else newest

            newest_election = str(newest)
            election = newest_election
        election = election if election is not None and election != '' else newest_election
        #current_app.log.info('Set election to: %s' % election)
        return election


    def set_election_metadata(self):
        sources = self.read_sources()
        election = self.set_election()

        if election not in sources:
            return

        # Get metadata about election
        election_meta = sources[election]['meta'] if 'meta' in sources[election] else {}
        return election_meta


    def parse_election(self, source, election_meta = {}):

        # Ensure we have a valid parser for this type
        parser_method = getattr(self, "parser", None)
        if callable(parser_method):
            # Check if election has base_url
            source['url'] = election_meta['base_url'] + source['url'] if 'base_url' in election_meta else source['url']

            # Get data from URL
            try:
                # Ballot questions spreadsheet requires latin-1 encoding
                #rows = unicodecsv.reader(scraped.splitlines(), delimiter=';', quotechar='|', encoding='latin-1')
                response = urllib.request.urlopen(source['url'])
                lines = [l.decode('latin-1') for l in response.readlines()]
                rows = csv.reader(lines, delimiter=';')
                return rows
            except Exception as err:
                current_app.log.error('[%s] Error when trying to read URL and parse CSV: %s' % (source['type'], source['url']))
                raise


    def from_dict(self, data, new=False):
        for field in data:
            setattr(self, field, data[field])


    def slugify(self, orig):
        slug = str(orig.encode('ascii', 'ignore').lower())
        slug = re.sub(r'[^a-z0-9]+', '-', slug).strip('-')
        slug = re.sub(r'[-]+', '-', slug)
        return slug


    def post_processing(self, type):

        # Handle any supplemental data
        spreadsheet_rows = self.supplement_connect('supplemental_' + type)
        supplemented_rows = []
        insert_rows = {'action': 'insert', 'rows': []}
        update_rows = {'action': 'update', 'rows': []}
        delete_rows = {'action': 'delete', 'rows': []}
        meta_rows = {'action': 'meta', 'rows': []}

        if type == 'contests' or type == 'results':
            updated = {"key" : "updated", "value" : db.func.current_timestamp(), "type" : "int"}
            meta_rows['rows'].append(updated)

            if type == 'results':
                sql = text("select count(distinct contest_id) as contest_count from results")
                result_contests = db.session.execute(sql)
                result_contest_count = [row[0] for row in result_contests]
                contest_count = {"key" : "contests", "value" : int(result_contest_count[0]), "type" : "int"}
                meta_rows['rows'].append(contest_count)

                # Use the first state level race to get general number of precincts reporting
                state_contest_results = Contest.query.filter_by(county_id='88').first()
                if state_contest_results is not None:
                    precincts_reporting = {"key" : "precincts_reporting", "value" : state_contest_results.precincts_reporting, "type" : "int"}
                    total_effected_precincts = {"key" : "total_effected_precincts", "value" : state_contest_results.total_effected_precincts, "type" : "int"}
                    meta_rows['rows'].append(precincts_reporting)
                    meta_rows['rows'].append(total_effected_precincts)

            supplemented_rows.append(meta_rows)

        if spreadsheet_rows is None:
            return supplemented_rows

        # for each row in the spreadsheet
        for spreadsheet_row in spreadsheet_rows:
            current_app.log.info('spreadsheet row is: %s' % spreadsheet_row)
            supplement_row = self.supplement_row(spreadsheet_row)
            if 'rows' in supplement_row:
                #supplemented_rows.append(supplement_row)
                if supplement_row['action'] == 'insert' and supplement_row['rows'] not in insert_rows['rows']:
                    #insert_rows['rows'] = [*insert_rows['rows'], *supplement_row['rows']]
                    insert_rows['rows'] = list(set(insert_rows['rows'] + supplement_row['rows']))
                elif supplement_row['action'] == 'update' and supplement_row['rows'] not in update_rows['rows']:
                    #update_rows['rows'] = [*update_rows['rows'], *supplement_row['rows']]
                    update_rows['rows'] = list(set(update_rows['rows'] + supplement_row['rows']))
                elif supplement_row['action'] == 'delete' and supplement_row['rows'] not in delete_rows['rows']:
                    #delete_rows['rows'] = [*delete_rows['rows'], *supplement_row['rows']]
                    delete_rows['rows'] = list(set(insert_rows['rows'] + supplement_row['rows']))
        if insert_rows not in supplemented_rows:
            supplemented_rows.append(insert_rows)
        if update_rows not in supplemented_rows:
            supplemented_rows.append(update_rows)
        if delete_rows not in supplemented_rows:
            supplemented_rows.append(delete_rows)
        return supplemented_rows


    def supplement_connect(self, source):
        """
        Connect to supplemental source (Google spreadsheets) given set.
        """
        sources = self.read_sources()
        election = self.set_election()

        if election not in sources:
            current_app.log.error('Election missing in sources: %s' % election)
            return

        if source not in sources[election]:
            current_app.log.error('Source missing in the %s election: %s' % (election, source))
            return

        data = {}
        result_json = None
        result = {}

        s = sources[election][source]
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
                token_json = token_result.json()
                if "token" in token_json:
                    token = token_json["token"]
                    authorized_headers = {"Authorization": f"Bearer {token}"}
                    result = requests.get(f"{url}?spreadsheet_id={spreadsheet_id}&worksheet_keys={worksheet_id}&external_use_s3={parser_store_in_s3}&bypass_cache={parser_bypass_cache}", headers=authorized_headers)
                    result_json = result.json()
                else:
                    current_app.log.error('Error in authorize. Token result is: %s' % token_json)
                    result_json = None
        if result_json is not None and worksheet_id in result_json:
            data["rows"] = result_json[worksheet_id]

            # set metadata and send the customized json output to the api
            if "generated" in result_json:
                data["generated"] = result_json["generated"]
            data["customized"] = datetime.datetime.now()
            if cache_timeout != 0:
                data["cache_timeout"] = data["customized"] + timedelta(seconds=int(cache_timeout))
            else:
                data["cache_timeout"] = 0
            output = json.dumps(data, default=str)
            #output = json.dumps(data) # since this is from the spreadsheet maybe it doesn't need to be a string.
            
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

            if result_json is not None:
                #output = json.dumps(result_json, default=str)
                #current_app.log.info('final data from sheet: %s' % (result_json))
                result = result_json["rows"]

        return result


class Area(ScraperModel, db.Model):

    __tablename__ = "areas"

    id = db.Column(db.String(255), primary_key=True, autoincrement=False, nullable=False)
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
    updated = db.Column(db.DateTime, default=db.func.current_timestamp(), onupdate=db.func.current_timestamp())

    def __init__(self, **kwargs):
        self.id = kwargs.get('id')
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

    def parser(self, row, group):

        # General data
        parsed = {
            'area_id': group + '-',
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
            parsed['name'] = row[1]

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

        parsed['id'] = parsed['area_id']

        return parsed

class Contest(ScraperModel, db.Model):

    __tablename__ = "contests"

    #list of county names
    mn_counties = ["Aitkin", "Anoka", "Becker", "Beltrami", "Benton", "Big Stone", "Blue Earth", "Brown", "Carlton", "Carver", "Cass", "Chippewa", "Chisago", "Clay", "Clearwater", "Cook", "Cottonwood", "Crow Wing", "Dakota", "Dodge", "Douglas", "Faribault", "Fillmore", "Freeborn", "Goodhue", "Grant", "Hennepin", "Houston", "Hubbard", "Isanti", "Itasca", "Jackson", "Kanabec", "Kandiyohi", "Kittson", "Koochiching", "Lac qui Parle", "Lake", "Lake of the Woods", "Le Sueur", "Lincoln", "Lyon", "McLeod", "Mahnomen", "Marshall", "Martin", "Meeker", "Mille Lacs", "Morrison", "Mower", "Murray", "Nicollet", "Nobles", "Norman", "Olmsted", "Otter Tail", "Pennington", "Pine", "Pipestone", "Polk", "Pope", "Ramsey", "Red Lake", "Redwood", "Renville", "Rice", "Rock", "Roseau", "Saint Louis", "Scott", "Sherburne", "Sibley", "Stearns", "Steele", "Stevens", "Swift", "Todd", "Traverse", "Wabasha", "Wadena", "Waseca", "Washington", "Watonwan", "Wilkin", "Winona", "Wright", "Yellow Medicine"]

    # Track which boundary sets we use
    found_boundary_types = []

    id = db.Column(db.String(255), primary_key=True, autoincrement=False, nullable=False)
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
    boundary = db.Column(db.String(255))
    partisan = db.Column(db.Boolean())
    question_body = db.Column(db.Text)
    sub_title = db.Column(db.String(255))
    incumbent_party = db.Column(db.String(255))
    called = db.Column(db.Boolean())
    updated = db.Column(db.DateTime, default=db.func.current_timestamp(), onupdate=db.func.current_timestamp())

    results = db.relationship('Result', backref=__tablename__, lazy=True)

    def __init__(self, **kwargs):
        self.result_id = kwargs.get('result_id')
        self.id = kwargs.get('id')
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
        self.called = kwargs.get('called')
    
    def __repr__(self):
        return '<Contest {}>'.format(self.id)

    def parser(self, row, group, source):
        """
        Parser for contest scraping.
        """

        election_meta = self.set_election_metadata()

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
        primary = election_meta['primary'] if 'primary' in election_meta else False

        re_question = re.compile(r'.*question.*', re.IGNORECASE)
        matched_question = re_question.match(row[4])
        primary = False if matched_question is not None else primary
        
        office_name = row[4]
        county_id = row[1]
        title = self.generate_title(office_name, county_id, row)

        parsed = {
            'id': contest_id,
            'contest_id': contest_id,
            'office_id': office_id,
            'results_group': group,
            'office_name': office_name,
            'district_code': row[5],
            'state': row[0],
            'county_id': county_id,
            'precinct_id': row[2],
            'precincts_reporting': int(row[11]),
            'total_effected_precincts': int(row[12]),
            'total_votes_for_office': int(row[15]),
            'seats': int(matched_seats.group(1)) if matched_seats is not None else 1,
            'ranked_choice': ranked_choice is not None,
            'primary': primary,
            'scope': source['contest_scope'] if 'contest_scope' in source else None,
            'title': title
        }

        # set fields that aren't directly in the data
        parsed['boundary'] = self.find_boundary(parsed, row)
        parsed = self.set_question_fields(parsed)
        parsed['partisan'] = self.set_partisanship(parsed)
        parsed['seats'] = self.set_seats(parsed)

        # Return contest record
        return parsed

    def generate_title(self, office_name, county_id, row):
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

        return title


    def find_boundary(self, parsed_row, row):
        boundary = ''
        boundary_type = False

        # State level race
        if parsed_row['scope'] == 'state':
            boundary = 'minnesota-state-2014/27-1'
            boundary_type = 'minnesota-state-2014'

        # US House districts
        if parsed_row['scope'] == 'us_house':
            us_house_match = re.compile(r'.*U.S. Representative District ([0-9]+).*', re.IGNORECASE).match(parsed_row['office_name'])
            if us_house_match is not None:
                boundary = 'congressional-districts-2012/' + us_house_match.group(1)
                boundary_type = 'congressional-districts-2012'
            #special presidential primary handling
            elif parsed_row['office_name'] == "U.S. Presidential Nominee":
                boundary = 'congressional-districts-2012/' + parsed_row['district_code']
                boundary_type = 'congressional-districts-2012'
            else:
                current_app.log.info('[%s] Could not find US House boundary for: %s' % ('results', parsed_row['office_name']))

        # State Senate districts
        if parsed_row['scope'] == 'state_senate':
            state_senate_match = re.compile(r'.*State Senator District (\w+).*', re.IGNORECASE).match(parsed_row['office_name'])
            if state_senate_match is not None:
                boundary = 'state-senate-districts-2012/' + "%02d" % (int(state_senate_match.group(1)),)
                boundary_type = 'state-senate-districts-2012'
            else:
                current_app.log.info('[%s] Could not find State Senate boundary for: %s' % ('results', parsed_row['office_name']))

        # State House districts
        if parsed_row['scope'] == 'state_house':
            state_house_match = re.compile(r'.*State Representative District (\w+).*', re.IGNORECASE).match(parsed_row['office_name'])
            if state_house_match is not None:
                district_number = "%02d" % (int(state_house_match.group(1)[0:-1]),)
                district_letter = state_house_match.group(1)[-1].lower()
                boundary = 'state-house-districts-2012/' + district_number + district_letter
                boundary_type = 'state-house-districts-2012'
            else:
                current_app.log.info('[%s] Could not find State House boundary for: %s' % ('results', parsed_row['office_name']))

        # State court districts.    Judge - 7th District Court 27
        if parsed_row['scope'] == 'district_court':
            court_match = re.compile(r'.*Judge - ([0-9]+).*', re.IGNORECASE).match(parsed_row['office_name'])
            if court_match is not None:
                boundary = 'district-courts-2012/' + court_match.group(1).lower() + '-1'
                boundary_type = 'district-courts-2012'
            else:
                current_app.log.info('[%s] Could not find State District Court boundary for: %s' % ('results', parsed_row['office_name']))

        # School district is in the office name. Special school district for
        # Minneapolis is "1-1". Unfortunately SSD1 and ISD1 are essentially the
        # same as far as the incoming data so we have to look at title.
        #
        # Minneapolis
        # sub-school districts are the same at the Minneapolis Park and Rec
        # districts. There are a number of sub-school districts it looks
        # like

        if parsed_row['scope'] == 'school':
            isd_match = re.compile(r'.*\(ISD #([0-9]+)\).*', re.IGNORECASE).match(parsed_row['office_name'])
            csd_match = re.compile(r'.*\( ?CSD +#([0-9]+)\).*', re.IGNORECASE).match(parsed_row['office_name'])
            ssd_match = re.compile(r'.*\(SSD #([0-9]+)\).*', re.IGNORECASE).match(parsed_row['office_name'])
            district_match = re.compile(r'.*district ([0-9]+) \(.*', re.IGNORECASE).match(parsed_row['office_name'])

            if isd_match is not None:
                isd_match_value = isd_match.group(1)

                boundary =  'school-districts-2018/' + "%04d" % (int(isd_match_value)) + "-1"
                boundary_type = 'school-districts-2018'

            elif csd_match is not None:
                csd_match_value = csd_match.group(1)

                boundary = 'school-districts-2018/' + "%04d" % (int(csd_match_value)) + "-1"
                boundary_type = 'school-districts-2018'

            elif ssd_match is not None:
                ssd_match_value = '1-3' if ssd_match.group(1) == '1' else ssd_match.group(1)

                if ssd_match_value == '1-3' and district_match is not None:
                    boundary =  'minneapolis-parks-and-recreation-districts-2014/' + district_match.group(1) + "-1"
                    boundary_type = 'minneapolis-parks-and-recreation-districts-2014'
                elif ssd_match_value == '1-3' and district_match is None: #Minneapolis at-large seats
                    boundary = 'minor-civil-divisions-2010/2705343000'
                    boundary_type = 'minor-civil-divisions-2010'
                else:
                    boundary = 'school-districts-2018/' + "%04d" % (int(ssd_match_value)) + "-1"
                    boundary_type = 'school-districts-2018'
            else:
                current_app.log.info('[%s] Could not find (I|S)SD boundary for: %s' % ('results', parsed_row['office_name']))

        # County should be provided, but the results also have results for county
        # comissioner which are sub-county boundaries
        if parsed_row['scope'] == 'county':
            comissioner_match = re.compile(r'.*County Commissioner District.*', re.IGNORECASE).match(parsed_row['office_name'])
            park_commissioner_match = re.compile(r'.*Park Commissioner District.*', re.IGNORECASE).match(parsed_row['office_name'])
            if comissioner_match is not None:
                boundary = 'county-commissioner-districts-2012/%s-%02d-1' % (int(parsed_row['county_id']),    int(parsed_row['district_code']))
                boundary_type = 'county-commissioner-districts-2012'
            elif park_commissioner_match is not None:
                boundary = "" #We don't currently have shapefiles for county park commissioner districts
                boundary_type = "county-park-commissioner-district"
            else:
                boundary = 'counties-2010/%s-1' % int(parsed_row['county_id'])
                boundary_type = 'counties-2010'

        # This includes both municipal (city) level results, plus sub-municpal, such
        # as city council results.
        #
        # For municpal results.    The boundary code is SSCCCMMMM where:
        #     * SS is state ID which is 27
        #     * CCC is the county FIPS code which is the MN County Code * 2 - 1
        #     * MMMM is the municpal code
        # The main issue is getting the county code which is not included in the
        # results but instead in a separate table.
        #
        # It also turns out that there cities, like White Bear Lake City
        # which is in multiple counties which means they have more than one
        # boundary.
        #
        # For the sub-municipal results, we need wards. Unfortunately the boundary
        # id for wards is the actual name of the city and the ward number due to the
        # face that the original boundary data did not have mcd codes in it.
        #
        # There are also minneapolis park and recs commissioner which is its own
        # thing.
        #
        # And there is also just wrong data occasionally.
        if parsed_row['scope'] == 'municipal':
            # Checks
            wards_matched = re.compile(r'.*(Council Member Ward|Council Member District) ([0-9]+).*\((((?!elect).)*)\).*', re.IGNORECASE).match(parsed_row['office_name'])
            mpls_parks_matched = re.compile(r'.*Park and Recreation Commissioner District ([0-9]+).*', re.IGNORECASE).match(parsed_row['office_name'])

            # Check for sub municipal parts first
            if wards_matched is not None:
                boundary = 'wards-2012/' + self.slugify(wards_matched.group(3)) + '-w-' + '{0:02d}'.format(int(wards_matched.group(2))) + '-1'
                boundary_type = 'wards-2012'
            elif mpls_parks_matched is not None:
                boundary = 'minneapolis-parks-and-recreation-districts-2014/' + mpls_parks_matched.group(1)
                boundary_type = 'minneapolis-parks-and-recreation-districts-2014'
            else:
                if parsed_row['county_id']:
                    boundary = self.boundary_make_mcd(parsed_row['county_id'], parsed_row['district_code'])
                    boundary_type = 'minor-civil-divisions-2010'
                else:
                    boundary_type = 'minor-civil-divisions-2010'
                    mcd = self.check_mcd(parsed_row)
                    if mcd != []:
                        boundaries = []
                        for r in mcd:
                            boundaries.append(self.boundary_make_mcd(r.county_id, parsed_row['district_code']))
                        boundary = ','.join(boundaries)
                    else:
                        current_app.log.info('[%s] Could not find corresponding county for municipality: %s' % ('results', parsed_row['office_name']))

        # Hospital districts.
        #
        # Mostly, the district ID provided is for the best municipal
        # entity.    The only way to really figure out the hospital district ID
        # (which is kind of arbitrary) is to use the boundary service
        #
        # Otherwise, the actual hospital id is given
        if parsed_row['scope'] == 'hospital':
            # MCD districts are 5 digits with leading zeros, while hospital districts
            # are 3 or 4
            if len(parsed_row['district_code']) < 5:
                boundary = 'hospital-districts-2012/%s-1' % (int(parsed_row['district_code']))
                boundary_type = 'hospital-districts-2012'
            else:
                # We need the county ID and it is not in results, so we have to look
                # it up, and there could more than one
                mcd = self.check_mcd(parsed_row)
                if mcd != []:
                    for r in mcd:
                        # Find intersection
                        mcd_boundary_id = self.boundary_make_mcd(r.county_id, parsed_row['district_code'])
                        boundary_url  = 'https://represent-minnesota.herokuapp.com/boundaries/?sets=%s,%s'
                        request = requests.get(boundary_url % ('hospital-districts-2012', mcd_boundary_id), verify = True)

                        if request.status_code == 200:
                            r = request.json()
                            boundary = r['objects'][0]['url']
                            boundary = boundary.lstrip('/boundaries/') # remove if this is present
                            boundary = boundary.rstrip('/')
                            break

                    if boundary == '':
                        current_app.log.info('[%s] Hospital boundary intersection not found: %s' % ('results', parsed_row['title']))

                else:
                    current_app.log.info('[%s] Could not find corresponding county for municpality: %s' % ('results', parsed_row['office_name']))


        # Add to types
        if boundary_type != False and boundary_type not in self.found_boundary_types:
            self.found_boundary_types.append(boundary_type)

        # General notice if not found
        if boundary == '':
            current_app.log.info('[%s] Could not find boundary for: %s' % ('results', parsed_row['office_name']))

        return boundary

    
    def boundary_make_mcd(self, county_id, district):
        """
        Makes MCD code from values.
        """
        bad_data = {
            '2713702872': '2713702890', # Aurora City
            '2703909154': '2710909154', # Bryon
            '2706109316': '2706103916', # Calumet
            '2716345952': '2716358900', # Scandia
            '2702353296': '2706753296'  # Raymond
        }
        fips = '{0:03d}'.format((int(county_id) * 2) - 1)
        mcd_id = '27' + fips + district
        if mcd_id in bad_data:
            mcd_id = bad_data[mcd_id]
        return 'minor-civil-divisions-2010/' + mcd_id


    def check_mcd(self, parsed_row):
        mcd = Area.query.filter_by(areas_group='municipalities', mcd_id=parsed_row['district_code']).all()
        return mcd

    
    def set_question_fields(self, parsed_row):
        # Get question data
        try:
            questions = Question.query.all()
        except:
            questions = []
        
        # Check if there is a question match for the contest
        for q in questions:
            if q.contest_id == parsed_row['contest_id']:
                parsed_row['question_body'] = q.question_body
                parsed_row['sub_title'] = q.sub_title

        return parsed_row
    
    
    def set_partisanship(self, parsed_row):
        # Determine partisanship for contests for other processing. We need to look
        # at all the candidates to know if the contest is nonpartisan or not.
        #results = db.engine.execute("select result_id from results where contest_id = '%s' and party_id not in ('%s')" % (parsed_row['contest_id'], "', '".join(self.nonpartisan_parties)))
        #results = Result.query.filter_by(contest_id=parsed_row['contest_id'], user_location=where)
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
    
    
    def supplement_row(self, spreadsheet_row):

        # parse/format the row
        spreadsheet_row = self.set_db_fields_from_spreadsheet(spreadsheet_row)

        supplemented_row = {}

        # Check for existing contest rows
        results = Contest.query.filter_by(id=spreadsheet_row['id']).all()

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

    # this handles the key names and value formats for the databse in the event that they are different in the spreadsheet
    def set_db_fields_from_spreadsheet(self, spreadsheet_row):
        spreadsheet_row['id'] = str(spreadsheet_row['id'])
        spreadsheet_row['incumbent_party'] = spreadsheet_row.get('incumbent.party', "")
        spreadsheet_row['question_help'] = spreadsheet_row.get('question.help', "")
        spreadsheet_row['question_body'] = spreadsheet_row.get('question.body', "")
        spreadsheet_row['precincts_reporting'] = spreadsheet_row.get('precincts.reporting', 0)
        return spreadsheet_row

class Meta(ScraperModel, db.Model):

    __tablename__ = "meta"

    key = db.Column(db.String(255), primary_key=True, autoincrement=False, nullable=False)
    value = db.Column(db.Text)
    type = db.Column(db.String(255))
    updated = db.Column(db.DateTime, default=db.func.current_timestamp(), onupdate=db.func.current_timestamp())

    def __init__(self, **kwargs):
        super(Meta, self).__init__(**kwargs)
    
    def __repr__(self):
        return '<Meta {}>'.format(self.key)

    def output_for_cache(self, query_result):
        output = {}
        if query_result is None:
            return output
        output["data"] = {}
        for query_item in query_result:
            itemValues = self.row2dict(query_item)
            output["data"][itemValues["key"]] = itemValues["value"]
        output["generated"] = datetime.datetime.now()
        return output

    def parser(self, key, row):
        """
        Parser for meta scraping.
        """

        parsed = {
            'key': key,
            'value': row,
            'type': type(row).__name__
        }
        return parsed


class Question(ScraperModel, db.Model):

    __tablename__ = "questions"

    id = db.Column(db.String(255), primary_key=True, autoincrement=False, nullable=False)
    contest_id = db.Column(db.String(255))
    title = db.Column(db.String(255))
    sub_title = db.Column(db.String(255))
    question_body = db.Column(db.Text)
    updated = db.Column(db.DateTime, default=db.func.current_timestamp(), onupdate=db.func.current_timestamp())

    def __init__(self, **kwargs):
        super(Question, self).__init__(**kwargs)
    
    def __repr__(self):
        return '<Question {}>'.format(self.id)

    def parser(self, row, group):

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
            'title': row[4],
            'sub_title': row[5].title(),
            'question_body': question_body
        }

        return parsed


class Result(ScraperModel, db.Model):

    __tablename__ = "results"

    id = db.Column(db.String(255), primary_key=True, autoincrement=False, nullable=False)
    #contest_id = db.Column(db.String(255))
    contest_id = db.Column(db.String(255), db.ForeignKey('contests.id'), nullable=False)
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
    updated = db.Column(db.DateTime, default=db.func.current_timestamp(), onupdate=db.func.current_timestamp())

    def __init__(self, **kwargs):
        self.id = kwargs.get('id')
        self.contest_id = kwargs.get('contest_id')
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

    def parser(self, row, group):
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

        # Return results record for the database
        return parsed

    def supplement_row(self, spreadsheet_row):

        if isinstance(spreadsheet_row, (bytes, bytearray)):
            spreadsheet_row = json.loads(spreadsheet_row)

        # parse/format the row
        spreadsheet_row = self.set_db_fields_from_spreadsheet(spreadsheet_row)
        supplemented_row = {}

        # Check for existing result rows
        results = Result.query.filter_by(id=spreadsheet_row['id']).all()

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
                        matching_result.ranked_choice_place = spreadsheet_row['ranked_choice_place'],
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
                current_app.log.info('row to insert: %s' % spreadsheet_row)
                insert_rows = []
                # Add new row, make sure to mark the row as supplemental
                spreadsheet_row['results_group'] = 'supplemental_results'
                insert_result = spreadsheet_row
                result_model = Result(**insert_result)
                if result_model not in insert_rows:
                    insert_rows.append(result_model)
                row_result = {
                    'action': 'insert',
                    'rows': insert_rows
                }
                current_app.log.info('insert row result: %s' % row_result)
                supplemented_row = row_result

        return supplemented_row

    # this handles the key names and value formats for the databse in the event that they are different in the spreadsheet
    def set_db_fields_from_spreadsheet(self, spreadsheet_row):
        spreadsheet_row['id'] = str(spreadsheet_row['id'])
        spreadsheet_row['contest_id'] = spreadsheet_row.get('contest.id', None)
        spreadsheet_row['candidate_id'] = spreadsheet_row.get('candidate.id', None)
        spreadsheet_row['office_name'] = spreadsheet_row.get('office.name', None)
        spreadsheet_row['percentage'] = spreadsheet_row.get('percentage', None)
        spreadsheet_row['votes_candidate'] = spreadsheet_row.get('votes.candidate', 0)
        spreadsheet_row['ranked_choice_place'] = spreadsheet_row.get('ranked.choice.place', None)
        spreadsheet_row['enabled'] = bool(spreadsheet_row.get('enabled', False))
        return spreadsheet_row