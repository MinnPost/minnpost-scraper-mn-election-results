import json
import re
import requests
import pytz
import datetime
from slugify import slugify
from flask import current_app
from src.storage import Storage

class Boundaries(object):

    def __init__(self, Area = None):
        self.area = Area
        self.found_boundary_types = []


    def output_for_cache(self, boundary_value, args = {}):
        output = {}
        if boundary_value is None:
            return output
        output["data"] = boundary_value
        if "display_cache_data" in args and args["display_cache_data"] == "true":
            output["generated"] = datetime.datetime.now(pytz.timezone(current_app.config["TIMEZONE"]))
        return output


    # todo: fix this so it can be election specific
    def find_boundary(self, parsed_row):
        boundary = ''
        boundary_type = False
        election_id = parsed_row['election_id']

        # State level race
        if parsed_row['scope'] == 'state':
            boundary = 'minnesota-state-2014/27-1'
            boundary_type = 'minnesota-state-2014'

        # US House districts
        if parsed_row['scope'] == 'us_house':
            us_house_match = re.compile(r'.*U.S. Representative District ([0-9]+).*', re.IGNORECASE).match(parsed_row['office_name'])
            if us_house_match is not None:
                boundary_slug = 'congressional-districts-2022/' + us_house_match.group(1)
                boundary_type = 'congressional-districts-2022'
                boundary = self.get_boundary_by_query(boundary_slug)
            #special presidential primary handling
            elif parsed_row['office_name'] == "U.S. Presidential Nominee":
                boundary_slug = 'congressional-districts-2022/' + parsed_row['district_code']
                boundary_type = 'congressional-districts-2022'
                boundary = self.get_boundary_by_query(boundary_slug)
            else:
                current_app.log.info('[%s] Could not find US House boundary for: %s' % ('results', parsed_row['office_name']))

        # State Senate districts
        if parsed_row['scope'] == 'state_senate':
            state_senate_match = re.compile(r'.*State Senator District (\w+).*', re.IGNORECASE).match(parsed_row['office_name'])
            if state_senate_match is not None:
                boundary_slug = 'state-senate-districts-2022/' + "%02d" % (int(state_senate_match.group(1)),)
                boundary_type = 'state-senate-districts-2022'
                boundary = self.get_boundary_by_query(boundary_slug)
            else:
                current_app.log.info('[%s] Could not find State Senate boundary for: %s' % ('results', parsed_row['office_name']))

        # State House districts
        if parsed_row['scope'] == 'state_house':
            state_house_match = re.compile(r'.*State Representative District (\w+).*', re.IGNORECASE).match(parsed_row['office_name'])
            if state_house_match is not None:
                district_number = "%02d" % (int(state_house_match.group(1)[0:-1]),)
                district_letter = state_house_match.group(1)[-1].lower()
                boundary_slug = 'state-house-districts-2022/' + district_number + district_letter
                boundary_type = 'state-house-districts-2022'
                boundary = self.get_boundary_by_query(boundary_slug)
            else:
                current_app.log.info('[%s] Could not find State House boundary for: %s' % ('results', parsed_row['office_name']))

        # State court districts.    Judge - 7th District Court 27
        if parsed_row['scope'] == 'district_court':
            court_match = re.compile(r'.*Judge - ([0-9]+).*', re.IGNORECASE).match(parsed_row['office_name'])
            if court_match is not None:
                boundary_slug = 'district-courts-2012/' + court_match.group(1).lower() + '-1'
                boundary_type = 'district-courts-2012'
                boundary = self.get_boundary_by_query(boundary_slug)
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

                boundary_slug =  'school-districts-2021/' + "%04d" % (int(isd_match_value)) + "-1"
                boundary_type = 'school-districts-2021'
                boundary = self.get_boundary_by_query(boundary_slug)

            elif csd_match is not None:
                csd_match_value = csd_match.group(1)

                boundary_slug = 'school-districts-2021/' + "%04d" % (int(csd_match_value)) + "-1"
                boundary_type = 'school-districts-2021'
                boundary = self.get_boundary_by_query(boundary_slug)

            elif ssd_match is not None:
                ssd_match_value = '1-3' if ssd_match.group(1) == '1' else ssd_match.group(1)

                if ssd_match_value == '1-3' and district_match is not None:
                    boundary_slug = 'minneapolis-park-and-recreation-board-districts-2022/' + district_match.group(1) + "-1"
                    boundary_type = 'minneapolis-park-and-recreation-board-districts-2022'
                    boundary = self.get_boundary_by_query(boundary_slug)
                elif ssd_match_value == '1-3' and district_match is None: #Minneapolis at-large seats
                    boundary_slug = 'minor-civil-divisions-2020/2705343000'
                    boundary_type = 'minor-civil-divisions-2020'
                    boundary = self.get_boundary_by_query(boundary_slug)
                else:
                    boundary_slug = 'school-districts-2021/' + "%04d" % (int(ssd_match_value)) + "-1"
                    boundary_type = 'school-districts-2021'
                    boundary = self.get_boundary_by_query(boundary_slug)
            else:
                current_app.log.info('[%s] Could not find (I|S)SD boundary for: %s' % ('results', parsed_row['office_name']))

        # County should be provided, but the results also have results for county
        # comissioner which are sub-county boundaries
        if parsed_row['scope'] == 'county':
            comissioner_match = re.compile(r'.*County Commissioner District.*', re.IGNORECASE).match(parsed_row['office_name'])
            park_commissioner_match = re.compile(r'.*Park Commissioner District.*', re.IGNORECASE).match(parsed_row['office_name'])
            if comissioner_match is not None:
                boundary_slug = 'county-commissioner-districts-2012/%s-%02d-1' % (int(parsed_row['county_id']),    int(parsed_row['district_code']))
                boundary_type = 'county-commissioner-districts-2012'
                boundary = self.get_boundary_by_query(boundary_slug)
            elif park_commissioner_match is not None:
                boundary_slug = "" #We don't currently have shapefiles for county park commissioner districts
                boundary_type = "county-park-commissioner-district"
                boundary = self.get_boundary_by_query(boundary_slug)
            else:
                boundary_slug = 'counties-2020/%s-1' % int(parsed_row['county_id'])
                boundary_type = 'counties-2020'
                boundary = self.get_boundary_by_query(boundary_slug)

        # This includes both municipal (city) level results, plus sub-municipal, such
        # as city council results.
        #
        # For municipal results.    The boundary code is SSCCCMMMM where:
        #     * SS is state ID which is 27
        #     * CCC is the county FIPS code which is the MN County Code * 2 - 1 (ex Hennepin county is 53, which is 27 * 2 - 1)
        #     * MMMM is the municpal code
        # The main issue is getting the county code which is not included in the
        # results but instead in a separate table.
        #
        # It also turns out that there are cities, like White Bear Lake City
        # which is in multiple counties which means they have more than one
        # boundary.
        #
        # For the sub-municipal results, we need wards. Unfortunately the boundary
        # id for wards is the actual name of the city and the ward number due to the
        # fact that the original boundary data did not have mcd codes in it.
        #
        # There are also minneapolis park and recs commissioner which is its own
        # thing.
        #
        # And there is also just wrong data occasionally.
        if parsed_row['scope'] == 'municipal':
            # Checks
            wards_matched = re.compile(r'.*(Council Member Ward|Council Member District) ([0-9]+).*\((((?!elect).)*)\).*', re.IGNORECASE).match(parsed_row['office_name'])
            mpls_parks_matched = re.compile(r'.*Park and Recreation Commissioner District ([0-9]+).*', re.IGNORECASE).match(parsed_row['office_name'])

            # Check for sub municipal parts first. These are not upgraded.
            if wards_matched is not None:
                boundary_slug = 'wards-2012/' + slugify(wards_matched.group(3), to_lower=True) + '-w-' + '{0:02d}'.format(int(wards_matched.group(2))) + '-1'
                boundary_type = 'wards-2012'
                boundary = self.get_boundary_by_query(boundary_slug)
            elif mpls_parks_matched is not None:
                boundary_slug = 'minneapolis-park-and-recreation-board-districts-2022/' + mpls_parks_matched.group(1)
                boundary_type = 'minneapolis-park-and-recreation-board-districts-2022'
                boundary = self.get_boundary_by_query(boundary_slug)
            else:
                # these should be upgraded to 2020+ version.
                if parsed_row['county_id']:
                    boundary = self.boundary_make_mcd(parsed_row['county_id'], parsed_row['district_code'], election_id)
                    if boundary != "":
                        boundary_type = 'minor-civil-divisions-2020'
                else:
                    boundary_type = 'minor-civil-divisions-2020'
                    mcd_area = self.check_mcd(parsed_row)
                    if mcd_area != []:
                        boundaries = []
                        for area in mcd_area:
                            if area.county_id:
                                boundary = self.boundary_make_mcd(area.county_id, parsed_row['district_code'], election_id)
                                if boundary != "":
                                    boundaries.append(boundary)
                        if len(boundaries) > 1:
                            boundary = ','.join(boundaries)
                        else:
                            boundary = boundaries[0]
                    else:
                        current_app.log.debug('[%s] Could not find corresponding county for municipality: %s. County id was %s and district code was %s' % ('results', parsed_row['office_name'], parsed_row['county_id'], parsed_row['district_code']))

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
                boundary_slug = 'hospital-districts-2012/%s-1' % (int(parsed_row['district_code']))
                boundary_type = 'hospital-districts-2012'
                boundary = self.get_boundary_by_query(boundary_slug)
            else:
                # We need the county ID and it is not in results, so we have to look
                # it up, and there could more than one
                mcd_area = self.check_mcd(parsed_row)
                if mcd_area != []:
                    for area in mcd_area:
                        # Find intersection
                        boundary_url = self.boundary_make_mcd(area.county_id, parsed_row['district_code'], election_id)
                        if boundary_url != "":
                            boundary = boundary_url
                            break
                    if boundary == '':
                        current_app.log.info('[%s] Hospital boundary intersection not found: %s' % ('results', parsed_row['title']))

                else:
                    current_app.log.info('[%s] Could not find corresponding county for municipality: %s' % ('results', parsed_row['office_name']))


        # Add to types
        if boundary_type != False and boundary_type not in self.found_boundary_types:
            self.found_boundary_types.append(boundary_type)

        # General notice if not found
        if boundary == '':
            current_app.log.info('[%s] Could not find boundary for: %s' % ('results', parsed_row['office_name']))

        return boundary


    def boundary_make_mcd(self, county_id, district, election_id = None):
        """Makes MCD code from values.
        """
        bad_data = {
            '2713702872': '271372394032', # Aurora City
            '2703909154': '271092393488', # Bryon
            '2706109316': '270612393500', # Calumet
            '2716345952': '27097665565', # Scandia
            '2702353296': '270672396325'  # Raymond
        }
        fips = '{0:03d}'.format((int(county_id) * 2) - 1)
        mcd_id = '27' + fips + district
        if mcd_id in bad_data:
            mcd_id = bad_data[mcd_id]
        boundary_url = self.get_boundary_by_query(None, 'name', mcd_id, None, election_id)
        return boundary_url


    def check_mcd(self, parsed_row):
        mcd = self.area.query.filter_by(areas_group='municipalities', mcd_id=parsed_row['district_code'], election_id=parsed_row['election_id']).all()
        return mcd

    
    def get_boundary_by_query(self, slug = None, key = None, value = None, intersect = None, election_id = None):
        storage_args = {
            'cache_timeout': 604800 # 1 week
        }
        storage = Storage(json.dumps(storage_args), "POST")
        boundary_domain = current_app.config['BOUNDARY_SERVICE_URL']
        request_url = boundary_domain + '/boundaries/%s';
        boundary_value = ""
        if slug is not None:
            request_url  = request_url % slug
        elif key == 'sets' and intersect is not None:
            request_url  = request_url % '?%s=%s,%s'
            request_url  = request_url % (key, value, intersect)
        else:
            request_url  = request_url % '?%s=%s'
            request_url  = request_url % (key, value)
        cached_output = storage.get(request_url)
        if cached_output is not None:
            boundary_data   = json.loads(cached_output)
            boundary_value  = boundary_data["data"]
        else:
            boundary_url = ""
            request      = requests.get(request_url, verify = False)
            if request.status_code == 200:
                if slug is not None:
                    boundary_url = request_url
                else:
                    r = request.json()
                    boundary_url   = r['objects'][0]['url']
                    boundary_url   = boundary_domain + boundary_url
                    verify_request = requests.get(boundary_url, verify = False)
                    if verify_request.status_code != 200:
                        boundary_url = ""
            if boundary_url != "":
                boundary_value = boundary_url.replace(boundary_domain + '/boundaries/', "")
                boundary_value = boundary_value.rstrip('/')
                boundary_output = self.output_for_cache(boundary_value)
                boundary_data   = storage.save(request_url, boundary_output, None, election_id)
                boundary_data   = json.loads(boundary_data)
                boundary_value  = boundary_data["data"]
        return boundary_value
