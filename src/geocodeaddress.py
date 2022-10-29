import json
import geocoder
import pytz
import datetime
from flask import current_app
from src.storage import Storage

class GeocodeAddress(object):

    def __init__(self):
        self.key = current_app.config["GEOCODER_MAPQUEST_KEY"]


    def output_for_cache(self, geocode_result, args = {}):
        output = {}
        if geocode_result is None:
            return output
        output["data"] = geocode_result
        if "display_cache_data" in args and args["display_cache_data"] == "true":
            output["generated"] = datetime.datetime.now(pytz.timezone(current_app.config["TIMEZONE"]))
        return output


    def get_geocoded_address(self, address = ''):
        storage_args = {
            'cache_timeout': 2592000, # 1 month
            'create_log_entries': "false"
        }
        storage = Storage(json.dumps(storage_args), "POST")
        geocode_value = ""
        cached_output = storage.get(address)
        if cached_output is not None:
            geocode_result = json.loads(cached_output)
            if isinstance(geocode_result, dict):
                geocode_value = geocode_result["data"]
            else:
                geocode_value = ""
        else:
            geocode_result = self.geocode(address)
            if geocode_result != "":
                geocode_output = self.output_for_cache(geocode_result)
                geocode_data   = storage.save(address, geocode_output, None, None)
                geocode_data   = json.loads(geocode_data)
                geocode_value  = geocode_data["data"]
        return geocode_value


    def geocode(self, address):
        try:
            g = geocoder.mapquest(address, key=self.key)
        except Exception as e:
            current_app.log.error('Error geocoding: %s' % e)
            return ""
        return g.json
