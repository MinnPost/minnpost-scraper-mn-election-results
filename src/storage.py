import hashlib
import json
import datetime
from datetime import timedelta
from src.extensions import cache
from flask import current_app

class Storage(object):

    def __init__(self, args = {}, http_method = "GET"):
        self.args = args
        self.http_method = http_method
        if self.http_method != "GET":
            self.args = json.loads(args)
        self.class_to_use = self.check_method()


    def check_method(self):
        class_to_use = CacheStorage(self.args, self.http_method)
        return class_to_use


    def save(self, key, data, group = None):
        try:
            data = json.loads(data)
        except:
            data = data
        class_to_use = self.class_to_use
        output = class_to_use.save(key, data, group)
        return output


    def get(self, key):
        class_to_use = self.class_to_use
        output = class_to_use.get(key)
        return output


    def clear(self, key_list_name):
        class_to_use = self.class_to_use
        output = class_to_use.clear(key_list_name)
        return output


class CacheStorage(object):

    def __init__(self, args = {}, http_method = "GET"):
        self.cache_timeout = current_app.config["CACHE_DEFAULT_TIMEOUT"]
        if http_method == "GET":
            self.bypass_cache = args.get("bypass_cache", "false")
            self.delete_cache = args.get("delete_cache", "false")
            self.cache_data = args.get("cache_data", "true")
            self.cache_timeout = int(args.get("cache_timeout", self.cache_timeout))
        else:
            self.bypass_cache = "false"
            self.delete_cache = "false"
            self.cache_data = "true"
            if "bypass_cache" in args:
                self.bypass_cache = args["bypass_cache"]
            if "delete_cache" in args:
                self.delete_cache = args["delete_cache"]
            if "cache_data" in args:
                self.cache_data = args["cache_data"]


    def save(self, key, data, group = None):
        hash_cache_key = hashlib.md5((key).encode('utf-8')).hexdigest()
        if self.cache_data == "true":
            if self.cache_timeout is not None and self.cache_timeout != 0:
                if "generated" in data:
                    if type(data["generated"]) == str:
                        data["generated"] = datetime.datetime.fromisoformat(data["generated"])
                    data["cache_timeout"] = data["generated"] + timedelta(seconds=int(self.cache_timeout))
            elif self.cache_timeout == 0:
                data["cache_timeout"] = 0
        else:
            current_app.log.info(f"Do not cache data for the {key} key.")
        data["cache_key"] = key
        data["loaded_from_cache"] = False
        data.pop("file_url", None)
        output = json.dumps(data, default=str)
        if self.cache_data == "true":
            try:
                current_app.log.info(f"Store data in the cache. The key is {key} and the timeout is {self.cache_timeout}.")
                cache.set(hash_cache_key, output, timeout=self.cache_timeout)

                if group != None:
                    cache_group_key = '{}-cache-keys'.format(group).lower()
                    cache_group = cache.get(cache_group_key)
                    cache_group_dict = json.loads(cache_group)
                    cache_group_dict.append(key)
                    cache_group_output = json.dumps(cache_group_dict, default=str)
                    cache.set(cache_group_key, cache_group_output, timeout=self.cache_timeout)
                    current_app.log.info(f"Store model data in the cache. The key is {cache_group_key} and the value is {cache_group_output}.")
            except Exception as exception:
                current_app.log.info(f"Failed to save data with key of {key}. Exception was {exception}")
                pass
            
        return output


    def get(self, key):
        loaded_from_cache = False
        hash_cache_key = hashlib.md5((key).encode('utf-8')).hexdigest()
        output = cache.get(hash_cache_key)
        if output != None:
            loaded_from_cache = True
            current_app.log.info(f"Get data from the cache. The key is {key}.")

        if self.delete_cache == "true":
            current_app.log.info(f"Delete data from the cache. The key is {key}.")
            cache.delete(key)

        if self.bypass_cache == "true":
            output = None
            loaded_from_cache = False
            current_app.log.info(f"Cached data for {key} is not available.")
        if output != None:
            output = json.loads(output)
            output["loaded_from_cache"] = loaded_from_cache
            output = json.dumps(output, default=str)
        return output


    def clear(self, key_list_name):
        data = {
            "deleted": [],
            "not_deleted": [],
            "saved": []
        }
        all_cache_keys = cache.get(key_list_name)
        if all_cache_keys is None:
            all_cache_keys = []

        if all_cache_keys != []:
            for cache_key in all_cache_keys:
                key_deleted = cache.delete(cache_key)

                if key_deleted == True:
                    data["deleted"].append(cache_key)
                    all_cache_keys.remove(cache_key)
                else:
                    data["not_deleted"].append(cache_key)
                    all_cache_keys.remove(cache_key)

        try:
            cache.set(key_list_name, all_cache_keys)
            data["saved"].append(all_cache_keys)
        except Exception as exception:
            data["deleted"] = []
            pass
        
        output = data
        return output
