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


    def delete(self, key):
        class_to_use = self.class_to_use
        output = class_to_use.delete(key)
        return output


    def get(self, key):
        class_to_use = self.class_to_use
        output = class_to_use.get(key)
        return output

    
    def clear_group(self, group_name):
        class_to_use = self.class_to_use
        output = class_to_use.clear_group(group_name)
        return output


class CacheStorage(object):

    def __init__(self, args = {}, http_method = "GET"):
        self.cache_timeout = current_app.config["CACHE_DEFAULT_TIMEOUT"]
        if http_method == "GET":
            self.bypass_cache = args.get("bypass_cache", "false")
            self.delete_cache = args.get("delete_cache", "false")
            self.cache_data = args.get("cache_data", "true")
            self.cache_timeout = int(args.get("cache_timeout", self.cache_timeout))
            self.display_cache_data = args.get("display_cache_data", "false")
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
            self.display_cache_data = args["display_cache_data"]
        # prevent error
        if self.delete_cache == "true" and self.cache_data == "false":
            self.bypass_cache = "true"


    def save(self, key, data, group = None):
        if group != None:
            cache_group_key = '{}-cache-keys'.format(group).lower()
            if self.display_cache_data == "true":
                data["cache_group"] = cache_group_key
        if self.cache_data == "true":
            if self.cache_timeout is not None and self.cache_timeout != 0:
                if "generated" in data and self.display_cache_data == "true":
                    if type(data["generated"]) == str:
                        data["generated"] = datetime.datetime.fromisoformat(data["generated"])
                    data["cache_timeout"] = data["generated"] + timedelta(seconds=int(self.cache_timeout))
            elif self.cache_timeout == 0:
                data["cache_timeout"] = 0
        else:
            current_app.log.info(f"Do not cache data for the {key} key.")
        if self.display_cache_data == "true":
            data["cache_key"] = key
            data["loaded_from_cache"] = False
        output = json.dumps(data, default=str)
        if self.cache_data == "true":
            current_app.log.info(f"Store data in the cache. The key is {key} and the timeout is {self.cache_timeout}.")
            hash_cache_key = hashlib.md5((key).encode('utf-8')).hexdigest()
            cache.set(hash_cache_key, output, timeout=self.cache_timeout)
            if group != None:
                cache_group = cache.get(cache_group_key)
                cache_group_list = {}
                if cache_group != None:
                    cache_group_list = json.loads(cache_group)
                cache_group_list[key] = True
                cache_group_output = json.dumps(cache_group_list, default=str)
                cache.set(cache_group_key, cache_group_output, timeout=self.cache_timeout)
                current_app.log.info(f"Store model data list in the cache. The key is {cache_group_key} and the timeout is {self.cache_timeout}.")
            
        return output


    def delete(self, key):
        deleted = False
        hash_cache_key = hashlib.md5((key).encode('utf-8')).hexdigest()
        output = cache.get(hash_cache_key)
        if output != None:
            current_app.log.info(f"Delete data from the cache. The key is {key}.")
            cache.delete(hash_cache_key)
            deleted = True
        return deleted


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
            if self.display_cache_data == "true":
                output["loaded_from_cache"] = loaded_from_cache
            output = json.dumps(output, default=str)
        return output


    def clear_group(self, group_name):
        data = {
            "deleted": {}
        }
        if group_name != None:
            cache_group_key = '{}-cache-keys'.format(group_name).lower()
            cache_group = cache.get(cache_group_key)
            cache_group_list = {}
            if cache_group != None:
                cache_group_list = json.loads(cache_group)
            query_list_cache = current_app.config['QUERY_LIST_CACHE_KEY']
            cache_query_key = '{}-cache-keys'.format(query_list_cache).lower()
            query_cache_keys = cache.get(cache_query_key)
            if query_cache_keys != None:
                cache_group_list = {**cache_group_list, **json.loads(query_cache_keys)}
            for cache_key in cache_group_list:
                #current_app.log.debug(f"key to check is {cache_key}")
                deleted = self.delete(cache_key)
                if deleted == True:
                    data["deleted"][cache_key] = True
                    cache_group_list.pop("cache_key", None)
                else:
                    data["deleted"][cache_key] = False
            output = json.dumps(cache_group_list, default=str)
            cache.set(cache_group_key, output, timeout=self.cache_timeout)

        return data
