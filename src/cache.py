from flask_caching import Cache

cache = Cache()

def clear_multiple_keys(key_list_name):
    result = ""
    all_cache_keys = cache.get(key_list_name)
    if all_cache_keys is None:
        all_cache_keys = []

    if all_cache_keys != []:
        for cache_key in all_cache_keys:
            key_deleted = cache.delete(cache_key)

            if key_deleted == True:
                result += ". Cache key %s deleted" % cache_key
                all_cache_keys.remove(cache_key)
            else:
                result += ". Cache key %s not deleted" % cache_key
                all_cache_keys.remove(cache_key)
    else:
        result += '. Cache key list is empty'

    try:
        cache.set(key_list_name, all_cache_keys)
        result += '. Saved cache key list.'
    except Exception as exception:
        result += '. Unable to save cache key list.'
    
    return result