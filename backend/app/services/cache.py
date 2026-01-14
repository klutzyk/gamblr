import time
import functools
import inspect
import hashlib
import json


# cache utility
class SimpleTTLCache:
    def __init__(self):
        self._store = {}

    # return the cached value if exists
    def get(self, key):
        item = self._store.get(key)
        if not item:
            return None

        value, expires_at = item
        if time.time() > expires_at:
            self._store.pop(key, None)
            return None

        return value

    # set value in cache with a ttl
    def set(self, key, value, ttl_seconds: int):
        self._store[key] = (value, time.time() + ttl_seconds)

    # clear all cached items
    def clear(self):
        self._store.clear()


# global instance cache (shared)
CACHE = SimpleTTLCache()


# Helper functions
# make a unique cache key
def make_cache_key(func, args, kwargs):
    """
    Generate a deterministic cache key based on:
    - function module + name
    - args and kwargs
    """
    raw = {
        "func": f"{func.__module__}.{func.__qualname__}",
        "args": args,
        "kwargs": kwargs,
    }
    encoded = json.dumps(raw, sort_keys=True, default=str).encode()
    return hashlib.sha256(encoded).hexdigest()


# Decorators
def cached(ttl_seconds: int):
    """
    Decorator to cache any function (sync or async) in the global CACHE.

    Example usage:
        @cached(ttl_seconds=300)
        async def fetch_player_props(...):
            ...

        @cached(ttl_seconds=1800)
        def fetch_top_players(...):
            ...
    """

    def decorator(func):
        is_async = inspect.iscoroutinefunction(func)

        if is_async:

            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                key = make_cache_key(func, args, kwargs)
                cached_value = CACHE.get(key)
                if cached_value is not None:
                    return cached_value

                value = await func(*args, **kwargs)
                CACHE.set(key, value, ttl_seconds)
                return value

            return async_wrapper

        else:

            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                key = make_cache_key(func, args, kwargs)
                cached_value = CACHE.get(key)
                if cached_value is not None:
                    return cached_value

                value = func(*args, **kwargs)
                CACHE.set(key, value, ttl_seconds)
                return value

            return sync_wrapper

    return decorator
