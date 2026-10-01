"""Server-side Google geocoding, without logging private addresses."""

from collections import OrderedDict
import math
import os
import threading
import time
import unicodedata

import requests

from route_proposal import Coordinate


_cache = OrderedDict()
_cache_lock = threading.Lock()
CACHE_TTL_SECONDS = 60 * 60
CACHE_MAX_ADDRESSES = 256


def geocode_address(address, *, cache=False):
    """Return validated coordinates, or None; cache only successful lookups."""
    address = " ".join(unicodedata.normalize("NFKC", str(address or "")).split())
    api_key = os.environ.get("GOOGLE_MAPS_API_KEY", "").strip()
    if not address or not api_key:
        return None
    cache_key = address.casefold()
    if cache:
        with _cache_lock:
            entry = _cache.get(cache_key)
            if entry and entry[0] > time.monotonic():
                _cache.move_to_end(cache_key)
                return entry[1]
            _cache.pop(cache_key, None)
    try:
        response = requests.get(
            "https://maps.googleapis.com/maps/api/geocode/json",
            params={"address": address, "key": api_key, "language": "sv"},
            timeout=10,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("status") != "OK" or not payload.get("results"):
            return None
        location = payload["results"][0]["geometry"]["location"]
        latitude, longitude = location["lat"], location["lng"]
        if isinstance(latitude, bool) or isinstance(longitude, bool):
            return None
        latitude, longitude = float(latitude), float(longitude)
        if not (math.isfinite(latitude) and math.isfinite(longitude)
                and -90 <= latitude <= 90 and -180 <= longitude <= 180):
            return None
        coordinate = Coordinate(latitude=latitude, longitude=longitude)
    except Exception:
        # HTTP errors may contain the address and API key in their URL.
        return None
    if cache:
        with _cache_lock:
            _cache[cache_key] = (time.monotonic() + CACHE_TTL_SECONDS, coordinate)
            _cache.move_to_end(cache_key)
            while len(_cache) > CACHE_MAX_ADDRESSES:
                _cache.popitem(last=False)
    return coordinate
