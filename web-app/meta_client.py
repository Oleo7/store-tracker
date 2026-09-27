"""Small, server-only Graph API transport shared by Instagram and Ads.

Only relative Graph paths are accepted. Callers choose read or Ads-write
credentials explicitly; this module never puts tokens in URLs or errors.
"""
import os
import re
import time
from urllib.parse import urlsplit

import requests


class MetaError(Exception):
    """An intentionally redacted upstream failure classification."""


_PATH = re.compile(r"[A-Za-z0-9_./-]{1,200}")
_VERSION = re.compile(r"v\d+\.0")


class MetaClient:
    def __init__(self, env=None, transport=None):
        self.env = os.environ if env is None else env
        self.transport = transport or requests

    def _url(self, path, version):
        if not isinstance(path, str) or not _PATH.fullmatch(path) or ".." in path or path.startswith("/"):
            raise MetaError("invalid_path")
        if not isinstance(version, str) or not _VERSION.fullmatch(version):
            raise MetaError("invalid_version")
        return f"https://graph.facebook.com/{version}/{path}"

    @staticmethod
    def safe_next(next_url):
        """Validate a paging URL before extracting its cursor; never follow it."""
        parsed = urlsplit(str(next_url or ""))
        if parsed.scheme != "https" or parsed.netloc != "graph.facebook.com" or parsed.username or parsed.password:
            raise MetaError("invalid_paging_host")
        return parsed

    def _request(self, method, path, *, params=None, data=None, credential="instagram", deadline=None):
        if credential == "instagram":
            token = self.env.get("INSTAGRAM_ACCESS_TOKEN", "")
        elif credential == "ads_read":
            token = self.env.get("META_ADS_ACCESS_TOKEN") or self.env.get("INSTAGRAM_ACCESS_TOKEN", "")
        elif credential == "ads_write":
            token = self.env.get("META_ADS_ACCESS_TOKEN", "")
        else:
            raise MetaError("invalid_credential")
        if not token:
            raise MetaError("not_configured")
        version = self.env.get("INSTAGRAM_GRAPH_API_VERSION", "v26.0")
        url = self._url(path, version)
        remaining = (deadline - time.monotonic()) if deadline is not None else 20
        if remaining <= 0:
            raise MetaError("timeout")
        try:
            sender = self.transport.get if method == "GET" else self.transport.post
            kwargs = {"headers": {"Authorization": "Bearer " + token},
                      "timeout": (min(3, remaining), min(15, remaining)), "allow_redirects": False}
            if method == "GET":
                kwargs["params"] = params or {}
            else:
                kwargs["data"] = data or {}
            response = sender(url, **kwargs)
            if response.status_code != 200:
                raise MetaError("upstream_http_" + str(response.status_code))
            body = response.json()
            if not isinstance(body, dict) or "error" in body:
                raise MetaError("invalid_response")
            return body
        except requests.Timeout:
            raise MetaError("timeout") from None
        except (requests.RequestException, ValueError):
            raise MetaError("network_or_json_error") from None

    def get(self, path, params=None, deadline=None, *, credential="instagram"):
        return self._request("GET", path, params=params, deadline=deadline, credential=credential)

    def ads_get(self, path, params=None):
        return self.get(path, params, credential="ads_read")

    def _ads_post(self, path, data):
        """Internal primitive. AdsService alone constructs typed payloads."""
        return self._request("POST", path, data=data, credential="ads_write")
