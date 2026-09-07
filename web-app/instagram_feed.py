"""Public Instagram metadata only; no media downloads or persistent storage."""
from collections import OrderedDict, deque
from datetime import datetime, timezone
import hashlib
import hmac
import logging
import os
import re
import threading
import time
from urllib.parse import urlsplit

import requests
from flask import Blueprint, Response, jsonify, request
from flask_cors import cross_origin

ORIGINS = ("https://polarbar.se", "https://www.polarbar.se")
FIELDS = "id,permalink,media_type,timestamp,username"
MIX = ("ugc", "ugc", "own")  # Change this tuple to change the default mix.
log = logging.getLogger("store_tracker.instagram")


class MetaError(Exception):
    """Safe error classification. Never include upstream text or request URLs."""


def integer(env, name, default, minimum, maximum):
    try:
        return max(minimum, min(maximum, int(env.get(name, default))))
    except (TypeError, ValueError):
        return default


def normalize(raw, source):
    if not isinstance(raw, dict) or raw.get("media_type") not in {"IMAGE", "VIDEO", "CAROUSEL_ALBUM"}:
        return None
    media_id = str(raw.get("id", ""))
    if not re.fullmatch(r"\d{1,40}", media_id):
        return None
    try:
        url = urlsplit(str(raw.get("permalink", "")))
        if (url.scheme != "https" or url.hostname not in {"instagram.com", "www.instagram.com"}
                or url.netloc != url.hostname or not re.fullmatch(r"/(p|reel|tv)/[A-Za-z0-9_-]+/?", url.path)):
            return None
        stamp = datetime.fromisoformat(str(raw["timestamp"]).replace("Z", "+00:00"))
        if stamp.tzinfo is None:
            return None
    except (KeyError, ValueError, TypeError):
        return None
    item = {"id": media_id, "permalink": "https://www.instagram.com" + url.path.rstrip("/") + "/",
            "media_type": raw["media_type"], "timestamp": stamp.astimezone(timezone.utc).isoformat(),
            "source": source}
    username = raw.get("username", "")
    if isinstance(username, str) and re.fullmatch(r"[A-Za-z0-9_.]{1,30}", username):
        item["username"] = username
    return item


def mix_feed(own, ugc, limit=12, pattern=MIX):
    # Own membership wins when a collaborative post is also in the tags collection.
    seen_ids, seen_urls = set(), set()
    pools = {}
    for source, rows in (("own", own), ("ugc", ugc)):
        pool = []
        for row in sorted(rows, key=lambda r: (r["timestamp"], r["id"]), reverse=True):
            if row["id"] in seen_ids or row["permalink"] in seen_urls:
                continue
            seen_ids.add(row["id"])
            seen_urls.add(row["permalink"])
            pool.append(dict(row, source=source))
        pools[source] = deque(pool)
    result = []
    while len(result) < limit and any(pools.values()):
        source = pattern[len(result) % len(pattern)]
        if not pools[source]:
            source = "own" if source == "ugc" else "ugc"
        result.append(pools[source].popleft())
    return result


class MetaClient:
    def __init__(self, env=None, transport=None):
        self.env = os.environ if env is None else env
        self.transport = transport or requests

    def get(self, path, params, deadline):
        token = self.env.get("INSTAGRAM_ACCESS_TOKEN", "")
        version = self.env.get("INSTAGRAM_GRAPH_API_VERSION", "v26.0")
        if not token or not re.fullmatch(r"v\d+\.0", version):
            raise MetaError("not_configured")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise MetaError("timeout")
        try:
            response = self.transport.get(
                f"https://graph.facebook.com/{version}/{path}", params=params,
                headers={"Authorization": "Bearer " + token},
                timeout=(min(3, remaining), min(15, remaining)), allow_redirects=False,
            )
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

    def collection(self, account, edge, deadline):
        result, after, seen = [], None, set()
        # Bounded work per refresh. Tags uses cursors even when 'next' is absent.
        for _ in range(3):
            params = {"fields": FIELDS, "limit": 25}
            if after:
                params["after"] = after
            body = self.get(f"{account}/{edge}", params, deadline)
            if not isinstance(body.get("data"), list):
                raise MetaError("invalid_collection")
            result.extend(body["data"])
            after = body.get("paging", {}).get("cursors", {}).get("after")
            if not after or after in seen:
                break
            seen.add(after)
        return [item for row in result if (item := normalize(row, "own" if edge == "media" else "ugc"))]

    def mention(self, account, kind, identifier, deadline):
        # The mentioned_media field requires a webhook media ID, not pagination.
        if kind == "media":
            fields = f"mentioned_media.media_id({identifier}){{id,media_type,timestamp,username}}"
            body = self.get(account, {"fields": fields}, deadline).get("mentioned_media", {})
            media_id = body.get("id")
        else:
            fields = f"mentioned_comment.comment_id({identifier}){{id,media}}"
            body = self.get(account, {"fields": fields}, deadline).get("mentioned_comment", {})
            media = body.get("media", {})
            media_id = media.get("id") if isinstance(media, dict) else media
        if not re.fullmatch(r"\d{1,40}", str(media_id or "")):
            return None
        # Only publish an item when the API also supplies an embeddable permalink.
        detail = self.get(str(media_id), {"fields": FIELDS}, deadline)
        return normalize(detail, "ugc")


class FeedService:
    def __init__(self, client=None, env=None, clock=time.monotonic):
        self.env = os.environ if env is None else env
        self.client = client or MetaClient(self.env)
        self.clock = clock
        self.lock = threading.Lock()
        self.events_lock = threading.Lock()
        self.events = OrderedDict()
        self.sources = {"own": [], "tags": [], "mentions": []}
        self.updated = None
        self.retry_at = 0
        self.stale = False

    def accept_mentions(self, payload):
        account = self.env.get("INSTAGRAM_ACCOUNT_ID")
        if not isinstance(payload, dict) or payload.get("object") != "instagram":
            return
        with self.events_lock:
            for entry in payload.get("entry", []):
                if not isinstance(entry, dict) or str(entry.get("id")) != account:
                    continue
                changes = entry.get("changes", [])
                if not isinstance(changes, list):
                    continue
                for change in changes:
                    if not isinstance(change, dict) or change.get("field") != "mentions":
                        continue
                    value = change.get("value", {})
                    if not isinstance(value, dict):
                        continue
                    kind = "comment" if value.get("comment_id") else "media"
                    identifier = str(value.get(kind + "_id", ""))
                    if re.fullmatch(r"\d{1,40}", identifier):
                        self.events[(kind, identifier)] = self.clock()
                        self.events.move_to_end((kind, identifier))
                        while len(self.events) > 100:
                            self.events.popitem(last=False)

    def result(self):
        return {"items": mix_feed(self.sources["own"], self.sources["tags"] + self.sources["mentions"],
                    integer(self.env, "INSTAGRAM_FEED_LIMIT", 12, 1, 24)), "stale": self.stale}

    def feed(self):
        ttl = integer(self.env, "INSTAGRAM_FEED_CACHE_TTL_SECONDS", 36000, 60, 604800)
        now = self.clock()
        if (self.updated is not None and now - self.updated < ttl and not self.stale) or now < self.retry_at:
            return self.result()
        # Readers get old data immediately while one request performs the refresh.
        if not self.lock.acquire(blocking=False):
            return self.result()
        try:
            now = self.clock()
            if (self.updated is not None and now - self.updated < ttl and not self.stale) or now < self.retry_at:
                return self.result()
            account = self.env.get("INSTAGRAM_ACCOUNT_ID", "")
            if not re.fullmatch(r"\d{1,40}", account):
                self.retry_at = now + 60
                self.stale = True
                return self.result()
            deadline = time.monotonic() + 25
            failed = False
            for source, edge in (("own", "media"), ("tags", "tags")):
                try:
                    self.sources[source] = self.client.collection(account, edge, deadline)
                except MetaError as error:
                    failed = True
                    log.warning("instagram_refresh source=%s reason=%s", source, str(error))
            with self.events_lock:
                # IDs are memory-only and retained for at most 30 days.
                self.events = OrderedDict((k, t) for k, t in self.events.items() if now - t < 2592000)
                events = list(self.events)[-12:]
            mentions = []
            mentions_failed = False
            for kind, identifier in reversed(events):
                try:
                    item = self.client.mention(account, kind, identifier, deadline)
                    if item:
                        mentions.append(item)
                except MetaError as error:
                    if str(error) in {"upstream_http_400", "upstream_http_404"}:
                        continue  # Deleted/private/unsupported media is not embeddable.
                    failed = True
                    mentions_failed = True
                    log.warning("instagram_refresh source=mentions reason=%s", str(error))
                    break
            if not mentions_failed:
                self.sources["mentions"] = mentions
            self.updated = now
            self.stale = failed
            self.retry_at = now + 60 if failed else 0
            log.info("instagram_refresh completed own=%d tags=%d mentions=%d stale=%s",
                     *(len(self.sources[k]) for k in ("own", "tags", "mentions")), failed)
            return self.result()
        finally:
            self.lock.release()


def create_blueprint(service=None):
    service = service or FeedService()
    bp = Blueprint("instagram", __name__)

    @bp.get("/api/public/instagram-feed")
    @cross_origin(origins=ORIGINS, methods=["GET"], supports_credentials=False, always_send=False)
    def feed():
        if request.headers.get("Origin") and request.headers["Origin"] not in ORIGINS:
            return jsonify({"items": []}), 403
        response = jsonify(service.feed())
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response

    @bp.route("/api/webhooks/instagram", methods=["GET", "POST"])
    def webhook():
        env = service.env
        if request.method == "GET":
            expected = env.get("INSTAGRAM_WEBHOOK_VERIFY_TOKEN", "")
            if (expected and request.args.get("hub.mode") == "subscribe" and
                    hmac.compare_digest(request.args.get("hub.verify_token", ""), expected)):
                return Response(request.args.get("hub.challenge", ""), mimetype="text/plain")
            return "", 403
        secret = env.get("INSTAGRAM_APP_SECRET", "")
        if not secret:
            return "", 503
        if request.content_length is None or request.content_length > 65536:
            return "", 413
        raw = request.get_data()
        signature = "sha256=" + hmac.new(secret.encode(), raw, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature, request.headers.get("X-Hub-Signature-256", "")):
            return "", 403
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict) or not isinstance(payload.get("entry", []), list):
            return "", 400
        service.accept_mentions(payload)
        return "", 200

    return bp
