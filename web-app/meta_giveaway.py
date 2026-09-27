"""Read-only Instagram comment snapshots and reproducible giveaway draws."""
from datetime import datetime, timezone
import hashlib
import json
import re
import secrets
import uuid
from urllib.parse import urlsplit

from flask import Blueprint, jsonify, request

from instagram_feed import InstagramMetaClient
from meta_client import MetaError
from meta_ads import AdsError

SNAPSHOT_COLUMNS = ("snapshot_id", "media_id", "snapshot_hash", "comment_count", "created_at", "created_by")
COMMENT_COLUMNS = ("snapshot_id", "comment_id", "username", "text", "timestamp")
DRAW_COLUMNS = ("draw_id", "snapshot_id", "snapshot_hash", "rules_json", "seed", "winners_json", "eligible_count", "created_at", "created_by")


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def snapshot_digest(comments):
    rows = sorted(comments, key=lambda row: str(row["comment_id"]))
    return hashlib.sha256(canonical(rows).encode("utf-8")).hexdigest()


def parse_stamp(value, *, client_input=False):
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if stamp.tzinfo is None:
            raise ValueError()
        return stamp.astimezone(timezone.utc)
    except (ValueError, AttributeError):
        raise AdsError("invalid_timestamp", 400 if client_input else 502) from None


class GiveawayStore:
    def __init__(self, spreadsheet, get_or_create, find_row, append_row, append_rows, worksheet_to_dicts):
        self.spreadsheet, self.get_or_create, self.find_row = spreadsheet, get_or_create, find_row
        self.append_row, self.append_rows, self.worksheet_to_dicts = append_row, append_rows, worksheet_to_dicts

    def sheet(self, name, columns):
        return self.get_or_create(self.spreadsheet(), name, columns, rows=1000)

    def save_snapshot(self, metadata, comments):
        comment_sheet = self.sheet("meta_giveaway_comments", COMMENT_COLUMNS)
        self.append_rows(comment_sheet, COMMENT_COLUMNS, [{"snapshot_id": metadata["snapshot_id"], **row} for row in comments])
        self.append_row(self.sheet("meta_giveaway_snapshots", SNAPSHOT_COLUMNS), SNAPSHOT_COLUMNS, metadata)

    def load_snapshot(self, snapshot_id):
        sheet = self.sheet("meta_giveaway_snapshots", SNAPSHOT_COLUMNS)
        row_index, _headers, row = self.find_row(sheet, "snapshot_id", snapshot_id)
        if not row_index:
            return None, None
        comment_sheet = self.sheet("meta_giveaway_comments", COMMENT_COLUMNS)
        comments = [item for item in self.worksheet_to_dicts(comment_sheet, expected_columns=COMMENT_COLUMNS)
                    if item.get("snapshot_id") == snapshot_id]
        return row, [{key: item[key] for key in ("comment_id", "username", "text", "timestamp")} for item in comments]

    def save_draw(self, draw):
        self.append_row(self.sheet("meta_giveaway_draws", DRAW_COLUMNS), DRAW_COLUMNS, {
            "draw_id": draw["draw_id"], "snapshot_id": draw["snapshot_id"], "snapshot_hash": draw["snapshot_hash"],
            "rules_json": canonical(draw["rules"]), "seed": draw["seed"], "winners_json": canonical(draw["winners"]),
            "eligible_count": draw["eligible_count"], "created_at": draw["created_at"], "created_by": draw["created_by"],
        })


class GiveawayService:
    def __init__(self, store, client=None, now=None):
        self.store = store
        self.client = client or InstagramMetaClient()
        self.now = now or (lambda: datetime.now(timezone.utc))

    def snapshot(self, media_id, actor):
        media = self.client.get_media(media_id)
        comments, after, seen, comment_ids = [], None, set(), set()
        for _ in range(10):
            page = self.client.list_comments(media_id, limit=100, after=after)
            for row in page["data"]:
                identifier = str(row.get("id", ""))
                username = str(row.get("username", ""))
                if not re.fullmatch(r"\d{1,40}", identifier) or not re.fullmatch(r"[A-Za-z0-9_.]{1,30}", username):
                    raise AdsError("invalid_comment_data", 502)
                if identifier in comment_ids:
                    raise AdsError("duplicate_comment_id", 502)
                comment_ids.add(identifier)
                parse_stamp(str(row.get("timestamp", "")))
                comments.append({"comment_id": identifier, "username": username,
                                 "text": str(row.get("text", ""))[:2000], "timestamp": str(row.get("timestamp", ""))})
            after = page.get("after")
            if not after:
                break
            if after in seen:
                raise AdsError("comment_pagination_loop", 502)
            seen.add(after)
        else:
            raise AdsError("comment_snapshot_limit", 409)
        if len(comments) > 1000:
            raise AdsError("comment_snapshot_limit", 409)
        digest = snapshot_digest(comments)
        metadata = {"snapshot_id": str(uuid.uuid4()), "media_id": str(media["id"]), "snapshot_hash": digest,
                    "comment_count": len(comments), "created_at": self.now().astimezone(timezone.utc).isoformat(),
                    "created_by": actor}
        self.store.save_snapshot(metadata, comments)
        return metadata

    def draw(self, snapshot_id, rules, actor, *, confirm, seed=None):
        if confirm is not True:
            raise AdsError("confirmation_required", 409)
        try:
            uuid.UUID(str(snapshot_id))
        except (ValueError, TypeError):
            raise AdsError("invalid_snapshot_id") from None
        if not isinstance(rules, dict) or set(rules) - {"required_keyword", "excluded_usernames", "start_at", "end_at", "winner_count"}:
            raise AdsError("invalid_rules")
        winner_count = rules.get("winner_count", 1)
        if isinstance(winner_count, bool) or not isinstance(winner_count, int) or not 1 <= winner_count <= 10:
            raise AdsError("invalid_winner_count")
        keyword = rules.get("required_keyword", "")
        excluded = rules.get("excluded_usernames", [])
        if not isinstance(keyword, str) or len(keyword) > 100 or not isinstance(excluded, list) or len(excluded) > 100 or any(not isinstance(v, str) for v in excluded):
            raise AdsError("invalid_rules")
        for key in ("start_at", "end_at"):
            if key in rules:
                parse_stamp(rules[key], client_input=True)
        if "start_at" in rules and "end_at" in rules and parse_stamp(rules["start_at"]) > parse_stamp(rules["end_at"]):
            raise AdsError("invalid_date_range")
        if seed is None:
            seed = secrets.token_hex(32)
        if not isinstance(seed, str) or not re.fullmatch(r"[A-Za-z0-9_-]{16,128}", seed):
            raise AdsError("invalid_seed")
        metadata, comments = self.store.load_snapshot(snapshot_id)
        if not metadata:
            raise AdsError("snapshot_not_found", 404)
        if snapshot_digest(comments) != metadata["snapshot_hash"] or len(comments) != int(metadata["comment_count"]):
            raise AdsError("snapshot_integrity_failed", 409)
        excluded_set = {value.casefold() for value in excluded}
        entrants = {}
        for row in sorted(comments, key=lambda item: (parse_stamp(item["timestamp"]), item["comment_id"])):
            username = row["username"].casefold()
            if username in excluded_set or (keyword and keyword.casefold() not in row["text"].casefold()):
                continue
            if "start_at" in rules and parse_stamp(row["timestamp"]) < parse_stamp(rules["start_at"]):
                continue
            if "end_at" in rules and parse_stamp(row["timestamp"]) > parse_stamp(rules["end_at"]):
                continue
            entrants.setdefault(username, row)
        if len(entrants) < winner_count:
            raise AdsError("not_enough_eligible_entries", 409)
        ranked = sorted(entrants.items(), key=lambda pair: (
            hashlib.sha256((seed + "\n" + metadata["snapshot_hash"] + "\n" + pair[0]).encode("utf-8")).hexdigest(), pair[0]))
        winners = [{"username": row["username"], "comment_id": row["comment_id"]} for _name, row in ranked[:winner_count]]
        draw = {"draw_id": str(uuid.uuid4()), "snapshot_id": snapshot_id, "snapshot_hash": metadata["snapshot_hash"],
                "rules": rules, "seed": seed, "winners": winners, "eligible_count": len(entrants),
                "created_at": self.now().astimezone(timezone.utc).isoformat(), "created_by": actor}
        self.store.save_draw(draw)
        return draw


def create_blueprint(service, current_user, user_is_admin):
    bp = Blueprint("meta_instagram", __name__)

    def admin():
        if not user_is_admin(current_user()):
            raise AdsError("admin_required", 403)
        return str(current_user().get("user_name") or "")

    def safe_origin():
        if request.headers.get("Sec-Fetch-Site") == "cross-site":
            raise AdsError("invalid_origin", 403)
        origin = request.headers.get("Origin")
        if origin:
            parsed = urlsplit(origin)
            if parsed.scheme != "https" or parsed.netloc != request.host or parsed.path:
                raise AdsError("invalid_origin", 403)

    @bp.errorhandler(AdsError)
    def ads_error(exc):
        return jsonify({"ok": False, "error": exc.code}), exc.status

    @bp.errorhandler(MetaError)
    def meta_error(exc):
        return jsonify({"ok": False, "error": str(exc)}), 502

    @bp.get("/meta/instagram/media")
    def media_list():
        try:
            limit = int(request.args.get("limit", "100"))
        except ValueError:
            raise AdsError("invalid_limit") from None
        return jsonify(service.client.list_own_media(limit=limit, after=request.args.get("after")))

    @bp.get("/meta/instagram/media/<media_id>")
    def media_detail(media_id):
        return jsonify(service.client.get_media(media_id))

    @bp.get("/meta/instagram/media/<media_id>/comments")
    def media_comments(media_id):
        try:
            limit = int(request.args.get("limit", "100"))
        except ValueError:
            raise AdsError("invalid_limit") from None
        return jsonify(service.client.list_comments(media_id, limit=limit, after=request.args.get("after")))

    @bp.post("/meta/giveaways/snapshots")
    def snapshot():
        actor = admin()
        safe_origin()
        data = request.get_json(silent=True)
        if not isinstance(data, dict) or set(data) != {"media_id"}:
            raise AdsError("invalid_payload")
        return jsonify(service.snapshot(data["media_id"], actor))

    @bp.post("/meta/giveaways/draws")
    def draw():
        actor = admin()
        safe_origin()
        data = request.get_json(silent=True)
        if not isinstance(data, dict) or set(data) - {"snapshot_id", "rules", "seed", "confirm"}:
            raise AdsError("invalid_payload")
        return jsonify(service.draw(data.get("snapshot_id"), data.get("rules"), actor,
                                    confirm=data.get("confirm"), seed=data.get("seed")))

    return bp
