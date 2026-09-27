"""Authenticated Store Tracker API and persistent Sheets journal for Meta Ads."""
from datetime import datetime, timezone
import json
from urllib.parse import urlsplit

from flask import Blueprint, jsonify, request

from meta_ads import AdsError, AdsService
from meta_client import MetaError

CHANGE_COLUMNS = ("change_id", "status", "change_json", "object_id", "verification_result", "updated_at")
AUDIT_COLUMNS = ("change_id", "operation", "object_type", "object_id", "before_json", "after_json",
                 "requested_by", "previewed_at", "approved_by", "approved_at", "applied_at",
                 "state_fingerprint_before", "verification_result", "meta_request_id")


class SheetsChangeStore:
    def __init__(self, spreadsheet, get_or_create, find_row, append_row, update_row):
        self.spreadsheet = spreadsheet
        self.get_or_create = get_or_create
        self.find_row = find_row
        self.append_row = append_row
        self.update_row = update_row

    def _sheet(self, title, columns):
        return self.get_or_create(self.spreadsheet(), title, columns, rows=1000)

    def _change(self, change_id):
        sheet = self._sheet("meta_ads_changes", CHANGE_COLUMNS)
        row_index, headers, row = self.find_row(sheet, "change_id", change_id)
        return sheet, row_index, headers, row

    def save_preview(self, change):
        encoded = json.dumps(change, ensure_ascii=False, separators=(",", ":"))
        if len(encoded) > 45000:
            raise AdsError("preview_too_large")
        sheet = self._sheet("meta_ads_changes", CHANGE_COLUMNS)
        self.append_row(sheet, CHANGE_COLUMNS, {"change_id": change["change_id"], "status": "PENDING",
                                                  "change_json": encoded, "object_id": change["object_id"],
                                                  "updated_at": change["created_at"]})

    def load_preview(self, change_id):
        _sheet, row_index, _headers, row = self._change(change_id)
        if not row_index:
            return None
        value = json.loads(row["change_json"])
        value["status"] = row["status"]
        return value

    def mark_applying(self, change_id, actor, approved_at):
        sheet, row_index, headers, row = self._change(change_id)
        if not row_index or row["status"] != "PENDING":
            raise AdsError("change_already_used", 409)
        change = json.loads(row["change_json"])
        audit = self._sheet("meta_ads_audit", AUDIT_COLUMNS)
        before_json = json.dumps(change["before"], ensure_ascii=False, separators=(",", ":"))
        if len(before_json) > 45000:
            raise AdsError("audit_too_large")
        # Claim the preview first. An audit failure leaves it unusable and never
        # reaches Meta, which is safer than risking a second write.
        self.update_row(sheet, row_index, headers, {"status": "APPLYING", "updated_at": approved_at})
        self.append_row(audit, AUDIT_COLUMNS, {"change_id": change_id, "operation": change["operation"],
                                                "object_type": change["object_type"], "object_id": change["object_id"],
                                                "before_json": before_json, "requested_by": change["requested_by"],
                                                "previewed_at": change["created_at"], "approved_by": actor,
                                                "approved_at": approved_at, "state_fingerprint_before": change["state_fingerprint"],
                                                "verification_result": "APPLYING"})

    def finish(self, change_id, object_id, after, result, request_id):
        sheet, row_index, headers, _row = self._change(change_id)
        if not row_index:
            raise AdsError("change_not_found", 404)
        audit = self._sheet("meta_ads_audit", AUDIT_COLUMNS)
        audit_index, audit_headers, _audit_row = self.find_row(audit, "change_id", change_id)
        if not audit_index:
            raise AdsError("audit_missing", 503)
        encoded_after = json.dumps(after, ensure_ascii=False, separators=(",", ":")) if after else ""
        if len(encoded_after) > 45000:
            encoded_after = ""  # Preserve verification state, even for oversized Meta data.
            result = "READ_BACK_TOO_LARGE"
        self.update_row(audit, audit_index, audit_headers, {"object_id": object_id, "after_json": encoded_after,
                                                              "applied_at": datetime.now(timezone.utc).isoformat(),
                                                              "verification_result": result, "meta_request_id": request_id or ""})
        self.update_row(sheet, row_index, headers, {"status": result, "object_id": object_id,
                                                    "verification_result": result})


def create_blueprint(service: AdsService, current_user, user_is_admin):
    bp = Blueprint("meta_ads", __name__)

    def admin():
        if not user_is_admin(current_user()):
            raise AdsError("admin_required", 403)
        return str(current_user().get("user_name") or "")

    def safe_origin():
        origin = request.headers.get("Origin")
        if request.headers.get("Sec-Fetch-Site") == "cross-site":
            raise AdsError("invalid_origin", 403)
        if origin:
            parsed = urlsplit(origin)
            if parsed.scheme != "https" or parsed.netloc != request.host or parsed.path:
                raise AdsError("invalid_origin", 403)

    def payload():
        if not request.is_json or (request.content_length is not None and request.content_length > 25000):
            raise AdsError("invalid_json")
        value = request.get_json(silent=True)
        if not isinstance(value, dict):
            raise AdsError("invalid_json")
        return value

    @bp.errorhandler(AdsError)
    def ads_error(exc):
        return jsonify({"ok": False, "error": exc.code}), exc.status

    @bp.errorhandler(MetaError)
    def meta_error(exc):
        return jsonify({"ok": False, "error": str(exc)}), 502

    @bp.get("/meta/capabilities")
    def capabilities():
        admin()
        return jsonify(service.capability_status())

    @bp.get("/meta/ads/account")
    def account():
        return jsonify(service.account())

    @bp.get("/meta/ads/<kind>")
    def listing(kind):
        mapping = {"campaigns": "campaign", "adsets": "adset", "ads": "ad", "creatives": "creative"}
        if kind not in mapping:
            raise AdsError("not_found", 404)
        try:
            limit = int(request.args.get("limit", "100"))
        except ValueError:
            raise AdsError("invalid_limit") from None
        return jsonify({"data": service.list(mapping[kind], limit=limit)})

    @bp.get("/meta/ads/<kind>/<identifier>")
    def detail(kind, identifier):
        mapping = {"campaigns": "campaign", "adsets": "adset", "ads": "ad", "creatives": "creative"}
        if kind not in mapping:
            raise AdsError("not_found", 404)
        return jsonify(service.get(mapping[kind], identifier))

    @bp.get("/meta/ads/adsets/<identifier>/targeting")
    def targeting(identifier):
        return jsonify(service.get_targeting(identifier))

    @bp.get("/meta/ads/insights")
    def insights():
        query = request.args.to_dict()
        for key in ("breakdowns", "filtering"):
            if key in query:
                try:
                    query[key] = json.loads(query[key])
                except ValueError:
                    raise AdsError("invalid_query") from None
        if "time_increment" in query and query["time_increment"] != "all_days":
            try:
                query["time_increment"] = int(query["time_increment"])
            except ValueError:
                raise AdsError("invalid_time_increment") from None
        return jsonify(service.insights(query))

    @bp.post("/meta/ads/<operation>/preview")
    def preview_change(operation):
        actor = admin()
        safe_origin()
        mapping = {"targeting": "targeting_change", "budget": "budget_change", "status": "status_change"}
        if operation not in mapping:
            raise AdsError("not_found", 404)
        return jsonify(service.preview(mapping[operation], payload(), actor))

    @bp.post("/meta/ads/<kind>/preview-create")
    def preview_create(kind):
        actor = admin()
        safe_origin()
        mapping = {"campaigns": "campaign", "adsets": "adset", "creatives": "creative", "ads": "ad"}
        if kind not in mapping:
            raise AdsError("not_found", 404)
        return jsonify(service.preview("create_" + mapping[kind], payload(), actor))

    @bp.post("/meta/ads/changes/<change_id>/apply")
    def apply(change_id):
        actor = admin()
        safe_origin()
        data = payload()
        if set(data) != {"confirm"} or data["confirm"] is not True:
            raise AdsError("confirmation_required", 409)
        return jsonify(service.apply(change_id, confirm=True, actor=actor))

    return bp
