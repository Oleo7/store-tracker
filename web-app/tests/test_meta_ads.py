import copy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
from unittest import TestCase

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from meta_ads import AdsError, AdsService
from meta_client import MetaClient, MetaError
from meta_ads_api import SheetsChangeStore


ENV = {"META_AD_ACCOUNT_ID": "1689435278682373", "META_ADS_ACCESS_TOKEN": "test-only",
       "META_ADS_WRITE_ENABLED": "true", "META_ADS_MAX_DAILY_BUDGET_MINOR": "100000",
       "META_ADS_MAX_LIFETIME_BUDGET_MINOR": "1000000"}


class MemoryStore:
    def __init__(self):
        self.changes = {}
        self.audit = []

    def save_preview(self, change):
        self.changes[change["change_id"]] = copy.deepcopy(change)

    def load_preview(self, change_id):
        return copy.deepcopy(self.changes.get(change_id))

    def mark_applying(self, change_id, actor, when):
        self.changes[change_id]["status"] = "APPLYING"
        self.audit.append({"change_id": change_id, "approved_by": actor, "verification_result": "APPLYING"})

    def finish(self, change_id, object_id, after, result, request_id):
        self.changes[change_id]["status"] = result
        self.audit[-1].update(object_id=object_id, after=after, verification_result=result)


class FakeMeta:
    def __init__(self):
        self.rows = {
            "act_1689435278682373": {"id": "act_1689435278682373", "currency": "SEK", "name": "Polarbär"},
            "100": {"id": "100", "account_id": "1689435278682373", "name": "Traffic", "objective": "OUTCOME_TRAFFIC", "status": "PAUSED"},
            "200": {"id": "200", "account_id": "1689435278682373", "campaign_id": "100", "name": "Gothenburg",
                    "status": "PAUSED", "daily_budget": "50000", "targeting": {"geo_locations": {"regions": [{"key": "123"}]},
                    "age_min": 21, "age_max": 60, "custom_audiences": [{"id": "987"}], "instagram_positions": ["stream"]}},
            "300": {"id": "300", "account_id": "1689435278682373", "adset_id": "200", "name": "Ad", "status": "PAUSED", "creative": {"id": "400"}},
            "400": {"id": "400", "account_id": "1689435278682373", "name": "Creative", "object_story_id": "868_999"},
        }
        self.posts = []
        self.permissions = {"ads_read", "ads_management"}
        self.fail_write = False
        self.fail_readback = False

    def get(self, path, params=None, deadline=None, *, credential=None):
        assert path == "me/permissions" and credential == "ads_write"
        return {"data": [{"permission": p, "status": "granted"} for p in self.permissions]}

    def ads_get(self, path, params=None):
        if path.endswith("/insights"):
            return {"data": [{"spend": "12.34"}]}
        if path.startswith("act_") and "/" in path:
            edge = path.rsplit("/", 1)[-1]
            kind = {"campaigns": "100", "adsets": "200", "ads": "300", "adcreatives": "400"}.get(edge)
            return {"data": [copy.deepcopy(self.rows[kind])]} if kind else {"data": []}
        return copy.deepcopy(self.rows[path])

    def safe_next(self, url):
        return MetaClient.safe_next(url)

    def _ads_post(self, path, data):
        if self.fail_write:
            raise MetaError("upstream_http_400")
        self.posts.append((path, copy.deepcopy(data)))
        if path.startswith("act_"):
            kind = path.rsplit("/", 1)[-1]
            new_id = {"campaigns": "101", "adsets": "201", "adcreatives": "401", "ads": "301"}[kind]
            row = {"id": new_id, "account_id": "1689435278682373", **data}
            for key in ("special_ad_categories", "targeting", "creative", "object_story_spec"):
                if isinstance(row.get(key), str) and row[key].startswith(("{", "[")):
                    import json
                    row[key] = json.loads(row[key])
            if kind == "ads":
                row["creative"] = {"id": row["creative"]["creative_id"]}
            self.rows[new_id] = row
            return {"id": new_id}
        for key, value in data.items():
            if key == "targeting":
                import json
                value = json.loads(value)
            self.rows[path][key] = value
        if self.fail_readback:
            self.rows[path][next(iter(data))] = "unexpected"
        return {"success": True}


class AdsTests(TestCase):
    def setUp(self):
        self.env = dict(ENV)
        self.meta = FakeMeta()
        self.store = MemoryStore()
        self.clock = datetime(2026, 9, 27, 12, tzinfo=timezone.utc)
        self.service = AdsService(self.store, self.meta, self.env, now=lambda: self.clock)

    def error(self, code, fn):
        with self.assertRaises(AdsError) as caught:
            fn()
        self.assertEqual(caught.exception.code, code)

    def test_reads_and_insights(self):
        self.assertEqual(self.service.account()["currency"], "SEK")
        for kind in ("campaign", "adset", "ad", "creative"):
            self.assertEqual(len(self.service.list(kind)), 1)
        self.assertEqual(self.service.get_targeting("200")["age_min"], 21)
        self.assertEqual(self.service.insights({"level": "adset", "since": "2026-09-01", "until": "2026-09-20"})["currency"], "SEK")

    def test_preview_is_read_only_and_apply_requires_saved_id(self):
        preview = self.service.preview("status_change", {"object_type": "ad", "object_id": "300", "status": "ACTIVE"}, "olle")
        self.assertFalse(self.meta.posts)
        self.assertEqual(preview["diff"]["status"], {"before": "PAUSED", "after": "ACTIVE"})
        self.error("change_not_found", lambda: self.service.apply("d12003b5-e200-41d5-aab5-f058afe4bedf", confirm=True, actor="olle"))
        self.error("confirmation_required", lambda: self.service.apply(preview["change_id"], confirm=False, actor="olle"))

    def test_status_apply_readback_audit_and_one_time_id(self):
        change = self.service.preview("status_change", {"object_type": "ad", "object_id": "300", "status": "ACTIVE"}, "olle")
        result = self.service.apply(change["change_id"], confirm=True, actor="olle")
        self.assertEqual(result["verification_result"], "VERIFIED")
        self.assertEqual(self.meta.rows["300"]["status"], "ACTIVE")
        self.assertEqual(self.store.audit[0]["verification_result"], "VERIFIED")
        self.error("change_already_used", lambda: self.service.apply(change["change_id"], confirm=True, actor="olle"))

    def test_expiry_and_stale_protection(self):
        change = self.service.preview("status_change", {"object_type": "ad", "object_id": "300", "status": "ACTIVE"}, "olle")
        self.clock += timedelta(minutes=31)
        self.error("preview_expired", lambda: self.service.apply(change["change_id"], confirm=True, actor="olle"))
        self.clock -= timedelta(minutes=31)
        self.meta.rows["300"]["name"] = "Changed externally"
        self.error("STALE_STATE", lambda: self.service.apply(change["change_id"], confirm=True, actor="olle"))
        self.assertFalse(self.meta.posts)

    def test_targeting_preserves_unrelated_settings(self):
        change = self.service.preview("targeting_change", {"object_id": "200", "mutation": {"mode": "add_custom_locations",
            "custom_locations": [{"latitude": 57.70, "longitude": 11.97, "radius": 10, "distance_unit": "kilometer"}]}}, "olle")
        self.assertEqual(change["proposed_after"]["targeting"]["custom_audiences"], [{"id": "987"}])
        self.assertEqual(change["proposed_after"]["targeting"]["geo_locations"]["regions"], [{"key": "123"}])
        self.service.apply(change["change_id"], confirm=True, actor="olle")
        self.assertEqual(self.meta.rows["200"]["targeting"]["custom_audiences"], [{"id": "987"}])

    def test_budget_caps_currency_minor_units_and_guard(self):
        change = self.service.preview("budget_change", {"object_type": "adset", "object_id": "200", "budget_type": "daily_budget", "amount_minor": 70000}, "olle")
        self.assertEqual(change["extra"]["minor_unit_factor"], 100)
        self.assertEqual(change["extra"]["delta_minor"], 20000)
        self.assertEqual(change["extra"]["delta_percent"], 40)
        self.service.apply(change["change_id"], confirm=True, actor="olle")
        self.assertEqual(self.meta.rows["200"]["daily_budget"], "70000")
        self.error("budget_cap_exceeded", lambda: self.service.preview("budget_change", {"object_type": "adset", "object_id": "200", "budget_type": "daily_budget", "amount_minor": 200000}, "olle"))
        self.env.pop("META_ADS_MAX_DAILY_BUDGET_MINOR")
        self.error("budget_cap_required", lambda: self.service.preview("budget_change", {"object_type": "adset", "object_id": "200", "budget_type": "daily_budget", "amount_minor": 80000}, "olle"))

    def test_create_all_paused_and_read_back(self):
        cases = [("create_campaign", {"name": "Test", "objective": "OUTCOME_TRAFFIC"}),
                 ("create_adset", {"campaign_id": "100", "name": "Test", "billing_event": "IMPRESSIONS",
                                    "optimization_goal": "LINK_CLICKS", "daily_budget_minor": 50000,
                                    "targeting": {"geo_locations": {"countries": ["SE"]}}}),
                 ("create_creative", {"name": "Test", "object_story_id": "868369943031594_999"}),
                 ("create_ad", {"adset_id": "200", "creative_id": "400", "name": "Test"})]
        for operation, payload in cases:
            kind = "creative" if operation == "create_creative" else operation.removeprefix("create_")
            with self.subTest(operation=operation):
                change = self.service.preview(operation, payload, "olle")
                if kind != "creative":
                    self.assertEqual(change["proposed_after"]["status"], "PAUSED")
                result = self.service.apply(change["change_id"], confirm=True, actor="olle")
                self.assertEqual(result["verification_result"], "VERIFIED")

    def test_failure_and_readback_mismatch_do_not_retry(self):
        change = self.service.preview("status_change", {"object_type": "ad", "object_id": "300", "status": "ACTIVE"}, "olle")
        self.meta.fail_write = True
        with self.assertRaises(MetaError):
            self.service.apply(change["change_id"], confirm=True, actor="olle")
        self.assertEqual(self.store.audit[-1]["verification_result"], "META_WRITE_FAILED")
        self.error("change_already_used", lambda: self.service.apply(change["change_id"], confirm=True, actor="olle"))
        self.meta.fail_write = False
        change = self.service.preview("status_change", {"object_type": "ad", "object_id": "300", "status": "ACTIVE"}, "olle")
        self.meta.fail_readback = True
        self.error("read_back_mismatch", lambda: self.service.apply(change["change_id"], confirm=True, actor="olle"))
        self.assertEqual(self.store.audit[-1]["verification_result"], "READ_BACK_MISMATCH")

    def test_credential_and_flag_are_fail_closed(self):
        self.env["META_ADS_WRITE_ENABLED"] = "false"
        self.error("ads_write_disabled", lambda: self.service.preview("status_change", {}, "olle"))
        self.env["META_ADS_WRITE_ENABLED"] = "true"
        self.meta.permissions.remove("ads_management")
        self.error("ads_write_permission_missing", lambda: self.service.preview("status_change", {}, "olle"))

    def test_account_scope_and_creation_validation(self):
        self.env["META_AD_ACCOUNT_ID"] = "221082042"
        self.error("account_not_allowed", self.service.account)
        self.env["META_AD_ACCOUNT_ID"] = "1689435278682373"
        base = {"campaign_id": "100", "name": "Test", "billing_event": "IMPRESSIONS",
                "optimization_goal": "LINK_CLICKS", "targeting": {"geo_locations": {"countries": ["SE"]}}}
        self.error("adset_budget_required", lambda: self.service.preview("create_adset", base, "olle"))
        self.error("lifetime_budget_end_time_required", lambda: self.service.preview("create_adset", {
            **base, "lifetime_budget_minor": 50000}, "olle"))
        self.error("unsupported_bid_strategy", lambda: self.service.preview("create_adset", {
            **base, "daily_budget_minor": 50000, "bid_strategy": "COST_CAP"}, "olle"))
        self.error("invalid_object_story_id", lambda: self.service.preview("create_creative", {
            "name": "Wrong Page", "object_story_id": "868_999"}, "olle"))

    def test_budget_cap_rechecked_at_apply(self):
        change = self.service.preview("budget_change", {"object_type": "adset", "object_id": "200",
            "budget_type": "daily_budget", "amount_minor": 70000}, "olle")
        self.env["META_ADS_MAX_DAILY_BUDGET_MINOR"] = "60000"
        self.error("budget_cap_exceeded", lambda: self.service.apply(change["change_id"], confirm=True, actor="olle"))
        self.assertFalse(self.meta.posts)

    def test_route_rejects_non_admin_and_unauthenticated(self):
        import app
        client = app.app.test_client()
        self.assertEqual(client.post("/meta/ads/status/preview", json={}).status_code, 401)
        self.assertEqual(client.get("/meta/ads/account").status_code, 401)
        self.assertEqual(client.get("/meta/instagram/media").status_code, 401)
        with client.session_transaction() as session:
            session["user"] = {"user_name": "seller", "admin": "N"}
        response = client.post("/meta/ads/status/preview", json={})
        self.assertEqual((response.status_code, response.json["error"]), (403, "admin_required"))
        self.assertEqual(client.post("/meta/ads/changes/d12003b5-e200-41d5-aab5-f058afe4bedf/apply", json={"confirm": True}).status_code, 403)

    def test_sheets_journal_persists_preview_claim_and_audit(self):
        sheets = {}

        def sheet(_spreadsheet, name, columns, rows=1000):
            return sheets.setdefault(name, {"columns": columns, "rows": []})

        def find(worksheet, column, value):
            for index, row in enumerate(worksheet["rows"], 2):
                if row.get(column) == value:
                    return index, worksheet["columns"], row
            return None, worksheet["columns"], None

        def append(worksheet, _columns, values):
            worksheet["rows"].append(dict(values))

        def update(worksheet, index, _headers, values):
            worksheet["rows"][index - 2].update(values)

        store = SheetsChangeStore(lambda: object(), sheet, find, append, update)
        change = self.service.preview("status_change", {"object_type": "ad", "object_id": "300",
            "status": "ACTIVE"}, "olle")
        saved = self.store.load_preview(change["change_id"])
        store.save_preview(saved)
        self.assertEqual(store.load_preview(change["change_id"])["status"], "PENDING")
        store.mark_applying(change["change_id"], "admin", self.clock.isoformat())
        self.assertEqual(store.load_preview(change["change_id"])["status"], "APPLYING")
        store.finish(change["change_id"], "300", {"status": "ACTIVE"}, "VERIFIED", "trace-1")
        self.assertEqual(store.load_preview(change["change_id"])["status"], "VERIFIED")
        audit = sheets["meta_ads_audit"]["rows"][0]
        self.assertEqual((audit["approved_by"], audit["verification_result"], audit["meta_request_id"]),
                         ("admin", "VERIFIED", "trace-1"))


if __name__ == "__main__":
    from unittest import main
    main()
