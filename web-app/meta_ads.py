"""Bounded Meta Ads reads and preview/apply writes for one owned ad account."""
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
import re
import threading
import uuid

from meta_client import MetaClient, MetaError

ACCOUNT_FIELDS = "id,name,account_id,currency,timezone_name,account_status,min_daily_budget"
CAMPAIGN_FIELDS = "id,account_id,name,objective,buying_type,special_ad_categories,status,effective_status,daily_budget,lifetime_budget,bid_strategy"
ADSET_FIELDS = "id,account_id,campaign_id,name,status,effective_status,daily_budget,lifetime_budget,billing_event,optimization_goal,bid_strategy,start_time,end_time,targeting"
AD_FIELDS = "id,account_id,adset_id,name,status,effective_status,creative{id,name,object_story_spec,object_story_id}"
CREATIVE_FIELDS = "id,account_id,name,object_story_spec,object_story_id,asset_feed_spec"
INSIGHT_FIELDS = "spend,impressions,reach,frequency,clicks,inline_link_clicks,ctr,cpc,cpm,actions,action_values,cost_per_action_type"
FIELDS = {"campaign": CAMPAIGN_FIELDS, "adset": ADSET_FIELDS, "ad": AD_FIELDS, "creative": CREATIVE_FIELDS}
EDGE = {"campaign": "campaigns", "adset": "adsets", "ad": "ads", "creative": "adcreatives"}
OBJECTIVE = {"OUTCOME_AWARENESS", "OUTCOME_TRAFFIC", "OUTCOME_ENGAGEMENT", "OUTCOME_LEADS", "OUTCOME_APP_PROMOTION", "OUTCOME_SALES"}
BREAKDOWNS = {"age", "gender", "country", "region", "dma", "publisher_platform", "platform_position", "device_platform", "impression_device"}
LEVELS = {"account", "campaign", "adset", "ad"}
_ID = re.compile(r"\d{1,40}")
_NAME = re.compile(r"[^\x00-\x1f]{1,255}")
_MINOR_UNITS = {"SEK": 100, "EUR": 100, "USD": 100, "GBP": 100, "DKK": 100, "NOK": 100, "JPY": 1, "ISK": 1}
POLARBAR_AD_ACCOUNT_ID = "1689435278682373"
POLARBAR_PAGE_ID = "868369943031594"


class AdsError(Exception):
    def __init__(self, code, status=400):
        self.code, self.status = code, status
        super().__init__(code)


def _require_id(value):
    if not _ID.fullmatch(str(value or "")):
        raise AdsError("invalid_id")
    return str(value)


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _fingerprint(value):
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _name(value):
    if not isinstance(value, str) or not _NAME.fullmatch(value.strip()):
        raise AdsError("invalid_name")
    return value.strip()


def _minor(value):
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise AdsError("invalid_budget_minor")
    return value


def _positive_cap(env, name):
    try:
        value = int(env.get(name, ""))
        return value if value > 0 else None
    except (ValueError, TypeError):
        return None


def _diff(before, after):
    keys = set(before) | set(after)
    return {key: {"before": before.get(key), "after": after.get(key)}
            for key in sorted(keys) if before.get(key) != after.get(key)}


class AdsService:
    def __init__(self, store, client=None, env=None, now=None):
        self.env = os.environ if env is None else env
        self.client = client or MetaClient(self.env)
        self.store = store
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.lock = threading.RLock()

    @property
    def account_id(self):
        configured = _require_id(self.env.get("META_AD_ACCOUNT_ID", POLARBAR_AD_ACCOUNT_ID))
        if configured != POLARBAR_AD_ACCOUNT_ID:
            raise AdsError("account_not_allowed", 403)
        return "act_" + configured

    def account(self):
        body = self.client.ads_get(self.account_id, {"fields": ACCOUNT_FIELDS})
        if str(body.get("id")) != self.account_id:
            raise AdsError("account_mismatch", 502)
        return body

    def _owned(self, row):
        if str(row.get("account_id", "")) != self.account_id[4:]:
            raise AdsError("object_outside_account", 403)
        return row

    def get(self, kind, identifier):
        if kind not in FIELDS:
            raise AdsError("invalid_object_type")
        row = self.client.ads_get(_require_id(identifier), {"fields": FIELDS[kind]})
        if str(row.get("id")) != str(identifier):
            raise AdsError("object_mismatch", 502)
        return self._owned(row)

    def list(self, kind, *, limit=100, max_pages=5):
        if kind not in FIELDS:
            raise AdsError("invalid_object_type")
        if not isinstance(limit, int) or not 1 <= limit <= 100:
            raise AdsError("invalid_limit")
        output, cursor, seen = [], None, set()
        for _ in range(max_pages):
            params = {"fields": FIELDS[kind], "limit": min(limit, 100)}
            if cursor:
                params["after"] = cursor
            body = self.client.ads_get(f"{self.account_id}/{EDGE[kind]}", params)
            rows = body.get("data")
            if not isinstance(rows, list):
                raise AdsError("invalid_collection", 502)
            output.extend(self._owned(row) for row in rows)
            paging = body.get("paging") or {}
            if paging.get("next"):
                self.client.safe_next(paging["next"])
            cursor = (paging.get("cursors") or {}).get("after")
            if not cursor or cursor in seen or len(output) >= limit:
                break
            seen.add(cursor)
        return output[:limit]

    def get_targeting(self, adset_id):
        return self.get("adset", adset_id).get("targeting") or {}

    # Named read operations keep callers away from Graph paths and field lists.
    def get_ad_account(self):
        return self.account()

    def list_campaigns(self, **kwargs):
        return self.list("campaign", **kwargs)

    def get_campaign(self, identifier):
        return self.get("campaign", identifier)

    def list_adsets(self, **kwargs):
        return self.list("adset", **kwargs)

    def get_adset(self, identifier):
        return self.get("adset", identifier)

    def list_ads(self, **kwargs):
        return self.list("ad", **kwargs)

    def get_ad(self, identifier):
        return self.get("ad", identifier)

    def list_creatives(self, **kwargs):
        return self.list("creative", **kwargs)

    def get_creative(self, identifier):
        return self.get("creative", identifier)

    def get_insights(self, query):
        return self.insights(query)

    def insights(self, query):
        if not isinstance(query, dict):
            raise AdsError("invalid_query")
        level = query.get("level", "account")
        if level not in LEVELS:
            raise AdsError("invalid_level")
        params = {"level": level, "fields": INSIGHT_FIELDS, "limit": 100}
        if "after" in query:
            cursor = query["after"]
            if not isinstance(cursor, str) or not re.fullmatch(r"[A-Za-z0-9_=-]{1,500}", cursor):
                raise AdsError("invalid_cursor")
            params["after"] = cursor
        for key in ("since", "until"):
            if key in query:
                try:
                    params.setdefault("time_range", {})[key] = date.fromisoformat(query[key]).isoformat()
                except (ValueError, TypeError):
                    raise AdsError("invalid_date") from None
        if "time_range" in params:
            if set(params["time_range"]) != {"since", "until"} or params["time_range"]["since"] > params["time_range"]["until"]:
                raise AdsError("invalid_date_range")
            params["time_range"] = _canonical(params["time_range"])
        elif "date_preset" in query:
            preset = query["date_preset"]
            if preset not in {"today", "yesterday", "last_7d", "last_14d", "last_30d", "last_90d", "this_month", "last_month", "maximum"}:
                raise AdsError("invalid_date_preset")
            params["date_preset"] = preset
        else:
            params["date_preset"] = "last_30d"
        increment = query.get("time_increment", "all_days")
        if increment != "all_days" and (isinstance(increment, bool) or not isinstance(increment, int) or not 1 <= increment <= 90):
            raise AdsError("invalid_time_increment")
        params["time_increment"] = increment
        breakdowns = query.get("breakdowns", [])
        if not isinstance(breakdowns, list) or len(breakdowns) > 3 or any(item not in BREAKDOWNS for item in breakdowns):
            raise AdsError("invalid_breakdowns")
        if breakdowns:
            params["breakdowns"] = ",".join(breakdowns)
        filtering = query.get("filtering", [])
        if not isinstance(filtering, list) or len(filtering) > 10 or len(_canonical(filtering)) > 4000:
            raise AdsError("invalid_filtering")
        if filtering:
            params["filtering"] = _canonical(filtering)
        body = self.client.ads_get(f"{self.account_id}/insights", params)
        if not isinstance(body.get("data"), list):
            raise AdsError("invalid_insights", 502)
        paging = body.get("paging") or {}
        if paging.get("next"):
            self.client.safe_next(paging["next"])
        return {"data": body["data"], "paging": {"cursors": paging.get("cursors", {})}, "currency": self.account().get("currency")}

    def _credentials_ready(self):
        if not self.env.get("META_ADS_ACCESS_TOKEN"):
            raise AdsError("ads_write_credential_missing", 503)
        # The Graph permission read uses the Ads token, never the Instagram fallback.
        body = self.client.get("me/permissions", {"limit": 100}, credential="ads_write")
        granted = {row.get("permission") for row in body.get("data", []) if row.get("status") == "granted"}
        if not {"ads_read", "ads_management"}.issubset(granted):
            raise AdsError("ads_write_permission_missing", 503)
        self.account()

    def _write_guard(self):
        if str(self.env.get("META_ADS_WRITE_ENABLED", "false")).lower() != "true":
            raise AdsError("ads_write_disabled", 503)
        self._credentials_ready()

    def capability_status(self):
        configured = bool(self.env.get("META_ADS_ACCESS_TOKEN") or self.env.get("INSTAGRAM_ACCESS_TOKEN"))
        ready = False
        if str(self.env.get("META_ADS_WRITE_ENABLED", "false")).lower() == "true":
            try:
                self._credentials_ready()
                ready = True
            except (MetaError, AdsError):
                pass
        return {"ads_read_configured": configured, "ads_write_enabled": ready,
                "instagram_comments_read_configured": bool(self.env.get("INSTAGRAM_ACCESS_TOKEN")),
                "graph_api_version": self.env.get("INSTAGRAM_GRAPH_API_VERSION", "v26.0")}

    @staticmethod
    def _locations(values):
        if not isinstance(values, list) or len(values) > 100:
            raise AdsError("invalid_locations")
        output = []
        for row in values:
            if not isinstance(row, dict) or set(row) != {"latitude", "longitude", "radius", "distance_unit"}:
                raise AdsError("invalid_location")
            latitude, longitude, radius = row["latitude"], row["longitude"], row["radius"]
            if (any(isinstance(v, bool) or not isinstance(v, (int, float)) for v in (latitude, longitude, radius))
                    or not -90 <= latitude <= 90 or not -180 <= longitude <= 180 or not 1 <= radius <= 80
                    or row["distance_unit"] not in {"kilometer", "mile"}):
                raise AdsError("invalid_location")
            output.append({"latitude": latitude, "longitude": longitude, "radius": radius,
                           "distance_unit": row["distance_unit"]})
        return output

    def _targeting_after(self, current, mutation):
        if not isinstance(current, dict) or not isinstance(mutation, dict):
            raise AdsError("invalid_targeting")
        mode = mutation.get("mode")
        proposed = deepcopy(current)
        geo = proposed.setdefault("geo_locations", {})
        if not isinstance(geo, dict):
            raise AdsError("invalid_current_targeting", 502)
        if mode == "replace_geo_locations":
            if set(mutation) != {"mode", "geo_locations"}:
                raise AdsError("invalid_targeting_mutation")
            value = mutation.get("geo_locations")
            allowed = {"countries", "regions", "cities", "zips", "custom_locations"}
            if not isinstance(value, dict) or not value or set(value) - allowed:
                raise AdsError("invalid_geo_locations")
            replacement = deepcopy(value)
            if "custom_locations" in replacement:
                replacement["custom_locations"] = self._locations(replacement["custom_locations"])
            for key in ("countries", "regions", "cities", "zips"):
                if key in replacement and (not isinstance(replacement[key], list) or len(replacement[key]) > 100):
                    raise AdsError("invalid_geo_locations")
                if key == "countries" and key in replacement and any(
                    not isinstance(item, str) or not re.fullmatch(r"[A-Z]{2}", item) for item in replacement[key]
                ):
                    raise AdsError("invalid_geo_locations")
                if key in {"regions", "cities", "zips"} and key in replacement and any(
                    not isinstance(item, dict) or set(item) != {"key"} or
                    not _ID.fullmatch(str(item.get("key", ""))) for item in replacement[key]
                ):
                    raise AdsError("invalid_geo_locations")
            if not any(isinstance(items, list) and items for items in replacement.values()):
                raise AdsError("empty_geo_locations")
            proposed["geo_locations"] = replacement
        elif mode in {"add_custom_locations", "remove_custom_locations"}:
            if set(mutation) != {"mode", "custom_locations"}:
                raise AdsError("invalid_targeting_mutation")
            locations = self._locations(mutation.get("custom_locations"))
            old = geo.get("custom_locations", [])
            if not isinstance(old, list):
                raise AdsError("invalid_current_targeting", 502)
            keys = {_canonical(row) for row in locations}
            if mode == "add_custom_locations":
                geo["custom_locations"] = old + [row for row in locations if _canonical(row) not in {_canonical(item) for item in old}]
            else:
                geo["custom_locations"] = [row for row in old if _canonical(row) not in keys]
        elif mode == "set_age_range":
            if set(mutation) != {"mode", "age_min", "age_max"}:
                raise AdsError("invalid_targeting_mutation")
            minimum, maximum = mutation.get("age_min"), mutation.get("age_max")
            if any(isinstance(v, bool) or not isinstance(v, int) for v in (minimum, maximum)) or not 18 <= minimum <= maximum <= 65:
                raise AdsError("invalid_age_range")
            proposed["age_min"], proposed["age_max"] = minimum, maximum
        elif mode == "set_platforms":
            allowed = {"publisher_platforms": {"facebook", "instagram"},
                       "facebook_positions": {"feed", "story", "reels"},
                       "instagram_positions": {"stream", "story", "reels", "explore"},
                       "device_platforms": {"mobile", "desktop"}}
            if not (set(mutation) - {"mode"}) <= set(allowed) or "publisher_platforms" not in mutation:
                raise AdsError("invalid_platforms")
            for key in allowed:
                if key in mutation:
                    values = mutation[key]
                    if not isinstance(values, list) or not values or len(values) > 30 or any(v not in allowed[key] for v in values):
                        raise AdsError("invalid_platforms")
                    proposed[key] = values
            if "instagram" not in proposed["publisher_platforms"] and proposed.get("instagram_positions"):
                raise AdsError("platform_positions_conflict")
            if "facebook" not in proposed["publisher_platforms"] and proposed.get("facebook_positions"):
                raise AdsError("platform_positions_conflict")
        elif mode == "replace_interests":
            if set(mutation) != {"mode", "interests"}:
                raise AdsError("invalid_targeting_mutation")
            interests = mutation.get("interests")
            if not isinstance(interests, list) or len(interests) > 100 or any(
                not isinstance(row, dict) or set(row) - {"id", "name"} or
                not _ID.fullmatch(str(row.get("id", ""))) or
                ("name" in row and (not isinstance(row["name"], str) or len(row["name"]) > 255))
                for row in interests
            ):
                raise AdsError("invalid_interests")
            proposed["interests"] = [{"id": str(row["id"]), **({"name": row["name"]} if isinstance(row.get("name"), str) else {})} for row in interests]
        else:
            raise AdsError("invalid_targeting_mutation")
        if proposed == current:
            raise AdsError("no_change")
        return proposed

    def _budget_after(self, kind, current, payload):
        if kind not in {"campaign", "adset"}:
            raise AdsError("budget_object_unsupported")
        budget_type = payload.get("budget_type")
        if budget_type not in {"daily_budget", "lifetime_budget"}:
            raise AdsError("invalid_budget_type")
        amount = _minor(payload.get("amount_minor"))
        cap_name = "META_ADS_MAX_DAILY_BUDGET_MINOR" if budget_type == "daily_budget" else "META_ADS_MAX_LIFETIME_BUDGET_MINOR"
        cap = _positive_cap(self.env, cap_name)
        if cap is None:
            raise AdsError("budget_cap_required", 409)
        if amount > cap:
            raise AdsError("budget_cap_exceeded", 409)
        account = self.account()
        currency = account.get("currency")
        if currency not in _MINOR_UNITS:
            raise AdsError("unsupported_currency", 409)
        if kind == "adset":
            campaign = self.get("campaign", current["campaign_id"])
            if campaign.get("daily_budget") or campaign.get("lifetime_budget"):
                raise AdsError("campaign_budget_controls_adset", 409)
        proposed = deepcopy(current)
        proposed[budget_type] = str(amount)
        other = "lifetime_budget" if budget_type == "daily_budget" else "daily_budget"
        if current.get(other):
            raise AdsError("budget_type_switch_unsupported", 409)
        try:
            old_minor = int(current.get(budget_type) or 0)
        except (TypeError, ValueError):
            raise AdsError("invalid_current_budget", 502) from None
        return proposed, {"currency": currency, "minor_unit_factor": _MINOR_UNITS[currency],
                          "before_minor": old_minor, "after_minor": amount,
                          "delta_minor": amount - old_minor,
                          "delta_percent": round((amount - old_minor) * 100 / old_minor, 2) if old_minor else None}

    def _create_fields(self, kind, payload):
        if not isinstance(payload, dict):
            raise AdsError("invalid_payload")
        if kind == "campaign":
            allowed = {"name", "objective", "buying_type", "special_ad_categories"}
            if set(payload) - allowed or payload.get("objective") not in OBJECTIVE:
                raise AdsError("invalid_campaign_fields")
            categories = payload.get("special_ad_categories", [])
            if not isinstance(categories, list) or len(categories) > 4 or any(c not in {"NONE", "CREDIT", "EMPLOYMENT", "HOUSING", "ISSUES_ELECTIONS_POLITICS"} for c in categories):
                raise AdsError("invalid_special_ad_categories")
            fields = {"name": _name(payload.get("name")), "objective": payload["objective"],
                      "special_ad_categories": categories, "status": "PAUSED"}
            if "buying_type" in payload:
                if payload["buying_type"] != "AUCTION":
                    raise AdsError("unsupported_buying_type")
                fields["buying_type"] = "AUCTION"
            return fields
        if kind == "adset":
            allowed = {"campaign_id", "name", "daily_budget_minor", "lifetime_budget_minor", "billing_event", "optimization_goal", "bid_strategy", "start_time", "end_time", "targeting"}
            if set(payload) - allowed or ("daily_budget_minor" in payload and "lifetime_budget_minor" in payload):
                raise AdsError("invalid_adset_fields")
            campaign = self.get("campaign", _require_id(payload.get("campaign_id")))
            campaign_budget = bool(campaign.get("daily_budget") or campaign.get("lifetime_budget"))
            if campaign_budget:
                if "daily_budget_minor" in payload or "lifetime_budget_minor" in payload:
                    raise AdsError("campaign_budget_controls_adset", 409)
            elif "daily_budget_minor" not in payload and "lifetime_budget_minor" not in payload:
                raise AdsError("adset_budget_required", 409)
            fields = {"campaign_id": campaign["id"], "name": _name(payload.get("name")), "status": "PAUSED"}
            if payload.get("billing_event") not in {"IMPRESSIONS", "LINK_CLICKS"}:
                raise AdsError("invalid_billing_event")
            if payload.get("optimization_goal") not in {"REACH", "IMPRESSIONS", "LINK_CLICKS", "LANDING_PAGE_VIEWS", "POST_ENGAGEMENT", "LEAD_GENERATION", "OFFSITE_CONVERSIONS"}:
                raise AdsError("invalid_optimization_goal")
            fields.update(billing_event=payload["billing_event"], optimization_goal=payload["optimization_goal"])
            compatible = {"OUTCOME_AWARENESS": {"REACH", "IMPRESSIONS"}, "OUTCOME_TRAFFIC": {"LINK_CLICKS", "LANDING_PAGE_VIEWS"},
                          "OUTCOME_ENGAGEMENT": {"POST_ENGAGEMENT"}, "OUTCOME_LEADS": {"LEAD_GENERATION"}, "OUTCOME_SALES": {"OFFSITE_CONVERSIONS"}}
            if fields["optimization_goal"] not in compatible.get(campaign.get("objective"), set()):
                raise AdsError("objective_optimization_mismatch")
            if "bid_strategy" in payload:
                if payload["bid_strategy"] != "LOWEST_COST_WITHOUT_CAP":
                    raise AdsError("unsupported_bid_strategy")
                fields["bid_strategy"] = payload["bid_strategy"]
            for source, target in (("daily_budget_minor", "daily_budget"), ("lifetime_budget_minor", "lifetime_budget")):
                if source in payload:
                    self._budget_after("adset", {"campaign_id": campaign["id"]}, {"budget_type": target, "amount_minor": payload[source]})
                    fields[target] = str(payload[source])
            schedule = {}
            for key in ("start_time", "end_time"):
                if key in payload:
                    try:
                        parsed = datetime.fromisoformat(payload[key].replace("Z", "+00:00"))
                        if parsed.tzinfo is None:
                            raise ValueError()
                    except (ValueError, AttributeError):
                        raise AdsError("invalid_schedule") from None
                    schedule[key] = parsed
                    fields[key] = parsed.isoformat()
            if "lifetime_budget" in fields and "end_time" not in fields:
                raise AdsError("lifetime_budget_end_time_required")
            if "end_time" in schedule and "start_time" in schedule and schedule["end_time"] <= schedule["start_time"]:
                raise AdsError("invalid_schedule")
            target = payload.get("targeting")
            if not isinstance(target, dict) or not isinstance(target.get("geo_locations"), dict) or len(_canonical(target)) > 16000:
                raise AdsError("invalid_targeting")
            if set(target) - {"geo_locations", "age_min", "age_max", "genders", "publisher_platforms", "facebook_positions", "instagram_positions", "device_platforms"}:
                raise AdsError("unsupported_targeting_fields")
            if set(target["geo_locations"]) - {"countries", "custom_locations"} or not target["geo_locations"]:
                raise AdsError("unsupported_geo_locations")
            if "countries" in target["geo_locations"] and (
                not isinstance(target["geo_locations"]["countries"], list) or
                not target["geo_locations"]["countries"] or
                any(not isinstance(item, str) or not re.fullmatch(r"[A-Z]{2}", item)
                    for item in target["geo_locations"]["countries"])
            ):
                raise AdsError("invalid_geo_locations")
            if "custom_locations" in target["geo_locations"]:
                if not self._locations(target["geo_locations"]["custom_locations"]):
                    raise AdsError("invalid_geo_locations")
            for key in ("age_min", "age_max"):
                if key in target and (isinstance(target[key], bool) or not isinstance(target[key], int) or not 18 <= target[key] <= 65):
                    raise AdsError("invalid_age_range")
            if target.get("age_min", 18) > target.get("age_max", 65):
                raise AdsError("invalid_age_range")
            if "genders" in target and (not isinstance(target["genders"], list) or not target["genders"] or
                                        any(value not in {1, 2} for value in target["genders"])):
                raise AdsError("invalid_genders")
            platform_values = {"publisher_platforms": {"facebook", "instagram"},
                               "facebook_positions": {"feed", "story", "reels"},
                               "instagram_positions": {"stream", "story", "reels", "explore"},
                               "device_platforms": {"mobile", "desktop"}}
            for key, allowed_values in platform_values.items():
                if key in target and (not isinstance(target[key], list) or not target[key] or
                                      any(value not in allowed_values for value in target[key])):
                    raise AdsError("invalid_platforms")
            fields["targeting"] = deepcopy(target)
            return fields
        if kind == "creative":
            allowed = {"name", "object_story_id"}
            if set(payload) - allowed or "object_story_id" not in payload:
                raise AdsError("invalid_creative_fields")
            fields = {"name": _name(payload.get("name"))}
            if not isinstance(payload["object_story_id"], str) or not re.fullmatch(POLARBAR_PAGE_ID + r"_\d+", payload["object_story_id"]):
                raise AdsError("invalid_object_story_id")
            fields["object_story_id"] = payload["object_story_id"]
            return fields
        if kind == "ad":
            if set(payload) != {"adset_id", "creative_id", "name"}:
                raise AdsError("invalid_ad_fields")
            adset = self.get("adset", _require_id(payload["adset_id"]))
            creative = self.get("creative", _require_id(payload["creative_id"]))
            return {"adset_id": adset["id"], "creative": {"creative_id": creative["id"]},
                    "name": _name(payload["name"]), "status": "PAUSED"}
        raise AdsError("invalid_object_type")

    def _state(self, operation, payload):
        if operation in {"targeting_change", "budget_change", "status_change"}:
            kind = payload.get("object_type", "adset" if operation == "targeting_change" else None)
            if operation == "targeting_change" and kind != "adset":
                raise AdsError("targeting_requires_adset")
            if operation == "budget_change" and kind not in {"campaign", "adset"}:
                raise AdsError("budget_object_unsupported")
            if operation == "status_change" and kind not in {"campaign", "adset", "ad"}:
                raise AdsError("status_object_unsupported")
            row = self.get(kind, _require_id(payload.get("object_id")))
            return kind, row["id"], row
        if operation.startswith("create_"):
            kind = operation.removeprefix("create_")
            if kind == "campaign" or kind == "creative":
                return kind, self.account_id, self.account()
            if kind == "adset":
                campaign = self.get("campaign", _require_id(payload.get("campaign_id")))
                return kind, campaign["id"], campaign
            if kind == "ad":
                adset = self.get("adset", _require_id(payload.get("adset_id")))
                creative = self.get("creative", _require_id(payload.get("creative_id")))
                return kind, adset["id"], {"adset": adset, "creative": creative}
        raise AdsError("invalid_operation")

    def preview(self, operation, payload, actor):
        self._write_guard()
        if not isinstance(payload, dict) or len(_canonical(payload)) > 20000:
            raise AdsError("invalid_payload")
        kind, object_id, before = self._state(operation, payload)
        extra = {}
        if operation == "targeting_change":
            after = deepcopy(before)
            after["targeting"] = self._targeting_after(before.get("targeting") or {}, payload.get("mutation"))
            diff = {"targeting": {"before": before.get("targeting") or {}, "after": after["targeting"]}}
        elif operation == "budget_change":
            after, extra = self._budget_after(kind, before, payload)
            diff = _diff(before, after)
        elif operation == "status_change":
            if payload.get("status") not in {"PAUSED", "ACTIVE"}:
                raise AdsError("invalid_status")
            after = deepcopy(before)
            after["status"] = payload["status"]
            diff = _diff(before, after)
        else:
            after = self._create_fields(kind, payload)
            diff = {key: {"before": None, "after": value} for key, value in after.items()}
            if operation == "create_adset":
                for budget_key in ("daily_budget", "lifetime_budget"):
                    if budget_key in after:
                        _proposed, extra = self._budget_after("adset", {"campaign_id": before["id"]}, {
                            "budget_type": budget_key, "amount_minor": int(after[budget_key])})
        if not diff:
            raise AdsError("no_change")
        created = self.now().astimezone(timezone.utc)
        try:
            ttl = min(3600, max(900, int(self.env.get("META_ADS_CHANGE_TTL_SECONDS", "1800"))))
        except (TypeError, ValueError):
            ttl = 1800
        change = {"change_id": str(uuid.uuid4()), "operation": operation, "object_type": kind,
                  "object_id": object_id, "before": before, "proposed_after": after, "diff": diff,
                  "state_fingerprint": _fingerprint(before), "created_at": created.isoformat(),
                  "expires_at": (created + timedelta(seconds=ttl)).isoformat(),
                  "requested_by": actor, "status": "PENDING", "extra": extra}
        # All data needed to apply is server-side. The caller only receives the ID and diff.
        self.store.save_preview(change)
        return {key: change[key] for key in ("change_id", "operation", "object_type", "object_id", "before", "proposed_after", "diff", "state_fingerprint", "created_at", "expires_at", "extra")}

    @staticmethod
    def _form(fields):
        return {key: _canonical(value) if isinstance(value, (dict, list)) else value for key, value in fields.items()}

    def _perform(self, change):
        kind, operation, proposed = change["object_type"], change["operation"], change["proposed_after"]
        if operation == "targeting_change":
            body = self.client._ads_post(change["object_id"], {"targeting": _canonical(proposed["targeting"])})
            return change["object_id"], body
        if operation == "budget_change":
            key = next(iter(change["diff"]))
            body = self.client._ads_post(change["object_id"], {key: proposed[key]})
            return change["object_id"], body
        if operation == "status_change":
            body = self.client._ads_post(change["object_id"], {"status": proposed["status"]})
            return change["object_id"], body
        body = self.client._ads_post(f"{self.account_id}/{EDGE[kind]}", self._form(proposed))
        identifier = body.get("id")
        if not _ID.fullmatch(str(identifier or "")):
            raise AdsError("create_response_missing_id", 502)
        return str(identifier), body

    @staticmethod
    def _verify(change, readback):
        operation, after = change["operation"], change["proposed_after"]
        if operation == "targeting_change":
            return readback.get("targeting") == after["targeting"]
        if operation == "budget_change":
            key = next(iter(change["diff"]))
            return str(readback.get(key)) == str(after[key])
        if operation == "status_change":
            return readback.get("status") == after["status"]
        if readback.get("status") != "PAUSED" and change["object_type"] != "creative":
            return False
        for key in ("name", "objective", "campaign_id", "adset_id"):
            if key in after and str(readback.get(key)) != str(after[key]):
                return False
        for key in ("daily_budget", "lifetime_budget", "billing_event", "optimization_goal", "bid_strategy", "object_story_id"):
            if key in after and str(readback.get(key)) != str(after[key]):
                return False
        if "targeting" in after and readback.get("targeting") != after["targeting"]:
            return False
        if change["object_type"] == "ad":
            return str((readback.get("creative") or {}).get("id")) == str(after["creative"]["creative_id"])
        return True

    def apply(self, change_id, *, confirm, actor):
        if not confirm:
            raise AdsError("confirmation_required", 409)
        try:
            uuid.UUID(str(change_id))
        except (ValueError, TypeError):
            raise AdsError("invalid_change_id") from None
        self._write_guard()
        with self.lock:
            change = self.store.load_preview(change_id)
            if change is None:
                raise AdsError("change_not_found", 404)
            if change.get("status") != "PENDING":
                raise AdsError("change_already_used", 409)
            if self.now() >= datetime.fromisoformat(change["expires_at"]):
                raise AdsError("preview_expired", 409)
            kind, object_id, current = self._state(change["operation"],
                                                    {**change["proposed_after"], "object_type": change["object_type"], "object_id": change["object_id"],
                                                     "campaign_id": change["before"].get("id") if change["operation"] == "create_adset" else change["proposed_after"].get("campaign_id"),
                                                     "adset_id": change["proposed_after"].get("adset_id"),
                                                     "creative_id": (change["proposed_after"].get("creative") or {}).get("creative_id")})
            if kind != change["object_type"] or object_id != change["object_id"] or _fingerprint(current) != change["state_fingerprint"]:
                raise AdsError("STALE_STATE", 409)
            if change["operation"] == "budget_change":
                budget_key = next(iter(change["diff"]))
                _proposed, budget_info = self._budget_after(kind, current, {
                    "budget_type": budget_key, "amount_minor": int(change["proposed_after"][budget_key])})
                if budget_info["currency"] != change["extra"]["currency"]:
                    raise AdsError("STALE_STATE", 409)
            if change["operation"] == "create_adset":
                for budget_key in ("daily_budget", "lifetime_budget"):
                    if budget_key in change["proposed_after"]:
                        _proposed, budget_info = self._budget_after("adset", {"campaign_id": current["id"]}, {
                            "budget_type": budget_key, "amount_minor": int(change["proposed_after"][budget_key])})
                        if budget_info["currency"] != change["extra"].get("currency"):
                            raise AdsError("STALE_STATE", 409)
            # Persist the attempt before touching Meta. A failed read-back never permits
            # a blind retry of a potentially successful upstream mutation.
            self.store.mark_applying(change_id, actor, self.now().isoformat())
            result_id, upstream = None, None
            try:
                result_id, upstream = self._perform(change)
                readback = self.get(kind, result_id)
                verified = self._verify(change, readback)
                result = "VERIFIED" if verified else "READ_BACK_MISMATCH"
                self.store.finish(change_id, result_id, readback, result, upstream.get("__fb_trace_id__"))
                if not verified:
                    raise AdsError("read_back_mismatch", 502)
                return {"change_id": change_id, "object_type": kind, "object_id": result_id,
                        "verification_result": result, "after": readback}
            except (MetaError, AdsError):
                if result_id is None:
                    self.store.finish(change_id, change["object_id"], None, "META_WRITE_FAILED", None)
                raise
