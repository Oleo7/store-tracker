"""Pure sales-coaching normalization, attribution, and aggregation helpers.

The module deliberately has no Flask or Google Sheets dependency.  Callers load
each source once, pass plain dictionaries in, and serialize the returned data.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, time, timedelta
import math
import statistics
import time as clock
import unicodedata


DEFINITIONS_VERSION = "sales_coaching_v1"
ANALYTICS_SNAPSHOT_VERSION = "sales_coaching_v1"
ATTRIBUTION_WINDOW_DAYS = 10
MIN_RATE_SAMPLE = 10
MIN_PRIORITY_COVERAGE = 0.70

CONTACT_ANALYTICS_COLUMNS = [
    "sales_user_name",
    "customer_number",
    "contact_type_key",
    "result_class",
    "activity_source",
    "source_suggestion_id",
    "source_trigger_key",
    "analytics_snapshot_version",
    "priority_snapshot_quality",
    "priority_score_version",
    "priority_score_at_contact",
    "priority_percentile_at_contact",
    "seller_portfolio_size_at_contact",
    "intent_timing_at_contact",
    "value_index_at_contact",
    "strategic_index_at_contact",
    "expected_order_dfp_at_contact",
    "lifecycle_at_contact",
    "customer_segment_at_contact",
]

DRILLDOWN_METRICS = frozenset({
    "human_activities",
    "reach",
    "positive_dialogue",
    "order_10d",
    "waiting_outcome",
    "priority_focus",
    "bom_ratio",
    "repeat_boms",
    "high_priority_boms",
    "data_quality",
})

QUALIFIED_DIALOGUE_RESULTS = frozenset({"positive", "neutral", "negative", "order"})
ANALYSABLE_RESULTS = QUALIFIED_DIALOGUE_RESULTS | {"unreachable"}
SYNCHRONOUS_CHANNELS = frozenset({"visit", "phone"})


def _text(value):
    return str(value or "").replace("\xa0", " ").strip()


def normalize_key(value):
    text = unicodedata.normalize("NFKD", _text(value))
    text = "".join(char for char in text if not unicodedata.combining(char))
    return " ".join(text.casefold().split())


def _number(value, default=None):
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value) if math.isfinite(float(value)) else default
    text = "".join(
        char for char in _text(value).replace(" ", "")
        if char.isdigit() or char in ",.-"
    )
    if text in {"", "-", ".", ","}:
        return default
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    else:
        text = text.replace(",", ".")
    try:
        parsed = float(text)
    except (TypeError, ValueError):
        return default
    return parsed if math.isfinite(parsed) else default


def _date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = _text(value).replace("Z", "+00:00")
    if not text:
        return None
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        pass
    for fmt in ("%Y/%m/%d", "%d/%m/%Y", "%d.%m.%Y"):
        try:
            return datetime.strptime(text[:10], fmt).date()
        except ValueError:
            continue
    return None


def _datetime(value):
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, date):
        return datetime.combine(value, time.min)
    text = _text(value).replace("Z", "+00:00")
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
        return parsed.replace(tzinfo=None)
    except ValueError:
        parsed_date = _date(text)
        return datetime.combine(parsed_date, time.min) if parsed_date else None


def normalize_contact_type(value):
    key = normalize_key(value)
    mapping = {
        "visit": "visit", "besok": "visit", "fysiskt besok": "visit",
        "phone": "phone", "telefon": "phone", "samtal": "phone",
        "email": "email", "e-mail": "email", "mejl": "email", "mail": "email",
    }
    return mapping.get(key, "unknown")


def normalize_result_class(value, *, email_id="", activity_source=""):
    if _text(email_id) or normalize_key(activity_source) == "crm_email":
        return "system_email"
    key = normalize_key(value).rstrip("!")
    mapping = {
        "order": "order",
        "order lagd": "order",
        "positive": "positive",
        "intresserad/aterkom :)": "positive",
        "intresserad": "positive",
        "positiv": "positive",
        "positivt": "positive",
        "negative": "negative",
        "kraver mer bearbetning": "negative",
        "aterkom ej": "negative",
        "negativ": "negative",
        "negativt": "negative",
        "neutral": "neutral",
        "neutralt": "neutral",
        "uppfoljning behovs": "neutral",
        "ej antraffbar": "unreachable",
        "unreachable": "unreachable",
        "system_email": "system_email",
        "inte antraffbar": "unreachable",
        "bom": "unreachable",
    }
    return mapping.get(key, "unknown")


def canonical_activity_source(value, *, planned_row=None, email_id=""):
    if _text(email_id):
        return "crm_email"
    requested = normalize_key(value)
    allowed = {"manual", "planned", "follow_up", "route", "system_suggestion", "crm_email"}
    if requested in allowed:
        return requested
    source = normalize_key((planned_row or {}).get("source"))
    if source in {"follow_up", "route", "system_suggestion"}:
        return source
    if planned_row:
        return "planned"
    return "manual"


class CustomerIdentityIndex:
    """Resolve strong identities first and reject ambiguity or contradictions."""

    def __init__(self, customers):
        self.customers = list(customers or ())
        self.by_id, self.by_number, self.by_name = {}, {}, {}
        self.ambiguous = {"customer_id": set(), "customer_number": set(), "customer_name": set()}
        for customer in self.customers:
            self._add(self.by_id, self.ambiguous["customer_id"], _text(customer.get("customer_id")), customer)
            self._add(self.by_number, self.ambiguous["customer_number"], normalize_key(customer.get("customer_number")), customer)
            self._add(self.by_name, self.ambiguous["customer_name"], normalize_key(customer.get("customer")), customer)

    @staticmethod
    def _add(mapping, ambiguous, key, customer):
        if not key or key in ambiguous:
            return
        if key in mapping:
            mapping.pop(key, None)
            ambiguous.add(key)
        else:
            mapping[key] = customer

    def resolve(self, row, *, name_field="customer", number_field="customer_number"):
        requested_id = _text(row.get("customer_id"))
        requested_number = normalize_key(row.get(number_field))
        requested_name = normalize_key(row.get(name_field))
        if requested_id:
            if requested_id in self.ambiguous["customer_id"]:
                return None, "ambiguous_customer_id"
            customer = self.by_id.get(requested_id)
            if customer is None:
                return None, "unknown_customer_id"
            number_customer = self.by_number.get(requested_number) if requested_number else None
            if number_customer is not None and number_customer is not customer:
                return None, "customer_identity_conflict"
            return customer, "customer_id"
        if requested_number:
            if requested_number in self.ambiguous["customer_number"]:
                return None, "ambiguous_customer_number"
            customer = self.by_number.get(requested_number)
            if customer is None:
                return None, "unknown_customer_number"
            return customer, "customer_number"
        if not requested_name:
            return None, "missing_customer_identity"
        if requested_name in self.ambiguous["customer_name"]:
            return None, "ambiguous_customer_name"
        customer = self.by_name.get(requested_name)
        return (customer, "unique_customer_name") if customer else (None, "unknown_customer_name")

    @staticmethod
    def identity_key(customer):
        customer_id = _text((customer or {}).get("customer_id"))
        customer_number = normalize_key((customer or {}).get("customer_number"))
        name = normalize_key((customer or {}).get("customer"))
        if customer_id:
            return f"id:{customer_id}"
        if customer_number:
            return f"number:{customer_number}"
        return f"name:{name}" if name else ""


def _seller_aliases(users):
    aliases = defaultdict(set)
    canonical = {}
    for user in users or ():
        user_name = _text(user.get("user_name"))
        if not user_name:
            continue
        canonical[normalize_key(user_name)] = user_name
        for value in (user_name, user.get("name")):
            key = normalize_key(value)
            if key:
                aliases[key].add(user_name)
    return aliases, canonical


def resolve_historical_seller(activity, users):
    aliases, canonical = _seller_aliases(users)
    stable = normalize_key(activity.get("sales_user_name"))
    if stable:
        return canonical.get(stable, _text(activity.get("sales_user_name"))), "sales_user_name"
    display = normalize_key(activity.get("sales_person"))
    matches = aliases.get(display, set())
    if len(matches) == 1:
        return next(iter(matches)), "legacy_sales_person"
    return "", "ambiguous_seller" if len(matches) > 1 else "unknown_seller"


def _legacy_snapshot_maps(planning_suggestions=(), score_events=()):
    by_suggestion = {}
    for row in planning_suggestions or ():
        suggestion_id = _text(row.get("suggestion_id"))
        if suggestion_id:
            by_suggestion[suggestion_id] = row
    events_by_suggestion = defaultdict(list)
    for row in score_events or ():
        suggestion_id = _text(row.get("suggestion_id"))
        if suggestion_id:
            events_by_suggestion[suggestion_id].append(row)
    for rows in events_by_suggestion.values():
        rows.sort(key=lambda row: (_datetime(row.get("occurred_at")) or datetime.min, _text(row.get("event_id"))))
    return by_suggestion, events_by_suggestion


def resolve_priority_snapshot(activity, *, planned_by_id=None, suggestion_by_id=None, events_by_suggestion=None):
    quality = normalize_key(activity.get("priority_snapshot_quality"))
    exact_score = _number(activity.get("priority_score_at_contact"))
    if quality == "exact" or (activity.get("analytics_snapshot_version") and exact_score is not None):
        result = {key: activity.get(key, "") for key in CONTACT_ANALYTICS_COLUMNS if key.endswith("_at_contact")}
        result.update({
            "quality": "exact",
            "score_version": _text(activity.get("priority_score_version")),
            "portfolio_size": _number(activity.get("seller_portfolio_size_at_contact")),
        })
        return result

    planned = (planned_by_id or {}).get(_text(activity.get("planned_activity_id")), {})
    suggestion_id = _text(activity.get("source_suggestion_id")) or _text(planned.get("source_suggestion_id"))
    suggestion = (suggestion_by_id or {}).get(suggestion_id)
    event = None
    activity_at = _datetime(activity.get("date_time")) or datetime.max
    for candidate in (events_by_suggestion or {}).get(suggestion_id, ()):
        occurred = _datetime(candidate.get("occurred_at"))
        if occurred and occurred <= activity_at:
            event = candidate
    source = suggestion or event
    if source:
        suffix = "_at_creation" if suggestion else ""
        def value(name):
            return source.get(f"{name}{suffix}", source.get(name, ""))
        return {
            "quality": "approximate",
            "priority_score_at_contact": value("priority_score"),
            "priority_percentile_at_contact": "",
            "intent_timing_at_contact": value("intent_timing"),
            "value_index_at_contact": value("value_index"),
            "strategic_index_at_contact": value("strategic_index"),
            "expected_order_dfp_at_contact": value("expected_order_dfp"),
            "lifecycle_at_contact": value("lifecycle"),
            "customer_segment_at_contact": "",
            "score_version": _text(source.get("score_version")),
            "portfolio_size": "",
        }
    return {"quality": "missing"}


def canonicalize_activities(activities, customers, users=(), *, planned_activities=(), planning_suggestions=(), score_events=()):
    identity = CustomerIdentityIndex(customers)
    planned_by_id = {_text(row.get("planned_activity_id")): row for row in planned_activities or () if _text(row.get("planned_activity_id"))}
    suggestion_by_id, events_by_suggestion = _legacy_snapshot_maps(planning_suggestions, score_events)
    canonical, excluded = [], []
    seen_contact_ids = set()
    for source_index, raw in enumerate(activities or ()):
        row = dict(raw)
        contact_id = _text(row.get("contact_id")) or f"legacy-row-{source_index + 2}"
        if contact_id in seen_contact_ids:
            excluded.append({"contact_id": contact_id, "reason": "duplicate_contact_id"})
            continue
        seen_contact_ids.add(contact_id)
        customer, identity_quality = identity.resolve(row)
        seller, seller_quality = resolve_historical_seller(row, users)
        contact_at = _datetime(row.get("date_time"))
        channel = normalize_contact_type(row.get("contact_type_key") or row.get("contact_channel"))
        activity_source = canonical_activity_source(
            row.get("activity_source"),
            planned_row=planned_by_id.get(_text(row.get("planned_activity_id"))),
            email_id=row.get("email_id"),
        )
        result_class = normalize_result_class(
            row.get("result_class") or row.get("result"),
            email_id=row.get("email_id"),
            activity_source=activity_source,
        )
        reasons = []
        if not customer:
            reasons.append(identity_quality)
        if not seller and activity_source != "crm_email":
            reasons.append(seller_quality)
        if contact_at is None:
            reasons.append("invalid_contact_datetime")
        snapshot = resolve_priority_snapshot(
            row,
            planned_by_id=planned_by_id,
            suggestion_by_id=suggestion_by_id,
            events_by_suggestion=events_by_suggestion,
        )
        canonical.append({
            **row,
            "contact_id": contact_id,
            "contact_at": contact_at,
            "contact_date": contact_at.date() if contact_at else None,
            "contact_week": _iso_week(contact_at.date()) if contact_at else "",
            "customer_record": customer,
            "customer_identity_key": identity.identity_key(customer),
            "customer_identity_quality": identity_quality if customer else "excluded",
            "identity_exclusion_reason": "" if customer else identity_quality,
            "sales_user_name": seller,
            "seller_quality": seller_quality,
            "contact_type_key": channel,
            "result_class": result_class,
            "activity_source": activity_source,
            "is_human": activity_source != "crm_email" and not _text(row.get("email_id")),
            "snapshot": snapshot,
            "priority_snapshot_quality": snapshot.get("quality", "missing"),
            "priority_score_at_contact": _number(snapshot.get("priority_score_at_contact")),
            "priority_percentile_at_contact": _number(snapshot.get("priority_percentile_at_contact")),
            "seller_portfolio_size_at_contact": _number(snapshot.get("portfolio_size")),
            "lifecycle_at_contact": _text(snapshot.get("lifecycle_at_contact")),
            "customer_segment_at_contact": _text(snapshot.get("customer_segment_at_contact")),
            "analysis_exclusions": reasons,
        })
    return {"activities": canonical, "excluded": excluded}


def group_logical_orders(order_rows, customers):
    identity = CustomerIdentityIndex(customers)
    grouped = {}
    excluded = []
    for source_index, raw in enumerate(order_rows or ()):
        row = dict(raw)
        order_date = _date(row.get("Order date") or row.get("date"))
        quantity = _number(row.get("Quantity"), 0) or 0
        total = _number(row.get("Total"), 0) or 0
        if order_date is None or (quantity <= 0 and total <= 0):
            continue
        identity_row = {
            "customer_id": row.get("customer_id"),
            "customer_number": row.get("Customer number") or row.get("customer_number"),
            "customer": row.get("Customer") or row.get("customer"),
        }
        customer, identity_quality = identity.resolve(identity_row)
        identity_key = identity.identity_key(customer)
        if not customer:
            excluded.append({"source_row": source_index, "reason": identity_quality})
            identity_key = (
                f"unresolved:{_text(identity_row['customer_id'])}:"
                f"{normalize_key(identity_row['customer_number'])}:"
                f"{normalize_key(identity_row['customer'])}"
            )
        reference = _text(row.get("Reference") or row.get("reference"))
        currency = _text(row.get("Currency") or row.get("currency")).upper()
        grouping = (
            (identity_key, "reference", reference, order_date.isoformat(), currency)
            if reference
            else (identity_key, "fallback", order_date.isoformat(), currency)
        )
        group = grouped.setdefault(grouping, {
            "order_id": "|".join(grouping),
            "reference": reference,
            "date": order_date,
            "customer": _text((customer or {}).get("customer") or identity_row["customer"]),
            "customer_id": _text((customer or {}).get("customer_id")),
            "customer_number": _text((customer or {}).get("customer_number")),
            "customer_identity_key": identity_key if customer else "",
            "identity_quality": identity_quality if customer else "excluded",
            "identity_exclusion_reason": "" if customer else identity_quality,
            "currency": currency,
            "total": 0.0,
            "dfp": 0.0,
            "source_rows": [],
        })
        group["date"] = min(group["date"], order_date)
        group["total"] += total
        group["source_rows"].append(source_index)
        if normalize_key(row.get("Unit")) == "dfp":
            group["dfp"] += quantity
    orders = sorted(grouped.values(), key=lambda row: (row["date"], row["order_id"]))
    return {"orders": orders, "excluded": excluded}


def attribute_orders_to_contacts(activities, grouped_orders, *, generated_at, window_days=ATTRIBUTION_WINDOW_DAYS):
    generated = _datetime(generated_at) or datetime.now()
    qualified_by_customer = defaultdict(list)
    maturity = {}
    excluded_contacts = []
    for activity in activities or ():
        contact_at = activity.get("contact_at") or _datetime(activity.get("date_time"))
        contact_id = _text(activity.get("contact_id"))
        mature = bool(contact_at and (generated.date() - contact_at.date()).days >= window_days)
        maturity[contact_id] = "mature" if mature else "waiting_outcome"
        if not activity.get("is_human"):
            excluded_contacts.append({"contact_id": contact_id, "reason": "system_email"})
            continue
        if not activity.get("customer_identity_key"):
            excluded_contacts.append({"contact_id": contact_id, "reason": activity.get("identity_exclusion_reason") or "missing_customer_identity"})
            continue
        if activity.get("result_class") not in QUALIFIED_DIALOGUE_RESULTS:
            excluded_contacts.append({"contact_id": contact_id, "reason": "not_qualified_dialogue"})
            continue
        if contact_at is None:
            excluded_contacts.append({"contact_id": contact_id, "reason": "invalid_contact_datetime"})
            continue
        qualified_by_customer[activity["customer_identity_key"]].append(activity)
    for rows in qualified_by_customer.values():
        rows.sort(key=lambda row: (row["contact_at"], _text(row.get("contact_id"))))

    order_to_contact, contact_to_orders, unattributed = {}, defaultdict(list), []
    for order in grouped_orders or ():
        if order.get("date") and order["date"] > generated.date():
            unattributed.append({"order_id": order.get("order_id"), "reason": "future_order"})
            continue
        if not order.get("customer_identity_key"):
            unattributed.append({"order_id": order.get("order_id"), "reason": order.get("identity_exclusion_reason") or "missing_customer_identity"})
            continue
        eligible = [
            activity for activity in qualified_by_customer.get(order["customer_identity_key"], ())
            if 0 <= (order["date"] - activity["contact_at"].date()).days <= window_days
        ]
        if not eligible:
            unattributed.append({"order_id": order.get("order_id"), "reason": "no_qualified_contact_in_window"})
            continue
        latest = max(eligible, key=lambda row: (row["contact_at"], _text(row.get("contact_id"))))
        days = (order["date"] - latest["contact_at"].date()).days
        attribution = {
            "order_id": order["order_id"],
            "contact_id": latest["contact_id"],
            "sales_user_name": latest.get("sales_user_name", ""),
            "contact_week": latest.get("contact_week", ""),
            "days_to_order": days,
            "order": order,
        }
        order_to_contact[order["order_id"]] = attribution
        contact_to_orders[latest["contact_id"]].append(attribution)
    return {
        "order_to_contact": order_to_contact,
        "contact_to_orders": dict(contact_to_orders),
        "unattributed_orders": unattributed,
        "excluded_contacts": excluded_contacts,
        "maturity": maturity,
    }


def _iso_week(value):
    iso = value.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def _rate(numerator, denominator, *, comparison=None, minimum=MIN_RATE_SAMPLE):
    if denominator <= 0:
        status, value = "not_computable", None
    else:
        value = round(numerator / denominator, 4)
        status = "small_sample" if denominator < minimum else "sufficient"
    return {
        "value": value,
        "numerator": int(numerator),
        "denominator": int(denominator),
        "status": status,
        "comparisons": comparison or {},
    }


def _filter_activities(activities, *, start, end, seller="", channel="all", segment="all", lifecycle="all"):
    selected = []
    for row in activities:
        contact_date = row.get("contact_date")
        if not contact_date or not (start <= contact_date <= end):
            continue
        if seller and normalize_key(row.get("sales_user_name")) != normalize_key(seller):
            continue
        if channel != "all" and row.get("contact_type_key") != channel:
            continue
        row_segment = _text(row.get("customer_segment_at_contact") or (row.get("customer_record") or {}).get("customer_segment")).upper() or "missing"
        if segment != "all" and row_segment != segment:
            continue
        row_lifecycle = normalize_key(row.get("lifecycle_at_contact")) or "missing"
        if lifecycle != "all" and row_lifecycle != lifecycle:
            continue
        selected.append(row)
    return selected


def _aggregate_period(rows, attribution):
    human = [row for row in rows if row.get("is_human")]
    sync = [row for row in human if row.get("contact_type_key") in SYNCHRONOUS_CHANNELS and row.get("result_class") in ANALYSABLE_RESULTS]
    reached = [row for row in human if row.get("result_class") in QUALIFIED_DIALOGUE_RESULTS and (row.get("contact_type_key") in SYNCHRONOUS_CHANNELS or row.get("contact_type_key") == "email")]
    positive = [row for row in reached if row.get("result_class") in {"positive", "order"}]
    mature = [row for row in reached if attribution["maturity"].get(row["contact_id"]) == "mature"]
    waiting = [row for row in reached if attribution["maturity"].get(row["contact_id"]) == "waiting_outcome"]
    ordered_contacts = [row for row in mature if attribution["contact_to_orders"].get(row["contact_id"])]
    visits = [row for row in human if row.get("contact_type_key") == "visit" and row.get("result_class") in ANALYSABLE_RESULTS]
    boms = [row for row in visits if row.get("result_class") == "unreachable"]
    snapshots = [row for row in human if row.get("priority_snapshot_quality") in {"exact", "approximate"}]
    top_priority = [row for row in snapshots if row.get("priority_percentile_at_contact") is not None and row["priority_percentile_at_contact"] >= 75]
    attributed = [item for row in ordered_contacts for item in attribution["contact_to_orders"].get(row["contact_id"], ())]
    totals_by_currency = defaultdict(float)
    for item in attributed:
        order = item["order"]
        totals_by_currency[order.get("currency") or "unknown"] += order.get("total", 0)
    return {
        "rows": rows,
        "human": human,
        "sync": sync,
        "reached": reached,
        "positive": positive,
        "mature": mature,
        "waiting": waiting,
        "ordered_contacts": ordered_contacts,
        "visits": visits,
        "boms": boms,
        "snapshots": snapshots,
        "top_priority": top_priority,
        "attributed": attributed,
        "rates": {
            "reach": _rate(
                len([row for row in sync if row.get("result_class") in QUALIFIED_DIALOGUE_RESULTS]),
                len(sync),
            ),
            "positive_dialogue": _rate(len(positive), len(reached)),
            "order_10d": _rate(len(attributed), len(mature)),
            "priority_focus": _rate(len(top_priority), len(snapshots)),
            "bom_ratio": _rate(len(boms), len(visits)),
        },
        "dfp": round(sum(item["order"].get("dfp", 0) for item in attributed), 3),
        "order_value_by_currency": {key: round(value, 2) for key, value in sorted(totals_by_currency.items())},
    }


def _comparison_dates(start, end):
    days = (end - start).days + 1
    return start - timedelta(days=days), start - timedelta(days=1)


def _seller_comparison(rows, attribution, sellers):
    result = []
    for seller in sellers:
        aggregate = _aggregate_period([row for row in rows if normalize_key(row.get("sales_user_name")) == normalize_key(seller)], attribution)
        result.append({
            "seller": seller,
            "human_activities": len(aggregate["human"]),
            **aggregate["rates"],
            "snapshot_coverage": _rate(len(aggregate["snapshots"]), len(aggregate["human"]), minimum=1),
        })
    return result


def _data_quality(rows, canonical_result, order_result, attribution):
    human = [row for row in rows if row.get("is_human")]
    secure = [row for row in human if row.get("customer_identity_key")]
    historical_seller = [row for row in human if row.get("sales_user_name")]
    standardized = [row for row in human if row.get("contact_type_key") != "unknown" and row.get("result_class") != "unknown"]
    snapshot = [row for row in human if row.get("priority_snapshot_quality") in {"exact", "approximate"}]
    reasons = defaultdict(int)
    for row in human:
        for reason in row.get("analysis_exclusions", ()):
            reasons[reason] += 1
        if row.get("contact_type_key") == "unknown":
            reasons["unknown_contact_type"] += 1
        if row.get("result_class") == "unknown":
            reasons["unknown_result"] += 1
    for row in canonical_result.get("excluded", ()):
        reasons[row.get("reason") or "excluded_activity"] += 1
    status = "sufficient"
    identity_rate = len(secure) / len(human) if human else 0
    standardized_rate = len(standardized) / len(human) if human else 0
    if human and (identity_rate < 0.9 or standardized_rate < 0.9):
        status = "limited_data_quality"
    elif len(human) < MIN_RATE_SAMPLE:
        status = "small_sample"
    return {
        "status": status,
        "secure_customer_identity": _rate(len(secure), len(human), minimum=1),
        "historical_seller_identity": _rate(len(historical_seller), len(human), minimum=1),
        "standardized_activity": _rate(len(standardized), len(human), minimum=1),
        "priority_snapshot_coverage": _rate(len(snapshot), len(human), minimum=1),
        "waiting_outcome_count": sum(1 for row in human if attribution["maturity"].get(row["contact_id"]) == "waiting_outcome" and row.get("result_class") in QUALIFIED_DIALOGUE_RESULTS),
        "excluded_legacy_rows": sum(reasons.values()),
        "exclusion_reasons": dict(sorted(reasons.items())),
        "unresolved_order_rows": len(order_result.get("excluded", ())),
        "unattributed_orders": len(attribution.get("unattributed_orders", ())),
    }


def build_sales_coaching_summary(*, activities, customers, users, order_rows, planned_activities=(), planning_suggestions=(), score_events=(), current_priorities=(), start, end, generated_at, seller="", channel="all", segment="all", lifecycle="all", score_version="", on_step=None):
    start, end = _date(start), _date(end)
    generated = _datetime(generated_at) or datetime.now()
    normalization_started = clock.perf_counter()
    canonical_result = canonicalize_activities(
        activities, customers, users,
        planned_activities=planned_activities,
        planning_suggestions=planning_suggestions,
        score_events=score_events,
    )
    order_result = group_logical_orders(order_rows, customers)
    if on_step:
        on_step("calculation.sales_coaching.normalization", normalization_started, len(canonical_result["activities"]))
    attribution_started = clock.perf_counter()
    attribution = attribute_orders_to_contacts(canonical_result["activities"], order_result["orders"], generated_at=generated)
    if on_step:
        on_step("calculation.sales_coaching.attribution", attribution_started, len(order_result["orders"]))
    aggregation_started = clock.perf_counter()
    rows = _filter_activities(canonical_result["activities"], start=start, end=end, seller=seller, channel=channel, segment=segment, lifecycle=lifecycle)
    team_rows = _filter_activities(canonical_result["activities"], start=start, end=end, seller="", channel=channel, segment=segment, lifecycle=lifecycle)
    comparison_start, comparison_end = _comparison_dates(start, end)
    comparison_rows = _filter_activities(canonical_result["activities"], start=comparison_start, end=comparison_end, seller=seller, channel=channel, segment=segment, lifecycle=lifecycle)
    current, previous = _aggregate_period(rows, attribution), _aggregate_period(comparison_rows, attribution)
    for key, rate in current["rates"].items():
        rate["comparisons"]["previous_period"] = previous["rates"][key]

    seller_roles = {"saljare", "account manager", "accountmanager"}
    seller_options = sorted(
        {row.get("sales_user_name") for row in canonical_result["activities"] if row.get("sales_user_name")}
        | {
            _text(user.get("user_name")) for user in users
            if _text(user.get("user_name")) and normalize_key(user.get("role")) in seller_roles
        }
    )
    seller_comparison = _seller_comparison(team_rows, attribution, seller_options)
    team_rates = defaultdict(list)
    for item in seller_comparison:
        for key in ("reach", "positive_dialogue", "order_10d", "priority_focus", "bom_ratio"):
            if item[key]["value"] is not None:
                team_rates[key].append(item[key]["value"])
    for key, rate in current["rates"].items():
        if team_rates[key]:
            rate["comparisons"]["team_median"] = statistics.median(team_rates[key])

    repeat_customers = defaultdict(list)
    for row in current["boms"]:
        if row.get("customer_identity_key"):
            repeat_customers[row["customer_identity_key"]].append(row)
    repeated = {key: value for key, value in repeat_customers.items() if len(value) >= 2}
    high_priority_boms = [row for row in current["boms"] if row.get("priority_percentile_at_contact") is not None and row["priority_percentile_at_contact"] >= 75]
    high_priority_score_fallback = [row for row in current["boms"] if row.get("priority_percentile_at_contact") is None and row.get("priority_snapshot_quality") == "exact" and (row.get("priority_score_at_contact") or 0) >= 70]

    aliases, _canonical_sellers = _seller_aliases(users)
    portfolio = []
    for raw_priority in current_priorities or ():
        priority = dict(raw_priority)
        owner_matches = aliases.get(normalize_key(priority.get("sales_person")), set())
        owner = next(iter(owner_matches)) if len(owner_matches) == 1 else ""
        if seller and normalize_key(owner) != normalize_key(seller):
            continue
        priority_segment = _text(priority.get("segment")).upper() or "missing"
        if segment != "all" and priority_segment != segment:
            continue
        priority_lifecycle = normalize_key(priority.get("lifecycle")) or "missing"
        if lifecycle != "all" and priority_lifecycle != lifecycle:
            continue
        priority["sales_user_name"] = owner
        priority["segment"] = priority_segment
        priority["lifecycle"] = priority_lifecycle
        portfolio.append(priority)
    by_owner = defaultdict(list)
    for priority in portfolio:
        by_owner[priority.get("sales_user_name")].append(priority)
    for owner_rows in by_owner.values():
        scores = sorted((_number(row.get("priority_score"), 0) or 0 for row in owner_rows))
        values = sorted((_number(row.get("value_index"), 0) or 0 for row in owner_rows))
        for priority in owner_rows:
            score = _number(priority.get("priority_score"), 0) or 0
            value = _number(priority.get("value_index"), 0) or 0
            priority["priority_percentile"] = round(100 * sum(item <= score for item in scores) / len(scores), 2) if scores else None
            priority["value_percentile"] = round(100 * sum(item <= value for item in values) / len(values), 2) if values else None
    contacted_ids = {
        _text((row.get("customer_record") or {}).get("customer_id"))
        for row in current["human"]
        if _text((row.get("customer_record") or {}).get("customer_id"))
    }
    strategic_portfolio = [
        row for row in portfolio
        if row.get("segment") == "A" or (row.get("value_percentile") or 0) >= 75
    ]
    strategic_contacted = [
        row for row in strategic_portfolio
        if _text(row.get("customer_id")) in contacted_ids
    ]
    priority_gap = [
        row for row in portfolio
        if (row.get("priority_percentile") or 0) >= 75
        and _text(row.get("customer_id")) not in contacted_ids
    ]

    contact_by_id = {row.get("contact_id"): row for row in canonical_result["activities"]}
    planned_period = []
    for raw_planned in planned_activities or ():
        planned = dict(raw_planned)
        scheduled = _datetime(planned.get("scheduled_at"))
        if not scheduled or not (start <= scheduled.date() <= end):
            continue
        if seller and normalize_key(planned.get("user_name")) != normalize_key(seller):
            continue
        planned["scheduled_datetime"] = scheduled
        planned_period.append(planned)
    accountable_planned = [
        row for row in planned_period
        if normalize_key(row.get("status")) != "cancelled"
    ]
    completed_planned = [
        row for row in accountable_planned
        if normalize_key(row.get("status")) == "completed"
    ]
    completed_in_time = []
    for planned in completed_planned:
        completed_contact = contact_by_id.get(_text(planned.get("completed_contact_id")))
        if completed_contact and completed_contact.get("contact_date") and completed_contact["contact_date"] <= planned["scheduled_datetime"].date():
            completed_in_time.append(planned)
    skipped_planned = [row for row in accountable_planned if normalize_key(row.get("status")) == "skipped"]
    overdue_planned = [
        row for row in accountable_planned
        if normalize_key(row.get("status") or "planned") == "planned"
        and row["scheduled_datetime"] < generated
    ]
    positive_three_days_old = [
        row for row in current["positive"]
        if row.get("contact_date") and (generated.date() - row["contact_date"]).days >= 3
    ]
    planned_by_source_contact = defaultdict(list)
    for planned in planned_activities or ():
        source_contact_id = _text(planned.get("source_contact_id"))
        if source_contact_id:
            planned_by_source_contact[source_contact_id].append(planned)
    positive_with_next_step = []
    for row in positive_three_days_old:
        has_follow_up = bool(_date(row.get("follow_up_date")))
        has_linked_plan = bool(planned_by_source_contact.get(row.get("contact_id")))
        has_order = bool(attribution["contact_to_orders"].get(row.get("contact_id")))
        if has_follow_up or has_linked_plan or has_order:
            positive_with_next_step.append(row)
    mature_positive_without_order_or_followup = [
        row for row in positive_three_days_old
        if attribution["maturity"].get(row.get("contact_id")) == "mature"
        and not attribution["contact_to_orders"].get(row.get("contact_id"))
        and not _date(row.get("follow_up_date"))
        and not planned_by_source_contact.get(row.get("contact_id"))
    ]

    weeks = defaultdict(list)
    for row in rows:
        if row.get("contact_week"):
            weeks[row["contact_week"]].append(row)
    weekly_trend = []
    for week, week_rows in sorted(weeks.items()):
        agg = _aggregate_period(week_rows, attribution)
        weekly_trend.append({
            "week": week,
            "human_activities": len(agg["human"]),
            "reached": len(agg["reached"]),
            "positive": len(agg["positive"]),
            "mature_attributed_orders": len(agg["attributed"]),
            "bom_ratio": agg["rates"]["bom_ratio"],
            "incomplete": week == _iso_week(generated.date()),
        })

    channel_effectiveness = {}
    for key in ("visit", "phone", "email"):
        aggregate = _aggregate_period([row for row in rows if row.get("contact_type_key") == key], attribution)
        days = [item["days_to_order"] for item in aggregate["attributed"]]
        channel_effectiveness[key] = {
            "human_activities": len(aggregate["human"]),
            "reach": aggregate["rates"]["reach"],
            "positive_dialogue": aggregate["rates"]["positive_dialogue"],
            "order_10d": aggregate["rates"]["order_10d"],
            "bom_ratio": aggregate["rates"]["bom_ratio"] if key == "visit" else None,
            "attributed_orders": len(aggregate["attributed"]),
            "dfp": aggregate["dfp"],
            "order_value_by_currency": aggregate["order_value_by_currency"],
            "median_days_to_order": statistics.median(days) if len(days) >= 5 else None,
        }

    kpis = {
        "human_activities": {"value": len(current["human"]), "comparison": len(previous["human"])},
        "reach": current["rates"]["reach"],
        "positive_dialogue": current["rates"]["positive_dialogue"],
        "order_10d": {
            **current["rates"]["order_10d"],
            "attributed_orders": len(current["attributed"]),
            "unique_order_customers": len({
                item["order"].get("customer_identity_key")
                for item in current["attributed"]
                if item["order"].get("customer_identity_key")
            }),
            "dfp": current["dfp"],
            "order_value_by_currency": current["order_value_by_currency"],
        },
        "priority_focus": current["rates"]["priority_focus"],
        "bom_ratio": current["rates"]["bom_ratio"],
    }
    data_quality = _data_quality(rows, canonical_result, order_result, attribution)
    coaching_matrix = [
        item for item in seller_comparison
        if item["order_10d"]["denominator"] >= MIN_RATE_SAMPLE
        and item["snapshot_coverage"]["value"] is not None
        and item["snapshot_coverage"]["value"] >= MIN_PRIORITY_COVERAGE
    ]
    result = {
        "meta": {
            "generated_at": generated.isoformat(timespec="seconds"),
            "period": {"start": start.isoformat(), "end": end.isoformat()},
            "comparison_period": {"start": comparison_start.isoformat(), "end": comparison_end.isoformat()},
            "filters": {"seller": seller, "channel": channel, "segment": segment, "lifecycle": lifecycle},
            "definitions_version": DEFINITIONS_VERSION,
            "score_version": score_version,
            "base_score_version": score_version,
        },
        "options": {
            "sellers": seller_options,
            "channels": ["all", "visit", "phone", "email"],
            "segments": ["all", "A", "B", "C", "missing"],
            "lifecycles": ["all", "prospect", "first_order", "established", "reactivation"],
        },
        "data_quality": data_quality,
        "kpis": kpis,
        "seller_comparison": seller_comparison,
        "coaching_matrix": coaching_matrix,
        "funnel": {
            "attempts": len(current["sync"]),
            "reached": len([row for row in current["sync"] if row.get("result_class") in QUALIFIED_DIALOGUE_RESULTS]),
            "positive": len(current["positive"]),
            "mature_attributed_orders": len(current["attributed"]),
            "reach_rate": current["rates"]["reach"],
            "positive_rate": current["rates"]["positive_dialogue"],
            "order_rate": current["rates"]["order_10d"],
        },
        "weekly_trend": weekly_trend,
        "visit_efficiency": {
            "bom_ratio": current["rates"]["bom_ratio"],
            "repeat_boms": {"customers": len(repeated), "visits": sum(len(value) for value in repeated.values())},
            "high_priority_boms": len(high_priority_boms),
            "high_priority_score_fallback": len(high_priority_score_fallback),
            "planned": _rate(len([row for row in current["boms"] if row.get("planned_activity_id")]), len([row for row in current["visits"] if row.get("planned_activity_id")])),
            "unplanned": _rate(len([row for row in current["boms"] if not row.get("planned_activity_id")]), len([row for row in current["visits"] if not row.get("planned_activity_id")])),
        },
        "channel_effectiveness": channel_effectiveness,
        "priority_allocation": {
            "priority_focus": current["rates"]["priority_focus"],
            "snapshot_coverage": _rate(len(current["snapshots"]), len(current["human"]), minimum=1),
            "strategic_coverage": _rate(
                len(strategic_contacted), len(strategic_portfolio), minimum=MIN_RATE_SAMPLE
            ),
            "priority_gap": {
                "count": len(priority_gap),
                "customers": [
                    {
                        "customer_id": _text(row.get("customer_id")),
                        "customer": _text(row.get("customer")),
                        "sales_user_name": row.get("sales_user_name", ""),
                        "priority_score": _number(row.get("priority_score")),
                        "priority_percentile": row.get("priority_percentile"),
                        "value_index": _number(row.get("value_index")),
                        "segment": row.get("segment", "missing"),
                    }
                    for row in sorted(
                        priority_gap,
                        key=lambda item: (
                            -(_number(item.get("priority_score"), 0) or 0),
                            _text(item.get("customer")),
                        ),
                    )[:200]
                ],
            },
        },
        "follow_up_discipline": {
            "positive_next_step_coverage": _rate(
                len(positive_with_next_step), len(positive_three_days_old)
            ),
            "positive_without_next_step": (
                len(positive_three_days_old) - len(positive_with_next_step)
            ),
            "planned_completed_in_time": _rate(
                len(completed_in_time), len(accountable_planned)
            ),
            "overdue_planned": len(overdue_planned),
            "skipped": len(skipped_planned),
            "cancelled_excluded": len([
                row for row in planned_period
                if normalize_key(row.get("status")) == "cancelled"
            ]),
            "positive_without_order_or_follow_up_10d": len(
                mature_positive_without_order_or_followup
            ),
        },
        "coaching_cards": [],
        "_analysis": {"rows": rows, "attribution": attribution},
    }
    if on_step:
        on_step("calculation.sales_coaching.aggregation", aggregation_started, len(rows))
    return result


def build_drilldown(summary, metric, *, limit=100):
    if metric not in DRILLDOWN_METRICS:
        raise ValueError("invalid_metric")
    limit = max(1, min(int(limit), 200))
    analysis = summary.get("_analysis", {})
    attribution = analysis.get("attribution", {})
    selected = []
    repeated_keys = defaultdict(int)
    for row in analysis.get("rows", ()):
        if row.get("is_human") and row.get("contact_type_key") == "visit" and row.get("result_class") == "unreachable":
            repeated_keys[row.get("customer_identity_key")] += 1
    for row in analysis.get("rows", ()):
        orders = attribution.get("contact_to_orders", {}).get(row.get("contact_id"), ())
        include = {
            "human_activities": row.get("is_human"),
            "reach": row.get("is_human") and row.get("contact_type_key") in SYNCHRONOUS_CHANNELS and row.get("result_class") in ANALYSABLE_RESULTS,
            "positive_dialogue": row.get("is_human") and row.get("result_class") in {"positive", "order"},
            "order_10d": bool(orders),
            "waiting_outcome": attribution.get("maturity", {}).get(row.get("contact_id")) == "waiting_outcome",
            "priority_focus": row.get("priority_percentile_at_contact") is not None and row.get("priority_percentile_at_contact") >= 75,
            "bom_ratio": row.get("contact_type_key") == "visit" and row.get("result_class") in ANALYSABLE_RESULTS,
            "repeat_boms": row.get("result_class") == "unreachable" and repeated_keys[row.get("customer_identity_key")] >= 2,
            "high_priority_boms": row.get("result_class") == "unreachable" and (row.get("priority_percentile_at_contact") or -1) >= 75,
            "data_quality": bool(row.get("analysis_exclusions")) or row.get("contact_type_key") == "unknown" or row.get("result_class") == "unknown",
        }[metric]
        if not include:
            continue
        first_order = orders[0] if orders else None
        customer = row.get("customer_record") or {}
        selected.append({
            "contact_id": row.get("contact_id"),
            "date_time": row.get("date_time"),
            "sales_user_name": row.get("sales_user_name"),
            "customer": customer.get("customer") or row.get("customer"),
            "customer_id": customer.get("customer_id") or "",
            "channel": row.get("contact_type_key"),
            "result_class": row.get("result_class"),
            "snapshot_quality": row.get("priority_snapshot_quality"),
            "priority_at_contact": row.get("priority_score_at_contact"),
            "priority_percentile_at_contact": row.get("priority_percentile_at_contact"),
            "order_reference": first_order["order"].get("reference") if first_order else "",
            "days_to_order": first_order.get("days_to_order") if first_order else None,
            "dfp": first_order["order"].get("dfp") if first_order else 0,
            "data_quality_flags": list(row.get("analysis_exclusions") or ()),
        })
    selected.sort(key=lambda row: (_datetime(row.get("date_time")) or datetime.min, row.get("contact_id") or ""), reverse=True)
    return {"metric": metric, "limit": limit, "rows": selected[:limit], "total_count": len(selected)}


def strip_internal_analysis(summary):
    return {key: value for key, value in summary.items() if key != "_analysis"}


def build_pre_contact_snapshot(*, customer, owner, priorities, planned_row=None, score_version=""):
    """Build an exact snapshot from the authoritative pre-append priority universe."""
    owner_keys = {normalize_key((owner or {}).get("user_name")), normalize_key((owner or {}).get("name"))}
    portfolio = [row for row in priorities or () if normalize_key(row.get("sales_person")) in owner_keys and not row.get("cancelled_flag")]
    customer_id = _text((customer or {}).get("customer_id"))
    customer_number = normalize_key((customer or {}).get("customer_number"))
    customer_name = normalize_key((customer or {}).get("customer"))
    target = next((row for row in portfolio if (
        (customer_id and _text(row.get("customer_id")) == customer_id)
        or (not customer_id and customer_number and normalize_key(row.get("customer_number")) == customer_number)
        or (not customer_id and not customer_number and normalize_key(row.get("customer")) == customer_name)
    )), None)
    if target is None:
        return {
            "analytics_snapshot_version": ANALYTICS_SNAPSHOT_VERSION,
            "priority_snapshot_quality": "missing",
            "priority_score_version": score_version,
        }
    scores = sorted((_number(row.get("priority_score"), 0) or 0 for row in portfolio))
    score = _number(target.get("priority_score"), 0) or 0
    percentile = round(100 * sum(value <= score for value in scores) / len(scores), 2) if scores else None
    source = normalize_key((planned_row or {}).get("source"))
    activity_source = source if source in {"follow_up", "route", "system_suggestion"} else ("planned" if planned_row else "manual")
    return {
        "sales_user_name": _text((owner or {}).get("user_name")),
        "customer_number": _text((customer or {}).get("customer_number")),
        "activity_source": activity_source,
        "source_suggestion_id": _text((planned_row or {}).get("source_suggestion_id")),
        "source_trigger_key": _text((planned_row or {}).get("source_trigger_key")),
        "analytics_snapshot_version": ANALYTICS_SNAPSHOT_VERSION,
        "priority_snapshot_quality": "exact",
        "priority_score_version": _text(target.get("score_version") or score_version),
        "priority_score_at_contact": score,
        "priority_percentile_at_contact": percentile if percentile is not None else "",
        "seller_portfolio_size_at_contact": len(portfolio),
        "intent_timing_at_contact": target.get("intent_timing", ""),
        "value_index_at_contact": target.get("value_index", ""),
        "strategic_index_at_contact": target.get("strategic_index", ""),
        "expected_order_dfp_at_contact": target.get("expected_order_dfp", ""),
        "lifecycle_at_contact": target.get("lifecycle", ""),
        "customer_segment_at_contact": _text(target.get("segment") or (customer or {}).get("customer_segment")),
    }
