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
from zoneinfo import ZoneInfo

from sales_coaching_rules import (
    add_seller_benchmarks,
    build_seller_signals,
    build_team_signals,
)


DEFINITIONS_VERSION = "sales_coaching_v3"
ANALYTICS_SNAPSHOT_VERSION = "sales_coaching_v2"
PRIORITY_PERCENTILE_BASIS = "owner_active_scored_portfolio_midrank_v2"
STOCKHOLM_ZONE = ZoneInfo("Europe/Stockholm")
ATTRIBUTION_WINDOW_DAYS = 10
MIN_RATE_SAMPLE = 10
MIN_PRIORITY_COVERAGE = 0.70

KPI_DEFINITIONS = {
    "human_activities": {
        "label": "Mänskliga aktiviteter",
        "definition": "Unika manuella eller planerade Besök, Telefon och manuella Mejl; automatiserade CRM-mejl exkluderas.",
        "drilldown_metric": "human_activities",
    },
    "reach": {
        "label": "Träffgrad",
        "definition": "Nådda synkrona kontakter dividerat med analyserbara kontaktförsök via Besök eller Telefon.",
        "drilldown_metric": "reach",
        "denominator_drilldown_metric": "attempts",
    },
    "positive_dialogue": {
        "label": "Positiv dialog",
        "definition": "Nådda mänskliga kontakter med positivt resultat eller order dividerat med alla nådda mänskliga kontakter.",
        "drilldown_metric": "positive_dialogue",
    },
    "positive_to_order_10d": {
        "label": "Positiv dialog → order inom 10 dagar",
        "definition": "Mogna positiva kontakter som följdes av attribuerad order inom 0–10 dagar dividerat med alla mogna positiva kontakter med säker kundidentitet.",
        "drilldown_metric": "positive_to_order_10d",
    },
    "order_10d": {
        "label": "Order inom 10 dagar",
        "definition": "Mogna nådda kontakter med säker kundidentitet och minst en exklusivt attribuerad order inom 0–10 dagar dividerat med mogna nådda kontakter med säker kundidentitet.",
        "drilldown_metric": "order_10d",
    },
    "priority_focus": {
        "label": "Prioritetsfokus",
        "definition": "Kontakter i säljarens historiska översta prioritetskvartil dividerat med aktiviteter där historisk prioritetpercentil finns.",
        "drilldown_metric": "priority_focus",
    },
    "bom_ratio": {
        "label": "Bom-ratio för besök",
        "definition": "Besök med Ej anträffbar dividerat med alla besök med analyserbart resultat.",
        "drilldown_metric": "bom_ratio",
    },
}

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
    "recommendation_eligible_at_contact",
    "suppression_reason_at_contact",
    "suppression_source_id_at_contact",
    "snapshot_created_at",
    "snapshot_lag_hours",
    "priority_percentile_basis_at_contact",
]

DRILLDOWN_METRICS = frozenset({
    "human_activities",
    "attempts",
    "reach",
    "positive_sync",
    "positive_dialogue",
    "positive_to_order_10d",
    "mature_reached_sync",
    "order_10d",
    "order_10d_sync",
    "waiting_outcome",
    "priority_focus",
    "bom_ratio",
    "planned_boms",
    "unplanned_boms",
    "repeat_boms",
    "high_priority_boms",
    "followup_success",
    "followup_gap",
    "followup_gap_10d",
    "planned_on_time",
    "planned_overdue",
    "planned_skipped",
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


def _optional_bool(value):
    if isinstance(value, bool):
        return value
    normalized = normalize_key(value)
    if normalized in {"true", "1", "yes", "y", "ja"}:
        return True
    if normalized in {"false", "0", "no", "n", "nej"}:
        return False
    return None


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


def _stockholm_datetime(value):
    """Return a timezone-aware Stockholm instant without changing legacy parsers."""
    if isinstance(value, date) and not isinstance(value, datetime):
        parsed = datetime.combine(value, time.min)
    elif isinstance(value, datetime):
        parsed = value
    else:
        text = _text(value).replace("Z", "+00:00")
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            parsed = _datetime(text)
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=STOCKHOLM_ZONE)
    return parsed.astimezone(STOCKHOLM_ZONE)


def _is_v2_contact(row):
    return _text(row.get("analytics_snapshot_version")) == ANALYTICS_SNAPSHOT_VERSION


def _is_comparable_priority_percentile(row):
    return (
        _is_v2_contact(row)
        and normalize_key(row.get("priority_percentile_basis_at_contact"))
        == PRIORITY_PERCENTILE_BASIS
        and row.get("priority_snapshot_quality") == "exact"
        and row.get("priority_percentile_at_contact") is not None
        and bool(row.get("customer_identity_key"))
        and bool(row.get("sales_user_name"))
    )


def normalize_contact_type(value):
    key = normalize_key(value)
    mapping = {
        "visit": "visit", "besok": "visit", "fysiskt besok": "visit",
        "mote": "visit", "meeting": "visit",
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


def _coached_sellers(users):
    """Return the stable seller identities allowed to affect coaching views."""
    sellers = []
    seen = set()
    for user in users or ():
        user_name = _text(user.get("user_name"))
        key = normalize_key(user_name)
        if (
            not key
            or _optional_bool(user.get("active")) is not True
            or _optional_bool(user.get("admin")) is True
            or key in seen
        ):
            continue
        seen.add(key)
        sellers.append(user_name)
    return sellers


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
            "analytics_snapshot_version": _text(
                activity.get("analytics_snapshot_version")
            ),
            "score_version": _text(activity.get("priority_score_version")),
            "portfolio_size": _number(activity.get("seller_portfolio_size_at_contact")),
            "snapshot_created_at": _text(activity.get("snapshot_created_at")),
            "snapshot_lag_hours": _number(activity.get("snapshot_lag_hours")),
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
            "recommendation_eligible_at_contact": _optional_bool(
                value("recommendation_eligible")
            ),
            "suppression_reason_at_contact": _text(
                value("recommendation_suppression_reason")
                or value("suppression_reason")
            ),
            "suppression_source_id_at_contact": "",
            "priority_percentile_basis_at_contact": "",
            "analytics_snapshot_version": "",
            "snapshot_created_at": "",
            "snapshot_lag_hours": None,
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
            "analytics_snapshot_version": _text(
                row.get("analytics_snapshot_version")
                or snapshot.get("analytics_snapshot_version")
            ),
            "priority_score_at_contact": _number(snapshot.get("priority_score_at_contact")),
            "priority_percentile_at_contact": _number(snapshot.get("priority_percentile_at_contact")),
            "priority_percentile_basis_at_contact": _text(
                snapshot.get("priority_percentile_basis_at_contact")
            ),
            "seller_portfolio_size_at_contact": _number(snapshot.get("portfolio_size")),
            "lifecycle_at_contact": _text(snapshot.get("lifecycle_at_contact")),
            "customer_segment_at_contact": _text(snapshot.get("customer_segment_at_contact")),
            "recommendation_eligible_at_contact": _optional_bool(
                snapshot.get("recommendation_eligible_at_contact")
            ),
            "suppression_reason_at_contact": _text(
                snapshot.get("suppression_reason_at_contact")
            ),
            "suppression_source_id_at_contact": _text(
                snapshot.get("suppression_source_id_at_contact")
            ),
            "snapshot_created_at": _text(snapshot.get("snapshot_created_at")),
            "snapshot_lag_hours": _number(snapshot.get("snapshot_lag_hours")),
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
        # Historical activity filters must never borrow today's customer segment.
        row_segment = _text(row.get("customer_segment_at_contact")).upper() or "missing"
        if segment != "all" and row_segment != segment:
            continue
        row_lifecycle = normalize_key(row.get("lifecycle_at_contact")) or "missing"
        if lifecycle != "all" and row_lifecycle != lifecycle:
            continue
        selected.append(row)
    return selected


def _is_reached_human(row):
    return (
        row.get("is_human")
        and row.get("contact_type_key") in (SYNCHRONOUS_CHANNELS | {"email"})
        and row.get("result_class") in QUALIFIED_DIALOGUE_RESULTS
    )


def _is_sync_reached(row):
    return (
        row.get("is_human")
        and row.get("contact_type_key") in SYNCHRONOUS_CHANNELS
        and row.get("result_class") in QUALIFIED_DIALOGUE_RESULTS
    )


def _is_attribution_eligible(row):
    return _is_reached_human(row) and bool(row.get("customer_identity_key"))


def _aggregate_period(rows, attribution):
    human = [row for row in rows if row.get("is_human")]
    sync = [row for row in human if row.get("contact_type_key") in SYNCHRONOUS_CHANNELS and row.get("result_class") in ANALYSABLE_RESULTS]
    sync_reached = [row for row in sync if _is_sync_reached(row)]
    sync_positive = [row for row in sync_reached if row.get("result_class") in {"positive", "order"}]
    sync_attribution_eligible = [row for row in sync_reached if row.get("customer_identity_key")]
    sync_mature = [row for row in sync_attribution_eligible if attribution["maturity"].get(row["contact_id"]) == "mature"]
    sync_converted = [row for row in sync_mature if attribution["contact_to_orders"].get(row["contact_id"])]
    reached = [row for row in human if _is_reached_human(row)]
    attribution_eligible = [row for row in reached if row.get("customer_identity_key")]
    positive = [row for row in reached if row.get("result_class") in {"positive", "order"}]
    mature = [row for row in attribution_eligible if attribution["maturity"].get(row["contact_id"]) == "mature"]
    mature_positive = [row for row in positive if row.get("customer_identity_key") and attribution["maturity"].get(row["contact_id"]) == "mature"]
    converted_positive = [row for row in mature_positive if attribution["contact_to_orders"].get(row["contact_id"])]
    waiting = [row for row in attribution_eligible if attribution["maturity"].get(row["contact_id"]) == "waiting_outcome"]
    ordered_contacts = [row for row in mature if attribution["contact_to_orders"].get(row["contact_id"])]
    visits = [row for row in human if row.get("contact_type_key") == "visit" and row.get("result_class") in ANALYSABLE_RESULTS]
    boms = [row for row in visits if row.get("result_class") == "unreachable"]
    snapshots = [row for row in human if row.get("priority_snapshot_quality") in {"exact", "approximate"}]
    v2_contacts = [row for row in human if _is_v2_contact(row)]
    percentile_rows = [row for row in human if _is_comparable_priority_percentile(row)]
    top_priority = [row for row in percentile_rows if row["priority_percentile_at_contact"] >= 75]
    attributed = [item for row in ordered_contacts for item in attribution["contact_to_orders"].get(row["contact_id"], ())]
    totals_by_currency = defaultdict(float)
    for item in attributed:
        order = item["order"]
        totals_by_currency[order.get("currency") or "unknown"] += order.get("total", 0)
    return {
        "rows": rows,
        "human": human,
        "sync": sync,
        "sync_reached": sync_reached,
        "sync_positive": sync_positive,
        "sync_attribution_eligible": sync_attribution_eligible,
        "sync_mature": sync_mature,
        "sync_converted": sync_converted,
        "reached": reached,
        "attribution_eligible": attribution_eligible,
        "positive": positive,
        "mature": mature,
        "mature_positive": mature_positive,
        "converted_positive": converted_positive,
        "waiting": waiting,
        "ordered_contacts": ordered_contacts,
        "visits": visits,
        "boms": boms,
        "snapshots": snapshots,
        "v2_contacts": v2_contacts,
        "percentile_rows": percentile_rows,
        "top_priority": top_priority,
        "attributed": attributed,
        "rates": {
            "reach": _rate(
                len(sync_reached),
                len(sync),
            ),
            "positive_dialogue": _rate(len(positive), len(reached)),
            "positive_to_order_10d": _rate(len(converted_positive), len(mature_positive)),
            "order_10d": _rate(len(ordered_contacts), len(mature)),
            "priority_focus": _rate(len(top_priority), len(percentile_rows)),
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
            "human_activities_total": len(aggregate["human"]),
            "human_activities_metric": {
                "value": len(aggregate["human"]),
                "denominator": len(aggregate["human"]),
                "metric_type": "count",
                "unit": "aktiviteter",
                "status": (
                    "sufficient"
                    if len(aggregate["human"]) >= MIN_RATE_SAMPLE
                    else "small_sample"
                ),
            },
            "channel_mix": {
                key: sum(row.get("contact_type_key") == key for row in aggregate["human"])
                for key in ("visit", "phone", "email")
            },
            "visit_breakdown": {
                "analysable": len(aggregate["visits"]),
                "reached": len(aggregate["visits"]) - len(aggregate["boms"]),
                "boms": len(aggregate["boms"]),
            },
            "positive_dialogues_count": len(aggregate["positive"]),
            "mature_positive_dialogues_count": len(aggregate["mature_positive"]),
            "converted_positive_contacts_count": len(aggregate["converted_positive"]),
            "waiting_positive_dialogues_count": (
                len(aggregate["positive"]) - len(aggregate["mature_positive"])
            ),
            "order_10d_converted_contacts": len(aggregate["ordered_contacts"]),
            "attributed_orders": len(aggregate["attributed"]),
            "waiting_outcome_count": len(aggregate["waiting"]),
            **aggregate["rates"],
            "snapshot_coverage": _rate(
                sum(
                    row.get("priority_snapshot_quality") == "exact"
                    for row in aggregate["v2_contacts"]
                ),
                len(aggregate["v2_contacts"]),
                minimum=1,
            ),
            "priority_percentile_coverage": _rate(
                len(aggregate["percentile_rows"]),
                len(aggregate["v2_contacts"]),
                minimum=1,
            ),
        })
    return result


def _sufficient_median(sellers, metric):
    values = [
        item[metric]["value"] for item in sellers
        if item[metric]["status"] == "sufficient"
    ]
    return statistics.median(values) if len(values) >= 2 else None


def _data_quality(rows, canonical_result, order_result, attribution):
    human = [row for row in rows if row.get("is_human")]
    secure = [row for row in human if row.get("customer_identity_key")]
    historical_seller = [row for row in human if row.get("sales_user_name")]
    standardized = [row for row in human if row.get("contact_type_key") != "unknown" and row.get("result_class") != "unknown"]
    reached = [row for row in human if _is_reached_human(row)]
    attributable_reached = [row for row in reached if row.get("customer_identity_key")]
    core_reasons = defaultdict(int)
    core_flagged_activity_rows = 0
    for row in human:
        row_reasons = set(row.get("analysis_exclusions", ()))
        if row.get("contact_type_key") == "unknown":
            row_reasons.add("unknown_contact_type")
        if row.get("result_class") == "unknown":
            row_reasons.add("unknown_result")
        if _is_reached_human(row) and not row.get("customer_identity_key"):
            row_reasons.add("missing_order_attribution_identity")
        if row_reasons:
            core_flagged_activity_rows += 1
        for reason in row_reasons:
            core_reasons[reason] += 1
    excluded_rows = list(canonical_result.get("excluded", ()))
    for row in excluded_rows:
        core_reasons[row.get("reason") or "excluded_activity"] += 1
    status = "sufficient"
    identity_rate = len(secure) / len(human) if human else 0
    standardized_rate = len(standardized) / len(human) if human else 0
    if human and (identity_rate < 0.9 or standardized_rate < 0.9):
        status = "limited_data_quality"
    elif len(human) < MIN_RATE_SAMPLE:
        status = "small_sample"
    secure_identity = _rate(len(secure), len(human), minimum=1)
    attribution_identity = _rate(
        len(attributable_reached), len(reached), minimum=1
    )
    standardized_activity = _rate(len(standardized), len(human), minimum=1)
    v2_contacts = [row for row in human if _is_v2_contact(row)]
    exact_snapshots = [
        row for row in v2_contacts
        if row.get("priority_snapshot_quality") == "exact"
    ]
    comparable_percentiles = [
        row for row in v2_contacts if _is_comparable_priority_percentile(row)
    ]
    exact_snapshot_rate = _rate(
        len(exact_snapshots), len(v2_contacts), minimum=1
    )
    comparable_percentile_rate = _rate(
        len(comparable_percentiles), len(v2_contacts), minimum=1
    )
    operationally_eligible = [
        row for row in v2_contacts
        if row.get("recommendation_eligible_at_contact") is True
    ]
    operationally_suppressed = [
        row for row in v2_contacts
        if row.get("recommendation_eligible_at_contact") is False
    ]
    suppression_reason_counts = defaultdict(int)
    for row in operationally_suppressed:
        suppression_reason_counts[
            row.get("suppression_reason_at_contact") or "unspecified"
        ] += 1
    late_snapshot_count = sum(
        (row.get("snapshot_lag_hours") or 0) > 24 for row in v2_contacts
    )
    historical_status = (
        "not_computable" if not v2_contacts
        else "sufficient"
        if comparable_percentile_rate["value"] is not None
        and comparable_percentile_rate["value"] >= MIN_PRIORITY_COVERAGE
        else "building"
    )
    return {
        "status": status,
        "core_analytics": {
            "status": status,
            "secure_customer_identity": secure_identity,
            "order_attribution_identity_coverage": attribution_identity,
            "standardized_activity": standardized_activity,
            "core_flagged_activity_rows": core_flagged_activity_rows,
        },
        "historical_priority": {
            "status": historical_status,
            "v2_contact_count": len(v2_contacts),
            "exact_snapshot_count": len(exact_snapshots),
            "exact_snapshot_rate": exact_snapshot_rate,
            "comparable_percentile_count": len(comparable_percentiles),
            "comparable_percentile_rate": comparable_percentile_rate,
            "operationally_eligible_count": len(operationally_eligible),
            "operationally_suppressed_count": len(operationally_suppressed),
            "suppression_reason_counts": dict(
                sorted(suppression_reason_counts.items())
            ),
            "late_snapshot_count": late_snapshot_count,
            "minimum_comparable_coverage": MIN_PRIORITY_COVERAGE,
            # Compatibility aliases for the first dashboard contract.
            "snapshot_coverage": exact_snapshot_rate,
            "priority_percentile_coverage": comparable_percentile_rate,
            "message": "Historisk prioriteringsdata byggs upp från lanseringen och påverkar inte kärnanalysen av aktivitet och order.",
        },
        "secure_customer_identity": secure_identity,
        "historical_seller_identity": _rate(len(historical_seller), len(human), minimum=1),
        "standardized_activity": standardized_activity,
        "order_attribution_identity_coverage": attribution_identity,
        "priority_snapshot_coverage": exact_snapshot_rate,
        "priority_percentile_coverage": comparable_percentile_rate,
        "waiting_outcome_count": sum(
            attribution["maturity"].get(row["contact_id"]) == "waiting_outcome"
            for row in attributable_reached
        ),
        "core_flagged_activity_rows": core_flagged_activity_rows,
        "flagged_activity_rows": core_flagged_activity_rows,
        "quality_issue_count": sum(core_reasons.values()),
        "excluded_legacy_rows": len(excluded_rows),
        "exclusion_reasons": dict(sorted(core_reasons.items())),
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
    coached_sellers = _coached_sellers(users)
    coached_keys = {normalize_key(value) for value in coached_sellers}
    coached_activities = [
        row for row in canonical_result["activities"]
        if normalize_key(row.get("sales_user_name")) in coached_keys
    ]
    order_result = group_logical_orders(order_rows, customers)
    if on_step:
        on_step("calculation.sales_coaching.normalization", normalization_started, len(canonical_result["activities"]))
    attribution_started = clock.perf_counter()
    attribution = attribute_orders_to_contacts(
        coached_activities, order_result["orders"], generated_at=generated
    )
    if on_step:
        on_step("calculation.sales_coaching.attribution", attribution_started, len(order_result["orders"]))
    aggregation_started = clock.perf_counter()
    rows = _filter_activities(coached_activities, start=start, end=end, seller=seller, channel=channel, segment=segment, lifecycle=lifecycle)
    team_rows = _filter_activities(coached_activities, start=start, end=end, seller="", channel="all", segment=segment, lifecycle=lifecycle)
    comparison_start, comparison_end = _comparison_dates(start, end)
    comparison_rows = _filter_activities(coached_activities, start=comparison_start, end=comparison_end, seller=seller, channel=channel, segment=segment, lifecycle=lifecycle)
    previous_team_rows = _filter_activities(
        coached_activities, start=comparison_start, end=comparison_end,
        seller="", channel="all", segment=segment, lifecycle=lifecycle,
    )
    current, previous = _aggregate_period(rows, attribution), _aggregate_period(comparison_rows, attribution)
    for key, rate in current["rates"].items():
        rate["comparisons"]["previous_period"] = previous["rates"][key]

    seller_options = coached_sellers
    seller_comparison = _seller_comparison(team_rows, attribution, coached_sellers)
    previous_seller_comparison = _seller_comparison(
        previous_team_rows, attribution, coached_sellers
    )
    repeat_customers = defaultdict(list)
    for row in current["boms"]:
        if row.get("customer_identity_key"):
            repeat_customers[row["customer_identity_key"]].append(row)
    repeated = {key: value for key, value in repeat_customers.items() if len(value) >= 2}
    high_priority_boms = [
        row for row in current["boms"]
        if _is_comparable_priority_percentile(row)
        and row["priority_percentile_at_contact"] >= 75
    ]
    boms_with_percentile = [
        row for row in current["boms"]
        if _is_comparable_priority_percentile(row)
    ]
    bom_priority_coverage = _rate(
        len(boms_with_percentile), len(current["boms"]), minimum=1
    )
    if not current["boms"]:
        high_priority_boms_metric = {
            "value": 0, "status": "sufficient", "coverage": bom_priority_coverage,
        }
    elif (
        bom_priority_coverage["value"] is None
        or bom_priority_coverage["value"] < MIN_PRIORITY_COVERAGE
    ):
        high_priority_boms_metric = {
            "value": None, "status": "limited_coverage", "coverage": bom_priority_coverage,
        }
    else:
        high_priority_boms_metric = {
            "value": len(high_priority_boms),
            "status": "sufficient",
            "coverage": bom_priority_coverage,
        }

    aliases, _canonical_sellers = _seller_aliases(users)
    current_priority_rows = []
    for raw_priority in current_priorities or ():
        priority = dict(raw_priority)
        owner_matches = aliases.get(normalize_key(priority.get("sales_person")), set())
        owner = next(iter(owner_matches)) if len(owner_matches) == 1 else ""
        if normalize_key(owner) not in coached_keys:
            continue
        priority_segment = _text(priority.get("segment")).upper() or "missing"
        priority_lifecycle = normalize_key(priority.get("lifecycle")) or "missing"
        priority["sales_user_name"] = owner
        priority["segment"] = priority_segment
        priority["lifecycle"] = priority_lifecycle
        priority["recommendation_eligible"] = (
            _optional_bool(priority.get("recommendation_eligible")) is True
        )
        current_priority_rows.append(priority)
    portfolio = [
        priority for priority in current_priority_rows
        if (not seller or normalize_key(priority.get("sales_user_name")) == normalize_key(seller))
        and (segment == "all" or priority.get("segment") == segment)
        and (lifecycle == "all" or priority.get("lifecycle") == lifecycle)
    ]
    by_owner = defaultdict(list)
    for priority in portfolio:
        by_owner[priority.get("sales_user_name")].append(priority)
    for owner_rows in by_owner.values():
        values = sorted((_number(row.get("value_index"), 0) or 0 for row in owner_rows))
        for priority in owner_rows:
            value = _number(priority.get("value_index"), 0) or 0
            below = sum(item < value for item in values)
            tied = sum(item == value for item in values)
            priority["value_percentile"] = round(
                100 * (below + 0.5 * tied) / len(values), 2
            ) if values else None
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

    contact_by_id = {row.get("contact_id"): row for row in canonical_result["activities"]}
    priority_by_customer_id = {}
    priority_by_customer_number = {}

    def add_strong_priority_identity(mapping, key, priority):
        if not key:
            return
        if key in mapping:
            mapping[key] = None
        else:
            mapping[key] = priority

    for priority in current_priority_rows:
        add_strong_priority_identity(
            priority_by_customer_id,
            _text(priority.get("customer_id")),
            priority,
        )
        add_strong_priority_identity(
            priority_by_customer_number,
            normalize_key(priority.get("customer_number")),
            priority,
        )

    def planned_current_customer(planned):
        customer_id = _text(planned.get("customer_id"))
        if customer_id:
            return priority_by_customer_id.get(customer_id)
        customer_number = normalize_key(planned.get("customer_number"))
        if customer_number:
            return priority_by_customer_number.get(customer_number)
        return None

    def collect_planned_period(period_start, period_end, selected_seller=""):
        collected = []
        for raw_planned in planned_activities or ():
            planned = dict(raw_planned)
            scheduled = _datetime(planned.get("scheduled_at"))
            if not scheduled or not (period_start <= scheduled.date() <= period_end):
                continue
            planned_owner_matches = aliases.get(
                normalize_key(planned.get("user_name")), set()
            )
            planned_owner = (
                next(iter(planned_owner_matches))
                if len(planned_owner_matches) == 1 else ""
            )
            if normalize_key(planned_owner) not in coached_keys:
                continue
            if (
                selected_seller
                and normalize_key(planned_owner) != normalize_key(selected_seller)
            ):
                continue
            current_customer = planned_current_customer(planned)
            if segment != "all":
                current_segment = (
                    current_customer.get("segment")
                    if current_customer else "missing"
                )
                if current_segment != segment:
                    continue
            if lifecycle != "all":
                current_lifecycle = (
                    current_customer.get("lifecycle")
                    if current_customer else "missing"
                )
                if current_lifecycle != lifecycle:
                    continue
            planned["user_name"] = planned_owner
            planned["scheduled_datetime"] = scheduled
            planned["current_customer_state"] = current_customer
            collected.append(planned)
        return collected

    planned_period = collect_planned_period(start, end, seller)
    team_planned_period = collect_planned_period(start, end)
    previous_team_planned_period = collect_planned_period(
        comparison_start, comparison_end
    )

    def is_accountable_planned(row):
        status = normalize_key(row.get("status"))
        if status == "cancelled":
            return False
        if status in {"completed", "skipped"}:
            return True
        return row["scheduled_datetime"] <= generated

    accountable_planned = [
        row for row in planned_period
        if is_accountable_planned(row)
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
    valid_planned_by_source_contact = defaultdict(list)
    for planned in planned_activities or ():
        source_contact_id = _text(planned.get("source_contact_id"))
        if (
            source_contact_id
            and normalize_key(planned.get("status")) in {"planned", "completed"}
        ):
            valid_planned_by_source_contact[source_contact_id].append(planned)
    positive_with_next_step = []
    for row in positive_three_days_old:
        has_follow_up = bool(_date(row.get("follow_up_date")))
        has_linked_plan = bool(
            valid_planned_by_source_contact.get(row.get("contact_id"))
        )
        has_order = bool(attribution["contact_to_orders"].get(row.get("contact_id")))
        if has_follow_up or has_linked_plan or has_order:
            positive_with_next_step.append(row)
    mature_positive_without_order_or_followup = [
        row for row in positive_three_days_old
        if attribution["maturity"].get(row.get("contact_id")) == "mature"
        and not attribution["contact_to_orders"].get(row.get("contact_id"))
        and not _date(row.get("follow_up_date"))
        and not valid_planned_by_source_contact.get(row.get("contact_id"))
    ]

    weeks = defaultdict(list)
    for row in rows:
        if row.get("contact_week"):
            weeks[row["contact_week"]].append(row)
    weekly_trend = []
    for week, week_rows in sorted(weeks.items()):
        agg = _aggregate_period(week_rows, attribution)
        iso_year, iso_week = (int(value) for value in week.replace("W", "").split("-"))
        week_start = date.fromisocalendar(iso_year, iso_week, 1)
        week_end = week_start + timedelta(days=6)
        waiting_outcome_count = len(agg["waiting"])
        mature_contact_count = len(agg["mature"])
        outcome_complete = waiting_outcome_count == 0
        weekly_trend.append({
            "week": week,
            "period": {
                "start": max(start, week_start).isoformat(),
                "end": min(end, week_end).isoformat(),
            },
            "human_activities": len(agg["human"]),
            "reached": len(agg["sync_reached"]),
            "positive": len(agg["sync_positive"]),
            "mature_converted_contacts": len(agg["sync_converted"]),
            "attributed_orders": len(agg["attributed"]),
            "bom_ratio": agg["rates"]["bom_ratio"],
            "order_10d": agg["rates"]["order_10d"],
            "positive_to_order_10d": agg["rates"]["positive_to_order_10d"],
            "outcome_complete": outcome_complete,
            "waiting_outcome_count": waiting_outcome_count,
            "mature_contact_count": mature_contact_count,
            "incomplete": not outcome_complete,
        })

    channel_effectiveness = {}
    for key in ("visit", "phone", "email"):
        aggregate = _aggregate_period([row for row in rows if row.get("contact_type_key") == key], attribution)
        days = [item["days_to_order"] for item in aggregate["attributed"]]
        channel_effectiveness[key] = {
            "human_activities": len(aggregate["human"]),
            "reach": aggregate["rates"]["reach"],
            "positive_dialogue": aggregate["rates"]["positive_dialogue"],
            "positive_to_order_10d": aggregate["rates"]["positive_to_order_10d"],
            "order_10d": aggregate["rates"]["order_10d"],
            "bom_ratio": aggregate["rates"]["bom_ratio"] if key == "visit" else None,
            "attributed_orders": len(aggregate["attributed"]),
            "dfp": aggregate["dfp"],
            "order_value_by_currency": aggregate["order_value_by_currency"],
            "median_days_to_order": statistics.median(days) if len(days) >= 5 else None,
        }

    weekday_names = ("Måndag", "Tisdag", "Onsdag", "Torsdag", "Fredag", "Lördag", "Söndag")
    time_bands = (
        ("Före 10", lambda hour: hour < 10),
        ("10–13", lambda hour: 10 <= hour < 13),
        ("13–16", lambda hour: 13 <= hour < 16),
        ("Efter 16", lambda hour: hour >= 16),
    )

    def visit_pattern(groups):
        patterns = []
        for label, visits in groups:
            rate = _rate(
                sum(row.get("result_class") == "unreachable" for row in visits),
                len(visits),
            )
            if rate["denominator"] >= MIN_RATE_SAMPLE:
                patterns.append({"label": label, "bom_ratio": rate})
        return patterns

    visit_efficiency = {
        "bom_ratio": current["rates"]["bom_ratio"],
        "reach": channel_effectiveness["visit"]["reach"],
        "repeat_boms": {"customers": len(repeated), "visits": sum(len(value) for value in repeated.values())},
        "high_priority_boms": len(high_priority_boms),
        "high_priority_boms_metric": high_priority_boms_metric,
        "planned": _rate(len([row for row in current["boms"] if row.get("planned_activity_id")]), len([row for row in current["visits"] if row.get("planned_activity_id")])),
        "unplanned": _rate(len([row for row in current["boms"] if not row.get("planned_activity_id")]), len([row for row in current["visits"] if not row.get("planned_activity_id")])),
        "weekday_patterns": visit_pattern([
            (weekday_names[index], [row for row in current["visits"] if row.get("contact_at") and row["contact_at"].weekday() == index])
            for index in range(7)
        ]),
        "time_band_patterns": visit_pattern([
            (label, [row for row in current["visits"] if row.get("contact_at") and predicate(row["contact_at"].hour)])
            for label, predicate in time_bands
        ]),
    }

    priority_allocation = {
        "priority_focus": current["rates"]["priority_focus"],
        "snapshot_coverage": _rate(
            sum(
                row.get("priority_snapshot_quality") == "exact"
                for row in current["v2_contacts"]
            ),
            len(current["v2_contacts"]),
            minimum=1,
        ),
        "priority_percentile_coverage": _rate(
            len(current["percentile_rows"]),
            len(current["v2_contacts"]),
            minimum=1,
        ),
        "strategic_coverage": _rate(
            len(strategic_contacted), len(strategic_portfolio), minimum=MIN_RATE_SAMPLE
        ),
    }

    follow_up_discipline = {
        "positive_next_step_coverage": _rate(
            len(positive_with_next_step), len(positive_three_days_old)
        ),
        "positive_without_next_step": (
            len(positive_three_days_old) - len(positive_with_next_step)
        ),
        "planned_completed_in_time": _rate(
            len(completed_in_time), len(accountable_planned)
        ),
        "accountable_planned": len(accountable_planned),
        "overdue_rate": _rate(
            len(overdue_planned), len(accountable_planned)
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
    }

    def planning_metrics(period_rows):
        accountable = [
            row for row in period_rows
            if is_accountable_planned(row)
        ]
        completed = [
            row for row in accountable
            if normalize_key(row.get("status")) == "completed"
        ]
        on_time = []
        for planned in completed:
            completed_contact = contact_by_id.get(
                _text(planned.get("completed_contact_id"))
            )
            if (
                completed_contact
                and completed_contact.get("contact_date")
                and completed_contact["contact_date"]
                <= planned["scheduled_datetime"].date()
            ):
                on_time.append(planned)
        overdue = [
            row for row in accountable
            if normalize_key(row.get("status") or "planned") == "planned"
            and row["scheduled_datetime"] < generated
        ]
        return {
            "planned_completed_in_time": _rate(len(on_time), len(accountable)),
            "overdue_rate": _rate(len(overdue), len(accountable)),
        }

    def next_step_metrics(aggregate):
        assessable = [
            row for row in aggregate["positive"]
            if row.get("contact_date")
            and (generated.date() - row["contact_date"]).days >= 3
        ]
        covered = [
            row for row in assessable
            if _date(row.get("follow_up_date"))
            or valid_planned_by_source_contact.get(row.get("contact_id"))
            or attribution["contact_to_orders"].get(row.get("contact_id"))
        ]
        return _rate(len(covered), len(assessable))

    def enrich_seller_metrics(items, source_rows, planned_rows):
        for item in items:
            seller_key = normalize_key(item["seller"])
            aggregate = _aggregate_period(
                [
                    row for row in source_rows
                    if normalize_key(row.get("sales_user_name")) == seller_key
                ],
                attribution,
            )
            seller_planned = [
                row for row in planned_rows
                if normalize_key(row.get("user_name")) == seller_key
            ]
            item["positive_next_step_coverage"] = next_step_metrics(aggregate)
            item.update(planning_metrics(seller_planned))
        return items

    enrich_seller_metrics(seller_comparison, team_rows, team_planned_period)
    enrich_seller_metrics(
        previous_seller_comparison,
        previous_team_rows,
        previous_team_planned_period,
    )
    seller_comparison = add_seller_benchmarks(
        seller_comparison, previous_seller_comparison
    )

    selected_seller_metrics = next(
        (
            item for item in seller_comparison
            if normalize_key(item["seller"]) == normalize_key(seller)
        ),
        None,
    )
    if selected_seller_metrics and channel == "all":
        for metric_key in (
            "reach", "positive_dialogue", "positive_to_order_10d", "order_10d",
            "bom_ratio", "priority_focus",
        ):
            current["rates"][metric_key]["comparisons"] = dict(
                selected_seller_metrics[metric_key]["comparisons"]
            )
        follow_up_discipline["positive_next_step_coverage"]["comparisons"] = dict(
            selected_seller_metrics["positive_next_step_coverage"]["comparisons"]
        )
        follow_up_discipline["planned_completed_in_time"]["comparisons"] = dict(
            selected_seller_metrics["planned_completed_in_time"]["comparisons"]
        )

    active_activity_counts = [row["human_activities"] for row in seller_comparison]
    filtered_activity_comparison = {
        "peer_median": None,
        "peer_count": 0,
        "delta_peer": None,
        "previous_period": len(previous["human"]),
        "previous_period_status": (
            "sufficient"
            if len(previous["human"]) >= MIN_RATE_SAMPLE else "small_sample"
        ),
        "delta_previous": len(current["human"]) - len(previous["human"]),
    }
    kpis = {
        "human_activities": {
            "value": len(current["human"]),
            "metric_type": "count",
            "unit": "aktiviteter",
            "status": "small_sample" if len(current["human"]) < MIN_RATE_SAMPLE else "sufficient",
            "unique_customers": len({row.get("customer_identity_key") for row in current["human"] if row.get("customer_identity_key")}),
            "channel_mix": {
                key: sum(row.get("contact_type_key") == key for row in current["human"])
                for key in ("visit", "phone", "email")
            },
            "comparisons": {
                **filtered_activity_comparison,
                **(
                    selected_seller_metrics["human_activities_metric"][
                        "comparisons"
                    ]
                    if selected_seller_metrics and channel == "all" else {}
                ),
            },
        },
        "reach": current["rates"]["reach"],
        "positive_dialogue": current["rates"]["positive_dialogue"],
        "positive_to_order_10d": {
            **current["rates"]["positive_to_order_10d"],
            "waiting_outcome_count": sum(
                bool(row.get("customer_identity_key"))
                and attribution["maturity"].get(row.get("contact_id"))
                == "waiting_outcome"
                for row in current["positive"]
            ),
        },
    }
    for key, metric in kpis.items():
        metric.update(KPI_DEFINITIONS[key])
    data_quality = _data_quality(rows, canonical_result, order_result, attribution)
    sales_matrix_sellers = [
        {
            **item,
            "sample_status": (
                "sufficient"
                if item["positive_dialogue"]["status"] == "sufficient"
                and item["positive_to_order_10d"]["status"] == "sufficient"
                else "small_sample"
            ),
        }
        for item in seller_comparison
        if item["positive_dialogue"]["value"] is not None
        and item["positive_to_order_10d"]["value"] is not None
    ]
    sales_positioned_names = {item["seller"] for item in sales_matrix_sellers}
    sales_matrix = {
        "type": "sales",
        "axes": {
            "x": {
                "key": "positive_to_order_10d",
                "label": "Positiv dialog → order inom 10 dagar",
            },
            "y": {"key": "positive_dialogue", "label": "Positiv dialog"},
        },
        "sellers": sales_matrix_sellers,
        "medians": {
            "positive_to_order_10d": _sufficient_median(
                seller_comparison, "positive_to_order_10d"
            ),
            "positive_dialogue": _sufficient_median(
                seller_comparison, "positive_dialogue"
            ),
        },
        "insufficient_sample": [
            {
                "seller": item["seller"],
                "human_activities": item["human_activities"],
                "reasons": [
                    reason for reason, applies in (
                        ("positive_denominator_zero", item["positive_dialogue"]["denominator"] == 0),
                        ("positive_order_denominator_zero", item["positive_to_order_10d"]["denominator"] == 0),
                    ) if applies
                ],
            }
            for item in seller_comparison
            if item["seller"] not in sales_positioned_names
        ],
    }

    comparable_sellers = [
        item for item in seller_comparison
        if item["order_10d"]["status"] == "sufficient"
        and item["priority_focus"]["status"] == "sufficient"
        and item["priority_percentile_coverage"]["value"] is not None
        and item["priority_percentile_coverage"]["value"] >= MIN_PRIORITY_COVERAGE
    ]
    comparable_names = {item["seller"] for item in comparable_sellers}
    team_priority_numerator = sum(
        item["priority_percentile_coverage"]["numerator"]
        for item in seller_comparison
    )
    team_priority_denominator = sum(
        item["priority_percentile_coverage"]["denominator"]
        for item in seller_comparison
    )
    team_priority_coverage = _rate(
        team_priority_numerator, team_priority_denominator, minimum=1
    )
    priority_matrix_available = (
        team_priority_coverage["value"] is not None
        and team_priority_coverage["value"] >= MIN_PRIORITY_COVERAGE
        and len(comparable_sellers) >= 2
    )
    priority_matrix_sellers = [
        {**item, "sample_status": "sufficient"}
        for item in comparable_sellers
    ] if priority_matrix_available else []
    priority_matrix = {
        "type": "priority",
        "available": priority_matrix_available,
        "axes": {
            "x": {"key": "order_10d", "label": "Kontakt → order inom 10 dagar"},
            "y": {"key": "priority_focus", "label": "Historiskt prioritetsfokus"},
        },
        "sellers": priority_matrix_sellers,
        "medians": {
            "order_10d": statistics.median(item["order_10d"]["value"] for item in comparable_sellers) if len(comparable_sellers) >= 2 else None,
            "priority_focus": statistics.median(item["priority_focus"]["value"] for item in comparable_sellers) if len(comparable_sellers) >= 2 else None,
        },
        "build_up": {
            "coverage": team_priority_coverage,
            "minimum_coverage": MIN_PRIORITY_COVERAGE,
            "comparable_seller_count": len(comparable_sellers),
            "required_seller_count": 2,
        },
        "insufficient_sample": [
            {
                "seller": item["seller"],
                "human_activities": item["human_activities"],
                "order_denominator": item["order_10d"]["denominator"],
                "priority_percentile_coverage": item["priority_percentile_coverage"],
                "reasons": [
                    reason for reason, applies in (
                        ("order_denominator_zero", item["order_10d"]["denominator"] == 0),
                        ("order_sample_below_10", 0 < item["order_10d"]["denominator"] < MIN_RATE_SAMPLE),
                        ("priority_denominator_zero", item["priority_focus"]["denominator"] == 0),
                        ("priority_sample_below_10", 0 < item["priority_focus"]["denominator"] < MIN_RATE_SAMPLE),
                        ("priority_percentile_coverage_below_70", item["priority_percentile_coverage"]["value"] is None or item["priority_percentile_coverage"]["value"] < MIN_PRIORITY_COVERAGE),
                    ) if applies
                ],
            }
            for item in seller_comparison if item["seller"] not in comparable_names
        ],
    }
    coaching_matrix = priority_matrix
    if seller and selected_seller_metrics:
        signal_metrics = {
            **selected_seller_metrics,
            "overdue_rate": selected_seller_metrics["overdue_rate"],
            "priority_percentile_coverage": selected_seller_metrics[
                "priority_percentile_coverage"
            ],
        }
        coaching_cards = build_seller_signals(
            seller=selected_seller_metrics["seller"],
            metrics=signal_metrics,
            repeat_boms=visit_efficiency["repeat_boms"],
            channel_effectiveness=channel_effectiveness,
        )
    else:
        previous_team_aggregate = _aggregate_period(previous_team_rows, attribution)
        previous_team_planning = planning_metrics(previous_team_planned_period)
        coaching_cards = build_team_signals(
            metrics={
                **current["rates"],
                **follow_up_discipline,
            },
            previous_metrics={
                **previous_team_aggregate["rates"],
                "positive_next_step_coverage": next_step_metrics(
                    previous_team_aggregate
                ),
                **previous_team_planning,
            },
            repeat_boms=visit_efficiency["repeat_boms"],
        )
    result = {
        "meta": {
            "generated_at": generated.isoformat(timespec="seconds"),
            "period": {"start": start.isoformat(), "end": end.isoformat()},
            "comparison_period": {"start": comparison_start.isoformat(), "end": comparison_end.isoformat()},
            "filters": {"seller": seller, "channel": channel, "segment": segment, "lifecycle": lifecycle},
            "definitions_version": DEFINITIONS_VERSION,
            "score_version": score_version,
            "base_score_version": score_version,
            "contact_metric_dimension_basis": "historical_snapshot",
            "planned_metric_dimension_basis": "current_customer_state",
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
        "team_comparison": {
            "selected_seller": seller,
            "sellers": seller_comparison,
            "benchmarks": {
                "human_activities_median": statistics.median(active_activity_counts) if active_activity_counts else None,
                "positive_dialogue_median": _sufficient_median(seller_comparison, "positive_dialogue"),
                "order_10d_median": _sufficient_median(seller_comparison, "order_10d"),
            },
        },
        "coaching_matrices": {"sales": sales_matrix, "priority": priority_matrix},
        "coaching_matrix": coaching_matrix,
        "funnel": {
            "attempts": len(current["sync"]),
            "reached": len(current["sync_reached"]),
            "positive": len(current["sync_positive"]),
            "reach_rate": current["rates"]["reach"],
            "steps": [
                {"key": "attempts", "label": "Synkrona kontaktförsök", "count": len(current["sync"]), "rate": None, "drilldown_metric": "attempts"},
                {"key": "reached", "label": "Nådda synkrona kontakter", "count": len(current["sync_reached"]), "rate": current["rates"]["reach"], "drilldown_metric": "reach"},
                {"key": "positive", "label": "Positiva synkrona dialoger", "count": len(current["sync_positive"]), "rate": _rate(len(current["sync_positive"]), len(current["sync_reached"])), "drilldown_metric": "positive_sync"},
            ],
        },
        "outcome_10d": {
            "mature_contact_count": len(current["mature"]),
            "attributed_order_contact_count": len(current["ordered_contacts"]),
            "waiting_outcome_count": len(current["waiting"]),
            "order_10d": current["rates"]["order_10d"],
            "positive_to_order_10d": current["rates"]["positive_to_order_10d"],
        },
        "weekly_trend": weekly_trend,
        "visit_efficiency": visit_efficiency,
        "channel_effectiveness": channel_effectiveness,
        "priority_allocation": priority_allocation,
        "follow_up_discipline": follow_up_discipline,
        "coaching_cards": coaching_cards,
        "_analysis": {
            "rows": rows,
            "attribution": attribution,
            "drilldown_contact_ids": {
                "followup_success": {
                    row.get("contact_id") for row in positive_with_next_step
                },
                "followup_gap": {
                    row.get("contact_id") for row in positive_three_days_old
                    if row not in positive_with_next_step
                },
                "followup_gap_10d": {
                    row.get("contact_id")
                    for row in mature_positive_without_order_or_followup
                },
            },
            "planned_drilldown_rows": {
                metric: [
                    {
                        "contact_id": _text(planned.get("completed_contact_id")),
                        "date_time": _text(planned.get("scheduled_at")),
                        "sales_user_name": _text(planned.get("user_name")),
                        "customer": _text(planned.get("customer")),
                        "customer_id": _text(planned.get("customer_id")),
                        "channel": normalize_contact_type(
                            planned.get("contact_type")
                        ),
                        "result_class": normalize_key(planned.get("status")),
                        "snapshot_quality": "",
                        "priority_at_contact": None,
                        "priority_percentile_at_contact": None,
                        "order_reference": "",
                        "days_to_order": None,
                        "dfp": 0,
                        "data_quality_flags": [],
                        "cohort_role": (
                            "numerator"
                            if (
                                metric == "planned_on_time"
                                and planned in completed_in_time
                            ) or (
                                metric == "planned_overdue"
                                and planned in overdue_planned
                            ) or (
                                metric == "planned_skipped"
                                and planned in skipped_planned
                            )
                            else "missed_outcome"
                            if metric in {"planned_on_time", "planned_overdue"}
                            else "denominator_only"
                        ),
                    }
                    for planned in planned_rows
                ]
                for metric, planned_rows in {
                    "planned_on_time": accountable_planned,
                    "planned_overdue": accountable_planned,
                    "planned_skipped": accountable_planned,
                }.items()
            },
        },
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
    drilldown_contact_ids = analysis.get("drilldown_contact_ids", {})
    planned_rows = analysis.get("planned_drilldown_rows", {}).get(metric)
    if planned_rows is not None:
        selected = list(planned_rows)
        selected.sort(
            key=lambda row: (
                _datetime(row.get("date_time")) or datetime.min,
                row.get("contact_id") or "",
            ),
            reverse=True,
        )
        return {
            "metric": metric,
            "limit": limit,
            "rows": selected[:limit],
            "total_count": len(selected),
        }
    selected = []
    repeated_keys = defaultdict(int)
    for row in analysis.get("rows", ()):
        if row.get("is_human") and row.get("customer_identity_key") and row.get("contact_type_key") == "visit" and row.get("result_class") == "unreachable":
            repeated_keys[row.get("customer_identity_key")] += 1
    for row in analysis.get("rows", ()):
        orders = attribution.get("contact_to_orders", {}).get(row.get("contact_id"), ())
        maturity = attribution.get("maturity", {}).get(row.get("contact_id"))
        include = {
            "human_activities": row.get("is_human"),
            "attempts": row.get("is_human") and row.get("contact_type_key") in SYNCHRONOUS_CHANNELS and row.get("result_class") in ANALYSABLE_RESULTS,
            "reach": row.get("is_human") and row.get("contact_type_key") in SYNCHRONOUS_CHANNELS and row.get("result_class") in ANALYSABLE_RESULTS,
            "positive_sync": _is_sync_reached(row) and row.get("result_class") in {"positive", "order"},
            "positive_dialogue": _is_reached_human(row),
            "positive_to_order_10d": _is_reached_human(row) and row.get("result_class") in {"positive", "order"} and bool(row.get("customer_identity_key")) and maturity == "mature",
            "mature_reached_sync": _is_sync_reached(row) and bool(row.get("customer_identity_key")) and maturity == "mature",
            "order_10d": _is_attribution_eligible(row) and maturity == "mature",
            "order_10d_sync": _is_sync_reached(row) and bool(row.get("customer_identity_key")) and maturity == "mature" and bool(orders),
            "waiting_outcome": _is_attribution_eligible(row) and maturity == "waiting_outcome",
            "priority_focus": row.get("is_human") and _is_comparable_priority_percentile(row),
            "bom_ratio": row.get("is_human") and row.get("contact_type_key") == "visit" and row.get("result_class") in ANALYSABLE_RESULTS,
            "planned_boms": row.get("is_human") and row.get("planned_activity_id") and row.get("contact_type_key") == "visit" and row.get("result_class") == "unreachable",
            "unplanned_boms": row.get("is_human") and not row.get("planned_activity_id") and row.get("contact_type_key") == "visit" and row.get("result_class") == "unreachable",
            "repeat_boms": row.get("is_human") and row.get("contact_type_key") == "visit" and row.get("result_class") == "unreachable" and repeated_keys[row.get("customer_identity_key")] >= 2,
            "high_priority_boms": row.get("is_human") and row.get("contact_type_key") == "visit" and row.get("result_class") == "unreachable" and _is_comparable_priority_percentile(row) and row.get("priority_percentile_at_contact") >= 75,
            "followup_success": row.get("contact_id") in drilldown_contact_ids.get("followup_success", set()),
            "followup_gap": row.get("contact_id") in drilldown_contact_ids.get("followup_gap", set()),
            "followup_gap_10d": row.get("contact_id") in drilldown_contact_ids.get("followup_gap_10d", set()),
            "data_quality": bool(row.get("analysis_exclusions")) or row.get("contact_type_key") == "unknown" or row.get("result_class") == "unknown",
        }[metric]
        if not include:
            continue
        first_order = orders[0] if orders else None
        customer = row.get("customer_record") or {}
        numerator = {
            "reach": _is_sync_reached(row),
            "positive_dialogue": row.get("result_class") in {"positive", "order"},
            "positive_to_order_10d": bool(orders),
            "order_10d": bool(orders),
            "priority_focus": (row.get("priority_percentile_at_contact") or -1) >= 75,
            "bom_ratio": row.get("result_class") == "unreachable",
        }.get(metric, True)
        cohort_role = (
            "numerator" if numerator
            else "missed_outcome"
            if metric in {
                "positive_to_order_10d", "order_10d",
                "followup_gap", "followup_gap_10d",
            }
            else "denominator_only"
        )
        selected.append({
            "contact_id": row.get("contact_id"),
            "date_time": row.get("date_time"),
            "sales_user_name": row.get("sales_user_name"),
            "customer": customer.get("customer") or row.get("customer"),
            "customer_id": customer.get("customer_id") or "",
            "channel": row.get("contact_type_key"),
            "result_class": row.get("result_class"),
            "snapshot_quality": row.get("priority_snapshot_quality"),
            "analytics_snapshot_version": row.get("analytics_snapshot_version"),
            "priority_at_contact": row.get("priority_score_at_contact"),
            "priority_percentile_at_contact": row.get("priority_percentile_at_contact"),
            "priority_percentile_basis_at_contact": row.get(
                "priority_percentile_basis_at_contact"
            ),
            "priority_percentile_comparability": (
                "comparable_v2"
                if _is_comparable_priority_percentile(row)
                else "legacy_incomparable"
                if row.get("priority_percentile_at_contact") is not None
                else "missing"
            ),
            "order_reference": first_order["order"].get("reference") if first_order else "",
            "days_to_order": first_order.get("days_to_order") if first_order else None,
            "dfp": first_order["order"].get("dfp") if first_order else 0,
            "data_quality_flags": list(row.get("analysis_exclusions") or ()),
            "cohort_role": cohort_role,
        })
    selected.sort(key=lambda row: (_datetime(row.get("date_time")) or datetime.min, row.get("contact_id") or ""), reverse=True)
    return {"metric": metric, "limit": limit, "rows": selected[:limit], "total_count": len(selected)}


def strip_internal_analysis(summary):
    return {key: value for key, value in summary.items() if key != "_analysis"}


def build_pre_contact_snapshot(
    *, customer, owner, priorities, planned_row=None, score_version="",
    contact_at=None, snapshot_created_at=None,
):
    """Build an exact snapshot from the authoritative pre-append priority universe."""
    owner_keys = {
        normalize_key((owner or {}).get("user_name")),
        normalize_key((owner or {}).get("name")),
    } - {""}
    owner_portfolio = [
        row for row in priorities or ()
        if normalize_key(row.get("sales_person")) in owner_keys
        and _optional_bool(row.get("cancelled_flag")) is not True
        and _optional_bool(row.get("active")) is not False
        and normalize_key(row.get("recommendation_suppression_reason"))
        != "invalid_or_inactive_owner"
    ]
    score_universe = [
        row for row in owner_portfolio
        if _number(row.get("priority_score")) is not None
    ]
    snapshot_instant = (
        _stockholm_datetime(snapshot_created_at)
        or datetime.now(STOCKHOLM_ZONE)
    )
    contact_instant = _stockholm_datetime(contact_at)
    snapshot_lag_hours = (
        round((snapshot_instant - contact_instant).total_seconds() / 3600, 2)
        if contact_instant else None
    )
    customer_id = _text((customer or {}).get("customer_id"))
    customer_number = normalize_key((customer or {}).get("customer_number"))
    customer_name = normalize_key((customer or {}).get("customer"))
    target = next((row for row in owner_portfolio if (
        (customer_id and _text(row.get("customer_id")) == customer_id)
        or (not customer_id and customer_number and normalize_key(row.get("customer_number")) == customer_number)
        or (not customer_id and not customer_number and normalize_key(row.get("customer")) == customer_name)
    )), None)
    if target is None:
        return {
            "analytics_snapshot_version": ANALYTICS_SNAPSHOT_VERSION,
            "priority_snapshot_quality": "missing",
            "priority_score_version": score_version,
            "snapshot_created_at": snapshot_instant.isoformat(timespec="seconds"),
            "snapshot_lag_hours": (
                snapshot_lag_hours if snapshot_lag_hours is not None else ""
            ),
        }
    target_eligible = _optional_bool(target.get("recommendation_eligible"))
    scores = sorted(_number(row.get("priority_score")) for row in score_universe)
    score = _number(target.get("priority_score"))
    percentile = (
        round(
            100
            * (
                sum(value < score for value in scores)
                + 0.5 * sum(value == score for value in scores)
            )
            / len(scores),
            2,
        )
        if score is not None and scores else None
    )
    source = normalize_key((planned_row or {}).get("source"))
    activity_source = source if source in {"follow_up", "route", "system_suggestion"} else ("planned" if planned_row else "manual")
    suppression_source_id = _text(
        target.get("recommendation_suppression_source_id")
    )
    suppression_reason = _text(
        target.get("recommendation_suppression_reason")
        or target.get("suppression_reason")
    )
    if (
        suppression_reason == "future_planned_activity"
        and suppression_source_id
        and suppression_source_id
        == _text((planned_row or {}).get("planned_activity_id"))
    ):
        suppression_reason = "current_planned_activity"
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
        "seller_portfolio_size_at_contact": len(score_universe),
        "intent_timing_at_contact": target.get("intent_timing", ""),
        "value_index_at_contact": target.get("value_index", ""),
        "strategic_index_at_contact": target.get("strategic_index", ""),
        "expected_order_dfp_at_contact": target.get("expected_order_dfp", ""),
        "lifecycle_at_contact": target.get("lifecycle", ""),
        "customer_segment_at_contact": _text(target.get("segment") or (customer or {}).get("customer_segment")),
        "recommendation_eligible_at_contact": (
            target_eligible if target_eligible is not None else ""
        ),
        "suppression_reason_at_contact": suppression_reason,
        "suppression_source_id_at_contact": suppression_source_id,
        "snapshot_created_at": snapshot_instant.isoformat(timespec="seconds"),
        "snapshot_lag_hours": (
            snapshot_lag_hours if snapshot_lag_hours is not None else ""
        ),
        "priority_percentile_basis_at_contact": (
            PRIORITY_PERCENTILE_BASIS if percentile is not None else ""
        ),
    }
