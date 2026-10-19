from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo
import re
from commercial_orders import order_volume, commercial_order
from contact_channel import recommend_contact_channel
from customer_obligations import stable_contact_source_id
from statistics import median
from value_prioritization import (VERSION as VALUE_MODEL_VERSION, POLICY as VALUE_POLICY,
    POLICY_VERSION as VALUE_POLICY_VERSION,
    REACHED_RESULTS, DO_NOT_CONTACT_RESULTS, complete_deliveries,
    segment_prior, business_opportunity, contact_opportunity,
    store_contact_guidance,
    store_format, empirical_format_prior)
import unicodedata
import uuid


FREEZER_FIELDS = ("Franui", "Schufrulade", "Boujee", "polarbar", "none")
OTHER_COMPETITOR_FREEZER_FIELDS = {"Schufrulade", "Boujee"}
# The application has one model. Historical models live only in Git history.
SCORE_VERSION = VALUE_MODEL_VERSION
GUIDANCE_VERSION = "3.0"
STOCKHOLM = ZoneInfo("Europe/Stockholm")


class FeatureSnapshot(dict):
    """In-process provenance for replay; attributes never enter API JSON."""
    def __init__(self, values, *, source_rows, as_of, canonical):
        super().__init__(values)
        self.source_rows = source_rows
        self.as_of = as_of
        self.canonical = canonical


def _local_datetime(value) -> datetime | None:
    """Stockholm wall time for legacy comparisons, preserving UTC ordering.

    Public snapshots carry an aware as_of. Historical row timestamps without
    an offset are Stockholm local time; an offset must never be truncated.
    """
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        except (TypeError, ValueError):
            parsed = parse_datetime(value)
    if parsed and parsed.tzinfo:
        parsed = parsed.astimezone(STOCKHOLM).replace(tzinfo=None)
    return parsed


def _snapshot_time(as_of=None, *, today=None) -> datetime:
    if as_of is None:
        return (datetime.combine(today, time.max, STOCKHOLM) if today
                else datetime.now(STOCKHOLM))
    parsed = _local_datetime(as_of)
    if parsed is None:
        raise ValueError("Invalid as_of timestamp")
    return parsed.replace(tzinfo=STOCKHOLM)


def _event_timestamp(value):
    local = _local_datetime(value)
    return local.replace(tzinfo=STOCKHOLM).timestamp() if local else None


def _known_at(row: dict, as_of: datetime) -> bool:
    recorded = _event_timestamp(row.get("recorded_at") or row.get("created_at"))
    return recorded is None or recorded <= as_of.timestamp()


def _canonicalize_rows(rows, customers, *, name_field, number_field):
    """Resolve identity BEFORE aggregation, using exact unique identities only.

    A conflicting strong identity is quarantined, never rescued by its name.
    This also keeps old name-only opt-outs/orders when newer rows carry IDs.
    """
    rows = list(rows or ())
    if customers is None:
        by_number, by_name = _inferred_ids(rows, name_field=name_field, number_field=number_field)
        result = []
        for original in rows:
            row = dict(original)
            cid, number, name = _identity_parts(row, name_field=name_field, number_field=number_field)
            inferred = by_number.get(number) if number else by_name.get(name)
            if not cid and inferred:
                row["customer_id"] = inferred
            result.append(row)
        return result
    indices = {key: defaultdict(list) for key in ("id", "number", "name")}
    for master in customers:
        for key, value in zip(("id", "number", "name"), _identity_parts(
                master, name_field="customer", number_field="customer_number")):
            if value:
                indices[key][value].append(master)
    result = []
    for original in rows:
        row = dict(original)
        cid, number, name = _identity_parts(row, name_field=name_field, number_field=number_field)
        matches = indices["id"].get(cid, []) if cid else indices["number"].get(number, []) if number else indices["name"].get(name, [])
        if len(matches) != 1:
            # Keep unknown identities isolated; the master lookup cannot use
            # a name fallback after a conflicting strong key.
            if cid or number or len(matches) > 1:
                row["_identity_unresolved"] = True
            result.append(row)
            continue
        master = matches[0]
        mid, mnumber, _ = _identity_parts(master, name_field="customer", number_field="customer_number")
        if (cid and mid != cid) or (number and mnumber and number != mnumber):
            row["_identity_unresolved"] = True
            result.append(row)
            continue
        row["customer_id"] = str(master.get("customer_id") or "").strip()
        row[number_field] = str(master.get("customer_number") or number).strip()
        row[name_field] = str(master.get("customer") or row.get(name_field) or "").strip()
        result.append(row)
    return result


def normalize_customer_key(value: str) -> str:
    text = str(value or "").replace("\xa0", " ").strip().casefold()
    return " ".join(text.split())


def _identity_parts(row: dict, *, name_field: str, number_field: str) -> tuple[str, str, str]:
    return (
        normalize_customer_key(row.get("customer_id")),
        normalize_customer_key(row.get(number_field)),
        normalize_customer_key(row.get(name_field)),
    )


def _identity_group_key(customer_id: str, customer_number: str, customer_name: str) -> str:
    if customer_id:
        return f"id:{customer_id}"
    if customer_number:
        return f"number:{customer_number}"
    return f"name:{customer_name}"


def _inferred_ids(rows, *, name_field, number_field):
    number_ids = defaultdict(set)
    name_ids = defaultdict(set)
    for row in rows:
        customer_id, customer_number, customer_name = _identity_parts(
            row, name_field=name_field, number_field=number_field
        )
        if not customer_id:
            continue
        if customer_number:
            number_ids[customer_number].add(customer_id)
        if customer_name:
            name_ids[customer_name].add(customer_id)
    return (
        {key: next(iter(values)) for key, values in number_ids.items() if len(values) == 1},
        {key: next(iter(values)) for key, values in name_ids.items() if len(values) == 1},
    )


def _feature_identity_indices(features: dict) -> dict:
    """Index a feature set using canonical identity with safe legacy names."""
    indices = {"id": {}, "number": {}, "name": {}}
    name_candidates = defaultdict(list)
    for feature in features.values():
        customer_id = normalize_customer_key(feature.get("customer_id"))
        customer_number = normalize_customer_key(feature.get("customer_number"))
        customer_name = normalize_customer_key(feature.get("customer_key"))
        if customer_id:
            indices["id"][customer_id] = feature
        if customer_number:
            indices["number"][customer_number] = feature
        if customer_name:
            name_candidates[customer_name].append(feature)
    indices["name"] = {
        name: matches[0]
        for name, matches in name_candidates.items()
        if len({id(match) for match in matches}) == 1
    }
    return indices


def _identity_feature(row: dict, indices: dict, *, name_field: str, number_field: str,
                      ambiguous_master_names: set[str] | None = None,
                      allow_name_after_strong_miss: bool = True) -> dict:
    customer_id, customer_number, customer_name = _identity_parts(
        row, name_field=name_field, number_field=number_field
    )
    if customer_id:
        match = indices["id"].get(customer_id)
        if match:
            return match
    if customer_number:
        match = indices["number"].get(customer_number)
        if match:
            match_id = normalize_customer_key(match.get("customer_id"))
            if customer_id and match_id and match_id != customer_id:
                return {}
            return match
    if (customer_id or customer_number) and not allow_name_after_strong_miss:
        return {}
    if customer_name and customer_name not in (ambiguous_master_names or set()):
        match = indices["name"].get(customer_name) or {}
        if (customer_id or customer_number) and (
            normalize_customer_key(match.get("customer_id"))
            or normalize_customer_key(match.get("customer_number"))
        ):
            return {}
        return match
    return {}


def parse_date(value) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    text = str(value or "").replace("\xa0", " ").strip()
    if not text:
        return None

    if _looks_like_excel_serial(text):
        parsed = _parse_excel_serial(text)
        if parsed:
            return parsed.date()

    normalized = text.replace("Z", "").replace("T", " ").strip()
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y/%m/%d",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y",
        "%d.%m.%Y %H:%M:%S",
        "%d.%m.%Y %H:%M",
        "%d.%m.%Y",
    ):
        try:
            return datetime.strptime(normalized[: len(datetime.now().strftime(fmt))], fmt).date()
        except ValueError:
            pass

    try:
        return datetime.fromisoformat(normalized).date()
    except ValueError:
        return None


def parse_datetime(value) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, time.min)

    text = str(value or "").replace("\xa0", " ").strip()
    if not text:
        return None

    if _looks_like_excel_serial(text):
        return _parse_excel_serial(text)

    normalized = text.replace("Z", "").replace("T", " ").strip()
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y/%m/%d",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y",
        "%d.%m.%Y %H:%M:%S",
        "%d.%m.%Y %H:%M",
        "%d.%m.%Y",
    ):
        try:
            return datetime.strptime(normalized[: len(datetime.now().strftime(fmt))], fmt)
        except ValueError:
            pass

    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        parsed_date = parse_date(value)
        return datetime.combine(parsed_date, time.min) if parsed_date else None


def normalize_contact_result(result: str) -> str:
    if not result:
        return "Okänd"

    value = str(result).replace("\xa0", " ").strip()
    mapping = {
        "Order lagd!": "Order lagd",
        "Order lagd": "Order lagd",
        "Intresserad/Återkom :)": "Positiv",
        "Positivt": "Positiv",
        "Positiv": "Positiv",
        "Kräver mer bearbetning!": "Negativ",
        "Återkom ej": "Negativ",
        "Negativ": "Negativ",
        "Negativt": "Negativ",
        "Neutral": "Neutral",
        "Neutralt": "Neutral",
        "Uppföljning behövs": "Neutral",
        "Ej anträffbar": "Ej anträffbar",
    }
    return mapping.get(value, value)


def build_order_features(order_rows: list[dict], *, customers=None, as_of=None) -> dict:
    snapshot = _snapshot_time(as_of)
    source_rows = list(order_rows)
    # Preserve the commercial-source exclusion before identity resolution can
    # replace an internal order's raw customer label with the master name.
    order_rows = [row for row in source_rows
                  if "polarbär" not in str(row.get("Customer") or "").casefold()]
    order_rows = _canonicalize_rows(order_rows, customers, name_field="Customer", number_field="Customer number")
    orders = {}
    inferred_by_number, _inferred_by_name = _inferred_ids(
        order_rows, name_field="Customer", number_field="Customer number"
    )
    for idx, row in enumerate(order_rows):
        if row.get("_identity_unresolved") or not _known_at(row, snapshot):
            continue
        customer = str(row.get("Customer") or "").strip()
        if not customer or "polarbär" in customer.casefold():
            continue

        dfp = _order_dfp(row)
        reference = str(row.get("Reference") or "").strip() or str(row.get("Order date") or row.get("Delivery date") or f"row-{idx}")
        order_date = parse_date(row.get("Order date"))
        if as_of is not None and order_date and _event_timestamp(row.get("Order date")) > snapshot.timestamp():
            continue
        confirmed_delivery_date = parse_date(row.get("Delivery date"))
        delivery_date = confirmed_delivery_date or order_date
        total = _parse_number(row.get("Total"))
        raw_customer_id = str(row.get("customer_id") or "").strip()
        customer_number = str(row.get("Customer number") or "").strip()
        customer_id = raw_customer_id or inferred_by_number.get(
            normalize_customer_key(customer_number), ""
        )
        sku = str(row.get("SKU") or row.get("Product") or "").strip()
        grouping_key = _identity_group_key(
            normalize_customer_key(customer_id),
            normalize_customer_key(customer_number),
            normalize_customer_key(customer),
        )

        order = orders.setdefault(
            (grouping_key, reference),
            {
                "reference": reference,
                "customer": customer,
                "customer_key": normalize_customer_key(customer),
                "customer_id": customer_id,
                "customer_number": customer_number,
                "order_date": order_date,
                "delivery_date": delivery_date,
                "dfp": 0.0,
                "sales": 0.0,
                "volume_known": False,
                "delivery_volume_complete": True,
                "confirmed_delivery": bool(confirmed_delivery_date),
                "skus": set(),
                "skus_complete": True,
            },
        )
        if dfp is None or delivery_date != order["delivery_date"]:
            # Do not present a partial total, or attach several delivery dates
            # to the final date of an inconsistent logical order.
            order["delivery_volume_complete"] = False
        if not confirmed_delivery_date:
            order["confirmed_delivery"] = False
        if dfp is not None:
            order["dfp"] += dfp
            order["volume_known"] = True
        order["sales"] += total
        if sku:
            order["skus"].add(normalize_customer_key(sku))
        else:
            order["skus_complete"] = False
        if order_date and (not order["order_date"] or order_date > order["order_date"]):
            order["order_date"] = order_date
        if delivery_date and (not order["delivery_date"] or delivery_date > order["delivery_date"]):
            order["delivery_date"] = delivery_date
        if customer_number and not order["customer_number"]:
            order["customer_number"] = customer_number
        if customer_id and not order["customer_id"]:
            order["customer_id"] = customer_id

    orders_by_customer = defaultdict(list)
    for order in orders.values():
        if not order.pop("volume_known"):
            order["dfp"] = None
        key = _identity_group_key(
            normalize_customer_key(order.get("customer_id")),
            normalize_customer_key(order.get("customer_number")),
            order["customer_key"],
        )
        orders_by_customer[key].append(order)

    features = {}
    for customer_orders in orders_by_customer.values():
        customer_orders.sort(key=lambda o: o.get("delivery_date") or o.get("order_date") or date.min)
        identity_orders = customer_orders
        customer_orders = [o for o in customer_orders if commercial_order(o["sales"], o["dfp"])]
        latest_order = (customer_orders or identity_orders)[-1]
        order_dates = [o["order_date"] for o in customer_orders if o.get("order_date")]
        delivery_dates = sorted({o["delivery_date"] for o in customer_orders if o.get("delivery_date")})
        # Match the existing unique-delivery-date grain, across references.
        # A missing row/order volume makes that delivery total unknown, not zero.
        # This map is additive: it does not alter scoring or historical fields.
        delivery_orders = defaultdict(list)
        for delivery_order in customer_orders:
            if delivery_order.get("delivery_date"):
                delivery_orders[delivery_order["delivery_date"]].append(delivery_order)
        delivery_dfp_by_date = {
            day.isoformat(): (
                _clean_number(sum(o["dfp"] for o in rows))
                if all(o["dfp"] is not None and o["delivery_volume_complete"] for o in rows)
                else None
            )
            for day, rows in delivery_orders.items()
        }
        # A Delivery date is a dated commercial delivery record, not proof
        # of physical receipt. An undated order whose fallback happens to
        # share this date must not erase the independently dated records.
        dated_delivery_orders = {
            day: [item for item in rows if item["confirmed_delivery"]]
            for day, rows in delivery_orders.items()
        }
        dated_delivery_orders = {day: rows for day, rows in dated_delivery_orders.items() if rows}
        confirmed_delivery_dfp_by_date = {
            day.isoformat(): (_clean_number(sum(o["dfp"] for o in rows))
                if all(o["dfp"] is not None and o["delivery_volume_complete"] for o in rows) else None)
            for day, rows in dated_delivery_orders.items()
        }
        confirmed_delivery_dates = sorted(dated_delivery_orders)
        first_dated_rows = dated_delivery_orders.get(confirmed_delivery_dates[0], []) if confirmed_delivery_dates else []
        first_dated_skus_known = bool(first_dated_rows) and all(o["skus_complete"] for o in first_dated_rows)
        first_dated_skus = {sku for o in first_dated_rows for sku in o["skus"]}
        known_volumes = [o["dfp"] for o in customer_orders if o["dfp"] is not None]
        total_dfp = sum(known_volumes) if known_volumes else None
        total_sales = sum(o["sales"] for o in customer_orders)
        order_count = len(customer_orders)
        avg_dfp = total_dfp / len(known_volumes) if known_volumes else None
        avg_sales = total_sales / order_count if order_count else 0
        latest_dfp = latest_order.get("dfp", 0)
        latest_sales = latest_order.get("sales", 0)
        first_delivery_date = next(
            (
                order.get("delivery_date") or order.get("order_date")
                for order in customer_orders
                if order.get("delivery_date") or order.get("order_date")
            ),
            None,
        )
        first_delivery_skus = {
            sku
            for order in customer_orders
            if (order.get("delivery_date") or order.get("order_date"))
            == first_delivery_date
            for sku in order.get("skus", set())
            if sku
        }
        first_delivery_orders = [
            order for order in customer_orders
            if (order.get("delivery_date") or order.get("order_date"))
            == first_delivery_date
        ]
        first_delivery_dfp = sum(
            order["dfp"] for order in first_delivery_orders if order["dfp"] is not None
        )
        if not any(o["dfp"] is not None for o in first_delivery_orders):
            first_delivery_dfp = None
        first_delivery_value = sum(
            order.get("sales", 0) for order in first_delivery_orders
        )

        gaps = []
        median_gap = None
        expected_cycle = None
        if len(delivery_dates) >= 2:
            gaps = [(delivery_dates[i] - delivery_dates[i - 1]).days for i in range(1, len(delivery_dates))]
            median_gap = median(gaps)
            if len(delivery_dates) >= 3:
                cycle_gaps = gaps[-4:] if len(gaps) >= 4 else gaps
                expected_cycle = _clamp(round(median(cycle_gaps)), 14, 75)

        primary_name_key = latest_order["customer_key"]
        feature_key = _identity_group_key(
            normalize_customer_key(latest_order.get("customer_id")),
            normalize_customer_key(latest_order.get("customer_number")),
            primary_name_key,
        )
        features[feature_key] = {
            "customer_key": primary_name_key,
            "customer_id": latest_order.get("customer_id", ""),
            "customer_number": latest_order.get("customer_number", ""),
            "order_identity_count": len(identity_orders),
            "order_count": order_count,
            "delivery_count": len(delivery_dates),
            "total_dfp": _clean_number(total_dfp),
            "total_sales": _clean_number(total_sales),
            "avg_dfp_per_order": _clean_number(avg_dfp),
            "avg_sales_per_order": _clean_number(avg_sales),
            "last_order_date": max(order_dates) if order_dates else None,
            # Expose an order with no confirmed delivery separately from the
            # legacy date-fallback delivery history. Additive v3.x input only.
            "latest_unconfirmed_order_date": max(
                (item["order_date"] for item in customer_orders
                 if not item.get("confirmed_delivery") and item.get("order_date")),
                default=None,
            ),
            "last_delivery_date": latest_order.get("delivery_date") if customer_orders else None,
            "latest_order_reference": latest_order.get("reference", ""),
            "delivery_dates": delivery_dates,
            "delivery_dfp_by_date": delivery_dfp_by_date,
            "confirmed_delivery_dfp_by_date": confirmed_delivery_dfp_by_date,
            "confirmed_delivery_dates": confirmed_delivery_dates,
            "confirmed_delivery_evidence": "dated_commercial_record",
            "first_verified_delivery_date": confirmed_delivery_dates[0] if confirmed_delivery_dates else None,
            "first_verified_order_sku_count": len(first_dated_skus) if first_dated_skus_known else None,
            "first_verified_order_sku_known": first_dated_skus_known,
            "delivery_gaps": gaps,
            "latest_order_dfp": _clean_number(latest_dfp),
            "latest_order_value": _clean_number(latest_sales),
            "first_order_sku_count": len(first_delivery_skus),
            "first_delivery_dfp": _clean_number(first_delivery_dfp),
            "first_delivery_value": _clean_number(first_delivery_value),
            "expected_order_dfp": _clean_number(
                (_weighted_recent_average(latest_dfp, avg_dfp) if latest_dfp is not None
                 else avg_dfp) if customer_orders else 0
            ),
            "expected_order_value": _clean_number(_weighted_recent_average(latest_sales, avg_sales)),
            "median_reorder_gap_days": _clean_number(median_gap),
            "expected_cycle_days": expected_cycle,
            "expected_next_order_date": (
                latest_order.get("delivery_date") + timedelta(days=expected_cycle)
                if latest_order.get("delivery_date") and expected_cycle
                else None
            ),
            "overdue_days": None,
        }

    # Preserve the historic name-key contract where it is unambiguous. Strong
    # identity keys remain in use for collisions, so equal names never merge.
    name_counts = defaultdict(int)
    for feature in features.values():
        name_counts[feature["customer_key"]] += 1
    return FeatureSnapshot({
        (feature["customer_key"] if name_counts[feature["customer_key"]] == 1 else key): feature
        for key, feature in features.items()
    }, source_rows=source_rows, as_of=snapshot.isoformat(), canonical=customers is not None)


def is_human_contact(row: dict) -> bool:
    """Use the same CRM-email exclusion as contact feature calculation."""
    return not (
        bool(str(row.get("email_id") or "").strip())
        or normalize_customer_key(row.get("activity_source")) == "crm_email"
    )


def build_contact_features(sales_activities: list[dict], order_features: dict, *, customers=None, as_of=None) -> dict:
    activities_by_customer = defaultdict(list)
    contact_count_30d = defaultdict(int)
    snapshot = _snapshot_time(as_of)
    source_rows = list(sales_activities)
    today = snapshot.date()
    sales_activities = _canonicalize_rows(sales_activities, customers, name_field="customer", number_field="customer_number")
    inferred_by_number, _inferred_by_name = _inferred_ids(
        sales_activities, name_field="customer", number_field="customer_number"
    )

    for idx, row in enumerate(sales_activities):
        contact_dt = _local_datetime(row.get("date_time"))
        if (not contact_dt or _event_timestamp(row.get("date_time")) > snapshot.timestamp()
                or not _known_at(row, snapshot) or row.get("_identity_unresolved")):
            continue

        customer_id, customer_number, customer_key = _identity_parts(
            row, name_field="customer", number_field="customer_number"
        )
        customer_id = customer_id or inferred_by_number.get(customer_number, "")
        if not customer_key and not customer_id and not customer_number:
            continue

        identity_key = _identity_group_key(customer_id, customer_number, customer_key)

        if 0 <= (today - contact_dt.date()).days <= 30:
            contact_count_30d[identity_key] += 1

        activities_by_customer[identity_key].append(
            {
                "sort_key": (_event_timestamp(row.get("date_time")), idx),
                "datetime": contact_dt,
                "row": row,
                "is_email": not is_human_contact(row),
                "follow_up_date": parse_date(row.get("follow_up_date")),
                "customer_id": customer_id,
                "customer_number": str(row.get("customer_number") or "").strip(),
                "customer_key": customer_key,
            }
        )

    features = {}
    order_indices = _feature_identity_indices(order_features)
    for identity_key, activities in activities_by_customer.items():
        activities.sort(key=lambda activity: activity["sort_key"])
        latest_activity = activities[-1]
        human_activities = [activity for activity in activities if not activity["is_email"]
                            and str(activity["row"].get("result") or "").strip().casefold() != "kontakt tillåten"]
        latest_human = human_activities[-1] if human_activities else None
        reached = [a for a in human_activities if normalize_contact_result(a["row"].get("result")) in REACHED_RESULTS]
        last_reached = reached[-1] if reached else None
        unanswered_dates = [a["datetime"].date() for a in human_activities if normalize_contact_result(a["row"].get("result")) == "Ej anträffbar"]
        opt_out_rows = [
            a for a in human_activities
            if str(a["row"].get("result") or "").strip().casefold() in DO_NOT_CONTACT_RESULTS
        ]
        permission_rows = [a for a in activities if not a["is_email"] and (
            str(a["row"].get("result") or "").strip().casefold() in DO_NOT_CONTACT_RESULTS
            or str(a["row"].get("result") or "").strip().casefold() == "kontakt tillåten")]
        latest_permission = permission_rows[-1] if permission_rows else None
        permission_denied = bool(latest_permission and str(latest_permission["row"].get("result") or "").strip().casefold() in DO_NOT_CONTACT_RESULTS)
        reported_orders = [a for a in human_activities if normalize_contact_result(a["row"].get("result")) == "Order lagd"]
        reached_by_purpose = {}
        completed_by_purpose = {}
        for activity in reached:
            purpose = str(activity["row"].get("purpose") or activity["row"].get("action_type") or activity["row"].get("completed_action_type") or "").strip()
            if purpose:
                reached_by_purpose[purpose] = activity["datetime"]
                if (str(activity["row"].get("obligation_outcome") or "").casefold() == "resolved"
                        or _is_truthy(activity["row"].get("onboarding_completed"))
                        or activity["row"].get("completed_action_type")
                        and str(activity["row"].get("obligation_outcome") or "").casefold() not in {"open", "needs_review", "cancelled"}):
                    completed_by_purpose[purpose] = activity["datetime"]
        # Additional v3.1 read fields; the existing planning-resolution field
        # is deliberately unchanged until the planned phase-2 migration.
        last_reached_sort_key = last_reached["sort_key"] if last_reached else None
        unanswered_sequence_dates = [
            a["datetime"].date() for a in human_activities
            if normalize_contact_result(a["row"].get("result")) == "Ej anträffbar"
            and (last_reached_sort_key is None or a["sort_key"] > last_reached_sort_key)
        ]
        planned_followups = [
            activity
            for activity in human_activities
            if activity["follow_up_date"]
        ]
        latest_plan = planned_followups[-1] if planned_followups else None
        follow_up_date = latest_plan["follow_up_date"] if latest_plan else None

        contact_resolved_followup = bool(
            latest_plan
            and any(
                activity["sort_key"] > latest_plan["sort_key"]
                and activity["datetime"].date() >= follow_up_date
                for activity in activities
            )
        )
        source_plan_id = (str(latest_plan["row"].get("contact_id") or "").strip()
            or _stable_legacy_contact_id(latest_plan["sort_key"][1] + 2,
                latest_plan.get("customer_key"), latest_plan["row"].get("date_time"))) if latest_plan else ""
        reached_resolved_followup = bool(
            latest_plan and (str(latest_plan["row"].get("follow_up_status") or "").casefold() in {"resolved", "cancelled"} or any(
                a["sort_key"] > latest_plan["sort_key"]
                and a["datetime"].date() >= follow_up_date
                and normalize_contact_result(a["row"].get("result")) in REACHED_RESULTS
                and source_plan_id
                and str(a["row"].get("source_contact_id") or "") == source_plan_id
                and bool(a["row"].get("resolves_obligation") or a["row"].get("follow_up_resolved"))
                for a in human_activities
            ))
        )
        latest_identity = latest_activity
        latest_order_date = _identity_feature(
            {
                "customer_id": latest_identity.get("customer_id"),
                "customer_number": latest_identity.get("customer_number"),
                "customer": latest_identity.get("customer_key"),
            },
            order_indices,
            name_field="customer",
            number_field="customer_number",
            allow_name_after_strong_miss=False,
        ).get("last_order_date")
        order_resolved_followup = bool(
            follow_up_date
            and latest_order_date
            and latest_order_date >= follow_up_date
        )

        latest_human_row = latest_human["row"] if latest_human else {}
        result = str(latest_human_row.get("result") or "").replace("\xa0", " ").strip()
        comment = str(latest_human_row.get("comment") or "").replace("\xa0", " ").strip()
        latest_activity_row = latest_activity["row"]
        features[identity_key] = {
            "customer_id": latest_activity.get("customer_id", ""),
            "customer_number": latest_activity.get("customer_number", ""),
            "customer_key": latest_activity.get("customer_key", ""),
            "latest_contact_date": latest_activity["datetime"].date(),
            "latest_contact_datetime": latest_activity["datetime"],
            "latest_human_contact_datetime": (
                latest_human["datetime"] if latest_human else None
            ),
            "latest_human_contact_date": (
                latest_human["datetime"].date() if latest_human else None
            ),
            "latest_reached_date": last_reached["datetime"].date() if last_reached else None,
            "latest_reached_datetime": last_reached["datetime"] if last_reached else None,
            "latest_reached_event_order": last_reached["sort_key"][1] if last_reached else None,
            "latest_reached_by_purpose": reached_by_purpose,
            "completed_by_purpose": completed_by_purpose,
            "contact_event_facts": {
                event_id: {
                        "occurred_at": a["datetime"], "event_order": a["sort_key"][1],
                        "purpose": a["row"].get("purpose") or a["row"].get("action_type") or "",
                } for a in human_activities for event_id in {
                    str(a["row"].get("contact_id") or _stable_legacy_contact_id(
                        a["sort_key"][1] + 2, a.get("customer_key"), a["row"].get("date_time"))),
                    stable_contact_source_id(a["row"])}
            },
            "dialogue_purpose_unknown": any(not (a["row"].get("purpose") or a["row"].get("action_type") or a["row"].get("completed_action_type")) for a in reached),
            "latest_reported_order_date": reported_orders[-1]["datetime"].date() if reported_orders else None,
            "unanswered_attempt_dates": unanswered_dates,
            "unanswered_sequence_dates": unanswered_sequence_dates,
            "last_opt_out_date": opt_out_rows[-1]["datetime"].date() if permission_denied else None,
            "last_opt_out_datetime": opt_out_rows[-1]["datetime"] if opt_out_rows else None,
            "contact_permission_status": "do_not_contact" if permission_denied else "allowed" if latest_permission else "",
            "contact_permission_datetime": latest_permission["datetime"] if latest_permission else None,
            "contact_permission_event_id": str(latest_permission["row"].get("contact_id") or "") if latest_permission else "",
            "latest_reached_result": (
                normalize_contact_result(last_reached["row"].get("result"))
                if last_reached else ""
            ),
            "follow_up_reached_resolved": reached_resolved_followup,
            "latest_follow_up_created_date": (
                latest_plan["datetime"].date() if latest_plan else None
            ),
            "latest_follow_up_created_datetime": latest_plan["datetime"] if latest_plan else None,
            "latest_follow_up_event_order": latest_plan["sort_key"][1] if latest_plan else None,
            "latest_follow_up_source_contact_id": source_plan_id,
            "latest_human_contact_id": str(
                latest_human_row.get("contact_id") or ""
            ).strip() or (
                _stable_legacy_contact_id(
                    latest_human["sort_key"][1] + 2,
                    latest_human.get("customer_key"),
                    latest_human_row.get("date_time"),
                ) if latest_human else ""
            ),
            "latest_contact_sales_person": str(
                latest_activity_row.get("sales_person") or ""
            ).strip(),
            "latest_contact_channel": str(
                latest_activity_row.get("contact_channel") or ""
            ).strip(),
            "latest_contact_result": result,
            "latest_contact_class": normalize_contact_result(result) if result else "",
            "latest_contact_comment": comment,
            "latest_contact_person": latest_human_row.get("customer_contact_person", ""),
            "latest_freezer_fields": _freezer_fields(latest_human_row),
            "latest_follow_up_date": follow_up_date,
            "follow_up_resolved": contact_resolved_followup or order_resolved_followup,
            "contact_count_30d": contact_count_30d.get(identity_key, 0),
            "has_order_after_latest_contact": bool(
                latest_order_date
                and latest_order_date >= latest_activity["datetime"].date()
            ),
            "self_ordering_signal": _has_self_ordering_signal(comment),
            "reached_self_ordering_signal": bool(last_reached and normalize_contact_result(last_reached["row"].get("result")) in {"Positiv", "Neutral"}
                                                 and _has_self_ordering_signal(str(last_reached["row"].get("comment") or ""))),
            "days_since_contact": None,
            "follow_up_due": False,
        }

    name_counts = defaultdict(int)
    for feature in features.values():
        name_counts[feature["customer_key"]] += 1
    return FeatureSnapshot({
        (feature["customer_key"] if name_counts[feature["customer_key"]] == 1 else key): feature
        for key, feature in features.items()
    }, source_rows=source_rows, as_of=snapshot.isoformat(), canonical=customers is not None)


def expected_reorder_cycle(delivery_dates, segment_median=28) -> int | None:
    dates = sorted({parsed for value in delivery_dates if (parsed := parse_date(value))})
    if len(dates) < 2:
        return None
    gaps = [(dates[index] - dates[index - 1]).days for index in range(1, len(dates))]
    if len(dates) == 2:
        observed = gaps[0]
        cycle = (0.5 * observed) + (0.5 * float(segment_median or 28))
    else:
        cycle_gaps = gaps[-4:] if len(gaps) >= 4 else gaps
        cycle = median(cycle_gaps)
    return int(_clamp(round(cycle), 14, 75))


def _planned_activity_index(planned_activities):
    """Index open activities without collapsing future and overdue state."""
    result = defaultdict(list)
    for row in planned_activities or ():
        if str(row.get("status") or "planned").strip().casefold() != "planned":
            continue
        scheduled_at = _local_datetime(row.get("scheduled_at"))
        if not scheduled_at:
            continue
        customer_id = str(row.get("customer_id") or "").strip()
        keys = {f"id:{customer_id}"} if customer_id else set()
        if not customer_id:
            customer_key = normalize_customer_key(row.get("customer"))
            if customer_key:
                keys.add(f"name:{customer_key}")
        for key in keys:
            result[key].append({"scheduled_at": scheduled_at, "row": row})
    for matches in result.values():
        matches.sort(key=lambda item: (
            item["scheduled_at"].replace(tzinfo=None),
            str(item["row"].get("planned_activity_id") or ""),
        ))
    return result


def _planned_activity_state(
    index, *, customer_id, customer_key, sales_person, latest_human_contact, now,
    owner_user_name=None,
):
    """Return the effective future plan and unresolved overdue commitment."""
    matches = index.get(f"id:{customer_id}", ()) if customer_id else ()
    if not matches and customer_key:
        matches = index.get(f"name:{customer_key}", ())
    owner_key = normalize_customer_key(sales_person)
    current = now.replace(tzinfo=None)
    latest_human = (
        latest_human_contact.replace(tzinfo=None)
        if latest_human_contact else None
    )
    future = []
    overdue = []
    for item in matches:
        if owner_user_name is not None:
            # Production resolves the current customer owner through users once
            # per snapshot. Match the booking's canonical login, never its
            # display name (which may be old or formatted differently).
            canonical_owner = normalize_customer_key(owner_user_name)
            booking_owner = normalize_customer_key(item["row"].get("user_name"))
            if not canonical_owner or booking_owner != canonical_owner:
                continue
        else:
            # Compatibility for pure/legacy callers without a users snapshot.
            # An explicit booking login still takes precedence over display text.
            row_owner = normalize_customer_key(
                item["row"].get("user_name") or item["row"].get("sales_person")
            )
            if row_owner and owner_key and row_owner != owner_key:
                continue
        scheduled = item["scheduled_at"].replace(tzinfo=None)
        if scheduled.date() >= current.date():
            future.append(item)
        else:
            # Completion belongs to the persisted activity/obligation, never
            # inferred from an unrelated later (possibly unanswered) contact.
            overdue.append(item)
    return {
        "future": future[0] if future else None,
        "overdue": overdue[0] if overdue else None,
        "future_all": future,
        "overdue_all": overdue,
    }


def customer_lifecycle(order_count, days_since_delivery, overdue_days):
    if order_count <= 0:
        return "prospect"
    if order_count == 1:
        return "reactivation" if (days_since_delivery or 0) > 90 else "first_order"
    if overdue_days is not None and overdue_days > 90:
        return "reactivation"
    return "established"


def decision_context_lifecycle(delivery_count):
    """Return the lifecycle implied by business history, without wall-clock aging."""
    count = int(delivery_count or 0)
    if count <= 0:
        return "prospect"
    if count == 1:
        return "first_order"
    return "established"


FIRST_ORDER_HIGH_VALUE_INDEX = 70

EMAIL_INTENT_STATUS = {
    "stockfiller_clicked_no_order": {
        "trigger": "stockfiller_click_followup",
        "event_field": "email_stockfiller_first_clicked_at",
        "engagement_label": "Stockfiller-klick",
        "modifier": 8,
    },
    "product_sheet_clicked_no_order": {
        "trigger": "product_sheet_click_followup",
        "event_field": "email_product_sheet_first_clicked_at",
        "engagement_label": "produktbladsklick",
        "modifier": 4,
    },
}


def _stable_legacy_contact_id(row_index, customer_key, date_time_value):
    identity = ":".join(
        ["legacy-contact", str(row_index), str(customer_key or "").strip(),
         str(date_time_value or "").strip()]
    )
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"polarbar-planning:{identity}"))


def _active_email_intent(
    email_feature, latest_human_contact, last_order_date, *, blocked=False, today=None, as_of=None
):
    status = str(email_feature.get("email_followup_status") or "").strip()
    intent = EMAIL_INTENT_STATUS.get(status)
    if not intent:
        return {}
    first_event = _local_datetime(email_feature.get(intent["event_field"]))
    if not first_event:
        return {}
    if as_of is not None and _event_timestamp(first_event) > _snapshot_time(as_of).timestamp():
        return {}
    age = ((today or date.today()) - first_event.date()).days
    if age < 0:
        return {}
    if latest_human_contact and latest_human_contact > first_event:
        return {}
    if last_order_date and last_order_date >= first_event.date():
        return {}
    email_id = str(email_feature.get("email_followup_email_id") or "").strip()
    if not email_id:
        return {}
    trigger = intent["trigger"]
    event_id = ":".join((
        email_id, trigger, first_event.isoformat(timespec="seconds")
    ))
    wait_days = max(0, int(_parse_number(
        email_feature.get("email_followup_wait_days_remaining")
    )))
    wait_days = max(wait_days, 3 - age)
    ready = not blocked and wait_days <= 0 and age <= 14
    proposal_label = str(
        email_feature.get("email_followup_proposal_label") or "Påminnelse"
    ).strip()
    engagement_label = intent["engagement_label"]
    reason = (
        f"{engagement_label[0].upper() + engagement_label[1:]} för {age} dagar sedan"
        f" · {proposal_label} · ingen senare order"
    )
    return {
        "trigger": trigger if ready else "",
        "event_id": event_id,
        "modifier": intent["modifier"] if ready else 0,
        "reason": reason,
        "first_event_at": first_event,
        "ready": ready,
        "waiting": bool(not blocked and wait_days > 0 and age <= 14),
        "wait_days": wait_days,
    }


def _guidance_date(value, today=None):
    """Format a known date, retaining the year when it would be ambiguous."""
    months = ("jan", "feb", "mar", "apr", "maj", "jun", "jul", "aug", "sep", "okt", "nov", "dec")
    text = f"{value.day} {months[value.month - 1]}"
    return f"{text} {value.year}" if today is None or value.year != today.year else text


# Action labels are presentation only; primary triggers still decide priority.
TRIGGER_ACTIONS = {
    "first_order_onboarding": ("check_placement", "Kontrollera placering och start"),
    "first_order_reorder": ("secure_repeat_purchase", "Säkra återköpet"),
    "established_reorder_due": ("secure_next_order", "Säkra nästa order"),
    "stockfiller_click_followup": ("follow_up_order_interest", "Följ upp beställningsintresset"),
    "product_sheet_click_followup": ("follow_up_product_interest", "Följ upp produktintresset"),
    "positive_dialogue_followup": ("follow_up_dialogue", "Följ upp dialogen"),
    "repeat_reactivation_due": ("reactivate_customer", "Återaktivera kunden"),
    "single_order_reactivation_due": ("reactivate_customer", "Återaktivera kunden"),
    "strategic_contact_due": ("contact_customer", "Ta ny kontakt"),
    "legacy_missed_followup": ("reschedule_followup", "Planera om uppföljningen"),
    "unanswered_retry_due": ("retry_unanswered", "Försök nå kunden igen"),
    "unanswered_verify_contact_due": ("verify_contact_details", "Kontrollera kontaktväg"),
}


FOCUS_LABELS = {
    "reactivation": "Återaktivering",
    "repeat_purchase": "Återköp",
    "second_purchase": "Andra köpet",
    "a_prospect": "A-prospekt",
    "warm_opportunity": "Varm möjlighet",
    "other": "Övrigt",
}

STATUS_LABELS = {
    "overdue_followup": "Missad uppföljning",
    "act_now": "Agera nu",
    "planned": "Planerad",
    "wait": "Avvakta",
    "idle": "Ingen åtgärd nu",
    "data_missing": "Data saknas",
}


def _activity_contact_type(row):
    value = normalize_customer_key((row or {}).get("contact_type"))
    return {
        "telefon": "phone", "phone": "phone",
        "mejl": "email", "email": "email",
        "besok": "visit", "besök": "visit", "visit": "visit",
    }.get(value, value or "")


def _contact_type_label(value):
    return {
        "phone": "Telefon", "email": "Mejl", "visit": "Besök",
    }.get(str(value or "").strip().casefold(), "Kontakt")


def _focus_key(
    *, lifecycle, segment, order_count, delivery_count, has_future_delivery,
    positive_signal, qualified_email_signal,
):
    if lifecycle == "reactivation" and not has_future_delivery:
        return "reactivation"
    if delivery_count >= 2:
        return "repeat_purchase"
    if delivery_count == 1 or order_count > 0:
        return "second_purchase"
    if segment == "A" and order_count == 0:
        return "a_prospect"
    if order_count == 0 and (positive_signal or qualified_email_signal):
        return "warm_opportunity"
    return "other"


def build_priority_customers(
    customers: list[dict],
    order_features: dict,
    contact_features: dict,
    responsible: str | None,
    today: date,
    limit: int = 30,
    email_features: dict | None = None,
    planned_activities=(),
    workflow_suppressions: dict | None = None,
    scoring_version: str = SCORE_VERSION,
    now: datetime | None = None,
    planning_owner_user_names: dict[str, str] | None = None,
    obligation_states: dict | None = None,
    contact_permissions: dict | None = None,
    as_of: datetime | None = None,
    allow_commercial_fallback: bool = True,
) -> list[dict]:
    """Build the authoritative current score for every active, in-scope customer."""
    snapshot = _snapshot_time(as_of or now, today=today)
    today = snapshot.date()
    # Replay the actual feature pipeline when a caller supplied features made
    # without the master or at another time. This prevents split name-only
    # history and future facts even for backwards-compatible entry points.
    if isinstance(order_features, FeatureSnapshot) and (
            not order_features.canonical or order_features.as_of != snapshot.isoformat()):
        order_features = build_order_features(order_features.source_rows, customers=customers, as_of=snapshot)
    if isinstance(contact_features, FeatureSnapshot) and (
            not contact_features.canonical or contact_features.as_of != snapshot.isoformat()):
        contact_features = build_contact_features(contact_features.source_rows, order_features, customers=customers, as_of=snapshot)
    planned_activities = [row for row in _canonicalize_rows(
        planned_activities, customers, name_field="customer", number_field="customer_number")
        if _known_at(row, snapshot) and not row.get("_identity_unresolved")]
    if scoring_version != SCORE_VERSION:
        raise ValueError("Only the current v3 scoring model is supported")
    # All feature-backed callers use the same history interpretation as Flask.
    # An explicit empty obligation map is authoritative and is never rebuilt.
    if obligation_states is None and isinstance(contact_features, FeatureSnapshot):
        from customer_obligations import build_customer_state
        derived_state = build_customer_state(customers, contact_features.source_rows,
            planned_activities, as_of=snapshot, owner_user_names=planning_owner_user_names,
            order_rows=order_features.source_rows if isinstance(order_features, FeatureSnapshot) else ())
        obligation_states = derived_state["obligations"]
        if contact_permissions is None:
            contact_permissions = derived_state["contact_permissions"]
    email_features = email_features or {}
    workflow_suppressions = workflow_suppressions or {}
    number_index = {
        normalize_customer_key(feature.get("customer_number")): feature
        for feature in order_features.values()
        if normalize_customer_key(feature.get("customer_number"))
    }
    order_indices = _feature_identity_indices(order_features)
    contact_indices = _feature_identity_indices(contact_features)
    email_indices = _feature_identity_indices(email_features)
    master_name_counts = defaultdict(int)
    master_id_counts = defaultdict(int)
    for master_customer in customers:
        master_name = normalize_customer_key(master_customer.get("customer"))
        if master_name:
            master_name_counts[master_name] += 1
        master_id = normalize_customer_key(master_customer.get("customer_id"))
        if master_id:
            master_id_counts[master_id] += 1
    ambiguous_master_names = {
        name for name, count in master_name_counts.items() if count > 1
    }
    benchmarks = _build_priority_benchmarks(
        customers,
        order_features,
        number_index,
        identity_indices=order_indices,
        ambiguous_master_names=ambiguous_master_names,
    )
    if obligation_states is not None:
        closed_activities = {str(item.get("planned_activity_id") or item.get("original_planned_activity_id") or "")
            for items in obligation_states.values() for item in items
            if item.get("status") in {"resolved", "cancelled"} and item.get("resolution_basis")}
        planned_activities = [row for row in (planned_activities or ())
                              if str(row.get("planned_activity_id") or "") not in closed_activities]
    activity_index = _planned_activity_index(planned_activities)
    value_priors = build_value_priors(customers, order_indices, ambiguous_master_names, today)
    guidance_now = snapshot
    represented_source_contact_ids = {
        str(row.get("source_contact_id") or "").strip()
        for row in planned_activities or ()
        if str(row.get("status") or "planned").strip().casefold() == "planned"
        and str(row.get("source_contact_id") or "").strip()
    }
    result = []

    for customer in customers:
        if _is_truthy(customer.get("cancelled_flag")):
            continue
        sales_person = str(customer.get("sales_person") or "").strip()
        if responsible and sales_person != responsible:
            continue
        name = str(customer.get("customer") or "").strip()
        customer_key = normalize_customer_key(name)
        if not customer_key:
            continue

        customer_id = str(customer.get("customer_id") or "").strip()
        customer_number_key = normalize_customer_key(customer.get("customer_number"))
        order = _identity_feature(
            customer,
            order_indices,
            name_field="customer",
            number_field="customer_number",
            ambiguous_master_names=ambiguous_master_names,
        )
        contact = _identity_feature(
            customer,
            contact_indices,
            name_field="customer",
            number_field="customer_number",
            ambiguous_master_names=ambiguous_master_names,
        )
        email_feature = _identity_feature(
            customer,
            email_indices,
            name_field="customer",
            number_field="customer_number",
            ambiguous_master_names=ambiguous_master_names,
        )
        segment = _segment_value(customer)
        defaults = _segment_defaults(benchmarks, segment)
        order_count = int(order.get("order_count") or 0)
        delivery_dates = sorted(order.get("delivery_dates") or ())
        completed_dates = [d for d in delivery_dates if d <= today]
        future_dates = [d for d in delivery_dates if d > today]
        delivery_count = len(completed_dates) if "delivery_dates" in order else int(order.get("delivery_count") or order_count)
        history_index = 100 if delivery_count >= 2 else 60 if delivery_count == 1 else 0
        first_order_sku_count = int(order.get("first_order_sku_count") or 0)
        if "delivery_dates" in order:
            future_delivery = future_dates[0] if future_dates else None
            last_delivery = completed_dates[-1] if completed_dates else None
        else:
            recorded_delivery = order.get("last_delivery_date")
            future_delivery = (
                recorded_delivery
                if recorded_delivery and recorded_delivery > today else None
            )
            last_delivery = (
                recorded_delivery
                if recorded_delivery and recorded_delivery <= today else None
            )
        last_order = order.get("last_order_date")
        days_since_delivery = (today - last_delivery).days if last_delivery else None

        expected_order_dfp = _positive_float(order.get("expected_order_dfp"))
        expected_order_value = _positive_float(order.get("expected_order_value"))
        if not order and not expected_order_dfp:
            expected_order_dfp = _positive_float(defaults.get("expected_order_dfp"))
        if not expected_order_value:
            expected_order_value = _positive_float(defaults.get("expected_order_value"))

        expected_cycle = None
        expected_cycle_source = ""
        if delivery_count >= 2:
            expected_cycle = expected_reorder_cycle(
                completed_dates,
                defaults.get("expected_cycle_days") or 28,
            )
            expected_cycle_source = "blend" if delivery_count == 2 else "customer"
        expected_next = (
            last_delivery + timedelta(days=expected_cycle)
            if last_delivery and expected_cycle else None
        )
        overdue_days = (today - expected_next).days if expected_next else None

        latest_human_date = contact.get("latest_human_contact_date")
        days_since_contact = (
            (today - latest_human_date).days if latest_human_date else None
        )
        latest_contact_class = contact.get("latest_contact_class") or ""
        latest_reached_date = contact.get("latest_reached_date")
        days_since_reached = (today - latest_reached_date).days if latest_reached_date else None
        unanswered_count_30d = sum(1 for d in contact.get("unanswered_attempt_dates", []) if 0 <= (today - d).days <= 30)
        latest_follow_up_date = contact.get("latest_follow_up_date")
        follow_up_resolved = bool(contact.get("follow_up_resolved"))
        has_unresolved_follow_up = bool(
            latest_follow_up_date and not follow_up_resolved
        )
        has_current_explicit_follow_up = bool(
            has_unresolved_follow_up
            and latest_follow_up_date > today
        )
        activity_state = _planned_activity_state(
            activity_index,
            customer_id=customer_id,
            customer_key=customer_key,
            sales_person=sales_person,
            latest_human_contact=contact.get("latest_human_contact_datetime"),
            now=guidance_now,
            owner_user_name=(
                planning_owner_user_names.get(customer_id, "")
                if planning_owner_user_names is not None else None
            ),
        )
        activity = activity_state["future"]
        has_order_after_latest_contact = bool(
            latest_human_date and last_order and last_order >= latest_human_date
        )
        active_email_intent = _active_email_intent(
            email_feature,
            contact.get("latest_human_contact_datetime"),
            last_order,
            blocked=bool(activity or has_current_explicit_follow_up),
            today=today,
            as_of=snapshot,
        )
        business_email_intent = _active_email_intent(
            email_feature,
            contact.get("latest_human_contact_datetime"),
            last_order,
            blocked=False,
            today=today,
            as_of=snapshot,
        )
        lifecycle = customer_lifecycle(delivery_count, days_since_delivery, overdue_days)
        context_lifecycle = decision_context_lifecycle(delivery_count)

        if delivery_count == 1:
            expected_order_dfp = _positive_float(
                order.get("first_delivery_dfp")
            ) or expected_order_dfp
            expected_order_value = _positive_float(
                order.get("first_delivery_value")
            ) or expected_order_value

        p90 = _positive_float(benchmarks.get("expected_order_dfp_p90")) or 1
        value_index = int(_clamp(round((expected_order_dfp / p90) * 100), 0, 100))
        strategic_index = {"A": 100, "B": 65, "C": 25}.get(segment, 15)
        # Only completed, dated commercial deliveries establish buying history.
        verified_delivery_dates = sorted(
            d for d in order.get("confirmed_delivery_dates", ())
            if d <= today
        ) if "confirmed_delivery_dates" in order else completed_dates
        business_delivery_count = len(verified_delivery_dates)
        business_last_delivery = (
            verified_delivery_dates[-1] if verified_delivery_dates else None
        )
        business_days_since_delivery = (
            (today - business_last_delivery).days
            if business_last_delivery else None
        )
        # This v3.x cycle prior must come ONLY from other buyers' confirmed
        # deliveries. Reusing the legacy benchmark inadvertently lets an
        # undelivered order change a customer's estimated 90-day business value.
        business_cycle_prior = value_priors.get(
            f"cycle:{segment}", value_priors.get("cycle:", 45)
        )
        business_expected_cycle = (
            expected_reorder_cycle(verified_delivery_dates, business_cycle_prior)
            if business_delivery_count >= 2 else business_cycle_prior
        )
        business_overdue_days = (
            (today - (business_last_delivery
                      + timedelta(days=business_expected_cycle))).days
            if business_last_delivery and business_delivery_count >= 2
            else None
        )
        # An unconfirmed order AFTER the last verified delivery is not a
        # missed reorder, even if our older delivery-date fallback says so.
        pending_unconfirmed_order_date = order.get("latest_unconfirmed_order_date")
        if pending_unconfirmed_order_date and (
            pending_unconfirmed_order_date > today
            or (business_last_delivery
                and pending_unconfirmed_order_date < business_last_delivery)
        ):
            pending_unconfirmed_order_date = None
        store_fmt = store_format(name)
        has_store_format_prior = (
            bool(store_fmt) and f"{segment}|{store_fmt}" in value_priors
            and value_priors[f"{segment}|{store_fmt}"] !=
                value_priors.get(segment, value_priors.get("", 9))
        )
        business_prior = value_priors.get(
            f"{segment}|{store_fmt}",
            value_priors.get(segment, value_priors.get("", 9)),
        )
        business_result = business_opportunity(
            delivery_values=complete_deliveries(order, today),
            prior_dfp=business_prior,
            delivery_count=business_delivery_count,
            cycle_days=business_expected_cycle,
            days_since_delivery=business_days_since_delivery,
        )
        business_result["business_prior_dfp"] = round(business_prior, 2)
        business_result["business_prior_basis"] = (
            "segment_and_format" if has_store_format_prior else "segment"
        )

        workflow_reason = workflow_suppressions.get(customer_id)
        decision_context = {
            "status_key": "idle",
            "reason_code": "recent_email_engagement_wait" if business_email_intent.get("waiting") else "",
        }
        latest_reached_result = contact.get("latest_reached_result") or ""
        positive_dialogue_without_order = bool(
            latest_reached_result == "Positiv" and latest_reached_date is not None
            and (not last_order or last_order < latest_reached_date))
        positive_prospect_signal = bool(business_delivery_count == 0 and order_count == 0 and positive_dialogue_without_order)
        positive_buyer_signal = bool(business_delivery_count >= 1 and positive_dialogue_without_order)
        negative_dialogue_signal = bool(latest_reached_result == "Negativ" and latest_reached_date is not None
            and (not last_order or last_order <= latest_reached_date))
        business_lifecycle = customer_lifecycle(business_delivery_count, business_days_since_delivery, business_overdue_days)
        materially_overdue = bool(business_delivery_count >= 2 and business_lifecycle != "reactivation"
            and business_overdue_days is not None and business_overdue_days >= int(VALUE_POLICY["material_reorder_overdue_days"])
            and (days_since_reached is None or days_since_reached >= int(VALUE_POLICY["material_reorder_min_dialogue_days"])))
        permission_status = contact.get("contact_permission_status") or ("do_not_contact" if contact.get("last_opt_out_date") else "")
        permission_event = contact.get("contact_permission_event_id") or ""
        permission_at = contact.get("contact_permission_datetime")
        permission = (contact_permissions or {}).get(customer_id) or {}
        explicit_permission_at = _local_datetime(permission.get("occurred_at"))
        if permission and explicit_permission_at and _event_timestamp(explicit_permission_at) <= snapshot.timestamp() and (
                permission_at is None or _event_timestamp(explicit_permission_at) > _event_timestamp(permission_at)
                or _event_timestamp(explicit_permission_at) == _event_timestamp(permission_at) and permission_status != "do_not_contact"):
            permission_status = str(permission.get("status") or permission_status)
            permission_at = explicit_permission_at
            permission_event = str(permission.get("event_id") or "")
        last_opt_out_date = ((permission_at or contact.get("last_opt_out_datetime") or snapshot).date()
                            if permission_status == "do_not_contact" else None)
        obligations = None if obligation_states is None else []
        for obligation in ((obligation_states or {}).get(customer_id) or []):
            if not _known_at(obligation, snapshot):
                continue
            item = dict(obligation)
            source = (contact.get("contact_event_facts") or {}).get(str(item.get("source_contact_id") or "")) or {}
            if not source:
                agreed = _local_datetime(item.get("agreed_at") or item.get("source_created_at") or item.get("created_at"))
                matched = {fact["event_order"]: fact for fact in (contact.get("contact_event_facts") or {}).values()
                           if agreed is not None and fact.get("occurred_at") == agreed}
                if len(matched) == 1:
                    source = next(iter(matched.values()))
            if source:
                item.setdefault("created_at", source.get("occurred_at"))
                item.setdefault("event_order", source.get("event_order"))
            obligations.append(item)
        first_verified_sku_count = order.get("first_verified_order_sku_count") if "first_verified_order_sku_count" in order else first_order_sku_count or None
        final_plan = activity_state.get("future") or activity_state.get("overdue")
        decision_arguments = dict(
            context=decision_context, today=today, business_value_raw=business_result["business_value_raw"],
            latest_attempt_date=latest_human_date, latest_attempt_result=latest_contact_class,
            latest_attempt_at=contact.get("latest_human_contact_datetime"),
            latest_reached_date=latest_reached_date, latest_reached_result=latest_reached_result,
            latest_reached_at=contact.get("latest_reached_datetime"),
            latest_reached_event_order=contact.get("latest_reached_event_order"),
            last_opt_out_date=last_opt_out_date, latest_follow_up_date=latest_follow_up_date,
            follow_up_reached_resolved=bool(contact.get("follow_up_reached_resolved")),
            unanswered_sequence_count=sum(1 for attempt_date in contact.get("unanswered_sequence_dates", ()) if 0 <= (today - attempt_date).days <= 30),
            last_order_date=last_order, delivery_count=business_delivery_count,
            days_since_delivery=business_days_since_delivery, overdue_days=business_overdue_days,
            expected_cycle_days=business_expected_cycle,
            follow_up_has_later_order=bool(latest_follow_up_date and last_order and last_order >= latest_follow_up_date),
            follow_up_created_date=contact.get("latest_follow_up_created_date"),
            follow_up_created_at=contact.get("latest_follow_up_created_datetime"),
            follow_up_event_order=contact.get("latest_follow_up_event_order"),
            source_contact_id=contact.get("latest_follow_up_source_contact_id") or "",
            unverified_order_without_delivery=bool(order_count > 0 and business_delivery_count == 0),
            positive_prospect_signal=positive_prospect_signal, positive_buyer_signal=positive_buyer_signal,
            pending_unconfirmed_order_date=pending_unconfirmed_order_date,
            reported_order_date=contact.get("latest_reported_order_date"),
            latest_verified_delivery_date=business_last_delivery,
            future_delivery=future_delivery,
            qualified_email_trigger=business_email_intent.get("trigger", "") if business_email_intent.get("ready") else "",
            qualified_email_reason=business_email_intent.get("reason", ""),
            first_order_sku_count=first_verified_sku_count, onboarding_value_index=value_index, segment=segment,
            first_delivery_date=order.get("first_verified_delivery_date"),
            reached_by_purpose=contact.get("latest_reached_by_purpose"),
            completed_by_purpose=contact.get("completed_by_purpose"),
            obligations=obligations, planned_activity=final_plan, workflow_suppression=workflow_reason,
            allow_commercial_fallback=allow_commercial_fallback,
            planned_activity_options=activity_state.get("future_all", []) + activity_state.get("overdue_all", []),
            self_ordering_signal=bool(contact.get("reached_self_ordering_signal", contact.get("self_ordering_signal"))),
            invalid_identity=not customer_id or not sales_person or (
                planning_owner_user_names is not None and not planning_owner_user_names.get(customer_id))
                or master_id_counts[normalize_customer_key(customer_id)] != 1,
        )
        list_guidance = store_contact_guidance(**decision_arguments)
        score_arguments = dict(
            business_value_raw=business_result["business_value_raw"], days_since_reached=days_since_reached,
            latest_attempt_date=latest_human_date, today=today, latest_result=latest_contact_class,
            positive_prospect_signal=positive_prospect_signal, positive_buyer_signal=positive_buyer_signal,
            negative_dialogue_signal=negative_dialogue_signal, materially_overdue=materially_overdue,
            days_since_delivery=business_days_since_delivery, first_order_sku_count=first_verified_sku_count,
        )
        contact_result = contact_opportunity(**score_arguments, decision=list_guidance,
            status=list_guidance["status_key"], trigger=list_guidance.get("reason_code", ""))
        work_decision = (store_contact_guidance(**decision_arguments, ignore_planning=True)
                         if list_guidance.get("status_key") == "planned" else list_guidance)
        work_result = contact_opportunity(**score_arguments, decision=work_decision,
            status=work_decision["status_key"], trigger=work_decision.get("reason_code", ""))
        contact_result.update(work_priority_raw=work_result["contact_priority_raw"],
            work_priority_score=work_result["contact_priority_score"],
            work_priority_precise=work_result["contact_priority_precise"],
            work_contact_factor=work_result["contact_relevance_factor"],
            work_can_start_unscheduled_contact=bool(work_decision.get("can_start_unscheduled_contact")),
            work_priority_reason_code=work_decision.get("reason_code", ""))
        final_trigger = list_guidance["reason_code"]
        channel = recommend_contact_channel(
            lifecycle=business_lifecycle, overdue_days=business_overdue_days, trigger_key=final_trigger,
            segment=segment, has_human_contact=bool(latest_human_date), phone=customer.get("phone"),
            email_available=bool(customer.get("email") or customer.get("email_last_order")),
            visible=bool(list_guidance["can_start_unscheduled_contact"] or list_guidance["status_key"] == "planned"),
        ) or {}
        selected_plan = next((item for item in decision_arguments["planned_activity_options"]
            if str(item["row"].get("planned_activity_id") or "") == list_guidance.get("planned_activity_id")), None)
        if list_guidance["status_key"] == "planned" or list_guidance.get("can_execute_planned_contact"):
            list_guidance["recommended_contact_type"] = _activity_contact_type(selected_plan["row"]) if selected_plan else ""
        else:
            list_guidance["recommended_contact_type"] = channel.get("recommended_contact_type", "")
        list_guidance["channel_reason_code"] = channel.get("channel_reason_code", "")
        positive_window = positive_dialogue_without_order and days_since_reached is not None and days_since_reached <= 45
        focus_key = _focus_key(lifecycle=business_lifecycle, segment=segment, order_count=order_count,
            delivery_count=business_delivery_count, has_future_delivery=bool(future_delivery),
            positive_signal=positive_window, qualified_email_signal=bool(business_email_intent.get("ready") or business_email_intent.get("waiting")))
        focus_label = ("En leverans, 11–90 dagar sedan"
                       if focus_key == "second_purchase" and business_delivery_count == 1
                       and business_days_since_delivery is not None and 11 <= business_days_since_delivery <= 90
                       else FOCUS_LABELS[focus_key])
        list_guidance.update(focus_key=focus_key, focus_label=focus_label,
            guidance_version="2.0", as_of=snapshot.isoformat(), model_version=VALUE_MODEL_VERSION,
            contact_permission_status=permission_status or "not_recorded",
            contact_permission_event_id=permission_event,
            evidence_quality="legacy_purpose_unknown" if contact.get("dialogue_purpose_unknown") and not list_guidance.get("obligation_id") else list_guidance.get("evidence_quality"))
        # Older API field names project ONLY this same v3 decision and score.
        # Stored historical values are never a fallback for live decisions.
        customer_guidance = dict(list_guidance)
        recommendation_eligible = bool(list_guidance["can_start_unscheduled_contact"])
        suppression = "" if recommendation_eligible else final_trigger
        status_text = list_guidance["reason_text"]
        reason_code, reason_text = final_trigger, list_guidance["reason_text"]
        next_action = _decision_next_action(list_guidance, segment=segment)
        priority_score = work_result["contact_priority_score"]
        intent_timing = round(100 * work_result["contact_relevance_factor"])
        priority_type = final_trigger
        # Compatibility fields are descriptions, not a second decision engine.
        suppression_source_type = "planned_activity" if list_guidance.get("planned_activity_id") else ""
        suppression_source_id = list_guidance.get("planned_activity_id") or ""
        active_obligations = [item for item in (obligations or ()) if item.get("status") not in {"resolved", "cancelled"}]
        follow_up_due = any(parse_date(item.get("due_date")) and parse_date(item["due_date"]) <= today
                            for item in active_obligations)
        future_follow_up_days = ((latest_follow_up_date - today).days
                                 if latest_follow_up_date and latest_follow_up_date > today and not follow_up_resolved else None)
        result.append({
            "value_model_version": VALUE_MODEL_VERSION,
            "policy_version": VALUE_POLICY_VERSION,
            "as_of": snapshot.isoformat(),
            **business_result, **contact_result,
            "list_guidance": list_guidance,
            **{field: list_guidance.get(field) for field in (
                "action_type", "action_scope", "reason_codes", "can_contact_now",
                "can_start_unscheduled_contact", "can_execute_planned_contact", "contact_not_before",
                "planned_activity_id", "has_planned_contact", "obligation_ids", "obligation_id",
                "obligation_revision", "source_contact_id", "requires_handling", "review_only", "requires_review", "review_obligation_ids",
                "recommended_contact_type", "contact_permission_status", "contact_permission_event_id",
                "evidence_quality")},
            "owner_user_name": (planning_owner_user_names or {}).get(customer_id, sales_person),
            "business_verified_delivery_count": business_delivery_count,
            "business_latest_verified_delivery_date": _iso_date(business_last_delivery),
            "business_latest_verified_delivery_dfp": (order.get("confirmed_delivery_dfp_by_date") or {}).get(_iso_date(business_last_delivery)) if business_last_delivery else None,
            "business_delivery_evidence": "dated_commercial_record",
            "business_first_order_sku_count": first_verified_sku_count,
            "business_expected_cycle_days": business_expected_cycle,
            "business_cycle_prior_days": business_cycle_prior,
            "business_days_since_delivery": business_days_since_delivery,
            "business_overdue_days": business_overdue_days,
            "latest_unconfirmed_order_date": _iso_date(pending_unconfirmed_order_date),
            "latest_reported_order_date": _iso_date(contact.get("latest_reported_order_date")),
            "data_warnings": list_guidance.get("data_warnings", []),
            "latest_reached_date": _iso_date(latest_reached_date),
            "days_since_reached": days_since_reached,
            "row": customer.get("row"),
            "customer_id": customer_id,
            "customer": name,
            "customer_number": str(customer.get("customer_number") or "").strip(),
            "sales_person": sales_person,
            "segment": segment,
            "lifecycle": business_lifecycle,
            "decision_context_lifecycle": decision_context_lifecycle(business_delivery_count),
            "score_version": SCORE_VERSION,
            "ranking_mode": "v3",
            "priority_score_precise": work_result["contact_priority_precise"],
            "priority_score": priority_score,
            "intent_timing": intent_timing,
            "history_index": history_index,
            "value_index": value_index,
            "strategic_index": strategic_index,
            "recommendation_eligible": recommendation_eligible,
            "recommendation_suppression_reason": suppression,
            "recommendation_suppression_source_type": suppression_source_type,
            "recommendation_suppression_source_id": suppression_source_id,
            "primary_reason_code": reason_code,
            "primary_reason_text": reason_text,
            "primary_trigger_type": final_trigger if recommendation_eligible else "",
            "primary_trigger_key": final_trigger if recommendation_eligible else "scoring_context",
            "covered_trigger_keys": [final_trigger] if recommendation_eligible else [],
            "active_email_intent_event": active_email_intent.get("event_id", ""),
            "decision_context_email_event": (
                "" if customer_guidance["focus_key"] == "a_prospect"
                else active_email_intent.get("event_id", "")
            ),
            "email_intent_trigger": active_email_intent.get("trigger", ""),
            "planning_status_text": status_text,
            "priority_level": _priority_level(priority_score),
            "priority_type": priority_type,
            "recommended_action": list_guidance["action_label"],
            "recommended_channel": _decision_channel_label(list_guidance["recommended_contact_type"]),
            "next_action": next_action,
            "customer_guidance": customer_guidance,
            "reasons": [status_text or reason_text],
            "order_count": order_count,
            "delivery_count": delivery_count,
            "first_order_sku_count": first_order_sku_count,
            "total_dfp": _clean_number(order.get("total_dfp") or 0),
            "expected_order_dfp": (
                None if order and order.get("expected_order_dfp") is None
                else _clean_number(expected_order_dfp)
            ),
            "expected_order_value": _clean_number(expected_order_value),
            "latest_order_dfp": order.get("latest_order_dfp"),
            "first_delivery_dfp": order.get("first_delivery_dfp"),
            "latest_order_volume_missing": order.get("latest_order_dfp") is None,
            "latest_order_reference": str(order.get("latest_order_reference") or ""),
            "latest_order_date": _iso_date(last_order),
            "latest_delivery_date": _iso_date(last_delivery),
            "latest_delivery_dfp": (order.get("delivery_dfp_by_date") or {}).get(
                _iso_date(last_delivery)
            ) if last_delivery else None,
            "next_delivery_dfp": (order.get("delivery_dfp_by_date") or {}).get(
                _iso_date(future_delivery)
            ) if future_delivery else None,
            "next_delivery_date": (
                _iso_date(future_delivery)
                if future_delivery and future_delivery > today else ""
            ),
            "days_since_delivery": days_since_delivery,
            "expected_cycle_days": expected_cycle,
            "expected_cycle_source": expected_cycle_source,
            "expected_next_order_date": _iso_date(expected_next),
            "overdue_days": overdue_days,
            "latest_contact_date": _iso_date(contact.get("latest_contact_date")),
            "latest_human_contact_date": _iso_date(latest_human_date),
            "latest_human_contact_id": contact.get("latest_human_contact_id", ""),
            "latest_contact_result": contact.get("latest_contact_result", ""),
            "latest_contact_comment": contact.get("latest_contact_comment", ""),
            "latest_contact_person": contact.get("latest_contact_person", ""),
            "latest_contact_class": latest_contact_class,
            "latest_contact_channel": contact.get("latest_contact_channel", ""),
            "latest_contact_sales_person": contact.get("latest_contact_sales_person", ""),
            "latest_follow_up_date": _iso_date(latest_follow_up_date),
            "future_follow_up_days": future_follow_up_days,
            "latest_freezer_fields": list(contact.get("latest_freezer_fields") or []),
            "follow_up_due": follow_up_due,
            "missad_uppfoljning": bool(
                follow_up_due and latest_follow_up_date and latest_follow_up_date < today
            ),
            "has_order_after_latest_contact": has_order_after_latest_contact,
            "self_ordering_signal": bool(contact.get("self_ordering_signal")),
            "email_priority_status": email_feature.get(
                "email_followup_status", ""
            ),
        })

    result.sort(key=lambda item: (
        -item["work_priority_precise"], -item["business_value_raw"],
        normalize_customer_key(item.get("customer")), item.get("customer_id") or "",
    ))
    return result[:limit]


def apply_workflow_suppressions(priority_customers, suppressions):
    """Project a matched workflow pause through every canonical decision field.

    Generic suggestion state never erases a commitment or replaces a safety
    reason. Scoped obligation rescheduling belongs to the obligation reducer.
    """
    result = []
    for customer in priority_customers:
        updated = dict(customer)
        state = (suppressions or {}).get(str(customer.get("customer_id") or "")) or ""
        reason = str(state.get("reason") or state.get("status") or "") if isinstance(state, dict) else str(state)
        scope = str(state.get("source_scope") or state.get("action_scope") or "") if isinstance(state, dict) else ""
        guidance = dict(updated.get("list_guidance") or updated.get("customer_guidance") or {})
        current_scope = str(guidance.get("action_scope") or "")
        protected = (guidance.get("reason_code") in {"do_not_contact", "negative_contact_cooldown", "invalid_customer_identity",
            "future_delivery", "reported_order_sync_wait", "reported_order_sync_review", "recent_order_pending_confirmation",
            "stale_unconfirmed_order_review", "order_without_verified_delivery", "unanswered_retry_wait"}
            or guidance.get("status_key") == "data_missing" or guidance.get("review_only"))
        commitment = current_scope.startswith(("obligation:", "planned_activity:")) or guidance.get("status_key") in {"planned", "overdue_followup"}
        if not reason or reason == "suggestion_planned" or protected or commitment and (not scope or scope != current_scope):
            result.append(updated)
            continue
        guidance.update(status_key="wait", status_label="Avvakta", action_type=reason,
            action_key="resume_suggestion" if reason == "dismissed" else "wait",
            action_label="Återuppta förslag" if reason == "dismissed" else "Avvakta",
            reason_code=reason, reason_codes=[reason],
            reason_text="Förslaget är snoozat" if reason == "snoozed" else "Tidigare förslag dolt",
            can_contact_now=False, can_start_unscheduled_contact=False, can_execute_planned_contact=False,
            recommended_contact_type="", channel_reason_code="")
        updated.update(list_guidance=dict(guidance), customer_guidance=dict(guidance),
            recommendation_eligible=False, recommendation_suppression_reason=reason,
            contact_priority_score=0, contact_priority_raw=0.0, contact_relevance_factor=0.0, contact_factor=0.0,
            work_priority_raw=0.0, work_priority_score=0, work_priority_precise=0.0, work_contact_factor=0.0,
            priority_score=0, priority_score_precise=0.0, contact_priority_precise=0.0,
            work_can_start_unscheduled_contact=False,
            contact_priority_bucket=3, primary_trigger_type="", primary_trigger_key="scoring_context", covered_trigger_keys=[],
            primary_reason_code=reason, primary_reason_text=guidance["reason_text"], planning_status_text=guidance["reason_text"],
            recommended_action=guidance["action_label"], recommended_channel="avvakta",
            next_action=_decision_next_action(guidance, segment=updated.get("segment")))
        for field in ("action_type", "reason_codes", "can_contact_now", "can_start_unscheduled_contact",
                      "can_execute_planned_contact", "recommended_contact_type"):
            updated[field] = guidance[field]
        result.append(updated)
    return result


def _priority_level(score: int) -> str:
    if score >= 80:
        return "Hög prio"
    if score >= 50:
        return "Medel prio"
    return "Låg prio"


def _decision_channel_label(contact_type: str | None) -> str:
    """Retain the legacy display field without using it as an action type."""
    return {"phone": "telefon", "email": "email", "visit": "besök"}.get(contact_type, "avvakta")


def _decision_next_action(guidance: dict, *, segment="") -> dict:
    """Project the final decision to legacy action semantics, never a new rule.

    Older consumers filter by action_type and render label. Contact channel
    stays separate, and an old consumer can still see that waiting/planned
    actions do not authorize an extra outbound contact.
    """
    code = str(guidance.get("reason_code") or "")
    status = guidance.get("status_key")
    allowed = bool(guidance.get("can_start_unscheduled_contact"))
    if guidance.get("review_only") or status == "data_missing":
        action_type = "review"
    elif code == "do_not_contact":
        action_type = "do_not_contact"
    elif status == "planned":
        action_type = "scheduled_followup"
    elif not allowed:
        action_type = "pause" if status == "wait" else "monitor"
    else:
        action_type = {
            "first_order_onboarding": "first_order_onboarding",
            "second_purchase_due": "trial_reorder",
            "established_reorder_due": "reorder",
            "positive_dialogue_followup": "warm_lead",
            "positive_buyer_followup": "warm_lead",
            "unanswered_retry_due": "retry",
            "unanswered_verify_contact_due": "retry",
            "overdue_explicit_follow_up": "follow_up",
            "overdue_planned_activity": "follow_up",
            "follow_up_due_today": "follow_up",
            "single_order_reactivation_due": "trial_reorder",
            "repeat_reactivation_due": "reactivation",
            "relationship_care_due": "route_fill",
            "value_based_contact_due": "new_ab" if str(segment).upper() in {"A", "B"} else "route_fill",
        }.get(code, code or "none")
    channel = str(guidance.get("recommended_contact_type") or "")
    label = str(guidance.get("action_label") or "")
    return {"action_type": action_type, "action_key": guidance.get("action_key", ""),
            "label": label, "action_label": label,
            "reason": guidance.get("reason_text", ""), "reason_code": code,
            "contact_type": channel, "can_contact_now": allowed,
            "can_start_unscheduled_contact": allowed,
            "can_execute_planned_contact": bool(guidance.get("can_execute_planned_contact")),
            "tone": "urgent" if status == "overdue_followup" and allowed else "neutral" if allowed else "low",
            "primary_cta": ({"phone": "Ring", "email": "Mejla", "visit": "Planera besök"}.get(channel, "Öppna")
                            if allowed else "Öppna")}


def _freezer_fields(row: dict) -> tuple[str, ...]:
    selected = tuple(field for field in FREEZER_FIELDS if _is_checked_value(row.get(field)))
    real_fields = tuple(field for field in selected if field != "none")
    if real_fields:
        return real_fields
    return ("none",) if "none" in selected else ()


def _has_self_ordering_signal(comment: str) -> bool:
    text = _searchable_text(comment)
    if not text:
        return False

    patterns = (
        r"\b(?:bestaller|lagger|ordrar)\s+(?:de\s+|han\s+|hon\s+)?sjalv\b",
        r"\bsjalv\s+(?:bestaller|lagger|ordrar)\b",
        r"\b(?:bestaller|lagger|ordrar)\s+(?:vid behov|nar det behovs|om det behovs|om de behovs)\b",
        r"\blagger\s+.*\bom det behovs\b",
        r"\border\s+sjalv\b",
    )
    return any(re.search(pattern, text) for pattern in patterns)


def _is_checked_value(value) -> bool:
    return str(value or "").strip().casefold() in {"1", "true", "yes", "on"}


def _searchable_text(value) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or "").replace("\xa0", " "))
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    return " ".join(ascii_text.casefold().split())


def _build_priority_benchmarks(
    customers: list[dict],
    order_features: dict,
    number_index: dict,
    *,
    identity_indices: dict | None = None,
    ambiguous_master_names: set[str] | None = None,
) -> dict:
    global_dfp = []
    global_value = []
    global_cycles = []
    by_segment = defaultdict(lambda: {"dfp": [], "value": [], "cycle": []})

    for customer in customers:
        if _is_truthy(customer.get("cancelled_flag")):
            continue

        customer_key = normalize_customer_key(customer.get("customer"))
        customer_number_key = normalize_customer_key(customer.get("customer_number"))
        if identity_indices is not None:
            feature = _identity_feature(
                customer,
                identity_indices,
                name_field="customer",
                number_field="customer_number",
                ambiguous_master_names=ambiguous_master_names,
            )
        else:
            feature = number_index.get(customer_number_key) or order_features.get(customer_key)
        if not feature:
            continue

        segment = _segment_value(customer)
        dfp = _positive_float(feature.get("expected_order_dfp"))
        value = _positive_float(feature.get("expected_order_value"))
        cycle = _positive_float(feature.get("median_reorder_gap_days"))

        if dfp:
            global_dfp.append(dfp)
            by_segment[segment]["dfp"].append(dfp)
        if value:
            global_value.append(value)
            by_segment[segment]["value"].append(value)
        if cycle:
            global_cycles.append(cycle)
            by_segment[segment]["cycle"].append(cycle)

    global_default = {
        "expected_order_dfp": _median_or(global_dfp, 0),
        "expected_order_value": _median_or(global_value, 0),
        "expected_cycle_days": _cycle_default(global_cycles, 45),
    }
    segment_defaults = {"": global_default}
    for segment, values in by_segment.items():
        segment_defaults[segment] = {
            "expected_order_dfp": _median_or(values["dfp"], global_default["expected_order_dfp"]),
            "expected_order_value": _median_or(values["value"], global_default["expected_order_value"]),
            "expected_cycle_days": _cycle_default(values["cycle"], global_default["expected_cycle_days"]),
        }

    return {
        "expected_order_dfp_p90": _percentile(global_dfp, 0.9) or global_default["expected_order_dfp"] or 1,
        "expected_order_value_p90": _percentile(global_value, 0.9) or global_default["expected_order_value"] or 1,
        "segment_defaults": segment_defaults,
    }


def build_value_priors(customers, order_indices, ambiguous_names, today):
    grouped = defaultdict(list)
    by_format = defaultdict(list)
    by_segment_cycle = defaultdict(list)
    global_values = []
    global_cycles = []
    for customer in customers:
        if _is_truthy(customer.get("cancelled_flag")):
            continue
        feature = _identity_feature(
            customer, order_indices, name_field="customer", number_field="customer_number",
            ambiguous_master_names=ambiguous_names,
        )
        segment = _segment_value(customer)
        volumes = complete_deliveries(feature, today)
        if volumes:
            average = sum(volumes) / len(volumes)
            grouped[segment].append(average)
            fmt = store_format(customer.get("customer"))
            if fmt:
                # One observation per BUYING STORE, never per order row.
                by_format[(segment, fmt)].append(average)
            global_values.append(average)
        # Keep cycle priors completely separate from the legacy model's
        # order-date fallback. Count each buying store only once.
        confirmed_dates = sorted({
            d for d in feature.get("confirmed_delivery_dates", ())
            if d <= today
        })
        if len(confirmed_dates) >= 2:
            gaps = [
                (confirmed_dates[i] - confirmed_dates[i - 1]).days
                for i in range(1, len(confirmed_dates))
                if confirmed_dates[i] > confirmed_dates[i - 1]
            ]
            if gaps:
                typical_gap = _clamp(median(gaps[-4:]), 14, 75)
                by_segment_cycle[segment].append(typical_gap)
                global_cycles.append(typical_gap)
    priors = {segment: segment_prior(grouped[segment], segment, global_values)
              for segment in ("A", "B", "C", "")}
    default_cycle = int(round(_clamp(median(global_cycles), 14, 75))) if global_cycles else 45
    priors["cycle:"] = default_cycle
    for segment in ("A", "B", "C"):
        segment_cycles = by_segment_cycle.get(segment, ())
        priors[f"cycle:{segment}"] = (
            int(round(_clamp(median(segment_cycles), 14, 75)))
            if segment_cycles else default_cycle
        )
    # Small format groups shrink entirely to their segment prior; larger
    # groups can add measured information without a second A/B/C bonus.
    for (segment, fmt), values in by_format.items():
        priors[f"{segment}|{fmt}"] = empirical_format_prior(
            priors.get(segment, priors[""]), values
        )
    return priors


def _segment_defaults(benchmarks: dict, segment: str) -> dict:
    defaults = benchmarks.get("segment_defaults", {})
    return defaults.get(str(segment or "").strip().upper()[:1]) or defaults.get("") or {
        "expected_order_dfp": 0,
        "expected_order_value": 0,
        "expected_cycle_days": 45,
    }


def _order_dfp(row: dict) -> float:
    return order_volume(row)


def _weighted_recent_average(latest, average) -> float:
    return (_positive_float(latest) * 0.65) + (_positive_float(average) * 0.35)


def _format_dfp(value) -> str:
    return f"{_clean_number(_positive_float(value))} DFP"


def _positive_float(value) -> float:
    try:
        number = float(value or 0)
    except (TypeError, ValueError):
        return 0.0
    return number if number > 0 else 0.0


def _median_or(values: list[float], fallback: float) -> float:
    cleaned = [_positive_float(value) for value in values if _positive_float(value)]
    return median(cleaned) if cleaned else fallback


def _percentile(values: list[float], fraction: float) -> float | None:
    cleaned = sorted(_positive_float(value) for value in values if _positive_float(value))
    if not cleaned:
        return None
    if len(cleaned) == 1:
        return cleaned[0]

    position = (len(cleaned) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(cleaned) - 1)
    weight = position - lower
    return (cleaned[lower] * (1 - weight)) + (cleaned[upper] * weight)


def _cycle_default(cycles: list[float], fallback: int) -> int:
    if not cycles:
        return fallback
    return _clamp(round(median(cycles)), 14, 75)


def _segment_value(customer: dict) -> str:
    segment = str(customer.get("customer_segment") or customer.get("segment") or "").strip().upper()
    return segment[:1] if segment else ""


def _segment_rank(segment) -> int:
    return {"A": 4, "B": 3, "C": 2}.get(str(segment or "").strip().upper()[:1], 1)


def _parse_number(value) -> float:
    text = str(value or "").replace("\xa0", " ").strip()
    if not text:
        return 0.0
    cleaned = "".join(ch for ch in text if ch.isdigit() or ch in ",.-")
    if cleaned in {"", "-", ".", ","}:
        return 0.0
    if "," in cleaned and "." in cleaned:
        if cleaned.rfind(",") > cleaned.rfind("."):
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    else:
        cleaned = cleaned.replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _clean_number(value):
    if value is None:
        return None
    number = float(value)
    return int(number) if number.is_integer() else round(number, 1)


def _clamp(value, lower, upper):
    return max(lower, min(upper, value))


def _iso_date(value):
    return value.isoformat() if isinstance(value, date) else ""


def _is_truthy(value) -> bool:
    text = str(value or "").replace("\xa0", " ").strip().casefold()
    return bool(text and text not in {"0", "false", "no", "nej", "off"})


def _looks_like_excel_serial(text: str) -> bool:
    normalized = text.replace(",", ".")
    if not normalized.replace(".", "", 1).isdigit():
        return False
    try:
        number = float(normalized)
    except ValueError:
        return False
    return 20000 <= number <= 80000


def _parse_excel_serial(text: str) -> datetime | None:
    try:
        number = float(text.replace(",", "."))
    except ValueError:
        return None
    base = datetime(1899, 12, 30)
    return base + timedelta(days=number)
