from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, time, timedelta
import re
from statistics import median
import unicodedata
import uuid


FREEZER_FIELDS = ("Franui", "Schufrulade", "Boujee", "polarbar", "none")
OTHER_COMPETITOR_FREEZER_FIELDS = {"Schufrulade", "Boujee"}


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


def build_order_features(order_rows: list[dict]) -> dict:
    orders = {}
    inferred_by_number, _inferred_by_name = _inferred_ids(
        order_rows, name_field="Customer", number_field="Customer number"
    )
    for idx, row in enumerate(order_rows):
        customer = str(row.get("Customer") or "").strip()
        if not customer or "polarbär" in customer.casefold():
            continue

        dfp = _order_dfp(row)
        if dfp <= 0:
            continue

        reference = str(row.get("Reference") or "").strip() or f"row-{idx}"
        order_date = parse_date(row.get("Order date"))
        delivery_date = parse_date(row.get("Delivery date")) or order_date
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
                "skus": set(),
            },
        )
        order["dfp"] += dfp
        order["sales"] += total
        if sku:
            order["skus"].add(normalize_customer_key(sku))
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
        if order.get("sales", 0) <= 0:
            continue
        key = _identity_group_key(
            normalize_customer_key(order.get("customer_id")),
            normalize_customer_key(order.get("customer_number")),
            order["customer_key"],
        )
        orders_by_customer[key].append(order)

    features = {}
    for customer_orders in orders_by_customer.values():
        customer_orders.sort(key=lambda o: o.get("delivery_date") or o.get("order_date") or date.min)
        latest_order = customer_orders[-1]
        order_dates = [o["order_date"] for o in customer_orders if o.get("order_date")]
        delivery_dates = sorted({o["delivery_date"] for o in customer_orders if o.get("delivery_date")})
        total_dfp = sum(o["dfp"] for o in customer_orders)
        total_sales = sum(o["sales"] for o in customer_orders)
        order_count = len(customer_orders)
        avg_dfp = total_dfp / order_count if order_count else 0
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
            order.get("dfp", 0) for order in first_delivery_orders
        )
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
            "order_count": order_count,
            "delivery_count": len(delivery_dates),
            "total_dfp": _clean_number(total_dfp),
            "total_sales": _clean_number(total_sales),
            "avg_dfp_per_order": _clean_number(avg_dfp),
            "avg_sales_per_order": _clean_number(avg_sales),
            "last_order_date": max(order_dates) if order_dates else latest_order.get("delivery_date"),
            "last_delivery_date": latest_order.get("delivery_date"),
            "latest_order_reference": latest_order.get("reference", ""),
            "delivery_dates": delivery_dates,
            "delivery_gaps": gaps,
            "latest_order_dfp": _clean_number(latest_dfp),
            "latest_order_value": _clean_number(latest_sales),
            "first_order_sku_count": len(first_delivery_skus),
            "first_delivery_dfp": _clean_number(first_delivery_dfp),
            "first_delivery_value": _clean_number(first_delivery_value),
            "expected_order_dfp": _clean_number(_weighted_recent_average(latest_dfp, avg_dfp)),
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
    return {
        (feature["customer_key"] if name_counts[feature["customer_key"]] == 1 else key): feature
        for key, feature in features.items()
    }


def build_contact_features(sales_activities: list[dict], order_features: dict) -> dict:
    activities_by_customer = defaultdict(list)
    contact_count_30d = defaultdict(int)
    today = date.today()
    inferred_by_number, _inferred_by_name = _inferred_ids(
        sales_activities, name_field="customer", number_field="customer_number"
    )

    for idx, row in enumerate(sales_activities):
        contact_dt = parse_datetime(row.get("date_time"))
        if not contact_dt:
            continue

        customer_id, customer_number, customer_key = _identity_parts(
            row, name_field="customer", number_field="customer_number"
        )
        customer_id = customer_id or inferred_by_number.get(customer_number, "")
        if not customer_key and not customer_id and not customer_number:
            continue

        identity_key = _identity_group_key(customer_id, customer_number, customer_key)

        if (today - contact_dt.date()).days <= 30:
            contact_count_30d[identity_key] += 1

        activities_by_customer[identity_key].append(
            {
                "sort_key": (contact_dt, idx),
                "datetime": contact_dt,
                "row": row,
                "is_email": bool(str(row.get("email_id") or "").strip()),
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
        human_activities = [activity for activity in activities if not activity["is_email"]]
        latest_human = human_activities[-1] if human_activities else None
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
            "latest_freezer_fields": _freezer_fields(latest_human_row),
            "latest_follow_up_date": follow_up_date,
            "follow_up_resolved": contact_resolved_followup or order_resolved_followup,
            "contact_count_30d": contact_count_30d.get(identity_key, 0),
            "has_order_after_latest_contact": bool(
                latest_order_date
                and latest_order_date >= latest_activity["datetime"].date()
            ),
            "self_ordering_signal": _has_self_ordering_signal(comment),
            "days_since_contact": None,
            "follow_up_due": False,
        }

    name_counts = defaultdict(int)
    for feature in features.values():
        name_counts[feature["customer_key"]] += 1
    return {
        (feature["customer_key"] if name_counts[feature["customer_key"]] == 1 else key): feature
        for key, feature in features.items()
    }


def _build_priority_customers_legacy(
    customers: list[dict],
    order_features: dict,
    contact_features: dict,
    responsible: str | None,
    today: date,
    limit: int = 30,
    email_features: dict | None = None,
) -> list[dict]:
    email_features = email_features or {}
    number_index = {
        normalize_customer_key(feature.get("customer_number")): feature
        for feature in order_features.values()
        if normalize_customer_key(feature.get("customer_number"))
    }
    benchmarks = _build_priority_benchmarks(customers, order_features, number_index)

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

        customer_number_key = normalize_customer_key(customer.get("customer_number"))
        order_feature = number_index.get(customer_number_key) or order_features.get(customer_key) or {}
        contact_feature = contact_features.get(customer_key) or {}
        email_feature = email_features.get(customer_key) or {}
        segment = _segment_value(customer)
        segment_defaults = _segment_defaults(benchmarks, segment)
        enriched_order = dict(order_feature)

        last_delivery = enriched_order.get("last_delivery_date")
        last_order = enriched_order.get("last_order_date")
        order_count = enriched_order.get("order_count", 0)
        total_dfp = enriched_order.get("total_dfp", 0)
        expected_order_dfp = enriched_order.get("expected_order_dfp")
        expected_order_value = enriched_order.get("expected_order_value")

        if order_count:
            expected_order_dfp = expected_order_dfp or enriched_order.get("avg_dfp_per_order") or segment_defaults["expected_order_dfp"]
            expected_order_value = (
                expected_order_value
                or enriched_order.get("avg_sales_per_order")
                or segment_defaults["expected_order_value"]
            )
        else:
            expected_order_dfp = segment_defaults["expected_order_dfp"]
            expected_order_value = segment_defaults["expected_order_value"]

        expected_cycle = enriched_order.get("expected_cycle_days")
        expected_cycle_source = "customer" if expected_cycle else ""
        expected_next = enriched_order.get("expected_next_order_date")
        if last_delivery and not expected_cycle:
            expected_cycle = segment_defaults["expected_cycle_days"]
            expected_cycle_source = "segment"
            expected_next = last_delivery + timedelta(days=expected_cycle) if expected_cycle else None

        overdue_days = (today - expected_next).days if expected_next else None
        days_since_delivery = (today - last_delivery).days if last_delivery else None

        latest_contact_date = contact_feature.get("latest_contact_date")
        latest_human_contact_date = contact_feature.get("latest_human_contact_date")
        contact_signal_date = latest_human_contact_date or latest_contact_date
        days_since_contact = (
            (today - contact_signal_date).days if contact_signal_date else None
        )
        has_order_after_latest_contact = bool(
            latest_contact_date
            and last_order
            and last_order >= latest_contact_date
        )
        latest_follow_up_date = contact_feature.get("latest_follow_up_date")
        follow_up_resolved = bool(contact_feature.get("follow_up_resolved"))
        follow_up_due = bool(
            latest_follow_up_date
            and latest_follow_up_date <= today
            and not follow_up_resolved
        )
        latest_contact_class = contact_feature.get("latest_contact_class")
        future_follow_up_days = _future_follow_up_days(
            latest_contact_date=latest_contact_date,
            latest_follow_up_date=latest_follow_up_date,
            follow_up_due=follow_up_due,
            follow_up_resolved=follow_up_resolved,
            today=today,
        )
        scheduled_followup = future_follow_up_days is not None
        self_ordering_followup = _is_self_ordering_followup(
            latest_contact_class=latest_contact_class,
            latest_contact_date=latest_contact_date,
            days_since_contact=days_since_contact,
            latest_follow_up_date=latest_follow_up_date,
            follow_up_due=follow_up_due,
            follow_up_resolved=follow_up_resolved,
            self_ordering_signal=contact_feature.get("self_ordering_signal"),
            today=today,
        )
        email_signal = _email_priority_signal(
            email_feature=email_feature,
            latest_human_contact=(
                contact_feature.get("latest_human_contact_datetime")
                or contact_feature.get("latest_contact_datetime")
            ),
            today=today,
        )

        score = _priority_score(
            segment=segment,
            order_count=order_count,
            expected_order_dfp=expected_order_dfp,
            expected_order_value=expected_order_value,
            benchmarks=benchmarks,
            overdue_days=overdue_days,
            latest_contact_class=latest_contact_class,
            has_order_after_latest_contact=has_order_after_latest_contact,
            days_since_contact=days_since_contact,
            follow_up_due=follow_up_due,
            latest_contact_date=latest_contact_date,
            last_order_date=last_order,
            last_delivery_date=last_delivery,
            self_ordering_followup=self_ordering_followup,
            future_follow_up_days=future_follow_up_days,
            freezer_fields=contact_feature.get("latest_freezer_fields"),
            email_signal=email_signal,
            today=today,
        )
        priority_type = _priority_type(
            follow_up_due=follow_up_due,
            scheduled_followup=scheduled_followup,
            has_order_after_latest_contact=has_order_after_latest_contact,
            order_count=order_count,
            overdue_days=overdue_days,
            latest_contact_class=latest_contact_class,
            days_since_contact=days_since_contact,
            segment=segment,
            self_ordering_followup=self_ordering_followup,
        )
        priority_level = _priority_level(score)
        next_action = _next_action(
            priority_type=priority_type,
            follow_up_due=follow_up_due,
            scheduled_followup=scheduled_followup,
            future_follow_up_days=future_follow_up_days,
            overdue_days=overdue_days,
            total_dfp=total_dfp,
            expected_order_dfp=expected_order_dfp,
            order_count=order_count,
            latest_contact_class=latest_contact_class,
            has_order_after_latest_contact=has_order_after_latest_contact,
            days_since_contact=days_since_contact,
            latest_contact_date=latest_contact_date,
            last_order_date=last_order,
            segment=segment,
            self_ordering_followup=self_ordering_followup,
            email_signal=email_signal,
            today=today,
        )

        result.append(
            {
                "row": customer.get("row"),
                "customer": name,
                "sales_person": sales_person,
                "segment": segment,
                "priority_score": score,
                "priority_level": priority_level,
                "priority_type": priority_type,
                "recommended_action": _recommended_action(priority_type),
                "order_count": order_count,
                "total_dfp": _clean_number(total_dfp),
                "expected_order_dfp": _clean_number(expected_order_dfp),
                "_expected_order_value_sort": _clean_number(expected_order_value),
                "latest_order_date": _iso_date(last_order),
                "latest_delivery_date": _iso_date(last_delivery),
                "days_since_delivery": days_since_delivery,
                "expected_cycle_days": expected_cycle,
                "expected_cycle_source": expected_cycle_source,
                "expected_next_order_date": _iso_date(expected_next),
                "overdue_days": overdue_days,
                "latest_contact_date": _iso_date(latest_contact_date),
                "latest_contact_result": contact_feature.get("latest_contact_result", ""),
                "latest_contact_comment": contact_feature.get("latest_contact_comment", ""),
                "latest_contact_class": latest_contact_class or "",
                "latest_contact_channel": contact_feature.get("latest_contact_channel", ""),
                "latest_contact_sales_person": contact_feature.get("latest_contact_sales_person", ""),
                "latest_follow_up_date": _iso_date(latest_follow_up_date),
                "future_follow_up_days": future_follow_up_days,
                "latest_freezer_fields": list(contact_feature.get("latest_freezer_fields") or []),
                "follow_up_due": follow_up_due,
                "missad_uppfoljning": bool(
                    follow_up_due
                    and latest_follow_up_date
                    and latest_follow_up_date < today
                ),
                "has_order_after_latest_contact": has_order_after_latest_contact,
                "self_ordering_signal": bool(contact_feature.get("self_ordering_signal")),
                "email_priority_status": email_signal.get("status", ""),
                "email_priority_active": bool(email_signal.get("active")),
                "email_priority_handled": bool(email_signal.get("handled")),
                "next_action": next_action,
                "recommended_channel": _recommended_channel(
                    next_action.get("action_type")
                ),
                "reasons": _priority_reasons(
                    follow_up_due=follow_up_due,
                    future_follow_up_days=future_follow_up_days,
                    has_order_after_latest_contact=has_order_after_latest_contact,
                    overdue_days=overdue_days,
                    latest_contact_class=latest_contact_class,
                    days_since_contact=days_since_contact,
                    total_dfp=enriched_order.get("total_dfp", 0),
                    expected_order_dfp=expected_order_dfp,
                    order_count=enriched_order.get("order_count", 0),
                    segment=segment,
                    latest_contact_date=latest_contact_date,
                    self_ordering_followup=self_ordering_followup,
                    freezer_fields=contact_feature.get("latest_freezer_fields"),
                    email_signal=email_signal,
                ),
            }
        )

    result.sort(
        key=lambda c: (
            c["priority_score"],
            c.get("_expected_order_value_sort") or 0,
            c.get("expected_order_dfp") or 0,
            _segment_rank(c.get("segment")),
            c.get("overdue_days") if c.get("overdue_days") is not None else -999,
            c.get("total_dfp") or 0,
        ),
        reverse=True,
    )
    for customer in result:
        customer.pop("_expected_order_value_sort", None)
    return result[:limit]


def calculate_priority_score_v2(intent_timing, value_index, strategic_index) -> int:
    score = round(
        (0.65 * float(intent_timing or 0))
        + (0.30 * float(value_index or 0))
        + (0.05 * float(strategic_index or 0))
    )
    return int(_clamp(score, 0, 100))


def established_intent_timing(overdue_days: int) -> int:
    if overdue_days < -14:
        return 10
    if overdue_days < 0:
        return 25
    if overdue_days <= 7:
        return 65
    if overdue_days <= 30:
        return 90
    if overdue_days <= 60:
        return 80
    if overdue_days <= 90:
        return 60
    return 60


def prospect_reactivation_intent_timing(days_since_contact: int | None) -> int:
    if days_since_contact is None:
        return 60
    if days_since_contact <= 7:
        return 15
    if days_since_contact <= 21:
        return 25
    if days_since_contact <= 45:
        return 45
    return 60


def first_order_intent_timing(days_since_delivery: int | None) -> int:
    if days_since_delivery is None or days_since_delivery <= 6:
        return 10
    if days_since_delivery <= 10:
        return 35
    if days_since_delivery <= 23:
        return 20
    if days_since_delivery <= 30:
        return 75
    if days_since_delivery <= 45:
        return 90
    if days_since_delivery <= 60:
        return 80
    if days_since_delivery <= 90:
        return 60
    return 60


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


def _future_activity_index(planned_activities, today):
    result = {}
    for row in planned_activities or ():
        if str(row.get("status") or "planned").strip().casefold() != "planned":
            continue
        if str(row.get("source_suggestion_id") or "").strip():
            continue
        scheduled = parse_date(row.get("scheduled_at"))
        if not scheduled or scheduled < today:
            continue
        keys = {
            str(row.get("customer_id") or "").strip(),
            normalize_customer_key(row.get("customer")),
        } - {""}
        for key in keys:
            current = result.get(key)
            if current is None or scheduled < current["date"]:
                result[key] = {"date": scheduled, "row": row}
    return result


def _v2_lifecycle(order_count, days_since_delivery, overdue_days):
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


def _stable_legacy_contact_id(row_index, customer_key, date_time_value):
    identity = ":".join(
        ["legacy-contact", str(row_index), str(customer_key or "").strip(),
         str(date_time_value or "").strip()]
    )
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"polarbar-planning:{identity}"))


def _active_email_intent(
    email_feature, latest_human_contact, last_order_date, *, blocked=False
):
    status = str(email_feature.get("email_followup_status") or "").strip()
    if status == "stockfiller_clicked_no_order":
        trigger = "stockfiller_click_followup"
        first_click = parse_datetime(
            email_feature.get("email_stockfiller_first_clicked_at")
        )
        modifier = 8
        reason = "Följ upp Stockfiller-klick"
    elif status == "product_sheet_clicked_no_order":
        trigger = "product_sheet_click_followup"
        first_click = parse_datetime(
            email_feature.get("email_product_sheet_first_clicked_at")
        )
        modifier = 4
        reason = "Följ upp produktbladsklick"
    else:
        return {}
    if not first_click:
        return {}
    if latest_human_contact and latest_human_contact > first_click:
        return {}
    if last_order_date and last_order_date >= first_click.date():
        return {}
    email_id = str(email_feature.get("email_followup_email_id") or "").strip()
    if not email_id:
        return {}
    event_id = ":".join((email_id, trigger, first_click.isoformat(timespec="seconds")))
    ready = not blocked and int(_parse_number(
        email_feature.get("email_followup_wait_days_remaining")
    )) <= 0
    return {
        "trigger": trigger if ready else "",
        "event_id": event_id,
        "modifier": modifier if ready else 0,
        "reason": reason,
        "first_clicked_at": first_click,
        "ready": ready,
    }


def _phase3_trigger_snapshot(
    *, lifecycle, delivery_count, days_since_delivery, segment,
    first_order_sku_count, value_index, overdue_days, latest_contact_class,
    days_since_contact, has_order_after_latest_contact,
    has_current_explicit_follow_up, priority_score,
    active_email_intent=None, legacy_missed_followup=False,
):
    triggers = []
    reasons = {}

    if lifecycle == "established" and overdue_days is not None and overdue_days >= 0:
        triggers.append("established_reorder_due")
        reasons["established_reorder_due"] = _v2_primary_reason(
            lifecycle, overdue_days, days_since_contact
        )

    if lifecycle == "first_order" and delivery_count == 1:
        days = int(days_since_delivery or 0)
        onboarding_risk = (
            segment in {"A", "B"}
            or first_order_sku_count == 1
            or value_index >= FIRST_ORDER_HIGH_VALUE_INDEX
        )
        if 7 <= days <= 10 and onboarding_risk:
            triggers.append("first_order_onboarding")
            reason = (
                "Första ordern hade endast 1 SKU"
                if first_order_sku_count == 1
                else "Onboarding efter första leverans"
            )
            reasons["first_order_onboarding"] = (
                "first_order_onboarding", reason
            )
        elif 24 <= days <= 90:
            triggers.append("first_order_reorder")
            reasons["first_order_reorder"] = (
                "first_order_reorder", "Dags att följa upp andra ordern"
            )

    positive_dialogue = (
        latest_contact_class == "Positiv"
        and not has_order_after_latest_contact
        and not has_current_explicit_follow_up
        and days_since_contact is not None
        and days_since_contact >= (7 if delivery_count == 0 else 3)
    )
    if positive_dialogue:
        triggers.append("positive_dialogue_followup")
        reasons["positive_dialogue_followup"] = (
            "positive_dialogue_followup", "Följ upp positiv dialog"
        )

    strategic_due = (
        lifecycle in {"prospect", "reactivation"}
        and (segment == "A" or priority_score >= 70)
        and (days_since_contact is None or days_since_contact > 45)
    )
    if strategic_due:
        triggers.append("strategic_contact_due")
        strategic_reason = (
            "Strategisk kund – aldrig kontaktad"
            if days_since_contact is None
            else f"Strategisk kund – {days_since_contact} dagar sedan senaste kontakt"
        )
        reasons["strategic_contact_due"] = (
            "strategic_contact_due", strategic_reason
        )

    email_trigger = str((active_email_intent or {}).get("trigger") or "")
    if email_trigger:
        triggers.append(email_trigger)
        reasons[email_trigger] = (
            email_trigger, str((active_email_intent or {}).get("reason") or "")
        )

    if legacy_missed_followup:
        triggers.append("legacy_missed_followup")
        reasons["legacy_missed_followup"] = (
            "legacy_missed_followup", "Missad uppföljning utan senare aktivitet"
        )

    precedence = (
        "established_reorder_due",
        "first_order_onboarding",
        "first_order_reorder",
        "positive_dialogue_followup",
        "strategic_contact_due",
        "stockfiller_click_followup",
        "product_sheet_click_followup",
        "legacy_missed_followup",
    )
    primary = next((key for key in precedence if key in triggers), "")
    reason_code, reason_text = reasons.get(primary, ("", ""))
    return {
        "primary_trigger_type": primary,
        "primary_trigger_key": primary,
        "covered_trigger_keys": triggers,
        "primary_reason_code": reason_code,
        "primary_reason_text": reason_text,
    }


def _v2_primary_reason(lifecycle, overdue_days, days_since_contact):
    if lifecycle == "established":
        if overdue_days is None or overdue_days < 0:
            remaining = abs(int(overdue_days or 0))
            return "established_reorder_expected", (
                f"Återorder väntas om {remaining} dagar"
                if remaining else "Återorder väntas snart"
            )
        if overdue_days <= 7:
            return "established_reorder_due", "Återorder väntas nu"
        if overdue_days <= 60:
            return "established_reorder_due", f"Försenad återorder +{overdue_days} dagar"
        return "established_reorder_due", f"Återorder kraftigt försenad +{overdue_days} dagar"
    if lifecycle == "first_order":
        return "first_order_timing", "Följ upp första leveransen"
    if lifecycle == "reactivation":
        if days_since_contact is None:
            return "reactivation_timing", "Reaktiveringskund – aldrig kontaktad"
        return "reactivation_timing", f"Reaktiveringskund – {days_since_contact} dagar sedan kontakt"
    if days_since_contact is None:
        return "prospect_timing", "Prospekt – aldrig kontaktat"
    return "prospect_timing", f"Prospekt – {days_since_contact} dagar sedan kontakt"


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
    scoring_version: str = "v2",
) -> list[dict]:
    """Build the authoritative v2 score for every active, in-scope customer."""
    if str(scoring_version or "").strip().casefold() == "legacy":
        return _build_priority_customers_legacy(
            customers,
            order_features,
            contact_features,
            responsible,
            today,
            limit=limit,
            email_features=email_features,
        )
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
    for master_customer in customers:
        master_name = normalize_customer_key(master_customer.get("customer"))
        if master_name:
            master_name_counts[master_name] += 1
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
    future_activities = _future_activity_index(planned_activities, today)
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
        delivery_count = int(order.get("delivery_count") or order_count)
        first_order_sku_count = int(order.get("first_order_sku_count") or 0)
        last_delivery = order.get("last_delivery_date")
        last_order = order.get("last_order_date")
        days_since_delivery = (today - last_delivery).days if last_delivery else None

        expected_order_dfp = _positive_float(order.get("expected_order_dfp"))
        expected_order_value = _positive_float(order.get("expected_order_value"))
        if not expected_order_dfp:
            expected_order_dfp = _positive_float(defaults.get("expected_order_dfp"))
        if not expected_order_value:
            expected_order_value = _positive_float(defaults.get("expected_order_value"))

        expected_cycle = None
        expected_cycle_source = ""
        if delivery_count >= 2:
            expected_cycle = expected_reorder_cycle(
                order.get("delivery_dates") or (),
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
        latest_follow_up_date = contact.get("latest_follow_up_date")
        follow_up_resolved = bool(contact.get("follow_up_resolved"))
        has_unresolved_follow_up = bool(
            latest_follow_up_date and not follow_up_resolved
        )
        has_current_explicit_follow_up = bool(
            has_unresolved_follow_up
            and latest_follow_up_date >= today
        )
        activity = future_activities.get(customer_id) or future_activities.get(
            customer_key
        )
        has_order_after_latest_contact = bool(
            latest_human_date and last_order and last_order >= latest_human_date
        )
        active_email_intent = _active_email_intent(
            email_feature,
            contact.get("latest_human_contact_datetime"),
            last_order,
            blocked=bool(activity or has_current_explicit_follow_up),
        )
        lifecycle = _v2_lifecycle(delivery_count, days_since_delivery, overdue_days)
        context_lifecycle = decision_context_lifecycle(delivery_count)

        if delivery_count == 1:
            expected_order_dfp = _positive_float(
                order.get("first_delivery_dfp")
            ) or expected_order_dfp
            expected_order_value = _positive_float(
                order.get("first_delivery_value")
            ) or expected_order_value

        if lifecycle == "established":
            intent_timing = established_intent_timing(int(overdue_days or 0))
        elif lifecycle == "first_order":
            intent_timing = first_order_intent_timing(days_since_delivery)
        else:
            intent_timing = prospect_reactivation_intent_timing(days_since_contact)

        if (
            not has_order_after_latest_contact
            and not has_current_explicit_follow_up
            and not activity
        ):
            if (
                latest_contact_class == "Positiv"
                and days_since_contact is not None
                and days_since_contact >= (7 if lifecycle == "prospect" else 3)
            ):
                intent_timing += 10 if lifecycle == "prospect" else 20
            elif (
                latest_contact_class == "Ej anträffbar"
                and days_since_contact is not None
                and days_since_contact >= 3
            ):
                intent_timing += 5
        if (
            lifecycle == "first_order"
            and first_order_sku_count == 1
            and days_since_delivery is not None
            and 7 <= days_since_delivery <= 60
        ):
            intent_timing += 8
        intent_timing += int(active_email_intent.get("modifier") or 0)
        intent_timing = int(_clamp(intent_timing, 0, 100))

        p90 = _positive_float(benchmarks.get("expected_order_dfp_p90")) or 1
        value_index = int(_clamp(round((expected_order_dfp / p90) * 100), 0, 100))
        strategic_index = {"A": 100, "B": 65, "C": 25}.get(segment, 15)
        priority_score = calculate_priority_score_v2(
            intent_timing, value_index, strategic_index
        )

        future_follow_up_days = _future_follow_up_days(
            latest_contact_date=contact.get("latest_contact_date"),
            latest_follow_up_date=latest_follow_up_date,
            follow_up_due=False,
            follow_up_resolved=follow_up_resolved,
            today=today,
        )
        suppression = ""
        status_text = ""
        if not sales_person or not customer_id:
            suppression = "invalid_or_inactive_owner"
            status_text = "Saknar giltig ägare eller kundidentitet"
        elif last_delivery and last_delivery > today:
            suppression = "future_delivery"
            status_text = "Framtida leverans registrerad"
        elif activity:
            suppression = "future_planned_activity"
            activity_type = str(activity["row"].get("contact_type") or "Aktivitet").strip()
            status_text = f"{activity_type.capitalize()} planerad {activity['date'].isoformat()}"
        elif has_current_explicit_follow_up:
            suppression = "explicit_follow_up"
            status_text = f"Uppföljning beslutad {latest_follow_up_date.isoformat()}"
        elif future_follow_up_days is not None:
            suppression = "future_planned_activity"
            status_text = f"Uppföljning planerad om {future_follow_up_days} dagar"
        elif days_since_contact is not None and 0 <= days_since_contact <= 2:
            suppression = "recent_human_contact"
            status_text = "Nyligen kontaktad"
        elif (
            latest_contact_class == "Negativ"
            and not has_order_after_latest_contact
            and days_since_contact is not None
            and 0 <= days_since_contact < 60
        ):
            suppression = "negative_contact_cooldown"
            status_text = "Negativ kontakt – rekommendation pausad"
        workflow_reason = workflow_suppressions.get(customer_id)
        if workflow_reason:
            suppression = str(workflow_reason)
            status_text = {
                "snoozed": "Rekommendation snoozad",
                "dismissed": "Samma beslutsunderlag markerat Ej relevant",
            }.get(suppression, status_text or suppression)

        recommendation_eligible = not suppression
        legacy_missed_followup = bool(
            has_unresolved_follow_up
            and latest_follow_up_date < today
            and not has_order_after_latest_contact
            and str(contact.get("latest_human_contact_id") or "").strip()
            not in represented_source_contact_ids
        )
        trigger_snapshot = _phase3_trigger_snapshot(
            lifecycle=lifecycle,
            delivery_count=delivery_count,
            days_since_delivery=days_since_delivery,
            segment=segment,
            first_order_sku_count=first_order_sku_count,
            value_index=value_index,
            overdue_days=overdue_days,
            latest_contact_class=latest_contact_class,
            days_since_contact=days_since_contact,
            has_order_after_latest_contact=has_order_after_latest_contact,
            has_current_explicit_follow_up=has_current_explicit_follow_up,
            priority_score=priority_score,
            active_email_intent=active_email_intent,
            legacy_missed_followup=legacy_missed_followup,
        )
        reason_code = trigger_snapshot["primary_reason_code"]
        reason_text = trigger_snapshot["primary_reason_text"]
        if not reason_text:
            reason_code, reason_text = _v2_primary_reason(
                lifecycle, overdue_days, days_since_contact
            )

        follow_up_due = bool(
            latest_follow_up_date
            and latest_follow_up_date <= today
            and not follow_up_resolved
        )
        scheduled_followup = future_follow_up_days is not None
        self_ordering_followup = _is_self_ordering_followup(
            latest_contact_class=latest_contact_class,
            latest_contact_date=contact.get("latest_contact_date"),
            days_since_contact=days_since_contact,
            latest_follow_up_date=latest_follow_up_date,
            follow_up_due=follow_up_due,
            follow_up_resolved=follow_up_resolved,
            self_ordering_signal=contact.get("self_ordering_signal"),
            today=today,
        )
        email_signal = _email_priority_signal(
            email_feature=email_feature,
            latest_human_contact=contact.get("latest_human_contact_datetime"),
            today=today,
        )
        priority_type = _priority_type(
            follow_up_due=follow_up_due,
            scheduled_followup=scheduled_followup,
            has_order_after_latest_contact=has_order_after_latest_contact,
            order_count=order_count,
            overdue_days=overdue_days,
            latest_contact_class=latest_contact_class,
            days_since_contact=days_since_contact,
            segment=segment,
            self_ordering_followup=self_ordering_followup,
        )
        next_action = _next_action(
            priority_type=priority_type,
            follow_up_due=follow_up_due,
            scheduled_followup=scheduled_followup,
            future_follow_up_days=future_follow_up_days,
            overdue_days=overdue_days,
            total_dfp=order.get("total_dfp", 0),
            expected_order_dfp=expected_order_dfp,
            order_count=order_count,
            latest_contact_class=latest_contact_class,
            has_order_after_latest_contact=has_order_after_latest_contact,
            days_since_contact=days_since_contact,
            latest_contact_date=contact.get("latest_contact_date"),
            last_order_date=last_order,
            segment=segment,
            self_ordering_followup=self_ordering_followup,
            email_signal=email_signal,
            today=today,
        )
        result.append({
            "row": customer.get("row"),
            "customer_id": customer_id,
            "customer": name,
            "customer_number": str(customer.get("customer_number") or "").strip(),
            "sales_person": sales_person,
            "segment": segment,
            "lifecycle": lifecycle,
            "decision_context_lifecycle": context_lifecycle,
            "score_version": "v2",
            "priority_score": priority_score,
            "intent_timing": intent_timing,
            "value_index": value_index,
            "strategic_index": strategic_index,
            "recommendation_eligible": recommendation_eligible,
            "recommendation_suppression_reason": suppression,
            "primary_reason_code": reason_code,
            "primary_reason_text": reason_text,
            "primary_trigger_type": trigger_snapshot["primary_trigger_type"],
            "primary_trigger_key": trigger_snapshot["primary_trigger_key"],
            "covered_trigger_keys": trigger_snapshot["covered_trigger_keys"],
            "active_email_intent_event": active_email_intent.get("event_id", ""),
            "email_intent_trigger": active_email_intent.get("trigger", ""),
            "planning_status_text": status_text,
            "priority_level": _priority_level(priority_score),
            "priority_type": priority_type,
            "recommended_action": _recommended_action(priority_type),
            "recommended_channel": _recommended_channel(next_action.get("action_type")),
            "next_action": next_action,
            "reasons": [status_text or reason_text],
            "order_count": order_count,
            "delivery_count": delivery_count,
            "first_order_sku_count": first_order_sku_count,
            "total_dfp": _clean_number(order.get("total_dfp") or 0),
            "expected_order_dfp": _clean_number(expected_order_dfp),
            "expected_order_value": _clean_number(expected_order_value),
            "latest_order_reference": str(order.get("latest_order_reference") or ""),
            "latest_order_date": _iso_date(last_order),
            "latest_delivery_date": _iso_date(last_delivery),
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
        -int(item.get("priority_score") or 0),
        -float(item.get("expected_order_dfp") or 0),
        item.get("row") if isinstance(item.get("row"), int) else 10**9,
        item.get("customer_id") or normalize_customer_key(item.get("customer")),
    ))
    return result[:limit]


def apply_workflow_suppressions(priority_customers, suppressions):
    """Overlay exact suggestion-state suppression without changing score/visibility."""
    result = []
    for customer in priority_customers:
        updated = dict(customer)
        reason = str(
            (suppressions or {}).get(str(customer.get("customer_id") or ""), "")
            or ""
        ).strip()
        if reason:
            updated["recommendation_eligible"] = False
            updated["recommendation_suppression_reason"] = reason
            updated["planning_status_text"] = {
                "snoozed": "Rekommendation snoozad",
                "dismissed": "Samma beslutsunderlag markerat Ej relevant",
                "suggestion_planned": "Rekommendationsaktivitet planerad",
            }.get(reason, updated.get("planning_status_text") or reason)
        result.append(updated)
    return result


def _priority_score(
    *,
    segment,
    order_count,
    expected_order_dfp,
    expected_order_value,
    benchmarks,
    overdue_days,
    latest_contact_class,
    has_order_after_latest_contact,
    days_since_contact,
    follow_up_due,
    latest_contact_date,
    last_order_date,
    last_delivery_date,
    self_ordering_followup,
    future_follow_up_days,
    freezer_fields,
    email_signal,
    today,
) -> int:
    value_index = _value_index(expected_order_value, expected_order_dfp, benchmarks)
    score = 0.0
    score += 50 * value_index
    score += 25 * _timing_index(overdue_days, order_count)
    score += 15 * _engagement_index(
        latest_contact_class,
        days_since_contact,
        follow_up_due,
        has_order_after_latest_contact,
    )
    score += 7 * _segment_index(segment)
    score += 3 * _repeat_index(order_count)

    if order_count == 0 and segment in ["A", "B"]:
        score += 6

    score += _freezer_opportunity_points(
        freezer_fields=freezer_fields,
        order_count=order_count,
        overdue_days=overdue_days,
        latest_contact_class=latest_contact_class,
        days_since_contact=days_since_contact,
    )

    if follow_up_due and latest_contact_class != "Negativ" and score < 50:
        score = 50 + (8 * value_index)

    if (
        latest_contact_class == "Negativ"
        and not has_order_after_latest_contact
        and days_since_contact is not None
        and days_since_contact <= 30
    ):
        score -= 25

    if latest_contact_class == "Order lagd" and last_order_date and (today - last_order_date).days <= 14:
        score -= 20

    if last_order_date and (today - last_order_date).days <= 10:
        score -= 28

    if last_delivery_date and (today - last_delivery_date).days < 0:
        score -= 30

    if latest_contact_date and days_since_contact is not None and days_since_contact <= 2 and not follow_up_due:
        score -= 12

    if self_ordering_followup:
        score = min(score, 79)

    single_order_cap = _single_order_confidence_cap(
        order_count=order_count,
        follow_up_due=follow_up_due,
        overdue_days=overdue_days,
        freezer_fields=freezer_fields,
        latest_contact_class=latest_contact_class,
    )
    if single_order_cap is not None:
        score = min(score, single_order_cap)

    future_follow_up_cap = _future_follow_up_score_cap(future_follow_up_days)
    if future_follow_up_cap is not None:
        score = min(score, future_follow_up_cap)

    negative_contact_cap = _negative_contact_score_cap(
        latest_contact_class=latest_contact_class,
        days_since_contact=days_since_contact,
        has_order_after_latest_contact=has_order_after_latest_contact,
    )
    if negative_contact_cap is not None:
        score = min(score, negative_contact_cap)

    score = _apply_email_priority_score(score, email_signal)

    return max(0, min(100, int(round(score))))


def _priority_type(
    *,
    follow_up_due,
    scheduled_followup,
    has_order_after_latest_contact,
    order_count,
    overdue_days,
    latest_contact_class,
    days_since_contact,
    segment,
    self_ordering_followup,
) -> str:
    if latest_contact_class == "Negativ" and not has_order_after_latest_contact:
        return "Återaktivera efter negativt besked"
    if follow_up_due:
        return "Försenad uppföljning"
    if scheduled_followup or self_ordering_followup:
        return "Planerad uppföljning"
    if order_count > 0 and overdue_days is not None and overdue_days >= 0:
        if order_count == 1:
            return "Återaktivera provorder"
        return "Rädda återorder"
    if latest_contact_class == "Positiv" and not has_order_after_latest_contact:
        return "Varm chans"
    if order_count == 0 and segment in ["A", "B"]:
        return "Ny A/B-chans"
    if latest_contact_class == "Ej anträffbar" and days_since_contact is not None and days_since_contact >= 3:
        return "Försök igen"
    return "Låg prio"


def _priority_level(score: int) -> str:
    if score >= 80:
        return "Hög prio"
    if score >= 50:
        return "Medel prio"
    return "Låg prio"


def _recommended_action(priority_type: str) -> str:
    return {
        "Återaktivera efter negativt besked": "Kontrollera om läget ändrats",
        "Försenad uppföljning": "Följ upp",
        "Planerad uppföljning": "Bevaka",
        "Rädda återorder": "Driv återorder",
        "Återaktivera provorder": "Följ upp första ordern",
        "Varm chans": "Följ upp positiv dialog",
        "Ny A/B-chans": "Bearbeta som prioriterad kund",
        "Försök igen": "Gör nytt försök",
        "Låg prio": "Bearbeta vid tid över",
    }[priority_type]


def _recommended_channel(action_type: str | None) -> str:
    if action_type in {"new_ab", "trial_reorder", "reorder", "route_fill"}:
        return "besök"
    if action_type in {
        "follow_up",
        "warm_lead",
        "retry",
        "negative_reactivation",
        "stockfiller_click_followup",
        "product_sheet_click_followup",
    }:
        return "telefon"
    return "avvakta"


def _next_action(
    *,
    priority_type,
    follow_up_due,
    scheduled_followup,
    future_follow_up_days,
    overdue_days,
    total_dfp,
    expected_order_dfp,
    order_count,
    latest_contact_class,
    has_order_after_latest_contact,
    days_since_contact,
    latest_contact_date,
    last_order_date,
    segment,
    self_ordering_followup,
    email_signal,
    today,
) -> dict:
    if latest_contact_class == "Negativ" and not has_order_after_latest_contact:
        follow_up_reason = " · planerad uppföljning är försenad" if follow_up_due else ""
        return {
            "label": "Kontrollera om läget ändrats",
            "action_type": "negative_reactivation",
            "tone": "low",
            "reason": f"Negativt besked senast{follow_up_reason}",
            "primary_cta": "Öppna",
        }

    if follow_up_due:
        return {
            "label": "Följ upp idag",
            "action_type": "follow_up",
            "tone": "urgent",
            "reason": "Uppföljning missad · ingen senare kontakt eller order",
            "primary_cta": "Ring",
        }

    email_action = _email_next_action(email_signal)
    if email_action:
        return email_action

    if scheduled_followup or self_ordering_followup:
        reason = "Framtida uppföljning finns"
        if future_follow_up_days is not None:
            reason = f"Planerad uppföljning om {future_follow_up_days} dagar"
        if self_ordering_followup:
            reason = f"{reason} · kommentar tyder på att kunden lägger order själv"
        return {
            "label": "Bevaka planerad uppföljning",
            "action_type": "scheduled_followup",
            "tone": "warning" if future_follow_up_days is not None and future_follow_up_days <= 7 else "low",
            "reason": reason,
            "primary_cta": "Öppna",
        }

    if order_count == 1 and overdue_days is not None and overdue_days >= 0:
        return {
            "label": "Följ upp första ordern",
            "action_type": "trial_reorder",
            "tone": "urgent" if overdue_days >= 21 else "warning",
            "reason": "Första ordern är redo för uppföljning",
            "primary_cta": "Planera besök",
        }

    if order_count > 0 and overdue_days is not None and overdue_days >= 7:
        return {
            "label": "Driv återorder",
            "action_type": "reorder",
            "tone": "urgent" if overdue_days >= 21 else "warning",
            "reason": f"Över normal återköpstid +{overdue_days} dagar",
            "primary_cta": "Planera besök",
        }

    if latest_contact_class == "Positiv" and not has_order_after_latest_contact:
        return {
            "label": "Stäng positiv dialog",
            "action_type": "warm_lead",
            "tone": "positive",
            "reason": "Positiv dialog utan order",
            "primary_cta": "Följ upp",
        }

    if latest_contact_class == "Ej anträffbar" and days_since_contact is not None and days_since_contact >= 3:
        return {
            "label": "Försök igen",
            "action_type": "retry",
            "tone": "neutral",
            "reason": "Ej anträffbar senast",
            "primary_cta": "Ring",
        }

    segment_value = str(segment or "").strip().upper()[:1]
    if order_count == 0 and segment_value in ["A", "B"]:
        return {
            "label": "Bearbeta ny A/B-kund",
            "action_type": "new_ab",
            "tone": "opportunity",
            "reason": f"Segment {segment_value} · ingen order ännu",
            "primary_cta": "Planera besök",
        }

    if last_order_date and 0 <= (today - last_order_date).days <= 10:
        return {
            "label": "Bevaka rotation",
            "action_type": "monitor",
            "tone": "low",
            "reason": "Order nyligen lagd",
            "primary_cta": "Bevaka",
        }

    if latest_contact_class == "Negativ" and days_since_contact is not None and days_since_contact <= 30:
        return {
            "label": "Pausa/bevaka",
            "action_type": "pause",
            "tone": "low",
            "reason": "Negativ dialog nyligen",
            "primary_cta": "Bevaka",
        }

    return {
        "label": "Bearbeta vid besöksrutt",
        "action_type": "route_fill",
        "tone": "low",
        "reason": "Lägre prioritet just nu",
        "primary_cta": "Planera besök",
    }


def _priority_reasons(
    *,
    follow_up_due,
    future_follow_up_days,
    has_order_after_latest_contact,
    overdue_days,
    latest_contact_class,
    days_since_contact,
    total_dfp,
    expected_order_dfp,
    order_count,
    segment,
    latest_contact_date,
    self_ordering_followup,
    freezer_fields,
    email_signal,
) -> list[str]:
    reasons = []
    email_reason = _email_priority_reason(email_signal)
    if email_reason:
        reasons.append(email_reason)
    if latest_contact_class == "Negativ" and not has_order_after_latest_contact:
        if days_since_contact is None:
            reasons.append("Negativ kontakt senast")
        else:
            reasons.append(f"Negativ kontakt för {days_since_contact} dagar sedan")
    if expected_order_dfp:
        reasons.append(f"Orderpotential ca {_format_dfp(expected_order_dfp)}")
    if follow_up_due:
        reasons.append("Försenad uppföljning")
    if future_follow_up_days is not None:
        reasons.append(f"Planerad uppföljning om {future_follow_up_days} dagar")
    if self_ordering_followup:
        reasons.append("Kommentar tyder på självbeställning")
    freezer_reason = _freezer_reason(freezer_fields, order_count, overdue_days)
    if freezer_reason:
        reasons.append(freezer_reason)
    if order_count == 1 and overdue_days is not None and overdue_days >= 0:
        reasons.append("Första ordern redo för uppföljning")
    if overdue_days is not None and overdue_days >= 0:
        reasons.append(f"Över normal återköpstid: +{overdue_days} dagar")
    if latest_contact_class == "Positiv" and not has_order_after_latest_contact:
        reasons.append("Positiv dialog utan order")
    if order_count > 0:
        reasons.append(f"Tidigare kund: {_clean_number(total_dfp)} DFP")
    if segment == "A":
        reasons.append("Segment A")
    if segment == "B":
        reasons.append("Segment B")
    if latest_contact_date is None:
        reasons.append("Ej kontaktad tidigare")
    if latest_contact_class == "Ej anträffbar":
        reasons.append("Ej anträffbar senast")
    return reasons[:3]


def _email_priority_signal(*, email_feature, latest_human_contact, today) -> dict:
    """Normalize the latest email outcome and mark it handled by later human work."""
    status = str(email_feature.get("email_followup_status") or "").strip()
    event_at = parse_datetime(
        email_feature.get("email_followup_last_event_at")
        or email_feature.get("email_followup_sent_at")
    )
    sent_at = parse_datetime(email_feature.get("email_followup_sent_at"))
    wait_days = max(0, int(_parse_number(email_feature.get("email_followup_wait_days_remaining"))))
    handled = bool(
        status
        and latest_human_contact
        and event_at
        and latest_human_contact > event_at
    )
    return {
        "status": status,
        "active": bool(status) and not handled,
        "handled": handled,
        "event_at": event_at,
        "sent_at": sent_at,
        "wait_days": wait_days,
        "days_since_sent": (
            max(0, (today - sent_at.date()).days)
            if sent_at else None
        ),
        "days_since_event": (
            max(0, (today - event_at.date()).days)
            if event_at else None
        ),
    }


def _apply_email_priority_score(score: float, email_signal: dict) -> float:
    """Apply intent and cooldown without replacing the customer's business value."""
    if not email_signal.get("active"):
        return score

    status = email_signal.get("status")
    wait_days = email_signal.get("wait_days", 0)
    days_since_sent = email_signal.get("days_since_sent")
    days_since_event = email_signal.get("days_since_event")

    if status == "ordered_within_10_days":
        return (
            max(0, min(score - 15, 20))
            if days_since_event is not None and days_since_event <= 10
            else score
        )

    if status in {"stockfiller_clicked_no_order", "product_sheet_clicked_no_order"}:
        if wait_days > 0:
            return max(0, min(score - 6, 49))
        boost = 18 if status == "stockfiller_clicked_no_order" else 12
        floor = 60 if status == "stockfiller_clicked_no_order" else 55
        return max(score + boost, floor)

    if days_since_sent is not None and days_since_sent <= 10:
        if status == "opened_no_click":
            return max(0, min(score - 8, 45))
        if status == "delivered_no_activity":
            return max(0, min(score - 12, 35))

    return score


def _email_next_action(email_signal: dict) -> dict | None:
    if not email_signal.get("active"):
        return None

    status = email_signal.get("status")
    wait_days = email_signal.get("wait_days", 0)
    days_since_sent = email_signal.get("days_since_sent")
    days_since_event = email_signal.get("days_since_event")

    if (
        status == "ordered_within_10_days"
        and days_since_event is not None
        and days_since_event <= 10
    ):
        return {
            "label": "Bevaka ny order",
            "action_type": "email_order_monitor",
            "tone": "low",
            "reason": "Order registrerad inom 10 dagar efter mejlet",
            "primary_cta": "Bevaka",
        }

    if status in {"stockfiller_clicked_no_order", "product_sheet_clicked_no_order"}:
        if wait_days > 0:
            return {
                "label": "Avvakta efter mejlklick",
                "action_type": "email_click_wait",
                "tone": "low",
                "reason": f"Följ upp om {wait_days} dagar om order saknas",
                "primary_cta": "Bevaka",
            }
        stockfiller = status == "stockfiller_clicked_no_order"
        return {
            "label": (
                "Följ upp Stockfiller-klick"
                if stockfiller else "Följ upp produktbladsintresse"
            ),
            "action_type": (
                "stockfiller_click_followup"
                if stockfiller else "product_sheet_click_followup"
            ),
            "tone": "urgent" if stockfiller else "warning",
            "reason": "Tydlig köpintention men ingen order registrerad",
            "primary_cta": "Ring",
        }

    if days_since_sent is not None and days_since_sent <= 10:
        if status == "opened_no_click":
            return {
                "label": "Avvakta mejlutfall",
                "action_type": "email_open_wait",
                "tone": "low",
                "reason": "Mejlet är öppnat men ingen stark köpsignal finns ännu",
                "primary_cta": "Bevaka",
            }
        if status == "delivered_no_activity":
            return {
                "label": "Avvakta mejlutfall",
                "action_type": "email_delivery_wait",
                "tone": "low",
                "reason": "Mejlet är nyligen levererat",
                "primary_cta": "Bevaka",
            }

    return None


def _email_priority_reason(email_signal: dict) -> str:
    if not email_signal.get("active"):
        return ""
    status = email_signal.get("status")
    wait_days = email_signal.get("wait_days", 0)
    days_since_event = email_signal.get("days_since_event")
    if (
        status == "ordered_within_10_days"
        and days_since_event is not None
        and days_since_event <= 10
    ):
        return "Order efter mejl – ingen ny aktivitet behövs"
    if status == "stockfiller_clicked_no_order":
        return (
            f"Stockfiller-klick – avvakta {wait_days} dagar"
            if wait_days else "Stockfiller-klick utan order"
        )
    if status == "product_sheet_clicked_no_order":
        return (
            f"Produktbladsklick – avvakta {wait_days} dagar"
            if wait_days else "Produktbladsklick utan order"
        )
    if status == "opened_no_click":
        return "Mejl öppnat utan klick"
    if status == "delivered_no_activity":
        return "Mejl levererat utan aktivitet"
    return ""


def _is_self_ordering_followup(
    *,
    latest_contact_class,
    latest_contact_date,
    days_since_contact,
    latest_follow_up_date,
    follow_up_due,
    follow_up_resolved,
    self_ordering_signal,
    today,
) -> bool:
    return bool(
        self_ordering_signal
        and latest_contact_date
        and days_since_contact is not None
        and latest_follow_up_date
        and latest_follow_up_date > today
        and not follow_up_due
        and not follow_up_resolved
        and latest_contact_class in {"Positiv", "Neutral"}
    )


def _future_follow_up_days(
    *,
    latest_contact_date,
    latest_follow_up_date,
    follow_up_due,
    follow_up_resolved,
    today,
) -> int | None:
    if not latest_contact_date or not latest_follow_up_date:
        return None
    if follow_up_due or follow_up_resolved:
        return None
    if latest_follow_up_date <= today:
        return None
    return (latest_follow_up_date - today).days


def _future_follow_up_score_cap(future_follow_up_days) -> int | None:
    if future_follow_up_days is None:
        return None
    if future_follow_up_days <= 7:
        return 85
    if future_follow_up_days <= 21:
        return 75
    if future_follow_up_days <= 45:
        return 65
    return 55


def _negative_contact_score_cap(*, latest_contact_class, days_since_contact, has_order_after_latest_contact) -> int | None:
    if latest_contact_class != "Negativ" or has_order_after_latest_contact or days_since_contact is None:
        return None
    if days_since_contact <= 30:
        return 30
    if days_since_contact <= 90:
        return 50
    if days_since_contact <= 180:
        return 75
    return 95


def _single_order_confidence_cap(
    *,
    order_count,
    follow_up_due,
    overdue_days,
    freezer_fields,
    latest_contact_class,
) -> int | None:
    if order_count != 1:
        return None
    if follow_up_due and latest_contact_class != "Negativ":
        return None

    fields = set(freezer_fields or [])
    if "polarbar" in fields and overdue_days is not None and overdue_days >= 0:
        return 95
    if overdue_days is not None and overdue_days >= 21:
        return 90
    return 85


def _freezer_opportunity_points(
    *,
    freezer_fields,
    order_count,
    overdue_days,
    latest_contact_class,
    days_since_contact,
) -> int:
    fields = set(freezer_fields or [])
    if not fields:
        return 0
    if latest_contact_class == "Negativ" and days_since_contact is not None and days_since_contact <= 30:
        return 0

    has_polarbar = "polarbar" in fields
    has_franui = "Franui" in fields
    has_other_competitor = bool(OTHER_COMPETITOR_FREEZER_FIELDS & fields)
    has_none = "none" in fields
    no_prior_order = order_count == 0
    overdue_reorder = order_count > 0 and overdue_days is not None and overdue_days >= 0

    if has_none:
        if no_prior_order:
            return 8
        if overdue_reorder:
            return 5
        return 2

    if has_polarbar and not has_franui and not has_other_competitor:
        return 8 if overdue_reorder else 3

    if has_polarbar:
        return 6 if overdue_reorder else 2

    if has_franui and not has_other_competitor:
        return 10 if no_prior_order else 8

    if has_franui and has_other_competitor:
        return 6

    if has_other_competitor:
        return 4

    return 0


def _freezer_reason(freezer_fields, order_count, overdue_days) -> str:
    fields = set(freezer_fields or [])
    if not fields:
        return ""

    has_polarbar = "polarbar" in fields
    has_franui = "Franui" in fields
    has_other_competitor = bool(OTHER_COMPETITOR_FREEZER_FIELDS & fields)
    has_none = "none" in fields
    no_prior_order = order_count == 0
    overdue_reorder = order_count > 0 and overdue_days is not None and overdue_days >= 0

    if has_none:
        return "Frysdisken: ingen loggad konkurrent"
    if has_polarbar and not has_franui and not has_other_competitor:
        return "Frysdisken: Polarbär"
    if has_polarbar and overdue_reorder:
        return "Frysdisken: Polarbär + konkurrent"
    if has_franui and not has_other_competitor:
        return "Frysdisken: Franui, bredda sortiment"
    if has_franui and has_other_competitor:
        return "Frysdisken: flera konkurrenter"
    if has_other_competitor and no_prior_order:
        return "Frysdisken: konkurrent finns"
    if has_other_competitor:
        return "Frysdisken: konkurrensläge"
    return ""


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


def _segment_defaults(benchmarks: dict, segment: str) -> dict:
    defaults = benchmarks.get("segment_defaults", {})
    return defaults.get(str(segment or "").strip().upper()[:1]) or defaults.get("") or {
        "expected_order_dfp": 0,
        "expected_order_value": 0,
        "expected_cycle_days": 45,
    }


def _value_index(expected_order_value, expected_order_dfp, benchmarks: dict) -> float:
    value = _positive_float(expected_order_value)
    value_p90 = _positive_float(benchmarks.get("expected_order_value_p90"))
    if value and value_p90:
        return _clamp(value / value_p90, 0, 1)

    dfp = _positive_float(expected_order_dfp)
    dfp_p90 = _positive_float(benchmarks.get("expected_order_dfp_p90"))
    if dfp and dfp_p90:
        return _clamp(dfp / dfp_p90, 0, 1)

    return 0


def _timing_index(overdue_days, order_count) -> float:
    if overdue_days is None:
        return 0.25 if order_count == 0 else 0
    if overdue_days < -14:
        return 0
    if overdue_days < 0:
        return 0.25
    if overdue_days < 7:
        return 0.45
    if overdue_days < 21:
        return 0.7
    return 1


def _engagement_index(
    latest_contact_class,
    days_since_contact,
    follow_up_due,
    has_order_after_latest_contact,
) -> float:
    index = 0
    if follow_up_due and latest_contact_class != "Negativ":
        index = max(index, 0.8)
    if latest_contact_class == "Positiv" and not has_order_after_latest_contact:
        index = max(index, 1 if days_since_contact is not None and days_since_contact >= 3 else 0.45)
    if latest_contact_class == "Neutral" and follow_up_due:
        index = max(index, 0.65)
    if latest_contact_class == "Ej anträffbar" and days_since_contact is not None and days_since_contact >= 3:
        index = max(index, 0.35)
    return index


def _segment_index(segment) -> float:
    return {"A": 1, "B": 0.65, "C": 0.25}.get(str(segment or "").strip().upper()[:1], 0.15)


def _repeat_index(order_count) -> float:
    if order_count >= 3:
        return 1
    if order_count == 2:
        return 0.65
    if order_count == 1:
        return 0.35
    return 0


def _order_dfp(row: dict) -> float:
    total_weight = _parse_number(row.get("Total weight"))
    return total_weight if total_weight > 0 else _parse_number(row.get("Quantity"))


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
