from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, time, timedelta
import re
from statistics import median
import unicodedata


FREEZER_FIELDS = ("Franui", "Schufrulade", "Boujee", "polarbar", "none")
OTHER_COMPETITOR_FREEZER_FIELDS = {"Schufrulade", "Boujee"}


def normalize_customer_key(value: str) -> str:
    text = str(value or "").replace("\xa0", " ").strip().casefold()
    return " ".join(text.split())


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
        customer_number = str(row.get("Customer number") or "").strip()
        grouping_key = normalize_customer_key(customer_number) or normalize_customer_key(customer)

        order = orders.setdefault(
            (grouping_key, reference),
            {
                "reference": reference,
                "customer": customer,
                "customer_key": normalize_customer_key(customer),
                "customer_number": customer_number,
                "order_date": order_date,
                "delivery_date": delivery_date,
                "dfp": 0.0,
                "sales": 0.0,
            },
        )
        order["dfp"] += dfp
        order["sales"] += total
        if order_date and (not order["order_date"] or order_date > order["order_date"]):
            order["order_date"] = order_date
        if delivery_date and (not order["delivery_date"] or delivery_date > order["delivery_date"]):
            order["delivery_date"] = delivery_date
        if customer_number and not order["customer_number"]:
            order["customer_number"] = customer_number

    orders_by_customer = defaultdict(list)
    for order in orders.values():
        if order.get("sales", 0) <= 0:
            continue
        key = normalize_customer_key(order.get("customer_number")) or order["customer_key"]
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

        median_gap = None
        expected_cycle = None
        if len(delivery_dates) >= 2:
            gaps = [(delivery_dates[i] - delivery_dates[i - 1]).days for i in range(1, len(delivery_dates))]
            median_gap = median(gaps)
            expected_cycle = _clamp(round(median_gap * 1.25), 14, 60)

        primary_name_key = latest_order["customer_key"]
        features[primary_name_key] = {
            "customer_key": primary_name_key,
            "customer_number": latest_order.get("customer_number", ""),
            "order_count": order_count,
            "total_dfp": _clean_number(total_dfp),
            "total_sales": _clean_number(total_sales),
            "avg_dfp_per_order": _clean_number(avg_dfp),
            "avg_sales_per_order": _clean_number(avg_sales),
            "last_order_date": max(order_dates) if order_dates else latest_order.get("delivery_date"),
            "last_delivery_date": latest_order.get("delivery_date"),
            "latest_order_dfp": _clean_number(latest_dfp),
            "latest_order_value": _clean_number(latest_sales),
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

    return features


def build_contact_features(sales_activities: list[dict], order_features: dict) -> dict:
    latest_by_customer = {}
    contact_count_30d = defaultdict(int)
    today = date.today()

    for row in sales_activities:
        contact_dt = parse_datetime(row.get("date_time"))
        if not contact_dt:
            continue

        customer_key = normalize_customer_key(row.get("customer"))
        if not customer_key:
            continue

        if (today - contact_dt.date()).days <= 30:
            contact_count_30d[customer_key] += 1

        if customer_key not in latest_by_customer or contact_dt > latest_by_customer[customer_key]["latest_contact_datetime"]:
            follow_up_date = parse_date(row.get("follow_up_date"))
            order_feature = order_features.get(customer_key, {})
            latest_order_date = order_feature.get("last_order_date")
            has_order_after_latest_contact = bool(latest_order_date and latest_order_date >= contact_dt.date())
            result = str(row.get("result") or "").replace("\xa0", " ").strip()
            comment = str(row.get("comment") or "").replace("\xa0", " ").strip()
            freezer_fields = _freezer_fields(row)
            latest_by_customer[customer_key] = {
                "latest_contact_date": contact_dt.date(),
                "latest_contact_datetime": contact_dt,
                "latest_contact_sales_person": str(row.get("sales_person") or "").strip(),
                "latest_contact_channel": str(row.get("contact_channel") or "").strip(),
                "latest_contact_result": result,
                "latest_contact_class": normalize_contact_result(result),
                "latest_contact_comment": comment,
                "latest_freezer_fields": freezer_fields,
                "latest_follow_up_date": follow_up_date,
                "contact_count_30d": 0,
                "has_order_after_latest_contact": has_order_after_latest_contact,
                "self_ordering_signal": _has_self_ordering_signal(comment),
                "days_since_contact": None,
                "follow_up_due": False,
            }

    for customer_key, feature in latest_by_customer.items():
        feature["contact_count_30d"] = contact_count_30d.get(customer_key, 0)

    return latest_by_customer


def build_priority_customers(
    customers: list[dict],
    order_features: dict,
    contact_features: dict,
    responsible: str | None,
    today: date,
    limit: int = 30,
) -> list[dict]:
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
        days_since_contact = (today - latest_contact_date).days if latest_contact_date else None
        has_order_after_latest_contact = bool(
            latest_contact_date
            and last_order
            and last_order >= latest_contact_date
        )
        latest_follow_up_date = contact_feature.get("latest_follow_up_date")
        follow_up_due = bool(
            latest_follow_up_date
            and latest_follow_up_date <= today
            and not has_order_after_latest_contact
        )
        latest_contact_class = contact_feature.get("latest_contact_class")
        future_follow_up_days = _future_follow_up_days(
            latest_contact_date=latest_contact_date,
            latest_follow_up_date=latest_follow_up_date,
            follow_up_due=follow_up_due,
            has_order_after_latest_contact=has_order_after_latest_contact,
            today=today,
        )
        scheduled_followup = future_follow_up_days is not None
        self_ordering_followup = _is_self_ordering_followup(
            latest_contact_class=latest_contact_class,
            latest_contact_date=latest_contact_date,
            days_since_contact=days_since_contact,
            latest_follow_up_date=latest_follow_up_date,
            follow_up_due=follow_up_due,
            has_order_after_latest_contact=has_order_after_latest_contact,
            self_ordering_signal=contact_feature.get("self_ordering_signal"),
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
                "latest_contact_class": latest_contact_class or "",
                "latest_contact_channel": contact_feature.get("latest_contact_channel", ""),
                "latest_contact_sales_person": contact_feature.get("latest_contact_sales_person", ""),
                "latest_follow_up_date": _iso_date(latest_follow_up_date),
                "future_follow_up_days": future_follow_up_days,
                "latest_freezer_fields": list(contact_feature.get("latest_freezer_fields") or []),
                "follow_up_due": follow_up_due,
                "has_order_after_latest_contact": has_order_after_latest_contact,
                "self_ordering_signal": bool(contact_feature.get("self_ordering_signal")),
                "next_action": _next_action(
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
                    today=today,
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

    if follow_up_due and score < 50:
        score = 50 + (8 * value_index)

    if latest_contact_class == "Negativ" and days_since_contact is not None and days_since_contact <= 30:
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
    )
    if single_order_cap is not None:
        score = min(score, single_order_cap)

    future_follow_up_cap = _future_follow_up_score_cap(future_follow_up_days)
    if future_follow_up_cap is not None:
        score = min(score, future_follow_up_cap)

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
    if follow_up_due and not has_order_after_latest_contact:
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
        "Försenad uppföljning": "Följ upp",
        "Planerad uppföljning": "Bevaka",
        "Rädda återorder": "Driv återorder",
        "Återaktivera provorder": "Följ upp första ordern",
        "Varm chans": "Följ upp positiv dialog",
        "Ny A/B-chans": "Bearbeta som prioriterad kund",
        "Försök igen": "Gör nytt försök",
        "Låg prio": "Bearbeta vid tid över",
    }[priority_type]


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
    today,
) -> dict:
    if follow_up_due and not has_order_after_latest_contact:
        return {
            "label": "Följ upp idag",
            "action_type": "follow_up",
            "tone": "urgent",
            "reason": "Försenad uppföljning · ingen order efter senaste kontakt",
            "primary_cta": "Ring",
        }

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
            "reason": f"Första ordern är redo för uppföljning · potential ca {_format_dfp(expected_order_dfp)}",
            "primary_cta": "Ring",
        }

    if order_count > 0 and overdue_days is not None and overdue_days >= 7:
        return {
            "label": "Ring för återorder",
            "action_type": "reorder",
            "tone": "urgent" if overdue_days >= 21 else "warning",
            "reason": f"Över normal återköpstid +{overdue_days} dagar · potential ca {_format_dfp(expected_order_dfp or total_dfp)}",
            "primary_cta": "Ring",
        }

    if latest_contact_class == "Positiv" and not has_order_after_latest_contact:
        return {
            "label": "Stäng positiv dialog",
            "action_type": "warm_lead",
            "tone": "positive",
            "reason": f"Positiv dialog · potential ca {_format_dfp(expected_order_dfp)}",
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
            "primary_cta": "Kontakta",
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
        "label": "Bearbeta vid rutt",
        "action_type": "route_fill",
        "tone": "low",
        "reason": "Lägre prioritet just nu",
        "primary_cta": "Öppna",
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
) -> list[str]:
    reasons = []
    if expected_order_dfp:
        reasons.append(f"Orderpotential ca {_format_dfp(expected_order_dfp)}")
    if follow_up_due and not has_order_after_latest_contact:
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
    if latest_contact_class == "Negativ" and days_since_contact is not None and days_since_contact <= 30:
        reasons.append("Negativ kontakt senaste 30 dagarna")
    return reasons[:3]


def _is_self_ordering_followup(
    *,
    latest_contact_class,
    latest_contact_date,
    days_since_contact,
    latest_follow_up_date,
    follow_up_due,
    has_order_after_latest_contact,
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
        and not has_order_after_latest_contact
        and latest_contact_class in {"Positiv", "Neutral"}
    )


def _future_follow_up_days(
    *,
    latest_contact_date,
    latest_follow_up_date,
    follow_up_due,
    has_order_after_latest_contact,
    today,
) -> int | None:
    if not latest_contact_date or not latest_follow_up_date:
        return None
    if follow_up_due or has_order_after_latest_contact:
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


def _single_order_confidence_cap(*, order_count, follow_up_due, overdue_days, freezer_fields) -> int | None:
    if order_count != 1 or follow_up_due:
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


def _build_priority_benchmarks(customers: list[dict], order_features: dict, number_index: dict) -> dict:
    global_dfp = []
    global_value = []
    global_cycles = []
    by_segment = defaultdict(lambda: {"dfp": [], "value": [], "cycle": []})

    for customer in customers:
        if _is_truthy(customer.get("cancelled_flag")):
            continue

        customer_key = normalize_customer_key(customer.get("customer"))
        customer_number_key = normalize_customer_key(customer.get("customer_number"))
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
    if follow_up_due:
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
    return _clamp(round(median(cycles) * 1.25), 21, 75)


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
