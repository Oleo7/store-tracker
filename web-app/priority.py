from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, time, timedelta
from statistics import median


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

        quantity = _parse_number(row.get("Quantity"))
        if quantity <= 0:
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
        order["dfp"] += quantity
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
            "avg_dfp_per_order": _clean_number(total_dfp / order_count) if order_count else 0,
            "last_order_date": max(order_dates) if order_dates else latest_order.get("delivery_date"),
            "last_delivery_date": latest_order.get("delivery_date"),
            "latest_order_dfp": _clean_number(latest_order.get("dfp", 0)),
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
            latest_by_customer[customer_key] = {
                "latest_contact_date": contact_dt.date(),
                "latest_contact_datetime": contact_dt,
                "latest_contact_sales_person": str(row.get("sales_person") or "").strip(),
                "latest_contact_channel": str(row.get("contact_channel") or "").strip(),
                "latest_contact_result": result,
                "latest_contact_class": normalize_contact_result(result),
                "latest_follow_up_date": follow_up_date,
                "contact_count_30d": 0,
                "has_order_after_latest_contact": has_order_after_latest_contact,
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
        enriched_order = dict(order_feature)

        last_delivery = enriched_order.get("last_delivery_date")
        last_order = enriched_order.get("last_order_date")
        expected_next = enriched_order.get("expected_next_order_date")
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

        score = _priority_score(
            segment=segment,
            order_count=enriched_order.get("order_count", 0),
            total_dfp=enriched_order.get("total_dfp", 0),
            overdue_days=overdue_days,
            latest_contact_class=latest_contact_class,
            has_order_after_latest_contact=has_order_after_latest_contact,
            days_since_contact=days_since_contact,
            follow_up_due=follow_up_due,
            latest_contact_date=latest_contact_date,
            last_order_date=last_order,
            today=today,
        )
        priority_type = _priority_type(
            follow_up_due=follow_up_due,
            has_order_after_latest_contact=has_order_after_latest_contact,
            order_count=enriched_order.get("order_count", 0),
            overdue_days=overdue_days,
            latest_contact_class=latest_contact_class,
            days_since_contact=days_since_contact,
            segment=segment,
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
                "order_count": enriched_order.get("order_count", 0),
                "total_dfp": _clean_number(enriched_order.get("total_dfp", 0)),
                "latest_order_date": _iso_date(last_order),
                "latest_delivery_date": _iso_date(last_delivery),
                "days_since_delivery": days_since_delivery,
                "expected_cycle_days": enriched_order.get("expected_cycle_days"),
                "expected_next_order_date": _iso_date(expected_next),
                "overdue_days": overdue_days,
                "latest_contact_date": _iso_date(latest_contact_date),
                "latest_contact_result": contact_feature.get("latest_contact_result", ""),
                "latest_contact_class": latest_contact_class or "",
                "latest_contact_channel": contact_feature.get("latest_contact_channel", ""),
                "latest_contact_sales_person": contact_feature.get("latest_contact_sales_person", ""),
                "follow_up_due": follow_up_due,
                "has_order_after_latest_contact": has_order_after_latest_contact,
                "reasons": _priority_reasons(
                    follow_up_due=follow_up_due,
                    has_order_after_latest_contact=has_order_after_latest_contact,
                    overdue_days=overdue_days,
                    latest_contact_class=latest_contact_class,
                    days_since_contact=days_since_contact,
                    total_dfp=enriched_order.get("total_dfp", 0),
                    order_count=enriched_order.get("order_count", 0),
                    segment=segment,
                    latest_contact_date=latest_contact_date,
                ),
            }
        )

    result.sort(
        key=lambda c: (
            c["priority_score"],
            _segment_rank(c.get("segment")),
            c.get("overdue_days") if c.get("overdue_days") is not None else -999,
            c.get("total_dfp") or 0,
        ),
        reverse=True,
    )
    return result[:limit]


def _priority_score(
    *,
    segment,
    order_count,
    total_dfp,
    overdue_days,
    latest_contact_class,
    has_order_after_latest_contact,
    days_since_contact,
    follow_up_due,
    latest_contact_date,
    last_order_date,
    today,
) -> int:
    score = 0
    if segment == "A":
        score += 25
    elif segment == "B":
        score += 15
    elif segment == "C":
        score += 5
    else:
        score += 3

    if total_dfp >= 80:
        score += 20
    elif total_dfp >= 30:
        score += 12
    elif order_count > 0:
        score += 7

    if order_count > 0 and overdue_days is not None:
        if overdue_days >= 21:
            score += 30
        elif overdue_days >= 7:
            score += 20
        elif overdue_days >= 0:
            score += 10

    if latest_contact_class == "Positiv" and not has_order_after_latest_contact:
        if days_since_contact is not None and days_since_contact >= 3:
            score += 30
        else:
            score += 10

    if latest_contact_class == "Neutral" and follow_up_due:
        score += 15

    if latest_contact_class == "Ej anträffbar" and days_since_contact is not None and days_since_contact >= 3:
        score += 8

    if latest_contact_class == "Negativ" and days_since_contact is not None and days_since_contact <= 30:
        score -= 25

    if latest_contact_class == "Order lagd" and last_order_date and (today - last_order_date).days <= 14:
        score -= 20

    if order_count == 0 and segment in ["A", "B"]:
        score += 10

    if order_count == 0 and latest_contact_date is None:
        score += 8

    if last_order_date and (today - last_order_date).days <= 10:
        score -= 20

    if latest_contact_date and days_since_contact is not None and days_since_contact <= 2 and not follow_up_due:
        score -= 15

    return max(0, min(100, int(round(score))))


def _priority_type(
    *,
    follow_up_due,
    has_order_after_latest_contact,
    order_count,
    overdue_days,
    latest_contact_class,
    days_since_contact,
    segment,
) -> str:
    if follow_up_due and not has_order_after_latest_contact:
        return "Försenad uppföljning"
    if order_count > 0 and overdue_days is not None and overdue_days >= 7:
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
        "Rädda återorder": "Driv återorder",
        "Varm chans": "Följ upp positiv dialog",
        "Ny A/B-chans": "Bearbeta som prioriterad kund",
        "Försök igen": "Gör nytt försök",
        "Låg prio": "Bearbeta vid tid över",
    }[priority_type]


def _priority_reasons(
    *,
    follow_up_due,
    has_order_after_latest_contact,
    overdue_days,
    latest_contact_class,
    days_since_contact,
    total_dfp,
    order_count,
    segment,
    latest_contact_date,
) -> list[str]:
    reasons = []
    if follow_up_due and not has_order_after_latest_contact:
        reasons.append("Försenad uppföljning")
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
