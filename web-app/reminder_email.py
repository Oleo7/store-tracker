"""Pure helpers for Store Tracker email proposals and Brevo events."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timezone
from email.utils import parseaddr
from hashlib import sha256
from html import escape
import json
import re
from urllib.parse import urlparse
from zoneinfo import ZoneInfo


EMAIL_MESSAGES_COLUMNS = [
    "email_id", "customer", "customer_number", "email_type", "sender_user_name", "sender_name",
    "sender_email", "subject", "body_text", "body_html", "latest_order_reference",
    "latest_delivery_date", "product_sheet_url", "stockfiller_url", "is_test",
    "recipient_count", "status", "created_at", "sent_at",
]

EMAIL_RECIPIENTS_COLUMNS = [
    "email_id", "customer", "intended_email", "actual_email", "greeting_name",
    "brevo_message_id", "send_status", "send_error", "sent_at", "delivered_at",
    "first_opened_at", "last_opened_at", "open_count",
    "product_sheet_first_clicked_at", "product_sheet_last_clicked_at", "product_sheet_click_count",
    "stockfiller_first_clicked_at", "stockfiller_last_clicked_at", "stockfiller_click_count",
    "bounce_type", "blocked_at", "unsubscribed_at", "last_event_at",
]

EMAIL_EVENTS_COLUMNS = [
    "event_key", "received_at", "event_time", "email_id", "brevo_message_id",
    "intended_email", "actual_email", "event_type", "url", "payload_json",
]

USER_COLUMNS = ["user_name", "name", "role", "email", "phone", "password", "active"]
SETTINGS_COLUMNS = ["key", "value", "description"]

EMAIL_PROPOSAL_TYPES = {
    "reminder": "Påminnelse",
    "reactivation": "Återaktivering",
    "new_customer": "Nykund",
}

EMAIL_PROPOSAL_PRODUCT_SETTINGS = {
    "reminder": "reminder_product_sheet_url",
    "reactivation": "reactivation_product_sheet_url",
    "new_customer": "new_customer_product_sheet_url",
}

EMAIL_PROPOSAL_CTA_LABELS = {
    "reminder": "Se Produktblad",
    "reactivation": "Se Produktblad",
    "new_customer": "Se nykundserbjudande",
}

STANDARD_PROPOSAL_SKUS = ("sku_10003", "sku_10005", "sku_10002", "sku_10006")

VISIBLE_EMAIL_EVENT_TYPES = {
    "opened": "Öppnat",
    "product_sheet_clicked": "Produktblad klickat",
    "stockfiller_clicked": "Stockfiller klickat",
}

BLOCKING_SEND_STATUSES = {"hardbounce", "blocked", "invalid", "spam", "unsubscribed"}
STOCKHOLM_TIMEZONE = ZoneInfo("Europe/Stockholm")
PLACEHOLDER_NAME_WORDS = {
    "butik", "butiken", "customer", "inköp", "info", "kontakt", "kontaktperson",
    "kund", "order", "sales",
}
GENERIC_EMAIL_WORDS = PLACEHOLDER_NAME_WORDS | {
    "bestallning", "beställning", "butikschef", "bc", "ekonomi", "faktura", "gem",
    "djupfryst", "frys", "frysansvarig", "kolonial", "lager", "mail", "mejeri",
    "reception", "sc", "scvaruflode", "service", "varuflode", "varuflöde",
}


def is_yes(value):
    return str(value or "").strip().casefold() in {"y", "yes", "ja", "1", "true", "on"}


def normalize_email(value):
    return str(value or "").strip().casefold()


def is_valid_email(value):
    text = str(value or "").strip()
    if not text or len(text) > 254 or "\n" in text or "\r" in text:
        return False
    _, address = parseaddr(text)
    if address != text or address.count("@") != 1:
        return False
    local, domain = address.rsplit("@", 1)
    return bool(local and "." in domain and not domain.startswith(".") and not domain.endswith("."))


def split_email_values(*values):
    result = []
    seen = set()
    for value in values:
        for candidate in re.split(r"[,;\n\r]+", str(value or "")):
            email = candidate.strip()
            key = normalize_email(email)
            if not key or key in seen:
                continue
            seen.add(key)
            result.append({"email": email, "valid": is_valid_email(email)})
    return result


def first_name(value):
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if not text or "@" in text or not re.search(r"[a-zåäö]", text, re.IGNORECASE):
        return ""
    first = text.split(" ", 1)[0].strip(".,:;–—-_()[]")
    if (
        not first
        or not re.search(r"[a-zåäö]", first, re.IGNORECASE)
        or first.casefold() in PLACEHOLDER_NAME_WORDS
    ):
        return ""
    return first


def greeting_name_from_email(value):
    """Infer a first name only when the mailbox looks personal, never store-generic."""
    email = normalize_email(value)
    if not is_valid_email(email):
        return ""
    local = email.rsplit("@", 1)[0].split("+", 1)[0]
    words = [word for word in re.split(r"[._-]+", local) if word]
    if not words or any(word in GENERIC_EMAIL_WORDS for word in words):
        return ""
    first = words[0]
    if not re.fullmatch(r"[a-zåäö]{2,}(?:-[a-zåäö]{2,})?", first, re.IGNORECASE):
        return ""
    return "-".join(part[:1].upper() + part[1:] for part in first.split("-"))


def recipient_greeting_name(email, customer_name=""):
    """Choose the safest editable greeting: personal mailbox, then explicit CRM name."""
    return greeting_name_from_email(email) or first_name(customer_name)


def safe_http_url(value):
    text = str(value or "").strip()
    try:
        parsed = urlparse(text)
    except ValueError:
        return ""
    return text if parsed.scheme in {"http", "https"} and parsed.netloc else ""


def _parse_date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip().replace("Z", "").replace("T", " ")
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
                "%Y/%m/%d", "%d/%m/%Y", "%d.%m.%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        return None


def _number(value):
    text = str(value or "").strip().replace("\xa0", "").replace(" ", "")
    if not text:
        return 0.0
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    else:
        text = text.replace(",", ".")
    text = re.sub(r"[^0-9.\-]", "", text)
    try:
        return float(text)
    except ValueError:
        return 0.0


def _is_ordered_row(row):
    return _number(row.get("Quantity")) > 0 or _number(row.get("Total")) > 0


def format_quantity(value):
    number = _number(value)
    if number.is_integer():
        return str(int(number))
    return (f"{number:.2f}".rstrip("0").rstrip(".")).replace(".", ",")


def swedish_short_date(value):
    parsed = _parse_date(value)
    if not parsed:
        return ""
    months = ["januari", "februari", "mars", "april", "maj", "juni",
              "juli", "augusti", "september", "oktober", "november", "december"]
    return f"{parsed.day} {months[parsed.month - 1]}"


def normalize_proposal_type(value):
    proposal_type = str(value or "").strip().casefold()
    return proposal_type if proposal_type in EMAIL_PROPOSAL_TYPES else "reminder"


def classify_customer_relationship(order_rows, customer_name, today=None, recent_days=60):
    """Put a customer in exactly one email relationship segment."""
    today = today or date.today()
    customer_key = str(customer_name or "").replace("\xa0", " ").strip().casefold()
    relevant = [
        row for row in order_rows
        if str(row.get("Customer", "")).replace("\xa0", " ").strip().casefold() == customer_key
        and _is_ordered_row(row)
    ]
    if not relevant:
        return {
            "email_type": "new_customer",
            "email_type_label": EMAIL_PROPOSAL_TYPES["new_customer"],
            "latest_delivery_date": "",
            "days_since_delivery": None,
            "has_prior_order": False,
        }

    deliveries = [_parse_date(row.get("Delivery date")) for row in relevant]
    deliveries = [delivery for delivery in deliveries if delivery]
    latest_delivery = max(deliveries) if deliveries else None
    days_since_delivery = (today - latest_delivery).days if latest_delivery else None
    email_type = (
        "reminder"
        if latest_delivery and days_since_delivery <= recent_days
        else "reactivation"
    )
    return {
        "email_type": email_type,
        "email_type_label": EMAIL_PROPOSAL_TYPES[email_type],
        "latest_delivery_date": latest_delivery.isoformat() if latest_delivery else "",
        "days_since_delivery": days_since_delivery,
        "has_prior_order": True,
    }


def count_unique_order_customers(order_rows):
    return len({
        str(row.get("Customer", "")).replace("\xa0", " ").strip().casefold()
        for row in order_rows
        if str(row.get("Customer", "")).strip() and _is_ordered_row(row)
    })


def round_store_count_to_ten(value):
    """Round store counts to the nearest ten, with five always rounded up."""
    count = max(0, int(_number(value)))
    return ((count + 5) // 10) * 10


def build_settings_product_catalog(settings):
    """Build the editable product catalogue from settings rows named sku_<number>."""
    products = []
    for key, value in (settings or {}).items():
        key_text = str(key or "").strip().casefold()
        match = re.fullmatch(r"sku_(\d+)", key_text)
        product = str(value or "").strip()
        if not match or not product:
            continue
        products.append({
            "key": key_text,
            "product": product,
            "unit": "DFP",
            "sort_order": int(match.group(1)),
        })
    products.sort(key=lambda item: (item["sort_order"], item["product"].casefold()))
    for item in products:
        item.pop("sort_order", None)
    return products


def _product_words(value):
    return set(re.findall(r"[a-zåäö]+", str(value or "").casefold()))


def _product_flavor(value):
    text = str(value or "").casefold()
    for flavor in ("jordgubb", "blåbär", "hallon", "mango"):
        if flavor in text:
            return flavor
    return ""


def _catalog_product_for_name(product_name, product_catalog):
    requested = str(product_name or "").strip()
    if not requested:
        return None
    requested_key = requested.casefold()
    exact = next(
        (item for item in product_catalog if str(item.get("product", "")).strip().casefold() == requested_key),
        None,
    )
    if exact:
        return exact

    flavor = _product_flavor(requested)
    if not flavor:
        return None
    requested_words = _product_words(requested)
    candidates = [item for item in product_catalog if _product_flavor(item.get("product")) == flavor]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda item: len(requested_words & _product_words(item.get("product"))),
    )


def canonicalize_proposal_order_rows(order_rows, product_catalog):
    """Map latest-order names to the current sku names and combine mapped duplicates."""
    totals = {}
    for row in order_rows or []:
        item = _catalog_product_for_name(row.get("product"), product_catalog)
        if not item:
            continue
        key = item["key"]
        if key not in totals:
            totals[key] = {
                "product": item["product"],
                "quantity": 0.0,
                "unit": "DFP",
            }
        totals[key]["quantity"] += _number(row.get("quantity"))
    return [
        {
            "product": row["product"],
            "quantity": format_quantity(row["quantity"]),
            "unit": row["unit"],
        }
        for row in totals.values()
        if row["quantity"] > 0
    ]


def build_fixed_proposal_order_rows(product_catalog, quantity):
    products_by_key = {str(item.get("key", "")).casefold(): item for item in product_catalog or []}
    return [
        {
            "product": products_by_key[key]["product"],
            "quantity": str(quantity),
            "unit": "DFP",
        }
        for key in STANDARD_PROPOSAL_SKUS
        if key in products_by_key
    ]


def build_product_catalog(order_rows):
    """Rank DFP products by unique buying stores, then total ordered volume."""
    products = {}
    for row in order_rows:
        product = str(row.get("Product", "")).strip()
        unit = str(row.get("Unit", "")).strip() or "DFP"
        quantity = _number(row.get("Quantity"))
        if not product or unit.casefold() != "dfp" or quantity <= 0:
            continue
        key = product.casefold()
        item = products.setdefault(key, {
            "product": product,
            "unit": "DFP",
            "total_quantity": 0.0,
            "customers": set(),
        })
        item["total_quantity"] += quantity
        customer = str(row.get("Customer", "")).replace("\xa0", " ").strip().casefold()
        if customer:
            item["customers"].add(customer)
    return sorted(
        products.values(),
        key=lambda item: (-len(item["customers"]), -item["total_quantity"], item["product"].casefold()),
    )


def build_reactivation_order_rows(product_catalog):
    return build_fixed_proposal_order_rows(product_catalog, 4)


def build_new_customer_order_rows(product_catalog):
    return build_fixed_proposal_order_rows(product_catalog, 3)


def build_latest_order_context(order_rows, customer_name):
    customer_key = str(customer_name or "").strip().casefold()
    relevant = [
        row for row in order_rows
        if str(row.get("Customer", "")).strip().casefold() == customer_key and _is_ordered_row(row)
    ]
    if not relevant:
        return {
            "reference": "", "delivery_date": "", "order_date": "", "placed_by": "",
            "buyer_email": "", "order_rows": [],
        }

    groups = defaultdict(list)
    for index, row in enumerate(relevant):
        reference = str(row.get("Reference", "")).strip() or f"__row_{index}"
        groups[reference].append(row)

    def group_key(item):
        reference, rows = item
        delivery = max((_parse_date(row.get("Delivery date")) or date.min for row in rows), default=date.min)
        ordered = max((_parse_date(row.get("Order date")) or date.min for row in rows), default=date.min)
        return delivery, ordered, reference

    reference, latest_rows = max(groups.items(), key=group_key)
    delivery_date = max((_parse_date(row.get("Delivery date")) or date.min for row in latest_rows), default=date.min)
    order_date = max((_parse_date(row.get("Order date")) or date.min for row in latest_rows), default=date.min)

    product_totals = defaultdict(float)
    product_units = {}
    for row in latest_rows:
        product = str(row.get("Product", "")).strip()
        if not product:
            continue
        unit = str(row.get("Unit", "")).strip() or "DFP"
        product_totals[(product, unit)] += _number(row.get("Quantity"))
        product_units[(product, unit)] = unit

    products = [
        {"product": product, "quantity": format_quantity(quantity), "unit": unit.upper() if unit.casefold() == "dfp" else unit}
        for (product, unit), quantity in product_totals.items()
    ]

    first_row = latest_rows[0]
    return {
        "reference": "" if reference.startswith("__row_") else reference,
        "delivery_date": "" if delivery_date == date.min else delivery_date.isoformat(),
        "order_date": "" if order_date == date.min else order_date.isoformat(),
        "placed_by": str(first_row.get("placedBy", "")).strip(),
        "buyer_email": str(first_row.get("buyerEmail", "")).strip(),
        "order_rows": products,
    }


def build_email_proposal_copy(proposal_type, customer_name, latest_delivery_date="",
                              has_order_rows=False, unique_store_count=0, untried_count=0):
    proposal_type = normalize_proposal_type(proposal_type)
    customer_name = str(customer_name or "").strip()
    rounded_store_count = round_store_count_to_ten(unique_store_count)
    if proposal_type == "reactivation":
        subject = "Polarbär växer och sänker priserna!"
        intro = (
            "Hej (namn)\n\n"
            "Det var ett tag sedan vi senast levererade Polarbär till er.\n\n"
            f"Sedan er senaste beställning har Polarbär köpts in av över {rounded_store_count} butiker.\n"
            "De större volymerna gör att vi nu har kunnat **sänka inköpspriset till 31,50 kr "
            "per bägare redan från 12 DFP**, ända ned till **29 kr vid större volymer**.\n\n"
            "**Fri frakt ingår fortfarande.**\n\n"
            "För att göra det enkelt föreslår jag en mindre order med våra fyra mest populära smaker:"
        )
        closing = (
            "Svara bara på det här mejlet med ”kör”, så ordnar jag beställningen.\n\n"
            "I produktbladet ser du de nya priserna och vårt återaktiveringserbjudande. "
            "Du kan också beställa direkt i Stockfiller via länken nedan."
        )
    elif proposal_type == "new_customer":
        subject = f"Ta in Polarbär hos {customer_name}?"
        store_sentence = (
            f"Totalt har nu över {rounded_store_count} butiker köpt in Polarbär och vi fortsätter växa."
            if rounded_store_count
            else "Polarbär finns redan i butiker runt om i Sverige och vi fortsätter växa."
        )
        intro = (
            f"Hej (namn)\n\n{store_sentence}\n"
            "Varumärket har blivit populärt på sociala medier och våra större volymer gör att vi nu "
            "har kunnat **sänka inköpspriset till 31,50 kr per bägare redan från 12 DFP**, ända ned "
            "till **29 kr vid större volymer**.\n\n"
            "**Fri frakt ingår fortfarande.**\n\n"
            "För att göra det enkelt föreslår jag en mindre order med våra fyra mest populära smaker:"
        )
        closing = (
            "Svara bara på det här mejlet med ”kör”, så ordnar jag en första beställning.\n\n"
            "I produktbladet ser du vårt nykundserbjudande. Du kan också beställa direkt i "
            "Stockfiller via länken nedan."
        )
    else:
        subject = f"Dags att fylla på Polarbär hos {customer_name}?"
        delivery_text = swedish_short_date(latest_delivery_date)
        if delivery_text:
            intro = (
                "Hej (namn)\n\n"
                f"Jag ville bara stämma av hur Polarbär-lagret ser ut efter er senaste leverans "
                f"den {delivery_text}. Börjar det bli dags att fylla på?"
            )
        else:
            intro = (
                "Hej (namn)\n\n"
                "Jag ville bara stämma av hur Polarbär-lagret ser ut hos er. Börjar det bli dags att fylla på?"
            )
        if has_order_rows:
            intro += "\n\nBaserat på er senaste order föreslår jag:"
        closing = (
            "Svara bara på det här mejlet med ”kör”, så ordnar jag beställningen.\n\n"
            "Kika gärna in vårt produktblad eller beställ själv i Stockfiller via länken nedan."
        )
    return {
        "subject": subject,
        "intro_text": intro,
        "closing_text": closing,
        "product_sheet_label": EMAIL_PROPOSAL_CTA_LABELS[proposal_type],
        "stockfiller_label": "Beställ direkt via Stockfiller",
    }


def build_default_copy(customer_name, latest_delivery_date, has_order_rows):
    """Backward-compatible reminder copy helper."""
    return build_email_proposal_copy(
        "reminder",
        customer_name,
        latest_delivery_date=latest_delivery_date,
        has_order_rows=has_order_rows,
    )


def _plain_order_lines(order_rows):
    lines = []
    for row in order_rows or []:
        product = str(row.get("product", "")).strip()
        quantity = str(row.get("quantity", "")).strip()
        unit = str(row.get("unit", "DFP")).strip() or "DFP"
        if product:
            suffix = " (ny för er)" if is_yes(row.get("new_for_customer")) else ""
            lines.append(f"• {quantity} {unit} {product}{suffix}".replace("•  ", "• "))
    return lines


def _html_paragraphs(value):
    paragraphs = [part.strip() for part in re.split(r"\n\s*\n", str(value or "")) if part.strip()]
    rendered = []
    for part in paragraphs:
        safe = escape(part).replace(chr(10), "<br>")
        safe = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", safe)
        rendered.append(f'<p style="margin:0 0 16px;line-height:1.55">{safe}</p>')
    return "".join(rendered)


def _plain_text_markup(value):
    return re.sub(r"\*\*(.+?)\*\*", r"\1", str(value or ""))


def _personalize_intro_text(intro_text, greeting):
    intro = str(intro_text or "").strip()
    lines = intro.splitlines()
    for index, line in enumerate(lines):
        if not line.strip():
            continue
        if line.strip().rstrip(",").casefold() == "hej (namn)":
            lines[index] = greeting
            return "\n".join(lines).strip()
        break
    return f"{greeting}\n\n{intro}".strip()


def render_reminder_email(*, greeting_name, subject, intro_text, closing_text, order_rows,
                          product_sheet_url, stockfiller_url, sender,
                          product_sheet_label="Se Produktblad",
                          stockfiller_label="Beställ direkt via Stockfiller"):
    greeting = f"Hej {first_name(greeting_name)}," if first_name(greeting_name) else "Hej,"
    personalized_intro = _personalize_intro_text(intro_text, greeting)
    product_sheet_url = safe_http_url(product_sheet_url)
    stockfiller_url = safe_http_url(stockfiller_url)
    order_lines = _plain_order_lines(order_rows)

    order_html = ""
    if order_lines:
        items = "".join(f'<li style="margin:0 0 8px">{escape(line[2:])}</li>' for line in order_lines)
        order_html = f'<ul style="margin:0 0 20px;padding-left:22px">{items}</ul>'

    buttons = []
    button_style = (
        "display:inline-block;background:#1a1a2e;color:#ffffff;text-decoration:none;"
        "font-weight:700;padding:12px 16px;border-radius:10px;margin:0 8px 10px 0"
    )
    if product_sheet_url:
        buttons.append(
            f'<a href="{escape(product_sheet_url, quote=True)}" style="{button_style}">'
            f"{escape(str(product_sheet_label or 'Se Produktblad'))}</a>"
        )
    if stockfiller_url:
        buttons.append(
            f'<a href="{escape(stockfiller_url, quote=True)}" style="{button_style}">'
            f"{escape(str(stockfiller_label or 'Beställ direkt via Stockfiller'))}</a>"
        )

    sender_name = str(sender.get("name", "")).strip()
    sender_role = str(sender.get("role", "")).strip()
    sender_phone = str(sender.get("phone", "")).strip()
    signature_parts = [escape(value) for value in (sender_name, sender_role) if value]
    if sender_phone:
        signature_parts.append(f"📞 {escape(sender_phone)}")
    signature_parts.append('<a href="https://www.xn--polarbr-bxa.se/" style="color:#1a1a2e">🌐 polarbär.se</a>')

    html_body = (
        '<!doctype html><html><body style="margin:0;background:#f6f6f6">'
        '<div style="max-width:640px;margin:0 auto;background:#ffffff;padding:28px;'
        'font-family:Arial,sans-serif;color:#1a1a2e;font-size:16px">'
        f'{_html_paragraphs(personalized_intro)}{order_html}{_html_paragraphs(closing_text)}'
        f'<div style="margin:22px 0 14px">{"".join(buttons)}</div>'
        '<p style="margin:18px 0 8px">Vänliga hälsningar,</p>'
        f'<p style="margin:0;line-height:1.5">{"<br>".join(signature_parts)}</p>'
        '</div></body></html>'
    )

    text_parts = [_plain_text_markup(personalized_intro)]
    if order_lines:
        text_parts.extend(["", *order_lines])
    text_parts.extend(["", _plain_text_markup(closing_text).strip()])
    if product_sheet_url:
        text_parts.extend(["", f"{product_sheet_label}: {product_sheet_url}"])
    if stockfiller_url:
        text_parts.append(f"{stockfiller_label}: {stockfiller_url}")
    text_parts.extend(["", "Vänliga hälsningar,", sender_name, sender_role])
    if sender_phone:
        text_parts.append(f"📞 {sender_phone}")
    text_parts.append("🌐 polarbär.se – https://www.xn--polarbr-bxa.se/")

    return {
        "subject": str(subject or "").strip(),
        "html": html_body,
        "text": "\n".join(part for part in text_parts if part is not None).strip(),
    }


def render_email_proposal(**kwargs):
    return render_reminder_email(**kwargs)


def normalize_message_id(value):
    return str(value or "").strip().strip("<>")


def normalize_brevo_event(payload):
    raw_type = str(payload.get("event") or payload.get("type") or "").strip()
    event_type = raw_type.replace("_", "").replace("-", "").casefold()
    aliases = {
        "request": "sent", "requests": "sent", "sent": "sent", "delivered": "delivered",
        "opened": "opened", "uniqueopened": "opened", "click": "clicked",
        "clicks": "clicked", "clicked": "clicked",
        "hardbounce": "hardbounce", "hardbounces": "hardbounce",
        "softbounce": "softbounce", "softbounces": "softbounce", "blocked": "blocked",
        "invalid": "invalid", "spam": "spam", "complaint": "spam",
        "unsubscribed": "unsubscribed", "deferred": "deferred", "error": "error",
    }
    return aliases.get(event_type, raw_type.casefold() or "unknown")


def stockholm_now():
    return datetime.now(STOCKHOLM_TIMEZONE)


def stockholm_today():
    return stockholm_now().date()


def stockholm_time_text(value=None):
    """Return a stable local timestamp for Sheets, regardless of source timezone."""
    if value is None or value == "":
        parsed = stockholm_now()
    elif isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)) or (isinstance(value, str) and value.isdigit()):
        try:
            parsed = datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (ValueError, OSError, OverflowError):
            parsed = stockholm_now()
    else:
        text = str(value).strip()
        if not text:
            parsed = stockholm_now()
        else:
            iso_text = text.replace("Z", "+00:00")
            try:
                parsed = datetime.fromisoformat(iso_text)
            except ValueError:
                parsed = None
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
                    try:
                        parsed = datetime.strptime(text, fmt)
                        break
                    except ValueError:
                        pass
                if parsed is None:
                    parsed = stockholm_now()

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=STOCKHOLM_TIMEZONE)
    else:
        parsed = parsed.astimezone(STOCKHOLM_TIMEZONE)
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def brevo_event_time(payload):
    # Brevo's epoch values are unambiguous and should win over formatted UTC dates.
    value = payload.get("ts_event") or payload.get("ts") or payload.get("event_time") or payload.get("date")
    if isinstance(value, str):
        text = value.strip()
        has_explicit_timezone = bool(
            text.endswith("Z") or re.search(r"[+-]\d{2}:?\d{2}$", text)
        )
        if text and not text.isdigit() and not has_explicit_timezone:
            # Brevo documents and returns its formatted event dates in UTC.
            value = f"{text}+00:00"
    return stockholm_time_text(value)


def email_event_key(message_id, event_type, event_time, url="", email=""):
    """Create the same idempotency key for webhook and Brevo API representations."""
    canonical = json.dumps({
        "message_id": normalize_message_id(message_id),
        "event_type": str(event_type or "").strip().casefold(),
        "event_time": stockholm_time_text(event_time),
        "url": str(url or "").strip(),
    }, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return sha256(canonical.encode("utf-8")).hexdigest()


def brevo_event_key(payload):
    return email_event_key(
        payload.get("message-id") or payload.get("messageId") or payload.get("message_id"),
        normalize_brevo_event(payload),
        brevo_event_time(payload),
        payload.get("link") or payload.get("url"),
        payload.get("email"),
    )


def classify_clicked_url(url, product_sheet_url, stockfiller_url):
    clicked = str(url or "").strip()
    if clicked and clicked == str(product_sheet_url or "").strip():
        return "product_sheet_clicked"
    if clicked and clicked == str(stockfiller_url or "").strip():
        return "stockfiller_clicked"
    return "clicked"
