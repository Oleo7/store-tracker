"""Pure, deterministic contact-channel recommendations for planning actions."""

from __future__ import annotations

import re


PHONE_TRIGGERS = {
    "positive_dialogue_followup",
    "stockfiller_click_followup",
    "product_sheet_click_followup",
    "email_open_followup",
}

EMAIL_INTENT_TRIGGERS = {
    "stockfiller_click_followup",
    "product_sheet_click_followup",
    "email_open_followup",
}


def normalized_phone(value):
    """Return a tel-safe phone string, or an empty string when unavailable."""
    text = str(value or "").strip()
    if not text or not re.fullmatch(r"[+0-9()\s-]+", text):
        return ""
    if text.count("+") > 1 or ("+" in text and not text.startswith("+")):
        return ""
    normalized = re.sub(r"[()\s-]", "", text)
    digits = normalized[1:] if normalized.startswith("+") else normalized
    if not digits.isdigit() or not 7 <= len(digits) <= 15:
        return ""
    return normalized


def recommend_contact_channel(
    *, lifecycle, overdue_days=None, trigger_key="", has_human_contact=False,
    segment="", phone="", email_available=False, visible=True,
):
    """Apply business precedence first, then deterministic availability fallback."""
    if not visible:
        return None

    lifecycle = str(lifecycle or "").strip().casefold()
    trigger_key = str(trigger_key or "").strip().casefold()
    segment = str(segment or "").strip().upper()[:1]
    phone_tel = normalized_phone(phone)

    if trigger_key in EMAIL_INTENT_TRIGGERS:
        base = "phone"
        reason = f"{trigger_key}_phone"
    elif trigger_key == "positive_dialogue_followup" and segment == "A":
        base = "visit"
        reason = "positive_dialogue_segment_a_visit"
    elif lifecycle == "reactivation":
        base = "visit"
        reason = "reactivation_visit"
    elif lifecycle == "established":
        try:
            overdue = int(float(overdue_days))
        except (TypeError, ValueError):
            overdue = 0
        if overdue >= 31:
            base = "visit"
            reason = "established_overdue_visit"
        else:
            base = "phone"
            reason = "established_phone"
    elif lifecycle == "first_order":
        base = "phone"
        reason = "first_order_phone"
    elif trigger_key in PHONE_TRIGGERS:
        base = "phone"
        reason = f"{trigger_key}_phone"
    elif lifecycle == "prospect" and not has_human_contact:
        base = "visit"
        reason = "prospect_never_contacted_visit"
    else:
        base = "phone"
        reason = "relationship_phone"

    if base == "visit":
        recommended = "visit"
    elif phone_tel:
        recommended = "phone"
    elif email_available:
        recommended = "email"
        reason = f"{reason}_email_fallback"
    else:
        recommended = "visit"
        reason = f"{reason}_visit_fallback"

    return {
        "recommended_contact_type": recommended,
        "base_contact_type": base,
        "channel_reason_code": reason,
        "can_call": bool(phone_tel),
        "phone_tel": phone_tel,
    }
