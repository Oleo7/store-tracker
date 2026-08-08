"""Deterministic dry-run planning for legacy CRM email customer identities."""

from __future__ import annotations

from collections import defaultdict
import unicodedata


def normalize(value):
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(char for char in text if not unicodedata.combining(char))
    return " ".join(text.replace("\xa0", " ").strip().casefold().split())


def _unique_index(customers, field):
    grouped = defaultdict(set)
    for customer in customers:
        key = normalize(customer.get(field))
        customer_id = str(customer.get("customer_id") or "").strip()
        if key and customer_id:
            grouped[key].add(customer_id)
    return grouped


def _summary(updates, examined, ambiguous, unresolved):
    return {
        "examined": examined,
        "backfilled": len(updates),
        "ambiguous": ambiguous,
        "unresolved": unresolved,
        "updates": updates,
    }


def plan_legacy_email_identity_backfill(
    *, customers, message_rows, recipient_rows, activity_rows
):
    """Return an idempotent plan; input rows are ``(sheet_row, dict)`` pairs."""
    by_number = _unique_index(customers, "customer_number")
    by_name = _unique_index(customers, "customer")
    message_updates = []
    message_examined = message_ambiguous = message_unresolved = 0
    email_ids = defaultdict(set)

    for row_index, row in message_rows:
        email_id = str(row.get("email_id") or "").strip()
        current_id = str(row.get("customer_id") or "").strip()
        if current_id:
            if email_id:
                email_ids[email_id].add(current_id)
            continue
        message_examined += 1
        number_key = normalize(row.get("customer_number"))
        name_key = normalize(row.get("customer"))
        candidates = by_number.get(number_key, set()) if number_key else set()
        basis = "customer_number"
        if not candidates and name_key:
            candidates = by_name.get(name_key, set())
            basis = "customer_name"
        if len(candidates) == 1:
            customer_id = next(iter(candidates))
            message_updates.append({
                "row_index": row_index,
                "customer_id": customer_id,
                "match_basis": basis,
                "email_id": email_id,
            })
            if email_id:
                email_ids[email_id].add(customer_id)
        elif len(candidates) > 1:
            message_ambiguous += 1
        else:
            message_unresolved += 1

    def dependent_plan(rows):
        updates = []
        examined = ambiguous = unresolved = 0
        for row_index, row in rows:
            if str(row.get("customer_id") or "").strip():
                continue
            examined += 1
            email_id = str(row.get("email_id") or "").strip()
            candidates = email_ids.get(email_id, set()) if email_id else set()
            if len(candidates) == 1:
                updates.append({
                    "row_index": row_index,
                    "customer_id": next(iter(candidates)),
                    "match_basis": "email_id",
                    "email_id": email_id,
                })
            elif len(candidates) > 1:
                ambiguous += 1
            else:
                unresolved += 1
        return _summary(updates, examined, ambiguous, unresolved)

    result = {
        "email_messages": _summary(
            message_updates,
            message_examined,
            message_ambiguous,
            message_unresolved,
        ),
        "email_recipients": dependent_plan(recipient_rows),
        "sales_activities": dependent_plan(activity_rows),
    }
    result["totals"] = {
        key: sum(result[sheet][key] for sheet in result)
        for key in ("examined", "backfilled", "ambiguous", "unresolved")
    }
    return result
