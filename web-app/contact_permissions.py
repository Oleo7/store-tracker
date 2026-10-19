"""One chronology rule for source contact permissions; row position is not time."""
import hashlib
import json

REFUSALS = frozenset({"återkom ej", "kontakta ej", "ring ej"})
ALLOWANCES = frozenset({"kontakt tillåten"})


def permission_order(row, occurred_at):
    """A refusal wins a simultaneous conflict; identity only stabilizes evidence."""
    refusal = str(row.get("result") or "").strip().casefold() in REFUSALS
    identity = str(row.get("contact_id") or "") or hashlib.sha256(
        json.dumps({k: row.get(k, "") for k in (
            "customer_id", "date_time", "result", "comment", "contact_channel")},
            sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    return occurred_at, refusal, identity
