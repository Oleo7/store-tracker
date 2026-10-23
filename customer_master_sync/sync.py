from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field, replace
from datetime import date, datetime, timezone
from difflib import SequenceMatcher
import json
import os
import re
import sys
import unicodedata
import uuid
from typing import Any, Iterable

from crm_write_coordination import coordinated_write, guard_client

import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound


ORDER_REQUIRED_COLUMNS = [
    "Reference",
    "Order date",
    "Customer",
    "Customer number",
    "Address",
    "Number",
    "Postal code",
    "City",
    "buyerEmail",
]
CUSTOMER_REQUIRED_COLUMNS = [
    "customer",
    "cancelled_flag",
    "customer_id",
    "customer_number",
    "email_last_order",
    "address_google",
    "address_number_google",
    "postal_code_google",
    "city_google",
]
INTERNAL_CUSTOMERS = {"Polarbär - Inköp", "Spakallarn"}
INTERNAL_CUSTOMER_KEYS = set()
REVIEW_SHEET_NAME = "_customer_sync_review"
STATE_SHEET_NAME = "_stockfiller_sync_state"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
VALID_MODES = {"off", "dry_run", "apply"}
GLN_COLUMN = "stockfiller_gln"

REVIEW_COLUMNS = [
    "status",
    "identity_key",
    "master_customer",
    "customer_id",
    "customer_number",
    "source_customer_identifier",
    "matched_row",
    "existing_customer",
    "reason",
    "confidence",
    "last_seen_at",
    GLN_COLUMN,
    "existing_stockfiller_gln",
]


def clean(value: object) -> str:
    if value is None:
        return ""
    text = unicodedata.normalize("NFKC", str(value)).replace("\u00a0", " ").strip()
    return re.sub(r"\s+", " ", text)


def folded(value: object) -> str:
    text = unicodedata.normalize("NFKD", clean(value).casefold())
    text = "".join(character for character in text if not unicodedata.combining(character))
    text = re.sub(r"[^0-9a-z]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def name_key(value: object) -> str:
    return folded(value)


def number_key(value: object) -> str:
    return clean(value).casefold()


def is_gln_like(value: object) -> bool:
    """Shape check only: even an invalid GLN must not become an internal number."""
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    compact = re.sub(r"[\s-]", "", clean(value))
    return bool(re.fullmatch(r"[0-9]{13}", compact))


def normalize_gln(value: object) -> str:
    if isinstance(value, bool):
        return ""
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    compact = re.sub(r"[\s-]", "", clean(value))
    if not re.fullmatch(r"[0-9]{13}", compact) or not compact.strip("0"):
        return ""
    checksum = sum(int(digit) * (1 if index % 2 == 0 else 3) for index, digit in enumerate(compact[:12]))
    return compact if (checksum + int(compact[-1])) % 10 == 0 else ""


def order_gln(row: dict[str, Any]) -> tuple[str, str]:
    buyer = clean(row.get("Buyer number"))
    fallback = clean(row.get("Customer number"))
    primary = normalize_gln(row.get("Buyer number"))
    secondary = normalize_gln(row.get("Customer number"))
    if primary and secondary and primary != secondary:
        return primary, "Buyer number and Customer number contain conflicting GLNs"
    if buyer and not primary:
        return secondary, "invalid Buyer number GLN"
    if is_gln_like(row.get("Customer number")) and not secondary:
        return primary, "invalid Customer number GLN"
    return primary or secondary, ""


def is_organization_number_like(value: object) -> bool:
    """Return true for Swedish organization-number source identifiers."""
    compact = re.sub(r"\s+", "", clean(value))
    return bool(re.fullmatch(r"\d{6}-?\d{4}", compact))


def trusted_customer_number(value: object) -> str:
    """Return a CRM-safe number, excluding temporary external identities."""
    candidate = clean(value)
    if is_gln_like(value) or is_organization_number_like(candidate):
        return ""
    return candidate


def postal_key(value: object) -> str:
    return re.sub(r"\s+", "", clean(value).casefold())


def address_key(address: object, number: object) -> str:
    street, house = street_parts(address, number)
    return " ".join(part for part in (street, house) if part)


def house_key(value: object) -> str:
    value = re.sub(r"\s+", "", clean(value).casefold()).replace("–", "-")
    return re.sub(r"(?<=\d)-(?=[a-z])", "", value)


def street_parts(address: object, number: object) -> tuple[str, str]:
    street, house = clean(address), clean(number)
    embedded = re.fullmatch(r"(.+?)[ ,]+(\d+\s*[a-zA-Z]?(?:\s*[-/]\s*\d+\s*[a-zA-Z]?)?)", street)
    if embedded:
        embedded_house = house_key(embedded[2])
        if not house or house_key(house) == embedded_house:
            street, house = embedded[1], embedded[2]
    return folded(street), house_key(house)


def street_city_key(address: object, number: object, city: object) -> tuple[str, str, str] | None:
    street, house = street_parts(address, number)
    normalized_city = city_key(city)
    return (street, house, normalized_city) if street and house and normalized_city else None


def city_key(value: object) -> str:
    return folded(value)


INTERNAL_CUSTOMER_KEYS.update(name_key(customer) for customer in INTERNAL_CUSTOMERS)


def ratio(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    direct = SequenceMatcher(None, left, right).ratio()
    token = SequenceMatcher(
        None,
        " ".join(sorted(left.split())),
        " ".join(sorted(right.split())),
    ).ratio()
    return max(direct, token)


def normalize_mode(value: object, default: str = "dry_run") -> str:
    mode = clean(value or default).casefold()
    if mode not in VALID_MODES:
        options = ", ".join(sorted(VALID_MODES))
        raise ValueError(f"CRM_CUSTOMER_SYNC_MODE must be one of: {options}")
    return mode


def require_columns(headers: Iterable[str], required: Iterable[str], label: str) -> None:
    available = {clean(header) for header in headers}
    missing = [column for column in required if column not in available]
    if missing:
        raise ValueError(f"{label} is missing required columns: {', '.join(missing)}")


def values_to_dicts(values: list[list[Any]]) -> tuple[list[str], list[dict[str, Any]]]:
    if not values:
        return [], []
    headers = [clean(header) for header in values[0]]
    rows = []
    for value_row in values[1:]:
        padded = list(value_row) + [""] * max(0, len(headers) - len(value_row))
        rows.append(dict(zip(headers, padded)))
    return headers, rows


def _order_date(value: object) -> date:
    text = clean(value)
    try:
        return date.fromisoformat(text[:10])
    except (TypeError, ValueError):
        return date.min


def _full_address_key(
    address: object,
    number: object,
    postal: object,
    city: object,
) -> tuple[str, str, str] | None:
    combined_address = address_key(address, number)
    normalized_postal = postal_key(postal)
    normalized_city = city_key(city)
    if not street_city_key(address, number, city) or not normalized_postal:
        return None
    return combined_address, normalized_postal, normalized_city


@dataclass(frozen=True)
class CustomerSyncConfig:
    sheet_key: str
    google_credentials: dict[str, Any]

    @classmethod
    def from_env(cls) -> "CustomerSyncConfig":
        load_dotenv()
        sheet_key = clean(os.environ.get("SHEET_KEY"))
        credentials_raw = clean(os.environ.get("GOOGLE_CREDENTIALS"))
        missing = []
        if not sheet_key:
            missing.append("SHEET_KEY")
        if not credentials_raw:
            missing.append("GOOGLE_CREDENTIALS")
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
        try:
            credentials = json.loads(credentials_raw)
        except json.JSONDecodeError as exc:
            raise ValueError("GOOGLE_CREDENTIALS must be valid JSON") from exc
        return cls(sheet_key=sheet_key, google_credentials=credentials)


@dataclass(frozen=True)
class OrderMaster:
    identity_key: str
    customer_number: str
    source_customer_identifier: str
    master_name: str
    master_name_key: str
    order_date: str
    reference: str
    row_number: int
    order_count: int
    address: str
    address_number: str
    postal: str
    city: str
    email: str
    normalized_address: str
    normalized_postal: str
    normalized_city: str
    full_address: tuple[str, str, str] | None
    customer_id: str = ""
    stockfiller_gln: str = ""
    street_city: tuple[str, str, str] | None = None
    identity_error: str = ""
    order_row_numbers: tuple[int, ...] = ()


@dataclass(frozen=True)
class EnrichedCustomer:
    row_number: int
    customer: str
    customer_id: str
    customer_number: str
    email_last_order: str
    cancelled_flag: str
    name_key: str
    number_key: str
    normalized_address: str
    normalized_postal: str
    normalized_city: str
    full_address: tuple[str, str, str] | None
    stockfiller_gln: str = ""
    raw_gln: str = ""
    street_city: tuple[str, str, str] | None = None


@dataclass(frozen=True)
class CustomerDecision:
    status: str
    identity_key: str
    master_customer: str
    customer_number: str
    source_customer_identifier: str
    latest_email: str
    order_date: str
    order_reference: str
    matched_row: int | None = None
    existing_customer: str = ""
    existing_customer_id: str = ""
    existing_customer_number: str = ""
    existing_email: str = ""
    existing_cancelled_flag: str = ""
    matched_by: str = ""
    reason: str = ""
    confidence: float = 1.0
    stockfiller_gln: str = ""
    existing_stockfiller_gln: str = ""
    order_row_numbers: tuple[int, ...] = ()
    source_address: str = ""
    source_address_number: str = ""
    source_postal: str = ""
    source_city: str = ""

    @property
    def backfills_gln(self) -> bool:
        return bool(self.matched_row and self.stockfiller_gln and not self.existing_stockfiller_gln
                    and self.status not in {"needs_review", "ignored_internal"})

    @property
    def changes_name(self) -> bool:
        return bool(
            self.matched_row
            and clean(self.existing_customer) != clean(self.master_customer)
            and self.status not in {"needs_review", "ignored_internal"}
        )

    @property
    def backfills_customer_number(self) -> bool:
        return bool(
            self.matched_row
            and not clean(self.existing_customer_number)
            and clean(self.customer_number)
            and self.status not in {"needs_review", "ignored_internal"}
        )

    @property
    def changes_email(self) -> bool:
        return bool(
            self.matched_row
            and clean(self.existing_email) != clean(self.latest_email)
            and self.status not in {"needs_review", "ignored_internal"}
        )


@dataclass(frozen=True)
class CustomerSyncResult:
    mode: str
    exact_matches: int
    updated_names: int
    backfilled_customer_numbers: int
    appended_customers: int
    updated_emails: int
    needs_review: int
    ignored_internal: int
    ignored_gln_identifiers: int
    ignored_organization_identifiers: int
    decisions: tuple[CustomerDecision, ...] = field(default_factory=tuple, repr=False)

    @property
    def backfilled_glns(self) -> int:
        return sum(decision.backfills_gln for decision in self.decisions)

    @property
    def applied(self) -> bool:
        return self.mode == "apply"

    def state_values(self, run_at: datetime | None = None) -> dict[str, str]:
        timestamp = (run_at or datetime.now(timezone.utc)).astimezone(timezone.utc)
        values = {
            "customer_sync_last_run_at": timestamp.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "customer_sync_last_mode": self.mode,
            "customer_sync_exact_matches": str(self.exact_matches),
            "customer_sync_updated_names": str(self.updated_names),
            "customer_sync_backfilled_customer_numbers": str(self.backfilled_customer_numbers),
            "customer_sync_appended_customers": str(self.appended_customers),
            "customer_sync_updated_emails": str(self.updated_emails),
            "customer_sync_backfilled_glns": str(self.backfilled_glns),
            "customer_sync_needs_review": str(self.needs_review),
            "customer_sync_ignored_internal": str(self.ignored_internal),
            "customer_sync_ignored_gln_identifiers": str(
                self.ignored_gln_identifiers
            ),
            "customer_sync_ignored_organization_identifiers": str(
                self.ignored_organization_identifiers
            ),
        }
        if self.applied:
            values["customer_sync_last_successful_at"] = values["customer_sync_last_run_at"]
        return values


def build_order_masters(order_rows: list[dict[str, Any]]) -> list[OrderMaster]:
    """Join order identities without dropping conflicting product-row evidence.

    Strong identifiers connect history first; weaker name/address bridges need
    consistent evidence. The complete resulting group is checked for conflicts
    before it can authorize any write.
    """
    if not order_rows:
        return []
    require_columns(order_rows[0].keys(), ORDER_REQUIRED_COLUMNS, "order_rows")
    logical = defaultdict(list)
    for row_number, row in enumerate(order_rows, 2):
        if clean(row.get("Customer")):
            logical[clean(row.get("Reference")) or f"row:{row_number}"].append((row_number, row))
    records = list(logical.values())
    parents = list(range(len(records)))

    def root(index: int) -> int:
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    def join(left: int, right: int) -> None:
        parents[root(right)] = root(left)

    strong = {}
    weak = defaultdict(list)
    for index, rows in enumerate(records):
        for _position, row in rows:
            if name_key(row.get("Customer")) in INTERNAL_CUSTOMER_KEYS:
                continue
            gln, _error = order_gln(row)
            for kind, value in (("id", clean(row.get("customer_id"))),
                                ("number", number_key(trusted_customer_number(row.get("Customer number")))),
                                ("gln", gln)):
                if value:
                    identity = (kind, value)
                    if identity in strong:
                        join(index, strong[identity])
                    strong[identity] = index
        # A weak bridge needs BOTH the exact name and a complete physical
        # street/house/city identity. Never bridge by name or city alone.
        row = rows[-1][1]
        address = street_city_key(row.get("Address"), row.get("Number"), row.get("City"))
        if address:
            weak[(name_key(row.get("Customer")), address)].append(index)

    for indexes in weak.values():
        rows = [row for index in indexes for _position, row in records[index]]
        evidence = [
            {clean(row.get("customer_id")) for row in rows} - {""},
            {number_key(trusted_customer_number(row.get("Customer number"))) for row in rows} - {""},
            {order_gln(row)[0] for row in rows} - {""},
            {postal_key(row.get("Postal code")) for row in rows} - {""},
        ]
        if all(len(values) <= 1 for values in evidence):
            for index in indexes[1:]:
                join(indexes[0], index)

    grouped = defaultdict(list)
    for index, rows in enumerate(records):
        grouped[root(index)].extend(rows)
    masters = []
    for rows in grouped.values():
        row_number, row = max(rows, key=lambda item: (_order_date(item[1].get("Order date")), item[0]))
        glns = {order_gln(source)[0] for _position, source in rows} - {""}
        ids = {clean(source.get("customer_id")) for _position, source in rows} - {""}
        numbers = {number_key(trusted_customer_number(source.get("Customer number"))) for _position, source in rows} - {""}
        errors = {order_gln(source)[1] for _position, source in rows} - {""}
        for label, values in (("customer_id", ids), ("customer_number", numbers), ("GLN", glns)):
            if len(values) > 1:
                errors.add(f"conflicting {label} values in order history; manual identity mapping required")
        # A shared GLN with different names AND physical stores is not a rename.
        names = {name_key(source.get("Customer")) for _position, source in rows}
        addresses = {street_city_key(source.get("Address"), source.get("Number"), source.get("City"))
                     for _position, source in rows} - {None}
        if glns and len(names) > 1 and len(addresses) > 1:
            errors.add("GLN is shared by conflicting store names and addresses")
        references = defaultdict(list)
        for position, source in rows:
            references[clean(source.get("Reference")) or f"row:{position}"].append(source)
        for sources in references.values():
            for field, normalize in (("Customer", name_key), ("Postal code", postal_key)):
                if len({normalize(source.get(field)) for source in sources} - {""}) > 1:
                    errors.add("order reference has conflicting customer metadata")
            if len({street_city_key(source.get("Address"), source.get("Number"), source.get("City")) for source in sources} - {None}) > 1:
                errors.add("order reference has conflicting address metadata")
        gln = next(iter(glns)) if len(glns) == 1 else ""
        cid = next(iter(ids)) if len(ids) == 1 else ""
        number = next((trusted_customer_number(source.get("Customer number")) for _position, source in reversed(rows)
                       if trusted_customer_number(source.get("Customer number"))), "")
        customer = clean(row.get("Customer"))
        address = street_city_key(row.get("Address"), row.get("Number"), row.get("City"))
        identity = (f"id:{cid}" if cid else f"number:{number_key(number)}" if number else f"gln:{gln}" if gln
                    else f"fallback:{name_key(customer)}|{'|'.join(address or ('', '', ''))}")
        masters.append(OrderMaster(
            identity_key=identity, customer_number=number,
            source_customer_identifier=clean(row.get("Customer number")), master_name=customer,
            master_name_key=name_key(customer), order_date=clean(row.get("Order date")),
            reference=clean(row.get("Reference")), row_number=row_number, order_count=len(references),
            address=clean(row.get("Address")), address_number=clean(row.get("Number")),
            postal=clean(row.get("Postal code")), city=clean(row.get("City")), email=clean(row.get("buyerEmail")),
            normalized_address=address_key(row.get("Address"), row.get("Number")),
            normalized_postal=postal_key(row.get("Postal code")), normalized_city=city_key(row.get("City")),
            full_address=_full_address_key(row.get("Address"), row.get("Number"), row.get("Postal code"), row.get("City")),
            customer_id=cid, stockfiller_gln=gln, street_city=address,
            identity_error="; ".join(sorted(errors)), order_row_numbers=tuple(sorted(position for position, _source in rows)),
        ))
    return sorted(masters, key=lambda master: (master.master_name.casefold(), master.identity_key))


def build_enriched_customers(customer_rows: list[dict[str, Any]]) -> list[EnrichedCustomer]:
    if not customer_rows:
        return []
    require_columns(
        customer_rows[0].keys() if customer_rows else [],
        CUSTOMER_REQUIRED_COLUMNS,
        "customers_enriched",
    )
    customers = []
    for row_number, row in enumerate(customer_rows, start=2):
        customer = clean(row.get("customer"))
        if not customer:
            continue
        customers.append(
            EnrichedCustomer(
                row_number=row_number,
                customer=customer,
                customer_id=clean(row.get("customer_id")),
                customer_number=clean(row.get("customer_number")),
                email_last_order=clean(row.get("email_last_order")),
                cancelled_flag=clean(row.get("cancelled_flag")),
                name_key=name_key(customer),
                number_key=number_key(
                    trusted_customer_number(row.get("customer_number"))
                ),
                normalized_address=address_key(
                    row.get("address_google"),
                    row.get("address_number_google"),
                ),
                normalized_postal=postal_key(row.get("postal_code_google")),
                normalized_city=city_key(row.get("city_google")),
                full_address=_full_address_key(
                    row.get("address_google"),
                    row.get("address_number_google"),
                    row.get("postal_code_google"),
                    row.get("city_google"),
                ),
                stockfiller_gln=normalize_gln(row.get(GLN_COLUMN)),
                raw_gln=clean(row.get(GLN_COLUMN)),
                street_city=street_city_key(row.get("address_google"), row.get("address_number_google"), row.get("city_google")),
            )
        )
    return customers


def _decision(
    status: str,
    master: OrderMaster,
    matched: EnrichedCustomer | None = None,
    *,
    matched_by: str = "",
    reason: str,
    confidence: float = 1.0,
) -> CustomerDecision:
    return CustomerDecision(
        status=status,
        identity_key=master.identity_key,
        master_customer=master.master_name,
        customer_number=master.customer_number,
        source_customer_identifier=master.source_customer_identifier,
        latest_email=master.email,
        order_date=master.order_date,
        order_reference=master.reference,
        matched_row=matched.row_number if matched else None,
        existing_customer=matched.customer if matched else "",
        existing_customer_id=matched.customer_id if matched else "",
        existing_customer_number=matched.customer_number if matched else "",
        existing_email=matched.email_last_order if matched else "",
        existing_cancelled_flag=matched.cancelled_flag if matched else "",
        matched_by=matched_by,
        reason=reason,
        confidence=round(confidence, 4),
        stockfiller_gln=master.stockfiller_gln,
        existing_stockfiller_gln=matched.raw_gln if matched else "",
        order_row_numbers=master.order_row_numbers,
        source_address=master.address, source_address_number=master.address_number,
        source_postal=master.postal, source_city=master.city,
    )


def _candidate_score(master: OrderMaster, customer: EnrichedCustomer) -> float:
    name_score = ratio(master.master_name_key, customer.name_key)
    if (
        master.stockfiller_gln and customer.stockfiller_gln
        and master.stockfiller_gln != customer.stockfiller_gln
        and master.street_city and customer.street_city
        and master.street_city != customer.street_city
        and master.master_name_key != customer.name_key and name_score < 0.78
    ):
        return 0.0  # Different external locations and addresses outweigh a shared chain/city name.
    best = name_score * 0.55
    # Strong name similarity is sufficient to block an automatic append even
    # when address enrichment is missing or differs. Fuzzy matching remains
    # review-only and never performs a rename.
    if name_score >= 0.78:
        best = max(best, name_score)
    if master.full_address and master.full_address == customer.full_address:
        return 1.0
    if master.street_city and master.street_city == customer.street_city:
        return 1.0  # Without postcode this is review-only unless the name also agrees.
    if (
        master.normalized_address
        and master.normalized_postal
        and master.normalized_address == customer.normalized_address
        and master.normalized_postal == customer.normalized_postal
    ):
        best = max(best, 0.94)
    if (
        master.normalized_postal
        and master.normalized_city
        and master.normalized_postal == customer.normalized_postal
        and master.normalized_city == customer.normalized_city
    ):
        best = max(
            best,
            0.55
            + 0.25 * ratio(master.normalized_address, customer.normalized_address)
            + 0.20 * name_score,
        )
    elif master.normalized_postal and master.normalized_postal == customer.normalized_postal:
        best = max(
            best,
            0.45
            + 0.25 * ratio(master.normalized_address, customer.normalized_address)
            + 0.30 * name_score,
        )
    elif master.normalized_city and master.normalized_city == customer.normalized_city:
        best = max(
            best,
            0.30
            + 0.30 * ratio(master.normalized_address, customer.normalized_address)
            + 0.40 * name_score,
        )
    return best


def _safe_match_decision(
    master: OrderMaster,
    matched: EnrichedCustomer,
    *,
    matched_by: str,
    names: dict[str, list[EnrichedCustomer]],
) -> CustomerDecision:
    if not matched.customer_id:
        return _decision("needs_review", master, matched, matched_by=matched_by,
                         reason="matched customer is missing customer_id")
    if matched.raw_gln and not matched.stockfiller_gln:
        return _decision("needs_review", master, matched, matched_by=matched_by,
                         reason="matched customer has invalid stockfiller_gln")
    if master.stockfiller_gln and matched.stockfiller_gln and master.stockfiller_gln != matched.stockfiller_gln:
        return _decision("needs_review", master, matched, matched_by=matched_by,
                         reason="matched customer has a conflicting GLN; external mapping must be reviewed")
    if matched_by == "stockfiller_gln" and ratio(master.master_name_key, matched.name_key) < 0.78:
        return _decision("needs_review", master, matched, matched_by=matched_by,
                         reason="GLN may be shared or the name change is unclear; manual review required")
    if (
        master.customer_number
        and matched.customer_number
        and number_key(master.customer_number) != matched.number_key
    ):
        return _decision(
            "needs_review",
            master,
            matched,
            matched_by=matched_by,
            reason="matched customer has a conflicting customer_number",
        )

    if matched_by not in {"customer_id", "customer_number", "stockfiller_gln"}:
        if (master.street_city and matched.street_city and master.street_city != matched.street_city
                and master.stockfiller_gln):
            return _decision("needs_review", master, matched, matched_by=matched_by,
                             reason="name/address evidence conflicts with the proposed GLN assignment")
        if master.normalized_postal and matched.normalized_postal and master.normalized_postal != matched.normalized_postal:
            return _decision("needs_review", master, matched, matched_by=matched_by,
                             reason="conflicting postcodes on a weak identity match")
        if master.master_name_key != matched.name_key and ratio(master.master_name_key, matched.name_key) < 0.78:
            return _decision("needs_review", master, matched, matched_by=matched_by,
                             reason="address alone cannot authorize an unclear name change")
    changes_name = clean(matched.customer) != clean(master.master_name)
    if changes_name and len(names.get(matched.name_key, [])) != 1:
        return _decision(
            "needs_review",
            master,
            matched,
            matched_by=matched_by,
            reason="existing customer name is not unique; history rename would be ambiguous",
        )
    target_name_matches = [
        customer
        for customer in names.get(master.master_name_key, [])
        if customer.row_number != matched.row_number
    ]
    if changes_name and target_name_matches:
        return _decision(
            "needs_review",
            master,
            matched,
            matched_by=matched_by,
            reason="proposed master name already belongs to another customer row",
        )

    if matched.name_key == master.master_name_key:
        status = "safe_update_case_spacing" if changes_name else "exact_match"
        reason = (
            "same normalized name; display differs"
            if changes_name
            else f"unique {matched_by} match"
        )
    else:
        status = "safe_update_name"
        reason = f"unique {matched_by} match"
    return _decision(
        status,
        master,
        matched,
        matched_by=matched_by,
        reason=reason,
    )


def _classify(
    master: OrderMaster,
    customers: list[EnrichedCustomer],
    numbers: dict[str, list[EnrichedCustomer]],
    names: dict[str, list[EnrichedCustomer]],
    addresses: dict[tuple[str, str, str], list[EnrichedCustomer]],
    ids: dict[str, list[EnrichedCustomer]],
    glns: dict[str, list[EnrichedCustomer]],
) -> CustomerDecision:
    if master.master_name_key in INTERNAL_CUSTOMER_KEYS:
        return _decision(
            "ignored_internal",
            master,
            reason="internal customer ignored by rule",
        )

    if master.identity_error:
        return _decision("needs_review", master, reason=master.identity_error)
    strong_matches = []
    for label, value, index in (("customer_id", master.customer_id, ids),
                                ("customer_number", number_key(master.customer_number), numbers),
                                ("stockfiller_gln", master.stockfiller_gln, glns)):
        matches = index.get(value, []) if value else []
        if len(matches) > 1:
            return _decision("needs_review", master, matches[0], matched_by=label,
                             reason=f"{label} exists on multiple customer rows")
        if matches:
            strong_matches.append((label, matches[0]))
        elif label == "customer_id" and value:
            return _decision("needs_review", master, reason="order has an unknown customer_id; never rebind by name")
    if len({customer.row_number for _label, customer in strong_matches}) > 1:
        return _decision("needs_review", master, reason="internal identifiers and GLN resolve to conflicting customers")
    if strong_matches:
        label, matched = strong_matches[0]
        return _safe_match_decision(master, matched, matched_by=label, names=names)

    name_matches = names.get(master.master_name_key, [])
    if len(name_matches) > 1:
        return _decision(
            "needs_review",
            master,
            name_matches[0],
            matched_by="normalized_name",
            reason="normalized customer name exists on multiple customer rows",
        )
    if len(name_matches) == 1:
        return _safe_match_decision(
            master,
            name_matches[0],
            matched_by="normalized_name",
            names=names,
        )

    if master.full_address:
        address_matches = addresses.get(master.full_address, [])
        if len(address_matches) > 1:
            return _decision(
                "needs_review",
                master,
                address_matches[0],
                matched_by="full_address",
                reason="full order address exists on multiple customer rows",
            )
        if len(address_matches) == 1:
            return _safe_match_decision(
                master,
                address_matches[0],
                matched_by="full_address",
                names=names,
            )

    scored = sorted(
        (
            (_candidate_score(master, customer), customer)
            for customer in customers
        ),
        key=lambda item: item[0],
        reverse=True,
    )
    plausible = [(score, customer) for score, customer in scored if score >= 0.62]
    if plausible:
        score, candidate = plausible[0]
        return _decision(
            "needs_review",
            master,
            candidate,
            matched_by="possible_duplicate",
            reason="possible duplicate; fuzzy matching never writes automatically",
            confidence=score,
        )

    if not master.customer_number and not master.full_address and not (master.stockfiller_gln and master.street_city):
        return _decision(
            "needs_review",
            master,
            reason="missing trusted number or GLN with street/house/city, and no complete address; cannot safely append",
            confidence=0.0,
        )

    return _decision(
        "new_customer",
        master,
        reason="no exact name, id, address, or plausible duplicate candidate",
    )


def _block_collisions(decisions: list[CustomerDecision]) -> list[CustomerDecision]:
    by_target: dict[int, list[int]] = defaultdict(list)
    new_by_name: dict[str, list[int]] = defaultdict(list)
    for index, decision in enumerate(decisions):
        if decision.status in {"needs_review", "ignored_internal"}:
            continue
        if decision.matched_row:
            by_target[decision.matched_row].append(index)
        elif decision.status == "new_customer":
            new_by_name[name_key(decision.master_customer)].append(index)

    blocked = set()
    for indexes in by_target.values():
        if len(indexes) > 1:
            blocked.update(indexes)
    for indexes in new_by_name.values():
        if len(indexes) > 1:
            blocked.update(indexes)

    output = list(decisions)
    for index in blocked:
        output[index] = replace(
            output[index],
            status="needs_review",
            reason="multiple order identities resolve to the same customer",
        )
    return output


def plan_customer_sync(
    order_rows: list[dict[str, Any]],
    customer_rows: list[dict[str, Any]],
) -> list[CustomerDecision]:
    masters = build_order_masters(order_rows)
    customers = build_enriched_customers(customer_rows)
    customer_ids: dict[str, list[EnrichedCustomer]] = defaultdict(list)
    numbers: dict[str, list[EnrichedCustomer]] = defaultdict(list)
    names: dict[str, list[EnrichedCustomer]] = defaultdict(list)
    addresses: dict[tuple[str, str, str], list[EnrichedCustomer]] = defaultdict(list)
    glns: dict[str, list[EnrichedCustomer]] = defaultdict(list)
    for customer in customers:
        if customer.customer_id:
            customer_ids[customer.customer_id].append(customer)
        if customer.number_key:
            numbers[customer.number_key].append(customer)
        if customer.stockfiller_gln:
            glns[customer.stockfiller_gln].append(customer)
        names[customer.name_key].append(customer)
        if customer.full_address:
            addresses[customer.full_address].append(customer)

    conflicted_rows = {
        customer.row_number
        for index in (customer_ids, numbers, glns) for matches in index.values()
        if len(matches) > 1
        for customer in matches
    }

    decisions = [
        _classify(master, customers, numbers, names, addresses, customer_ids, glns)
        for master in masters
    ]
    # Safe GLN complements also supply negative evidence for other stores in
    # this snapshot. Do not make the outcome depend on customer/order sorting.
    proposals = defaultdict(set)
    gln_targets = defaultdict(set)
    for decision in _block_collisions(decisions):
        if decision.backfills_gln and decision.matched_row not in conflicted_rows:
            proposals[decision.matched_row].add(decision.stockfiller_gln)
            gln_targets[decision.stockfiller_gln].add(decision.matched_row)
    inferred = {row: next(iter(values)) for row, values in proposals.items()
                if len(values) == 1 and len(gln_targets[next(iter(values))]) == 1}
    if inferred:
        customers = [replace(customer, stockfiller_gln=inferred.get(customer.row_number, customer.stockfiller_gln))
                     for customer in customers]
        by_row = {customer.row_number: customer for customer in customers}
        names = {key: [by_row[c.row_number] for c in values] for key, values in names.items()}
        numbers = {key: [by_row[c.row_number] for c in values] for key, values in numbers.items()}
        customer_ids = {key: [by_row[c.row_number] for c in values] for key, values in customer_ids.items()}
        addresses = {key: [by_row[c.row_number] for c in values] for key, values in addresses.items()}
        glns = defaultdict(list)
        for customer in customers:
            if customer.stockfiller_gln:
                glns[customer.stockfiller_gln].append(customer)
        decisions = [_classify(master, customers, numbers, names, addresses, customer_ids, glns) for master in masters]
    conflicted_rows.update({
        customer.row_number
        for index in (customer_ids, numbers, glns) for matches in index.values()
        if len(matches) > 1
        for customer in matches
    })
    decisions = [
        replace(
            decision,
            status="needs_review",
            reason="customer_id, customer_number or GLN exists on multiple customer rows",
        )
        if decision.matched_row in conflicted_rows
        else decision
        for decision in decisions
    ]
    return _block_collisions(decisions)


def build_result(mode: str, decisions: list[CustomerDecision]) -> CustomerSyncResult:
    return CustomerSyncResult(
        mode=mode,
        exact_matches=sum(decision.status == "exact_match" for decision in decisions),
        updated_names=sum(decision.changes_name for decision in decisions),
        backfilled_customer_numbers=sum(
            decision.backfills_customer_number for decision in decisions
        ),
        appended_customers=sum(
            decision.status == "new_customer" for decision in decisions
        ),
        updated_emails=sum(decision.changes_email for decision in decisions),
        needs_review=sum(decision.status == "needs_review" for decision in decisions),
        ignored_internal=sum(
            decision.status == "ignored_internal" for decision in decisions
        ),
        ignored_gln_identifiers=sum(
            is_gln_like(decision.source_customer_identifier)
            for decision in decisions
        ),
        ignored_organization_identifiers=sum(
            is_organization_number_like(
                decision.source_customer_identifier
            )
            for decision in decisions
        ),
        decisions=tuple(decisions),
    )


def _column_name(zero_based_index: int) -> str:
    number = zero_based_index + 1
    name = ""
    while number:
        number, remainder = divmod(number - 1, 26)
        name = chr(65 + remainder) + name
    return name


def _batch_cell_updates(worksheet, updates: list[tuple[int, int, Any]]) -> None:
    if not updates:
        return
    data = [
        {
            "range": (
                f"{_column_name(column_index)}{row_number}:"
                f"{_column_name(column_index)}{row_number}"
            ),
            "values": [[value]],
        }
        for row_number, column_index, value in updates
    ]
    worksheet.batch_update(data, value_input_option="RAW")


def _history_rename_targets(
    spreadsheet,
    renames: list[CustomerDecision],
) -> list[tuple[Any, list[tuple[int, int, Any]], list[tuple[int, str]]]]:
    rename_by_key = {
        name_key(decision.existing_customer): decision.master_customer
        for decision in renames
    }
    targets = []
    for sheet_name in (
        "sales_activities",
        "email_messages",
        "email_recipients",
        "planned_activities",
    ):
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except WorksheetNotFound as exc:
            if sheet_name == "planned_activities":
                continue
            raise ValueError(
                f"{sheet_name} is required to preserve history during a customer rename"
            ) from exc
        values = worksheet.get_all_values()
        if not values:
            raise ValueError(f"{sheet_name} is empty; cannot preserve customer history")
        headers = [clean(header) for header in values[0]]
        require_columns(headers, ["customer"], sheet_name)
        customer_index = headers.index("customer")
        updates = []
        expected = []
        for row_number, row in enumerate(values[1:], start=2):
            current = row[customer_index] if customer_index < len(row) else ""
            replacement = rename_by_key.get(name_key(current))
            if replacement is not None and clean(current) != clean(replacement):
                updates.append((row_number, customer_index, replacement))
                expected.append((row_number, replacement))
        targets.append((worksheet, updates, expected))
    return targets


def _apply_history_renames(
    spreadsheet,
    renames: list[CustomerDecision],
) -> list[tuple[Any, int, str]]:
    verification = []
    for worksheet, updates, expected in _history_rename_targets(spreadsheet, renames):
        _batch_cell_updates(worksheet, updates)
        verification.extend((worksheet, row_number, value) for row_number, value in expected)
    return verification


def _apply_planning_snapshots(
    spreadsheet,
    decisions: list[CustomerDecision],
) -> list[tuple[Any, int, str]]:
    by_customer_id = {
        decision.existing_customer_id: decision
        for decision in decisions
        if (
            decision.existing_customer_id
            and decision.status not in {"needs_review", "ignored_internal"}
        )
    }
    if not by_customer_id:
        return []
    try:
        worksheet = spreadsheet.worksheet("planned_activities")
    except WorksheetNotFound:
        return []
    values = worksheet.get_all_values()
    if not values:
        return []
    headers = [clean(header) for header in values[0]]
    required = {"customer_id", "customer", "customer_number", "customer_key"}
    if not required.issubset(headers):
        raise ValueError(
            "planned_activities is missing customer snapshot columns"
        )
    indexes = {column: headers.index(column) for column in required}
    updates = []
    verification = []
    for row_number, row in enumerate(values[1:], start=2):
        customer_id = clean(
            row[indexes["customer_id"]]
            if indexes["customer_id"] < len(row) else ""
        )
        decision = by_customer_id.get(customer_id)
        if not decision:
            continue
        customer_number = decision.customer_number or decision.existing_customer_number
        snapshot = {
            "customer": decision.master_customer,
            "customer_number": customer_number,
            "customer_key": (
                number_key(customer_number)
                or name_key(decision.master_customer)
            ),
        }
        for column, value in snapshot.items():
            current = row[indexes[column]] if indexes[column] < len(row) else ""
            if clean(current) != clean(value):
                updates.append((row_number, indexes[column], value))
        verification.append((worksheet, row_number, decision.master_customer))
    _batch_cell_updates(worksheet, updates)
    return verification


def _verify_history(verification: list[tuple[Any, int, str]]) -> None:
    by_worksheet: dict[int, tuple[Any, dict[int, str]]] = {}
    for worksheet, row_number, value in verification:
        key = id(worksheet)
        if key not in by_worksheet:
            by_worksheet[key] = (worksheet, {})
        by_worksheet[key][1][row_number] = value
    for worksheet, expected in by_worksheet.values():
        values = worksheet.get_all_values()
        headers = [clean(header) for header in values[0]]
        customer_index = headers.index("customer")
        for row_number, value in expected.items():
            actual_row = values[row_number - 1] if row_number <= len(values) else []
            actual = actual_row[customer_index] if customer_index < len(actual_row) else ""
            if clean(actual) != clean(value):
                raise RuntimeError(
                    f"history verification failed at row {row_number}: "
                    f"expected {value!r}, got {actual!r}"
                )


def _apply_customer_changes(
    worksheet,
    headers: list[str],
    decisions: list[CustomerDecision],
) -> None:
    customer_index = headers.index("customer")
    customer_id_index = headers.index("customer_id")
    number_index = headers.index("customer_number")
    email_index = headers.index("email_last_order")
    updates = []
    for decision in decisions:
        if decision.status in {"needs_review", "ignored_internal", "new_customer"}:
            continue
        if decision.changes_name:
            updates.append((decision.matched_row, customer_index, decision.master_customer))
        if decision.backfills_customer_number:
            updates.append(
                (decision.matched_row, number_index, decision.customer_number)
            )
        if decision.changes_email:
            updates.append((decision.matched_row, email_index, decision.latest_email))
        if decision.backfills_gln:
            updates.append((decision.matched_row, headers.index(GLN_COLUMN), decision.stockfiller_gln))
    _batch_cell_updates(worksheet, updates)

    append_values = []
    for decision in decisions:
        if decision.status != "new_customer":
            continue
        row = [""] * len(headers)
        row[customer_index] = decision.master_customer
        row[customer_id_index] = str(uuid.uuid4())
        row[number_index] = decision.customer_number
        row[email_index] = decision.latest_email
        if decision.stockfiller_gln:
            row[headers.index(GLN_COLUMN)] = decision.stockfiller_gln
        for key, value in (("address_google", decision.source_address), ("address_number_google", decision.source_address_number),
                           ("postal_code_google", decision.source_postal), ("city_google", decision.source_city)):
            row[headers.index(key)] = value
        append_values.append(row)
    if append_values:
        worksheet.append_rows(append_values, value_input_option="RAW")


def _verify_customer_changes(
    worksheet,
    headers: list[str],
    decisions: list[CustomerDecision],
) -> None:
    values = worksheet.get_all_values()
    actual_headers = [clean(header) for header in values[0]] if values else []
    if actual_headers != headers:
        raise RuntimeError("customers_enriched headers changed during customer sync")
    customer_index = headers.index("customer")
    customer_id_index = headers.index("customer_id")
    number_index = headers.index("customer_number")
    email_index = headers.index("email_last_order")

    for decision in decisions:
        if decision.status in {"needs_review", "ignored_internal"}:
            continue
        if decision.status == "new_customer":
            matches = []
            for row in values[1:]:
                customer = row[customer_index] if customer_index < len(row) else ""
                number = row[number_index] if number_index < len(row) else ""
                if decision.stockfiller_gln:
                    matched = normalize_gln(row[headers.index(GLN_COLUMN)] if headers.index(GLN_COLUMN) < len(row) else "") == decision.stockfiller_gln
                elif decision.customer_number:
                    matched = number_key(number) == number_key(decision.customer_number)
                else:
                    matched = name_key(customer) == name_key(decision.master_customer)
                if matched:
                    matches.append(row)
            if len(matches) != 1:
                raise RuntimeError(
                    f"new customer verification found {len(matches)} rows for "
                    f"{decision.master_customer!r}"
                )
            actual_row = matches[0]
        else:
            if not decision.matched_row or decision.matched_row > len(values):
                raise RuntimeError(
                    f"matched customer row {decision.matched_row} disappeared"
                )
            actual_row = values[decision.matched_row - 1]

        actual_customer = (
            actual_row[customer_index] if customer_index < len(actual_row) else ""
        )
        actual_number = (
            actual_row[number_index] if number_index < len(actual_row) else ""
        )
        actual_customer_id = (
            actual_row[customer_id_index]
            if customer_id_index < len(actual_row) else ""
        )
        if not clean(actual_customer_id):
            raise RuntimeError(
                f"customer_id verification failed for {decision.master_customer!r}"
            )
        if decision.existing_customer_id and clean(actual_customer_id) != decision.existing_customer_id:
            raise RuntimeError(f"existing customer_id changed for {decision.master_customer!r}")
        if decision.stockfiller_gln:
            actual_gln = actual_row[headers.index(GLN_COLUMN)] if headers.index(GLN_COLUMN) < len(actual_row) else ""
            if normalize_gln(actual_gln) != decision.stockfiller_gln:
                raise RuntimeError(f"GLN verification failed for {decision.master_customer!r}")
        actual_email = (
            actual_row[email_index] if email_index < len(actual_row) else ""
        )
        if clean(actual_customer) != clean(decision.master_customer):
            raise RuntimeError(
                f"customer verification failed for {decision.master_customer!r}"
            )
        if decision.customer_number and (
            number_key(actual_number) != number_key(decision.customer_number)
        ):
            raise RuntimeError(
                f"customer_number verification failed for {decision.master_customer!r}"
            )
        if clean(actual_email) != clean(decision.latest_email):
            raise RuntimeError(
                f"email_last_order verification failed for {decision.master_customer!r}"
            )


def _get_or_create_review_sheet(spreadsheet, rows: int):
    try:
        worksheet = spreadsheet.worksheet(REVIEW_SHEET_NAME)
    except WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=REVIEW_SHEET_NAME,
            rows=max(rows, 100),
            cols=len(REVIEW_COLUMNS),
        )
    column_count = getattr(worksheet, "col_count", len(REVIEW_COLUMNS))
    if column_count < len(REVIEW_COLUMNS):
        worksheet.add_cols(len(REVIEW_COLUMNS) - column_count)
    hide = getattr(worksheet, "hide", None)
    if callable(hide):
        hide()
    return worksheet


def _write_review_sheet(
    spreadsheet,
    decisions: list[CustomerDecision],
    now: datetime,
) -> None:
    review = [decision for decision in decisions if decision.status == "needs_review"]
    worksheet = _get_or_create_review_sheet(spreadsheet, len(review) + 1)
    timestamp = now.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    values = [REVIEW_COLUMNS]
    for decision in review:
        row = asdict(decision)
        row["customer_id"] = decision.existing_customer_id
        row["last_seen_at"] = timestamp
        values.append([row.get(column, "") for column in REVIEW_COLUMNS])
    worksheet.clear()
    worksheet.update(values=values, range_name="A1", raw=True)


@coordinated_write
def run_customer_sync(
    spreadsheet,
    *,
    mode: str = "dry_run",
    order_rows: list[dict[str, Any]] | None = None,
    now: datetime | None = None,
) -> CustomerSyncResult:
    mode = normalize_mode(mode)
    if mode == "off":
        return CustomerSyncResult("off", 0, 0, 0, 0, 0, 0, 0, 0, 0)

    if order_rows is None:
        order_values = spreadsheet.worksheet("order_rows").get_all_values(
            value_render_option="UNFORMATTED_VALUE"
        )
        _, order_rows = values_to_dicts(order_values)
    customer_sheet = spreadsheet.worksheet("customers_enriched")
    customer_values = customer_sheet.get_all_values()
    customer_headers, customer_rows = values_to_dicts(customer_values)
    require_columns(customer_headers, CUSTOMER_REQUIRED_COLUMNS, "customers_enriched")
    if len(customer_headers) != len(set(customer_headers)):
        raise ValueError("customers_enriched has duplicate column headers")

    decisions = plan_customer_sync(order_rows, customer_rows)
    result = build_result(mode, decisions)
    if mode == "dry_run":
        return result

    renames = [decision for decision in decisions if decision.changes_name]
    history_verification = _apply_history_renames(spreadsheet, renames)
    history_verification.extend(
        _apply_planning_snapshots(spreadsheet, decisions)
    )
    if GLN_COLUMN not in customer_headers and any(
        decision.backfills_gln
        or (decision.status == "new_customer" and decision.stockfiller_gln)
        for decision in decisions
    ):
        if getattr(customer_sheet, "col_count", len(customer_headers) + 1) < len(customer_headers) + 1:
            customer_sheet.add_cols(1)
        _batch_cell_updates(customer_sheet, [(1, len(customer_headers), GLN_COLUMN)])
        customer_headers.append(GLN_COLUMN)
    _apply_customer_changes(
        customer_sheet,
        customer_headers,
        decisions,
    )
    _verify_history(history_verification)
    _verify_customer_changes(customer_sheet, customer_headers, decisions)
    _write_review_sheet(
        spreadsheet,
        decisions,
        now or datetime.now(timezone.utc),
    )
    return result


def open_spreadsheet(config: CustomerSyncConfig):
    credentials = Credentials.from_service_account_info(
        config.google_credentials,
        scopes=SCOPES,
    )
    return guard_client(gspread.authorize(credentials)).open_by_key(config.sheet_key)


def _read_state(spreadsheet) -> dict[str, str]:
    try:
        worksheet = spreadsheet.worksheet(STATE_SHEET_NAME)
    except WorksheetNotFound:
        return {}
    values = worksheet.get_all_values()
    return {
        clean(row[0]): clean(row[1])
        for row in values[1:]
        if len(row) >= 2 and clean(row[0])
    }


@coordinated_write
def _write_state(spreadsheet, state: dict[str, str]) -> None:
    try:
        worksheet = spreadsheet.worksheet(STATE_SHEET_NAME)
    except WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=STATE_SHEET_NAME,
            rows=max(len(state) + 1, 20),
            cols=3,
        )
    now_text = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    values = [["key", "value", "updated_at"]]
    values.extend(
        [key, value, now_text]
        for key, value in sorted(state.items())
    )
    worksheet.clear()
    worksheet.update(values=values, range_name="A1", raw=True)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reconcile CRM_DATABASE order customers into customers_enriched."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply safe changes. Without this flag the command is read-only.",
    )
    return parser


def _result_payload(result: CustomerSyncResult) -> dict[str, Any]:
    status_counts = Counter(decision.status for decision in result.decisions)
    updated_names_by_match = Counter(
        decision.matched_by
        for decision in result.decisions
        if decision.changes_name
    )
    review_reasons = Counter(
        decision.reason
        for decision in result.decisions
        if decision.status == "needs_review"
    )

    def report_decision(decision: CustomerDecision) -> dict[str, Any]:
        return {
            "status": decision.status,
            "identity_key": decision.identity_key,
            "master_customer": decision.master_customer,
            "customer_number": decision.customer_number,
            "source_customer_identifier": (
                decision.source_customer_identifier
            ),
            "stockfiller_gln": decision.stockfiller_gln,
            "existing_stockfiller_gln": decision.existing_stockfiller_gln,
            "backfills_gln": decision.backfills_gln,
            "order_date": decision.order_date,
            "order_reference": decision.order_reference,
            "matched_row": decision.matched_row,
            "existing_customer": decision.existing_customer,
            "existing_customer_id": decision.existing_customer_id,
            "existing_customer_number": decision.existing_customer_number,
            "source_address": decision.source_address,
            "source_address_number": decision.source_address_number,
            "source_postal": decision.source_postal,
            "source_city": decision.source_city,
            "existing_cancelled_flag": decision.existing_cancelled_flag,
            "matched_by": decision.matched_by,
            "reason": decision.reason,
            "confidence": decision.confidence,
            "changes_name": decision.changes_name,
            "backfills_customer_number": decision.backfills_customer_number,
            "changes_email": decision.changes_email,
        }

    return {
        "mode": result.mode,
        "exact_matches": result.exact_matches,
        "updated_names": result.updated_names,
        "backfilled_customer_numbers": result.backfilled_customer_numbers,
        "appended_customers": result.appended_customers,
        "updated_emails": result.updated_emails,
        "backfilled_glns": result.backfilled_glns,
        "needs_review": result.needs_review,
        "ignored_internal": result.ignored_internal,
        "ignored_gln_identifiers": result.ignored_gln_identifiers,
        "ignored_organization_identifiers": (
            result.ignored_organization_identifiers
        ),
        "status_counts": dict(sorted(status_counts.items())),
        "updated_names_by_match": dict(sorted(updated_names_by_match.items())),
        "review_reasons": dict(sorted(review_reasons.items())),
        "proposed_changes": [
            report_decision(decision)
            for decision in result.decisions
            if (
                decision.changes_name
                or decision.backfills_customer_number
                or decision.changes_email
                or decision.backfills_gln
                or decision.status == "new_customer"
            )
        ],
        "review": [
            report_decision(decision)
            for decision in result.decisions
            if decision.status == "needs_review"
        ],
    }


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    mode = "apply" if args.apply else "dry_run"
    try:
        config = CustomerSyncConfig.from_env()
        spreadsheet = open_spreadsheet(config)
        run_at = datetime.now(timezone.utc)
        result = run_customer_sync(spreadsheet, mode=mode, now=run_at)
        if result.applied:
            state = _read_state(spreadsheet)
            state.update(result.state_values(run_at))
            _write_state(spreadsheet, state)
    except Exception as exc:
        print(f"Customer master sync failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(_result_payload(result), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
