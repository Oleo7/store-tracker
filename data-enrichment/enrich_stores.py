import os
import logging
import json
import math
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from crm_write_coordination import guard_client
import googlemaps
from dotenv import load_dotenv

load_dotenv()
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

SHEET_KEY = os.environ["SHEET_KEY"]
GOOGLE_MAPS_API_KEY = os.environ["GOOGLE_MAPS_API_KEY"]

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def parse_coordinate_value(value, kind: str):
    if pd.isna(value):
        return None

    limits = {
        "latitude": (55.0, 70.0),
        "longitude": (10.0, 25.0),
    }
    lower, upper = limits[kind]

    def in_range(number):
        return math.isfinite(number) and lower <= number <= upper

    normalized = str(value).strip().replace("\xa0", "").replace(" ", "").replace(",", ".")
    if not normalized:
        return None

    try:
        parsed = float(normalized)
        if in_range(parsed):
            return parsed
    except ValueError:
        pass

    sign = -1 if normalized.startswith("-") else 1
    integer_part = normalized.lstrip("+-").split(".", 1)[0]
    digits = "".join(ch for ch in integer_part if ch.isdigit())
    if not digits:
        return None

    raw_number = sign * int(digits)
    for decimals in range(1, 13):
        candidate = raw_number / (10 ** decimals)
        if in_range(candidate):
            return candidate

    return None


def parse_coordinate_column(series: pd.Series, kind: str) -> pd.Series:
    return pd.to_numeric(series.apply(lambda value: parse_coordinate_value(value, kind)), errors="coerce")


def get_location_info(store_name: str, gmaps: googlemaps.Client) -> tuple:
    query = f"{store_name}, Sweden"
    try:
        result = gmaps.geocode(query, language="sv")
    except Exception as e:
        log.warning(f"Geocoding failed for '{query}': {e}")
        return None, None, None, None, None, None, None

    if not result:
        log.warning(f"No geocoding result for '{query}'")
        return None, None, None, None, None, None, None

    r = result[0]
    lat = r["geometry"]["location"]["lat"]
    lng = r["geometry"]["location"]["lng"]

    components = r["address_components"]
    street       = next((c["long_name"] for c in components if "route" in c["types"]), None)
    number       = next((c["long_name"] for c in components if "street_number" in c["types"]), None)
    postal       = next((c["long_name"] for c in components if "postal_code" in c["types"]), None)
    city         = next((c["long_name"] for c in components if "postal_town" in c["types"]), None)
    region = next((c["long_name"] for c in components if "administrative_area_level_1" in c["types"]), None)

    return city, street, number, postal, region, lat, lng


from crm_geocoding import apply_geocoding_results


def main():
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    client = guard_client(gspread.authorize(creds))
    spreadsheet = client.open_by_key(SHEET_KEY)
    sheet = spreadsheet.worksheet("customers_enriched")
    values = sheet.get_all_values()
    headers = values[0]
    source = [dict(zip(headers, row + [''] * (len(headers)-len(row)))) for row in values[1:]]
    pending = [row for row in source if str(row.get('enriched')).lower() != 'true']
    log.info('Geocoding %s customer rows', len(pending))
    gmaps = googlemaps.Client(key=GOOGLE_MAPS_API_KEY)
    results = {}
    for row in pending:
        cid = str(row.get('customer_id') or '').strip()
        if cid:
            geo = get_location_info(row['customer'], gmaps)
            if geo[-2] is not None and geo[-1] is not None:
                results[cid] = geo
    count = apply_geocoding_results(sheet, source, results, sheet_id=SHEET_KEY)
    log.info('Updated geography for %s unchanged customers; conflicts wait for next run', count)


if __name__ == "__main__":
    main()
