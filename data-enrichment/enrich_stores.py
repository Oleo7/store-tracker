import os
import logging
import json
import math
import googlemaps
from dotenv import load_dotenv

load_dotenv()
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

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


def main():
    # Auth
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SHEET_KEY)

    # Read customers_enriched directly
    log.info("Reading customers_enriched sheet...")
    enriched_sheet = spreadsheet.worksheet("customers_enriched")
    df = pd.DataFrame(enriched_sheet.get_all_records())
    for col in ["city_google", "address_google", "address_number_google",
                "postal_code_google", "region_google", "enriched", "modified"]:
        df[col] = df[col].astype(object)
    df["latitude_google"] = parse_coordinate_column(df["latitude_google"], "latitude")
    df["longitude_google"] = parse_coordinate_column(df["longitude_google"], "longitude")
    log.info(f"Loaded {len(df)} rows")

    # Rows where enriched column is not True need processing
    needs_enrichment = df[df["enriched"].astype(str).str.lower() != "true"].copy()
    log.info(f"{len(needs_enrichment)} rows to enrich")

    if needs_enrichment.empty:
        log.info("Nothing to do — all rows already enriched")
        return

    # Geocode
    gmaps = googlemaps.Client(key=GOOGLE_MAPS_API_KEY)
    enriched_cols = ["city_google", "address_google", "address_number_google",
                     "postal_code_google", "region_google", "latitude_google", "longitude_google"]
    total = len(needs_enrichment)
    results = []
    for i, (_, row) in enumerate(needs_enrichment.iterrows()):
        results.append(get_location_info(row["customer"], gmaps))
        if (i + 1) % 10 == 0 or (i + 1) == total:
            log.info(f"Progress: {i + 1}/{total}")

    needs_enrichment[enriched_cols] = pd.DataFrame(results, index=needs_enrichment.index)
    needs_enrichment["enriched"] = True
    needs_enrichment["modified"] = ""

    # Merge back and write the full sheet
    df.update(needs_enrichment)

    # Write coordinates as numeric values so Sheets cannot treat decimal commas as thousands separators.
    for col in ["latitude_google", "longitude_google"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").round(7)
        df[col] = df[col].where(df[col].notna(), "")

    set_with_dataframe(enriched_sheet, df)
    log.info(f"Done — wrote {len(df)} total rows to 'customers_enriched'")


if __name__ == "__main__":
    main()
