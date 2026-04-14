import os
import logging
import json
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


def get_location_info(store_name: str, gmaps: googlemaps.Client) -> tuple:
    query = f"{store_name}, Sweden"
    try:
        result = gmaps.geocode(query)
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
    df["latitude_google"]  = df["latitude_google"].astype(float)
    df["longitude_google"] = df["longitude_google"].astype(float)
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
    set_with_dataframe(enriched_sheet, df)
    log.info(f"Done — wrote {len(df)} total rows to 'customers_enriched'")


if __name__ == "__main__":
    main()
