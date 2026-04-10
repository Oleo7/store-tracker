import os
import logging
import json
import googlemaps
import pandas as pd
from tqdm import tqdm
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

tqdm.pandas()

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
        return None, None, None, None, None, None

    if not result:
        log.warning(f"No geocoding result for '{query}'")
        return None, None, None, None, None, None

    r = result[0]
    lat = r["geometry"]["location"]["lat"]
    lng = r["geometry"]["location"]["lng"]

    components = r["address_components"]
    street = next((c["long_name"] for c in components if "route" in c["types"]), None)
    number = next((c["long_name"] for c in components if "street_number" in c["types"]), None)
    postal = next((c["long_name"] for c in components if "postal_code" in c["types"]), None)
    city = next((c["long_name"] for c in components if "postal_town" in c["types"]), None)

    return city, street, number, postal, lat, lng


def main():
    # Auth
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SHEET_KEY)

    # Read source data
    log.info("Reading customers sheet...")
    source_sheet = spreadsheet.worksheet("customers")
    df = pd.DataFrame(source_sheet.get_all_records())
    log.info(f"Loaded {len(df)} customers")

    # Load already-enriched data (if it exists) to skip processed rows
    enriched_cols = ["city_google", "address_google", "address_number_google",
                     "postal_code_google", "latitude_google", "longitude_google"]
    try:
        enriched_sheet = spreadsheet.worksheet("customer_enriched")
        existing_df = pd.DataFrame(enriched_sheet.get_all_records())
        already_done = set(existing_df["customer"].dropna().unique()) if "customer" in existing_df.columns else set()
        log.info(f"Found {len(already_done)} already-enriched customers — skipping them")
    except gspread.WorksheetNotFound:
        existing_df = pd.DataFrame()
        already_done = set()
        log.info("No enriched sheet yet — starting fresh")

    # Split into new vs already processed
    needs_enrichment = df[~df["customer"].isin(already_done)].copy()
    log.info(f"{len(needs_enrichment)} customers to enrich")

    if needs_enrichment.empty:
        log.info("Nothing to do — all customers already enriched")
        return

    # Geocode new rows
    gmaps = googlemaps.Client(key=GOOGLE_MAPS_API_KEY)
    needs_enrichment[enriched_cols] = needs_enrichment["customer"].progress_apply(
        lambda x: pd.Series(get_location_info(x, gmaps))
    )

    # Combine with existing enriched data and write back
    combined = pd.concat([existing_df, needs_enrichment], ignore_index=True)

    try:
        enriched_sheet = spreadsheet.worksheet("customer_enriched")
    except gspread.WorksheetNotFound:
        enriched_sheet = spreadsheet.add_worksheet(title="customer_enriched", rows=5000, cols=30)

    set_with_dataframe(enriched_sheet, combined)
    log.info(f"Done — wrote {len(combined)} total rows to 'customer_enriched'")


if __name__ == "__main__":
    main()