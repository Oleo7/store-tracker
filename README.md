# Ändringar lokalt
python web-app/app.py

# Deploy ändringar (skicka in i terminalen)
git status --> se vilka filer som ändrats
git add --> filen du vill lägga till
git commit -m --> lagt till denna grejen inom "dsfndsjfn"
git push --> skicka ändringarna till github

# Store Tracker

A mobile-friendly web app for sales reps to track store visits, built with Flask and Google Sheets as the backend.

## Project Structure

```
store_tracker/
  web-app/                  # Flask web application
    app.py
    index.html
    requirements.txt
  data-enrichment/          # Scripts to enrich customer data with geocoding
    enrich_stores.py
    enrich_stores.ipynb
    requirements.txt
  .env                      # Local environment variables (never commit this)
```

## Prerequisites

- Python 3.10+
- A Google Cloud service account with access to the target Google Sheet
- A Google Maps API key

## Environment Variables

Create a `.env` file in the project root with the following variables:

```
SHEET_KEY=<your Google Sheet ID>
GOOGLE_MAPS_API_KEY=<your Google Maps API key>
GOOGLE_CREDENTIALS=<service account JSON as a single-line string>
```

To get the `GOOGLE_CREDENTIALS` value, open your service account JSON file and paste the entire contents as a single line.

---

## Web App

### Local setup

```bash
cd web-app
python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # macOS/Linux
pip install -r requirements.txt
```

### Run locally

```bash
python app.py
```

The app will be available at `http://localhost:5000`.

### Deploy to Render

1. Create a new **Web Service** in Render
2. Set the root directory to `web-app/`
3. Set the build command to `pip install -r requirements.txt`
4. Set the start command to `python app.py`
5. Add the environment variables (`SHEET_KEY`, `GOOGLE_CREDENTIALS`) under **Environment**

---

## Data Enrichment

Enriches the `customers` sheet with geocoded address data (city, street, postal code, coordinates) using the Google Maps API. Results are written to a `customers_enriched` worksheet. Already-processed rows are skipped on subsequent runs.

### Setup

```bash
cd data-enrichment
pip install -r requirements.txt
```

### Run the script

```bash
python enrich_stores.py
```

### Run the notebook

Open `enrich_stores.ipynb` in Jupyter or VS Code and run the cells in order. Requires the same `.env` file in the project root.
