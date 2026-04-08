from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
import gspread
from google.oauth2.service_account import Credentials
from datetime import date
import os
import json

app = Flask(__name__)
CORS(app)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]
SHEET_ID = "1uWhuacNGdhfIfA9mQjI04fR23ZCa6rGeXrlyNLbxEwc"
CREDS_FILE = "credentials.json"

@app.route("/")
def index():
    return send_file("index.html")


COLUMNS = ["store_name", "area", "contact", "phone", "status", "priority",
           "last_visited", "next_followup", "notes"]

COL_STATUS = 5       # E
COL_LAST_VISITED = 7 # G
COL_NEXT_FOLLOWUP = 8 # H
COL_NOTES = 9        # I


def get_sheet():
    env_creds = os.environ.get("GOOGLE_CREDENTIALS")
    if env_creds:
        info = json.loads(env_creds)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open_by_key(SHEET_ID).sheet1


@app.route("/stores", methods=["GET"])
def get_stores():
    sheet = get_sheet()
    rows = sheet.get_all_values()
    stores = []
    for i, row in enumerate(rows[1:], start=2):  # row 2 onwards, 1-indexed
        # Pad short rows
        while len(row) < 9:
            row.append("")
        stores.append({
            "row": i,
            **dict(zip(COLUMNS, row[:9]))
        })
    return jsonify(stores)


@app.route("/stores/<int:row>", methods=["POST"])
def update_store(row):
    data = request.get_json()
    sheet = get_sheet()

    batch = []
    if "status" in data:
        batch.append({"range": f"E{row}", "values": [[data["status"]]]})
        if data["status"] == "Visited":
            batch.append({"range": f"G{row}", "values": [[date.today().strftime("%Y-%m-%d")]]})
    if "next_followup" in data:
        batch.append({"range": f"H{row}", "values": [[data["next_followup"]]]})
    if "notes" in data:
        batch.append({"range": f"I{row}", "values": [[data["notes"]]]})

    if batch:
        sheet.batch_update(batch)

    return jsonify({"ok": True})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
