"""
TTB Permit Scraper for Grryp Prospecting Engine.

Downloads the TTB list of permittees and identifies new brewery permits.
Run nightly via cron.
"""

import sys
import os
import json
import csv
import io
import requests
from datetime import datetime, date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import (
    TTB_ALL_PERMITS_JSON, TTB_NEW_PERMITS_CSV,
    TTB_ALL_PERMITS_JSON_DATED, TTB_NEW_PERMITS_CSV_DATED,
    DATA_DIR
)
from db import get_conn, init_db, upsert_lead

# Brewery-related permit types from TTB
BREWERY_PERMIT_TYPES = [
    "brewery", "brewpub", "brew pub", "brewer",
    "microbrewery", "micro brewery",
]


def try_download(urls, description):
    """Try multiple URLs, return response from first that works."""
    for url in urls:
        try:
            print(f"  Trying: {url}")
            resp = requests.get(url, timeout=60, headers={
                "User-Agent": "GrrypProspectingEngine/1.0 (brewery lead research)"
            })
            if resp.status_code == 200:
                print(f"  OK: {len(resp.content)} bytes")
                return resp
            else:
                print(f"  Got {resp.status_code}, trying next...")
        except Exception as e:
            print(f"  Error: {e}, trying next...")
    print(f"  FAILED: Could not download {description}")
    return None


def get_ttb_urls():
    """Generate URL candidates with dated paths."""
    now = date.today()
    # Try current month and previous month
    dates = [
        now.strftime("%Y-%m"),
        date(now.year, now.month - 1 if now.month > 1 else 12,
             1).strftime("%Y-%m") if now.month > 1
        else date(now.year - 1, 12, 1).strftime("%Y-%m")
    ]

    json_urls = [TTB_ALL_PERMITS_JSON]
    csv_urls = [TTB_NEW_PERMITS_CSV]
    for d in dates:
        json_urls.append(TTB_ALL_PERMITS_JSON_DATED.format(date=d))
        csv_urls.append(TTB_NEW_PERMITS_CSV_DATED.format(date=d))

    return json_urls, csv_urls


def is_brewery_permit(record):
    """Check if a permit record is brewery-related."""
    # Check permit type field (varies by data format)
    for field in ["permit_type", "Permit Type", "PERMIT_TYPE",
                  "type", "Type", "industry", "Industry"]:
        val = str(record.get(field, "")).lower()
        if any(bt in val for bt in BREWERY_PERMIT_TYPES):
            return True

    # Check business name for brewery keywords
    name = str(record.get("business_name", record.get("Business Name",
               record.get("BUSINESS_NAME", "")))).lower()
    brewery_keywords = ["brew", "beer", "ale", "lager", "taproom",
                       "tap room", "malt", "hops", "ferment"]
    if any(kw in name for kw in brewery_keywords):
        return True

    return False


def normalize_record(record):
    """Normalize a TTB record to our lead schema."""
    # Handle various field name formats (TTB isn't consistent)
    def get_field(record, *names):
        for name in names:
            if name in record and record[name]:
                return str(record[name]).strip()
        return ""

    permit_id = get_field(record, "permit_number", "Permit Number",
                          "PERMIT_NUMBER", "permit_no", "Permit No")
    if not permit_id:
        # Generate a synthetic ID from name + location
        name = get_field(record, "business_name", "Business Name", "BUSINESS_NAME")
        city = get_field(record, "city", "City", "CITY")
        state = get_field(record, "state", "State", "STATE")
        permit_id = f"SYNTH-{name}-{city}-{state}".replace(" ", "_")[:100]

    return {
        "permit_id": permit_id,
        "business_name": get_field(record, "business_name", "Business Name",
                                   "BUSINESS_NAME", "name", "Name"),
        "trade_name": get_field(record, "trade_name", "Trade Name",
                                "TRADE_NAME", "dba", "DBA"),
        "owner_name": get_field(record, "owner_name", "Owner Name",
                                "OWNER_NAME", "owner", "Owner",
                                "principal", "Principal"),
        "street": get_field(record, "street", "Street", "STREET",
                           "address", "Address", "ADDRESS"),
        "city": get_field(record, "city", "City", "CITY"),
        "state": get_field(record, "state", "State", "STATE"),
        "zip": get_field(record, "zip", "Zip", "ZIP", "zip_code", "Zip Code"),
        "county": get_field(record, "county", "County", "COUNTY"),
        "permit_type": get_field(record, "permit_type", "Permit Type",
                                 "PERMIT_TYPE", "type", "Type"),
        "status": get_field(record, "status", "Status", "STATUS"),
        "issue_date": get_field(record, "issue_date", "Issue Date",
                                "ISSUE_DATE", "effective_date",
                                "Effective Date", "date", "Date"),
        "source": "ttb",
    }


def scrape_json(resp):
    """Parse the full TTB JSON permit file for brewery permits."""
    data = resp.json()

    # Handle different JSON structures
    records = data if isinstance(data, list) else data.get("data", data.get("results", []))

    brewery_records = []
    for record in records:
        if is_brewery_permit(record):
            brewery_records.append(normalize_record(record))

    return brewery_records


def scrape_csv(resp):
    """Parse the TTB new permits CSV for brewery permits."""
    content = resp.text
    reader = csv.DictReader(io.StringIO(content))

    brewery_records = []
    for record in reader:
        if is_brewery_permit(record):
            brewery_records.append(normalize_record(record))

    return brewery_records


def run():
    print(f"\n{'='*60}")
    print(f"TTB Scraper - {datetime.now().isoformat()}")
    print(f"{'='*60}")

    init_db()
    conn = get_conn()

    json_urls, csv_urls = get_ttb_urls()
    total_found = 0
    new_count = 0
    errors = []

    # Try the weekly new permits CSV first (smaller, more targeted)
    print("\n[1/2] Fetching new permits CSV...")
    csv_resp = try_download(csv_urls, "new permits CSV")
    if csv_resp:
        try:
            records = scrape_csv(csv_resp)
            print(f"  Found {len(records)} brewery-related permits in CSV")
            total_found += len(records)
            for rec in records:
                result = upsert_lead(conn, rec)
                if result == "new":
                    new_count += 1
                    print(f"    NEW: {rec['business_name']} - {rec['city']}, {rec['state']}")
            conn.commit()
        except Exception as e:
            errors.append(f"CSV parse error: {e}")
            print(f"  ERROR parsing CSV: {e}")

    # Then try the full JSON (larger, catches anything CSV missed)
    print("\n[2/2] Fetching full permits JSON...")
    json_resp = try_download(json_urls, "full permits JSON")
    if json_resp:
        try:
            # Save raw JSON for reference
            raw_path = DATA_DIR / f"ttb_raw_{date.today().isoformat()}.json"
            raw_path.write_bytes(json_resp.content)
            print(f"  Saved raw data to {raw_path}")

            records = scrape_json(json_resp)
            print(f"  Found {len(records)} brewery-related permits in JSON")
            total_found += len(records)
            for rec in records:
                result = upsert_lead(conn, rec)
                if result == "new":
                    new_count += 1
            conn.commit()
        except Exception as e:
            errors.append(f"JSON parse error: {e}")
            print(f"  ERROR parsing JSON: {e}")

    # Log the run
    conn.execute("""
        INSERT INTO scrape_log (source, run_date, records_found, new_leads, errors)
        VALUES (?, ?, ?, ?, ?)
    """, ("ttb", datetime.now().isoformat(), total_found, new_count,
          "; ".join(errors) if errors else None))
    conn.commit()
    conn.close()

    print(f"\n{'='*60}")
    print(f"RESULTS: {total_found} brewery permits found, {new_count} NEW leads added")
    if errors:
        print(f"ERRORS: {'; '.join(errors)}")
    print(f"{'='*60}\n")

    return new_count


if __name__ == "__main__":
    run()
