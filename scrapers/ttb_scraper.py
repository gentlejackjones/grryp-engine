"""
TTB Permit Scraper for Grryp Prospecting Engine.

Downloads the TTB list of permittees and identifies brewery-related permits.
TTB data uses "Brewer's Notices" (not in this dataset), but breweries often
appear here with Wholesaler/Wine Producer/DSP permits, and we catch them by name.

We also flag all brand-new permits (new_permit_flag=1) with brewery keywords.
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
from config import TTB_ALL_PERMITS_JSON, TTB_NEW_PERMITS_CSV, DATA_DIR
from db import get_conn, init_db, upsert_lead

# TTB JSON field indices:
# 0: Permit_Number, 1: Owner_Name, 2: Operating_Name (DBA),
# 3: Street, 4: City, 5: State, 6: Zip, 7: County,
# 8: Industry_Type, 9: New_Permit_Flag (1 = issued in last 7 days)
IDX_PERMIT = 0
IDX_OWNER = 1
IDX_DBA = 2
IDX_STREET = 3
IDX_CITY = 4
IDX_STATE = 5
IDX_ZIP = 6
IDX_COUNTY = 7
IDX_TYPE = 8
IDX_NEW = 9

# Keywords that indicate a brewery (checked against owner name + trade name)
BREWERY_KEYWORDS = [
    "brewing", "brewery", "brewhouse", "brew house", "brewpub", "brew pub",
    "brew co", "brew works", "brewworks", "beer", "beers",
    "taproom", "tap room", "tap house", "taphouse",
    "ale house", "alehouse", "ales",
    "lager", "hops", "hoppy", "malt house",
    "ferment", "craft beverage",
]

# Exclude false positives (wholesale, Costco, etc.)
EXCLUDE_KEYWORDS = [
    "costco wholesale", "walmart", "sam's club", "total wine",
    "wholesale corp", "wholesale inc",
]


def download(url, description):
    """Download a URL, return response or None."""
    try:
        print(f"  Fetching: {url}")
        resp = requests.get(url, timeout=120, headers={
            "User-Agent": "GrrypProspectingEngine/1.0 (brewery lead research)"
        })
        if resp.status_code == 200:
            print(f"  OK: {len(resp.content):,} bytes")
            return resp
        else:
            print(f"  Got HTTP {resp.status_code}")
    except Exception as e:
        print(f"  Error: {e}")
    return None


def is_brewery_record(record):
    """Check if a TTB array record is brewery-related by name."""
    name = f"{record[IDX_OWNER]} {record[IDX_DBA]}".lower()

    # Exclude obvious false positives
    if any(ex in name for ex in EXCLUDE_KEYWORDS):
        return False

    # Match brewery keywords
    return any(kw in name for kw in BREWERY_KEYWORDS)


def record_to_lead(record):
    """Convert a TTB array record to our lead dict."""
    return {
        "permit_id": str(record[IDX_PERMIT]),
        "business_name": str(record[IDX_OWNER]).strip(),
        "trade_name": str(record[IDX_DBA]).strip() if record[IDX_DBA] else "",
        "owner_name": str(record[IDX_OWNER]).strip(),
        "street": str(record[IDX_STREET]).strip(),
        "city": str(record[IDX_CITY]).strip(),
        "state": str(record[IDX_STATE]).strip(),
        "zip": str(record[IDX_ZIP]).strip(),
        "county": str(record[IDX_COUNTY]).strip(),
        "permit_type": str(record[IDX_TYPE]).strip(),
        "status": "new" if record[IDX_NEW] == 1 else "active",
        "issue_date": "",
        "source": "ttb",
    }


def scrape_json(resp):
    """Parse TTB JSON. Structure: {Permit Data: [[...], ...]}"""
    data = resp.json()
    records = data.get("Permit Data", [])
    print(f"  Total records in dataset: {len(records):,}")

    brewery_records = []
    new_brewery_records = []

    for record in records:
        if not isinstance(record, list) or len(record) < 10:
            continue
        if is_brewery_record(record):
            lead = record_to_lead(record)
            brewery_records.append(lead)
            if record[IDX_NEW] == 1:
                new_brewery_records.append(lead)

    if new_brewery_records:
        print(f"\n  ** NEW BREWERY PERMITS (last 7 days): **")
        for r in new_brewery_records:
            print(f"    {r['business_name']} ({r['trade_name']}) - {r['city']}, {r['state']}")

    return brewery_records


def scrape_csv(resp):
    """Parse TTB CSV for brewery-related permits."""
    content = resp.text
    # CSV may have no newlines (TTB formatting quirk) - detect and handle
    if content.count('\n') <= 1 and ',' in content:
        # Try splitting on permit number pattern (XX-X-XXXXX)
        import re
        lines = re.split(r'(?=,[A-Z]{2}-[A-Z]-\d{5})', content)
        if len(lines) > 1:
            header = lines[0].split(',')[:9]
            # Reconstruct as proper CSV
            records = []
            for line in lines[1:]:
                fields = line.strip(',').split(',')
                if len(fields) >= 9:
                    record = dict(zip(
                        ["Permit_Number", "Owner_Name", "Operating_Name",
                         "Street", "City", "State", "Prem_Zip", "Prem_County",
                         "Industry_Type"],
                        fields[:9]
                    ))
                    records.append(record)
            brewery_records = []
            for r in records:
                name = f"{r.get('Owner_Name', '')} {r.get('Operating_Name', '')}".lower()
                if any(ex in name for ex in EXCLUDE_KEYWORDS):
                    continue
                if any(kw in name for kw in BREWERY_KEYWORDS):
                    brewery_records.append({
                        "permit_id": r.get("Permit_Number", ""),
                        "business_name": r.get("Owner_Name", "").strip(),
                        "trade_name": r.get("Operating_Name", "").strip(),
                        "owner_name": r.get("Owner_Name", "").strip(),
                        "street": r.get("Street", "").strip(),
                        "city": r.get("City", "").strip(),
                        "state": r.get("State", "").strip(),
                        "zip": r.get("Prem_Zip", "").strip(),
                        "county": r.get("Prem_County", "").strip(),
                        "permit_type": r.get("Industry_Type", "").strip(),
                        "status": "new",
                        "issue_date": "",
                        "source": "ttb_csv",
                    })
            return brewery_records

    # Standard CSV parsing
    reader = csv.DictReader(io.StringIO(content))
    brewery_records = []
    for r in reader:
        name = f"{r.get('Owner_Name', '')} {r.get('Operating_Name', '')}".lower()
        if any(ex in name for ex in EXCLUDE_KEYWORDS):
            continue
        if any(kw in name for kw in BREWERY_KEYWORDS):
            brewery_records.append({
                "permit_id": r.get("Permit_Number", ""),
                "business_name": r.get("Owner_Name", "").strip(),
                "trade_name": r.get("Operating_Name", "").strip(),
                "owner_name": r.get("Owner_Name", "").strip(),
                "street": r.get("Street", "").strip(),
                "city": r.get("City", "").strip(),
                "state": r.get("State", "").strip(),
                "zip": r.get("Prem_Zip", "").strip(),
                "county": r.get("Prem_County", "").strip(),
                "permit_type": r.get("Industry_Type", "").strip(),
                "status": "new",
                "issue_date": "",
                "source": "ttb_csv",
            })
    return brewery_records


def run():
    print(f"\n{'='*60}")
    print(f"TTB Scraper - {datetime.now().isoformat()}")
    print(f"{'='*60}")

    init_db()
    conn = get_conn()
    total_found = 0
    new_count = 0
    errors = []

    # 1. Weekly new permits CSV
    print("\n[1/2] Fetching new permits CSV...")
    csv_resp = download(TTB_NEW_PERMITS_CSV, "new permits CSV")
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

    # 2. Full permits JSON
    print("\n[2/2] Fetching full permits JSON...")
    json_resp = download(TTB_ALL_PERMITS_JSON, "full permits JSON")
    if json_resp:
        try:
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
