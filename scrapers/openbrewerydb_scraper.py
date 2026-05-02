"""
Open Brewery DB Scraper for Grryp Prospecting Engine.

Free API, no auth required. Key feature: brewery_type=planning
gives us breweries that are actively in planning — the hottest leads.

API docs: https://www.openbrewerydb.org/
"""

import sys
import os
import requests
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import get_conn, init_db, upsert_lead

API_BASE = "https://api.openbrewerydb.org/v1/breweries"

# Brewery types we care about, in priority order
TARGET_TYPES = [
    "planning",    # In planning — HOTTEST leads
    "micro",       # Microbreweries — core Grryp market
    "brewpub",     # Brewpubs — need tap handles
    "nano",        # Nano breweries — small but passionate
    "regional",    # Regional — larger, still potential
    "contract",    # Contract — lower priority
]


def fetch_page(params):
    """Fetch a single page from the API."""
    resp = requests.get(API_BASE, params=params, timeout=30, headers={
        "User-Agent": "GrrypProspectingEngine/1.0"
    })
    resp.raise_for_status()
    return resp.json()


def fetch_all_by_type(brewery_type):
    """Fetch all breweries of a given type, paginating through results."""
    all_results = []
    page = 1
    per_page = 200

    while True:
        params = {
            "by_type": brewery_type,
            "per_page": per_page,
            "page": page,
        }
        results = fetch_page(params)
        if not results:
            break
        all_results.extend(results)
        if len(results) < per_page:
            break
        page += 1

    return all_results


def brewery_to_lead(brewery):
    """Convert an Open Brewery DB record to our lead schema."""
    # Build a unique ID from their ID
    permit_id = f"OBDB-{brewery.get('id', '')}"

    return {
        "permit_id": permit_id,
        "business_name": brewery.get("name", ""),
        "trade_name": brewery.get("name", ""),
        "owner_name": "",
        "street": brewery.get("street") or brewery.get("address_1") or "",
        "city": brewery.get("city", ""),
        "state": brewery.get("state", ""),
        "zip": brewery.get("postal_code", "")[:5] if brewery.get("postal_code") else "",
        "county": brewery.get("county_province", ""),
        "permit_type": f"brewery ({brewery.get('brewery_type', 'unknown')})",
        "status": "planning" if brewery.get("brewery_type") == "planning" else "active",
        "issue_date": "",
        "source": "openbrewerydb",
    }


def run():
    print(f"\n{'='*60}")
    print(f"Open Brewery DB Scraper - {datetime.now().isoformat()}")
    print(f"{'='*60}")

    init_db()
    conn = get_conn()
    total_found = 0
    new_count = 0
    errors = []

    for btype in TARGET_TYPES:
        print(f"\n  Fetching type: {btype}...", end=" ")
        try:
            breweries = fetch_all_by_type(btype)
            print(f"{len(breweries)} found")
            total_found += len(breweries)

            for brewery in breweries:
                lead = brewery_to_lead(brewery)
                result = upsert_lead(conn, lead)
                if result == "new":
                    new_count += 1
                    if btype == "planning":
                        print(f"    NEW PLANNING: {lead['business_name']} - {lead['city']}, {lead['state']}")

            conn.commit()
        except Exception as e:
            errors.append(f"{btype}: {e}")
            print(f"ERROR: {e}")

    # Log
    conn.execute("""
        INSERT INTO scrape_log (source, run_date, records_found, new_leads, errors)
        VALUES (?, ?, ?, ?, ?)
    """, ("openbrewerydb", datetime.now().isoformat(), total_found, new_count,
          "; ".join(errors) if errors else None))
    conn.commit()
    conn.close()

    print(f"\n{'='*60}")
    print(f"RESULTS: {total_found} breweries found, {new_count} NEW leads added")
    if errors:
        print(f"ERRORS: {'; '.join(errors)}")
    print(f"{'='*60}\n")

    return new_count


if __name__ == "__main__":
    run()
