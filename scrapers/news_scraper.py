"""
Google News RSS Scraper for Grryp Prospecting Engine.

Monitors Google News for brewery openings, new brewery announcements,
and brewery license approvals nationwide. Free, no auth needed.
"""

import sys
import os
import re
import xml.etree.ElementTree as ET
import requests
from datetime import datetime
from urllib.parse import quote_plus

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import get_conn, init_db

# Google News RSS search queries
SEARCH_QUERIES = [
    '"new brewery" opening',
    '"brewery opening" 2026',
    '"new brewpub" OR "new taproom" opening',
    '"brewery license" approved',
    '"brewery permit" application',
    'brewery "grand opening"',
]


def fetch_rss(query, when="30d"):
    """Fetch Google News RSS for a query. Returns list of articles."""
    encoded = quote_plus(query)
    url = f"https://news.google.com/rss/search?q={encoded}+when:{when}&hl=en-US&gl=US&ceid=US:en"

    resp = requests.get(url, timeout=30, headers={
        "User-Agent": "GrrypProspectingEngine/1.0"
    })
    resp.raise_for_status()

    articles = []
    root = ET.fromstring(resp.content)
    for item in root.findall(".//item"):
        title = item.findtext("title", "")
        link = item.findtext("link", "")
        pub_date = item.findtext("pubDate", "")
        source = item.findtext("source", "")
        description = item.findtext("description", "")

        # Clean HTML from description
        description = re.sub(r'<[^>]+>', '', description)

        articles.append({
            "title": title,
            "link": link,
            "pub_date": pub_date,
            "source": source,
            "description": description[:500],
        })

    return articles


def extract_location(title, description):
    """Try to extract city/state from article title and description."""
    text = f"{title} {description}"

    # Common patterns: "in [City], [State]" or "[City], [ST]"
    patterns = [
        r'in\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*([A-Z]{2})\b',
        r'in\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*([A-Z][a-z]+)',
        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*([A-Z]{2})\s',
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(1), match.group(2)

    return "", ""


def run():
    print(f"\n{'='*60}")
    print(f"News Scraper - {datetime.now().isoformat()}")
    print(f"{'='*60}")

    init_db()
    conn = get_conn()
    total_articles = 0
    new_articles = 0
    errors = []

    # Create news table if it doesn't exist
    conn.execute("""
        CREATE TABLE IF NOT EXISTS news_leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            link TEXT UNIQUE,
            pub_date TEXT,
            source TEXT,
            description TEXT,
            city TEXT,
            state TEXT,
            processed INTEGER DEFAULT 0,
            lead_id INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()

    for query in SEARCH_QUERIES:
        print(f"\n  Searching: {query}...", end=" ")
        try:
            articles = fetch_rss(query)
            print(f"{len(articles)} articles")
            total_articles += len(articles)

            for article in articles:
                # Check if we already have this article
                existing = conn.execute(
                    "SELECT id FROM news_leads WHERE link = ?",
                    (article["link"],)
                ).fetchone()

                if not existing:
                    city, state = extract_location(article["title"], article["description"])
                    conn.execute("""
                        INSERT INTO news_leads (title, link, pub_date, source, description, city, state)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (article["title"], article["link"], article["pub_date"],
                          article["source"], article["description"], city, state))
                    new_articles += 1
                    print(f"    NEW: {article['title'][:80]}")

            conn.commit()
        except Exception as e:
            errors.append(f"Query '{query}': {e}")
            print(f"ERROR: {e}")

    # Log
    conn.execute("""
        INSERT INTO scrape_log (source, run_date, records_found, new_leads, errors)
        VALUES (?, ?, ?, ?, ?)
    """, ("google_news", datetime.now().isoformat(), total_articles, new_articles,
          "; ".join(errors) if errors else None))
    conn.commit()
    conn.close()

    print(f"\n{'='*60}")
    print(f"RESULTS: {total_articles} articles found, {new_articles} NEW articles saved")
    if errors:
        print(f"ERRORS: {'; '.join(errors)}")
    print(f"{'='*60}\n")

    return new_articles


if __name__ == "__main__":
    run()
