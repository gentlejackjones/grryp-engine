"""Database setup and helpers for the Grryp Prospecting Engine."""

import sqlite3
from datetime import datetime
from config import DB_PATH


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            permit_id TEXT UNIQUE,
            business_name TEXT,
            trade_name TEXT,
            owner_name TEXT,
            street TEXT,
            city TEXT,
            state TEXT,
            zip TEXT,
            county TEXT,
            permit_type TEXT,
            status TEXT,
            issue_date TEXT,
            source TEXT DEFAULT 'ttb',
            score INTEGER DEFAULT 0,
            stage TEXT DEFAULT 'new',
            website TEXT,
            social TEXT,
            enrichment_notes TEXT,
            outreach_status TEXT DEFAULT 'none',
            outreach_draft TEXT,
            last_contacted TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS scrape_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            run_date TEXT,
            records_found INTEGER DEFAULT 0,
            new_leads INTEGER DEFAULT 0,
            errors TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS outreach_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id INTEGER REFERENCES leads(id),
            email_subject TEXT,
            email_body TEXT,
            sent_at TEXT,
            opened_at TEXT,
            replied_at TEXT,
            status TEXT DEFAULT 'drafted'
        );
    """)
    conn.commit()
    conn.close()
    print(f"Database initialized at {DB_PATH}")


def upsert_lead(conn, lead_data):
    """Insert or update a lead. Returns 'new' or 'existing'."""
    existing = conn.execute(
        "SELECT id FROM leads WHERE permit_id = ?",
        (lead_data["permit_id"],)
    ).fetchone()

    if existing:
        conn.execute("""
            UPDATE leads SET
                business_name = ?, trade_name = ?, owner_name = ?,
                street = ?, city = ?, state = ?, zip = ?, county = ?,
                permit_type = ?, status = ?, issue_date = ?,
                updated_at = ?
            WHERE permit_id = ?
        """, (
            lead_data.get("business_name"), lead_data.get("trade_name"),
            lead_data.get("owner_name"), lead_data.get("street"),
            lead_data.get("city"), lead_data.get("state"),
            lead_data.get("zip"), lead_data.get("county"),
            lead_data.get("permit_type"), lead_data.get("status"),
            lead_data.get("issue_date"), datetime.now().isoformat(),
            lead_data["permit_id"]
        ))
        return "existing"
    else:
        conn.execute("""
            INSERT INTO leads (
                permit_id, business_name, trade_name, owner_name,
                street, city, state, zip, county,
                permit_type, status, issue_date, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            lead_data["permit_id"], lead_data.get("business_name"),
            lead_data.get("trade_name"), lead_data.get("owner_name"),
            lead_data.get("street"), lead_data.get("city"),
            lead_data.get("state"), lead_data.get("zip"),
            lead_data.get("county"), lead_data.get("permit_type"),
            lead_data.get("status"), lead_data.get("issue_date"),
            lead_data.get("source", "ttb")
        ))
        return "new"


def get_unscored_leads(conn, limit=50):
    return conn.execute(
        "SELECT * FROM leads WHERE score = 0 AND stage = 'new' ORDER BY created_at DESC LIMIT ?",
        (limit,)
    ).fetchall()


def get_leads_needing_outreach(conn, min_score=80):
    return conn.execute(
        "SELECT * FROM leads WHERE score >= ? AND outreach_status = 'none' ORDER BY score DESC",
        (min_score,)
    ).fetchall()


def update_lead_score(conn, lead_id, score, stage, enrichment_notes=None):
    conn.execute("""
        UPDATE leads SET score = ?, stage = ?, enrichment_notes = ?, updated_at = ?
        WHERE id = ?
    """, (score, stage, enrichment_notes, datetime.now().isoformat(), lead_id))


def save_outreach_draft(conn, lead_id, subject, body):
    conn.execute("""
        INSERT INTO outreach_log (lead_id, email_subject, email_body, status)
        VALUES (?, ?, ?, 'drafted')
    """, (lead_id, subject, body))
    conn.execute("""
        UPDATE leads SET outreach_status = 'drafted', outreach_draft = ?, updated_at = ?
        WHERE id = ?
    """, (body, datetime.now().isoformat(), lead_id))


if __name__ == "__main__":
    init_db()
