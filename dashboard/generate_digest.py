"""
Daily Digest Generator.

Creates a markdown summary of pipeline activity for review.
This is what OpenClaw reads during its daily check-in.
"""

import sys
import os
from datetime import datetime, date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATA_DIR
from db import get_conn


def run():
    today = date.today().isoformat()
    conn = get_conn()

    # Pipeline stats
    total_leads = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
    new_today = conn.execute(
        "SELECT COUNT(*) FROM leads WHERE DATE(created_at) = ?", (today,)
    ).fetchone()[0]

    # Score distribution
    hot = conn.execute("SELECT COUNT(*) FROM leads WHERE score >= 80").fetchone()[0]
    warm = conn.execute("SELECT COUNT(*) FROM leads WHERE score >= 50 AND score < 80").fetchone()[0]
    cold = conn.execute("SELECT COUNT(*) FROM leads WHERE score >= 30 AND score < 50").fetchone()[0]
    unscored = conn.execute("SELECT COUNT(*) FROM leads WHERE score = 0").fetchone()[0]

    # Outreach status
    drafted = conn.execute(
        "SELECT COUNT(*) FROM leads WHERE outreach_status = 'drafted'"
    ).fetchone()[0]
    sent = conn.execute(
        "SELECT COUNT(*) FROM leads WHERE outreach_status = 'sent'"
    ).fetchone()[0]

    # Hot leads detail
    hot_leads = conn.execute("""
        SELECT business_name, trade_name, city, state, score, enrichment_notes,
               outreach_status, outreach_draft
        FROM leads WHERE score >= 80
        ORDER BY score DESC LIMIT 20
    """).fetchall()

    # New leads today
    new_leads = conn.execute("""
        SELECT business_name, trade_name, city, state, permit_type, issue_date
        FROM leads WHERE DATE(created_at) = ?
        ORDER BY created_at DESC
    """, (today,)).fetchall()

    # Pending outreach drafts
    pending = conn.execute("""
        SELECT l.business_name, l.city, l.state, l.score,
               o.email_subject, o.email_body
        FROM outreach_log o
        JOIN leads l ON o.lead_id = l.id
        WHERE o.status = 'drafted'
        ORDER BY l.score DESC
    """).fetchall()

    # Scrape log
    recent_scrapes = conn.execute("""
        SELECT source, run_date, records_found, new_leads, errors
        FROM scrape_log ORDER BY created_at DESC LIMIT 5
    """).fetchall()

    conn.close()

    # Build digest
    digest = f"""# Grryp Prospecting Engine - Daily Digest
## {today}

### Pipeline Summary
| Metric | Count |
|--------|-------|
| Total leads | {total_leads} |
| New today | {new_today} |
| Hot (80+) | {hot} |
| Warm (50-79) | {warm} |
| Cold (30-49) | {cold} |
| Unscored | {unscored} |
| Outreach drafted | {drafted} |
| Outreach sent | {sent} |

"""

    if new_leads:
        digest += "### New Leads Today\n"
        for lead in new_leads:
            name = lead["trade_name"] or lead["business_name"]
            loc = f"{lead['city'] or '?'}, {lead['state'] or '?'}"
            digest += f"- **{name}** - {loc} ({lead['permit_type'] or 'brewery'})\n"
        digest += "\n"

    if hot_leads:
        digest += "### Hot Leads (Score 80+)\n"
        for lead in hot_leads:
            name = lead["trade_name"] or lead["business_name"]
            loc = f"{lead['city'] or '?'}, {lead['state'] or '?'}"
            status = lead["outreach_status"]
            digest += f"- **{name}** ({loc}) - Score: {lead['score']} - Outreach: {status}\n"
            if lead["enrichment_notes"]:
                digest += f"  > {lead['enrichment_notes']}\n"
        digest += "\n"

    if pending:
        digest += "### Outreach Awaiting Approval\n"
        for p in pending:
            digest += f"\n#### {p['business_name']} ({p['city']}, {p['state']}) - Score: {p['score']}\n"
            digest += f"**Subject:** {p['email_subject']}\n\n"
            digest += f"{p['email_body']}\n"
            digest += "\n---\n"

    if recent_scrapes:
        digest += "\n### Recent Scrape Runs\n"
        for s in recent_scrapes:
            err = f" - ERRORS: {s['errors']}" if s["errors"] else ""
            digest += f"- {s['source']} @ {s['run_date']}: {s['records_found']} found, {s['new_leads']} new{err}\n"

    # Write digest
    digest_path = DATA_DIR / f"digest-{today}.md"
    digest_path.write_text(digest)
    print(f"Digest written to {digest_path}")

    # Also write a "latest" symlink
    latest_path = DATA_DIR / "digest-latest.md"
    latest_path.write_text(digest)

    return digest


if __name__ == "__main__":
    print(run())
