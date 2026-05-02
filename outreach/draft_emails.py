"""
Outreach Email Drafter via Local LLM (Ollama).

Generates personalized cold outreach for high-scoring leads.
Nothing sends without human approval.
"""

import sys
import os
import json
import ollama

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import OLLAMA_MODEL, GRRYP_CONTEXT
from db import get_conn, get_leads_needing_outreach, save_outreach_draft


OUTREACH_PROMPT = """You are writing a cold outreach email for Grryp, a custom tap handle company.

{context}

Write a short, personalized cold email to this brewery. The tone should be:
- Friendly and genuine, not salesy
- Reference something specific about them (location, name, style implied by their name)
- Brief (under 150 words)
- End with a soft CTA (not pushy)

DO NOT:
- Use generic phrases like "I came across your brewery"
- Be overly formal
- Make up facts about them you don't know
- Use exclamation marks more than once

LEAD INFO:
Business Name: {business_name}
Trade Name: {trade_name}
Owner: {owner_name}
Location: {city}, {state}
Permit Status: {status}
Score Reasoning: {reasoning}

Respond with ONLY a JSON object:
{{"subject": "<email subject line>", "body": "<full email body>"}}
"""


def draft_email(lead):
    """Draft an outreach email for a single lead."""
    prompt = OUTREACH_PROMPT.format(
        context=GRRYP_CONTEXT,
        business_name=lead["business_name"] or "Unknown",
        trade_name=lead["trade_name"] or lead["business_name"] or "Unknown",
        owner_name=lead["owner_name"] or "there",
        city=lead["city"] or "your area",
        state=lead["state"] or "",
        status=lead["status"] or "new permit",
        reasoning=lead["enrichment_notes"] or "High-potential brewery lead",
    )

    try:
        response = ollama.chat(
            model=OLLAMA_MODEL,
            messages=[{"role": "user", "content": prompt}],
            options={"temperature": 0.7}
        )
        text = response["message"]["content"].strip()

        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()

        result = json.loads(text)
        return result
    except Exception as e:
        print(f"    Error drafting for {lead['business_name']}: {e}")
        return None


def run():
    from datetime import datetime
    print(f"\n{'='*60}")
    print(f"Outreach Drafter - {datetime.now().isoformat()}")
    print(f"{'='*60}")

    conn = get_conn()
    leads = get_leads_needing_outreach(conn, min_score=80)
    print(f"Found {len(leads)} leads needing outreach (score >= 80)")

    drafted = 0
    for lead in leads:
        name = lead["business_name"] or "Unknown"
        print(f"\n  Drafting for: {name} (score: {lead['score']})...")

        result = draft_email(lead)
        if result and "subject" in result and "body" in result:
            save_outreach_draft(conn, lead["id"], result["subject"], result["body"])
            drafted += 1
            print(f"    Subject: {result['subject']}")
        else:
            print(f"    FAILED to draft")

    conn.commit()
    conn.close()

    print(f"\n{'='*60}")
    print(f"RESULTS: {drafted} outreach emails drafted (awaiting approval)")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    run()
