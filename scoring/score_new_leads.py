"""
Lead Scoring via Local LLM (Ollama).

Scores unscored leads based on their potential as Grryp customers.
Runs after the scraper.
"""

import sys
import os
import json
import ollama

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import OLLAMA_MODEL, GRRYP_CONTEXT, SCORE_HOT, SCORE_WARM, SCORE_COLD
from db import get_conn, get_unscored_leads, update_lead_score


SCORING_PROMPT = """You are a lead scoring assistant for Grryp, a custom tap handle company.

{context}

Score this brewery lead from 0-100 based on how likely they are to become a Grryp customer.

SCORING CRITERIA:
- Pre-opening brewery (no website/social yet, recent permit) = 85-100
- Recently opened (< 6 months, setting up taproom) = 65-85
- Established brewery expanding or rebranding = 50-65
- Established brewery with no clear need = 20-50
- Contract brewer / production only (no taproom) = 5-20
- Not actually a brewery = 0

BONUS POINTS:
+5 if in Texas (Grryp's home state, easy relationship)
+3 if in a craft-beer-heavy state (CO, CA, OR, WA, MI, NC, VA, PA, OH)
+5 if trade name suggests taproom/brewpub
+3 if small/micro operation (likely needs custom handles more)

LEAD DATA:
Business Name: {business_name}
Trade Name: {trade_name}
Owner: {owner_name}
Location: {city}, {state} {zip}
County: {county}
Permit Type: {permit_type}
Status: {status}
Issue Date: {issue_date}

Respond with ONLY a JSON object (no other text):
{{"score": <0-100>, "stage": "<hot|warm|cold|skip>", "reasoning": "<1-2 sentence explanation>"}}
"""


def score_lead(lead):
    """Score a single lead using the local LLM."""
    prompt = SCORING_PROMPT.format(
        context=GRRYP_CONTEXT,
        business_name=lead["business_name"] or "Unknown",
        trade_name=lead["trade_name"] or "N/A",
        owner_name=lead["owner_name"] or "Unknown",
        city=lead["city"] or "Unknown",
        state=lead["state"] or "Unknown",
        zip=lead["zip"] or "",
        county=lead["county"] or "",
        permit_type=lead["permit_type"] or "Unknown",
        status=lead["status"] or "Unknown",
        issue_date=lead["issue_date"] or "Unknown",
    )

    try:
        response = ollama.chat(
            model=OLLAMA_MODEL,
            messages=[{"role": "user", "content": prompt}],
            options={"temperature": 0.3}
        )
        text = response["message"]["content"].strip()

        # Extract JSON from response (handle markdown code blocks)
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()

        result = json.loads(text)
        return result
    except (json.JSONDecodeError, KeyError) as e:
        print(f"    Parse error for {lead['business_name']}: {e}")
        return {"score": 50, "stage": "warm", "reasoning": "Auto-scored: LLM parse error"}
    except Exception as e:
        print(f"    Ollama error for {lead['business_name']}: {e}")
        return None


def run():
    from datetime import datetime
    print(f"\n{'='*60}")
    print(f"Lead Scoring - {datetime.now().isoformat()}")
    print(f"{'='*60}")

    conn = get_conn()
    leads = get_unscored_leads(conn, limit=50)
    print(f"Found {len(leads)} unscored leads")

    hot = warm = cold = skip = errors = 0

    for lead in leads:
        name = lead["business_name"] or "Unknown"
        loc = f"{lead['city'] or '?'}, {lead['state'] or '?'}"
        print(f"\n  Scoring: {name} ({loc})...", end=" ")

        result = score_lead(lead)
        if result is None:
            errors += 1
            print("ERROR")
            continue

        score = result.get("score", 50)
        stage = result.get("stage", "warm")
        reasoning = result.get("reasoning", "")

        update_lead_score(conn, lead["id"], score, stage, reasoning)

        if score >= SCORE_HOT:
            hot += 1
            print(f"HOT ({score}) - {reasoning}")
        elif score >= SCORE_WARM:
            warm += 1
            print(f"WARM ({score})")
        elif score >= SCORE_COLD:
            cold += 1
            print(f"COLD ({score})")
        else:
            skip += 1
            print(f"SKIP ({score})")

    conn.commit()
    conn.close()

    print(f"\n{'='*60}")
    print(f"RESULTS: {hot} hot, {warm} warm, {cold} cold, {skip} skip, {errors} errors")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    run()
