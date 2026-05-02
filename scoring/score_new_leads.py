"""
Lead Scoring for Grryp Prospecting Engine.

Two-tier scoring:
1. Rule-based fast scoring for bulk leads (no LLM needed)
2. LLM-powered deep scoring for new/promising leads only

This keeps GPU time focused on leads that matter.
"""

import sys
import os
import json
import ollama

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import OLLAMA_MODEL, GRRYP_CONTEXT, SCORE_HOT, SCORE_WARM, SCORE_COLD
from db import get_conn, get_unscored_leads, update_lead_score


# States with strong craft beer scenes
CRAFT_STATES = {"TX", "CO", "CA", "OR", "WA", "MI", "NC", "VA", "PA", "OH",
                "VT", "ME", "NY", "FL", "MN", "WI", "IL", "GA", "AZ", "NM"}

# Keywords suggesting a taproom/brewpub (higher value for Grryp)
TAPROOM_KEYWORDS = ["taproom", "tap room", "brewpub", "brew pub", "taphouse",
                    "tap house", "ale house", "alehouse", "beer garden",
                    "beer hall", "tasting room"]

# Keywords suggesting production-only (lower value)
PRODUCTION_KEYWORDS = ["distribut", "wholesale", "import", "export",
                       "supply", "logistics", "beverage group"]


def rule_score(lead):
    """Fast rule-based scoring. Returns (score, stage, reasoning)."""
    score = 35  # Base score for any brewery-related business
    reasons = []

    name = f"{lead['business_name'] or ''} {lead['trade_name'] or ''}".lower()
    state = (lead["state"] or "").upper()
    status = (lead["status"] or "").lower()
    permit_type = (lead["permit_type"] or "").lower()

    # NEW permit = high priority
    if status == "new":
        score += 40
        reasons.append("New permit (last 7 days)")

    # State bonuses
    if state == "TX":
        score += 8
        reasons.append("Texas (home state)")
    elif state in CRAFT_STATES:
        score += 4
        reasons.append(f"Craft beer state ({state})")

    # Taproom/brewpub keywords = needs tap handles
    if any(kw in name for kw in TAPROOM_KEYWORDS):
        score += 15
        reasons.append("Taproom/brewpub in name")

    # Small/micro indicators
    if any(kw in name for kw in ["micro", "small batch", "nano", "garage"]):
        score += 5
        reasons.append("Small/micro brewery")

    # Production/wholesale indicators = less likely to need custom handles
    if any(kw in name for kw in PRODUCTION_KEYWORDS):
        score -= 15
        reasons.append("Production/wholesale focus")

    # Permit type adjustments
    if "wholesaler" in permit_type:
        score -= 5  # Wholesaler permit alone is less interesting
    if "wine producer" in permit_type:
        score += 3  # Often means alternating premises brewery

    # Clamp
    score = max(0, min(100, score))

    # Determine stage
    if score >= SCORE_HOT:
        stage = "hot"
    elif score >= SCORE_WARM:
        stage = "warm"
    elif score >= SCORE_COLD:
        stage = "cold"
    else:
        stage = "skip"

    return score, stage, "; ".join(reasons) if reasons else "Standard brewery lead"


LLM_SCORING_PROMPT = """You are a lead scoring assistant for Grryp, a custom tap handle company.

{context}

Score this brewery lead from 0-100. This is a NEWLY ISSUED permit (last 7 days), meaning this
brewery is likely in planning or just opening — prime time for tap handle sales.

SCORING GUIDE:
- 90-100: New brewery in planning, taproom-focused, in TX or nearby
- 80-89: New brewery, likely has a taproom, good location
- 65-79: New permit but unclear if they need tap handles
- 50-64: Might be production-only or a side permit for existing business
- Below 50: Unlikely to need custom tap handles

LEAD DATA:
Business Name: {business_name}
Trade Name: {trade_name}
Owner: {owner_name}
Location: {city}, {state} {zip}
County: {county}
Permit Type: {permit_type}

Respond with ONLY a JSON object:
{{"score": <0-100>, "stage": "<hot|warm|cold|skip>", "reasoning": "<1-2 sentence explanation>"}}
"""


def llm_score(lead):
    """Deep LLM scoring for high-priority leads."""
    prompt = LLM_SCORING_PROMPT.format(
        context=GRRYP_CONTEXT,
        business_name=lead["business_name"] or "Unknown",
        trade_name=lead["trade_name"] or "N/A",
        owner_name=lead["owner_name"] or "Unknown",
        city=lead["city"] or "Unknown",
        state=lead["state"] or "Unknown",
        zip=lead["zip"] or "",
        county=lead["county"] or "",
        permit_type=lead["permit_type"] or "Unknown",
    )

    try:
        response = ollama.chat(
            model=OLLAMA_MODEL,
            messages=[{"role": "user", "content": prompt}],
            options={"temperature": 0.3}
        )
        text = response["message"]["content"].strip()

        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()

        result = json.loads(text)
        return result["score"], result["stage"], result.get("reasoning", "")
    except Exception as e:
        print(f"    LLM error: {e}")
        return None, None, None


def run():
    from datetime import datetime
    print(f"\n{'='*60}")
    print(f"Lead Scoring - {datetime.now().isoformat()}")
    print(f"{'='*60}")

    conn = get_conn()
    leads = get_unscored_leads(conn, limit=5000)
    print(f"Found {len(leads)} unscored leads")

    hot = warm = cold = skip = errors = 0
    llm_count = 0

    # Separate new permits from established
    new_leads = [l for l in leads if (l["status"] or "").lower() == "new"]
    established = [l for l in leads if (l["status"] or "").lower() != "new"]

    print(f"  New permits: {len(new_leads)} (will LLM-score)")
    print(f"  Established: {len(established)} (rule-scored)")

    # LLM-score new permits (small number, worth the GPU time)
    if new_leads:
        print(f"\n--- LLM Scoring: New Permits ---")
        for lead in new_leads:
            name = lead["business_name"] or "Unknown"
            loc = f"{lead['city'] or '?'}, {lead['state'] or '?'}"
            print(f"\n  {name} ({loc})...", end=" ")

            score, stage, reasoning = llm_score(lead)
            if score is None:
                # Fall back to rule scoring
                score, stage, reasoning = rule_score(lead)
                errors += 1

            llm_count += 1
            update_lead_score(conn, lead["id"], score, stage, reasoning)

            if score >= SCORE_HOT:
                hot += 1
                print(f"HOT ({score}) - {reasoning}")
            elif score >= SCORE_WARM:
                warm += 1
                print(f"WARM ({score}) - {reasoning}")
            else:
                cold += 1
                print(f"COLD ({score})")

    # Rule-score established permits (fast, no LLM)
    if established:
        print(f"\n--- Rule Scoring: {len(established)} Established Leads ---")
        for lead in established:
            score, stage, reasoning = rule_score(lead)
            update_lead_score(conn, lead["id"], score, stage, reasoning)

            if score >= SCORE_HOT:
                hot += 1
            elif score >= SCORE_WARM:
                warm += 1
            elif score >= SCORE_COLD:
                cold += 1
            else:
                skip += 1

    conn.commit()
    conn.close()

    print(f"\n{'='*60}")
    print(f"RESULTS: {hot} hot, {warm} warm, {cold} cold, {skip} skip")
    print(f"  LLM-scored: {llm_count} | Rule-scored: {len(established)}")
    if errors:
        print(f"  LLM errors (fell back to rules): {errors}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    run()
