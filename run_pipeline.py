#!/usr/bin/env python3
"""
Grryp Prospecting Engine - Master Pipeline Runner.

Runs all stages in sequence: scrape -> score -> draft outreach -> digest.
Can also run individual stages.

Usage:
    python run_pipeline.py              # Run full pipeline
    python run_pipeline.py scrape       # Scrape only
    python run_pipeline.py score        # Score only
    python run_pipeline.py draft        # Draft outreach only
    python run_pipeline.py digest       # Generate digest only
    python run_pipeline.py init         # Initialize database only
"""

import sys
import os

# Ensure we can import from project root
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db import init_db


def run_scrape():
    from scrapers.ttb_scraper import run as scrape
    return scrape()


def run_score():
    from scoring.score_new_leads import run as score
    return score()


def run_draft():
    from outreach.draft_emails import run as draft
    return draft()


def run_digest():
    from dashboard.generate_digest import run as digest
    return digest()


def main():
    stage = sys.argv[1] if len(sys.argv) > 1 else "all"

    if stage == "init":
        init_db()
    elif stage == "scrape":
        init_db()
        run_scrape()
    elif stage == "score":
        run_score()
    elif stage == "draft":
        run_draft()
    elif stage == "digest":
        run_digest()
    elif stage == "all":
        print("\n>>> FULL PIPELINE RUN <<<\n")
        init_db()

        print("\n--- STAGE 1: SCRAPE ---")
        new = run_scrape()

        print("\n--- STAGE 2: SCORE ---")
        run_score()

        print("\n--- STAGE 3: DRAFT OUTREACH ---")
        run_draft()

        print("\n--- STAGE 4: GENERATE DIGEST ---")
        run_digest()

        print("\n>>> PIPELINE COMPLETE <<<\n")
    else:
        print(f"Unknown stage: {stage}")
        print("Usage: python run_pipeline.py [all|scrape|score|draft|digest|init]")
        sys.exit(1)


if __name__ == "__main__":
    main()
