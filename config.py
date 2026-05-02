"""Grryp Brewery Prospecting Engine - Configuration"""

import os
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "leads.db"

# Ensure directories exist
DATA_DIR.mkdir(exist_ok=True)

# TTB Data Sources
TTB_BASE = "https://www.ttb.gov"
TTB_ALL_PERMITS_JSON = f"{TTB_BASE}/system/files/frl/FRL_All_Permits.json"
TTB_NEW_PERMITS_CSV = f"{TTB_BASE}/system/files/frl/FRL_Basic_Permits_Issued_Since_the_Last_Publication.csv"

# Fallback dated paths (TTB sometimes uses dated directories)
TTB_ALL_PERMITS_JSON_DATED = f"{TTB_BASE}/system/files/{{date}}/FRL_All_Permits.json"
TTB_NEW_PERMITS_CSV_DATED = f"{TTB_BASE}/system/files/{{date}}/FRL_Basic_Permits_Issued_Since_the_Last_Publication.csv"

# Ollama (local LLM)
OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "qwen2.5:72b")

# Lead scoring thresholds
SCORE_HOT = 80      # Draft outreach immediately
SCORE_WARM = 50     # Enrich and watch
SCORE_COLD = 30     # Log but don't act

# Grryp business context (used in LLM prompts)
GRRYP_CONTEXT = """
Grryp is a custom tap handle company based in Fort Worth, Texas.
We design and manufacture unique, high-quality tap handles for craft breweries.
Our ideal customer is a brewery that is either:
- In planning / pre-opening (needs handles before launch)
- Recently opened (still setting up their taproom)
- Expanding (adding taps, rebranding, or opening new locations)

We ship nationwide. Our sweet spot is craft breweries with their own taproom.
Brewpubs and microbreweries are our primary market.
Large production breweries and contract brewers are lower priority.
"""
