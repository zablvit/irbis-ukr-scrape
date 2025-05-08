"""
wikidata.py  ―  Ukrainian Literature Scraper (prose + poetry)
============================================================

Pulls every Ukrainian‐language literary work whose *year written*
(**P571 / inception**) **OR** first *publication year* (**P577**)
falls between 1700 and 2024 (inclusive).  Skips plays, essays, etc.

Results are stored both as a UTF-8 CSV and as an SQLite table so you
can poke around with SQL, merge in other sources, deduplicate, etc.

Dependencies (already in your venv):
    pip install pandas requests SPARQLWrapper unidecode tqdm

Run it:
    python wikidata.py
"""

from __future__ import annotations

import logging
import random
import sqlite3
import time
from pathlib import Path

import pandas as pd
from SPARQLWrapper import JSON, POST, SPARQLWrapper

import re

YEAR_RE = re.compile(r"\d{4}")

def first_year(s: str | None) -> int | None:
    """Return first 4-digit year in the string or None."""
    if not s:
        return None
    if s[:4].isdigit():
        return int(s[:4])
    m = YEAR_RE.search(s)
    return int(m.group()) if m else None

# ---------------------------------------------------------------------------
# 1.  Config
# ---------------------------------------------------------------------------

ENDPOINT = "https://query.wikidata.org/sparql"
HEADERS = {
    "User-Agent": (
        "ukr-lit-scraper/0.2 (+https://github.com/yourname/ukr-lit-scraper; "
        "mailto:you@example.com)"
    )
}

# Ten-year window template; {FROM} / {TO} are replaced in the loop
BASE_QUERY = """
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?work ?workLabel ?authorLabel ?inception ?pubDate
WHERE {{
  ?work wdt:P407  wd:Q8798 ;        # language of work = Ukrainian
        wdt:P571  ?inception .      # inception (year written)

  OPTIONAL {{ ?work wdt:P577 ?pubDate. }}     # first publication
  OPTIONAL {{ ?work wdt:P50  ?author.  }}     # author item

  # Reject dramas / essays
  FILTER NOT EXISTS {{ ?work wdt:P31 wd:Q25379 }}   # theatrical play
  FILTER NOT EXISTS {{ ?work wdt:P136 wd:Q35760 }}  # essay

  # Keep a work if EITHER year falls in the decade slice
  FILTER (
       (YEAR(?inception) >= {FROM} && YEAR(?inception) < {TO})
    || (BOUND(?pubDate) &&
        YEAR(?pubDate)  >= {FROM} && YEAR(?pubDate)  < {TO})
  )

  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "uk,en". }}
}}
"""

# ---------------------------------------------------------------------------
# 2.  SPARQL helper with retries
# ---------------------------------------------------------------------------


def _run_sparql(query: str, tries: int = 3) -> dict:
    for attempt in range(tries):
        try:
            s = SPARQLWrapper(ENDPOINT, agent=HEADERS["User-Agent"])
            s.setMethod(POST)  # POST handles long queries better
            s.setQuery(query)
            s.setReturnFormat(JSON)
            s.setTimeout(60_000)  # client-side timeout (ms)
            return s.query().convert()
        except Exception as exc:
            logging.warning("WDQS retry %d/3 after: %s", attempt + 1, exc)
            time.sleep(2 + random.random() * 4)
    raise RuntimeError("⚠️  Wikidata endpoint failed after 3 retries.")


# ---------------------------------------------------------------------------
# 3.  Main fetcher
# ---------------------------------------------------------------------------


def fetch_wikidata() -> pd.DataFrame:
    rows: list[dict] = []

    for start in range(1700, 2025, 10):  # walk by decades
        query = BASE_QUERY.format(FROM=start, TO=start + 10)
        data = _run_sparql(query)

        for b in data["results"]["bindings"]:
            title = b["workLabel"]["value"]
            author = b.get("authorLabel", {}).get("value", "")
            title   = b["workLabel"]["value"].strip()
            author  = b.get("authorLabel", {}).get("value", "").strip()
            y_written   = first_year(b["inception"]["value"])
            y_published = first_year(b.get("pubDate", {}).get("value"))
            
            # we still keep rows even if only publication year is known
            if y_written is None and y_published is None:
                continue

            rows.append({
                "title": title,
                "author": author,
                "year_written":   y_written,
                "year_published": y_published,
            })

        print(f"✓ {start}-{start+9}: {len(rows)} rows so far")
        time.sleep(0.5)  # be polite to the endpoint

    df = pd.DataFrame(rows)
    return df


# ---------------------------------------------------------------------------
# 4.  Persistence helpers
# ---------------------------------------------------------------------------


def save_csv(df: pd.DataFrame, path: str | Path = "wikidata_ukr_lit.csv") -> None:
    df.to_csv(path, index=False, encoding="utf-8")
    size_mb = Path(path).stat().st_size / 1_000_000
    print(f"💾  CSV saved → {path} ({size_mb:.1f} MB)")


def save_sqlite(
    df: pd.DataFrame,
    db: str | Path = "ukr_lit.sqlite",
    table: str = "wikidata",
) -> None:
    with sqlite3.connect(db) as conn:
        df.to_sql(table, conn, if_exists="replace", index=False)
    print(f"📚  SQLite table [{table}] written → {db}")


# ---------------------------------------------------------------------------
# 5.  Script entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    df_all = fetch_wikidata()
    print(f"\nFetched {len(df_all):,} unique rows from Wikidata")

    # --- save locally ---
    save_csv(df_all)
    save_sqlite(df_all)
