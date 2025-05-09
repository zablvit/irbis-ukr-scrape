#!/usr/bin/env python3
# nbuv_irbis_scrape.py  –  scrape Vernadsky IRBIS HTML

"""
1. Opens your start URL (Ukrainian-language filter).
2. Captures the session token `Z21ID` IRBIS issues on the first page.
3. Posts the **form data** you pasted (`S21STN` = start record) in a loop
   (20 records per page), each time bumping `S21STN`.
4. From every “preitem” list page collects all links that contain
   `S21FMT=fullwebr`, follows them, and extracts:
      • title      (label “Назва(и):”)
      • author(s)  (label “Автор(и):”)
      • first 4-digit year  (label “Дата:” or anywhere in the record)
5. Appends rows to `ukrainian_literature_1700-2024.csv`, merges/dedups
   against anything already there, re-writes the combined CSV and the
   SQLite table `combined` in `ukr_lit.sqlite`.
"""

from __future__ import annotations
import re, sqlite3, logging
from pathlib import Path
from typing import Dict, List

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from unidecode import unidecode

# -------------------------------------------------- CONSTANTS ---------

BASE_CGI   = "https://irbis-nbuv.gov.ua/cgi-bin/ua/elib.exe"
START_GET  = (
    "https://irbis-nbuv.gov.ua/cgi-bin/ua/elib.exe?"
    "S21CNR=20&S21REF=10&S21STN=1&C21COM=S&I21DBN=UKRLIB&P21DBN=UKRLIB"
    "&S21All=(<.>J=ukr<.>)&S21FMT=preitem&S21SRW=dz&S21SRD=UP"
)
PAGE_SIZE  = 20                        # records per page
CSV_PATH   = Path("ukrainian_literature_1700-2024.csv")
SQLITE_DB  = Path("ukr_lit.sqlite")
YEAR_RE    = re.compile(r"\b(17\d{2}|18\d{2}|19\d{2}|20[0-2]\d)\b")

ENC_S21ALL = "%28%3C.%3EJ%3Dukr%3C.%3E%29"   # encoded (<.>J=ukr<.>)

HEADERS = {
    "User-Agent": "irbis-scraper/0.1 (mailto:you@example.com)",
}

logging.basicConfig(level=logging.INFO, format="%(message)s")

# -------------------------------------------------- HELPERS ---------

def _s(val) -> str:
    """Return a clean string; convert NaN/None/float to empty str."""
    if val is None:
        return ""
    if isinstance(val, float):  # catches NaN
        return "" if pd.isna(val) else str(val)
    return str(val)

def key(author: str, title: str) -> str:
    return (
        f"{unidecode(_s(author)).lower().strip()}|"
        f"{unidecode(_s(title)).lower().strip()}"
    )

def get_soup(session: requests.Session, url: str, encoding="cp1251") -> BeautifulSoup:
    r = session.get(url, headers=HEADERS, timeout=30)
    # use server hint else fall back to cp1251
    r.encoding = r.encoding or "cp1251"
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def post_soup(session: requests.Session, data: Dict[str, str], encoding="cp1251") -> BeautifulSoup:
    r = session.post(BASE_CGI, headers=HEADERS, data=data, timeout=30)
    # use server hint else fall back to cp1251
    r.encoding = r.encoding or "cp1251"
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def extract_hits(soup: BeautifulSoup) -> List[dict]:
    """
    Parse one preitem list‑page.
    Handles two link styles:
        1) classic    …S21FMT=fullwebr
        2) DLib item  /dlib/item/0001234
    Extracts title, author (if <em> present), first 4‑digit year.
    """
    out = []

    for tr in soup.find_all("tr"):
        b_tag = tr.find("b")
        if not b_tag or not re.match(r"\d+\.", b_tag.get_text()):
            continue                # not a record row

        # pick first link that looks like a record
        link = tr.find("a", href=re.compile(r"S21FMT=fullwebr|/dlib/item/"))
        if not link:
            continue

        title = link.get_text(strip=True)
        if not title:
            continue

        em = tr.find("em")
        author = em.get_text(strip=True) if em else ""

        # year: try the dedicated <span>YYYY</span> cell; ignore row‑index numbers
        yr_span = tr.find("span", string=re.compile(r"^\d{4}$"))
        year = int(yr_span.text) if yr_span else None

        out.append({
            "title":           title,
            "author":          author,
            "year_written":    None,
            "year_published":  year if year else None,
            "work_key":        key(author, title),
        })

    logging.info(f"   extracted {len(out)} rows from page")
    return out

# -------------------------------------------------- SCRAPER ---------

def harvest() -> pd.DataFrame:
    sess = requests.Session()
    rows: List[dict] = []

    # 1) first GET → scrape it **and** pull the session token
    resp = sess.get(START_GET, headers=HEADERS, timeout=30)
    resp.encoding = resp.encoding or "cp1251"
    html  = resp.text
    page  = BeautifulSoup(html, "lxml")
    rows.extend(extract_hits(page))

    m = re.search(r"Z21ID=([^&\"'>]+)", html)
    z_param = m.group(1) if m else ""

    # 2) iterate the remaining pages via POST
    start_rec = 1
    pbar = tqdm(unit="page")
    while True:
        pbar.set_description(f"page {start_rec}")
        # Form data for this page
        data = {
            "C21COM": "S",
            "P21DBN": "UKRLIB",
            "I21DBN": "UKRLIB",
            "S21FMT": "preitem",
            "S21ALL": ENC_S21ALL,
            "S21CNR": str(PAGE_SIZE),
            "S21REF": "10",        # IRBIS expects constant ref index
            "S21SRD": "UP",
            "S21SRW": "dz",
            "S21STN": str(start_rec),
            "Z21ID":  z_param,         # keep the same session
        }

        page = post_soup(sess, data)
        got = extract_hits(page)
        if not got:
            break
        rows.extend(got)

        # --- determine next page via last ordinal number on current page ---
        indices = [int(n) for n in re.findall(r"<b>(\d+)\.</b>", str(page))]
        if not indices:
            # fallback: pick min hidden S21STN > current
            hidden = [int(v) for v in re.findall(r'name="S21STN"\s+value="(\d+)"', str(page))]
            cands = [v for v in hidden if v > start_rec]
            if not cands:
                logging.info("no next S21STN found – stop")
                break
            next_start = min(cands)
        else:
            next_start = max(indices) + 1

        logging.info(f"page done, next S21STN = {next_start}")

        if next_start <= start_rec or next_start > 40000:   # safety cap
            break
        start_rec = next_start

        pbar.update()

    pbar.close()
    return pd.DataFrame(rows)

# -------------------------------------------------- MERGE / SAVE ----

def load_master() -> pd.DataFrame:
    if not CSV_PATH.exists() or CSV_PATH.stat().st_size == 0:
        return pd.DataFrame()

    df = pd.read_csv(CSV_PATH,
                     dtype={"year_written":"Int64","year_published":"Int64"})
    if not df.empty:
        if "work_key" in df.columns:
            df = df.drop(columns=["work_key"])
        df["work_key"] = df.apply(lambda r: key(r.author, r.title), axis=1)
    return df

def save(df: pd.DataFrame):
    df.drop(columns="work_key").to_csv(CSV_PATH, index=False, encoding="utf-8")
    with sqlite3.connect(SQLITE_DB) as cx:
        df.drop(columns="work_key").to_sql("combined", cx, if_exists="replace", index=False)
    print(f"💾  saved {len(df):,} unique works  → CSV & SQLite")

# -------------------------------------------------- MAIN ------------

if __name__ == "__main__":
    new = harvest()
    print(f"✅  scraped {len(new):,} rows")

    master = load_master()
    merged = (
        pd.concat([master, new], ignore_index=True)
          .drop_duplicates("work_key", keep="first")
          .sort_values("year_published")
          .reset_index(drop=True)
    )
    save(merged)
