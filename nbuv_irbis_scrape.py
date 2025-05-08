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
import re, sqlite3, time
from pathlib import Path
from typing import Dict, List

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from unidecode import unidecode

# -------------------------------------------------- CONSTANTS ---------

BASE_CGI   = "https://irbis-nbuv.gov.ua/cgi-bin/irbis_ir/cgiirbis_64.exe"
START_GET  = (
    "https://irbis-nbuv.gov.ua/cgi-bin/irbis_ir/cgiirbis_64.exe?"
    "S21CNR=20&S21REF=10&S21STN=1&C21COM=S&I21DBN=ELIB&P21DBN=ELIB"
    "&S21All=(<.>J=ukr<.>)&S21FMT=preitem&S21SRW=dz&S21SRD=UP"
)
PAGE_SIZE  = 20                        # records per page
CSV_PATH   = Path("ukrainian_literature_1700-2024.csv")
SQLITE_DB  = Path("ukr_lit.sqlite")
YEAR_RE    = re.compile(r"\b(17\d{2}|18\d{2}|19\d{2}|20[0-2]\d)\b")

HEADERS = {
    "User-Agent": "irbis-scraper/0.1 (mailto:you@example.com)",
}

# -------------------------------------------------- HELPERS ---------

def key(author: str, title: str) -> str:
    return f"{unidecode(author).lower().strip()}|{unidecode(title).lower().strip()}"

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
    Walk one preitem list‑page and return rows
    (title, author, year, work_key).
    """
    out = []
    for a in soup.select('a[href*="S21FMT=fullwebr"]'):
        title = a.get_text(strip=True)
        if not title:
            continue

        # author sits in <em> inside the same <p>
        p = a.find_parent("p")
        author = ""
        if p:
            em = p.find("em")
            if em:
                author = em.get_text(strip=True)

        # year = first 4‑digit number in the surrounding row
        tr = a.find_parent("tr")
        block = str(tr) if tr else a.parent
        m = YEAR_RE.search(block)
        year = int(m.group()) if m else 0
        if not author or year == 0:
            continue

        if any(x in title.lower() for x in ("п'єс", "драма", "комедія", "есе")):
            continue

        out.append({
            "title":           title,
            "author":          author,
            "year_written":    None,
            "year_published":  year,
            "work_key":        key(author, title),
        })
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
            "P21DBN": "ELIB",
            "I21DBN": "ELIB",
            "S21FMT": "preitem",
            "S21ALL": "(<.>RPUB=!<.>)*(<.>J=ukr<.>)",
            "S21CNR": str(PAGE_SIZE),
            "S21REF": str(start_rec),
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

        start_rec += PAGE_SIZE
        pbar.update()

        # IRBIS returns same page if index too high → stop
        if str(start_rec) not in page.text:
            break

        time.sleep(0.4)   # polite delay

    pbar.close()
    return pd.DataFrame(rows)

# -------------------------------------------------- MERGE / SAVE ----

def load_master() -> pd.DataFrame:
    if not CSV_PATH.exists() or CSV_PATH.stat().st_size == 0:
        return pd.DataFrame()

    df = pd.read_csv(CSV_PATH,
                     dtype={"year_written":"Int64","year_published":"Int64"})
    if not df.empty and "work_key" not in df.columns:
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
