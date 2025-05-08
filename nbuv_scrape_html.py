#!/usr/bin/env python3
# -----------------------------------------------------------
#  nbuv_scrape_html.py
# -----------------------------------------------------------
"""
Scrape the "Цифрова бібліотека" (digital library) of the
Vernadsky National Library for *all* Ukrainian-language publications
and save them to CSV + SQLite.

* Starting URL = the one Vitaliy pasted (sorted by year).
* Navigates with the built-in "Наступні" / "Далі" link that carries
  a fresh `Z21ID` session id on every page.
* For each list entry it follows `S21FMT=fullwebr` to pick up structured
  fields (title, author, date).

The output lives in:

    ukrainian_literature_1700-2024.csv
    ukr_lit.sqlite   (table = combined)
"""

from __future__ import annotations

import re
import sqlite3
import sys
import time
from pathlib import Path
from typing import Iterator, List, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from unidecode import unidecode

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

START_URL = (
    "https://irbis-nbuv.gov.ua/cgi-bin/irbis_ir/cgiirbis_64.exe?"
    "C21COM=S&I21DBN=ELIB&P21DBN=ELIB"
    "&S21FMT=preitem"
    "&S21ALL=(<.>RPUB=!<.>)*(<.>J=ukr<.>)"
    "&S21SRW=GOD&S21SRD=UP&S21STN=1&S21CNR=20"
)

HEADERS = {
    "User-Agent": "ukr-lit-html-scraper/0.1 (mailto:you@example.com)",
}

CSV_PATH = Path("ukrainian_literature_1700-2024.csv")
SQLITE_DB = Path("ukr_lit.sqlite")

YEAR_RE = re.compile(r"\b(1[789]\d{2}|20[0-2]\d|1700)\b")  # 1700-2024


# ---------------------------------------------------------------------------
# CORE
# ---------------------------------------------------------------------------


def normal_key(author: str, title: str) -> str:
    return (
        f"{unidecode(author).lower().strip()}|{unidecode(title).lower().strip()}"
    )


def get_soup(session: requests.Session, url: str) -> BeautifulSoup:
    for _ in range(3):
        r = session.get(url, headers=HEADERS, timeout=30)
        if r.status_code == 200:
            return BeautifulSoup(r.content, "lxml")
        time.sleep(1.5)
    raise RuntimeError(f"Cannot fetch {url} – status {r.status_code}")


def extract_record(session: requests.Session, url: str) -> Tuple[str, str, int]:
    """Return (title, author, year) from the fullwebr page."""
    soup = get_soup(session, url)

    # title = first <h1> or bold block before "Назва(и):"
    h = soup.find("h1")
    title = h.get_text(strip=True) if h else ""

    # try to find explicit label
    label = soup.find(text=re.compile(r"Назва", re.I))
    if label:
        blk = label.find_parent()
        if blk:
            title = blk.get_text(" ", strip=True).replace("Назва(и):", "").strip()

    # author
    author = ""
    a_lab = soup.find(text=re.compile(r"Автор", re.I))
    if a_lab:
        author = (
            a_lab.find_parent().get_text(" ", strip=True).replace("Автор(и):", "").strip()
        )

    # date/year
    year = None
    d_lab = soup.find(text=re.compile(r"Дата", re.I))
    if d_lab:
        txt = d_lab.find_parent().get_text(" ", strip=True)
        m = YEAR_RE.search(txt)
        if m:
            year = int(m.group())

    # as fallback search the whole page
    if year is None:
        m2 = YEAR_RE.search(soup.get_text(" ", strip=True))
        if m2:
            year = int(m2.group())

    if not title:
        raise ValueError("no title?!")

    return title, author or "", year or 0


def iter_result_pages(session: requests.Session, start_url: str) -> Iterator[str]:
    """Yield result-page URLs in order, following 'Наступні' links."""
    url = start_url
    while url:
        yield url
        soup = get_soup(session, url)
        nxt = soup.find("a", string=re.compile("Наступн|Далі", re.I))
        url = None
        if nxt and nxt.has_attr("href"):
            url = requests.compat.urljoin(url or start_url, nxt["href"])


def collect_rows() -> List[dict]:
    session = requests.Session()
    rows = []

    for page_url in iter_result_pages(session, START_URL):
        page = get_soup(session, page_url)
        print("📄 scanning", page_url.split("S21STN=")[-1])

        # each record link has S21FMT=fullwebr
        for a in page.find_all("a", href=re.compile("S21FMT=fullwebr")):
            rec_url = requests.compat.urljoin(page_url, a["href"])
            try:
                title, author, year = extract_record(session, rec_url)
            except Exception as exc:
                print("  ⤫", exc, rec_url[:80])
                continue

            rows.append(
                {
                    "title": title,
                    "author": author,
                    "year_written": None,
                    "year_published": year,
                    "work_key": normal_key(author, title),
                }
            )

        # tiny pause between pages
        time.sleep(0.6)

    return rows


# ---------------------------------------------------------------------------
# SAVE / MERGE
# ---------------------------------------------------------------------------


def load_master() -> pd.DataFrame:
    if CSV_PATH.exists():
        df = pd.read_csv(
            CSV_PATH, dtype={"year_written": "Int64", "year_published": "Int64"}
        )
        df["work_key"] = df.apply(lambda r: normal_key(r.author, r.title), axis=1)
        return df
    return pd.DataFrame(
        columns=["title", "author", "year_written", "year_published", "work_key"]
    )


def save_master(df: pd.DataFrame):
    df.drop(columns="work_key").to_csv(CSV_PATH, index=False, encoding="utf-8")
    with sqlite3.connect(SQLITE_DB) as conn:
        df.drop(columns="work_key").to_sql(
            "combined", conn, if_exists="replace", index=False
        )
    print(f"✔ Updated CSV & SQLite  —  {len(df):,} total rows")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------


def main():
    rows = collect_rows()
    df_new = pd.DataFrame(rows)
    print(f"\n🔎 scraped {len(df_new):,} rows")

    master = load_master()
    combined = (
        pd.concat([master, df_new], ignore_index=True)
        .drop_duplicates("work_key", keep="first")
        .sort_values(["year_written", "year_published"], na_position="last")
        .reset_index(drop=True)
    )

    save_master(combined)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit("\n⤵ interrupted by user")
