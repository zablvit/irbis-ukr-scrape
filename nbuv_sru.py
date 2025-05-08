"""
nbuv_sru.py  ―  Vernadsky Library SRU fetcher
=============================================

1. Hits the SRU endpoint of the National Library of Ukraine (IRBIS-64).
2. Retrieves MARCXML batches (100 recs per request) for *Ukrainian*
   fiction & poetry, years 1700-2024.
3. Extracts title (245$a), author (100$a / 700$a) and year_published
   (260$c or 264$c YY[YY]…).
4. Appends to the existing CSV/SQLite produced by wikidata.py,
   removes dupes (same author|title) and re-saves both files.

Requires:  requests, lxml, pymarc, pandas, unidecode
"""

from __future__ import annotations

import re
import sqlite3
import time
from pathlib import Path
from typing import Generator

import pandas as pd
import requests
from lxml import etree
from pymarc import MARCReader, record_to_xml
from unidecode import unidecode

# ---------------------------------------------------------------------------
# 0.  Config
# ---------------------------------------------------------------------------

SRU_BASE = "http://irbis-nbuv.gov.ua/cgi-bin/irbis64r_2018/sru"
RECORDS_PER_PAGE = 100

YEAR_FROM = 1700
YEAR_TO   = 2024

# CQL query – adjust if you find a better index combo.
# `lng=ukr`      (language), `vid=художня` (subject/genre).
# Some indexes differ per install; test in your browser first.
CQL = (
    'language="ukr" AND '
    f'date>={YEAR_FROM} AND date<={YEAR_TO} AND '
    '(subject any "художня" OR subject any "поезія" OR subject any "проза")'
)

CSV_PATH = Path("ukrainian_literature_1700-2024.csv")   # combined master
SQLITE_DB = Path("ukr_lit.sqlite")                      # combined master

# ---------------------------------------------------------------------------
# 1.  Helpers
# ---------------------------------------------------------------------------


def sru_iter() -> Generator[bytes, None, None]:
    """Yield MARCXML `<record>` chunks from SRU."""
    start = 1
    while True:
        params = {
            "operation": "searchRetrieve",
            "version": "1.2",
            "query": CQL,
            "recordSchema": "marcxml",
            "maximumRecords": RECORDS_PER_PAGE,
            "startRecord": start,
        }
        r = requests.get(SRU_BASE, params=params, timeout=30)
        r.raise_for_status()

        xml = etree.fromstring(r.content)
        ns = {"srw": "http://www.loc.gov/zing/srw/"}
        total = int(xml.xpath("string(//srw:numberOfRecords)", namespaces=ns) or "0")
        recs = xml.xpath("//srw:record/srw:recordData/marc:record",
                         namespaces={**ns, "marc": "http://www.loc.gov/MARC21/slim"})

        if not recs:
            break

        for rec in recs:
            yield etree.tostring(rec)

        start += RECORDS_PER_PAGE
        if start > total:
            break
        time.sleep(0.7)  # be polite


YEAR_RE = re.compile(r"(\d{4})")


def parse_marcxml(xml_bytes: bytes) -> dict | None:
    """Return dict with title / author / year_published (int) or None."""
    rec = next(MARCReader(record_to_xml(xml_bytes), to_unicode=True))

    def f(tag, code="a"):
        field = rec.get_fields(tag)
        return field[0][code] if field else ""

    title = f("245")
    if not title:
        return None

    # prefer 100$a (main author) else first 700$a
    author = f("100") or f("700")
    if not author:
        return None

    # pick first four-digit year in 260$c or 264$c
    date_str = f("260", "c") or f("264", "c")
    m = YEAR_RE.search(date_str)
    if not m:
        return None
    year = int(m.group(1))

    return {
        "title": title.strip(" /:;"),
        "author": author.strip(" /:;"),
        "year_written": None,          # MARC lacks this
        "year_published": year,
    }


def normal_key(author: str, title: str) -> str:
    return f"{unidecode(author).lower().strip()}|{unidecode(title).lower().strip()}"


# ---------------------------------------------------------------------------
# 2.  Fetch & build DataFrame
# ---------------------------------------------------------------------------


def fetch_nbuv() -> pd.DataFrame:
    rows = []
    for xml_rec in sru_iter():
        row = parse_marcxml(xml_rec)
        if row:
            rows.append(row)
    df = pd.DataFrame(rows)
    df["work_key"] = df.apply(lambda r: normal_key(r.author, r.title), axis=1)
    return df


# ---------------------------------------------------------------------------
# 3.  Merge with existing master CSV
# ---------------------------------------------------------------------------


def load_master() -> pd.DataFrame:
    if CSV_PATH.exists():
        df = pd.read_csv(CSV_PATH, dtype={"year_written": "Int64", "year_published": "Int64"})
        df["work_key"] = df.apply(lambda r: normal_key(r.author, r.title), axis=1)
        return df
    return pd.DataFrame(columns=["title", "author", "year_written", "year_published", "work_key"])


def save_master(df: pd.DataFrame):
    df.drop(columns="work_key").to_csv(CSV_PATH, index=False, encoding="utf-8")
    with sqlite3.connect(SQLITE_DB) as conn:
        df.drop(columns="work_key").to_sql("combined", conn, if_exists="replace", index=False)
    print(f"✔ Combined set → {len(df):,} rows; CSV & SQLite updated.")


# ---------------------------------------------------------------------------
# 4.  CLI entry-point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("⇢ Fetching Vernadsky (SRU)… this may take a while")
    df_nb = fetch_nbuv()
    print(f"   SRU returned {len(df_nb):,} candidate rows")

    master = load_master()
    combined = (
        pd.concat([master, df_nb], ignore_index=True)
          .sort_values(["year_written", "year_published"], na_position="last")
          .drop_duplicates("work_key", keep="first")
          .reset_index(drop=True)
    )

    save_master(combined)
