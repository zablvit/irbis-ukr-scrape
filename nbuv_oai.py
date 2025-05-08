#!/usr/bin/env python3
# nbuv_oai.py  –  harvest Vernadsky IRBIS via OAI-PMH

"""
Pull every Ukrainian-language record whose publication year is 1700-2024,
filter to fiction & poetry, then append to the master CSV/SQLite
(ukrainian_literature_1700-2024.csv, ukr_lit.sqlite).

Requires:  pip install sickle pymarc pandas unidecode lxml tqdm
"""

from __future__ import annotations
import re, sqlite3, time
from pathlib import Path

import pandas as pd
from sickle import Sickle
from sickle.iterator import OAIItemIterator
from pymarc import parse_xml_to_array
from tqdm import tqdm
from unidecode import unidecode

# --------------------------- CONFIG ---------------------------------

OAI_BASE = "https://irbis-nbuv.gov.ua/cgi-bin/irbis64r_2018/oai"
SET      = "ELIB"                       # IRBIS database = digital library
YEAR_MIN, YEAR_MAX = 1700, 2024
CSV_PATH   = Path("ukrainian_literature_1700-2024.csv")
SQLITE_DB  = Path("ukr_lit.sqlite")
YEAR_RE = re.compile(r"\b(17\d{2}|18\d{2}|19\d{2}|20[0-2]\d)\b")

# --------------------------- HELPERS --------------------------------

def key(author: str, title: str) -> str:
    return f"{unidecode(author).lower().strip()}|{unidecode(title).lower().strip()}"

def parse_marcxml(xml_bytes: bytes) -> dict | None:
    recs = parse_xml_to_array(xml_bytes)
    if not recs:
        return None
    rec = recs[0]

    title  = (rec.title() or "").rstrip(" /.:;")
    author = (rec.author() or "")
    if not title or not author:
        return None

    raw_date = (rec.pubyear() or "") + " " + (rec["260"]["c"] if rec["260"] and "c" in rec["260"] else "")
    m = YEAR_RE.search(raw_date)
    if not m:
        return None
    year = int(m.group())
    if not (YEAR_MIN <= year <= YEAR_MAX):
        return None

    # very coarse genre filter – drop drama & essay keywords
    lower = title.lower()
    if any(x in lower for x in ("п'єс", "драма", "комедія", "трагедія", "есе")):
        return None

    return {
        "title":           title,
        "author":          author,
        "year_written":    None,
        "year_published":  year,
        "work_key":        key(author, title),
    }

# --------------------------- HARVEST --------------------------------

def harvest() -> pd.DataFrame:
    sickle = Sickle(OAI_BASE)
    it: OAIItemIterator = sickle.ListRecords(
        metadataPrefix="marcxml",
        set=SET,
    )
    rows = []
    for item in tqdm(it, unit="rec"):
        row = parse_marcxml(item.raw)
        if row:
            rows.append(row)
    return pd.DataFrame(rows)

# --------------------------- MERGE / SAVE ---------------------------

def load_master() -> pd.DataFrame:
    if CSV_PATH.exists():
        df = pd.read_csv(CSV_PATH, dtype={"year_written": "Int64", "year_published": "Int64"})
        df["work_key"] = df.apply(lambda r: key(r.author, r.title), axis=1)
        return df
    return pd.DataFrame(columns=["title","author","year_written","year_published","work_key"])

def save(df: pd.DataFrame):
    df.drop(columns="work_key").to_csv(CSV_PATH, index=False, encoding="utf-8")
    with sqlite3.connect(SQLITE_DB) as cx:
        df.drop(columns="work_key").to_sql("combined", cx, if_exists="replace", index=False)
    print(f"💾  saved {len(df):,} unique works → CSV & SQLite")

# --------------------------- MAIN ----------------------------------

if __name__ == "__main__":
    t0 = time.time()
    new = harvest()
    print(f"✅  harvested {len(new):,} candidate rows in {time.time()-t0:.0f}s")

    master = load_master()
    combined = (
        pd.concat([master, new], ignore_index=True)
          .drop_duplicates("work_key", keep="first")
          .sort_values(["year_written","year_published"], na_position="last")
          .reset_index(drop=True)
    )
    save(combined)