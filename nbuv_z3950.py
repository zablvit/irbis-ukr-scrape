#!/usr/bin/env python3
# nbuv_z3950.py  —  connects to NB-UV Z39.50, merges into master CSV+SQLite
# --------------------------------------------------------

from __future__ import annotations
import os, re, sqlite3, sys
from pathlib import Path

import pandas as pd
from tqdm import tqdm
from unidecode import unidecode

try:
    from PyZ3950 import zoom
except ImportError:
    sys.exit("❌ PyZ3950 not installed.  pip install git+https://github.com/asl2/PyZ3950.git")

try:
    from pymarc import Record
except ImportError:
    sys.exit("❌ pymarc not installed.  pip install pymarc")

# ---------- CONFIG ----------

HOSTS   = ["z3950.nbuv.gov.ua", "irbis-nbuv.gov.ua"]   # try each until one works
PORTS   = [2100, 210]                                  # both are in use
DB      = "IRBIS"

QUERY = 'w=ukr and d>=1700 and d<=2024 and (g="художня" or g="поезія" or g="проза")'

CSV_OUT   = Path("ukrainian_literature_1700-2024.csv")
SQLITE_DB = Path("ukr_lit.sqlite")

YEAR_RE = re.compile(r"\b(17\d{2}|18\d{2}|19\d{2}|20[0-2]\d)\b")

# ---------- helpers ----------

def normal_key(author: str, title: str) -> str:
    return f"{unidecode(author).lower().strip()}|{unidecode(title).lower().strip()}"

def parse(record_bytes: bytes) -> dict | None:
    rec = Record(record_bytes)

    title  = rec.title() or ""
    author = rec.author() or ""
    if not title or not author:
        return None

    date_str = (rec.pubyear() or "") + " " + (rec['260']['c'] if rec['260'] and 'c' in rec['260'] else "")
    m = YEAR_RE.search(date_str)
    if not m:
        return None
    year = int(m.group())

    return {
        "title":          title.rstrip(" /.:;"),
        "author":         author.rstrip(" /.:;"),
        "year_written":   None,
        "year_published": year,
        "work_key":       normal_key(author, title),
    }

def connect_first_working() -> zoom.Connection:
    for h in HOSTS:
        for p in PORTS:
            try:
                conn = zoom.Connection(
                    h, p,
                    databaseName=DB,
                    preferredRecordSyntax="USMARC",
                    elementSetName="F"
                )
                _ = conn.search(zoom.Query("CCL", "w=ukr and d=2000"))  # tiny ping
                print(f"🔗 connected to {h}:{p}/{DB}")
                return conn
            except Exception as e:
                print(f"  …{h}:{p} no ({e.__class__.__name__})")
    raise RuntimeError("Could not connect to any host/port combo.")

# ---------- fetch ----------

def fetch() -> pd.DataFrame:
    conn = connect_first_working()
    rs = conn.search(zoom.Query("CCL", QUERY))
    rows = []
    print(f"▶ retrieving {len(rs):,} MARC records…")

    for r in tqdm(rs, unit="rec", smoothing=0.1):
        row = parse(r.data)
        if row:
            rows.append(row)

    conn.close()
    return pd.DataFrame(rows)

# ---------- merge/save ----------

def load_master() -> pd.DataFrame:
    if CSV_OUT.exists():
        df = pd.read_csv(CSV_OUT, dtype={"year_written": "Int64", "year_published": "Int64"})
        df["work_key"] = df.apply(lambda r: normal_key(r.author, r.title), axis=1)
        return df
    return pd.DataFrame(columns=["title","author","year_written","year_published","work_key"])

def save(df: pd.DataFrame):
    df.drop(columns="work_key").to_csv(CSV_OUT, index=False, encoding="utf-8")
    with sqlite3.connect(SQLITE_DB) as conn:
        df.drop(columns="work_key").to_sql("combined", conn, if_exists="replace", index=False)
    print(f"💾 saved {len(df):,} unique works → CSV & SQLite")

# ---------- main ----------

if __name__ == "__main__":
    try:
        new = fetch()
        print(f"✅ fetched {len(new):,} usable rows")
        master = load_master()
        combined = (
            pd.concat([master, new], ignore_index=True)
              .drop_duplicates("work_key", keep="first")
              .sort_values(["year_written","year_published"], na_position="last")
              .reset_index(drop=True)
        )
        save(combined)
    except KeyboardInterrupt:
        sys.exit("\n⏹️  interrupted")
    except Exception as e:
        sys.exit(f"🛑 {e.__class__.__name__}: {e}")