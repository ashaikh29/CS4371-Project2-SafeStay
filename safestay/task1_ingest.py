"""Task 1 - Ingest: download every 2022-2024 San Francisco incident report and save it as Parquet.

    python -m safestay.task1_ingest                 # full download
    python -m safestay.task1_ingest --max-pages 1   # quick test: only the first page

Pages through the DataSF Socrata API with $limit/$offset (ordered by the system row id :id
so paging is stable). Every page is cached under cache/socrata/sanfrancisco/... with its
retrieval time; re-running reuses the cache and only downloads pages that are missing.
The cached pages are then combined with Spark into data/raw/incidents.parquet.
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql import types as T

from safestay import config
from safestay.http_cache import cache_key, get_json

DEFAULT_PAGE_SIZE = 50_000  # the largest $limit Socrata's SODA 2 endpoints accept
RAW_PARQUET = config.RAW_DIR / "incidents.parquet"


def incidents_query(page_size: int) -> dict:
    """The Socrata query, without $offset."""
    return {
        "$select": ",".join(config.INCIDENT_FIELDS),
        "$where": (f"{config.DATE_FIELD} between '{config.START_DATE}' "
                   f"and '{config.END_DATE}'"),
        "$order": ":id",
        "$limit": page_size,
    }


def query_cache_dir(page_size: int) -> Path:
    """Folder holding the cached pages of this exact query (changing the query changes it)."""
    return (config.CACHE_DIR / "socrata" / "sanfrancisco" / config.INCIDENTS_DATASET
            / cache_key(config.INCIDENTS_URL, incidents_query(page_size)))


def auth_headers() -> dict:
    token = config.socrata_app_token()
    return {"X-App-Token": token} if token else {}


def download_pages(page_size: int, max_pages: int | None) -> list[Path]:
    base = incidents_query(page_size)
    out_dir = query_cache_dir(page_size)
    if not auth_headers():
        print("WARNING: SOCRATA_APP_TOKEN is not set; requests may be throttled.")
    print(f"Downloading {config.CITY_NAME} incidents {config.START_DATE[:10]} to "
          f"{config.END_DATE[:10]} from {config.INCIDENTS_URL}\n  cache: {out_dir}")

    pages: list[Path] = []
    offset, page_no, total, started = 0, 0, 0, time.monotonic()
    while max_pages is None or page_no < max_pages:
        page_file = out_dir / f"page_{page_no:05d}.json"
        record = get_json(config.INCIDENTS_URL, page_file,
                          params={**base, "$offset": offset}, headers=auth_headers())
        rows = record["data"]
        if not isinstance(rows, list):
            raise RuntimeError(f"Unexpected response on page {page_no}: {str(rows)[:300]}")
        if rows:
            pages.append(page_file)
        total += len(rows)
        source = "cache" if record["from_cache"] else "API"
        print(f"  page {page_no:4d}  offset {offset:>9,}  rows {len(rows):>6,}  "
              f"total {total:>10,}  ({source}, {time.monotonic() - started:,.0f}s)")
        if len(rows) < page_size:
            break  # last page
        offset += page_size
        page_no += 1
    return pages


def pages_to_parquet(pages: list[Path]) -> Path:
    """Combine the cached JSON pages into one raw Parquet dataset with Spark."""
    from safestay.spark_session import get_spark

    spark = get_spark("SafeStay-Task1-Ingest")
    row_schema = T.StructType([T.StructField(f, T.StringType())
                               for f in config.INCIDENT_FIELDS])
    page_schema = T.StructType([
        T.StructField("retrieved_at", T.StringType()),
        T.StructField("data", T.ArrayType(row_schema)),
    ])
    pages_df = spark.read.schema(page_schema).option("multiLine", True).json(
        [str(p) for p in pages])
    raw = (pages_df
           .select("retrieved_at", F.explode("data").alias("r"))
           .select(*[F.col(f"r.{f}").alias(f) for f in config.INCIDENT_FIELDS],
                   "retrieved_at"))

    raw.write.mode("overwrite").parquet(str(RAW_PARQUET))
    n = spark.read.parquet(str(RAW_PARQUET)).count()
    print(f"Raw Parquet written: {RAW_PARQUET} ({n:,} rows)")
    meta = {"dataset": config.INCIDENTS_URL, "pages": len(pages), "rows": n,
            "page_files": [str(p) for p in pages]}
    (RAW_PARQUET.parent / "ingest_meta.json").write_text(json.dumps(meta, indent=2))
    return RAW_PARQUET


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    parser.add_argument("--max-pages", type=int, default=None,
                        help="stop after this many pages (for testing)")
    args = parser.parse_args()

    pages = download_pages(args.page_size, args.max_pages)
    if not pages:
        raise SystemExit("No rows were downloaded; check your network connection.")
    pages_to_parquet(pages)


if __name__ == "__main__":
    main()
