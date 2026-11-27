"""Task 1 - Clean, classify and summarise the San Francisco incidents with Spark.

    python -m safestay.task1_pipeline

Reads data/raw/incidents.parquet (written by task1_ingest) and writes:
  outputs/neighbourhood_month.parquet  one row per neighbourhood, year and month
  outputs/t1_summary.txt               row counts before/after every cleaning step, etc.
  outputs/t1_category_mapping.csv      every incident_category seen, its category and count
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone

from pyspark import StorageLevel
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from safestay import config
from safestay.spark_session import get_spark
from safestay.task1_ingest import RAW_PARQUET

NULL_MARKERS = ["", "(NULL)", "NULL", "NONE", "N/A", "NAN"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def clean_text(col: str):
    """Trimmed string, or null for empty / placeholder values such as '(null)'."""
    c = F.trim(F.col(col))
    return F.when(c.isNull() | F.upper(c).isin(NULL_MARKERS), F.lit(None)).otherwise(c)


def category_expr():
    value = F.upper(F.col("category_source"))
    return (F.when(value.isin(config.PERSONS_VALUES), F.lit(config.CATEGORY_PERSONS))
             .when(value.isin(config.PROPERTY_VALUES), F.lit(config.CATEGORY_PROPERTY))
             .otherwise(F.lit(config.CATEGORY_OTHER)))


class StepLog:
    """Keeps the row count after each cleaning step for t1_summary.txt."""

    def __init__(self, df: DataFrame):
        self.rows = [("Raw rows downloaded", df.count(), None)]
        self._prev = None

    def step(self, name: str, df: DataFrame) -> DataFrame:
        df = df.persist(StorageLevel.MEMORY_AND_DISK)
        n = df.count()
        removed = self.rows[-1][1] - n
        self.rows.append((name, n, removed))
        if self._prev is not None:
            self._prev.unpersist()
        self._prev = df
        print(f"  {name:<55} removed {removed:>10,}  remaining {n:>10,}")
        return df


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
def standardise(raw: DataFrame) -> DataFrame:
    """Rename the DataSF columns to common names, with proper types."""
    return raw.select(
        clean_text(config.ID_FIELD).alias("incident_id"),
        F.to_timestamp(clean_text(config.DATE_FIELD)).alias("ts"),
        clean_text(config.NEIGHBOURHOOD_FIELD).alias("neighbourhood"),
        clean_text("latitude").cast("double").alias("latitude"),
        clean_text("longitude").cast("double").alias("longitude"),
        clean_text(config.OFFENCE_TYPE_FIELD).alias("offence_type"),
        clean_text(config.CATEGORY_FIELD).alias("category_source"),
        clean_text("police_district").alias("police_district"),
    )


def clean(df: DataFrame, log: StepLog) -> DataFrame:
    min_lat, max_lat, min_lon, max_lon = config.CITY_BBOX

    # Duplicate reports: the same row_id downloaded more than once, or identical rows.
    df = log.step("1. Duplicate reports (same id or identical row)",
                  df.dropDuplicates(["incident_id"]).dropDuplicates())
    # No date (missing or unparseable), or outside 2022-01-01 .. 2024-12-31.
    df = log.step("2. No date / unparseable date", df.filter(F.col("ts").isNotNull()))
    df = log.step("3. Date outside 2022-2024",
                  df.filter((F.year("ts") >= config.FIRST_YEAR)
                            & (F.year("ts") <= config.LAST_YEAR)))
    # No neighbourhood.
    df = log.step("4. No neighbourhood", df.filter(F.col("neighbourhood").isNotNull()))
    # No valid coordinates (missing, zero, or not a real latitude/longitude).
    valid_coords = (F.col("latitude").isNotNull() & F.col("longitude").isNotNull()
                    & (F.col("latitude") != 0) & (F.col("longitude") != 0)
                    & F.col("latitude").between(-90, 90)
                    & F.col("longitude").between(-180, 180))
    df = log.step("5. Missing or invalid coordinates", df.filter(valid_coords))
    # Outside San Francisco: coordinates outside its bounding box, or police_district
    # 'Out of SF' (incidents the SFPD recorded as happening outside the city).
    inside = (F.col("latitude").between(min_lat, max_lat)
              & F.col("longitude").between(min_lon, max_lon)
              & (F.coalesce(F.col("police_district"), F.lit("")) != config.OUT_OF_CITY_DISTRICT))
    df = log.step("6. Outside San Francisco", df.filter(inside))
    return df


def summarise(spark, df: DataFrame) -> DataFrame:
    """One row per neighbourhood, year and month (months with no incidents get zeros)."""
    keys = ["neighbourhood", "year", "month"]
    df = df.withColumn("year", F.year("ts")).withColumn("month", F.month("ts"))

    counts = df.groupBy(keys).agg(
        F.count(F.lit(1)).alias("total_incidents"),
        F.sum(F.when(F.col("category") == config.CATEGORY_PERSONS, 1).otherwise(0))
         .alias("incidents_persons"),
        F.sum(F.when(F.col("category") == config.CATEGORY_PROPERTY, 1).otherwise(0))
         .alias("incidents_property"),
        F.sum(F.when(F.col("category") == config.CATEGORY_OTHER, 1).otherwise(0))
         .alias("incidents_other"),
    )

    # Three most common offence types (ties broken alphabetically, so results are stable).
    by_type = (df.withColumn("offence_type", F.coalesce("offence_type", F.lit("UNKNOWN")))
                 .groupBy(keys + ["offence_type"]).count())
    rank = Window.partitionBy(keys).orderBy(F.desc("count"), F.asc("offence_type"))
    top = (by_type.withColumn("rank", F.row_number().over(rank)).filter("rank <= 3")
                  .groupBy(keys)
                  .agg(*[F.max(F.when(F.col("rank") == i, F.col("offence_type")))
                          .alias(f"top_offence_{i}") for i in (1, 2, 3)],
                       *[F.max(F.when(F.col("rank") == i, F.col("count")))
                          .alias(f"top_offence_{i}_count") for i in (1, 2, 3)]))

    # Full neighbourhood x month grid, so averages in later tasks count zero months.
    neighbourhoods = df.select("neighbourhood").distinct()
    months = spark.createDataFrame(
        [(y, m) for y in range(config.FIRST_YEAR, config.LAST_YEAR + 1) for m in range(1, 13)],
        ["year", "month"])
    grid = neighbourhoods.crossJoin(months)

    return (grid.join(counts, keys, "left").join(top, keys, "left")
                .fillna(0, subset=["total_incidents", "incidents_persons",
                                   "incidents_property", "incidents_other"])
                .orderBy("neighbourhood", "year", "month"))


def write_mapping(df: DataFrame) -> None:
    """Every incident_category seen, the category it was mapped to, and how many rows."""
    mapping = (df.groupBy(F.coalesce("category_source", F.lit("(missing)")).alias("source_value"),
                          "category")
                 .count().orderBy(F.desc("count")).toPandas())
    mapping.insert(0, "source_field", config.CATEGORY_FIELD)
    mapping.to_csv(config.OUTPUT_DIR / "t1_category_mapping.csv", index=False)


def write_summary(log: StepLog, final: DataFrame, table: DataFrame) -> str:
    # Format inside Spark: collecting a timestamp would shift it to the laptop's time zone.
    fmt = "yyyy-MM-dd HH:mm:ss"
    stats = final.agg(F.date_format(F.min("ts"), fmt).alias("first"),
                      F.date_format(F.max("ts"), fmt).alias("last"),
                      F.countDistinct("neighbourhood").alias("n_nb")).first()
    cats = {r["category"]: r["count"] for r in final.groupBy("category").count().collect()}

    lines = [
        f"Task 1 summary - {config.CITY_NAME}",
        f"Generated: {datetime.now(timezone.utc).isoformat(timespec='seconds')}",
        f"Source: {config.INCIDENTS_URL} (date field '{config.DATE_FIELD}', "
        f"neighbourhood field '{config.NEIGHBOURHOOD_FIELD}')",
        f"Requested date range: {config.START_DATE[:10]} to {config.END_DATE[:10]}",
        "",
        "Cleaning steps (row counts):",
        f"  {'Step':<55} {'Removed':>10} {'Remaining':>10}",
    ]
    for name, remaining, removed in log.rows:
        lines.append(f"  {name:<55} {'' if removed is None else f'{removed:,}':>10} "
                     f"{remaining:>10,}")
    start, end = log.rows[0][1], log.rows[-1][1]
    lines += [
        f"  Total removed: {start - end:,} of {start:,} ({(start - end) / max(start, 1):.1%})",
        "",
        f"Final incidents: {end:,}",
        f"Number of neighbourhoods: {stats['n_nb']}",
        f"Date range of cleaned data: {stats['first']} to {stats['last']}",
        "",
        "Incidents by category:",
        *[f"  {c:<26} {cats.get(c, 0):>10,}" for c in
          (config.CATEGORY_PERSONS, config.CATEGORY_PROPERTY, config.CATEGORY_OTHER)],
        "",
        f"Category mapping: based on '{config.CATEGORY_FIELD}'.",
        f"  {config.CATEGORY_PERSONS}: {'; '.join(config.PERSONS_VALUES)}",
        f"  {config.CATEGORY_PROPERTY}: {'; '.join(config.PROPERTY_VALUES)}",
        f"  {config.CATEGORY_OTHER}: every other value (see t1_category_mapping.csv)",
        "",
        f"neighbourhood_month.parquet: {table.count():,} rows "
        f"(neighbourhoods x {config.LAST_YEAR - config.FIRST_YEAR + 1} years x 12 months)",
        f"Raw input: {RAW_PARQUET}",
    ]
    text = "\n".join(lines) + "\n"
    (config.OUTPUT_DIR / "t1_summary.txt").write_text(text)
    return text


def main() -> None:
    argparse.ArgumentParser(description=__doc__,
                            formatter_class=argparse.RawDescriptionHelpFormatter).parse_args()
    if not RAW_PARQUET.exists():
        raise SystemExit(f"{RAW_PARQUET} not found. Run: python -m safestay.task1_ingest")
    config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    spark = get_spark("SafeStay-Task1-Pipeline")
    raw = spark.read.parquet(str(RAW_PARQUET))
    print(f"Cleaning {config.CITY_NAME} incidents from {RAW_PARQUET}")

    df = standardise(raw)
    log = StepLog(df)
    print(f"  {'Raw rows downloaded':<55} {'':>18} remaining {log.rows[0][1]:>10,}")
    df = clean(df, log)
    df = df.withColumn("category", category_expr()).persist(StorageLevel.MEMORY_AND_DISK)

    table = summarise(spark, df).persist(StorageLevel.MEMORY_AND_DISK)
    out = config.OUTPUT_DIR / "neighbourhood_month.parquet"
    table.coalesce(1).write.mode("overwrite").parquet(str(out))
    write_mapping(df)
    print()
    print(write_summary(log, df, table))
    print(f"Wrote {out}, {config.OUTPUT_DIR / 't1_summary.txt'}, "
          f"{config.OUTPUT_DIR / 't1_category_mapping.csv'}")
    spark.stop()


if __name__ == "__main__":
    main()
