# SafeStay: CS4371 Group Project 2 (San Francisco)

An LLM-assisted trip planner for San Francisco built on open city data. This README covers setup
and **Task 1 (incident data pipeline with Spark)**. Later tasks will be added here as they are built.

## 1. Install

You need Python 3.9 or newer and Java 17 (PySpark runs on the JVM).

```bash
java -version          # should print 17.x; on macOS: brew install openjdk@17
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 2. Environment variables

API keys are read only from environment variables. Copy the template and fill it in:

```bash
cp .env.example .env
```

| Variable | Needed for | Notes |
|---|---|---|
| `SOCRATA_APP_TOKEN` | Task 1 | Free. Sign in at data.sf.gov, then *Developer Settings → Create New App Token*. Without it, downloads may be throttled. |
| `SPARK_DRIVER_MEMORY` | Task 1 | Optional, default `4g`. |

`.env` is git-ignored; only `.env.example` (empty values) is committed. The code loads
`.env` itself, and variables already set in your shell take precedence.

## 3. Task 1: incident data pipeline

```bash
python -m safestay.task1_ingest      # download 2022–2024 incidents → data/raw/incidents.parquet
python -m safestay.task1_pipeline    # clean, classify, summarise → outputs/
```

For a quick test, `python -m safestay.task1_ingest --max-pages 1` fetches only the first
50,000 rows. The full download is about 374,000 rows and takes about a minute.

**Source:** San Francisco Police Department Incident Reports (2018 to present), DataSF,
`https://data.sf.gov/resource/wg3w-h783.json`. Neighbourhoods are the 41 *Analysis
Neighborhoods* (`analysis_neighborhood`), and dates come from `incident_datetime`.

**Ingest** (`safestay/task1_ingest.py`)
- Queries the date range 2022‑01‑01 to 2024‑12‑31, paging with `$limit` (50,000) and `$offset`,
  ordered by `:id` so paging is stable.
- Downloads only the columns listed in `INCIDENT_FIELDS` in `safestay/config.py`.
- Every page is cached in `cache/socrata/sanfrancisco/…/page_NNNNN.json` with its `retrieved_at`
  time. A re-run reuses cached pages and downloads only missing ones. Calls are at least
  1 second apart, and failed calls are retried with backoff.
- Spark combines the pages into `data/raw/incidents.parquet`.

**Pipeline** (`safestay/task1_pipeline.py`), all in Spark
1. Remove duplicate reports: the same `row_id` more than once, or identical rows.
2. Remove rows with no parseable date, and rows dated outside 2022–2024.
3. Remove rows with no `analysis_neighborhood`.
4. Remove rows with missing, zero or out-of-range coordinates.
5. Keep only incidents inside San Francisco: inside the city's bounding box, and not marked
   `police_district = 'Out of SF'` by the SFPD.
6. Add `category`: *crimes against persons*, *crimes against property*, or *other*.
7. Aggregate to one row per neighbourhood, year and month, with totals per category and the
   three most common incident categories. Months with no incidents are included as zeros, so
   averages in later tasks are correct.

**Outputs** (in `outputs/`)

| File | Contents |
|---|---|
| `neighbourhood_month.parquet` | `neighbourhood, year, month, total_incidents, incidents_persons, incidents_property, incidents_other, top_offence_1..3, top_offence_1..3_count` |
| `t1_summary.txt` | Row counts before and after each cleaning step, number of neighbourhoods, date range, category totals and the category mapping |
| `t1_category_mapping.csv` | Every `incident_category` value seen, the category it was mapped to, and its row count. Use it to check and document the mapping in the report. |

### Category mapping

The mapping is `PERSONS_VALUES` and `PROPERTY_VALUES` in `safestay/config.py`, applied to
`incident_category`. Values not listed there map to *other*. It follows the FBI NIBRS
"crime against" groups:

- **Crimes against persons:** Assault, Homicide, Rape, Sex Offense, Human Trafficking (all
  variants), Offences Against the Family and Children.
- **Crimes against property:** Larceny Theft, Burglary, Motor Vehicle Theft, Robbery,
  Vandalism, Malicious Mischief, Arson, Stolen Property, Fraud, Forgery and Counterfeiting,
  Embezzlement.
- **Other:** everything else, for example drug, weapons and warrant offences, non-criminal
  reports, lost property and missing persons.

### Using Task 1 results in later tasks

Later tasks get incident figures from `safestay/incidents.py` and never re-clean the raw
data, so every task shows the same numbers. Averages are rounded once, there, to 1 decimal
place; seasonal ratios are rounded to 2.

```python
from safestay import incidents

incidents.month_outlook("July")        # per neighbourhood, July averaged over 2022–2024 (Tasks 2, 8)
incidents.overall_monthly_average()    # per neighbourhood, average over all 36 months (Task 3)
incidents.city_monthly_totals()        # city's average incident total per calendar month (Task 6)
incidents.neighbourhoods()             # the 41 neighbourhood names
```


