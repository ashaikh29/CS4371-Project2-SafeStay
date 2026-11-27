"""Shared access to the Task 1 table (outputs/neighbourhood_month.parquet).

Tasks 2, 3, 6 and 8 get every incident figure from here, so the same question
always gives the same number everywhere (which also keeps the LLM number checks
consistent). The table has only ~1,500 rows, so pandas is fine for this.
"""
from __future__ import annotations

import calendar
from functools import lru_cache

import pandas as pd

from safestay import config

TABLE_PATH = config.OUTPUT_DIR / "neighbourhood_month.parquet"
N_YEARS = config.LAST_YEAR - config.FIRST_YEAR + 1
DECIMALS = 1  # every average is rounded once, here, before anyone sees it


@lru_cache(maxsize=1)
def load_table() -> pd.DataFrame:
    """The Task 1 table: one row per neighbourhood, year and month (zeros included)."""
    if not TABLE_PATH.exists():
        raise FileNotFoundError(f"{TABLE_PATH} not found. Run Task 1 first: "
                                "python -m safestay.task1_pipeline")
    return pd.read_parquet(TABLE_PATH)


def neighbourhoods() -> list[str]:
    """The 41 neighbourhood names, sorted."""
    return sorted(load_table()["neighbourhood"].unique())


def month_number(month: int | str) -> int:
    """7, '7', 'July', 'jul' -> 7."""
    if isinstance(month, int) or str(month).strip().isdigit():
        m = int(month)
    else:
        name = str(month).strip().lower()[:3]
        abbrs = [a.lower() for a in calendar.month_abbr]
        if name not in abbrs:
            raise ValueError(f"Unknown month: {month!r}")
        m = abbrs.index(name)
    if not 1 <= m <= 12:
        raise ValueError(f"Month must be 1-12, got {month!r}")
    return m


def overall_monthly_average() -> pd.DataFrame:
    """Per neighbourhood: average incidents per month over all 36 months (Task 2, Task 3)."""
    t = load_table()
    out = (t.groupby("neighbourhood")["total_incidents"].mean()
             .rename("avg_incidents_all_months").reset_index())
    out["avg_incidents_all_months"] = out["avg_incidents_all_months"].round(DECIMALS)
    return out.sort_values("avg_incidents_all_months", ascending=False, ignore_index=True)


def month_outlook(month: int | str) -> pd.DataFrame:
    """Per neighbourhood, for one calendar month averaged over 2022-2024 (Tasks 2 and 8).

    Columns: neighbourhood, month, avg_incidents, avg_persons, avg_property, avg_other,
    share_of_city_pct, avg_incidents_all_months, seasonal_ratio.
    """
    m = month_number(month)
    t = load_table()
    in_month = t[t["month"] == m]
    out = (in_month.groupby("neighbourhood")
                   [["total_incidents", "incidents_persons",
                     "incidents_property", "incidents_other"]]
                   .mean()
                   .rename(columns={"total_incidents": "avg_incidents",
                                    "incidents_persons": "avg_persons",
                                    "incidents_property": "avg_property",
                                    "incidents_other": "avg_other"})
                   .reset_index())
    out["share_of_city_pct"] = 100 * out["avg_incidents"] / out["avg_incidents"].sum()

    overall = t.groupby("neighbourhood")["total_incidents"].mean().rename("avg_all")
    out = out.join(overall, on="neighbourhood")
    out["seasonal_ratio"] = (out["avg_incidents"] / out["avg_all"]).where(out["avg_all"] > 0)
    out = out.rename(columns={"avg_all": "avg_incidents_all_months"})
    out.insert(1, "month", calendar.month_name[m])

    out["seasonal_ratio"] = out["seasonal_ratio"].round(2)
    num = ["avg_incidents", "avg_persons", "avg_property", "avg_other",
           "share_of_city_pct", "avg_incidents_all_months"]
    out[num] = out[num].round(DECIMALS)
    return out.sort_values("avg_incidents", ascending=False, ignore_index=True)


def city_monthly_totals() -> pd.DataFrame:
    """Per calendar month: the city's average monthly incident total, 2022-2024 (Task 6)."""
    t = load_table()
    per_year_month = t.groupby(["year", "month"])["total_incidents"].sum().reset_index()
    out = (per_year_month.groupby("month")["total_incidents"].mean()
             .rename("avg_city_incidents").reset_index())
    out["avg_city_incidents"] = out["avg_city_incidents"].round(DECIMALS)
    out.insert(1, "month_name", [calendar.month_name[m] for m in out["month"]])
    return out
