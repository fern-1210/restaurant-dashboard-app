"""
# BETTER WAY (for when you're ready)
Create SQL views/materialized tables for each dashboard block and treat Streamlit
as a pure presentation layer.

# SIMPLE VERSION (what we're building now)
Use one Python module with parameterized SQL functions that prepare the exact data
frames needed by dashboard pages. Not every function below is imported by app.py;
several (e.g. monthly revenue trend, cashflow-by-month, expense breakdowns,
get_bank_trend_monthly, transaction detail) are intentionally kept for future tabs
or notebooks—not dead code.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

import pandas as pd


@dataclass
class DashboardPeriod:
    """
    Hold date boundaries used by dashboard filters.

    Parameters:
        start_date: Inclusive start date (`YYYY-MM-DD`).
        end_date: Inclusive end date (`YYYY-MM-DD`).

    Returns:
        DashboardPeriod object with normalized date strings.
    """

    # Store period start for SQL filtering.
    start_date: str
    # Store period end for SQL filtering.
    end_date: str


def _to_iso_date(value: str) -> str:
    """
    Convert user-like date input to ISO date string.

    Parameters:
        value: Date-like string accepted by pandas.

    Returns:
        ISO date string in `YYYY-MM-DD` format.
    """

    # Parse date using pandas for robust format handling.
    dt = pd.to_datetime(value, errors="raise")
    # Return just the date part as ISO text.
    return dt.date().isoformat()


def build_period_from_year_month(year: int, month: int | None) -> DashboardPeriod:
    """
    Build a DashboardPeriod for a selected year and optional month.

    Parameters:
        year: Selected year (e.g. 2024).
        month: Selected month (1-12), or None for full year.

    Returns:
        DashboardPeriod spanning the selected range.
    """
    import calendar

    if month is None:
        start = f"{year}-01-01"
        end = f"{year}-12-31"
    else:
        _, last_day = calendar.monthrange(year, month)
        start = f"{year}-{month:02d}-01"
        end = f"{year}-{month:02d}-{last_day}"
    return build_period(start, end)


def build_period(start_date: str, end_date: str) -> DashboardPeriod:
    """
    Build and validate an inclusive dashboard period.

    Parameters:
        start_date: Raw start date string.
        end_date: Raw end date string.

    Returns:
        DashboardPeriod with validated ISO dates.
    """

    # Normalize both inputs into stable ISO strings.
    start_iso = _to_iso_date(start_date)
    # Normalize both inputs into stable ISO strings.
    end_iso = _to_iso_date(end_date)
    # Prevent inverted ranges that would silently return empty sets.
    if start_iso > end_iso:
        raise ValueError("start_date must be <= end_date")
    # Return validated period object.
    return DashboardPeriod(start_date=start_iso, end_date=end_iso)


def get_available_date_bounds(conn: sqlite3.Connection) -> DashboardPeriod:
    """
    Read earliest and latest available business dates from revenue data.

    Parameters:
        conn: Open SQLite connection.

    Returns:
        DashboardPeriod spanning min/max `revenue_daily.date`.
    """

    # Query min/max dates from trusted daily revenue table.
    row = conn.execute("SELECT MIN(date), MAX(date) FROM revenue_daily;").fetchone()
    # Fail fast if revenue is empty to avoid ambiguous dashboards.
    if not row or row[0] is None or row[1] is None:
        raise ValueError("revenue_daily has no rows; load revenue first.")
    # Return available period boundaries.
    return DashboardPeriod(start_date=str(row[0]), end_date=str(row[1]))


def get_overview_kpis(conn: sqlite3.Connection, period: DashboardPeriod) -> pd.DataFrame:
    """
    Compute owner-facing headline KPIs for a selected period.

    Parameters:
        conn: Open SQLite connection.
        period: Selected date range.

    Returns:
        One-row DataFrame containing net revenue, costs, profit, and margin.
    """

    # Aggregate trusted revenue metrics for selected date range.
    kpi_df = pd.read_sql_query(
        """
        SELECT
            COALESCE(SUM(sales_net), 0.0) AS revenue_net,
            COALESCE(SUM(costs), 0.0) AS cogs_total,
            COALESCE(SUM(profit), 0.0) AS gross_profit,
            COALESCE(SUM(num_sales), 0.0) AS num_sales_total,
            COALESCE(SUM(quantity), 0.0) AS quantity_total
        FROM revenue_daily
        WHERE date BETWEEN ? AND ?
        """,
        conn,
        params=[period.start_date, period.end_date],
    )
    # Compute gross margin safely (avoid division by zero).
    revenue_net = float(kpi_df.loc[0, "revenue_net"])
    # Convert profit to float for arithmetic.
    gross_profit = float(kpi_df.loc[0, "gross_profit"])
    # Apply safe margin formula.
    margin_pct = (gross_profit / revenue_net * 100.0) if revenue_net else 0.0
    # Add derived field to the KPI table.
    kpi_df["gross_margin_pct"] = round(margin_pct, 2)
    # Return enriched KPI result.
    return kpi_df


def get_monthly_revenue_trend(conn: sqlite3.Connection, period: DashboardPeriod) -> pd.DataFrame:
    """
    Build monthly trend data for net revenue and profit.

    Parameters:
        conn: Open SQLite connection.
        period: Selected date range.

    Returns:
        DataFrame with one row per month in the selected period.
    """

    # Group daily data into month buckets for trend charts.
    return pd.read_sql_query(
        """
        SELECT
            strftime('%Y-%m', date) AS month,
            COALESCE(SUM(sales_net), 0.0) AS revenue_net,
            COALESCE(SUM(costs), 0.0) AS cogs_total,
            COALESCE(SUM(profit), 0.0) AS gross_profit
        FROM revenue_daily
        WHERE date BETWEEN ? AND ?
        GROUP BY strftime('%Y-%m', date)
        ORDER BY month
        """,
        conn,
        params=[period.start_date, period.end_date],
    )


def get_month_over_month(conn: sqlite3.Connection, period: DashboardPeriod) -> pd.DataFrame:
    """
    Compute month-over-month percentage change in net revenue.

    Parameters:
        conn: Open SQLite connection.
        period: Selected date range.

    Returns:
        DataFrame with monthly revenue and MoM percentage change.
    """

    # Reuse monthly trend as the base for MoM calculations.
    trend_df = get_monthly_revenue_trend(conn, period)
    # Compute prior month revenue for each row.
    trend_df["previous_revenue_net"] = trend_df["revenue_net"].shift(1)
    # Compute MoM % change using vectorized arithmetic.
    trend_df["mom_revenue_pct"] = (
        (trend_df["revenue_net"] - trend_df["previous_revenue_net"])
        / trend_df["previous_revenue_net"].replace(0, pd.NA)
        * 100.0
    )
    # Keep readable precision for dashboard display.
    trend_df["mom_revenue_pct"] = trend_df["mom_revenue_pct"].round(2)
    # Return monthly trend with MoM enrichment.
    return trend_df


def get_yoy_same_month(conn: sqlite3.Connection, month_yyyy_mm: str) -> pd.DataFrame:
    """
    Compare a target month with the same month in the prior year.

    Parameters:
        conn: Open SQLite connection.
        month_yyyy_mm: Target month in `YYYY-MM` format.

    Returns:
        DataFrame with current month, prior year same month, and YoY change.
    """

    # Parse target month into timestamp for date arithmetic.
    target_ts = pd.to_datetime(f"{month_yyyy_mm}-01", errors="raise")
    # Derive prior-year month key.
    prior_key = (target_ts - pd.DateOffset(years=1)).strftime("%Y-%m")

    # Pull current and prior month totals in one query.
    yoy_df = pd.read_sql_query(
        """
        SELECT
            month,
            revenue_net
        FROM (
            SELECT
                strftime('%Y-%m', date) AS month,
                COALESCE(SUM(sales_net), 0.0) AS revenue_net
            FROM revenue_daily
            GROUP BY strftime('%Y-%m', date)
        )
        WHERE month IN (?, ?)
        ORDER BY month
        """,
        conn,
        params=[prior_key, month_yyyy_mm],
    )

    # Pivot rows to easier dictionary lookups.
    values = {row["month"]: float(row["revenue_net"]) for _, row in yoy_df.iterrows()}
    # Read current period value (default 0 if missing).
    current_value = values.get(month_yyyy_mm, 0.0)
    # Read prior-year period value (default 0 if missing).
    prior_value = values.get(prior_key, 0.0)
    # Compute YoY percentage safely.
    yoy_pct = ((current_value - prior_value) / prior_value * 100.0) if prior_value else None

    # Return a one-row standardized structure for KPI cards.
    return pd.DataFrame(
        [
            {
                "month": month_yyyy_mm,
                "revenue_net": current_value,
                "prior_year_month": prior_key,
                "prior_year_revenue_net": prior_value,
                "yoy_revenue_pct": round(yoy_pct, 2) if yoy_pct is not None else None,
            }
        ]
    )


def get_expense_by_description(conn: sqlite3.Connection, period: DashboardPeriod) -> pd.DataFrame:
    """
    Aggregate expense transactions by normalized description (unmapped-safe).

    Parameters:
        conn: Open SQLite connection.
        period: Selected date range.

    Returns:
        DataFrame with expense totals grouped by description_norm.
    """

    return pd.read_sql_query(
        """
        SELECT
            COALESCE(description_norm, '(blank)') AS description_norm,
            ROUND(SUM(ABS(amount)), 2) AS expense_total,
            COUNT(*) AS transaction_count
        FROM bank_transactions
        WHERE posted_date BETWEEN ? AND ?
          AND amount < 0
        GROUP BY COALESCE(description_norm, '(blank)')
        ORDER BY expense_total DESC
        """,
        conn,
        params=[period.start_date, period.end_date],
    )


def get_expense_by_category(conn: sqlite3.Connection, period: DashboardPeriod) -> pd.DataFrame:
    """
    Aggregate expense transactions by category and subcategory.

    Parameters:
        conn: Open SQLite connection.
        period: Selected date range.

    Returns:
        DataFrame with expense totals grouped by category levels.
    """

    # Group debit transactions into mapped category buckets.
    return pd.read_sql_query(
        """
        SELECT
            COALESCE(m.category, 'UNMAPPED') AS category,
            COALESCE(m.subcategory, 'UNMAPPED') AS subcategory,
            ROUND(SUM(ABS(b.amount)), 2) AS expense_total
        FROM bank_transactions b
        LEFT JOIN transaction_category_map m
            ON b.description_norm = m.description_norm
        WHERE b.posted_date BETWEEN ? AND ?
          AND b.amount < 0
        GROUP BY
            COALESCE(m.category, 'UNMAPPED'),
            COALESCE(m.subcategory, 'UNMAPPED')
        ORDER BY expense_total DESC
        """,
        conn,
        params=[period.start_date, period.end_date],
    )


def get_cashflow_monthly(conn: sqlite3.Connection, period: DashboardPeriod) -> pd.DataFrame:
    """
    Build monthly cash-in and cash-out totals from bank transactions.

    Parameters:
        conn: Open SQLite connection.
        period: Selected date range.

    Returns:
        DataFrame with cash_in, cash_out, and net_cashflow per month.
    """

    # Aggregate signed bank amounts into monthly cashflow metrics.
    return pd.read_sql_query(
        """
        SELECT
            strftime('%Y-%m', posted_date) AS month,
            ROUND(SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END), 2) AS cash_in,
            ROUND(SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END), 2) AS cash_out,
            ROUND(SUM(amount), 2) AS net_cashflow
        FROM bank_transactions
        WHERE posted_date BETWEEN ? AND ?
        GROUP BY strftime('%Y-%m', posted_date)
        ORDER BY month
        """,
        conn,
        params=[period.start_date, period.end_date],
    )


# ---- Filter & period helpers (year + month with All months) ----

def get_available_years(conn: sqlite3.Connection) -> list[int]:
    """
    Return distinct years that have data, sorted newest first.

    Parameters:
        conn: Open SQLite connection.

    Returns:
        List of year integers (e.g. [2025, 2024, 2023]).
    """
    # Union years from bank and revenue so we show all years with any data.
    rows = conn.execute(
        """
        SELECT DISTINCT CAST(strftime('%Y', posted_date) AS INTEGER) AS year
        FROM bank_transactions
        WHERE posted_date IS NOT NULL
        UNION
        SELECT DISTINCT CAST(strftime('%Y', date) AS INTEGER) AS year
        FROM revenue_daily
        WHERE date IS NOT NULL
        ORDER BY year DESC
        """
    ).fetchall()
    return [r[0] for r in rows if r[0] is not None]


def get_available_months_for_year(conn: sqlite3.Connection, year: int) -> list[str]:
    """
    Return distinct months (YYYY-MM) for a given year that have data, sorted newest first.

    Parameters:
        conn: Open SQLite connection.
        year: Selected year (e.g. 2024).

    Returns:
        List of "YYYY-MM" strings (e.g. ["2024-03", "2024-02", "2024-01"]).
    """
    rows = conn.execute(
        """
        SELECT DISTINCT strftime('%Y-%m', posted_date) AS month
        FROM bank_transactions
        WHERE posted_date IS NOT NULL AND strftime('%Y', posted_date) = ?
        UNION
        SELECT DISTINCT strftime('%Y-%m', date) AS month
        FROM revenue_daily
        WHERE date IS NOT NULL AND strftime('%Y', date) = ?
        ORDER BY month DESC
        """,
        [str(year), str(year)],
    ).fetchall()
    return [r[0] for r in rows if r[0]]


def period_from_year_month(year: int, month_yyyy_mm: str | None) -> DashboardPeriod:
    """
    Build DashboardPeriod from year and optional month selection.

    Parameters:
        year: Selected year (e.g. 2024).
        month_yyyy_mm: "YYYY-MM" for a specific month, or None for full year.

    Returns:
        DashboardPeriod spanning the selected range.
    """
    if month_yyyy_mm is None or month_yyyy_mm == "":
        return build_period_from_year_month(year, None)
    dt = pd.to_datetime(f"{month_yyyy_mm}-01", errors="raise")
    return build_period_from_year_month(dt.year, dt.month)


# ---- Summary page helpers (single-page dashboard) ----

def get_available_months(conn: sqlite3.Connection) -> list[str]:
    """
    Return distinct Gregorian calendar months that have data, sorted newest first.

    Parameters:
        conn: Open SQLite connection.

    Returns:
        List of "YYYY-MM" strings (e.g. ["2025-03", "2025-02", ...]).
    """
    # Union months from bank and revenue so we show all months with any data.
    rows = conn.execute(
        """
        SELECT DISTINCT strftime('%Y-%m', posted_date) AS month
        FROM bank_transactions
        WHERE posted_date IS NOT NULL
        UNION
        SELECT DISTINCT strftime('%Y-%m', date) AS month
        FROM revenue_daily
        WHERE date IS NOT NULL
        ORDER BY month DESC
        """
    ).fetchall()
    return [r[0] for r in rows if r[0]]


def get_compare_period(month_yyyy_mm: str, compare_to: str) -> str | None:
    """
    Derive the comparison period (YYYY-MM) for delta calculations.

    Parameters:
        month_yyyy_mm: Selected month in "YYYY-MM" format.
        compare_to: "Previous month" or "Same month last year".

    Returns:
        Compare period as "YYYY-MM", or None if invalid.
    """
    dt = pd.to_datetime(f"{month_yyyy_mm}-01", errors="coerce")
    if pd.isna(dt):
        return None
    if compare_to == "Previous month":
        prev = dt - pd.DateOffset(months=1)
        return prev.strftime("%Y-%m")
    if compare_to == "Same month last year":
        prev = dt - pd.DateOffset(years=1)
        return prev.strftime("%Y-%m")
    return None


def _month_to_period(month_yyyy_mm: str) -> DashboardPeriod:
    """Convert YYYY-MM to start/end dates for the full month."""
    dt = pd.to_datetime(f"{month_yyyy_mm}-01", errors="raise")
    start = dt.strftime("%Y-%m-%d")
    _, last = __import__("calendar").monthrange(dt.year, dt.month)
    end = f"{dt.year:04d}-{dt.month:02d}-{last:02d}"
    return DashboardPeriod(start_date=start, end_date=end)


def _format_delta(current: float, compare: float) -> tuple[str, str]:
    """
    Compute absolute and percent delta for display.
    Returns (delta_abs_str, delta_pct_str) e.g. ("+€420", "+8.4%").
    """
    if compare == 0 or compare is None:
        return ("N/A", "N/A")
    diff = current - compare
    pct = (diff / compare) * 100.0
    sign = "+" if diff >= 0 else "-"
    abs_str = f"{sign}€{abs(diff):,.2f}"
    pct_str = f"{sign}{abs(pct):.1f}%"
    return (abs_str, pct_str)


def get_summary_kpis(
    conn: sqlite3.Connection,
    month_yyyy_mm: str,
    compare_to: str,
) -> dict:
    """
    Compute all 4 Summary KPI values and deltas for the selected month.

    Parameters:
        conn: Open SQLite connection.
        month_yyyy_mm: Selected month "YYYY-MM".
        compare_to: "Previous month" or "Same month last year".

    Returns:
        Dict with keys: revenue_gross, total_expenditure, caixa_expenditure,
        millennium_expenditure. Each value is a dict with: value, delta_abs,
        delta_pct, source.
    """
    period = _month_to_period(month_yyyy_mm)
    compare_month = get_compare_period(month_yyyy_mm, compare_to)
    compare_period = _month_to_period(compare_month) if compare_month else None
    return _get_summary_kpis_impl(conn, period, compare_period)


def get_summary_kpis_for_period(
    conn: sqlite3.Connection,
    period: DashboardPeriod,
    compare_period: DashboardPeriod | None = None,
) -> dict:
    """
    Compute all 4 Summary KPI values and deltas for a date range (e.g. full year).

    Parameters:
        conn: Open SQLite connection.
        period: Selected date range.
        compare_period: Optional comparison period for deltas (e.g. prior year).

    Returns:
        Dict with keys: revenue_gross, total_expenditure, caixa_expenditure,
        millennium_expenditure. Each value is a dict with: value, delta_abs,
        delta_pct, source.
    """
    return _get_summary_kpis_impl(conn, period, compare_period)


def _get_summary_kpis_impl(
    conn: sqlite3.Connection,
    period: DashboardPeriod,
    compare_period: DashboardPeriod | None,
) -> dict:

    def _revenue_gross(p: DashboardPeriod) -> float:
        row = conn.execute(
            "SELECT COALESCE(SUM(sales_gross), 0) FROM revenue_daily WHERE date BETWEEN ? AND ?",
            [p.start_date, p.end_date],
        ).fetchone()
        return float(row[0] or 0)

    def _expenditure(p: DashboardPeriod, bank_filter: str) -> float:
        # bank_filter: "both", "caixa", "millennium"
        if bank_filter == "both":
            sql = """
                SELECT COALESCE(SUM(ABS(amount)), 0) FROM bank_transactions
                WHERE posted_date BETWEEN ? AND ? AND amount < 0
                AND bank IN ('caixa', 'millennium')
            """
        else:
            sql = """
                SELECT COALESCE(SUM(ABS(amount)), 0) FROM bank_transactions
                WHERE posted_date BETWEEN ? AND ? AND amount < 0 AND bank = ?
            """
        params = [p.start_date, p.end_date] if bank_filter == "both" else [p.start_date, p.end_date, bank_filter]
        row = conn.execute(sql, params).fetchone()
        return float(row[0] or 0)

    rev_cur = _revenue_gross(period)
    rev_cmp = _revenue_gross(compare_period) if compare_period else 0
    d_abs, d_pct = _format_delta(rev_cur, rev_cmp)

    tot_cur = _expenditure(period, "both")
    tot_cmp = _expenditure(compare_period, "both") if compare_period else 0
    tot_d_abs, tot_d_pct = _format_delta(tot_cur, tot_cmp)

    cax_cur = _expenditure(period, "caixa")
    cax_cmp = _expenditure(compare_period, "caixa") if compare_period else 0
    cax_d_abs, cax_d_pct = _format_delta(cax_cur, cax_cmp)

    mil_cur = _expenditure(period, "millennium")
    mil_cmp = _expenditure(compare_period, "millennium") if compare_period else 0
    mil_d_abs, mil_d_pct = _format_delta(mil_cur, mil_cmp)

    return {
        "revenue_gross": {
            "value": rev_cur,
            "delta_abs": d_abs,
            "delta_pct": d_pct,
            "source": "revenue_daily · sales_gross",
        },
        "total_expenditure": {
            "value": tot_cur,
            "delta_abs": tot_d_abs,
            "delta_pct": tot_d_pct,
            "source": "bank_transactions · bank in (Caixa, Millennium) · amount < 0",
        },
        "caixa_expenditure": {
            "value": cax_cur,
            "delta_abs": cax_d_abs,
            "delta_pct": cax_d_pct,
            "source": "bank_transactions · bank = Caixa · amount < 0",
        },
        "millennium_expenditure": {
            "value": mil_cur,
            "delta_abs": mil_d_abs,
            "delta_pct": mil_d_pct,
            "source": "bank_transactions · bank = Millennium · amount < 0",
        },
    }


def get_top_outflows(
    conn: sqlite3.Connection,
    month_yyyy_mm: str,
    limit: int = 10,
) -> pd.DataFrame:
    """
    Top outflow transactions (expenses) for the month, most negative first.

    Parameters:
        conn: Open SQLite connection.
        month_yyyy_mm: Selected month "YYYY-MM".
        limit: Max rows to return.

    Returns:
        DataFrame with posted_date, bank, description_raw, category, subcategory, amount.
    """
    period = _month_to_period(month_yyyy_mm)
    return pd.read_sql_query(
        """
        SELECT
            b.posted_date,
            b.bank,
            b.description_raw,
            COALESCE(m.category, 'UNMAPPED') AS category,
            COALESCE(m.subcategory, 'UNMAPPED') AS subcategory,
            b.amount
        FROM bank_transactions b
        LEFT JOIN transaction_category_map m ON b.description_norm = m.description_norm
        WHERE b.posted_date BETWEEN ? AND ? AND b.amount < 0
        ORDER BY b.amount ASC
        LIMIT ?
        """,
        conn,
        params=[period.start_date, period.end_date, limit],
    )


def get_inflows_with_cumulative(
    conn: sqlite3.Connection,
    month_yyyy_mm: str,
    limit: int = 10,
) -> pd.DataFrame:
    """
    Inflow transactions sorted newest first, with cumulative sum for the month.

    Parameters:
        conn: Open SQLite connection.
        month_yyyy_mm: Selected month "YYYY-MM".
        limit: Max rows to return.

    Returns:
        DataFrame with posted_date, bank, description_raw, category, subcategory, amount, cumulative_inflow.
    """
    period = _month_to_period(month_yyyy_mm)
    # Fetch all inflows for the month, oldest first, to compute true cumulative.
    # Include category/subcategory from mapping so categorization is visible in the UI.
    df_all = pd.read_sql_query(
        """
        SELECT
            b.posted_date,
            b.bank,
            b.description_raw,
            COALESCE(m.category, 'UNMAPPED') AS category,
            COALESCE(m.subcategory, 'UNMAPPED') AS subcategory,
            b.amount
        FROM bank_transactions b
        LEFT JOIN transaction_category_map m ON b.description_norm = m.description_norm
        WHERE b.posted_date BETWEEN ? AND ? AND b.amount > 0
        ORDER BY b.posted_date ASC, b.id ASC
        """,
        conn,
        params=[period.start_date, period.end_date],
    )
    if df_all.empty:
        df_all["cumulative_inflow"] = []
        return df_all
    # Cumulative from start of month.
    df_all["cumulative_inflow"] = df_all["amount"].cumsum()
    # Return top N newest (most recent first).
    return df_all.sort_values("posted_date", ascending=False).head(limit).reset_index(drop=True)


def get_data_source_status(conn: sqlite3.Connection) -> dict[str, bool]:
    """
    Check which data sources have been loaded (have rows).

    Parameters:
        conn: Open SQLite connection.

    Returns:
        Dict with keys: vendus_revenue, caixa_bank, millennium_bank, category_mapping.
    """
    rev = conn.execute("SELECT COUNT(*) FROM revenue_daily").fetchone()[0] or 0
    cax = conn.execute(
        "SELECT COUNT(*) FROM bank_transactions WHERE bank = 'caixa'"
    ).fetchone()[0] or 0
    mil = conn.execute(
        "SELECT COUNT(*) FROM bank_transactions WHERE bank = 'millennium'"
    ).fetchone()[0] or 0
    map_count = conn.execute("SELECT COUNT(*) FROM transaction_category_map").fetchone()[0] or 0
    return {
        "vendus_revenue": rev > 0,
        "caixa_bank": cax > 0,
        "millennium_bank": mil > 0,
        "category_mapping": map_count > 0,
    }


def get_last_import_timestamp(conn: sqlite3.Connection) -> str | None:
    """
    Get the most recent imported_at timestamp across revenue and bank tables.

    Parameters:
        conn: Open SQLite connection.

    Returns:
        Formatted string e.g. "19 Mar 2025 · 09:14", or None if unavailable.
    """
    row = conn.execute(
        """
        SELECT MAX(ts) FROM (
            SELECT imported_at AS ts FROM revenue_daily WHERE imported_at IS NOT NULL
            UNION ALL
            SELECT imported_at AS ts FROM bank_transactions WHERE imported_at IS NOT NULL
        )
        """
    ).fetchone()
    if not row or row[0] is None:
        return None
    try:
        dt = pd.to_datetime(row[0])
        return dt.strftime("%d %b %Y · %H:%M")
    except Exception:
        return None


def get_category_inflows(conn: sqlite3.Connection, period: DashboardPeriod) -> pd.DataFrame:
    """
    Aggregate inflow transactions by category for bar charts.

    Parameters:
        conn: Open SQLite connection.
        period: Selected date range.

    Returns:
        DataFrame with columns: category, amount (positive totals).
    """
    df = pd.read_sql_query(
        """
        SELECT
            COALESCE(m.category, 'UNMAPPED') AS category,
            ROUND(SUM(b.amount), 2) AS amount
        FROM bank_transactions b
        LEFT JOIN transaction_category_map m ON b.description_norm = m.description_norm
        WHERE b.posted_date BETWEEN ? AND ? AND b.amount > 0
        GROUP BY COALESCE(m.category, 'UNMAPPED')
        ORDER BY amount DESC
        """,
        conn,
        params=[period.start_date, period.end_date],
    )
    return df


def get_category_outflows(conn: sqlite3.Connection, period: DashboardPeriod) -> pd.DataFrame:
    """
    Aggregate outflow transactions by category for bar charts.

    Parameters:
        conn: Open SQLite connection.
        period: Selected date range.

    Returns:
        DataFrame with columns: category, amount (positive totals, i.e. expense amounts).
    """
    df = pd.read_sql_query(
        """
        SELECT
            COALESCE(m.category, 'UNMAPPED') AS category,
            ROUND(SUM(ABS(b.amount)), 2) AS amount
        FROM bank_transactions b
        LEFT JOIN transaction_category_map m ON b.description_norm = m.description_norm
        WHERE b.posted_date BETWEEN ? AND ? AND b.amount < 0
        GROUP BY COALESCE(m.category, 'UNMAPPED')
        ORDER BY amount DESC
        """,
        conn,
        params=[period.start_date, period.end_date],
    )
    return df


def get_transactions_by_category(
    conn: sqlite3.Connection,
    period: DashboardPeriod,
    direction: str,
) -> pd.DataFrame:
    """
    Return transaction-level rows grouped by category for drill-down expanders.

    Parameters:
        conn: Open SQLite connection.
        period: Selected date range.
        direction: "inflow" (amount > 0) or "outflow" (amount < 0).

    Returns:
        DataFrame with posted_date, bank, description_raw, category, amount.
    """
    if direction == "inflow":
        amount_filter = "b.amount > 0"
    else:
        amount_filter = "b.amount < 0"
    sql = f"""
        SELECT
            b.posted_date,
            b.bank,
            b.description_raw,
            COALESCE(m.category, 'UNMAPPED') AS category,
            b.amount
        FROM bank_transactions b
        LEFT JOIN transaction_category_map m ON b.description_norm = m.description_norm
        WHERE b.posted_date BETWEEN ? AND ?
          AND {amount_filter}
        ORDER BY COALESCE(m.category, 'UNMAPPED'), b.posted_date DESC
        """
    return pd.read_sql_query(sql, conn, params=[period.start_date, period.end_date])


def get_bank_trend_monthly(
    conn: sqlite3.Connection,
    period: DashboardPeriod,
    metric: str,
) -> pd.DataFrame:
    """
    Build monthly trend by bank for line chart (Net, Outflow, or Inflow).

    Parameters:
        conn: Open SQLite connection.
        period: Selected date range.
        metric: "net" | "outflow" | "inflow".

    Returns:
        DataFrame with columns: month, bank, value.
    """
    if metric == "net":
        expr = "ROUND(SUM(b.amount), 2)"
    elif metric == "outflow":
        expr = "ROUND(SUM(CASE WHEN b.amount < 0 THEN ABS(b.amount) ELSE 0 END), 2)"
    elif metric == "inflow":
        expr = "ROUND(SUM(CASE WHEN b.amount > 0 THEN b.amount ELSE 0 END), 2)"
    else:
        raise ValueError("metric must be net, outflow, or inflow")
    sql = f"""
        SELECT
            strftime('%Y-%m', b.posted_date) AS month,
            b.bank,
            {expr} AS value
        FROM bank_transactions b
        WHERE b.posted_date BETWEEN ? AND ?
          AND b.bank IN ('caixa', 'millennium')
        GROUP BY strftime('%Y-%m', b.posted_date), b.bank
        ORDER BY month, b.bank
        """
    return pd.read_sql_query(sql, conn, params=[period.start_date, period.end_date])


def get_bank_details_kpis(
    conn: sqlite3.Connection,
    period: DashboardPeriod,
    compare_period: DashboardPeriod | None = None,
) -> dict:
    """
    Compute Bank Details KPI values and optional deltas for selected period.

    Parameters:
        conn: Open SQLite connection.
        period: Selected date range.

    Returns:
        Dict with KPI dicts for total_inflow, total_outflow, net_cashflow,
        caixa_outflow, millennium_outflow, plus top outflow category fields.
    """

    # Local helper computes all base values for any period (current or compare).
    def _values_for_period(p: DashboardPeriod) -> dict[str, float]:
        row = conn.execute(
            """
            SELECT
                ROUND(SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END), 2) AS inflow,
                ROUND(SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END), 2) AS outflow,
                ROUND(SUM(amount), 2) AS net
            FROM bank_transactions
            WHERE posted_date BETWEEN ? AND ? AND bank IN ('caixa', 'millennium')
            """,
            [p.start_date, p.end_date],
        ).fetchone()
        inflow = float(row[0] or 0)
        outflow = float(row[1] or 0)
        net = float(row[2] or 0)

        cax = conn.execute(
            """
            SELECT COALESCE(SUM(ABS(amount)), 0) FROM bank_transactions
            WHERE posted_date BETWEEN ? AND ? AND amount < 0 AND bank = 'caixa'
            """,
            [p.start_date, p.end_date],
        ).fetchone()[0] or 0
        mil = conn.execute(
            """
            SELECT COALESCE(SUM(ABS(amount)), 0) FROM bank_transactions
            WHERE posted_date BETWEEN ? AND ? AND amount < 0 AND bank = 'millennium'
            """,
            [p.start_date, p.end_date],
        ).fetchone()[0] or 0
        return {
            "total_inflow": inflow,
            "total_outflow": outflow,
            "net_cashflow": net,
            "caixa_outflow": float(cax),
            "millennium_outflow": float(mil),
        }

    cur_values = _values_for_period(period)

    # Keep the existing outflow-category highlight for context under KPIs.
    top_row = conn.execute(
        """
        SELECT
            COALESCE(m.category, 'UNMAPPED') AS category,
            ROUND(SUM(ABS(b.amount)), 2) AS amt
        FROM bank_transactions b
        LEFT JOIN transaction_category_map m ON b.description_norm = m.description_norm
        WHERE b.posted_date BETWEEN ? AND ? AND b.amount < 0
        GROUP BY COALESCE(m.category, 'UNMAPPED')
        ORDER BY amt DESC
        LIMIT 1
        """,
        [period.start_date, period.end_date],
    ).fetchone()
    top_cat = top_row[0] if top_row else "—"
    top_amt = float(top_row[1] or 0) if top_row else 0.0

    def _kpi_payload(value: float, compare_value: float, source: str) -> dict[str, str | float]:
        """Return one KPI payload with value, delta strings and source text."""
        delta_abs, delta_pct = _format_delta(value, compare_value)
        return {
            "value": value,
            "delta_abs": delta_abs,
            "delta_pct": delta_pct,
            "source": source,
        }

    # Optional compare period adds meaningful trend deltas for Bank Details cards.
    cmp_values = _values_for_period(compare_period) if compare_period else {k: 0.0 for k in cur_values.keys()}

    return {
        "total_inflow": _kpi_payload(
            cur_values["total_inflow"],
            cmp_values["total_inflow"],
            "bank_transactions · bank in (Caixa, Millennium) · amount > 0",
        ),
        "total_outflow": _kpi_payload(
            cur_values["total_outflow"],
            cmp_values["total_outflow"],
            "bank_transactions · bank in (Caixa, Millennium) · amount < 0",
        ),
        "net_cashflow": _kpi_payload(
            cur_values["net_cashflow"],
            cmp_values["net_cashflow"],
            "bank_transactions · bank in (Caixa, Millennium) · sum(amount)",
        ),
        "caixa_outflow": _kpi_payload(
            cur_values["caixa_outflow"],
            cmp_values["caixa_outflow"],
            "bank_transactions · bank = Caixa · amount < 0",
        ),
        "millennium_outflow": _kpi_payload(
            cur_values["millennium_outflow"],
            cmp_values["millennium_outflow"],
            "bank_transactions · bank = Millennium · amount < 0",
        ),
        "top_outflow_category": top_cat,
        "top_outflow_amount": top_amt,
    }


def get_transaction_detail(conn: sqlite3.Connection, period: DashboardPeriod) -> pd.DataFrame:
    """
    Return transaction-level detail with category labels for drill-down tables.

    Parameters:
        conn: Open SQLite connection.
        period: Selected date range.

    Returns:
        DataFrame containing date, description, amount, and category fields.
    """

    # Return detailed rows with mapped category fields for filtering/search.
    return pd.read_sql_query(
        """
        SELECT
            b.posted_date,
            b.bank,
            b.account_id,
            b.description_raw,
            b.description_norm,
            b.amount,
            COALESCE(m.category, 'UNMAPPED') AS category,
            COALESCE(m.subcategory, 'UNMAPPED') AS subcategory,
            b.source_file
        FROM bank_transactions b
        LEFT JOIN transaction_category_map m
            ON b.description_norm = m.description_norm
        WHERE b.posted_date BETWEEN ? AND ?
        ORDER BY b.posted_date DESC, b.id DESC
        """,
        conn,
        params=[period.start_date, period.end_date],
    )
