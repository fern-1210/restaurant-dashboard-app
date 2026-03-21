"""
# What
Streamlit app entrypoint for the VENN financial dashboard (Summary + Bank Details tabs).

# Why
Single entry point so you run: streamlit run app.py

# How
- Connects to SQLite warehouse
- Renders sidebar filters (year, month, compare) and compact data sources panel
- Renders Summary tab: revenue + operational-day KPIs, legend expander, readable category bars
- Renders Bank Details tab: bank KPIs, category drill-down expanders
"""

from __future__ import annotations

import calendar

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from dashboard.data_prep import (
    DashboardPeriod,
    build_period,
    get_available_months_for_year,
    get_available_years,
    get_bank_details_kpis,
    get_category_inflows,
    get_category_outflows,
    get_compare_period,
    get_data_source_status,
    get_last_import_timestamp,
    get_operational_days_for_period,
    get_summary_kpis_for_period,
    get_transactions_by_category,
    period_from_year_month,
)
from scripts_pipeline.db import connect_sqlite
from scripts_pipeline.paths import DB_PATH

# ---- Colour palette (plan: green inflow, red outflow, neutral) ----
COLOR_INFLOW = "#639922"
COLOR_OUTFLOW = "#C75050"
COLOR_NEUTRAL = "#5B8A9E"


def _render_small_copy(text: str) -> None:
    """Render compact helper text to keep section copy visually light."""
    # Use markdown so we can control font size for all helper copy.
    st.markdown(f"<p style='margin:0 0 0.4rem 0;font-size:0.86rem;color:#6b7280;'>{text}</p>", unsafe_allow_html=True)


def _render_source_line(source: str, show_sources: bool) -> None:
    """Render data source text only when the sidebar toggle is enabled."""
    # Keep source hidden by default to reduce dashboard noise.
    if show_sources:
        st.caption(f"SOURCE: {source}")


def _metric_card(
    label: str,
    value: str,
    delta: str,
    delta_up: bool,
    style: str,
    source: str,
    show_sources: bool,
    *,
    show_delta: bool = True,
) -> None:
    """Render a styled KPI card with value, optional delta row, and optional source line."""
    # Map style names to card colours so visuals stay consistent.
    styles = {
        "hero_green": {"bg": "#F2F9EC", "border": COLOR_INFLOW, "delta": "#2E7D32"},
        "hero_red": {"bg": "#FDF2F2", "border": COLOR_OUTFLOW, "delta": "#B42318"},
        "hero_neutral": {"bg": "#F8FAFC", "border": COLOR_NEUTRAL, "delta": "#334155"},
    }
    # Fallback to neutral style if an unknown style is passed.
    card_style = styles.get(style, styles["hero_neutral"])
    # Choose delta color from direction for fast visual reading.
    delta_color = "#2E7D32" if delta_up else card_style["delta"]
    # ----------------------------------
    # Delta row: hide for count-only KPIs (e.g. operational days)
    # ----------------------------------
    if show_delta:
        delta_block = (
            f'<div style="font-size:0.86rem;color:{delta_color};font-weight:600;margin-top:0.35rem;">{delta}</div>'
        )
    else:
        delta_block = '<div style="font-size:0.86rem;color:#94a3b8;margin-top:0.35rem;">&nbsp;</div>'
    # Render card with compact hierarchy: label, value, delta.
    st.markdown(
        f"""
        <div style="
            background:{card_style['bg']};
            border-left:6px solid {card_style['border']};
            border-radius:10px;
            padding:0.9rem 1rem;
            min-height:130px;
            box-shadow:0 1px 4px rgba(15, 23, 42, 0.06);
        ">
            <div style="font-size:0.78rem;letter-spacing:0.03em;color:#475569;font-weight:700;">{label}</div>
            <div style="font-size:1.6rem;font-weight:800;color:#0f172a;line-height:1.2;margin-top:0.35rem;">{value}</div>
            {delta_block}
        </div>
        """,
        unsafe_allow_html=True,
    )
    # Show source line below each card only when requested.
    _render_source_line(source, show_sources)


def _style_summary_category_bar(fig: go.Figure, n_categories: int, bar_color: str) -> None:
    # ----------------------------------
    # Readable horizontal bars: height, fonts, grid, unified bar colour
    # ----------------------------------
    height = max(320, min(960, 34 * max(n_categories, 1)))
    fig.update_layout(
        height=height,
        showlegend=False,
        plot_bgcolor="white",
        margin=dict(l=8, r=36, t=12, b=56),
        xaxis=dict(
            title="Amount (€)",
            showgrid=True,
            gridcolor="rgba(15, 23, 42, 0.08)",
            tickfont=dict(size=13),
            title_font=dict(size=14),
            tickprefix="€",
            separatethousands=True,
        ),
        yaxis=dict(
            title="",
            showgrid=False,
            tickfont=dict(size=13),
            automargin=True,
        ),
    )
    fig.update_traces(
        marker_color=bar_color,
        hovertemplate="<b>%{y}</b><br>€%{x:,.2f}<extra></extra>",
    )


def _format_month(m: str) -> str:
    """Display month as 'March 2025'."""
    y, mo = int(m[:4]), int(m[5:7])
    return f"{calendar.month_name[mo]} {y}"


def _format_period_label(period: DashboardPeriod, month_yyyy_mm: str | None) -> str:
    """Display period label for header (e.g. 'March 2025' or 'Full year 2024')."""
    if month_yyyy_mm:
        return _format_month(month_yyyy_mm)
    year = period.start_date[:4]
    return f"Full year {year}"


def _section_header(title: str, intro: str, accent_color: str) -> None:
    """
    Render a section header with left accent bar and brief intro.
    Uses st.columns for accent (Streamlit-only layout).
    """
    col_bar, col_content = st.columns([1, 35])
    with col_bar:
        # Accent stripe: thin colored block via container
        st.markdown(
            f'<div style="width:4px;min-height:28px;background:{accent_color};border-radius:2px;"></div>',
            unsafe_allow_html=True,
        )
    with col_content:
        # Force uppercase headings for visual consistency.
        st.subheader(title.upper())
        # Keep supporting copy small and consistent.
        _render_small_copy(intro)


def _render_data_sources(conn) -> None:
    """Compact data sources panel with intro and no extra line spacing."""
    status = get_data_source_status(conn)
    last_import = get_last_import_timestamp(conn)

    _render_small_copy("Data loaded from Vendus, Caixa, Millennium, and category mapping.")
    lines = [
        "Vendus revenue CSV: " + ("Loaded" if status["vendus_revenue"] else "Not loaded"),
        "Caixa bank statement: " + ("Loaded" if status["caixa_bank"] else "Not loaded"),
        "Millennium bank statement: " + ("Loaded" if status["millennium_bank"] else "Not loaded"),
        "Category mapping: " + ("Loaded" if status.get("category_mapping") else "Not loaded"),
    ]
    last_import_line = "Last import: " + (last_import if last_import else "unavailable")
    st.markdown(
        (
            "<p style='margin:0;font-size:0.78rem;color:#6b7280;font-style:italic;'>"
            + "<br>".join(lines)
            + "<br><br>"
            + last_import_line
            + "</p>"
        ),
        unsafe_allow_html=True,
    )


def main() -> None:
    st.set_page_config(
        page_title="VENN financial dashboard",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    conn = connect_sqlite(DB_PATH)

    # ---- Sidebar: Filters ----
    with st.sidebar:
        st.title("FILTERS")

        years = get_available_years(conn)
        if not years:
            st.error("No data found. Load revenue and bank statements first.")
            conn.close()
            st.stop()

        selected_year = st.selectbox("YEAR", options=years, key="filter_year")
        months_for_year = get_available_months_for_year(conn, selected_year)
        month_options = [None] + months_for_year  # None = "All months"

        def _month_label(i: int) -> str:
            if i == 0:
                return "ALL MONTHS"
            return _format_month(month_options[i])

        month_idx = st.selectbox(
            "MONTH",
            range(len(month_options)),
            format_func=lambda i: _month_label(i),
            key="filter_month",
        )
        selected_month = month_options[month_idx] if month_idx > 0 else None

        compare_to = st.selectbox(
            "COMPARE TO",
            options=["Previous month", "Same month last year"],
            key="filter_compare",
        )

        st.divider()
        st.subheader("DATA SOURCES")
        _render_data_sources(conn)
        st.divider()
        show_sources = st.checkbox(
            "SHOW DATA SOURCES",
            value=False,
            help="Show which DB table each metric pulls from.",
        )

    # Build period and compare period for KPI deltas
    period = period_from_year_month(selected_year, selected_month)
    compare_period: DashboardPeriod | None = None
    if selected_month:
        compare_month = get_compare_period(selected_month, compare_to)
        if compare_month:
            compare_period = period_from_year_month(int(compare_month[:4]), compare_month)
    else:
        # All months: compare to prior year
        compare_period = build_period(
            f"{selected_year - 1}-01-01",
            f"{selected_year - 1}-12-31",
        )

    # ---- Main: Page header ----
    st.title("VENN FINANCIAL DASHBOARD")
    _render_small_copy(
        f"{_format_period_label(period, selected_month)} · Porto · plant-based. "
        "Summary: Vendus revenue and trading-day counts; category bars are bank inflows/outflows. "
        "Bank Details tab has full cash KPIs and transactions."
    )
    st.divider()

    # ---- Tabs ----
    summary_tab, bank_tab = st.tabs(["SUMMARY", "BANK DETAILS"])

    with summary_tab:
        _render_summary_tab(conn, period, compare_period, show_sources)

    with bank_tab:
        _render_bank_details_tab(conn, period, compare_period, show_sources)

    conn.close()


def _render_summary_tab(
    conn,
    period: DashboardPeriod,
    compare_period: DashboardPeriod | None,
    show_sources: bool,
) -> None:
    """Summary tab: revenue KPIs, operational days, legend, category bar charts."""
    # ----------------------------------
    # Collapsible glossary so first-time readers decode VAT vs net and data sources
    # ----------------------------------
    with st.expander("How to read this dashboard", expanded=False):
        st.markdown(
            """
**Glossary**
- **Net revenue (with VAT)** — Total sales including VAT from Vendus (`sales_gross`).
- **Net revenue (without VAT)** — Same period, excluding VAT (`sales_net`).
- **Operational day** — A calendar day in the filter range with at least one sale recorded.
- **Closed day** — A day in the range with no sale row (closed, holiday, or missing POS export).

**Two different data sources**
- Top revenue cards come from **Vendus** daily totals.
- **Inflow / outflow** charts use **Caixa + Millennium** movements and your category mapping — not the same as “sales by product”.

**Compare to** (sidebar) — Revenue card deltas use **Previous month** or **same month last year** when a single month is selected; for **All months** they compare to the **prior calendar year**.
            """.strip()
        )

    kpis = get_summary_kpis_for_period(conn, period, compare_period)
    days_info = get_operational_days_for_period(conn, period)

    st.subheader("BUSINESS REVENUE")
    rev_c1, rev_c2 = st.columns(2)
    with rev_c1:
        k = kpis["revenue_gross"]
        delta_str = f"{k['delta_abs']} | {k['delta_pct']}" if k["delta_abs"] != "N/A" else "N/A"
        _metric_card(
            label="NET REVENUE WITH VAT",
            value=f"€{k['value']:,.2f}",
            delta=delta_str,
            delta_up=not str(k["delta_abs"]).startswith("-"),
            style="hero_green",
            source=k["source"],
            show_sources=show_sources,
        )
    with rev_c2:
        k = kpis["revenue_net"]
        delta_str = f"{k['delta_abs']} | {k['delta_pct']}" if k["delta_abs"] != "N/A" else "N/A"
        _metric_card(
            label="NET REVENUE WITHOUT VAT",
            value=f"€{k['value']:,.2f}",
            delta=delta_str,
            delta_up=not str(k["delta_abs"]).startswith("-"),
            style="hero_green",
            source=k["source"],
            show_sources=show_sources,
        )

    vat_bridge = kpis["revenue_gross"]["value"] - kpis["revenue_net"]["value"]
    avg_line = ""
    if days_info["avg_net_per_open_day"] is not None:
        avg_line = f"Average net revenue per **open day:** €{days_info['avg_net_per_open_day']:,.2f}. "
    max_d = days_info["max_revenue_date"]
    through_line = f"Latest revenue date in this period: **{max_d}**." if max_d else ""
    st.caption(
        f"{avg_line}{through_line} **VAT included in gross (approx.):** €{vat_bridge:,.2f} "
        "(gross − net for the period)."
    )

    st.subheader("OPERATIONAL DAYS")
    st.caption(
        "Closed days include holidays and any day with no POS row; they may also reflect export gaps."
    )
    day_c1, day_c2 = st.columns(2)
    with day_c1:
        _metric_card(
            label="DAYS OPEN (WITH SALES)",
            value=f"{days_info['operational_days']}",
            delta="",
            delta_up=True,
            style="hero_neutral",
            source=days_info["source"],
            show_sources=show_sources,
            show_delta=False,
        )
    with day_c2:
        _metric_card(
            label="DAYS CLOSED (NO SALES)",
            value=f"{days_info['closed_days']}",
            delta="",
            delta_up=True,
            style="hero_neutral",
            source=days_info["source"],
            show_sources=show_sources,
            show_delta=False,
        )
    st.caption(f"Calendar days in period: **{days_info['total_calendar_days']}**.")

    st.divider()

    # Inflow block (green)
    _section_header(
        "Inflow by category",
        "Money coming in, grouped by category. Shows where revenue and other credits come from.",
        COLOR_INFLOW,
    )
    inflow_df = get_category_inflows(conn, period)
    if inflow_df.empty:
        st.info("No inflow transactions for this period.")
    else:
        inflow_top_col, inflow_exclude_col = st.columns([1, 2])
        with inflow_top_col:
            inflow_top_n = st.selectbox(
                "SHOW (INFLOW)",
                options=["TOP 5", "TOP 10", "ALL"],
                index=1,
                key="inflow_top_n",
            )
        with inflow_exclude_col:
            inflow_exclude = st.multiselect(
                "EXCLUDE CATEGORIES (INFLOW)",
                options=sorted(inflow_df["category"].dropna().unique().tolist()),
                key="inflow_exclude_categories",
            )
        inflow_view = inflow_df[~inflow_df["category"].isin(inflow_exclude)].copy()
        inflow_view = inflow_view.sort_values("amount", ascending=False).reset_index(drop=True)
        if inflow_top_n == "TOP 5":
            inflow_view = inflow_view.head(5)
        elif inflow_top_n == "TOP 10":
            inflow_view = inflow_view.head(10)
        if inflow_view.empty:
            st.info("No inflow categories left after filters.")
        else:
            fig_in = px.bar(
                inflow_view,
                x="amount",
                y="category",
                orientation="h",
                color="category",
                color_discrete_sequence=[COLOR_INFLOW, "#97C459", "#FAC775", COLOR_NEUTRAL],
            )
            _style_summary_category_bar(fig_in, len(inflow_view), COLOR_INFLOW)
            st.plotly_chart(fig_in, use_container_width=True)
        _render_source_line("bank_transactions + transaction_category_map · amount > 0 grouped by category", show_sources)

    st.divider()

    # Outflow block (red)
    _section_header(
        "Outflow by category",
        "Money going out, grouped by category. Cost breakdown by expense type.",
        COLOR_OUTFLOW,
    )
    outflow_df = get_category_outflows(conn, period)
    if outflow_df.empty:
        st.info("No outflow transactions for this period.")
    else:
        outflow_top_col, outflow_exclude_col = st.columns([1, 2])
        with outflow_top_col:
            outflow_top_n = st.selectbox(
                "SHOW (OUTFLOW)",
                options=["TOP 5", "TOP 10", "ALL"],
                index=1,
                key="outflow_top_n",
            )
        with outflow_exclude_col:
            outflow_exclude = st.multiselect(
                "EXCLUDE CATEGORIES (OUTFLOW)",
                options=sorted(outflow_df["category"].dropna().unique().tolist()),
                key="outflow_exclude_categories",
            )
        outflow_view = outflow_df[~outflow_df["category"].isin(outflow_exclude)].copy()
        outflow_view = outflow_view.sort_values("amount", ascending=False).reset_index(drop=True)
        if outflow_top_n == "TOP 5":
            outflow_view = outflow_view.head(5)
        elif outflow_top_n == "TOP 10":
            outflow_view = outflow_view.head(10)
        if outflow_view.empty:
            st.info("No outflow categories left after filters.")
        else:
            fig_out = px.bar(
                outflow_view,
                x="amount",
                y="category",
                orientation="h",
                color="category",
                color_discrete_sequence=[COLOR_OUTFLOW, "#E07B7B", "#FAC775", COLOR_NEUTRAL],
            )
            _style_summary_category_bar(fig_out, len(outflow_view), COLOR_OUTFLOW)
            st.plotly_chart(fig_out, use_container_width=True)
        _render_source_line("bank_transactions + transaction_category_map · amount < 0 grouped by category", show_sources)


def _render_bank_details_tab(
    conn,
    period: DashboardPeriod,
    compare_period: DashboardPeriod | None,
    show_sources: bool,
) -> None:
    """Bank Details tab: KPIs and category expanders."""
    _section_header(
        "Bank transaction metrics",
        "Cash in, cash out, and net flow across Caixa and Millennium for the selected period.",
        COLOR_NEUTRAL,
    )

    kpis = get_bank_details_kpis(conn, period, compare_period)
    row1_col1, row1_col2, row1_col3 = st.columns(3)
    with row1_col1:
        k = kpis["total_inflow"]
        _metric_card(
            label="TOTAL INFLOW",
            value=f"€{k['value']:,.2f}",
            delta=f"{k['delta_abs']} | {k['delta_pct']}" if k["delta_abs"] != "N/A" else "N/A",
            delta_up=not str(k["delta_abs"]).startswith("-"),
            style="hero_green",
            source=k["source"],
            show_sources=show_sources,
        )
    with row1_col2:
        k = kpis["total_outflow"]
        _metric_card(
            label="TOTAL OUTFLOW",
            value=f"€{k['value']:,.2f}",
            delta=f"{k['delta_abs']} | {k['delta_pct']}" if k["delta_abs"] != "N/A" else "N/A",
            delta_up=not str(k["delta_abs"]).startswith("-"),
            style="hero_red",
            source=k["source"],
            show_sources=show_sources,
        )
    with row1_col3:
        k = kpis["net_cashflow"]
        _metric_card(
            label="NET CASHFLOW",
            value=f"€{k['value']:,.2f}",
            delta=f"{k['delta_abs']} | {k['delta_pct']}" if k["delta_abs"] != "N/A" else "N/A",
            delta_up=not str(k["delta_abs"]).startswith("-"),
            style="hero_neutral",
            source=k["source"],
            show_sources=show_sources,
        )

    row2_col1, row2_col2 = st.columns(2)
    with row2_col1:
        k = kpis["caixa_outflow"]
        _metric_card(
            label="CAIXA OUTFLOW",
            value=f"€{k['value']:,.2f}",
            delta=f"{k['delta_abs']} | {k['delta_pct']}" if k["delta_abs"] != "N/A" else "N/A",
            delta_up=not str(k["delta_abs"]).startswith("-"),
            style="hero_red",
            source=k["source"],
            show_sources=show_sources,
        )
    with row2_col2:
        k = kpis["millennium_outflow"]
        _metric_card(
            label="MILLENNIUM OUTFLOW",
            value=f"€{k['value']:,.2f}",
            delta=f"{k['delta_abs']} | {k['delta_pct']}" if k["delta_abs"] != "N/A" else "N/A",
            delta_up=not str(k["delta_abs"]).startswith("-"),
            style="hero_red",
            source=k["source"],
            show_sources=show_sources,
        )
    st.divider()

    # Inflow expanders
    _section_header(
        "Inflow transactions by category",
        "Expand each category to see individual transactions.",
        COLOR_INFLOW,
    )
    inflow_df = get_transactions_by_category(conn, period, "inflow")
    if inflow_df.empty:
        st.info("No inflow transactions for this period.")
    else:
        inflow_df["bank"] = inflow_df["bank"].str.capitalize()
        inflow_totals = inflow_df.groupby("category", as_index=False)["amount"].sum().sort_values("amount", ascending=False)
        for cat in inflow_totals["category"].tolist():
            cat_df = inflow_df[inflow_df["category"] == cat]
            total = cat_df["amount"].sum()
            with st.expander(f"{cat} — €{total:,.2f}"):
                show = cat_df[["value_date", "bank", "description_raw", "amount"]]
                st.dataframe(
                    show,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "value_date": st.column_config.DateColumn("Date", format="DD MMM YYYY"),
                        "bank": st.column_config.TextColumn("Bank"),
                        "description_raw": st.column_config.TextColumn("Description"),
                        "amount": st.column_config.NumberColumn("Amount", format="€%.2f"),
                    },
                )
        _render_source_line("bank_transactions + transaction_category_map · posted rows where amount > 0", show_sources)

    st.divider()

    # Outflow expanders
    _section_header(
        "Outflow transactions by category",
        "Expand each category to see individual transactions.",
        COLOR_OUTFLOW,
    )
    outflow_df = get_transactions_by_category(conn, period, "outflow")
    if outflow_df.empty:
        st.info("No outflow transactions for this period.")
    else:
        outflow_df["bank"] = outflow_df["bank"].str.capitalize()
        outflow_totals = (
            outflow_df.assign(abs_amount=outflow_df["amount"].abs())
            .groupby("category", as_index=False)["abs_amount"]
            .sum()
            .sort_values("abs_amount", ascending=False)
        )
        for cat in outflow_totals["category"].tolist():
            cat_df = outflow_df[outflow_df["category"] == cat]
            total = cat_df["amount"].abs().sum()
            with st.expander(f"{cat} — €{total:,.2f}"):
                show = cat_df[["value_date", "bank", "description_raw", "amount"]]
                st.dataframe(
                    show,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "value_date": st.column_config.DateColumn("Date", format="DD MMM YYYY"),
                        "bank": st.column_config.TextColumn("Bank"),
                        "description_raw": st.column_config.TextColumn("Description"),
                        "amount": st.column_config.NumberColumn("Amount", format="€%.2f"),
                    },
                )
        _render_source_line("bank_transactions + transaction_category_map · posted rows where amount < 0", show_sources)

if __name__ == "__main__":
    main()
