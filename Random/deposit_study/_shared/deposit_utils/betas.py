"""Beta engine (endpoint, linear regression, cubic polynomial) for the Deposit Studies.

The three ``*_beta_lag`` functions and ``_shorten`` are the notebook's original
logic, unchanged. What is new is that cycles, dates and table names come from the
Study (rate_cycles.csv, study_params.json) instead of being hardcoded in cells.

Column naming: ``{method} Lag {lag} {cycle_name}``  (e.g. ``Lm_Coef Lag 0 TTC PD``)
Short names:   ``{prefix}_{lag}_{cycle_code}``       (e.g. ``lm_0_TTC_PD``)
"""
from __future__ import annotations
import re
from functools import reduce

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score

LAGS = range(4)


# --------------------------------------------------------------------------
# Data loaders (SQL / spark side)
# --------------------------------------------------------------------------
def create_pg_products_view(study):
    """Temp view pg_products over this version's nmds_agg (identical SQL to the original cell)."""
    study.sql("""
    CREATE OR REPLACE TEMP VIEW pg_products AS
    SELECT
      EOPDATE,
      PRODUCT_GROUP,
      PRODUCT AS ACCT_PRODUCT,
      SUM(EOPBOOK)  AS TOTAL_BALANCE,
      SUM(AVGBOOK)  AS AVG_BALANCE,
      TRY_DIVIDE(SUM(sumprod_eoprate), SUM(eopbook)) AS WAR
    FROM {{nmds_agg}}
    WHERE product_group IS NOT NULL
    GROUP BY 1, 2, 3
    """, quiet=True)


def load_war_data(study):
    """Returns (raw_df, war_pivot, acct_to_pg). Same logic as the original 'WAR Pivot' cell."""
    create_pg_products_view(study)
    raw_df = study.spark.sql("SELECT * FROM pg_products").toPandas()
    raw_df["EOPDATE"] = pd.to_datetime(raw_df["EOPDATE"])
    raw_df = raw_df.dropna(subset=["EOPDATE"])

    # Collapse to unique (ACCT_PRODUCT, EOPDATE) via balance-weighted WAR
    raw_df["_war_num"] = raw_df["WAR"].fillna(0) * raw_df["TOTAL_BALANCE"].fillna(0)
    raw_df = (
        raw_df.groupby(["ACCT_PRODUCT", "EOPDATE"], as_index=False)
        .agg(
            PRODUCT_GROUP=("PRODUCT_GROUP", "first"),
            TOTAL_BALANCE=("TOTAL_BALANCE", "sum"),
            AVG_BALANCE=("AVG_BALANCE", "sum"),
            _war_num=("_war_num", "sum"),
        )
    )
    raw_df["WAR"] = (raw_df["_war_num"] / raw_df["TOTAL_BALANCE"]).where(raw_df["TOTAL_BALANCE"] > 0)
    raw_df = raw_df.drop(columns="_war_num")

    war_pivot = (
        raw_df[["ACCT_PRODUCT", "EOPDATE", "WAR"]]
        .pivot(index="ACCT_PRODUCT", columns="EOPDATE", values="WAR")
        .reset_index()
    )
    war_pivot.columns = [
        c.strftime("%Y-%m-%d") if isinstance(c, pd.Timestamp) else c
        for c in war_pivot.columns
    ]
    war_pivot.rename(columns={"ACCT_PRODUCT": "EMP_NAME"}, inplace=True)

    acct_to_pg = (
        raw_df[["ACCT_PRODUCT", "PRODUCT_GROUP"]]
        .drop_duplicates()
        .set_index("ACCT_PRODUCT")["PRODUCT_GROUP"]
        .to_dict()
    )
    return raw_df, war_pivot, acct_to_pg


def load_effr_monthly(study):
    """Month-end fed funds table (last observation per month)."""
    effr = study.spark.sql("SELECT * FROM {src}".format(src=study.tokens()["src_fed_funds"])).toPandas()
    return effr_monthly_from_raw(effr)


def effr_monthly_from_raw(effr: pd.DataFrame) -> pd.DataFrame:
    effr = effr.copy()
    effr["effective_date"] = pd.to_datetime(effr["effective_date"])
    effr["month_end"] = effr["effective_date"] + pd.offsets.MonthEnd(0)
    return (
        effr.sort_values("effective_date")
        .groupby("month_end")
        .last()
        .reset_index()
    )


def build_named_cycles(effr_monthly: pd.DataFrame, cycles: pd.DataFrame):
    """List of (cycle_name, [month_end dates]) in the order of rate_cycles.csv.

    Replaces the hardcoded rate_cycle_* blocks. Dates are inclusive and are matched
    against the month-end effective_date, exactly as the original cell did.
    """
    out = []
    for _, r in cycles.iterrows():
        lo, hi = pd.Timestamp(r["start"]), pd.Timestamp(r["end"])
        dates = (
            effr_monthly.loc[
                (effr_monthly["effective_date"] >= lo) & (effr_monthly["effective_date"] <= hi),
                "effective_date",
            ].sort_values().tolist()
        )
        out.append((r["name"], dates))
    return out


# --------------------------------------------------------------------------
# Beta functions (unchanged from the notebook)
# --------------------------------------------------------------------------
def lm_beta_lag(grouped_df_ts, rate_cycle, lag_months, effr_monthly, cycle_name=""):
    rate_cycle = sorted(rate_cycle)
    start_date = rate_cycle[0]
    end_date = rate_cycle[-1]
    start_adj = rate_cycle[lag_months]
    end_adj = rate_cycle[-(lag_months + 1)]

    x = effr_monthly.loc[
        (effr_monthly["effective_date"] >= start_date)
        & (effr_monthly["effective_date"] <= end_adj),
        "UP_FFR"
    ].values

    results = []
    date_cols = pd.to_datetime(grouped_df_ts.columns[1:])
    for _, row in grouped_df_ts.iterrows():
        mask = (date_cols >= start_adj) & (date_cols <= end_date)
        y = row.iloc[1:][mask].astype(float).values
        coef, r2 = np.nan, np.nan
        if len(y) == len(x):
            valid = ~np.isnan(y) & ~np.isnan(x)
            if valid.sum() >= 2:
                model = LinearRegression()
                model.fit(x[valid].reshape(-1, 1), y[valid])
                pred = model.predict(x[valid].reshape(-1, 1))
                coef = model.coef_[0] * 100
                r2 = r2_score(y[valid], pred) * 100
        results.append([row["EMP_NAME"], coef, round(r2, 2)])        # beta unrounded (full precision); R2 to 2 dp

    return pd.DataFrame(results, columns=[
        "EMP_NAME",
        f"Lm_Coef Lag {lag_months} {cycle_name}",
        f"R_Square Lag {lag_months} {cycle_name}"
    ])


def endpoint_beta_lag(grouped_df_ts, rate_cycle, lag_months, effr_monthly, cycle_name=""):
    start_date = rate_cycle[0]
    end_date = rate_cycle[-1]
    start_adj = rate_cycle[lag_months]
    end_adj = rate_cycle[-(lag_months + 1)]

    fed_begin = effr_monthly.loc[effr_monthly["effective_date"] == start_date, "UP_FFR"].iloc[0]
    fed_end = effr_monthly.loc[effr_monthly["effective_date"] == end_adj, "UP_FFR"].iloc[0]
    fed_change = fed_end - fed_begin

    start_adj_str = start_adj.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    results = []
    for _, row in grouped_df_ts.iterrows():
        war_start = float(row.get(start_adj_str, np.nan)) if start_adj_str in grouped_df_ts.columns else np.nan
        war_end = float(row.get(end_str, np.nan)) if end_str in grouped_df_ts.columns else np.nan
        if pd.notna(war_start) and pd.notna(war_end) and fed_change != 0:
            beta = ((war_end - war_start) / fed_change) * 100
        else:
            beta = np.nan
        results.append([row["EMP_NAME"], beta])                       # beta unrounded (full precision)

    return pd.DataFrame(results, columns=[
        "EMP_NAME",
        f"Endpoint_Beta Lag {lag_months} {cycle_name}"
    ])


def poly_beta_lag(grouped_df_ts, rate_cycle, lag_months, effr_monthly,
                  rate_col="UP_FFR", cycle_name=""):
    """Cubic polynomial beta: derivative of y = b0 + b1*x + b2*x^2 + b3*x^3
    evaluated at the end-of-cycle fed funds rate."""
    rate_cycle = sorted(rate_cycle)
    start_date = rate_cycle[0]
    end_date = rate_cycle[-1]
    start_adj = rate_cycle[lag_months]
    end_adj = rate_cycle[-(lag_months + 1)]

    x = effr_monthly.loc[
        (effr_monthly["effective_date"] >= start_date)
        & (effr_monthly["effective_date"] <= end_adj),
        rate_col
    ].values.astype(float)

    # Fed funds rate at end of lag-adjusted window for derivative evaluation
    x_end = x[-1] if len(x) > 0 else np.nan

    results = []
    date_cols = pd.to_datetime(grouped_df_ts.columns[1:])
    for _, row in grouped_df_ts.iterrows():
        mask = (date_cols >= start_adj) & (date_cols <= end_date)
        y = row.iloc[1:][mask].astype(float).values
        poly_beta, r2 = np.nan, np.nan
        if len(y) == len(x):
            valid = ~np.isnan(y) & ~np.isnan(x)
            if valid.sum() >= 4:  # need >= 4 points for cubic
                xv, yv = x[valid], y[valid]
                X_design = np.column_stack(
                    [np.ones(len(xv)), xv, xv**2, xv**3]
                )
                coefs, _, _, _ = np.linalg.lstsq(X_design, yv, rcond=None)
                pred = X_design @ coefs
                ss_res = np.sum((yv - pred) ** 2)
                ss_tot = np.sum((yv - np.mean(yv)) ** 2)
                r2 = round((1 - ss_res / ss_tot) * 100, 2) if ss_tot > 0 else np.nan
                b1, b2, b3 = coefs[1], coefs[2], coefs[3]
                # dWAR/dx = b1 + 2*b2*x + 3*b3*x^2  at end-of-cycle rate
                poly_beta = (b1 + 2 * b2 * x_end + 3 * b3 * x_end**2) * 100   # unrounded (full precision)
        results.append([row["EMP_NAME"], poly_beta, r2])

    sfx = "_FFE" if rate_col == "EFFR" else ""
    label = f"Lag {lag_months} {cycle_name}"
    return pd.DataFrame(results, columns=[
        "EMP_NAME",
        f"Poly_Beta{sfx} {label}",
        f"Poly_R_Square{sfx} {label}",
    ])


def _shorten(col):
    m = re.match(
        r"(Poly_Beta(?:_FFE)?|Poly_R_Square(?:_FFE)?"
        r"|Endpoint_Beta|Lm_Coef|R_Square)"
        r" Lag (\d+) (.+)", col
    )
    if not m:
        return col
    pfx_map = {
        "Endpoint_Beta": "ed", "Lm_Coef": "lm", "R_Square": "r2",
        "Poly_Beta": "pb", "Poly_R_Square": "pr2",
        "Poly_Beta_FFE": "pbe", "Poly_R_Square_FFE": "pr2e",
    }
    pfx = pfx_map.get(m.group(1), m.group(1))
    lag = m.group(2)
    cyc = m.group(3).replace(" ", "_").replace("-", "")
    return f"{pfx}_{lag}_{cyc}"


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------
def _merge_all(dfs):
    return reduce(lambda l, r: pd.merge(l, r, on="EMP_NAME", how="outer"), dfs)


def run_all_betas(war_pivot, named_cycles, effr_monthly):
    """Runs endpoint, linear and cubic poly (UP_FFR and EFFR) for every cycle and lag.

    Returns (final_results with long column names, short_results with short names).
    """
    regression_dfs, endpoint_dfs, poly_dfs = [], [], []
    for cycle_name, cycle in named_cycles:
        for lag in LAGS:
            regression_dfs.append(lm_beta_lag(war_pivot, cycle, lag, effr_monthly, cycle_name))
            endpoint_dfs.append(endpoint_beta_lag(war_pivot, cycle, lag, effr_monthly, cycle_name))
            poly_dfs.append(poly_beta_lag(war_pivot, cycle, lag, effr_monthly, "UP_FFR", cycle_name))
            poly_dfs.append(poly_beta_lag(war_pivot, cycle, lag, effr_monthly, "EFFR", cycle_name))
    final_results = _merge_all(endpoint_dfs).merge(_merge_all(regression_dfs), on="EMP_NAME", how="outer")
    final_results = final_results.merge(_merge_all(poly_dfs), on="EMP_NAME", how="outer")
    short_results = final_results.copy()
    short_results.columns = [_shorten(c) for c in short_results.columns]
    return final_results, short_results


def save_final_results(study, final_results: pd.DataFrame) -> str:
    """Long-name results go to outputs/intermediate/ (Delta cannot hold names with spaces)."""
    p = study.output("intermediate", "final_results", "csv")
    final_results.to_csv(p, index=False)
    return str(p)


def load_final_results(study) -> pd.DataFrame:
    p = study.output("intermediate", "final_results", "csv")
    if not p.exists():
        raise FileNotFoundError(f"{p} not found. Run 03_beta_models first.")
    return pd.read_csv(p)
