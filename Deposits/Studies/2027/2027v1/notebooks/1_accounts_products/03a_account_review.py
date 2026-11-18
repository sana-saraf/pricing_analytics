# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 03a | Account review by subproduct (before product assignment)
# MAGIC
# MAGIC **What this builds.** A review pack for every `sub_key` (bank | subproduct | current product), grouped by product group and current product, so the subproducts can be checked before they are rolled into products in 04:
# MAGIC
# MAGIC * monthly and yearly balances, account counts and WAR, next to month-end fed funds
# MAGIC * the yearly account rate distribution (P10 to P90 at each year-end month)
# MAGIC * the beta report from 03 (TTC linear and poly at lags 0 to 3 with R2, every UP / DOWN cycle, average up / down, the recommended method)
# MAGIC * a preview roll-up by current product (sums of its subproducts, balance-weighted betas)
# MAGIC
# MAGIC | Output | What it is |
# MAGIC |---|---|
# MAGIC | `outputs/html/Account_Review_Dashboard_<tag>.html` | Searchable cards: product group -> product -> subproduct, with charts and tables |
# MAGIC | `outputs/excel/Account_Review_<tag>.xlsx` | The same data as tabs (rollup, subproducts, beta report, yearly, monthly long and wide, fed funds) |
# MAGIC
# MAGIC **Reads:** `mv_account_war` (01, including the monthly account count and rate percentiles), `mv_account_metrics` (02), `mv_account_betas` (03), fed funds from `00_setup`.
# MAGIC
# MAGIC **Run after:** 03. **Next:** 04 product assignment.

# COMMAND ----------

# DBTITLE 1,Install openpyxl
# MAGIC %pip install -q openpyxl

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ../00_setup

# COMMAND ----------

# DBTITLE 1,Step 1 header
# MAGIC %md
# MAGIC ## Step 1. Inputs from 01 to 03
# MAGIC `mv_account_war` must carry the monthly `accounts` and `rate_p10` ... `rate_p90` columns (01 Step 7). If it does not, re-run 01.

# COMMAND ----------

# DBTITLE 1,1. Load the subproduct history, metrics and betas
import math, html as _html

for t in ("mv_account_war", "mv_account_metrics", "mv_account_betas"):
    assert S.table_exists(t), f"{S.table(t)} not built: run 01, 02 and 03 first"
PCT = ["rate_min", "rate_p10", "rate_p25", "rate_p50", "rate_p75", "rate_p90", "rate_max"]
war = spark.table(S.table("mv_account_war")).toPandas()
missing = [c for c in ["accounts", "accounts_with_balance"] + PCT if c not in war.columns]
assert not missing, f"mv_account_war has no {missing}: re-run 01_account_mapping (Step 7 adds them)"
metrics = spark.table(S.table("mv_account_metrics")).toPandas()
betas = spark.table(S.table("mv_account_betas")).toPandas()

war["eopdate"] = pd.to_datetime(war["eopdate"])
war["period"] = war["eopdate"].dt.to_period("M")
fed = EFFR[["effective_date", "UP_FFR"]].copy()
fed["period"] = pd.to_datetime(fed["effective_date"]).dt.to_period("M")
fed = fed.sort_values("effective_date").groupby("period", as_index=False)["UP_FFR"].last().rename(columns={"UP_FFR": "fed_funds"})
war = war.merge(fed, on="period", how="left")
SNAP = pd.Timestamp(S.snapshot_date)

for g in ("product_group",):
    for d in (war, metrics, betas):
        d[g] = d[g].fillna("Unmapped").replace("", "Unmapped")
print(f"{war.sub_key.nunique():,} sub_keys | {war.eopdate.nunique()} months ({war.eopdate.min():%Y-%m} to {war.eopdate.max():%Y-%m}) | "
      f"{len(metrics):,} with a snapshot balance | {len(betas):,} with betas | fed funds months matched: {war.fed_funds.notna().mean():.1%}")

# COMMAND ----------

# DBTITLE 1,Step 2 header
# MAGIC %md
# MAGIC ## Step 2. Review tables
# MAGIC
# MAGIC | Table | One row per | Notes |
# MAGIC |---|---|---|
# MAGIC | `monthly` | sub_key x month | balance, average balance, accounts (all in-scope / with a positive balance), WAR, the account rate distribution (unweighted, approximate), fed funds (upper bound, %) |
# MAGIC | `yearly` | sub_key x year | months, year-end balance (last month of the year with data), average monthly balance, YoY change in year-end balance, year-end and average accounts, balance-weighted average WAR, year-end WAR, average fed funds, P10 to P90 at the year-end month |
# MAGIC | `subproducts` | sub_key | snapshot metrics (02), history, and the headline betas (03); status = Active (accounts at the snapshot) or History only |
# MAGIC | `beta_report` | sub_key | every beta column from `mv_account_betas` |
# MAGIC | `product_rollup` | product group x current product | the subproducts summed at the snapshot, with balance-weighted recommended beta, R2 and average up / down. A preview only: the product betas are fitted in 05 |

# COMMAND ----------

# DBTITLE 1,2. Build the review tables
KEY_COLS = ["product_group", "product", "sub_key", "legacy_bank", "subproductid", "acct_typ_desc"]

# ---- monthly (long)
monthly = war.sort_values(["sub_key", "eopdate"])[
    ["sub_key", "legacy_bank", "subproductid", "product", "product_group", "acct_typ_desc", "eopdate", "accounts", "accounts_with_balance",
     "eopbook", "avgbook", "sumprod_eoprate", "war"] + PCT + ["fed_funds"]].reset_index(drop=True)

# ---- yearly
w = monthly.copy()
w["year"] = w.eopdate.dt.year
g = w.groupby(["sub_key", "year"])
yearly = g.agg(months=("eopdate", "count"), year_end_month=("eopdate", "max"), avg_balance=("eopbook", "mean"),
               avg_accounts=("accounts", "mean"), _sp=("sumprod_eoprate", "sum"), _sb=("eopbook", "sum"),
               avg_fed_funds=("fed_funds", "mean")).reset_index()
yearly["avg_war"] = np.where(yearly._sb > 0, yearly._sp / yearly._sb.where(yearly._sb > 0), np.nan)
ye = (g.tail(1)[["sub_key", "year", "eopbook", "accounts", "war"] + PCT[1:6]]
      .rename(columns={"eopbook": "year_end_balance", "accounts": "year_end_accounts", "war": "year_end_war"}))
yearly = yearly.merge(ye, on=["sub_key", "year"], how="left").sort_values(["sub_key", "year"])
prev = yearly.groupby("sub_key")["year_end_balance"].shift(1)
yearly["yoy_balance_change"] = np.where(prev > 0, yearly.year_end_balance / prev.where(prev > 0) - 1, np.nan)
keys = betas[KEY_COLS].drop_duplicates("sub_key")
yearly = keys.merge(yearly.drop(columns=["_sp", "_sb"]), on="sub_key", how="right")[
    KEY_COLS + ["year", "months", "year_end_month", "year_end_balance", "avg_balance", "yoy_balance_change", "year_end_accounts",
                "avg_accounts", "avg_war", "year_end_war", "avg_fed_funds"] + PCT[1:6]].reset_index(drop=True)

# ---- subproducts: snapshot metrics + history + headline betas
hist = (monthly[monthly.eopbook > 0].groupby("sub_key")
        .agg(months_with_balance=("eopdate", "nunique"), first_month=("eopdate", "min"), last_month=("eopdate", "max")).reset_index())
MET = ["accounts", "eopbook", "avgbook", "war"] + PCT + ["rate_std", "zero_rate_balance_share", "share_of_product_balance"]
BETA_HEAD = ["months_ttc", "recommended_method", "recommended_beta", "recommended_r2", "recommended_reason", "best_lm_lag", "best_lm_beta",
             "best_lm_r2", "best_poly_lag", "best_poly_beta", "best_poly_r2", "avg_up", "avg_down", "flag_thin_history", "flag_low_balance"]
subs = (betas[KEY_COLS + BETA_HEAD].merge(metrics[["sub_key"] + MET], on="sub_key", how="left").merge(hist, on="sub_key", how="left"))
subs["status"] = np.where(subs.accounts.fillna(0) > 0, "Active", "History only")
for c in ["accounts", "eopbook", "avgbook"]:
    subs[c] = subs[c].fillna(0)
subs = subs[KEY_COLS + ["status"] + MET + ["months_with_balance", "first_month", "last_month"] + BETA_HEAD]
subs = subs.sort_values(["product_group", "product", "eopbook"], ascending=[True, True, False]).reset_index(drop=True)

# ---- beta report (every column of mv_account_betas, keys first)
beta_report = betas[KEY_COLS + [c for c in betas.columns if c not in KEY_COLS]].sort_values(["product_group", "product", "snapshot_balance"], ascending=[True, True, False])

# ---- product rollup (preview of the roll-up into products)
def wavg(d, col, wcol="eopbook"):
    x = d[[col, wcol]].dropna()
    x = x[x[wcol] > 0]
    return float((x[col] * x[wcol]).sum() / x[wcol].sum()) if len(x) else np.nan

rollup = pd.DataFrame([dict(product_group=gk[0], product=gk[1], subproducts=len(d), active_subproducts=int((d.status == "Active").sum()),
                            banks=", ".join(sorted(d.legacy_bank.unique())), accounts=int(d.accounts.sum()), eopbook=float(d.eopbook.sum()),
                            war=wavg(d, "war"), wtd_recommended_beta=wavg(d, "recommended_beta"), wtd_recommended_r2=wavg(d, "recommended_r2"),
                            wtd_avg_up=wavg(d, "avg_up"), wtd_avg_down=wavg(d, "avg_down"))
                       for gk, d in subs.groupby(["product_group", "product"])])
rollup = rollup.sort_values(["product_group", "eopbook"], ascending=[True, False]).reset_index(drop=True)

# ---- wide month tables
def wide(col):
    p = monthly.pivot(index="sub_key", columns="eopdate", values=col).sort_index(axis=1)
    p.columns = [f"{c:%Y-%m-%d}" for c in p.columns]
    return keys[["product_group", "product", "sub_key"]].merge(p.reset_index(), on="sub_key", how="right").sort_values(["product_group", "product", "sub_key"])

fed_out = fed.assign(month=fed.period.astype(str)).loc[lambda d: d.period.between(monthly.eopdate.min().to_period("M"), monthly.eopdate.max().to_period("M")), ["month", "fed_funds"]]
print(f"monthly {len(monthly):,} rows | yearly {len(yearly):,} | subproducts {len(subs):,} ({(subs.status == 'Active').sum()} active) | "
      f"products {len(rollup)} in {rollup.product_group.nunique()} groups")
display(rollup)

# COMMAND ----------

# DBTITLE 1,2. Checks
# 1) the active subproducts tie to mv_account_metrics (02) and so to mv_accounts, by bank
chk = (subs.groupby("legacy_bank").agg(review_accounts=("accounts", "sum"), review_balance=("eopbook", "sum"))
       .join(metrics.groupby("legacy_bank").agg(metrics_accounts=("accounts", "sum"), metrics_balance=("eopbook", "sum"))))
assert ((chk.review_accounts == chk.metrics_accounts) & ((chk.review_balance - chk.metrics_balance).abs() < 1)).all(), chk
# 2) every sub_key with history or betas is in the review once
assert subs.sub_key.is_unique and set(subs.sub_key) == set(war.sub_key) == set(betas.sub_key), "sub_keys differ between war, betas and the review"
# 3) the snapshot month of the monthly history equals the snapshot metrics (different path: mv_account_war vs mv_accounts)
sm = monthly[monthly.eopdate == SNAP].set_index("sub_key")
act = subs[subs.status == "Active"].set_index("sub_key")
assert ((sm.loc[act.index, "eopbook"] - act.eopbook).abs() < 0.01).all() and (sm.loc[act.index, "accounts"] == act.accounts).all()
# 4) the rollup sums back to the subproducts
assert abs(rollup.eopbook.sum() - subs.eopbook.sum()) < 1 and rollup.accounts.sum() == subs.accounts.sum()
print("Checks passed: review ties to mv_account_metrics by bank, every sub_key once, snapshot month = snapshot metrics, rollup = subproducts")
display(chk.reset_index())

rows = []
no_ff = monthly[monthly.fed_funds.isna()].eopdate.dt.strftime("%Y-%m").unique()
if len(no_ff):
    rows.append(("NO_FED_FUNDS_MONTH", "INFO", f"{len(no_ff)} months", "no fed funds for: " + ", ".join(sorted(no_ff)), 0, 0.0))
neg_yoy = yearly[(yearly.yoy_balance_change < -0.5) & (yearly.year_end_balance > MIN_BAL)]
if len(neg_yoy):
    rows.append(("BALANCE_DROP_OVER_50PCT", "INFO", f"{neg_yoy.sub_key.nunique()} sub_keys",
                 "year-end balance fell by more than half in a year: " + ", ".join(sorted(neg_yoy.sub_key.unique())[:10]), 0, 0.0))
issues_review = log_issues("03a review", rows)

# COMMAND ----------

# DBTITLE 1,Step 3 header
# MAGIC %md
# MAGIC ## Step 3. Write `outputs/excel/Account_Review_<tag>.xlsx`
# MAGIC Navy headers, frozen panes, filters, number formats by column (balances $, WAR / rates / betas %, fed funds in % points, R2 in points 0 to 100) and a color scale on R2.

# COMMAND ----------

# DBTITLE 1,3. Account review workbook (expanded)
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Font, Alignment
from openpyxl.formatting.rule import ColorScaleRule

NAVY = "1F4E79"
WIDE_FMT = {"monthly_balance": '"$"#,##0', "monthly_accounts": "#,##0", "monthly_war": "0.00%"}

def xl_format(col, sheet):
    n = str(col).lower()
    if sheet in WIDE_FMT and re.match(r"\d{4}-\d{2}-\d{2}$", n): return WIDE_FMT[sheet]
    if re.search(r"(^|_)r2($|_)", n): return "0.0"
    if "fed_funds" in n: return "0.00"
    if "share" in n or "yoy" in n: return "0.00%"
    if re.search(r"eopbook|avgbook|balance|sumprod", n): return '"$"#,##0'
    if re.search(r"beta|^lm_\d|^poly_\d|^avg_up|^avg_down|^up_|^down_|war|^rate_", n): return "0.00%"
    if n in ("year",) or n.endswith("_lag"): return "0"
    if re.search(r"accounts|months|subproducts", n): return "#,##0"
    if re.search(r"month$|eopdate", n): return "yyyy-mm-dd"
    return None

def style_book(path):
    wb = load_workbook(path)
    for ws in wb.worksheets:
        hdr = [c.value for c in ws[1]]
        for c in ws[1]:
            c.fill, c.font = PatternFill("solid", fgColor=NAVY), Font(bold=True, color="FFFFFF", size=9)
            c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        ws.row_dimensions[1].height = 30
        for i, name in enumerate(hdr, 1):
            f = xl_format(name, ws.title)
            if f and ws.max_row > 1:
                for (cell,) in ws.iter_rows(min_row=2, max_row=ws.max_row, min_col=i, max_col=i):
                    cell.number_format = f
            if f == "0.0" and ws.max_row > 1:
                L = get_column_letter(i)
                ws.conditional_formatting.add(f"{L}2:{L}{ws.max_row}", ColorScaleRule(
                    start_type="num", start_value=0, start_color="F8696B", mid_type="num", mid_value=50, mid_color="FFEB84",
                    end_type="num", end_value=100, end_color="63BE7B"))
        ws.auto_filter.ref = ws.dimensions
    wb.save(path)

def _dates(df):
    d = df.copy()
    for c in d.columns:
        if pd.api.types.is_datetime64_any_dtype(d[c]):
            d[c] = d[c].dt.date
    return d

README_REV = pd.DataFrame({"tab": ["product_rollup", "subproducts", "beta_report", "yearly", "monthly", "monthly_balance",
                                   "monthly_accounts", "monthly_war", "fed_funds"],
    "what": ["By product group and current product: the subproducts summed at the snapshot; balance-weighted betas (preview; product betas come from 05)",
             "One row per sub_key: status, snapshot metrics (02), history, headline betas (03)",
             "Every beta column from mv_account_betas (03): TTC linear / poly by lag with R2, each UP / DOWN cycle by lag, averages, recommendation",
             "sub_key x year: year-end and average balance, YoY, accounts, average and year-end WAR, average fed funds, year-end P10 to P90",
             "sub_key x month: balance, accounts, WAR, account rate distribution, fed funds",
             "End balance, one column per month", "In-scope accounts, one column per month", "WAR, one column per month (the beta input)",
             "Month-end fed funds (upper bound, % points)"]})
# ---- Model Config tab: study settings at a glance ----
_cfg_rows = [
    ("Study tag", S.tag), ("Snapshot date", str(S.snapshot_date)), ("Mode", MODE),
    ("Mapping source", MAPPING_SOURCE), ("TTC cycle", TTC), ("Lags", str(LAGS)),
    ("Min months TTC", MIN_MONTHS), ("Min balance", f"${MIN_BAL:,.0f}"),
    ("Poly rate column", POLY_COL), ("R2 tolerance", TOL), ("Up/Down lag", UD_LAG),
    ("Products", rollup.mv_product.nunique() if "mv_product" in rollup.columns else len(rollup)),
    ("Subproducts", len(subs)), ("BDSP accounts", ", ".join(BDSP) if BDSP else "none"),
    ("Excluded subproducts", ", ".join(EXCL) if EXCL else "none"),
    ("Source tables mode", S.params.get("source_tables", "production")),
    ("Built", pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")),
]
model_config = pd.DataFrame(_cfg_rows, columns=["Setting", "Value"])

# ---- Accounts data tabs (merged from former accounts.xlsx in 01) ----
acc_df = spark.table(S.table("mv_accounts"))
acc_n = acc_df.count()
acc_recon = sqlq("""
SELECT legacy_bank, COUNT(*) AS accounts, COUNT(DISTINCT accountid) AS distinct_accounts, SUM(eopbook) AS balance,
       TRY_DIVIDE(SUM(sumprod_eoprate), SUM(eopbook)) AS war
FROM {{mv_accounts}} GROUP BY 1 ORDER BY 1
""").toPandas()
acc_by_sub = sqlq("""
SELECT legacy_bank, sub_key, subproductid, acct_typ_desc, COALESCE(product, 'UNMAPPED') AS product, product_group, pf_flag, bdsp_flag,
       COUNT(*) AS accounts, SUM(eopbook) AS eopbook, TRY_DIVIDE(SUM(sumprod_eoprate), SUM(eopbook)) AS war
FROM {{mv_accounts}} GROUP BY ALL ORDER BY legacy_bank, product, subproductid
""").toPandas()

# ---- Updated README ----
README_REV = pd.DataFrame({"tab": [
    "model_config", "reconciliation", "accounts_by_subproduct",
    "product_rollup", "subproducts", "beta_report", "yearly", "monthly",
    "monthly_balance", "monthly_accounts", "monthly_war", "fed_funds"],
    "what": [
    "Study settings: snapshot date, mode, cycle, lags, thresholds, product count",
    "Balance reconciliation by legacy bank at the snapshot",
    "Every subproduct at the snapshot: accounts, balance, WAR",
    "By product group and current product: subproducts summed at the snapshot; balance-weighted betas (preview)",
    "One row per sub_key: status, snapshot metrics (02), history, headline betas (03)",
    "Every beta column from mv_account_betas (03): TTC linear / poly by lag with R2, each UP / DOWN cycle by lag, averages, recommendation",
    "sub_key x year: year-end and average balance, YoY, accounts, average and year-end WAR, average fed funds, year-end P10 to P90",
    "sub_key x month: balance, accounts, WAR, account rate distribution, fed funds",
    "End balance, one column per month", "In-scope accounts, one column per month", "WAR, one column per month (the beta input)",
    "Month-end fed funds (upper bound, % points)"]})

REVIEW_XLSX = out("excel", "Account_Review", "xlsx")
write_xlsx(REVIEW_XLSX, {
    "README": README_REV,
    "model_config": model_config,
    "reconciliation": acc_recon,
    "accounts_by_subproduct": acc_by_sub,
    "product_rollup": rollup,
    "subproducts": _dates(subs),
    "beta_report": beta_report,
    "yearly": _dates(yearly),
    "monthly": _dates(monthly.drop(columns=["sumprod_eoprate"])),
    "monthly_balance": wide("eopbook"),
    "monthly_accounts": wide("accounts"),
    "monthly_war": wide("war"),
    "fed_funds": fed_out,
})
style_book(REVIEW_XLSX)
print(f"-> {REVIEW_XLSX} ({acc_n:,} accounts, {len(subs)} subproducts, {rollup.shape[0]} product rows)")

# COMMAND ----------

# DBTITLE 1,Step 4 header
# MAGIC %md
# MAGIC ## Step 4. Write `outputs/html/Account_Review_Dashboard_<tag>.html`
# MAGIC Same look as the earlier Product Mapping Dashboard. Sections by product group, a card per current product (its subproducts summed), and inside it a card per subproduct. Opening a card shows the rate tiles, a chart (monthly or yearly; balance or accounts as bars, WAR and fed funds as lines), the beta report by lag and the yearly table. Search, sort, bank and status filters, and a download of the filtered rows to Excel. Charts use Chart.js and the download uses SheetJS, both from a CDN (the page needs internet access for those two).

# COMMAND ----------

# DBTITLE 1,4. HTML template (layout, styles, script)
REVIEW_TEMPLATE = r"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Account Review __TAG__</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#f8f9fa;color:#1a1a2e;font-size:14px;line-height:1.5}
.wrap{padding:1.5rem 1.25rem;max-width:1400px;margin:0 auto}
.phdr{margin-bottom:1.25rem;display:flex;align-items:flex-start;justify-content:space-between;flex-wrap:wrap;gap:12px;padding:20px 24px;background:#fff;border-radius:12px;border:0.5px solid #e2e8f0}
.phdr h1{font-size:20px;font-weight:600;margin-bottom:3px}.phdr .sub{font-size:13px;color:#64748b}
.dpill{display:inline-flex;align-items:center;background:#e6f1fb;color:#185fa5;border-radius:999px;padding:5px 14px;font-size:12px;font-weight:600}
.summary{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin-bottom:1rem}
.sc{background:#fff;border-radius:10px;padding:14px 16px;border:0.5px solid #e2e8f0}
.sc .lbl{font-size:11px;text-transform:uppercase;letter-spacing:.5px;color:#64748b;margin-bottom:5px}.sc .val{font-size:22px;font-weight:700}
.controls{display:flex;gap:8px;margin-bottom:.75rem;flex-wrap:wrap;align-items:center;background:#fff;padding:12px 16px;border-radius:10px;border:0.5px solid #e2e8f0}
input[type=text]{flex:1;min-width:220px;font-size:13px;padding:7px 12px;border:0.5px solid #cbd5e1;border-radius:8px;background:#f8f9fa;outline:none}
input[type=text]:focus{border-color:#185fa5;background:#fff}
select{font-size:13px;padding:7px 12px;border:0.5px solid #cbd5e1;border-radius:8px;background:#f8f9fa;cursor:pointer}
.fgrp{display:flex}.fb{padding:7px 12px;border:0.5px solid #cbd5e1;background:#f8f9fa;color:#374151;cursor:pointer;font-size:12px;font-weight:500;border-right-width:0}
.fb:first-child{border-radius:8px 0 0 8px}.fb:last-child{border-radius:0 8px 8px 0;border-right-width:0.5px}
.fb.on{background:#185fa5;color:#fff;border-color:#185fa5}.fb:hover:not(.on){background:#e6f1fb;color:#185fa5}
.dl-btn{padding:7px 16px;border:0.5px solid #185fa5;border-radius:8px;background:#e6f1fb;color:#185fa5;cursor:pointer;font-size:13px;font-weight:600}.dl-btn:hover{background:#185fa5;color:#fff}
.ex-btn{padding:7px 14px;border:0.5px solid #cbd5e1;border-radius:8px;background:#f8f9fa;color:#374151;cursor:pointer;font-size:12px}.ex-btn:hover{background:#e6f1fb;color:#185fa5}
.notes{font-size:12px;color:#64748b;margin:0 0 1rem 2px}
.sec-hdr{font-size:11px;text-transform:uppercase;letter-spacing:.7px;font-weight:700;padding:7px 12px;border-radius:8px;margin:18px 0 8px;display:flex;justify-content:space-between;gap:12px}
.sec-hdr span.t{font-weight:600;text-transform:none;letter-spacing:0}
.card{background:#fff;border:0.5px solid #e2e8f0;border-radius:12px;margin-bottom:8px;overflow:hidden}
.card:hover{box-shadow:0 2px 8px rgba(0,0,0,.06)}
.card.sub{border-radius:10px;margin:6px 0;background:#fcfdfe}
.card-hdr{display:flex;align-items:center;gap:12px;padding:11px 16px;cursor:pointer;user-select:none;border-left:3px solid transparent}
.card.sub>.card-hdr{border-left-color:#cbd5e1;padding:9px 14px}
.card-hdr:hover{background:#f8f9fa}
.chev{width:16px;height:16px;flex-shrink:0;color:#94a3b8;transition:transform .2s}
.card.open>.card-hdr .chev{transform:rotate(90deg)}
.ci{flex:1;min-width:0}.cn{font-size:14px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.card.sub .cn{font-size:13px}.csub{font-size:12px;color:#64748b;margin-top:2px}
.mono{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:11px;color:#94a3b8}
.badge{display:inline-block;padding:2px 8px;border-radius:999px;font-size:11px;font-weight:600;margin-right:4px}
.bsnv{background:#fee2e2;color:#991b1b}.bpnfp{background:#dbeafe;color:#1e40af}.bhist{background:#f1f5f9;color:#475569}
.cs{display:flex;gap:16px;flex-shrink:0;align-items:center}.cst{text-align:right;min-width:64px}
.cst .cv{font-size:13px;font-weight:700}.cst .cl{font-size:10px;text-transform:uppercase;letter-spacing:.4px;color:#64748b}
.card-body{display:none;border-top:0.5px solid #f1f5f9;padding:0 16px 14px}
.card.open>.card-body{display:block}
.rs{display:grid;grid-template-columns:repeat(auto-fit,minmax(105px,1fr));gap:8px;padding:12px 0}
.rsc{border-radius:8px;padding:8px 10px;background:#f8f9fa;border:0.5px solid #e2e8f0}
.rsc .rv{font-size:14px;font-weight:700}.rsc .rl{font-size:10px;text-transform:uppercase;letter-spacing:.4px;color:#64748b;margin-top:2px}
.tg{display:flex;gap:10px;margin:4px 0 6px}.tb{padding:4px 10px;border:0.5px solid #cbd5e1;border-radius:6px;font-size:11px;cursor:pointer;background:#f8f9fa}
.tb.on{background:#185fa5;color:#fff;border-color:#185fa5}
.chart{position:relative;height:290px;margin-bottom:6px}
.h3{font-size:11px;text-transform:uppercase;letter-spacing:.6px;font-weight:700;color:#185fa5;margin:12px 0 6px}
.rec{font-size:12px;background:#f8f9fa;border:0.5px solid #e2e8f0;border-radius:8px;padding:8px 10px;margin-bottom:6px}
.flag{color:#a32d2d;font-weight:600}.note{font-size:11px;color:#64748b;margin:-4px 0 6px}
.tw{overflow-x:auto}
table.bt{border-collapse:collapse;font-size:12px;width:100%}
table.bt th{font-size:10px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:.3px;padding:6px 8px;border-bottom:0.5px solid #e2e8f0;text-align:left;background:#f8f9fa;white-space:nowrap}
table.bt td{padding:6px 8px;border-bottom:0.5px solid #f1f5f9;white-space:nowrap}
table.bt th.r,table.bt td.r{text-align:right}table.bt tr:hover td{background:#f8f9fa}
td.best{background:#e1f5ee !important;font-weight:700}
.nr{text-align:center;padding:48px;color:#64748b;background:#fff;border-radius:12px;border:0.5px solid #e2e8f0}
</style></head>
<body><div class="wrap">
<div class="phdr"><div><h1>Account Review by Subproduct</h1><div class="sub">__SUB__</div></div><div class="dpill">Snapshot __SNAP__</div></div>
<div class="summary" id="sum"></div>
<div class="controls">
 <input type="text" id="srch" placeholder="Search groups, products, subproducts, descriptions..." oninput="render()">
 <select id="srt" onchange="render()">
  <option value="bal-d">Balance (high to low)</option><option value="bal-a">Balance (low to high)</option>
  <option value="acc-d">Accounts (high to low)</option><option value="war-d">WAR (high to low)</option>
  <option value="beta-d">Recommended beta (high to low)</option><option value="beta-a">Recommended beta (low to high)</option>
  <option value="r2-a">R2 (low to high)</option><option value="name-a">Name A-Z</option>
 </select>
 <div class="fgrp"><div class="fb on" onclick="setF('b','all',this)">All banks</div><div class="fb" onclick="setF('b','SNV',this)">SNV</div><div class="fb" onclick="setF('b','PNFP',this)">PNFP</div></div>
 <div class="fgrp"><div class="fb on" onclick="setF('s','A',this)">Active</div><div class="fb" onclick="setF('s','H',this)">History only</div><div class="fb" onclick="setF('s','all',this)">All</div></div>
 <button class="ex-btn" id="exBtn" onclick="toggleAll()">Expand all</button>
 <button class="dl-btn" onclick="dlXl()">Download Excel</button>
</div>
<div class="notes">__NOTES__</div>
<div id="cards"></div>
</div>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js"></script>
<script>
var D=__DATA__;
var M=D.M,FF=D.FF,K=D.K,NM=M.length;
K.forEach(function(k,i){k.ix=i;});
var PAL=[["#E6F1FB","#185FA5","#0C447C"],["#EEEDFE","#534AB7","#3C3489"],["#E1F5EE","#0F6E56","#085041"],["#FAEEDA","#854F0B","#633806"],["#FAECE7","#993C1D","#712B13"],["#FBEAF0","#993556","#72243E"],["#EAF3DE","#3B6D11","#27500A"],["#F1EFE8","#5F5E5A","#444441"]];
var GC={};(function(){var t={};K.forEach(function(k){t[k.grp]=(t[k.grp]||0)+(k.bal||0);});Object.keys(t).sort(function(a,b){return t[b]-t[a];}).forEach(function(g,i){GC[g]=g==='Unmapped'?["#FCEBEB","#A32D2D","#791F1F"]:PAL[i%PAL.length];});})();
var PID={};K.forEach(function(k){var n=k.grp+'||'+k.prod;if(!(n in PID))PID[n]=Object.keys(PID).length;});
var bF='all',sF='A',allExp=false,charts={},VIEW={},PROD={},SER={};
var CHEV='<svg class="chev" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M9 18l6-6-6-6"/></svg>';
function fc(n){if(n==null||isNaN(n))return'\u2014';var a=Math.abs(n);if(a>=1e9)return'$'+(n/1e9).toFixed(2)+'B';if(a>=1e6)return'$'+(n/1e6).toFixed(1)+'M';if(a>=1e3)return'$'+(n/1e3).toFixed(0)+'K';return'$'+n.toFixed(0);}
function fp(r,d){if(r==null||isNaN(r))return'\u2014';return(r*100).toFixed(d==null?2:d)+'%';}
function ffp(r){if(r==null||isNaN(r))return'\u2014';return Number(r).toFixed(2)+'%';}
function fn(n){if(n==null||isNaN(n))return'\u2014';return Math.round(n).toLocaleString();}
function f1(n){if(n==null||isNaN(n))return'\u2014';return Number(n).toFixed(1);}
function esc(s){return String(s==null?'':s).replace(/[&<>"']/g,function(c){return{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];});}
function bb(b){return'<span class="badge b'+b.toLowerCase()+'">'+b+'</span>';}
function st(v,l){return'<div class="cst"><div class="cv">'+v+'</div><div class="cl">'+l+'</div></div>';}
function tile(v,l){return'<div class="rsc"><div class="rv">'+v+'</div><div class="rl">'+l+'</div></div>';}
function r2c(v){if(v==null||isNaN(v))return'\u2014';var c=v>=80?'#0f6e56':v>=50?'#854f0b':'#a32d2d';return'<span style="color:'+c+';font-weight:600">'+v.toFixed(1)+'</span>';}
function yoy(v){if(v==null||isNaN(v))return'\u2014';return'<span style="color:'+(v>=0?'#0f6e56':'#a32d2d')+'">'+(v>=0?'+':'')+(v*100).toFixed(1)+'%</span>';}
function keep(k,q){if(bF!=='all'&&k.bank!==bF)return false;if(sF!=='all'&&k.st!==sF)return false;if(!q)return true;return[k.k,k.grp,k.prod,k.sp,k.desc,k.bank].join(' ').toLowerCase().indexOf(q)>=0;}
function val(o,f){var isK=!!o.bt;if(f==='bal')return o.bal||0;if(f==='acc')return o.acc||0;if(f==='war')return o.war;if(f==='beta')return isK?o.bt.b:o.beta;if(f==='r2')return isK?o.bt.r2:o.r2;return(isK?o.sp+' '+(o.desc||''):o.prod).toLowerCase();}
function sorter(){var s=document.getElementById('srt').value.split('-'),f=s[0],d=s[1];return function(a,b){var x=val(a,f),y=val(b,f);if(x==null&&y==null)return 0;if(x==null)return 1;if(y==null)return -1;if(typeof x==='string')return d==='a'?x.localeCompare(y):y.localeCompare(x);return d==='a'?x-y:y-x;};}
function full(k,f){var a=new Array(NM).fill(null),s=k[f];for(var i=0;i<s.length;i++)a[k.i0+i]=s[i];return a;}
function kser(k){if(!SER[k.ix])SER[k.ix]={b:full(k,'b'),a:full(k,'a'),w:full(k,'w')};return SER[k.ix];}
function pser(p){var b=new Array(NM).fill(null),a=new Array(NM).fill(null),sp=new Array(NM).fill(0);
 (p.all||p.keys).forEach(function(k){for(var i=0;i<k.b.length;i++){var j=k.i0+i,v=k.b[i];if(v!=null){b[j]=(b[j]||0)+v;if(k.w[i]!=null)sp[j]+=k.w[i]*v;}if(k.a[i]!=null)a[j]=(a[j]||0)+k.a[i];}});
 return{b:b,a:a,sp:sp,w:b.map(function(v,j){return v>0?sp[j]/v:null;})};}
function pyear(s){var Y={},ord=[];for(var j=0;j<NM;j++){if(s.b[j]==null)continue;var y=M[j].slice(0,4);if(!Y[y]){Y[y]={n:0,sb:0,sp:0,sa:0,ff:0,nf:0};ord.push(y);}var o=Y[y];o.n++;o.sb+=s.b[j];o.sp+=s.sp[j];o.sa+=(s.a[j]||0);if(FF[j]!=null){o.ff+=FF[j];o.nf++;}o.ye=s.b[j];o.yea=s.a[j];o.yew=s.w[j];}
 var prev=null;return ord.map(function(y){var o=Y[y];var r=[+y,o.n,o.ye,o.sb/o.n,(prev!=null&&prev>0)?o.ye/prev-1:null,o.yea,o.sa/o.n,o.sb>0?o.sp/o.sb:null,o.yew,o.nf?o.ff/o.nf:null];prev=o.ye;return r;});}
function pc(x){return x==null?null:x*100;}
function mk(id,lab,bar,barLbl,money,war,fed){var el=document.getElementById('c_'+id);if(!el)return;
 if(typeof Chart==='undefined'){el.parentNode.innerHTML='<div class="note">Charts need internet access (Chart.js CDN).</div>';return;}
 if(charts[id]){try{charts[id].destroy();}catch(e){}}
 charts[id]=new Chart(el,{data:{labels:lab,datasets:[
  {type:'bar',label:barLbl,data:bar,yAxisID:'y',backgroundColor:'rgba(24,95,165,0.30)',borderWidth:0,order:3},
  {type:'line',label:'WAR',data:war,yAxisID:'y1',borderColor:'#dc2626',backgroundColor:'#dc2626',pointRadius:lab.length>30?0:3,borderWidth:2,order:1},
  {type:'line',label:'Fed funds (upper)',data:fed,yAxisID:'y1',borderColor:'#0f6e56',backgroundColor:'#0f6e56',borderDash:[5,3],pointRadius:lab.length>30?0:3,borderWidth:1.5,order:2}]},
  options:{responsive:true,maintainAspectRatio:false,animation:false,interaction:{mode:'index',intersect:false},
   plugins:{legend:{position:'bottom',labels:{boxWidth:12,font:{size:11}}},tooltip:{callbacks:{label:function(c){var v=c.parsed.y;if(v==null)return c.dataset.label+': \u2014';return c.dataset.label+': '+(c.dataset.yAxisID==='y'?(money?fc(v):fn(v)):v.toFixed(2)+'%');}}}},
   scales:{x:{ticks:{maxTicksLimit:16,font:{size:10}},grid:{display:false}},
    y:{position:'left',beginAtZero:true,ticks:{callback:function(v){return money?fc(v):fn(v);},font:{size:10}},title:{display:true,text:barLbl,font:{size:10}}},
    y1:{position:'right',beginAtZero:true,grid:{drawOnChartArea:false},ticks:{callback:function(v){return v.toFixed(2)+'%';},font:{size:10}},title:{display:true,text:'Rate (%)',font:{size:10}}}}}});}
function redraw(id){var isP=id.charAt(0)==='p',o=isP?PROD[id]:K[+id.slice(1)];if(!o)return;var s=isP?o.s:kser(o),Y=isP?o.y:o.y,v=VIEW[id]||{per:'M',met:'B'},B=v.met==='B';
 if(v.per==='M')mk(id,M,B?s.b:s.a,B?'Balance':'Accounts',B,s.w.map(pc),FF);
 else mk(id,Y.map(function(r){return String(r[0]);}),Y.map(function(r){return B?r[2]:r[5];}),B?'Year-end balance':'Year-end accounts',B,Y.map(function(r){return pc(r[7]);}),Y.map(function(r){return r[9];}));}
function setV(id,k,v,el){VIEW[id]=VIEW[id]||{per:'M',met:'B'};VIEW[id][k]=v;var p=el.parentNode;for(var i=0;i<p.children.length;i++)p.children[i].classList.remove('on');el.classList.add('on');redraw(id);}
function tgl(id){return'<div class="tg"><div class="fgrp"><span class="tb on" onclick="setV(\''+id+'\',\'per\',\'M\',this)">Monthly</span><span class="tb" onclick="setV(\''+id+'\',\'per\',\'Y\',this)">Yearly</span></div>'+
 '<div class="fgrp"><span class="tb on" onclick="setV(\''+id+'\',\'met\',\'B\',this)">Balance</span><span class="tb" onclick="setV(\''+id+'\',\'met\',\'A\',this)">Accounts</span></div></div>';}
function btable(bt){var s='<div class="rec"><b>Recommended:</b> '+esc(bt.m||'\u2014')+' &middot; beta <b>'+fp(bt.b,1)+'</b> &middot; R2 '+r2c(bt.r2)+' &middot; '+esc(bt.why||'')+
  ' &middot; TTC months '+fn(bt.mt)+' &middot; avg up <b>'+fp(bt.up,1)+'</b> &middot; avg down <b>'+fp(bt.dn,1)+'</b>'+(bt.fl?' &middot; <span class="flag">'+esc(bt.fl)+'</span>':'')+'</div>';
 s+='<div class="tw"><table class="bt"><thead><tr><th>Lag</th><th class="r">TTC linear</th><th class="r">R2</th><th class="r">TTC poly</th><th class="r">R2</th>';
 D.CYC.forEach(function(c){s+='<th class="r">'+esc(c.name)+' ('+c.dir.toLowerCase()+')</th>';});
 s+='<th class="r">Avg up</th><th class="r">Avg down</th></tr></thead><tbody>';
 D.LAGS.forEach(function(L,i){s+='<tr><td>'+L+'</td><td class="r'+(bt.bl===L?' best':'')+'">'+fp(bt.lm[i],1)+'</td><td class="r">'+r2c(bt.lr[i])+'</td><td class="r'+(bt.bp===L?' best':'')+'">'+fp(bt.pl[i],1)+'</td><td class="r">'+r2c(bt.pr[i])+'</td>';
  D.CYC.forEach(function(c){var a=bt.ud[c.code];s+='<td class="r">'+fp(a?a[i]:null,1)+'</td>';});
  s+='<td class="r'+(L===D.UDL?' best':'')+'">'+fp(bt.au[i],1)+'</td><td class="r'+(L===D.UDL?' best':'')+'">'+fp(bt.ad[i],1)+'</td></tr>';});
 return s+'</tbody></table></div><div class="note" style="margin-top:4px">Shaded: best-R2 linear lag, best-R2 poly lag, and the up / down lag used for the averages (lag '+D.UDL+').</div>';}
function ytable(Y,pct){var s='<div class="tw"><table class="bt"><thead><tr><th>Year</th><th class="r">Months</th><th class="r">Year-end balance</th><th class="r">Avg balance</th><th class="r">YoY</th><th class="r">Year-end accounts</th><th class="r">Avg accounts</th><th class="r">Avg WAR</th><th class="r">Year-end WAR</th><th class="r">Avg fed funds</th>';
 if(pct)s+='<th class="r">P10</th><th class="r">P25</th><th class="r">P50</th><th class="r">P75</th><th class="r">P90</th>';
 s+='</tr></thead><tbody>';
 Y.forEach(function(r){s+='<tr><td>'+r[0]+'</td><td class="r">'+r[1]+'</td><td class="r">'+fc(r[2])+'</td><td class="r">'+fc(r[3])+'</td><td class="r">'+yoy(r[4])+'</td><td class="r">'+fn(r[5])+'</td><td class="r">'+fn(r[6])+'</td><td class="r">'+fp(r[7])+'</td><td class="r">'+fp(r[8])+'</td><td class="r">'+ffp(r[9])+'</td>';
  if(pct)for(var i=10;i<15;i++)s+='<td class="r">'+fp(r[i])+'</td>';s+='</tr>';});
 return s+'</tbody></table></div>';}
function kcard(k){var id='k'+k.ix;
 return'<div class="card sub" id="'+id+'"><div class="card-hdr" onclick="tog(\''+id+'\')">'+CHEV+'<div class="ci"><div class="cn">'+esc(k.sp)+(k.desc?' &middot; '+esc(k.desc):'')+'</div><div class="csub">'+bb(k.bank)+(k.st==='H'?'<span class="badge bhist">History only, last '+esc(k.lm)+'</span>':'')+'<span class="mono">'+esc(k.k)+'</span></div></div>'+
  '<div class="cs">'+st(fc(k.bal),'Balance')+st(fn(k.acc),'Accounts')+st(fp(k.war),'WAR')+st(fp(k.bt.b,1),'Rec. beta')+st(f1(k.bt.r2),'R2')+st(fp(k.bt.up,1),'Avg up')+st(fp(k.bt.dn,1),'Avg down')+'</div></div><div class="card-body"></div></div>';}
function kbody(k){var id='k'+k.ix,r=k.r||[];
 var h='<div class="rs">'+tile(fp(k.war),'WAR')+tile(fp(r[0]),'Min')+tile(fp(r[1]),'P10')+tile(fp(r[2]),'P25')+tile(fp(r[3]),'Median')+tile(fp(r[4]),'P75')+tile(fp(r[5]),'P90')+tile(fp(r[6]),'Max')+
  tile(fn(k.acc),'Accounts')+tile(fp(k.zs,1),'Zero-rate balance')+tile(fp(k.sh,1),'Share of product')+tile(fn(k.mh),'Months w/ balance')+tile(esc(k.fm||'\u2014')+' to '+esc(k.lm||'\u2014'),'History')+'</div>';
 h+=tgl(id)+'<div class="chart"><canvas id="c_'+id+'"></canvas></div>';
 h+='<div class="h3">Beta report</div>'+btable(k.bt)+'<div class="h3">By year (rate percentiles at the year-end month)</div>'+ytable(k.y,true);return h;}
function pcard(p,c){var bk=Object.keys(p.banks).sort().map(bb).join('');
 return'<div class="card pc" id="'+p.id+'"><div class="card-hdr" style="border-left-color:'+c[1]+'" onclick="tog(\''+p.id+'\')">'+CHEV+'<div class="ci"><div class="cn">'+esc(p.prod)+'</div><div class="csub">'+bk+p.keys.length+' subproduct'+(p.keys.length>1?'s':'')+'</div></div>'+
  '<div class="cs">'+st(fc(p.bal),'Balance')+st(fn(p.acc),'Accounts')+st(fp(p.war),'WAR')+st(fp(p.beta,1),'Wtd beta')+st(f1(p.r2),'Wtd R2')+st(fp(p.up,1),'Avg up')+st(fp(p.dn,1),'Avg down')+'</div></div><div class="card-body"></div></div>';}
function pbody(p){p.all=K.filter(function(k){return k.grp===p.grp&&k.prod===p.prod&&(bF==='all'||k.bank===bF);});p.s=pser(p);p.y=pyear(p.s);
 var h='<div class="rs">'+tile(fc(p.bal),'Balance')+tile(fn(p.acc),'Accounts')+tile(fp(p.war),'WAR')+tile(p.keys.length,'Subproducts')+tile(fp(p.beta,1),'Wtd rec. beta')+tile(f1(p.r2),'Wtd R2')+tile(fp(p.up,1),'Wtd avg up')+tile(fp(p.dn,1),'Wtd avg down')+'</div>';
 h+='<div class="note">Chart and yearly table: every subproduct of this product summed as currently mapped (before 04), including history-only ones'+(bF==='all'?'':' ('+bF+' only)')+' ('+p.all.length+' subproducts). Tiles and weighted betas: the subproducts listed below, at the snapshot balance. The product betas are fitted in 05.</div>';
 h+=tgl(p.id)+'<div class="chart"><canvas id="c_'+p.id+'"></canvas></div><div class="h3">By year</div>'+ytable(p.y,false)+'<div class="h3">Subproducts ('+p.keys.length+')</div>';
 p.keys.forEach(function(k){h+=kcard(k);});return h;}
function tog(id){var el=document.getElementById(id);if(!el)return;var b=null;for(var i=0;i<el.children.length;i++)if(el.children[i].classList.contains('card-body'))b=el.children[i];
 if(!b.getAttribute('data-f')){b.innerHTML=id.charAt(0)==='p'?pbody(PROD[id]):kbody(K[+id.slice(1)]);b.setAttribute('data-f','1');}
 el.classList.toggle('open');if(el.classList.contains('open'))redraw(id);}
function setF(w,v,el){if(w==='b')bF=v;else sF=v;var p=el.parentNode;for(var i=0;i<p.children.length;i++)p.children[i].classList.remove('on');el.classList.add('on');render();}
function toggleAll(){allExp=!allExp;document.getElementById('exBtn').textContent=allExp?'Collapse all':'Expand all';document.querySelectorAll('.card.pc').forEach(function(c){if(c.classList.contains('open')!==allExp)tog(c.id);});}
function filtered(){var q=document.getElementById('srch').value.toLowerCase().trim();return K.filter(function(k){return keep(k,q);});}
function render(){var ks=filtered(),so=sorter();PROD={};allExp=false;document.getElementById('exBtn').textContent='Expand all';
 var pm={},pl=[];ks.forEach(function(k){var n=k.grp+'||'+k.prod;if(!pm[n]){pm[n]={id:'p'+PID[n],grp:k.grp,prod:k.prod,keys:[],bal:0,acc:0,sp:0,bb:0,bw:0,rb:0,rw:0,r2b:0,uw:0,ub:0,dw:0,db:0,banks:{}};pl.push(pm[n]);}
  var p=pm[n],b=k.bal||0;p.keys.push(k);p.banks[k.bank]=1;p.bal+=b;p.acc+=(k.acc||0);
  if(b>0){if(k.war!=null){p.sp+=k.war*b;p.bb+=b;}if(k.bt.b!=null){p.bw+=k.bt.b*b;p.rb+=b;}if(k.bt.r2!=null){p.rw+=k.bt.r2*b;p.r2b+=b;}if(k.bt.up!=null){p.uw+=k.bt.up*b;p.ub+=b;}if(k.bt.dn!=null){p.dw+=k.bt.dn*b;p.db+=b;}}});
 pl.forEach(function(p){p.war=p.bb?p.sp/p.bb:null;p.beta=p.rb?p.bw/p.rb:null;p.r2=p.r2b?p.rw/p.r2b:null;p.up=p.ub?p.uw/p.ub:null;p.dn=p.db?p.dw/p.db:null;p.keys.sort(so);PROD[p.id]=p;});
 var tb=0,ta=0,ts=0,tw=0;ks.forEach(function(k){var b=k.bal||0;tb+=b;ta+=(k.acc||0);if(b>0&&k.war!=null){ts+=k.war*b;tw+=b;}});
 var grs={};pl.forEach(function(p){(grs[p.grp]=grs[p.grp]||{n:p.grp,ps:[],bal:0,acc:0}).ps.push(p);grs[p.grp].bal+=p.bal;grs[p.grp].acc+=p.acc;});
 var gl=Object.keys(grs).map(function(g){return grs[g];}).sort(function(a,b){return b.bal-a.bal;});
 document.getElementById('sum').innerHTML='<div class="sc"><div class="lbl">Product groups</div><div class="val">'+gl.length+'</div></div><div class="sc"><div class="lbl">Current products</div><div class="val">'+pl.length+'</div></div>'+
  '<div class="sc"><div class="lbl">Subproducts</div><div class="val">'+ks.length+'</div></div><div class="sc"><div class="lbl">Snapshot balance</div><div class="val">'+fc(tb)+'</div></div>'+
  '<div class="sc"><div class="lbl">Accounts</div><div class="val">'+fn(ta)+'</div></div><div class="sc"><div class="lbl">WAR</div><div class="val">'+fp(tw?ts/tw:null)+'</div></div>';
 if(!ks.length){document.getElementById('cards').innerHTML='<div class="nr">No subproducts match the current filters.</div>';return;}
 var h=[];gl.forEach(function(g){var c=GC[g.n]||PAL[7];g.ps.sort(so);
  h.push('<div class="sec-hdr" style="background:'+c[0]+';color:'+c[2]+';border:0.5px solid '+c[1]+'"><span>'+esc(g.n)+'</span><span class="t">'+g.ps.length+' product'+(g.ps.length>1?'s':'')+' &middot; '+fc(g.bal)+' &middot; '+fn(g.acc)+' accounts</span></div>');
  g.ps.forEach(function(p){h.push(pcard(p,c));});});
 document.getElementById('cards').innerHTML=h.join('');}
function dlXl(){if(typeof XLSX==='undefined'){alert('The Excel library did not load (needs internet access). The full data is in '+D.xlsx);return;}
 var ks=filtered(),s1=[],s2=[];ks.forEach(function(k){var r=k.r||[];
  s1.push({product_group:k.grp,product:k.prod,sub_key:k.k,bank:k.bank,subproductid:k.sp,description:k.desc,status:k.st==='A'?'Active':'History only',accounts:k.acc,balance:k.bal,war:k.war,rate_p10:r[1],rate_p25:r[2],rate_p50:r[3],rate_p75:r[4],rate_p90:r[5],
   recommended_method:k.bt.m,recommended_beta:k.bt.b,recommended_r2:k.bt.r2,avg_up:k.bt.up,avg_down:k.bt.dn,months_ttc:k.bt.mt});
  k.y.forEach(function(y){s2.push({product_group:k.grp,product:k.prod,sub_key:k.k,year:y[0],months:y[1],year_end_balance:y[2],avg_balance:y[3],yoy_balance_change:y[4],year_end_accounts:y[5],avg_accounts:y[6],avg_war:y[7],year_end_war:y[8],avg_fed_funds:y[9],rate_p10:y[10],rate_p25:y[11],rate_p50:y[12],rate_p75:y[13],rate_p90:y[14]});});});
 var wb=XLSX.utils.book_new();XLSX.utils.book_append_sheet(wb,XLSX.utils.json_to_sheet(s1),'subproducts');XLSX.utils.book_append_sheet(wb,XLSX.utils.json_to_sheet(s2),'yearly');XLSX.writeFile(wb,'Account_Review_'+D.tag+'_filtered.xlsx');}
render();
</script></body></html>"""
print(f"template: {len(REVIEW_TEMPLATE):,} characters")

# COMMAND ----------

# DBTITLE 1,4. Build the dashboard data and write the HTML
def _n(v, nd=None):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f):
        return None
    return round(f, nd) if nd is not None else f

def _s(v):
    return None if v is None or (not isinstance(v, str) and pd.isna(v)) else str(v)

def _lag(v):
    v = _n(v)
    return None if v is None else int(v)

MONTHS = sorted(monthly.eopdate.unique())
MI = {pd.Timestamp(d): i for i, d in enumerate(MONTHS)}
ff_map = dict(zip(fed.period, fed.fed_funds))
CYCS = [dict(name=c["name"], dir=c["direction"], code=re.sub(r"\W", "", c["short_code"])) for _, c in UPDOWN.iterrows()]
Y_COLS = ["year", "months", "year_end_balance", "avg_balance", "yoy_balance_change", "year_end_accounts", "avg_accounts",
          "avg_war", "year_end_war", "avg_fed_funds"] + PCT[1:6]
Y_ND = [None, None, 0, 0, 4, None, 1, 6, 6, 4, 6, 6, 6, 6, 6]

by_key = {k: d for k, d in monthly.groupby("sub_key")}
yr_key = {k: d for k, d in yearly.groupby("sub_key")}
bt_key = betas.set_index("sub_key")
K = []
for r in subs.itertuples(index=False):
    m = by_key[r.sub_key].sort_values("eopdate")
    idx = [MI[pd.Timestamp(d)] for d in m.eopdate]
    i0, i1 = min(idx), max(idx)
    b, a, w = [None] * (i1 - i0 + 1), [None] * (i1 - i0 + 1), [None] * (i1 - i0 + 1)
    for j, eb, ac, wr in zip(idx, m.eopbook, m.accounts, m.war):
        b[j - i0], a[j - i0], w[j - i0] = _n(eb, 0), _lag(ac), _n(wr, 6)
    bt = bt_key.loc[r.sub_key]
    flags = [t for t, f in (("thin history", bt.get("flag_thin_history")), ("low balance", bt.get("flag_low_balance"))) if f is True or f == 1]
    yr = yr_key.get(r.sub_key, pd.DataFrame(columns=Y_COLS))
    active = r.status == "Active"
    K.append(dict(
        k=r.sub_key, bank=r.legacy_bank, sp=_s(r.subproductid), desc=_s(r.acct_typ_desc), grp=_s(r.product_group), prod=_s(r.product),
        st="A" if active else "H", acc=_lag(r.accounts), bal=_n(r.eopbook, 2), war=_n(r.war, 6) if active else None,
        r=[_n(getattr(r, c), 6) for c in PCT] if active else None, zs=_n(r.zero_rate_balance_share, 4), sh=_n(r.share_of_product_balance, 4),
        mh=_lag(r.months_with_balance), fm=f"{r.first_month:%Y-%m}" if pd.notna(r.first_month) else None,
        lm=f"{r.last_month:%Y-%m}" if pd.notna(r.last_month) else None, i0=i0, b=b, a=a, w=w,
        y=[[_n(v, nd) if nd is not None else _lag(v) for v, nd in zip(row, Y_ND)] for row in yr[Y_COLS].itertuples(index=False)],
        bt=dict(m=_s(bt.recommended_method), b=_n(bt.recommended_beta, 5), r2=_n(bt.recommended_r2, 2), why=_s(bt.recommended_reason),
                mt=_lag(bt.months_ttc), bl=_lag(bt.best_lm_lag), bp=_lag(bt.best_poly_lag), up=_n(bt.avg_up, 5), dn=_n(bt.avg_down, 5),
                lm=[_n(bt[f"lm_{L}"], 5) for L in LAGS], lr=[_n(bt[f"lm_r2_{L}"], 2) for L in LAGS],
                pl=[_n(bt[f"poly_{L}"], 5) for L in LAGS], pr=[_n(bt[f"poly_r2_{L}"], 2) for L in LAGS],
                au=[_n(bt[f"avg_up_{L}"], 5) for L in LAGS], ad=[_n(bt[f"avg_down_{L}"], 5) for L in LAGS],
                ud={c["code"]: [_n(bt.get(f"{c['dir'].lower()}_{c['code']}_{L}"), 5) for L in LAGS] for c in CYCS},
                fl=", ".join(flags))))

REVIEW_HTML = out("html", "Account_Review_Dashboard", "html")
DATA = dict(tag=S.tag, snap=str(S.snapshot_date), M=[f"{pd.Timestamp(d):%Y-%m}" for d in MONTHS],
            FF=[_n(ff_map.get(pd.Timestamp(d).to_period("M")), 4) for d in MONTHS], LAGS=LAGS, UDL=UD_LAG, CYC=CYCS, K=K,
            xlsx=str(REVIEW_XLSX.relative_to(MV_ROOT)))
ttc = NAMED[TTC]
sub_line = (f"{S.tag} &middot; {len(K)} subproducts by product group and current product (before 04) &middot; "
            f"history {DATA['M'][0]} to {DATA['M'][-1]} &middot; TTC {_html.escape(TTC)} ({ttc[0]:%Y-%m} to {ttc[-1]:%Y-%m})")
notes = ("Balances and accounts at the snapshot; WAR = sum(rate x balance) / sum(balance); rate percentiles are unweighted across accounts. "
         "Betas from 03: TTC linear and cubic poly at lags " + ", ".join(map(str, LAGS)) + "; up / down endpoint betas per cycle, averaged at lag "
         f"{UD_LAG}; R2 in points (0 to 100). Recommended = best-R2 linear vs best-R2 poly lag, AVG when within {TOL:g} pts. Fed funds = month-end upper bound.")
page = (REVIEW_TEMPLATE.replace("__TAG__", _html.escape(S.tag)).replace("__SUB__", sub_line).replace("__SNAP__", _html.escape(str(S.snapshot_date)))
        .replace("__NOTES__", _html.escape(notes)).replace("__DATA__", json.dumps(DATA, separators=(",", ":"), allow_nan=False).replace("</", "<\\/")))
REVIEW_HTML.write_text(page, encoding="utf-8")

# check the page data against the tables (a different path from the workbook): balances, accounts, keys, snapshot month
kb = pd.DataFrame([(k["k"], k["bank"], k["bal"] or 0, k["acc"] or 0) for k in K], columns=["sub_key", "legacy_bank", "bal", "acc"])
assert len(kb) == subs.sub_key.nunique() and kb.sub_key.is_unique
tie = kb.groupby("legacy_bank")[["bal", "acc"]].sum().join(metrics.groupby("legacy_bank")[["eopbook", "accounts"]].sum())
assert ((tie.bal - tie.eopbook).abs() < 1).all() and (tie.acc == tie.accounts).all(), tie
si = MI[SNAP]
snap_series = sum((k["b"][si - k["i0"]] or 0) for k in K if k["i0"] <= si < k["i0"] + len(k["b"]))
assert abs(snap_series - metrics.eopbook.sum()) < len(K), (snap_series, metrics.eopbook.sum())
print(f"-> {REVIEW_HTML} ({REVIEW_HTML.stat().st_size / 1e6:.1f} MB) | {len(K)} subproducts | {len(set((k['grp'], k['prod']) for k in K))} products | "
      f"page balance ties to mv_account_metrics by bank; the snapshot month of the series sums to ${snap_series / 1e9:,.2f}B")
displayHTML(f'<a href="/#workspace{str(REVIEW_HTML).replace("/Workspace", "", 1)}" target="_blank">{REVIEW_HTML.name}</a> &nbsp; '
            f'<a href="/#workspace{str(REVIEW_XLSX).replace("/Workspace", "", 1)}" target="_blank">{REVIEW_XLSX.name}</a>')