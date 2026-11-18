# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 15b | WAL review workbook (InputsToDecide)
# MAGIC
# MAGIC **What this builds.** `outputs/review/wal_review_<tag>.xlsx`, in the shared workbook style (00_setup `xl_*`):
# MAGIC * **Review** (front page): study, months, weighting, the steps, and the sign-off cells *Reviewed by*, *Review date*, *Reviewed and Confirmed* (Y).
# MAGIC * **All products**: one row per product: Current, Calculated, Calculated on EOP balance, each month, the change Calculated vs Current (red when more than 25%), linked to each product tab.
# MAGIC * **One tab per product**: the WAL candidates (`row_id` CURRENT, CALCULATED, CALCULATED_EOP, one row per month, OVERRIDE) with `final` (x, exactly one; the WAL in use is pre-marked) and `note`; below it the distribution by month: accounts, average and EOP balance, WAL weighted by average balance / by EOP balance / simple, min, balance-weighted P10 / P25 / P50 (median) / P75 / P90, max.
# MAGIC * **Distribution** and **Month to month**: every product side by side, with colour scales.
# MAGIC
# MAGIC **WAL definitions** (config/study_params.json -> wal): Current = simple average of the `current_months` WALs; Calculated = the `calculated_month` WAL; both weighted by average balance (AvgBook), accounts mapped with the snapshot products. Percentiles are weighted by average balance (accounts with a positive average balance).
# MAGIC
# MAGIC **After the review.** Upload the filled-in workbook to `inputs/decisions/reviewed/`; `00_read_and_recommend` (decision = wals) writes the picks to `inputs/decisions/current/wals.csv`, which the Assumptions Summary uses for Management WAL.
# MAGIC
# MAGIC **Reads:** the shared WAL history (`wal.history_table`, built by 15), `mv_product_map`, `outputs/excel/wal_summary_<tag>.csv` (15, for a tie-out). **Run after:** 15_wal_ftp.

# COMMAND ----------

# DBTITLE 1,Install openpyxl
# MAGIC %pip install -q openpyxl

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ../00_setup

# COMMAND ----------

# DBTITLE 1,1. WAL distribution by product and month (from the shared history)
WCFG = S.params.get("wal", {}) or {}
_ls = lambda s: [x.strip() for x in str(s or "").split(",") if x.strip()]
HIST = WCFG["history_table"]
SCEN = str(WCFG.get("scenario", "0")).strip() or "0"
CUR_M, CALC_M = _ls(WCFG.get("current_months")), str(WCFG.get("calculated_month") or "").strip()
assert spark.catalog.tableExists(HIST), f"{HIST} is missing: run 15_wal_ftp first"
assert S.table_exists("mv_product_map"), "run 05_product_betas first"
PCTS = (10, 25, 50, 75, 90)

# one row per account and month with its snapshot product (same mapping as 15's metric view)
_base = f"""
SELECT CAST(h.wal_month AS STRING) AS wal_month, COALESCE(p.mv_product, 'UNMATCHED') AS mv_product, h.wal, h.avgbook, h.eopbook
FROM {HIST} h
LEFT JOIN (SELECT legacy_bank, CAST(accountid AS STRING) AS accountid, mv_product FROM {S.table('mv_product_map')}
           WHERE eopdate = '{S.snapshot_date}') p
  ON p.legacy_bank = h.legacy_bank AND p.accountid = h.accountid
WHERE h.scenario = '{SCEN}' AND h.wal IS NOT NULL"""
# balance-weighted percentiles: the WAL where the running share of average balance (accounts sorted by WAL) reaches p
stats = spark.sql(f"""
WITH b AS ({_base}),
s AS (SELECT wal_month, mv_product, COUNT(*) AS accounts, SUM(avgbook) AS avg_balance, SUM(eopbook) AS eop_balance,
            TRY_DIVIDE(SUM(wal * avgbook), SUM(avgbook)) AS wal_avgbal, TRY_DIVIDE(SUM(wal * eopbook), SUM(eopbook)) AS wal_eop,
            AVG(wal) AS wal_simple, PERCENTILE(wal, 0.5) AS median_accounts, MIN(wal) AS wal_min, MAX(wal) AS wal_max
      FROM b GROUP BY 1, 2),
c AS (SELECT wal_month, mv_product, wal,
             SUM(avgbook) OVER (PARTITION BY wal_month, mv_product ORDER BY wal ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
               / SUM(avgbook) OVER (PARTITION BY wal_month, mv_product) AS cum
      FROM b WHERE avgbook > 0),
q AS (SELECT wal_month, mv_product, {', '.join(f'MIN(CASE WHEN cum >= {p / 100} THEN wal END) AS p{p}' for p in PCTS)} FROM c GROUP BY 1, 2)
SELECT s.*, {', '.join(f'q.p{p}' for p in PCTS)} FROM s LEFT JOIN q USING (wal_month, mv_product) ORDER BY mv_product, wal_month""").toPandas()
for c in stats.columns.drop(["wal_month", "mv_product"]):
    stats[c] = pd.to_numeric(stats[c], errors="coerce")
MONTHS = sorted(stats.wal_month.unique())
print(f"{stats.mv_product.nunique()} products x {len(MONTHS)} months ({', '.join(MONTHS)}) | Current = avg {', '.join(CUR_M)} | Calculated = {CALC_M}")
display(stats[stats.wal_month == CALC_M].sort_values("avg_balance", ascending=False))

# COMMAND ----------

# DBTITLE 1,2. Candidates per product, the WAL in use, and the tie-out to 15
piv = stats.pivot_table(index="mv_product", columns="wal_month", values="wal_avgbal", aggfunc="first")
piv_eop = stats.pivot_table(index="mv_product", columns="wal_month", values="wal_eop", aggfunc="first")
cur_cols = [m for m in CUR_M if m in piv.columns]
cand = pd.DataFrame(index=piv.index)
cand["wal_current"] = piv[cur_cols].mean(axis=1, skipna=False) if len(cur_cols) == len(CUR_M) else np.nan   # every current month needed
cand["wal_calculated"] = piv[CALC_M] if CALC_M in piv.columns else np.nan
cand["wal_calc_eop"] = piv_eop[CALC_M] if CALC_M in piv_eop.columns else np.nan
calc_rows = stats[stats.wal_month == CALC_M].set_index("mv_product")
cand["avg_balance"], cand["accounts"] = calc_rows.avg_balance, calc_rows.accounts
cand["change_pct"] = cand.wal_calculated / cand.wal_current - 1
cand = cand.drop(index="UNMATCHED", errors="ignore").sort_values("avg_balance", ascending=False)

# the WAL in use: inputs/decisions/current/wals.csv (after a review), else the calculated WAL (the recommendation)
WALS_CUR = DECISIONS_DIR / "current" / "wals.csv"
cur_dec = (pd.read_csv(WALS_CUR, dtype=str, keep_default_na=False).set_index("product") if WALS_CUR.exists() else pd.DataFrame())
cand["in_use_row"] = [str(cur_dec.row_id.get(p, "") or "CALCULATED") if "row_id" in cur_dec else "CALCULATED" for p in cand.index]
cand["in_use_wal"] = [pd.to_numeric(cur_dec.wal.get(p), errors="coerce") if "wal" in cur_dec and p in cur_dec.index else cand.wal_calculated.get(p)
                      for p in cand.index]
CHG_FLAG = 0.25                                            # red when Calculated moves more than 25% from Current
cand["flag"] = np.where(cand.change_pct.abs() > CHG_FLAG, f"Calculated vs Current > {CHG_FLAG:.0%}", "")

# tie-out: Current / Calculated here = 15's wal_summary (same history, same mapping, computed in a different way)
_ws = Path(out("excel", "wal_summary", "csv"))
if _ws.exists():
    t = cand[["wal_current", "wal_calculated"]].join(pd.read_csv(_ws).set_index("mv_product")[["wal_current", "wal_calculated"]], rsuffix="_15")
    d = max((t.wal_current - t.wal_current_15).abs().max(), (t.wal_calculated - t.wal_calculated_15).abs().max())
    if d > 1e-9:
        alert("warn", f"WALs differ from 15's {_ws.name} (max {d:.2e}): re-run 15_wal_ftp, then this notebook", [])
    else:
        print(f"Tie-out: Current and Calculated = 15's {_ws.name} for all {len(t)} products (max diff {d:.1e}).")
else:
    alert("warn", f"{_ws.name} not found: run 15_wal_ftp first", [])
print(f"{len(cand)} products | {int((cand.flag != '').sum())} flagged (Calculated vs Current > {CHG_FLAG:.0%}) | in use from "
      + (f"{WALS_CUR.name} ({len(cur_dec)} products)" if len(cur_dec) else "the calculated WAL (no reviewed WALs yet)"))
display(cand.reset_index())

# COMMAND ----------

# DBTITLE 1,3. Write outputs/review/wal_review_<tag>.xlsx
REVIEW_DIR = OUTPUTS / "review"
REVIEW_DIR.mkdir(parents=True, exist_ok=True)
WAL_REVIEW_XLSX = REVIEW_DIR / f"{out_name('wal_review')}.xlsx"
CAND_COLS = ["row_id", "description", "wal", "wal_eop", "accounts", "avg_balance", "recommended", "final", "note"]
DIST_COLS = ["wal_month", "accounts", "avg_balance", "eop_balance", "wal_avgbal", "wal_eop", "wal_simple", "wal_min",
             *[f"p{p}" for p in PCTS], "wal_max", "median_accounts"]
DIST_FMT = {"accounts": "int", "avg_balance": "$", "eop_balance": "$", **{c: "m" for c in DIST_COLS if c.startswith(("wal_", "p", "median")) and c != "wal_month"}}
LEGEND = [("used", "green = WAL in use"), ("alt", "blue = recommended (Calculated)"), ("edit", "yellow = you fill in"),
          ("override", "orange = override"), ("warn", "red = check (moved > 25%)")]

def wal_rows(p):
    """The WAL candidates for one product: CURRENT, CALCULATED, CALCULATED_EOP, each month, then OVERRIDE."""
    c, st = cand.loc[p], stats[stats.mv_product == p].set_index("wal_month")
    g = lambda m, k: st[k].get(m) if m in st.index else np.nan
    rows = [dict(row_id="CURRENT", description=f"average of {', '.join(CUR_M)} (avg-balance weighted)", wal=c.wal_current,
                 wal_eop=np.nanmean([g(m, "wal_eop") for m in CUR_M]) if all(m in st.index for m in CUR_M) else np.nan),
            dict(row_id="CALCULATED", description=f"{CALC_M} (avg-balance weighted)", wal=c.wal_calculated, wal_eop=c.wal_calc_eop,
                 accounts=g(CALC_M, "accounts"), avg_balance=g(CALC_M, "avg_balance")),
            dict(row_id="CALCULATED_EOP", description=f"{CALC_M} weighted by EOP balance", wal=c.wal_calc_eop, wal_eop=c.wal_calc_eop,
                 accounts=g(CALC_M, "accounts"), avg_balance=g(CALC_M, "avg_balance"))]
    rows += [dict(row_id=f"MONTH_{m}", description=f"{m} only (avg-balance weighted)", wal=g(m, "wal_avgbal"), wal_eop=g(m, "wal_eop"),
                  accounts=g(m, "accounts"), avg_balance=g(m, "avg_balance")) for m in MONTHS]
    rows.append(dict(row_id="OVERRIDE", description="management WAL: type it in 'wal', put the x in 'final', say why in 'note'"))
    df = pd.DataFrame(rows).reindex(columns=CAND_COLS)
    df["recommended"] = np.where(df.row_id == "CALCULATED", "recommended", "")
    df["final"] = np.where(df.row_id == c.in_use_row, "x", "")
    df["note"] = ""
    return df

wb = _XWB()
xl_front(wb, "WAL review", "decide",
         [("Study", S.tag), ("Snapshot date", str(S.snapshot_date)), ("Current months", ", ".join(CUR_M)), ("Calculated month", CALC_M),
          ("Weighting", "average balance (AvgBook); EOP-weighted and simple averages shown for comparison"), ("Products", len(cand)),
          ("WAL in use", WALS_CUR.name if len(cur_dec) else "Calculated (no reviewed WALs yet)")],
         ["'All products' has every product's Current, Calculated and monthly WALs; red rows moved more than 25% from Current. Click a product to open its tab.",
          "On a product tab, the x in 'final' marks the WAL in use. Move it to the row you want (exactly one x per tab).",
          "To set your own WAL: type it in the OVERRIDE row's 'wal' (months), put the x there, and say why in 'note'.",
          "The table under the candidates shows the spread by month: min, balance-weighted P10 to P90 (P50 = median), max, and the WAL weighted three ways.",
          "Fill Reviewed by, Review date and Y in 'Reviewed and Confirmed'. Save, and upload to inputs/decisions/reviewed/ of this study.",
          "In 00_read_and_recommend: decision = wals, action = final review (reviewed Excel), Run all, then confirm."])

used = {"Review", "All products", "Distribution", "Month to month"}
SHEET = {}
for p in cand.index:
    SHEET[p] = xl_safe_name(p, used)
    used.add(SHEET[p])

# All products
ap = cand.reset_index().rename(columns={"mv_product": "product"})
ap.insert(1, "tab", ap["product"].map(SHEET))
for m in MONTHS:
    ap[f"wal_{m}"] = ap["product"].map(piv[m])
ap = ap[["product", "tab", "avg_balance", "accounts", "wal_current", "wal_calculated", "wal_calc_eop", "change_pct",
         *[f"wal_{m}" for m in MONTHS], "in_use_row", "in_use_wal", "flag"]]
ws = xl_sheet(wb, "All products", "WAL review: all products",
              [f"WAL in months by product. Current = avg of {', '.join(CUR_M)}; Calculated = {CALC_M}; both weighted by average balance.",
               "Red = Calculated moved more than 25% from Current. Click a tab name to review that product."], "decide", LEGEND)
xl_table(ws, ap, fmt={"avg_balance": "$", "accounts": "int", "change_pct": "pct1", **{c: "m" for c in ap.columns if c.startswith("wal_") or c == "in_use_wal"}},
         widths={"product": 32, "tab": 26, "flag": 30}, fills=lambda r: "warn" if r.flag else None, bars=("avg_balance",),
         links={"tab": lambda r: r.tab})

# one tab per product
for p in cand.index:
    c = cand.loc[p]
    ps = xl_sheet(wb, SHEET[p], f"WAL review: {p}",
                  [f"Pick the WAL for {p}: one x in 'final'. Recommended = Calculated ({CALC_M}, average-balance weighted).",
                   "Below: the spread of account WALs by month (balance-weighted percentiles) to judge how stable the WAL is."], "decide", LEGEND)
    facts = [("product", p), ("avg balance (calc month)", c.avg_balance), ("accounts (calc month)", c.accounts),
             ("in use", f"{c.in_use_row}  {c.in_use_wal:.1f} months" if pd.notna(c.in_use_wal) else c.in_use_row)]
    for i, (a, b) in enumerate(facts, XL_TABLE_ROW):
        ps.cell(i, 1, a).font = _XF(bold=True)
        ps.cell(i, 2, _xv(b))
    ps.cell(XL_TABLE_ROW + 1, 2).number_format = XL_FMT["$"]
    top = XL_TABLE_ROW + len(facts) + 1
    rows = wal_rows(p)
    fill = lambda r, _u=c.in_use_row: "override" if r.row_id == "OVERRIDE" else ("used" if r.row_id == _u else ("alt" if r.recommended else None))
    last = xl_table(ps, rows, row=top, fmt={"wal": "m", "wal_eop": "m", "accounts": "int", "avg_balance": "$"}, edit=("final", "note"),
                    fills=fill, widths={"row_id": 20, "description": 58, "note": 50},
                    bold=lambda r, h, _u=c.in_use_row: h == "wal" and r.row_id in (_u, "CALCULATED"))
    ps.cell(last, CAND_COLS.index("wal") + 1).fill = _XP("solid", fgColor=XL["edit"])     # OVERRIDE wal: you type it
    dv = _XDV(type="list", formula1='"x"', allow_blank=True)
    ps.add_data_validation(dv)
    dv.add(f"{_xcol(CAND_COLS.index('final') + 1)}{top + 1}:{_xcol(CAND_COLS.index('final') + 1)}{last}")
    d0 = last + 3
    ps.cell(d0 - 1, 1, "Distribution by month (WAL in months; percentiles weighted by average balance)").font = _XF(bold=True, color=XL["navy"])
    xl_table(ps, stats[stats.mv_product == p][DIST_COLS], row=d0, fmt=DIST_FMT, freeze=False, autofilter=False,
             fills=lambda r: "used" if r.wal_month == CALC_M else None)

# every product side by side
dist = stats[(stats.wal_month == CALC_M) & (stats.mv_product != "UNMATCHED")].sort_values("avg_balance", ascending=False)
ds = xl_sheet(wb, "Distribution", f"WAL distribution by product, {CALC_M}",
              ["Each product's account WALs at the calculated month: weighted three ways, min, balance-weighted percentiles, max.",
               "Colour scales run red (short) to green (long) down each column; a wide P10 to P90 spread means the WAL is less stable."], "assist")
xl_table(ds, dist[["mv_product"] + DIST_COLS[1:]], fmt=DIST_FMT, widths={"mv_product": 32}, scales=("wal_avgbal", "wal_eop", "p50"),
         bars=("avg_balance",))
mm = piv.drop(index="UNMATCHED", errors="ignore").reindex(cand.index).reset_index()
mm.columns = ["mv_product"] + [f"wal_{m}" for m in piv.columns]
for a, b in zip(MONTHS, MONTHS[1:]):
    mm[f"chg_{b[:7]}"] = mm[f"wal_{b}"] / mm[f"wal_{a}"] - 1
ms = xl_sheet(wb, "Month to month", "WAL month to month by product",
              ["Average-balance weighted WAL (months) by month, and the % change from the month before.",
               "Big moves point to a change in the Empyrean run or in the product's accounts: check them before choosing a WAL."], "assist")
xl_table(ms, mm, fmt={**{c: "m" for c in mm.columns if c.startswith("wal_")}, **{c: "pct1" for c in mm.columns if c.startswith("chg_")}},
         widths={"mv_product": 32}, scales=tuple(c for c in mm.columns if c.startswith("chg_")))
FIRST = ["Review", "All products", "Distribution", "Month to month"]     # front tabs, then one per product (largest first)
wb._sheets = [wb[n] for n in FIRST] + [s for s in wb.worksheets if s.title not in FIRST]
wb.save(WAL_REVIEW_XLSX)
print(f"{len(cand)} product tabs -> {WAL_REVIEW_XLSX}")
print(f"Tabs: {', '.join(wb.sheetnames[:4])}, then one per product")
print(f"After review: upload to {DECISIONS_DIR / 'reviewed'} and run 00_read_and_recommend (decision = wals).")