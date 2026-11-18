# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 15a | WAL review workbook (InputsToDecide)
# MAGIC
# MAGIC **What this builds.** `outputs/review/wal_review_<tag>.xlsx`, in the shared study workbook style (00_setup, *Workbook style*):
# MAGIC * **Review** (front page): study, snapshot, the WAL months, and the sign-off cells: *Reviewed by*, *Review date*, *Reviewed and Confirmed* (Y).
# MAGIC * **All products**: one row per product with Current, Calculated, the change, the EOP-weighted and median WALs, and a link to each product tab.
# MAGIC * **Methods**: what each WAL candidate means (InputsToInform).
# MAGIC * **One tab per product**:
# MAGIC   * **Pick the WAL**: candidate WALs (Current and Calculated, each weighted by average balance or by EOP balance, the account median, each month), plus an **OVERRIDE** row. Fill in `final` (x, exactly one) and `note`; for OVERRIDE also type the WAL (months).
# MAGIC   * **WAL by month**: accounts, average and EOP balance, WAL weighted by average balance / EOP balance / unweighted, min, P10, P25, median, P75, P90, max, transfer rate, FTP match.
# MAGIC   * **By bank**: the same by SNV / PNFP.
# MAGIC
# MAGIC **Colours:** green = the row pre-marked as final (the default: Calculated, or the override in `inputs/reference/wal_assumptions.csv`); blue = Current, for comparison; red on All products = Calculated moved more than 25% from Current; yellow = you fill in; orange = OVERRIDE.
# MAGIC
# MAGIC **After the review.** Upload the filled-in workbook to `inputs/decisions/reviewed/`; `00_read_and_recommend` (decision = wals) turns the picks into `inputs/decisions/current/wals.csv`, which 09 uses for the Assumptions Summary WAL Management column.
# MAGIC
# MAGIC **Reads:** the shared WAL history (`wal.history_table`), `mv_product_map`, `mv_final_betas` (product list), `outputs/excel/wal_summary_<tag>.csv` (15). **Run after:** 15_wal_ftp.

# COMMAND ----------

# DBTITLE 1,Install openpyxl
# MAGIC %pip install -q openpyxl

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ../00_setup

# COMMAND ----------

# DBTITLE 1,1. WAL distribution by product, month and bank (account level, shared history x snapshot products)
WCFG = S.params.get("wal", {}) or {}
_ls = lambda s: [x.strip() for x in str(s or "").split(",") if x.strip()]
HIST, SCEN = WCFG["history_table"], str(WCFG.get("scenario", "0")).strip() or "0"
CUR_M, CALC_M = _ls(WCFG.get("current_months")), str(WCFG.get("calculated_month") or "").strip()
assert spark.catalog.tableExists(HIST), f"{HIST} not built: run 15_wal_ftp first"
assert S.table_exists("mv_product_map") and S.table_exists("mv_final_betas"), "run 05 and 06 first"
WAL_SUM = Path(out("excel", "wal_summary", "csv"))
assert WAL_SUM.exists(), f"{WAL_SUM.name} missing: run 15_wal_ftp first"
wsum = pd.read_csv(WAL_SUM).set_index("mv_product")

# one pass over the account rows; product total rows have legacy_bank = 'ALL'
dist = spark.sql(f"""
WITH a AS (
  SELECT CAST(h.wal_month AS STRING) AS wal_month, m.mv_product, h.legacy_bank, h.wal, h.avgbook, h.eopbook, h.xfer_rate, h.ftp_matched
  FROM {HIST} h
  JOIN (SELECT legacy_bank, CAST(accountid AS STRING) AS accountid, mv_product FROM {S.table('mv_product_map')}
        WHERE eopdate = '{S.snapshot_date}' AND mv_product IS NOT NULL) m
    ON m.legacy_bank = h.legacy_bank AND m.accountid = h.accountid
  WHERE h.scenario = '{SCEN}'
)
SELECT wal_month, mv_product, COALESCE(legacy_bank, 'ALL') AS legacy_bank,
       COUNT(*) AS accounts, SUM(avgbook) AS avg_balance, SUM(eopbook) AS eop_balance,
       TRY_DIVIDE(SUM(wal * avgbook), SUM(avgbook) FILTER (WHERE wal IS NOT NULL)) AS wal_avgbal,
       TRY_DIVIDE(SUM(wal * eopbook), SUM(eopbook) FILTER (WHERE wal IS NOT NULL)) AS wal_eop,
       AVG(wal) AS wal_simple, MIN(wal) AS wal_min,
       PERCENTILE_APPROX(wal, ARRAY(0.1, 0.25, 0.5, 0.75, 0.9), 10000) AS _p, MAX(wal) AS wal_max,
       TRY_DIVIDE(SUM(xfer_rate * avgbook), SUM(avgbook) FILTER (WHERE xfer_rate IS NOT NULL)) AS xfer_rate,
       TRY_DIVIDE(SUM(avgbook) FILTER (WHERE ftp_matched), SUM(avgbook)) AS ftp_match
FROM a
GROUP BY GROUPING SETS ((wal_month, mv_product), (wal_month, mv_product, legacy_bank))
""").toPandas()
for i, p in enumerate(("p10", "p25", "p50", "p75", "p90")):
    dist[p] = dist._p.map(lambda v: v[i] if v is not None else None)
dist = dist.drop(columns="_p")
num_cols = [c for c in dist.columns if c not in ("wal_month", "mv_product", "legacy_bank")]
dist[num_cols] = dist[num_cols].apply(pd.to_numeric, errors="coerce")
MONTHS = sorted(dist.wal_month.unique())

prods = spark.table(S.table("mv_final_betas")).select("mv_product", "snapshot_balance").toPandas()
prods = prods.sort_values("snapshot_balance", ascending=False, ignore_index=True)
wala = (pd.read_csv(REFERENCE / "wal_assumptions.csv", dtype=str, keep_default_na=False).set_index("product_name")
        if (REFERENCE / "wal_assumptions.csv").exists() else pd.DataFrame(columns=["management_wal", "notes"]))
print(f"{len(dist):,} product / month / bank rows | months {', '.join(MONTHS)} | {len(prods)} products | "
      f"Current = avg {', '.join(CUR_M)} | Calculated = {CALC_M}")

# COMMAND ----------

# DBTITLE 1,2. Candidate WALs per product (what the reviewer picks from)
PICK_COLS = ["row_id", "basis", "months", "weighting", "wal", "final", "note"]
METHODS = pd.DataFrame([
    ("CURRENT", "Current assumption", "average of the current months' WALs", "average balance", "The study's Current WAL (Assumptions Summary)."),
    ("CURRENT_EOP", "Current, EOP weighted", "average of the current months' WALs", "EOP balance", "Same months, each account weighted by its month-end balance."),
    ("CALCULATED", "Calculated (default)", "calculated month", "average balance", "The study's Calculated WAL; the default Management WAL."),
    ("CALCULATED_EOP", "Calculated, EOP weighted", "calculated month", "EOP balance", "Calculated month weighted by month-end balance."),
    ("MEDIAN_CALC", "Account median", "calculated month", "none (each account counts once)", "Half the accounts have a shorter WAL, half longer. Not balance weighted."),
    ("MONTH_<yyyy-mm-dd>", "One month", "that month", "average balance", "Each loaded month on its own, to see the trend."),
    ("OVERRIDE", "Management override", "", "", "Type the WAL (months) in 'wal', put x in 'final', and write the reason in 'note'.")],
    columns=["row_id", "basis", "months", "weighting", "meaning"])

T = dist[dist.legacy_bank == "ALL"].set_index(["mv_product", "wal_month"])
_m = lambda p, m, c: T[c].get((p, m), np.nan)
_avg = lambda xs: float(np.mean(xs)) if xs and all(pd.notna(x) for x in xs) else np.nan     # every current month needed

def pick_rows(p):
    rows = [dict(row_id="CURRENT", basis="Current assumption", months=", ".join(CUR_M), weighting="average balance",
                 wal=_avg([_m(p, m, "wal_avgbal") for m in CUR_M])),
            dict(row_id="CURRENT_EOP", basis="Current, EOP weighted", months=", ".join(CUR_M), weighting="EOP balance",
                 wal=_avg([_m(p, m, "wal_eop") for m in CUR_M])),
            dict(row_id="CALCULATED", basis="Calculated (default)", months=CALC_M, weighting="average balance", wal=_m(p, CALC_M, "wal_avgbal")),
            dict(row_id="CALCULATED_EOP", basis="Calculated, EOP weighted", months=CALC_M, weighting="EOP balance", wal=_m(p, CALC_M, "wal_eop")),
            dict(row_id="MEDIAN_CALC", basis="Account median", months=CALC_M, weighting="none", wal=_m(p, CALC_M, "p50"))]
    rows += [dict(row_id=f"MONTH_{m}", basis="One month", months=m, weighting="average balance", wal=_m(p, m, "wal_avgbal")) for m in MONTHS]
    ov = pd.to_numeric(wala["management_wal"].get(p, ""), errors="coerce") if len(wala) else np.nan
    rows.append(dict(row_id="OVERRIDE", basis="Management override", months="", weighting="", wal=ov,
                     note=(wala["notes"].get(p, "") if len(wala) and "notes" in wala else "")))
    df = pd.DataFrame(rows).reindex(columns=PICK_COLS)
    df["final"] = np.where(df.row_id == ("OVERRIDE" if pd.notna(ov) else "CALCULATED"), "x", "")   # pre-marked default
    df["note"] = df.note.fillna("")
    return df

display(pick_rows(prods.mv_product.iloc[0]))

# COMMAND ----------

# DBTITLE 1,3. Write outputs/review/wal_review_<tag>.xlsx
REVIEW_DIR = OUTPUTS / "review"
REVIEW_DIR.mkdir(parents=True, exist_ok=True)
WAL_XLSX = REVIEW_DIR / f"{out_name('wal_review')}.xlsx"
MON_COLS = ["wal_month", "accounts", "avg_balance", "eop_balance", "wal_avgbal", "wal_eop", "wal_simple", "wal_min",
            "p10", "p25", "p50", "p75", "p90", "wal_max", "xfer_rate", "ftp_match"]
MON_FMT = {"accounts": "int", "avg_balance": "$", "eop_balance": "$", "xfer_rate": "pct", "ftp_match": "pct1",
           **{c: "m" for c in ("wal_avgbal", "wal_eop", "wal_simple", "wal_min", "p10", "p25", "p50", "p75", "p90", "wal_max")}}
LEGEND = [("used", "green = pre-marked final"), ("alt", "blue = Current (compare)"), ("edit", "yellow = you fill in"), ("override", "orange = override")]

wb = _XWB()
xl_front(wb, "WAL review", "decide",
         [("Study", S.tag), ("Snapshot date", str(S.snapshot_date)), ("Current months (average)", ", ".join(CUR_M)),
          ("Calculated month", CALC_M), ("Months loaded", ", ".join(MONTHS)), ("Products", len(prods)),
          ("Products mapped with", "the snapshot product map, for every month (like the betas)"), ("WAL units", "months")],
         ["'All products' lists every product: Current vs Calculated, the change (red = more than 25%), and the EOP-weighted and median WALs. Click a product to open its tab.",
          "On each product tab, 'Pick the WAL' (top) is what you decide. The default pick (x in 'final', green) is Calculated, or the override already in inputs/reference/wal_assumptions.csv.",
          "Compare the candidates with 'WAL by month' and 'By bank' below them: a wide P10-P90 range or a big gap between average-balance and EOP weighting means a few large accounts drive the WAL.",
          "Move the x in 'final' to the row you want (exactly one x). To override: type the WAL (months) on the OVERRIDE row, put the x there, and write the reason in 'note'.",
          "Fill Reviewed by, Review date, put Y in 'Reviewed and Confirmed', save, and upload the file to inputs/decisions/reviewed/ of this study.",
          "In 00_read_and_recommend: decision = wals, action = final review (reviewed Excel), Run all, then confirm."])

# All products
used = set(["Review", "All products", "Methods"])
sheet_of, allp = {}, []
for p in prods.mv_product:
    sheet_of[p] = xl_safe_name(p, used)
    used.add(sheet_of[p])
    pr = pick_rows(p).set_index("row_id").wal
    cur, calc = pr.get("CURRENT"), pr.get("CALCULATED")
    allp.append(dict(product=p, tab=sheet_of[p], avg_balance=_m(p, CALC_M, "avg_balance"), accounts=_m(p, CALC_M, "accounts"),
                     wal_current=cur, wal_calculated=calc,
                     change_pct=(calc / cur - 1) if pd.notna(cur) and pd.notna(calc) and cur else np.nan,
                     wal_calc_eop=pr.get("CALCULATED_EOP"), wal_calc_median=pr.get("MEDIAN_CALC"),
                     p10=_m(p, CALC_M, "p10"), p90=_m(p, CALC_M, "p90"), override=pr.get("OVERRIDE"),
                     xfer_rate=_m(p, CALC_M, "xfer_rate")))
ALLP = pd.DataFrame(allp)
ws = xl_sheet(wb, "All products", "WAL review: all products",
              [f"One row per product (largest first). Current = avg of {', '.join(CUR_M)}; Calculated = {CALC_M}; both weighted by average balance. WAL in months.",
               "Red = Calculated moved more than 25% from Current. Click the tab name to open the product."], "assist",
              [("warn", "red = moved > 25%")])
xl_table(ws, ALLP, fmt={"avg_balance": "$", "accounts": "int", "change_pct": "pct1", "xfer_rate": "pct",
                        **{c: "m" for c in ("wal_current", "wal_calculated", "wal_calc_eop", "wal_calc_median", "p10", "p90", "override")}},
         widths={"product": 34, "tab": 26}, fills=lambda r: "warn" if pd.notna(r.change_pct) and abs(r.change_pct) > 0.25 else None,
         bars=("avg_balance",), links={"tab": lambda r: r.tab})

ws = xl_sheet(wb, "Methods", "WAL review: what each candidate means",
              ["Each product tab offers these WALs. All come from the Empyrean account WAL file, with each account mapped to its snapshot product.",
               "Weighted = sum(WAL x balance) / sum(balance) over the accounts with a WAL."], "inform")
xl_table(ws, METHODS, widths={"row_id": 22, "basis": 26, "months": 34, "weighting": 26, "meaning": 90}, autofilter=False)

for p in prods.mv_product:
    sn = sheet_of[p]
    pk = pick_rows(p)
    ws = xl_sheet(wb, sn, f"WAL review: {p}",
                  [f"Pick the WAL for {p}: exactly one x in 'final'. Current = avg {', '.join(CUR_M)}; Calculated = {CALC_M}. WAL in months.",
                   "Below: the WAL by month (weighted by average / EOP balance, unweighted, and account percentiles) and by bank."], "decide", LEGEND)
    ws.cell(5, 1, "product").font = _XF(bold=True)
    ws.cell(5, 2, p).font = _XF(bold=True)
    last = xl_table(ws, pk, fmt={"wal": "m"}, widths={"row_id": 22, "basis": 26, "months": 26, "weighting": 18, "wal": 12, "note": 60},
                    edit=("final", "note"),
                    fills=lambda r: "override" if r.row_id == "OVERRIDE" else ("used" if r.final == "x" else ("alt" if r.row_id == "CURRENT" else None)),
                    bold=lambda r, c: c == "wal" and (r.final == "x" or r.row_id == "CURRENT"))
    ws.cell(last, PICK_COLS.index("wal") + 1).fill = _XP("solid", fgColor=XL["edit"])          # OVERRIDE wal: you type it
    dv = _XDV(type="list", formula1='"x"', allow_blank=True)
    ws.add_data_validation(dv)
    dv.add(f"{_xcol(PICK_COLS.index('final') + 1)}{XL_TABLE_ROW + 1}:{_xcol(PICK_COLS.index('final') + 1)}{last}")

    r0 = last + 3
    ws.cell(r0 - 1, 1, "WAL by month (all banks)").font = _XF(bold=True, color=XL["navy"])
    mon = dist[(dist.mv_product == p) & (dist.legacy_bank == "ALL")].sort_values("wal_month")[MON_COLS]
    last = xl_table(ws, mon, row=r0, fmt=MON_FMT, fills=lambda r: "used" if r.wal_month == CALC_M else ("alt" if r.wal_month in CUR_M else None),
                    scales=("wal_avgbal",), freeze=False, autofilter=False)
    r0 = last + 3
    ws.cell(r0 - 1, 1, "By bank").font = _XF(bold=True, color=XL["navy"])
    bk = dist[(dist.mv_product == p) & (dist.legacy_bank != "ALL")].sort_values(["wal_month", "legacy_bank"])[["legacy_bank"] + MON_COLS]
    xl_table(ws, bk, row=r0, fmt=MON_FMT, freeze=False, autofilter=False)
    for j, w in enumerate([22, 26, 26, 18, 12, 10, 60], 1):           # the pick table sets the first columns; keep them readable
        ws.column_dimensions[_xcol(j)].width = max(ws.column_dimensions[_xcol(j)].width or 0, w)
    ws.freeze_panes = ws.cell(XL_TABLE_ROW + 1, 2)

assert wb.sheetnames[:3] == ["Review", "All products", "Methods"], wb.sheetnames[:3]   # built in this order
wb.save(WAL_XLSX)
print(f"{len(prods)} product tabs -> {WAL_XLSX}")
print(f"Sheets: {', '.join(wb.sheetnames[:4])}, ...")
moved = ALLP[ALLP.change_pct.abs() > 0.25]
log_issues("15a WAL review", [("WAL_MOVED_OVER_25PCT", "INFO", ", ".join(moved["product"]), f"Calculated WAL moved more than 25% from Current ({len(moved)} products)", "", "")] if len(moved) else [])
display(ALLP)

# COMMAND ----------

# DBTITLE 1,4. Check: the workbook ties to 15 (wal_summary) and every tab has one final pick
from openpyxl import load_workbook
chk_wb = load_workbook(WAL_XLSX)
rows = []
for p in prods.mv_product:
    ws = chk_wb[sheet_of[p]]
    assert ws.cell(5, 2).value == p, f"tab {ws.title}: product cell says {ws.cell(5, 2).value}"
    hdr = [c.value for c in ws[XL_TABLE_ROW]][:len(PICK_COLS)]
    assert hdr == PICK_COLS, f"tab {ws.title}: header {hdr}"
    pk = {}
    r = XL_TABLE_ROW + 1
    while ws.cell(r, 1).value:
        pk[ws.cell(r, 1).value] = (ws.cell(r, 5).value, ws.cell(r, 6).value)
        r += 1
    n_x = sum(1 for v in pk.values() if str(v[1] or "").strip().lower() == "x")
    s_cur, s_calc = (wsum.wal_current.get(p), wsum.wal_calculated.get(p)) if p in wsum.index else (np.nan, np.nan)
    rows.append(dict(product=p, finals=n_x, cur_xlsx=pk.get("CURRENT", (None,))[0], cur_15=s_cur,
                     calc_xlsx=pk.get("CALCULATED", (None,))[0], calc_15=s_calc))
C = pd.DataFrame(rows)
_d = lambda a, b: (pd.to_numeric(C[a]) - pd.to_numeric(C[b])).abs()
C["ok"] = (C.finals == 1) & ((_d("cur_xlsx", "cur_15") < 1e-9) | (C.cur_xlsx.isna() & C.cur_15.isna())) \
          & ((_d("calc_xlsx", "calc_15") < 1e-9) | (C.calc_xlsx.isna() & C.calc_15.isna()))
if C.ok.all():
    print(f"All {len(C)} product tabs: exactly one final x, and Current / Calculated match 15's wal_summary (max diff "
          f"{max(_d('cur_xlsx', 'cur_15').max(), _d('calc_xlsx', 'calc_15').max()):.1e}).")
else:
    alert("warn", "WAL review workbook does not tie to 15", [f"{r.product}: finals {r.finals}, current {r.cur_xlsx} vs {r.cur_15}, calculated {r.calc_xlsx} vs {r.calc_15}"
                                                            for r in C[~C.ok].itertuples()])
display(C[~C.ok] if not C.ok.all() else C.head(5))