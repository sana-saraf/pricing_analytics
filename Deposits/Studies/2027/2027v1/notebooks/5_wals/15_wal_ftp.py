# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 15 | WALs and FTP: shared account history, metric view, product WALs
# MAGIC
# MAGIC **What it does**
# MAGIC 1. **Shared history** (`wal.history_table` in `config/study_params.json`, not versioned): one row per account and month from the Empyrean WAL file (`wal.source_table`) with the FTP columns of `sources.src_emp_ftp_results` for the same SecID and month. Only months that are new, re-delivered (newer `load_timestamp`) or listed in `wal.reload_months` are loaded. Every study reuses it.
# MAGIC 2. **SecID -> account:** SNV `SNV_nmd_<accountid>` -> `<accountid>`; PNFP `<account_type>_<account_number>` -> `<account_type><account_number>` (the study's `accountid`); HSA keeps its SecID (the study holds HSA as one rolled-up account, so HSA shows as unmatched).
# MAGIC 3. **Metric view** `mv_wal_<tag>`: the history joined to this study's **snapshot** product map (`mv_product_map`), so every WAL month uses the snapshot products, like the betas. WAL and FTP rates are weighted by average balance.
# MAGIC 4. **Product WALs:** **Current** = the simple average of the `wal.current_months` WALs; **Calculated** = the `wal.calculated_month` WAL.
# MAGIC
# MAGIC **Outputs:** `outputs/excel/wal_<tag>.xlsx`, `outputs/excel/wal_summary_<tag>.csv` (read by 09 for the Assumptions Summary WAL columns), a copy of the workbook in `Deposit Study Artifacts/Detailed Reports/WALs/`.
# MAGIC
# MAGIC **Run after:** 05_product_betas (needs `mv_product_map`).

# COMMAND ----------

# DBTITLE 1,Install openpyxl
# MAGIC %pip install openpyxl --quiet

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ../00_setup

# COMMAND ----------

# DBTITLE 1,1. Settings and the months to load
import shutil

# settings: config/study_params.json -> wal (no table names or dates typed here)
WCFG = S.params.get("wal", {}) or {}
_ls = lambda s: [x.strip() for x in str(s or "").split(",") if x.strip()]
WAL_SRC = WCFG["source_table"]
HIST = WCFG["history_table"]                          # shared, not versioned
FTP_SRC = S.params["sources"]["src_emp_ftp_results"]
SCEN = str(WCFG.get("scenario", "0")).strip() or "0"
CUR_M = _ls(WCFG.get("current_months"))
CALC_M = str(WCFG.get("calculated_month") or "").strip()
RELOAD = set(_ls(WCFG.get("reload_months")))
assert CUR_M and CALC_M, "set wal.current_months and wal.calculated_month in config/study_params.json"
T_MV = S.table("mv_wal")

# months in the WAL file vs the history: load new months, re-delivered months (newer load_timestamp) and wal.reload_months
src_m = spark.sql(f"""SELECT CAST(StartDate AS STRING) AS m, MAX(load_timestamp) AS ts, COUNT(*) AS n
                      FROM {WAL_SRC} WHERE ScenarioNum = '{SCEN}' GROUP BY 1 ORDER BY 1""").toPandas()
HIST_EXISTS = spark.catalog.tableExists(HIST)
hist_m = (spark.sql(f"SELECT CAST(wal_month AS STRING) AS m, MAX(src_load_timestamp) AS ts FROM {HIST} GROUP BY 1").toPandas()
          if HIST_EXISTS else pd.DataFrame(columns=["m", "ts"]))
_h = dict(zip(hist_m.m, hist_m.ts))
LOAD = sorted(m for m, ts in zip(src_m.m, src_m.ts)
              if m not in _h or m in RELOAD or (pd.notna(ts) and pd.notna(_h[m]) and pd.Timestamp(ts) > pd.Timestamp(_h[m])))
missing = [m for m in CUR_M + [CALC_M] if m not in set(src_m.m)]
if missing:
    alert("warn", f"{WAL_SRC} has no WAL for {', '.join(missing)}", ["Those WAL columns stay blank until the month is delivered."])
print(f"WAL file {WAL_SRC} (scenario {SCEN}): {', '.join(src_m.m)}")
print(f"History {HIST}: {'exists, ' + ', '.join(sorted(_h)) if HIST_EXISTS else 'not made yet'}")
print(f"Months to load: {', '.join(LOAD) or 'none (history is up to date)'}")
print(f"Current = average of {', '.join(CUR_M)} | Calculated = {CALC_M} | metric view {T_MV}")

# COMMAND ----------

# DBTITLE 1,2. Load the shared history (only new or re-delivered months)
# One row per account and month. SecID -> accountid: SNV strips 'SNV_nmd_', PNFP drops the '_' (D_800107233660 -> D800107233660),
# HSA keeps its SecID. FTP columns come from the FTP results for the same SecID and month (left join, one row per SecID).
def _select(months):
    ms = ", ".join(f"'{m}'" for m in months)
    return f"""
WITH w AS (
  SELECT * FROM {WAL_SRC}
  WHERE ScenarioNum = '{SCEN}' AND CAST(StartDate AS STRING) IN ({ms}) AND ImportSource IN ('SNV_NMD', 'BNC_DDA', '_HSA')
),
f AS (
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY EOPDATE, SecID ORDER BY ABS(COALESCE(CurrBook, 0)) DESC) AS rn
    FROM {FTP_SRC} WHERE EOPDATE IN ({ms}) AND ImportSource IN ('SNV_NMD', 'BNC_DDA', '_HSA')
  ) WHERE rn = 1
)
SELECT
  w.StartDate                                                             AS wal_month,
  CASE WHEN w.ImportSource = 'SNV_NMD' THEN 'SNV' ELSE 'PNFP' END         AS legacy_bank,
  CASE WHEN w.ImportSource = 'SNV_NMD' THEN REGEXP_REPLACE(w.SecID, '^SNV_nmd_', '')
       WHEN w.ImportSource = 'BNC_DDA' THEN REPLACE(w.SecID, '_', '')
       ELSE w.SecID END                                                   AS accountid,
  w.SecID                                                                 AS sec_id,
  w.ImportSource                                                          AS import_source,
  w.AccountNum                                                            AS emp_accountnum,
  w.GLAccount                                                             AS gl_account,
  w.CostCenter                                                            AS cost_center,
  CAST(w.ScenarioNum AS STRING)                                           AS scenario,
  w.AvgBook                                                               AS avgbook,
  w.EOPBook                                                               AS eopbook,
  w.EOPRate                                                               AS eop_rate,
  w.WAL                                                                   AS wal,
  w.Duration                                                              AS duration,
  f.SecID IS NOT NULL                                                     AS ftp_matched,
  f.CurrBook                                                              AS ftp_currbook,
  f.CurrCoupon                                                            AS ftp_curr_coupon,
  f.XferRate                                                              AS xfer_rate,
  f.AvgXferRate                                                           AS avg_xfer_rate,
  f.XferSpread                                                            AS xfer_spread,
  f.XferRateBase                                                          AS xfer_rate_base,
  f.XferRateTLP                                                           AS xfer_rate_tlp,
  f.XferRateOther                                                         AS xfer_rate_other,
  f.LIQ_PREM                                                              AS liq_prem,
  f.ICS_CHARGE                                                            AS ics_charge,
  f.COLL_CHARGE                                                           AS coll_charge,
  f.NONACCR_CHARGE                                                        AS nonaccr_charge,
  f.CONVEXITY_CHARGE                                                      AS convexity_charge,
  f.NMD_BK_Charge                                                         AS nmd_bk_charge,
  f.FTPBeta                                                               AS ftp_beta,
  f.StableBalance                                                         AS stable_balance,
  f.NMDMgtOverlay                                                         AS nmd_mgt_overlay,
  f.DepProductMn                                                          AS dep_product_mn,
  f.ALMClassCode                                                          AS alm_class_code,
  f.FTPModel                                                              AS ftp_model,
  w.source_file                                                           AS src_file,
  w.load_timestamp                                                        AS src_load_timestamp,
  CURRENT_TIMESTAMP()                                                     AS loaded_at
FROM w
LEFT JOIN f ON f.SecID = w.SecID AND f.EOPDATE = CAST(w.StartDate AS STRING)
"""

if not HIST_EXISTS:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {HIST} CLUSTER BY (wal_month, legacy_bank) AS {_select(['1900-01-31'])} LIMIT 0")
    spark.sql(f"COMMENT ON TABLE {HIST} IS 'Shared NMD WAL + FTP history, one row per account and month. Built by the deposit studies notebook 15_wal_ftp from the Empyrean WAL file and the FTP results (same SecID and month).'")
for m in LOAD:                                          # one month at a time: REPLACE WHERE keeps every other month as it is
    spark.sql(f"INSERT INTO {HIST} REPLACE WHERE wal_month = DATE'{m}' {_select([m])}")
    print(f"loaded {m}")

hist = spark.sql(f"""
SELECT CAST(wal_month AS STRING) AS wal_month, legacy_bank, import_source, COUNT(*) AS accounts, COUNT(DISTINCT sec_id) AS secids,
       SUM(eopbook) AS eopbook, SUM(avgbook) AS avgbook, AVG(CAST(ftp_matched AS INT)) AS ftp_match_rate,
       TRY_DIVIDE(SUM(wal * avgbook), SUM(CASE WHEN wal IS NOT NULL THEN avgbook END)) AS wal_avgbal
FROM {HIST} GROUP BY ALL ORDER BY 1, 2, 3""").toPandas()
assert (hist.accounts == hist.secids).all(), "history has more than one row for a SecID and month"
display(hist)

# COMMAND ----------

# DBTITLE 1,3. Check: the history ties to the WAL file
chk = spark.sql(f"""
WITH s AS (SELECT CAST(StartDate AS STRING) AS wal_month, ImportSource AS import_source, COUNT(*) AS src_rows, SUM(EOPBook) AS src_eopbook
           FROM {WAL_SRC} WHERE ScenarioNum = '{SCEN}' AND ImportSource IN ('SNV_NMD', 'BNC_DDA', '_HSA') GROUP BY 1, 2),
     h AS (SELECT CAST(wal_month AS STRING) AS wal_month, import_source, COUNT(*) AS hist_rows, SUM(eopbook) AS hist_eopbook FROM {HIST} GROUP BY 1, 2)
SELECT * FROM s FULL JOIN h USING (wal_month, import_source) ORDER BY 1, 2""").toPandas()
chk["ok"] = (chk.src_rows == chk.hist_rows) & ((chk.src_eopbook.astype(float) - chk.hist_eopbook.astype(float)).abs() < 1)
display(chk)
if chk.ok.all():
    print("History ties to the WAL file: rows and end-of-period balance match for every month and source.")
else:
    alert("warn", "History does not tie to the WAL file", [f"{r.wal_month} {r.import_source}: {r.src_rows} vs {r.hist_rows} rows" for r in chk[~chk.ok].itertuples()]
          + ["Set wal.reload_months to reload those months."])

# COMMAND ----------

# DBTITLE 1,4. Metric view mv_wal (history x snapshot products)
assert S.table_exists("mv_product_map"), "run 05_product_betas first"
# every WAL month is mapped with the SNAPSHOT product map (accounts keep their snapshot product), like the betas
MV_YAML = f"""
version: 1.1
comment: "{S.tag} WAL and FTP by product and month: shared history {HIST} joined to this study's snapshot products ({S.table('mv_product_map')}). Rates and WAL weighted by average balance."
source: |-
  SELECT h.*, m.mv_product, m.product_group, m.product AS current_product, m.sub_key, m.mv_product IS NOT NULL AS matched
  FROM {HIST} h
  LEFT JOIN (SELECT legacy_bank, CAST(accountid AS STRING) AS accountid, mv_product, product_group, product, sub_key
             FROM {S.table('mv_product_map')} WHERE eopdate = '{S.snapshot_date}') m
    ON m.legacy_bank = h.legacy_bank AND m.accountid = h.accountid
  WHERE h.scenario = '{SCEN}'
dimensions:
  - name: wal_month
    expr: wal_month
  - name: legacy_bank
    expr: legacy_bank
  - name: mv_product
    expr: COALESCE(mv_product, 'UNMATCHED')
    comment: "Study product at the snapshot; UNMATCHED = WAL account not in the study (internal, closed, HSA roll-up)"
  - name: product_group
    expr: COALESCE(product_group, 'UNMATCHED')
  - name: current_product
    expr: current_product
  - name: sub_key
    expr: sub_key
  - name: import_source
    expr: import_source
  - name: matched
    expr: matched
  - name: ftp_matched
    expr: ftp_matched
measures:
  - name: accounts
    expr: COUNT(1)
  - name: avg_balance
    expr: SUM(avgbook)
  - name: eop_balance
    expr: SUM(eopbook)
  - name: wal
    expr: TRY_DIVIDE(SUM(wal * avgbook), SUM(avgbook) FILTER (WHERE wal IS NOT NULL))
    comment: "WAL (months) weighted by average balance"
  - name: wal_eop_wtd
    expr: TRY_DIVIDE(SUM(wal * eopbook), SUM(eopbook) FILTER (WHERE wal IS NOT NULL))
    comment: "WAL (months) weighted by end-of-period balance"
  - name: eop_rate
    expr: TRY_DIVIDE(SUM(eop_rate * avgbook), SUM(avgbook) FILTER (WHERE eop_rate IS NOT NULL))
  - name: xfer_rate
    expr: TRY_DIVIDE(SUM(xfer_rate * avgbook), SUM(avgbook) FILTER (WHERE xfer_rate IS NOT NULL))
  - name: xfer_spread
    expr: TRY_DIVIDE(SUM(xfer_spread * avgbook), SUM(avgbook) FILTER (WHERE xfer_spread IS NOT NULL))
  - name: xfer_rate_base
    expr: TRY_DIVIDE(SUM(xfer_rate_base * avgbook), SUM(avgbook) FILTER (WHERE xfer_rate_base IS NOT NULL))
  - name: xfer_rate_tlp
    expr: TRY_DIVIDE(SUM(xfer_rate_tlp * avgbook), SUM(avgbook) FILTER (WHERE xfer_rate_tlp IS NOT NULL))
  - name: xfer_rate_other
    expr: TRY_DIVIDE(SUM(xfer_rate_other * avgbook), SUM(avgbook) FILTER (WHERE xfer_rate_other IS NOT NULL))
  - name: liq_prem
    expr: TRY_DIVIDE(SUM(liq_prem * avgbook), SUM(avgbook) FILTER (WHERE liq_prem IS NOT NULL))
  - name: ftp_beta
    expr: TRY_DIVIDE(SUM(ftp_beta * avgbook), SUM(avgbook) FILTER (WHERE ftp_beta IS NOT NULL))
  - name: ftp_match_share
    expr: TRY_DIVIDE(SUM(avgbook) FILTER (WHERE ftp_matched), SUM(avgbook))
    comment: "Share of average balance with FTP results for the same SecID and month"
"""
spark.sql(f"CREATE OR REPLACE VIEW {T_MV} WITH METRICS LANGUAGE YAML AS $$ {MV_YAML} $$")
print(f"Metric view {T_MV}")

# COMMAND ----------

# DBTITLE 1,5. Product WALs: Current (average of the current months) and Calculated
MEAS = ["accounts", "avg_balance", "eop_balance", "wal", "wal_eop_wtd", "eop_rate", "xfer_rate", "xfer_spread", "xfer_rate_base",
        "xfer_rate_tlp", "xfer_rate_other", "liq_prem", "ftp_beta", "ftp_match_share"]
_mq = lambda dims, where="": spark.sql(
    f"SELECT {', '.join(dims)}, {', '.join(f'MEASURE({m}) AS {m}' for m in MEAS)} FROM {T_MV} {where} GROUP BY ALL ORDER BY {', '.join(str(i + 1) for i in range(len(dims)))}").toPandas()
monthly = _mq(["CAST(wal_month AS STRING) AS wal_month", "mv_product"])
by_bank = _mq(["CAST(wal_month AS STRING) AS wal_month", "legacy_bank", "matched"])
by_sub = _mq(["CAST(wal_month AS STRING) AS wal_month", "mv_product", "legacy_bank", "sub_key"])
for d in (monthly, by_bank, by_sub):
    for c in MEAS:
        d[c] = pd.to_numeric(d[c], errors="coerce")

piv = monthly.pivot_table(index="mv_product", columns="wal_month", values="wal", aggfunc="first")
cur_cols = [m for m in CUR_M if m in piv.columns]
summary = pd.DataFrame(index=piv.index)
summary["wal_current"] = piv[cur_cols].mean(axis=1, skipna=False) if cur_cols else np.nan     # both months needed
summary["wal_calculated"] = piv[CALC_M] if CALC_M in piv.columns else np.nan
for m in piv.columns:
    summary[f"wal_{m}"] = piv[m]
calc = monthly[monthly.wal_month == CALC_M].set_index("mv_product")
for c in ["accounts", "avg_balance", "eop_balance", "xfer_rate", "xfer_spread", "xfer_rate_base", "xfer_rate_tlp", "liq_prem", "ftp_beta", "ftp_match_share"]:
    summary[f"{c}_calc_month"] = calc[c] if len(calc) else np.nan
summary = summary.reset_index().sort_values("avg_balance_calc_month", ascending=False, ignore_index=True)
summary["current_months"] = ", ".join(CUR_M)
summary["calculated_month"] = CALC_M

# unmatched balance (WAL accounts with no study product) by month and bank
um = by_bank[~by_bank.matched.astype(bool)]
if len(um):
    log_issues("15 WALs", [("WAL_ACCOUNTS_NOT_IN_STUDY", "INFO", f"{r.wal_month} {r.legacy_bank}",
                            f"{int(r.accounts):,} WAL accounts not in the study's snapshot accounts (internal, closed, HSA roll-up)", "", float(r.avg_balance))
                           for r in um.itertuples()])
print(f"{len(summary)} products | Current = average of {', '.join(cur_cols) or 'none'} | Calculated = {CALC_M if CALC_M in piv.columns else 'not loaded'}")
display(summary[["mv_product", "wal_current", "wal_calculated"] + [f"wal_{m}" for m in piv.columns] + ["avg_balance_calc_month", "xfer_rate_calc_month", "ftp_match_share_calc_month"]])

# COMMAND ----------

# DBTITLE 1,6. Check: metric view WAL = WAL computed straight from the history
# independent recompute (plain SQL on the history + product map, no metric view) for the calculated month
direct = spark.sql(f"""
SELECT COALESCE(m.mv_product, 'UNMATCHED') AS mv_product,
       SUM(h.wal * h.avgbook) / SUM(CASE WHEN h.wal IS NOT NULL THEN h.avgbook END) AS wal_direct, SUM(h.avgbook) AS avg_balance_direct
FROM {HIST} h
LEFT JOIN (SELECT legacy_bank, CAST(accountid AS STRING) AS accountid, mv_product FROM {S.table('mv_product_map')} WHERE eopdate = '{S.snapshot_date}') m
  ON m.legacy_bank = h.legacy_bank AND m.accountid = h.accountid
WHERE h.scenario = '{SCEN}' AND h.wal_month = DATE'{CALC_M}'
GROUP BY 1""").toPandas()
cmp = summary[["mv_product", "wal_calculated", "avg_balance_calc_month"]].merge(direct, on="mv_product", how="outer")
cmp["wal_diff"] = cmp.wal_calculated.astype(float) - cmp.wal_direct.astype(float)
cmp["bal_diff"] = cmp.avg_balance_calc_month.astype(float) - cmp.avg_balance_direct.astype(float)
bad = cmp[(cmp.wal_diff.abs() > 1e-9) | (cmp.bal_diff.abs() > 0.01) | cmp.wal_diff.isna() & cmp.wal_direct.notna()]
if len(bad):
    alert("warn", "Metric view and the direct WAL differ", [f"{r.mv_product}: {r.wal_calculated} vs {r.wal_direct}" for r in bad.itertuples()])
else:
    print(f"Metric view WAL = direct WAL for all {len(cmp)} products at {CALC_M} (max diff {cmp.wal_diff.abs().max():.2e}).")

# COMMAND ----------

# DBTITLE 1,7. Write the WAL workbook and the summary 09 reads
SET = pd.DataFrame([("study", S.tag), ("snapshot_date", S.snapshot_date), ("wal source", WAL_SRC), ("ftp source", FTP_SRC), ("history table", HIST),
                    ("metric view", T_MV), ("scenario", SCEN), ("current months (average)", ", ".join(CUR_M)), ("calculated month", CALC_M),
                    ("weighting", "average balance (AvgBook)"), ("product map", "snapshot products for every WAL month"),
                    ("months loaded this run", ", ".join(LOAD) or "none")], columns=["setting", "value"])
WAL_XLSX = out("excel", "wal", "xlsx")
write_xlsx(WAL_XLSX, {"Product WALs": summary, "Monthly by product": monthly, "By subproduct": by_sub, "By bank": by_bank,
                      "History check": chk, "Metric view check": cmp, "Settings": SET})
summary.to_csv(out("excel", "wal_summary", "csv"), index=False)
DET = OUTPUTS / "Deposit Study Artifacts" / "Detailed Reports" / "WALs"
DET.mkdir(parents=True, exist_ok=True)
shutil.copy(WAL_XLSX, DET / Path(WAL_XLSX).name)
print("->", WAL_XLSX)
print("->", out("excel", "wal_summary", "csv"))
print("->", DET / Path(WAL_XLSX).name)