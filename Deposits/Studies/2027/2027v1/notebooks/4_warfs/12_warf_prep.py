# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 12 | WARF prep: month-end WARF data mapped to the study products
# MAGIC
# MAGIC **What this builds.** `mv_warf_monthly`: one row per month x bank x counterparty type x FSE flag x LCR rollup x study product, with balance, WARF balance, runoff and rate x balance.
# MAGIC
# MAGIC | Step | What it does |
# MAGIC |---|---|
# MAGIC | 1 | Reads the WARF source (`finance.staging.depositsplusLCR`), month-end rows only (`eobm_flag = 'Y'`), from `start_date` |
# MAGIC | 2 | Maps every account to the study's subproduct and product, month by month: PNFP through `notebooks/sql/pnfp_src.sql`, SNV through `src_lsnv_nmds`, then the study's `current/accounts.csv` and `mv_product_selection` |
# MAGIC | 3 | Months after the study sources end (or an account missing that month) take the account's latest known mapping (`mapping_basis`) |
# MAGIC | 4 | Sums to `mv_warf_monthly` and checks the totals tie to the source |
# MAGIC
# MAGIC **WARF** = sum(runoff_amt) / sum(warf_balance) (as in the WARF and Counterparty Enrichment notebook). **FSE** = `fse_flag = 'Y'`.
# MAGIC
# MAGIC **Settings:** `config/study_params.json` -> `warf` (`source_table`, `start_date`). **Run after:** 04 (needs `mv_product_selection`). **Next:** 13_warf_results.

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ../00_setup

# COMMAND ----------

# DBTITLE 1,1. Settings
from pyspark.sql import functions as F

WCFG = S.params.get("warf", {}) or {}
WARF_SRC = (WCFG.get("source_table") or "finance.staging.depositsplusLCR").strip()
WARF_START = (WCFG.get("start_date") or "2024-10-01").strip()
WARF_END = (WCFG.get("end_date") or "").strip() or str(spark.sql(f"SELECT MAX(asofdate) FROM {WARF_SRC} WHERE eobm_flag = 'Y'").first()[0])
INCL_INT = (str(S.params.get("pnfp_include_internal", "N")).strip().upper() or "N")[0]
T_MONTHLY = S.table("mv_warf_monthly")
assert S.table_exists("mv_product_selection"), "run 04_product_assignment first (mv_product_selection is missing)"
print(f"WARF source: {WARF_SRC} | {WARF_START} to {WARF_END} | month-end rows (eobm_flag = 'Y') | PNFP internal included: {INCL_INT}")
print(f"Writes: {T_MONTHLY}")

# COMMAND ----------

# DBTITLE 1,2. Account to study product, by month
# PNFP: the study's own rule (same file 01 and 10 read). SNV: subproduct straight from the source, BDSP and excluded lists from the params.
sql((STUDY_ROOT / "notebooks" / "sql" / "pnfp_src.sql").read_text(encoding="utf-8").replace("__INCLUDE_INTERNAL__", INCL_INT))
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW warf_src_accounts AS
SELECT 'PNFP' AS legacy_bank, LAST_DAY(eopdate) AS month, CAST(accountid AS STRING) AS accountid, subproductid,
       pf_flag, 'N' AS bdsp_flag
FROM pnfp_src WHERE in_scope AND eopdate >= '{WARF_START}'
UNION ALL
SELECT 'SNV', LAST_DAY(EOPDate), CAST(AccountID AS STRING), SubProductID, '*',
       CASE WHEN CAST(AccountID AS STRING) IN ({_in(BDSP)}) THEN 'Y' ELSE 'N' END
FROM {S.params['sources']['src_lsnv_nmds']}
WHERE EOPDate >= '{WARF_START}' AND SubProductID NOT IN ({_in(EXCL)})
""")

# the study's mapping decision (join_key = bank|subproduct|pf|bdsp, SNV pf = *) -> product, then sub_key -> mv_product
cur = read_decision("current", "accounts")
to_view(cur[["join_key", "product", "product_group"]].astype(str), "warf_key_map")
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW warf_acct_month AS
WITH a AS (
  SELECT s.*, CONCAT_WS('|', legacy_bank, subproductid, pf_flag, bdsp_flag) AS join_key,
         ROW_NUMBER() OVER (PARTITION BY legacy_bank, accountid, month ORDER BY subproductid) AS rn
  FROM warf_src_accounts s
),
k AS (
  SELECT a.legacy_bank, a.month, a.accountid, a.subproductid, NULLIF(k.product_group, '') AS product_group,
         CONCAT_WS('|', a.legacy_bank, a.subproductid, COALESCE(NULLIF(k.product, ''), 'UNMAPPED')) AS sub_key
  FROM a LEFT JOIN warf_key_map k ON k.join_key = a.join_key
  WHERE a.rn = 1
)
SELECT k.*, COALESCE(sel.mv_product, 'UNMAPPED') AS mv_product
FROM k LEFT JOIN {S.table('mv_product_selection')} sel ON sel.sub_key = k.sub_key
""")
spark.sql("""
CREATE OR REPLACE TEMP VIEW warf_acct_last AS
SELECT * FROM (SELECT m.*, ROW_NUMBER() OVER (PARTITION BY legacy_bank, accountid ORDER BY month DESC) AS rn FROM warf_acct_month m)
WHERE rn = 1
""")
display(spark.sql("""
SELECT legacy_bank, MIN(month) AS first_month, MAX(month) AS last_month, COUNT(DISTINCT month) AS months,
       COUNT(DISTINCT accountid) AS accounts, COUNT(DISTINCT mv_product) AS products
FROM warf_acct_month GROUP BY 1 ORDER BY 1
"""))

# COMMAND ----------

# DBTITLE 1,3. mv_warf_monthly
spark.sql(f"""
CREATE OR REPLACE TABLE {T_MONTHLY} AS
WITH d AS (
  SELECT LAST_DAY(asofdate) AS month, asofdate, legacy_bank, CAST(accountid AS STRING) AS accountid,
         COALESCE(counterparty_type, '(blank)') AS counterparty_type, COALESCE(fse_flag, 'N') AS fse_flag,
         COALESCE(nmd_time_flag, '(blank)') AS nmd_time_flag, COALESCE(lcr_rollup_1, '(no rollup)') AS lcr_rollup_1,
         CAST(ledger_balance AS DOUBLE) AS ledger_balance, CAST(warf_balance AS DOUBLE) AS warf_balance,
         CAST(runoff_amt AS DOUBLE) AS runoff_amt, CAST(war_amt AS DOUBLE) AS war_amt,
         CASE WHEN op_deposit_flag = 'Y' THEN CAST(ledger_balance AS DOUBLE) ELSE 0 END AS op_deposit_balance
  FROM {WARF_SRC}
  WHERE eobm_flag = 'Y' AND asofdate >= '{WARF_START}' AND asofdate <= '{WARF_END}'
),
j AS (
  SELECT d.*,
         COALESCE(m.mv_product, l.mv_product, '(not in study)')       AS mv_product,
         COALESCE(m.product_group, l.product_group, '(not in study)') AS product_group,
         CASE WHEN m.accountid IS NOT NULL THEN 'same month'
              WHEN l.accountid IS NOT NULL THEN 'latest known month'
              ELSE 'not in study sources' END                      AS mapping_basis
  FROM d
  LEFT JOIN warf_acct_month m ON m.legacy_bank = d.legacy_bank AND m.accountid = d.accountid AND m.month = d.month
  LEFT JOIN warf_acct_last l ON l.legacy_bank = d.legacy_bank AND l.accountid = d.accountid AND m.accountid IS NULL
)
SELECT month, MAX(asofdate) AS asofdate, legacy_bank, counterparty_type, fse_flag, nmd_time_flag, lcr_rollup_1,
       mv_product, product_group, mapping_basis,
       COUNT(*) AS accounts, SUM(ledger_balance) AS ledger_balance, SUM(warf_balance) AS warf_balance,
       SUM(runoff_amt) AS runoff_amt, SUM(war_amt) AS war_amt, SUM(op_deposit_balance) AS op_deposit_balance
FROM j
GROUP BY month, legacy_bank, counterparty_type, fse_flag, nmd_time_flag, lcr_rollup_1, mv_product, product_group, mapping_basis
""")

# the monthly table must tie to the source, bank by bank and month by month
chk = spark.sql(f"""
WITH s AS (SELECT LAST_DAY(asofdate) AS month, legacy_bank, COUNT(*) AS src_accounts, SUM(CAST(ledger_balance AS DOUBLE)) AS src_balance,
                  SUM(CAST(runoff_amt AS DOUBLE)) AS src_runoff
           FROM {WARF_SRC} WHERE eobm_flag = 'Y' AND asofdate >= '{WARF_START}' AND asofdate <= '{WARF_END}' GROUP BY 1, 2),
     t AS (SELECT month, legacy_bank, SUM(accounts) AS accounts, SUM(ledger_balance) AS balance, SUM(runoff_amt) AS runoff
           FROM {T_MONTHLY} GROUP BY 1, 2)
SELECT s.*, t.accounts, t.balance, t.runoff FROM s FULL JOIN t USING (month, legacy_bank)
""").toPandas()
bad = chk[(chk.src_accounts != chk.accounts) | ((chk.src_balance - chk.balance).abs() > 1) | ((chk.src_runoff - chk.runoff).abs() > 1)]
assert bad.empty, f"mv_warf_monthly does not tie to {WARF_SRC}:\n{bad}"
print(f"{T_MONTHLY}: {len(chk)} bank-months tie to the source (accounts, balance, runoff)")

# COMMAND ----------

# DBTITLE 1,4. Coverage: how each month was mapped
cov = spark.sql(f"""
SELECT month, legacy_bank, mapping_basis, SUM(accounts) AS accounts, SUM(ledger_balance) AS balance
FROM {T_MONTHLY} GROUP BY ALL
""").toPandas()
cov["share"] = cov.balance / cov.groupby(["month", "legacy_bank"]).balance.transform("sum")
piv = cov.pivot_table(index=["month", "legacy_bank"], columns="mapping_basis", values="share", aggfunc="sum").fillna(0).round(3)
display(piv.reset_index())
ops = spark.sql(f"SELECT month, legacy_bank, SUM(op_deposit_balance) AS op_deposit_balance FROM {T_MONTHLY} GROUP BY ALL").toPandas()
ops = ops.pivot_table(index="month", columns="legacy_bank", values="op_deposit_balance", aggfunc="sum").fillna(0)
missing = [f"{pd.Timestamp(m):%Y-%m}: " + ", ".join(b for b in ops.columns if ops.loc[m, b] <= 0) for m in ops.index if (ops.loc[m] <= 0).any()]
if missing:
    alert("warn", "Months without operational deposits loaded (op_deposit_flag = Y) for a bank",
          missing + ["WARF is understated in these months (everything falls into the retail rollups). 13 drops them when warf.ops_months_only = Y."])
print("'not in study sources' = accounts the study does not cover (for example time deposits, closed or excluded subproducts). "
      "They stay in the counterparty and bank WARFs, and show as '(not in study)' in the product views.")

# COMMAND ----------

