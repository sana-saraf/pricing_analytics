# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 02 | Account metrics by subproduct
# MAGIC
# MAGIC **What this builds.** For every `sub_key` (bank | subproduct | current product): accounts, balances, WAR and the rate distribution at the snapshot, plus how much history it has. Writes the input file **`inputs/account_metrics_<tag>.xlsx`** and the table `mv_account_metrics`.
# MAGIC
# MAGIC **Reads:** `mv_accounts` and `mv_account_war` built by 01 (from the source tables and `inputs/account_mapping_<tag>.xlsx`).
# MAGIC
# MAGIC **Run after:** 01.

# COMMAND ----------

# DBTITLE 1,Install openpyxl
# MAGIC %pip install -q openpyxl

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ../00_setup

# COMMAND ----------

# DBTITLE 1,Step 1 header
# MAGIC %md
# MAGIC ## Step 1. Inputs from 01
# MAGIC Confirms 01 has run and shows which mapping values the accounts were built with.

# COMMAND ----------

# DBTITLE 1,1. Check the inputs
MAP_FILE = inp("account_mapping")
assert MAP_FILE.exists(), f"{MAP_FILE} not found: run 01_account_mapping first"
for t in ("mv_accounts", "mv_account_war"):
    assert S.table_exists(t), f"{S.table(t)} not built: run 01_account_mapping first"
mapping_used = read_tab(MAP_FILE, "mapping_used")
print(f"Mapping file: {MAP_FILE}")
display(mapping_used.groupby(["legacy_bank", "value_source"], as_index=False).agg(keys=("join_key", "count")))

# COMMAND ----------

# DBTITLE 1,Step 2 header
# MAGIC %md
# MAGIC ## Step 2. Metrics
# MAGIC
# MAGIC | Column | Definition |
# MAGIC |---|---|
# MAGIC | `accounts`, `eopbook`, `avgbook` | count and sums over the snapshot accounts of the `sub_key` |
# MAGIC | `war` | sum(rate x end balance) / sum(end balance) |
# MAGIC | `rate_min` ... `rate_max`, `rate_std` | the distribution of account rates (unweighted; percentiles are approximate) |
# MAGIC | `zero_rate_balance_share` | share of the balance on accounts paying 0 |
# MAGIC | `share_of_product_balance` | the subproduct's share of its current product's balance in that bank |
# MAGIC | `months_history`, `first_month`, `last_month` | months with a positive balance in `mv_account_war` |

# COMMAND ----------

# DBTITLE 1,2. mv_account_metrics
sql("""
CREATE OR REPLACE TABLE {{mv_account_metrics}} AS
WITH h AS (
  SELECT sub_key, COUNT(DISTINCT CASE WHEN eopbook > 0 THEN eopdate END) AS months_history,
         MIN(eopdate) AS first_month, MAX(eopdate) AS last_month
  FROM {{mv_account_war}} GROUP BY sub_key
),
m AS (
  SELECT sub_key, legacy_bank, subproductid, COALESCE(product, 'UNMAPPED') AS product,
         MAX(product_group) AS product_group, MAX(acct_typ_desc) AS acct_typ_desc,
         COUNT(*) AS accounts, SUM(eopbook) AS eopbook, SUM(avgbook) AS avgbook,
         TRY_DIVIDE(SUM(sumprod_eoprate), SUM(eopbook)) AS war,
         MIN(currcoupon) AS rate_min,
         PERCENTILE_APPROX(currcoupon, 0.10) AS rate_p10, PERCENTILE_APPROX(currcoupon, 0.25) AS rate_p25,
         PERCENTILE_APPROX(currcoupon, 0.50) AS rate_p50, PERCENTILE_APPROX(currcoupon, 0.75) AS rate_p75,
         PERCENTILE_APPROX(currcoupon, 0.90) AS rate_p90, MAX(currcoupon) AS rate_max, STDDEV(currcoupon) AS rate_std,
         TRY_DIVIDE(SUM(CASE WHEN currcoupon = 0 THEN eopbook ELSE 0 END), SUM(eopbook)) AS zero_rate_balance_share
  FROM {{mv_accounts}}
  GROUP BY sub_key, legacy_bank, subproductid, COALESCE(product, 'UNMAPPED')
)
SELECT m.*,
       TRY_DIVIDE(m.eopbook, SUM(m.eopbook) OVER (PARTITION BY m.legacy_bank, m.product)) AS share_of_product_balance,
       h.months_history, h.first_month, h.last_month
FROM m LEFT JOIN h ON m.sub_key = h.sub_key
""")
metrics = spark.table(S.table("mv_account_metrics")).orderBy("legacy_bank", "product", F.desc("eopbook")).toPandas()
war_long = spark.table(S.table("mv_account_war")).orderBy("sub_key", "eopdate").toPandas()
tot = spark.table(S.table("mv_accounts")).groupBy("legacy_bank").agg(F.count("*").alias("n"), F.sum("eopbook").alias("b")).toPandas().set_index("legacy_bank")
for bank, g in metrics.groupby("legacy_bank"):
    assert g.accounts.sum() == tot.loc[bank, "n"] and abs(g.eopbook.sum() - tot.loc[bank, "b"]) < 1, f"{bank}: metrics do not tie to mv_accounts"
print(f"{len(metrics):,} sub_keys with a snapshot balance (tie to mv_accounts by bank) | "
      f"{war_long.sub_key.nunique():,} sub_keys with history | {war_long.eopdate.nunique()} months")
display(metrics.head(50))

# COMMAND ----------

# DBTITLE 1,2. Known issues in the metrics
rows = []
thin = metrics[(metrics.months_history < MIN_MONTHS) & (metrics.eopbook > 0)]
if len(thin):
    rows.append(("THIN_HISTORY", "MEDIUM", f"{len(thin)} sub_keys",
                 f"fewer than {MIN_MONTHS} months with balance: {', '.join(thin.sub_key.head(10))}{' ...' if len(thin) > 10 else ''}",
                 int(thin.accounts.sum()), float(thin.eopbook.sum())))
hist_only = sorted(set(war_long.sub_key) - set(metrics.sub_key))
if hist_only:
    rows.append(("HISTORY_ONLY_SUBPRODUCT", "INFO", f"{len(hist_only)} sub_keys",
                 "in the monthly history but no account at the snapshot; they get betas in 03 but carry no snapshot balance", 0, 0.0))
neg = metrics[metrics.eopbook < 0]
if len(neg):
    rows.append(("NEGATIVE_BALANCE", "MEDIUM", f"{len(neg)} sub_keys", ", ".join(neg.sub_key), int(neg.accounts.sum()), float(neg.eopbook.sum())))
issues_metrics = log_issues("02 metrics", rows)



# COMMAND ----------

# DBTITLE 1,Step 3 header
# MAGIC %md
# MAGIC ## Step 3. Write `inputs/account_metrics_<tag>.xlsx`
# MAGIC Tabs: the subproduct metrics, a rollup by bank and current product, and the monthly balance and WAR by `sub_key` (one column per month) that the betas in 03 are fitted on.

# COMMAND ----------

# DBTITLE 1,3. Account metrics workbook
wide = lambda col: war_long.pivot(index="sub_key", columns="eopdate", values=col).sort_index(axis=1).rename(columns=str).reset_index()
rollup = metrics.groupby(["legacy_bank", "product"], as_index=False).agg(
    subproducts=("sub_key", "nunique"), accounts=("accounts", "sum"), eopbook=("eopbook", "sum"))
README_MET = pd.DataFrame({"tab": ["subproduct_metrics", "product_rollup", "monthly_balance", "monthly_war"],
    "what": ["One row per sub_key at the snapshot (Step 2 definitions)", "By bank and current product",
             "End balance by sub_key and month (mv_account_war)", "WAR by sub_key and month (mv_account_war); the beta input"]})
MET_FILE = inp("account_metrics")
write_xlsx(MET_FILE, {"README": README_MET, "subproduct_metrics": metrics, "product_rollup": rollup,
                      "monthly_balance": wide("eopbook"), "monthly_war": wide("war")})
print("->", MET_FILE)
display(rollup)