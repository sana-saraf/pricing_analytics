# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 06a | Legacy Assumptions Crosswalk
# MAGIC
# MAGIC Maps legacy betas, truncations and core ratios (WARFs) from the old Empyrean account structure to this study's products.
# MAGIC
# MAGIC | Step | What it does | Result |
# MAGIC |---|---|---|
# MAGIC | 1 | Read `inputs/reference/legacy_assumptions_by_account.csv` | `legacy_accts` temp view |
# MAGIC | 2 | Join to FTP (`emp_ftp_results`) at the snapshot to get individual SecIDs and balances | `ftp_mapped` |
# MAGIC | 3 | Convert FTP SecID → study accountid, join to study product map (`mv_product_map`) | `legacy_by_account` |
# MAGIC | 4 | Balance-weight legacy assumptions up to new product level | `legacy_by_product` table + CSV |
# MAGIC
# MAGIC **SecID → accountid:** PNFP `D_<acct_type><acct_number>` → strip first `_` → `<acct_type><acct_number>`; SNV `SNV_nmd_<AccountID>` → strip prefix → `<AccountID>`.
# MAGIC
# MAGIC **Outputs:** `outputs/reports/legacy_assumptions_by_product_<tag>.csv`, `legacy_assumptions` sandbox table.
# MAGIC
# MAGIC **Run after:** 05_product_betas (needs `mv_product_map`).

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ../00_setup

# COMMAND ----------

# DBTITLE 1,1. Read legacy assumptions by account
LEGACY_CSV = INPUTS / "reference" / "legacy_assumptions_by_account.csv"
if not LEGACY_CSV.exists():
    print(f"⚠ {LEGACY_CSV.name} not found in inputs/reference/.")
    print("  This file contains the legacy (Empyrean) betas, truncations, and core ratios by account.")
    print("  Without it, 06b/12b/16 review workbooks will not have a LEGACY row.")
    print("  Place the file and re-run, or skip this notebook.")
    dbutils.notebook.exit("SKIPPED — legacy_assumptions_by_account.csv not found")
legacy_raw = pd.read_csv(LEGACY_CSV, dtype=str, keep_default_na=False)
legacy_raw = legacy_raw[legacy_raw.AccountNum.str.strip() != ""]

# Cast numeric columns
for c in ["LegacyBeta", "LegacyTrunc", "LegacyCoreRatio"]:
    legacy_raw[c] = pd.to_numeric(legacy_raw[c], errors="coerce")

# Only accounts with at least one assumption
has_assumption = legacy_raw[legacy_raw[["LegacyBeta", "LegacyTrunc", "LegacyCoreRatio"]].notna().any(axis=1)].copy()
print(f"Legacy assumptions: {len(has_assumption)} accounts with values out of {len(legacy_raw)} total")
print(f"  PNFP: {len(has_assumption[has_assumption.legacy_bank == 'PNFP'])} | SNV: {len(has_assumption[has_assumption.legacy_bank == 'SNV'])}")
to_view(has_assumption, "legacy_accts")
display(has_assumption)

# COMMAND ----------

# DBTITLE 1,2. Join to FTP at the snapshot — get individual SecIDs + balances
FTP_TABLE = S.params.get("sources", {}).get("src_emp_ftp_results", "finance.ftp_validation.emp_ftp_results")

sql(f"""
CREATE OR REPLACE TEMP VIEW ftp_mapped AS
SELECT
  la.legacy_bank,
  la.AccountName,
  la.AccountNum,
  la.LegacyBeta,
  la.LegacyTrunc,
  la.LegacyCoreRatio,
  ftp.SecID,
  ftp.CurrBook,
  -- Convert FTP SecID -> study accountid
  CASE
    WHEN ftp.SecID LIKE 'SNV_nmd_%' THEN REPLACE(ftp.SecID, 'SNV_nmd_', '')
    WHEN ftp.SecID LIKE 'D\\_%'     THEN CONCAT('D', SUBSTRING(ftp.SecID, 3))
    WHEN ftp.SecID LIKE 'S\\_%'     THEN CONCAT('S', SUBSTRING(ftp.SecID, 3))
    WHEN ftp.SecID LIKE 'X\\_%'     THEN CONCAT('X', SUBSTRING(ftp.SecID, 3))
    ELSE ftp.SecID
  END AS accountid
FROM legacy_accts la
INNER JOIN {FTP_TABLE} ftp
  ON CAST(la.AccountNum AS STRING) = ftp.AccountNum
  AND ftp.EOPDATE = '{{{{snapshot_date}}}}'
  AND ftp.CurrBook > 0
""")

ftp_stats = sqlq("""
SELECT legacy_bank, COUNT(DISTINCT AccountNum) AS legacy_accounts,
       COUNT(DISTINCT SecID) AS individual_accounts, SUM(CurrBook) AS total_balance
FROM ftp_mapped GROUP BY legacy_bank ORDER BY legacy_bank
""").toPandas()
print(f"FTP mapped at {{S.snapshot_date}}:")
display(ftp_stats)

# COMMAND ----------

# DBTITLE 1,3. Join to study product map — get new product for each account
sql("""
CREATE OR REPLACE TEMP VIEW legacy_by_account AS
SELECT
  f.legacy_bank,
  f.AccountName   AS legacy_account_name,
  f.AccountNum    AS legacy_account_num,
  f.SecID,
  f.accountid,
  f.CurrBook      AS balance,
  f.LegacyBeta    AS legacy_beta,
  f.LegacyTrunc   AS legacy_trunc,
  f.LegacyCoreRatio AS legacy_core_ratio,
  COALESCE(pm.product, 'UNMAPPED')       AS product,
  COALESCE(pm.product_group, 'UNMAPPED') AS product_group
FROM ftp_mapped f
LEFT JOIN {{mv_product_map}} pm
  ON f.accountid = pm.accountid
  AND f.legacy_bank = pm.legacy_bank
""")

# Coverage check
coverage = sqlq("""
SELECT legacy_bank,
  COUNT(*) AS accounts, SUM(balance) AS balance,
  SUM(CASE WHEN product = 'UNMAPPED' THEN 1 ELSE 0 END) AS unmapped_accounts,
  SUM(CASE WHEN product = 'UNMAPPED' THEN balance ELSE 0 END) AS unmapped_balance
FROM legacy_by_account GROUP BY legacy_bank
""").toPandas()
print("Product map coverage:")
display(coverage)

# Show a sample
print("\nSample: legacy account -> new product")
display(sqlq("""
SELECT legacy_bank, legacy_account_name, legacy_account_num, product, product_group,
       COUNT(*) AS accounts, ROUND(SUM(balance), 0) AS balance
FROM legacy_by_account WHERE product <> 'UNMAPPED'
GROUP BY ALL ORDER BY legacy_bank, balance DESC
""").toPandas())

# COMMAND ----------

# DBTITLE 1,4. Balance-weight legacy assumptions to new products
# product_group can differ by legacy_bank for the same product (e.g. IB DDA IOLTA)
# so we group by product only, then pick the dominant product_group by balance.
sql("""
CREATE OR REPLACE TEMP VIEW _legacy_agg AS
SELECT
  product,
  -- Balance-weighted legacy beta (only accounts that have a beta)
  ROUND(SUM(CASE WHEN legacy_beta IS NOT NULL THEN legacy_beta * balance END)
      / NULLIF(SUM(CASE WHEN legacy_beta IS NOT NULL THEN balance END), 0), 6) AS legacy_beta,
  SUM(CASE WHEN legacy_beta IS NOT NULL THEN balance ELSE 0 END)              AS legacy_beta_balance,
  COUNT(CASE WHEN legacy_beta IS NOT NULL THEN 1 END)                         AS legacy_beta_accounts,
  -- Balance-weighted legacy truncation (only accounts that have a truncation)
  ROUND(SUM(CASE WHEN legacy_trunc IS NOT NULL THEN legacy_trunc * balance END)
      / NULLIF(SUM(CASE WHEN legacy_trunc IS NOT NULL THEN balance END), 0), 0) AS legacy_trunc,
  SUM(CASE WHEN legacy_trunc IS NOT NULL THEN balance ELSE 0 END)               AS legacy_trunc_balance,
  -- Balance-weighted legacy core ratio / WARF
  ROUND(SUM(CASE WHEN legacy_core_ratio IS NOT NULL THEN legacy_core_ratio * balance END)
      / NULLIF(SUM(CASE WHEN legacy_core_ratio IS NOT NULL THEN balance END), 0), 6) AS legacy_core_ratio,
  SUM(CASE WHEN legacy_core_ratio IS NOT NULL THEN balance ELSE 0 END)                AS legacy_core_ratio_balance,
  COUNT(CASE WHEN legacy_core_ratio IS NOT NULL THEN 1 END)                           AS legacy_core_ratio_accounts,
  -- Total balance in product
  SUM(balance) AS total_balance,
  COUNT(*) AS total_accounts
FROM legacy_by_account
WHERE product <> 'UNMAPPED'
GROUP BY product
""")

# Pick the dominant product_group per product (by balance)
sql("""
CREATE OR REPLACE TEMP VIEW _pg_rank AS
SELECT product, product_group,
  ROW_NUMBER() OVER (PARTITION BY product ORDER BY SUM(balance) DESC) AS rn
FROM legacy_by_account WHERE product <> 'UNMAPPED'
GROUP BY product, product_group
""")

sql("""
CREATE OR REPLACE TEMP VIEW legacy_by_product AS
SELECT pg.product_group, a.*
FROM _legacy_agg a
JOIN _pg_rank pg ON a.product = pg.product AND pg.rn = 1
ORDER BY a.total_balance DESC
""")

result = sqlq("SELECT * FROM legacy_by_product").toPandas()
print(f"Legacy assumptions rolled up to {len(result)} products:")
display(result)

# Write outputs
out_csv = S.output("reports", "legacy_assumptions_by_product", "csv")
result.to_csv(out_csv, index=False)
print(f"\nWrote {out_csv}")

# Also write the account-level detail for auditability
detail = sqlq("SELECT * FROM legacy_by_account ORDER BY legacy_bank, legacy_account_num, product").toPandas()
detail_csv = S.output("reports", "legacy_assumptions_by_account_detail", "csv")
detail.to_csv(detail_csv, index=False)
print(f"Wrote {detail_csv}")

# COMMAND ----------

# DBTITLE 1,5. Summary and coverage check
# Re-read from temp view (avoid name collisions with 00_setup helpers)
leg = sqlq("SELECT * FROM legacy_by_product").toPandas()
has_beta = leg[leg.legacy_beta.notna()]
has_trunc = leg[leg.legacy_trunc.notna()]
has_core = leg[leg.legacy_core_ratio.notna()]
print(f"Products with legacy beta:       {len(has_beta)} / {len(leg)}")
print(f"Products with legacy truncation: {len(has_trunc)} / {len(leg)}")
print(f"Products with legacy core ratio: {len(has_core)} / {len(leg)}")

# Check for products in the study that got no legacy assumption at all
all_products = sqlq("SELECT DISTINCT product FROM {{mv_product_map}}").toPandas()["product"].tolist()
leg_products = set(leg["product"].tolist())
missing = [p for p in all_products if p not in leg_products]
if missing:
    print(f"\nProducts with NO legacy assumptions ({len(missing)}):")
    for p in sorted(missing):
        print(f"  {p}")
else:
    print("\nAll products have at least one legacy assumption.")