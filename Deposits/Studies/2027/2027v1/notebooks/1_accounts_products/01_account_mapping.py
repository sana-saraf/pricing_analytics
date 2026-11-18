# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 01 | Account mapping, from the source tables
# MAGIC
# MAGIC **What this builds.** The mapping that says which product every legacy account belongs to, then the account-level DataFrame built from it.
# MAGIC
# MAGIC | Step | What it does | Result |
# MAGIC |---|---|---|
# MAGIC | 1 | Profiles the two production sources at the snapshot | the account universe |
# MAGIC | 2 | PNFP 33-product rule: rate class + product grouping -> `product` and `subproductid` | `pnfp_src` (all months) |
# MAGIC | 3 | SNV: product derivation from `snv_src.sql` (ACCT_TYP_CDE + SubProductID rule) | `snv_src` (all months) |
# MAGIC | 4 | Mapping keys: every bank + subproduct + public-funds flag + BDSP flag that holds balance in any month | `mapping_keys` |
# MAGIC | 5a | **Reference** values: the original `dim_nmd.csv` of the version (plus production `dim_nmd` for comparison) | `map_reference` |
# MAGIC | 5b | **Derived** values: PNFP from the Step 2 rule, SNV from the Step 3 rule | `map_derived` |
# MAGIC | 5c | Picks the values by `mapping_source` in `config/mv_study_config.xlsx`, compares them, writes the input file | **`inputs/account_mapping_<tag>.xlsx`** |
# MAGIC | 6 | Builds the account DataFrame at the snapshot | `mv_accounts` table, `outputs/excel/accounts_<tag>.xlsx` |
# MAGIC | 7 | Monthly history by subproduct (feeds 02 and the betas) | `mv_account_war` table |
# MAGIC
# MAGIC **Choosing which values get printed.** `mapping_source` = `REFERENCE` (the originals), `DERIVED` (your SQL in Step 5b) or `DERIVED_ELSE_REFERENCE` (your value where you filled it, the original elsewhere). The workbook always holds all three side by side.
# MAGIC
# MAGIC **Reads:** production `pnfp_deposits`, `lsnv_nmds`, `dim_nmd`; `inputs/reference/dim_nmd.csv`. Nothing from the version's sandbox tables is used to build the result; they are only used in the checks.
# MAGIC
# MAGIC **Known issues** are shown as soon as each step finds them and logged to `outputs/reports/known_issues_<tag>.csv`.

# COMMAND ----------

# DBTITLE 1,Install openpyxl
# MAGIC %pip install -q openpyxl

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ../00_setup

# COMMAND ----------

# DBTITLE 1,Step 1 header
# MAGIC %md
# MAGIC ## Step 1. The sources at the snapshot
# MAGIC PNFP comes from production `pnfp_deposits` (one row per account per `load_date`). Internal and closed accounts are out of scope, as in the version's `lpnfp_nmds_agg`. SNV comes from production `lsnv_nmds` (one row per account per `EOPDate`); the subproducts listed in `snv_excluded_subproducts` are out of scope, as in the version's `lsnv_nmds_agg`.

# COMMAND ----------

# DBTITLE 1,1. Source profile
source_profile = sqlq("""
SELECT 'PNFP' AS legacy_bank, '{{src_pnfp_deposits}}' AS source_table,
       COUNT(DISTINCT load_date) AS months, MIN(load_date) AS first_month, MAX(load_date) AS last_month,
       SUM(CASE WHEN load_date = '{{snapshot_date}}' THEN 1 ELSE 0 END) AS snapshot_rows,
       SUM(CASE WHEN load_date = '{{snapshot_date}}' AND internal = '1' THEN 1 ELSE 0 END) AS snapshot_internal_rows,
       SUM(CASE WHEN load_date = '{{snapshot_date}}' AND closed = '1' THEN 1 ELSE 0 END) AS snapshot_closed_rows,
       SUM(CASE WHEN load_date = '{{snapshot_date}}' AND internal <> '1' AND closed <> '1' THEN 1 ELSE 0 END) AS snapshot_rows_in_scope,
       SUM(CASE WHEN load_date = '{{snapshot_date}}' AND internal <> '1' AND closed <> '1' THEN ledger ELSE 0 END) AS snapshot_balance_in_scope
FROM {{src_pnfp_deposits}}
UNION ALL
SELECT 'SNV', '{{src_lsnv_nmds}}',
       COUNT(DISTINCT EOPDate), MIN(EOPDate), MAX(EOPDate),
       SUM(CASE WHEN EOPDate = '{{snapshot_date}}' THEN 1 ELSE 0 END),
       0, 0,
       SUM(CASE WHEN EOPDate = '{{snapshot_date}}' AND SubProductID NOT IN (__EXCL__) THEN 1 ELSE 0 END),
       SUM(CASE WHEN EOPDate = '{{snapshot_date}}' AND SubProductID NOT IN (__EXCL__) THEN EOPBook ELSE 0 END)
FROM {{src_lsnv_nmds}}
""".replace("__EXCL__", _in(EXCL))).toPandas()
print(f"Snapshot {S.snapshot_date} | SNV excluded subproducts: {EXCL} | SNV BDSP accounts: {BDSP}")
display(source_profile)

# COMMAND ----------

# DBTITLE 1,Step 2 header
# MAGIC %md
# MAGIC ## Step 2. PNFP: rate class, product and `subproductid` (the 33-product rule)
# MAGIC Every PNFP account gets its product straight from the source:
# MAGIC
# MAGIC * **Rate class** (first match wins): override rate 787 -> `DNC`; override 100 and prime not 100 -> `ZeroRate`; override description WSJ PRIME / Fed Funds Target / Fed Funds Effective / Pinnacle Index or prime rate 657 / 659 -> `Indexed`; service charge AG / EG -> `Standard`; override not 0 and not equal to prime -> `Standard Plus`; else `Standard`.
# MAGIC * **Type, group, segment** from the service charge code in `{{src_product_grouping}}` (ICS = 4G / 3K, IOLTA = GL 400, Brokered = GL 170 / 300, Repo = GL 350, NIB from `Dep_Port_Sale_Perform`, IB DDA Hybrid = 3Y / AG / EG / 2H; segment: internal -> Commercial, public funds -> Public Funds, Business or Repo -> Commercial, else Consumer).
# MAGIC * **Product** = Internal Deposits, ICS <group> <segment>, <group> IOLTA, Brokered DDA / MMA, or <group> <segment> <rate label> (no label for public funds, NIB DDA, Savings, Repo, Hybrid; ZeroRate -> Standard; DNC -> Exception; Standard Plus -> Exception for MMA and IB DDA Consumer).
# MAGIC * **`subproductid`** = service charge code + `_000_00` Standard / ZeroRate, `_000_01` Indexed, `_070_00` Standard Plus, `_081_00` DNC; internal accounts `_INT`.
# MAGIC * **In scope**: open accounts; internal ones only when `pnfp_include_internal = Y` in `config/study_params.json`.
# MAGIC
# MAGIC The cell checks that every key gives one product and that every product is in `inputs/product_lookup.csv`.
# MAGIC
# MAGIC <details><summary>Previous rule (kept for reference)</summary>
# MAGIC
# MAGIC `subproductid = service_charge_code || '_' || suffix`. The suffix comes from the account's override rate number and prime rate number, checked in this order:
# MAGIC
# MAGIC | Rule | Condition | Suffix | Rate class |
# MAGIC |---|---|---|---|
# MAGIC | R1 | override rate in 590, 786, 2, 791, 789 | `000_01` | Indexed |
# MAGIC | R2 | (override rate 0, blank or 100, or prime rate 999) and override rate is not 787 | `000_00` | Standard |
# MAGIC | R3 | prime rate or override rate is 787 (do-not-change) | `082_00` | Exception |
# MAGIC | R4 | anything else | `082_00` | Exception |
# MAGIC
# MAGIC This is the live logic in the version's notebook 01 Step 3 (OPTIONS 1, 2a and 2c switched on). `dnc_flag` = Y for the 787 accounts; it drives the Standard Plus rename in Step 6. `rate_class_rule` is kept on every row so the counts below show what each branch catches.
# MAGIC </details>

# COMMAND ----------

# DBTITLE 1,2. pnfp_src (all months) and the rule counts
# internal accounts: Y = in the study as 'Internal Deposits', N = out of scope (config/study_params.json)
INCLUDE_INTERNAL = "N" if str(S.params.get("pnfp_include_internal") or "Y").strip().upper() == "N" else "Y"
# the rule lives in notebooks/sql/pnfp_src.sql (also read by 10_truncation_cohorts); edit it there
PNFP_SRC_SQL = (STUDY_ROOT / "notebooks" / "sql" / "pnfp_src.sql").read_text(encoding="utf-8")
sql(PNFP_SRC_SQL.replace("__INCLUDE_INTERNAL__", INCLUDE_INTERNAL))
print(f"Internal accounts in scope: {INCLUDE_INTERNAL}")

rule_counts = sqlq("""
SELECT product, rate_class, COUNT(*) AS accounts, SUM(eopbook) AS balance, COUNT(DISTINCT subproductid) AS subproducts
FROM pnfp_src
WHERE eopdate = '{{snapshot_date}}' AND in_scope
GROUP BY ALL ORDER BY product, rate_class
""").toPandas()
display(rule_counts) if len(rule_counts) else print("No PNFP accounts in scope at the snapshot.")

# every in-scope PNFP key must give one product, and every product must be in inputs/product_lookup.csv
PRODUCT_LOOKUP = pd.read_csv(INPUTS / "product_lookup.csv", dtype=str, keep_default_na=False)
to_view(PRODUCT_LOOKUP, "product_lookup")
chk = sqlq("""
SELECT (SELECT COUNT(*) FROM (SELECT subproductid, pf_flag FROM pnfp_src WHERE in_scope GROUP BY 1, 2
                              HAVING COUNT(DISTINCT product) > 1)) AS keys_with_two_products,
       (SELECT CONCAT_WS(', ', COLLECT_SET(product)) FROM pnfp_src s WHERE in_scope
          AND NOT EXISTS (SELECT 1 FROM product_lookup l WHERE l.product_name = s.product)) AS products_not_in_lookup
""").toPandas().iloc[0]
print(f"PNFP products at the snapshot: {rule_counts['product'].nunique()} | keys with two products: {chk.keys_with_two_products} | "
      f"not in product_lookup.csv: {chk.products_not_in_lookup or 'none'}")
assert chk.keys_with_two_products == 0, "a PNFP subproduct key gives two products; fix the rule in this cell"
log_issues("01 pnfp rule", [] if not chk.products_not_in_lookup else [
    ("PRODUCT_NOT_IN_LOOKUP", "HIGH", chk.products_not_in_lookup, "the PNFP rule produces product names missing from inputs/product_lookup.csv", "", "")])

# COMMAND ----------

# DBTITLE 1,Step 3 header
# MAGIC %md
# MAGIC ## Step 3. SNV: product derivation from `snv_src.sql`
# MAGIC SNV product group, client group, rate class and product name are derived from `ACCT_TYP_CDE` and `SubProductID` suffix — the rule lives in `notebooks/sql/snv_src.sql`. Two accounts are BDSP (`snv_bdsp_accounts` in the config). The subproducts in `snv_excluded_subproducts` are dropped.

# COMMAND ----------

# DBTITLE 1,3. snv_src (all months) — rule from notebooks/sql/snv_src.sql
# the rule lives in notebooks/sql/snv_src.sql (also read by 10_truncation_cohorts); edit it there
SNV_SRC_SQL = (STUDY_ROOT / "notebooks" / "sql" / "snv_src.sql").read_text(encoding="utf-8")
sql(SNV_SRC_SQL.replace("__BDSP__", _in(BDSP)).replace("__EXCL__", _in(EXCL)))

rule_counts_snv = sqlq("""
SELECT product, rate_class, COUNT(*) AS accounts, SUM(eopbook) AS balance, COUNT(DISTINCT subproductid) AS subproducts
FROM snv_src
WHERE eopdate = '{{snapshot_date}}' AND in_scope
GROUP BY ALL ORDER BY product, rate_class
""").toPandas()
display(rule_counts_snv) if len(rule_counts_snv) else print("No SNV accounts in scope at the snapshot.")

# every in-scope SNV key must give one product, and every product must be in inputs/product_lookup.csv
chk_snv = sqlq("""
SELECT (SELECT COUNT(*) FROM (SELECT subproductid FROM snv_src WHERE in_scope GROUP BY 1
                              HAVING COUNT(DISTINCT product) > 1)) AS keys_with_two_products,
       (SELECT CONCAT_WS(', ', COLLECT_SET(product)) FROM snv_src s WHERE in_scope
          AND NOT EXISTS (SELECT 1 FROM product_lookup l WHERE l.product_name = s.product)) AS products_not_in_lookup
""").toPandas().iloc[0]
print(f"SNV products at the snapshot: {rule_counts_snv['product'].nunique()} | keys with two products: {chk_snv.keys_with_two_products} | "
      f"not in product_lookup.csv: {chk_snv.products_not_in_lookup or 'none'}")
log_issues("01 snv rule", [] if not chk_snv.products_not_in_lookup else [
    ("PRODUCT_NOT_IN_LOOKUP", "HIGH", chk_snv.products_not_in_lookup, "the SNV rule produces product names missing from inputs/product_lookup.csv", "", "")])

# COMMAND ----------

# DBTITLE 1,Step 4 header
# MAGIC %md
# MAGIC ## Step 4. Mapping keys: everything that needs a mapping row
# MAGIC One row per key the accounts are joined on: PNFP `bank + subproductid + pf_flag` (BDSP is always N), SNV `bank + subproductid + bdsp_flag` (SNV has no public-funds flag in the source; it comes from the mapping). Keys are taken from **every month** so the history used by the betas is fully mapped, not just the snapshot.

# COMMAND ----------

# DBTITLE 1,4. mapping_keys
sql("""
CREATE OR REPLACE TEMP VIEW mapping_keys AS
WITH a AS (
  SELECT 'PNFP' AS legacy_bank, eopdate, acct_typ_code, subproductid, pf_flag, 'N' AS bdsp_flag, eopbook, sumprod_eoprate
  FROM pnfp_src WHERE in_scope
  UNION ALL
  SELECT 'SNV', eopdate, acct_typ_code, subproductid,
         CASE WHEN client_group = 'Public Funds' THEN 'Y' ELSE 'N' END, bdsp_flag, eopbook, sumprod_eoprate
  FROM snv_src WHERE in_scope
)
SELECT legacy_bank, subproductid, pf_flag, bdsp_flag,
       MAX(acct_typ_code)                                        AS acct_typ_code,
       CONCAT_WS(',', SORT_ARRAY(COLLECT_SET(acct_typ_code)))    AS acct_typ_codes_seen,
       COUNT(DISTINCT eopdate)                                   AS months,
       MIN(eopdate)                                              AS first_month,
       MAX(eopdate)                                              AS last_month,
       SUM(CASE WHEN eopdate = '{{snapshot_date}}' THEN 1 ELSE 0 END)       AS snapshot_accounts,
       SUM(CASE WHEN eopdate = '{{snapshot_date}}' THEN eopbook ELSE 0 END) AS snapshot_balance,
       TRY_DIVIDE(SUM(CASE WHEN eopdate = '{{snapshot_date}}' THEN sumprod_eoprate END),
                  SUM(CASE WHEN eopdate = '{{snapshot_date}}' THEN eopbook END)) AS snapshot_war
FROM a
GROUP BY legacy_bank, subproductid, pf_flag, bdsp_flag
""")
keys = spark.table("mapping_keys").toPandas()
print(f"{len(keys):,} mapping keys | {int((keys.snapshot_accounts > 0).sum()):,} hold accounts at the snapshot | "
      f"{int((keys.snapshot_accounts == 0).sum()):,} only in history")
display(keys.groupby("legacy_bank", as_index=False).agg(keys=("subproductid", "count"), snapshot_accounts=("snapshot_accounts", "sum"),
                                                      snapshot_balance=("snapshot_balance", "sum")))

# COMMAND ----------

# DBTITLE 1,Step 5a header
# MAGIC %md
# MAGIC ## Step 5a. Reference values: the originals
# MAGIC `inputs/reference/dim_nmd.csv` is the version's own `dim_nmd.csv`, copied here at export. It is what the version's notebook 01 used. Production `dim_nmd` is loaded too, for comparison only. Duplicate keys in the reference are reported here: the version's notebook 01 joins every account of a duplicated key twice.

# COMMAND ----------

# DBTITLE 1,5a. Reference and production dim_nmd
MAP_VALUES = ["acct_typ_desc", "product_group", "client_group", "rate_class", "product", "product_num", "dim_pf_flag"]

def keyed(df):
    """Adds join_key: PNFP bank|subproduct|pf|bdsp, SNV bank|subproduct|*|bdsp (SNV is not joined on pf_flag)."""
    d = df.copy()
    for c in ["legacy_bank", "subproductid", "pf_flag", "bdsp_flag"]:
        d[c] = d[c].astype("string").fillna("").str.strip()
    d["join_key"] = np.where(d.legacy_bank == "PNFP",
                             d.legacy_bank + "|" + d.subproductid + "|" + d.pf_flag + "|" + d.bdsp_flag,
                             d.legacy_bank + "|" + d.subproductid + "|*|" + d.bdsp_flag)
    return d

def value_frame(df):
    """dim_nmd-shaped rows -> join_key + the value columns as clean strings."""
    d = keyed(df)
    d["dim_pf_flag"] = d["pf_flag"]                     # carried into mv_accounts.pf_flag for SNV
    for c in MAP_VALUES:
        d[c] = (d[c] if c in d else pd.Series("", index=d.index)).astype("string").fillna("").str.strip()
        d.loc[d[c].isin(["nan", "None", "<NA>"]), c] = ""
    d["product_num"] = d["product_num"].str.replace(r"\.0$", "", regex=True)
    return d

reference_raw = pd.read_csv(REFERENCE / "dim_nmd.csv", dtype=str, keep_default_na=False, encoding="utf-8-sig")
production_raw = spark.table(S.params["sources"]["src_dim_nmd"]).toPandas().astype("string")
ref = value_frame(reference_raw)
prod = value_frame(production_raw)

ref_dups = ref[ref.join_key.duplicated(keep=False)].sort_values("join_key")
print(f"Reference dim_nmd.csv: {len(ref):,} rows, {ref.join_key.nunique():,} keys | production dim_nmd: {len(prod):,} rows")
if len(ref_dups):
    print("Duplicate keys in the reference (the first row is kept here):")
    display(ref_dups)
ref_d = ref.drop_duplicates("join_key", keep="first")
prod_d = prod.drop_duplicates("join_key", keep="first")
to_view(ref_d[["join_key", "legacy_bank", "subproductid", "pf_flag", "bdsp_flag", "acct_typ_code"] + MAP_VALUES], "map_reference")

# COMMAND ----------

# DBTITLE 1,Step 5b header
# MAGIC %md
# MAGIC ## Step 5b. Derived values: PNFP from the Step 2 rule, SNV from the Step 3 rule
# MAGIC PNFP rows come straight from Step 2 (product, group, segment, rate class; `product_num` = the product code in `inputs/product_lookup.csv`). SNV rows come from Step 3's `snv_src.sql` rule (product group, client group, rate class, product — all derived from `ACCT_TYP_CDE` and `SubProductID`). With `mapping_source = DERIVED` this is the mapping used. Note: the PNFP keys changed with the new rule (`_070_00`, `_081_00`, `_INT`), so `REFERENCE` leaves those PNFP keys unmapped; use `DERIVED` or `DERIVED_ELSE_REFERENCE`.
# MAGIC
# MAGIC <details><summary>Original guidance</summary>
# MAGIC
# MAGIC One row per mapping key (`mapping_keys`). Write how each value comes from the source data. The structural columns are already derived by rule (account type from the key, rate class from the subproduct suffix). The judgement columns are left NULL for you to fill. Any column you leave NULL falls back to the reference when `mapping_source = DERIVED_ELSE_REFERENCE`.
# MAGIC
# MAGIC Useful inputs on `mapping_keys`: `acct_typ_code`, `acct_typ_codes_seen`, `subproductid`, `pf_flag`, `bdsp_flag`, `months`, `snapshot_accounts`, `snapshot_balance`, `snapshot_war`. You can also join the source views `pnfp_src` / `snv_src` (rate, override and prime rate numbers, `dnc_flag`, `ics_flag`, `index_flag`) or production `{{src_dim_nmd}}`.
# MAGIC </details>

# COMMAND ----------

# DBTITLE 1,5b. map_derived (PNFP rule + SNV rule)
sql(r"""
-- ============================== YOUR DERIVATION ==============================
-- PNFP: the Step 2 rule, one row per key; product_num = product_code from inputs/product_lookup.csv
-- SNV: the reference dim_nmd.csv rows, unchanged
CREATE OR REPLACE TEMP VIEW map_derived AS
WITH pnfp AS (
  SELECT 'PNFP' AS legacy_bank, subproductid, pf_flag, 'N' AS bdsp_flag,
         MAX(acct_typ_code) AS acct_typ_code, MAX(acct_typ_desc) AS acct_typ_desc, MAX(product_group) AS product_group,
         MAX(product_segment) AS client_group, MAX(CASE WHEN eopdate = '{{snapshot_date}}' THEN rate_class END) AS rate_class_snap,
         MAX(rate_class) AS rate_class_any, MAX(product) AS product
  FROM pnfp_src WHERE in_scope
  GROUP BY subproductid, pf_flag
)
SELECT p.legacy_bank, p.subproductid, p.pf_flag, p.bdsp_flag, p.acct_typ_code, p.acct_typ_desc, p.product_group, p.client_group,
       COALESCE(p.rate_class_snap, p.rate_class_any) AS rate_class, p.product, l.product_code AS product_num, p.pf_flag AS dim_pf_flag
FROM pnfp p LEFT JOIN product_lookup l ON l.product_name = p.product
UNION ALL
-- SNV: derived from snv_src.sql rule (product group, client group, rate class, product from ACCT_TYP_CDE + SubProductID)
SELECT 'SNV' AS legacy_bank, s.subproductid,
       CASE WHEN s.client_group = 'Public Funds' THEN 'Y' ELSE 'N' END AS pf_flag,
       s.bdsp_flag,
       MAX(s.acct_typ_code) AS acct_typ_code,
       '' AS acct_typ_desc,
       MAX(s.product_group) AS product_group,
       MAX(s.client_group) AS client_group,
       MAX(s.rate_class) AS rate_class,
       MAX(s.product) AS product,
       MAX(l.product_code) AS product_num,
       CASE WHEN s.client_group = 'Public Funds' THEN 'Y' ELSE 'N' END AS dim_pf_flag
FROM snv_src s
LEFT JOIN product_lookup l ON l.product_name = s.product
WHERE s.in_scope AND s.eopdate = '{{snapshot_date}}'
GROUP BY s.subproductid, s.bdsp_flag, s.client_group
""")
der = value_frame(spark.table("map_derived").toPandas())
filled = {c: int((der[c] != "").sum()) for c in MAP_VALUES}
print(f"Derived rows: {len(der):,} | filled values per column: {filled}")

# COMMAND ----------

# DBTITLE 1,Step 5c header
# MAGIC %md
# MAGIC ## Step 5c. Pick the values, compare, write `inputs/account_mapping_<tag>.xlsx`
# MAGIC The `mapping_used` tab is what Step 6 joins to. `compare` puts reference, derived and production next to each other for every key, with the columns that differ.

# COMMAND ----------

# DBTITLE 1,5c. mapping_used, compare and the input workbook
k = keyed(keys)
m = (k.merge(ref_d[["join_key"] + MAP_VALUES].add_suffix("_ref"), left_on="join_key", right_on="join_key_ref", how="left")
      .merge(der.drop_duplicates("join_key")[["join_key"] + MAP_VALUES].add_suffix("_der"), left_on="join_key", right_on="join_key_der", how="left")
      .merge(prod_d[["join_key"] + MAP_VALUES].add_suffix("_prod"), left_on="join_key", right_on="join_key_prod", how="left"))
for c in MAP_VALUES:
    for s in ("_ref", "_der", "_prod"):
        m[c + s] = m[c + s].astype("string").fillna("")
    r, d = m[c + "_ref"], m[c + "_der"]
    m[c] = r if MAPPING_SOURCE == "REFERENCE" else (d if MAPPING_SOURCE == "DERIVED" else d.where(d != "", r))

# ---- decisions (00_setup): source = reference, derived = Step 5b, current = what is used (current wins for its keys) ----
_ids = ["join_key", "legacy_bank", "subproductid", "pf_flag", "bdsp_flag", "acct_typ_code", "months", "snapshot_accounts",
        "snapshot_balance", "snapshot_war"]
write_decision("source", "accounts", m[_ids].assign(**{c: m[c + "_ref"] for c in MAP_VALUES}))
write_decision("derived", "accounts", m[_ids].assign(**{c: m[c + "_der"] for c in MAP_VALUES}))
_cur, CUR_INFO = resolve_current("accounts", m[_ids + MAP_VALUES])
_cur = _cur.set_index("join_key")
_picked = m[MAP_VALUES].copy()
for c in MAP_VALUES:
    m[c] = m["join_key"].map(_cur[c]).fillna(m[c]).astype("string")
m["current_edited"] = (m[MAP_VALUES] != _picked).any(axis=1)

has_ref = (m[[c + "_ref" for c in MAP_VALUES]] != "").any(axis=1)
has_der = (m[[c + "_der" for c in MAP_VALUES if c not in ("rate_class", "dim_pf_flag")]] != "").any(axis=1)
m["value_source"] = np.select(
    [m.product_group == "", MAPPING_SOURCE == "REFERENCE", MAPPING_SOURCE == "DERIVED", has_der & has_ref, has_der],
    ["NONE (unmapped)", "REFERENCE", "DERIVED", "DERIVED + REFERENCE", "DERIVED"], default="REFERENCE")
m.loc[m.current_edited & (m.product_group != ""), "value_source"] = "CURRENT (edited)"
m["ref_vs_derived"] = m.apply(lambda r: ", ".join(c for c in MAP_VALUES if r[c + "_der"] != "" and r[c + "_der"] != r[c + "_ref"]), axis=1)
m["ref_vs_production"] = m.apply(lambda r: ", ".join(c for c in MAP_VALUES if r[c + "_ref"] != r[c + "_prod"]), axis=1)

KEY_COLS = ["join_key", "legacy_bank", "subproductid", "pf_flag", "bdsp_flag", "acct_typ_code", "acct_typ_codes_seen",
            "months", "first_month", "last_month", "snapshot_accounts", "snapshot_balance", "snapshot_war"]
mapping_used = m[KEY_COLS + MAP_VALUES + ["value_source"]].sort_values(["legacy_bank", "product", "subproductid"])
compare = m[KEY_COLS + ["ref_vs_derived", "ref_vs_production"] + [c + s for c in MAP_VALUES for s in ("_ref", "_der", "_prod")]]

# the view Step 6 joins to: blanks become NULL so an unmapped key stays unmapped
mu = mapping_used.copy()
for c in MAP_VALUES:
    mu[c] = mu[c].replace("", None)
to_view(mu[["join_key", "legacy_bank", "subproductid", "pf_flag", "bdsp_flag"] + MAP_VALUES], "map_used")

unused_ref = ref_d[~ref_d.join_key.isin(k.join_key)]
README_MAP = pd.DataFrame({"tab": ["mapping_used", "keys", "compare", "reference_dim_nmd", "derived", "production_dim_nmd",
                                   "reference_duplicates", "reference_not_used", "pnfp_rate_class_rule", "source_profile"],
    "what": [f"The values used: inputs/decisions/current/accounts.csv (mapping_source = {MAPPING_SOURCE} only fills new keys); value_source says where each row came from",
             "Every key the accounts are joined on, with months of history and snapshot balance",
             "Reference vs derived vs production for every key; ref_vs_derived / ref_vs_production list the columns that differ",
             "The original dim_nmd.csv of the version (inputs/reference/dim_nmd.csv)",
             "Your Step 5b values", "Production dim_nmd, for comparison only",
             "Reference rows that share a key (the first is used)", "Reference rows no account uses",
             "Accounts and balance caught by each PNFP rate-class rule at the snapshot", "Source tables at the snapshot"]})
MAP_FILE = inp("account_mapping")
write_xlsx(MAP_FILE, {"README": README_MAP, "mapping_used": mapping_used, "keys": keys, "compare": compare,
                      "reference_dim_nmd": reference_raw, "derived": der.drop(columns=["join_key"]), "production_dim_nmd": production_raw,
                      "reference_duplicates": ref_dups, "reference_not_used": unused_ref, "pnfp_rate_class_rule": rule_counts,
                      "source_profile": source_profile})
print(f"mapping_source = {MAPPING_SOURCE} ->", MAP_FILE)
display(mapping_used.groupby(["legacy_bank", "value_source"], as_index=False).agg(keys=("join_key", "count"),
        snapshot_accounts=("snapshot_accounts", "sum"), snapshot_balance=("snapshot_balance", "sum")))

# COMMAND ----------

# DBTITLE 1,5c. Known issues in the mapping
rows = []
kb = k.set_index("join_key")
for jk, g in ref_dups.groupby("join_key"):
    a = kb.loc[jk] if jk in kb.index else None
    rows.append(("DUPLICATE_MAPPING_KEY", "HIGH", jk,
                 f"{len(g)} rows in dim_nmd.csv for this key (products: {', '.join(sorted(set(g['product'])))}). One is used here. "
                 "The version's notebook 01 joins every account of this key once per row, so lpnfp_nmds / nmds_agg double-count it.",
                 int(a.snapshot_accounts) if a is not None else 0, float(a.snapshot_balance) if a is not None else 0.0))
un = mapping_used[mapping_used.product_group == ""]
for r in un[un.snapshot_accounts > 0].itertuples():
    rows.append(("UNMAPPED_KEY", "HIGH", r.join_key,
                 f"no product_group from {MAPPING_SOURCE}; accounts show as UNMAPPED ({r.months} months of history, last {r.last_month})",
                 int(r.snapshot_accounts), float(r.snapshot_balance)))
hist_un = un[un.snapshot_accounts == 0]
if len(hist_un):
    rows.append(("UNMAPPED_HISTORY_ONLY_KEYS", "INFO", f"{len(hist_un)} keys",
                 f"no product_group and no account at the snapshot (last seen {hist_un.last_month.min()} to {hist_un.last_month.max()}); "
                 "their history appears as UNMAPPED sub_keys. List: mapping_used tab, value_source = NONE", 0, 0.0))
if MAPPING_SOURCE != "REFERENCE":
    d = m[m.ref_vs_derived != ""]
    if len(d):
        rows.append(("DERIVED_DIFFERS_FROM_REFERENCE", "MEDIUM", f"{len(d)} keys", "see the compare tab",
                     int(d.snapshot_accounts.sum()), float(d.snapshot_balance.sum())))
d = m[(m.ref_vs_production != "") & (m.snapshot_accounts > 0)]
if len(d):
    rows.append(("REFERENCE_DIFFERS_FROM_PRODUCTION", "INFO", f"{len(d)} keys",
                 "the version's dim_nmd.csv differs from production dim_nmd on these keys (see compare); the reference is what the version used",
                 int(d.snapshot_accounts.sum()), float(d.snapshot_balance.sum())))
issues_mapping = log_issues("01 mapping", rows)

# COMMAND ----------

# DBTITLE 1,Step 6 header
# MAGIC %md
# MAGIC ## Step 6. The account DataFrame
# MAGIC Every in-scope account in every month, joined to `map_used` (`accounts_all`); the snapshot month is saved as `mv_accounts`, one row per account. The product comes straight from the mapping (the Step 2 rule already produces Standard Plus and Exception, so the old Standard Plus rename is gone). PNFP scope = `in_scope` from Step 2 (open accounts, internal ones per `pnfp_include_internal`).
# MAGIC
# MAGIC `sub_key = bank | subproductid | product` is the key every later step uses.

# COMMAND ----------

# DBTITLE 1,6. accounts_all and mv_accounts
sql("""
CREATE OR REPLACE TEMP VIEW accounts_all AS
WITH pnfp AS (
  SELECT 'PNFP' AS legacy_bank, s.eopdate, s.accountid, s.acct_typ_code, m.acct_typ_desc, s.subproductid, s.pf_flag,
         'N' AS bdsp_flag, m.product_group,
         m.product,
         CAST(m.product_num AS BIGINT) AS product_num,
         s.eopbook, s.avgbook, s.currcoupon, s.sumprod_eoprate,
         s.override_rate, s.prime_rate, s.dnc_flag, s.ics_flag, s.index_flag, s.open_date
  FROM pnfp_src s
  LEFT JOIN map_used m
    ON m.legacy_bank = 'PNFP' AND m.subproductid = s.subproductid AND m.pf_flag = s.pf_flag AND m.bdsp_flag = 'N'
  WHERE s.in_scope
),
snv AS (
  SELECT 'SNV', s.eopdate, s.accountid, s.acct_typ_code, m.acct_typ_desc, s.subproductid, m.dim_pf_flag,
         s.bdsp_flag, m.product_group, m.product, CAST(m.product_num AS BIGINT),
         s.eopbook, s.avgbook, s.currcoupon, s.sumprod_eoprate,
         CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), s.open_date
  FROM snv_src s
  LEFT JOIN map_used m
    ON m.legacy_bank = 'SNV' AND m.subproductid = s.subproductid AND m.bdsp_flag = s.bdsp_flag
  WHERE s.in_scope
)
SELECT *,
       CONCAT_WS('|', legacy_bank, subproductid, COALESCE(product, 'UNMAPPED')) AS sub_key,
       product_group IS NOT NULL AS mapped
FROM (SELECT * FROM pnfp UNION ALL SELECT * FROM snv);

CREATE OR REPLACE TABLE {{mv_accounts}} AS
SELECT * FROM accounts_all WHERE eopdate = '{{snapshot_date}}'
""")
acc = spark.table(S.table("mv_accounts"))
display(acc.groupBy("legacy_bank", "mapped").agg(F.count("*").alias("accounts"), F.countDistinct("accountid").alias("distinct_accounts"),
                                                F.sum("eopbook").alias("balance")).orderBy("legacy_bank", "mapped"))

# COMMAND ----------

# DBTITLE 1,6. Checks against the version and the accounts workbook
recon = sqlq("""
SELECT legacy_bank, COUNT(*) AS accounts, COUNT(DISTINCT accountid) AS distinct_accounts, SUM(eopbook) AS balance,
       TRY_DIVIDE(SUM(sumprod_eoprate), SUM(eopbook)) AS war,
       SUM(CASE WHEN NOT mapped THEN 1 ELSE 0 END) AS unmapped_accounts, SUM(CASE WHEN NOT mapped THEN eopbook ELSE 0 END) AS unmapped_balance
FROM {{mv_accounts}} GROUP BY 1 ORDER BY 1
""").toPandas()
assert (recon.accounts == recon.distinct_accounts).all(), "an account appears twice in mv_accounts"

# The version's own tables, for comparison only (this notebook does not read them to build anything)
rows = []
if S.table_exists("nmds_agg"):
    g = sqlq("SELECT legacy_bank, SUM(eopbook) AS version_nmds_agg_balance FROM {{nmds_agg}} WHERE eopdate = '{{snapshot_date}}' GROUP BY 1").toPandas()
    recon = recon.merge(g, on="legacy_bank", how="left")
    # the version's nmds_agg has no internal accounts; take them out before comparing
    ib = sqlq("SELECT legacy_bank, SUM(eopbook) AS internal_balance FROM {{mv_accounts}} WHERE product = 'Internal Deposits' GROUP BY 1").toPandas()
    recon = recon.merge(ib, on="legacy_bank", how="left").fillna({"internal_balance": 0.0})
    recon["difference"] = recon.balance - recon.internal_balance - recon.version_nmds_agg_balance
    dup_bal = sum(float(kb.loc[jk].snapshot_balance) * (len(gr) - 1) for jk, gr in ref_dups.groupby("join_key") if jk in kb.index)
    recon["explained_by_duplicate_keys"] = np.where(recon.legacy_bank == "PNFP", -dup_bal, 0.0)
    for r in recon.itertuples():
        left = r.difference - r.explained_by_duplicate_keys
        if abs(left) >= 1:
            rows.append(("BALANCE_DIFFERS_FROM_VERSION", "HIGH", r.legacy_bank,
                         f"mv_accounts differs from the version's nmds_agg by ${r.difference:,.2f}; ${left:,.2f} is not explained by duplicated keys",
                         int(r.accounts), float(r.difference)))
        elif abs(r.difference) >= 1:
            rows.append(("VERSION_DOUBLE_COUNT", "MEDIUM", r.legacy_bank,
                         f"the version's nmds_agg is ${-r.difference:,.2f} higher than the source; all of it is the duplicated dim_nmd key(s)",
                         "", float(-r.difference)))
if S.table_exists("lpnfp_nmds") and MAPPING_SOURCE == "REFERENCE":     # the new PNFP rule changes subproducts on purpose
    acct_chk = sqlq("""
    SELECT COUNT(*) AS pnfp_accounts, SUM(CASE WHEN v.accountid IS NULL THEN 1 ELSE 0 END) AS missing_in_version,
           SUM(CASE WHEN v.accountid IS NOT NULL AND v.subproductid <> a.subproductid THEN 1 ELSE 0 END) AS subproduct_differs,
           SUM(CASE WHEN v.accountid IS NOT NULL AND COALESCE(v.product, '') <> COALESCE(a.product, '') THEN 1 ELSE 0 END) AS product_differs
    FROM {{mv_accounts}} a
    LEFT JOIN (SELECT DISTINCT accountid, subproductid, product FROM {{lpnfp_nmds}}
               WHERE eopdate = '{{snapshot_date}}' AND internal_flag <> '1' AND closed_flag <> '1') v ON a.accountid = v.accountid
    WHERE a.legacy_bank = 'PNFP'
    """).toPandas()
    print("PNFP account by account vs the version's lpnfp_nmds (rate-class rule and mapping reproduced if all three are 0):")
    display(acct_chk)
    c = acct_chk.iloc[0]
    if c.missing_in_version or c.subproduct_differs or (c.product_differs and MAPPING_SOURCE == "REFERENCE"):
        rows.append(("ACCOUNTS_DIFFER_FROM_VERSION", "HIGH", "PNFP",
                     f"missing {int(c.missing_in_version)}, subproduct differs {int(c.subproduct_differs)}, product differs {int(c.product_differs)}", "", ""))
display(recon)
issues_accounts = log_issues("01 accounts", rows)

ACCOUNT_DICT = [
    ("legacy_bank", "constant 'PNFP'", "constant 'SNV'", "Legacy bank"),
    ("eopdate", "pnfp_deposits.load_date", "lsnv_nmds.EOPDate", "= snapshot_date"),
    ("accountid", "pnfp_deposits.account_type || account_number", "lsnv_nmds.AccountID", "Account key (string)"),
    ("acct_typ_code", "pnfp_deposits.service_charge_code", "lsnv_nmds.ACCT_TYP_CDE", "Legacy account type code"),
    ("acct_typ_desc", "mapping", "mapping", "From inputs/account_mapping (mapping_used)"),
    ("subproductid", "service_charge_code || '_' || rate-class suffix, or '_INT' for internal (Step 2)", "lsnv_nmds.SubProductID", "Join key to the mapping"),
    ("pf_flag", "pnfp_deposits.pf = '1'", "mapping (dim_pf_flag)", "PNFP joins the mapping on subproductid + pf_flag"),
    ("bdsp_flag", "constant 'N'", "'Y' for accounts in snv_bdsp_accounts", "SNV joins the mapping on subproductid + bdsp_flag"),
    ("product_group", "mapping", "mapping", "NULL = no mapping row (UNMAPPED)"),
    ("product", "mapping (the 33-product rule of Step 2)", "mapping", "Current product"),
    ("product_num", "product_code in inputs/product_lookup.csv", "mapping", ""),
    ("eopbook", "pnfp_deposits.ledger", "lsnv_nmds.EOPBook", "End-of-period balance"),
    ("avgbook", "pnfp_deposits.avgbal", "lsnv_nmds.AvgBook", "Average balance"),
    ("currcoupon", "pnfp_deposits.rate", "lsnv_nmds.CurrCoupon", "Account rate"),
    ("sumprod_eoprate", "rate x ledger", "CurrCoupon x EOPBook", "WAR numerator"),
    ("override_rate", "pnfp_deposits.override_rate_number", "NULL", "Rate-class input (PNFP)"),
    ("prime_rate", "pnfp_deposits.prime_rate_num", "NULL", "Rate-class input (PNFP)"),
    ("dnc_flag", "override rate = 787", "NULL", "Do-not-change rate flag"),
    ("ics_flag", "pnfp_deposits.ics", "NULL", ""),
    ("index_flag", "pnfp_deposits.index", "NULL", ""),
    ("open_date", "pnfp_deposits.date7_opened", "lsnv_nmds.OpenDate", ""),
    ("sub_key", "legacy_bank | subproductid | product", "same", "Key used by every later step"),
    ("mapped", "product_group IS NOT NULL", "same", "FALSE = no mapping row"),
]
assert [c[0] for c in ACCOUNT_DICT] == acc.columns, f"dictionary out of sync: {set(acc.columns) ^ {c[0] for c in ACCOUNT_DICT}}"
n = acc.count()
prof = acc.agg(*[F.count(F.col(c)).alias(c) for c in acc.columns]).toPandas().T[0]
dictionary = pd.DataFrame([dict(column=c, data_type=dict(acc.dtypes)[c], pnfp_source=p, snv_source=s, rule=r,
                                non_null=int(prof[c]), null_pct=round(100 * (1 - prof[c] / n), 2)) for c, p, s, r in ACCOUNT_DICT])
by_sub = sqlq("""
SELECT legacy_bank, sub_key, subproductid, acct_typ_desc, COALESCE(product, 'UNMAPPED') AS product, product_group, pf_flag, bdsp_flag,
       COUNT(*) AS accounts, SUM(eopbook) AS eopbook, TRY_DIVIDE(SUM(sumprod_eoprate), SUM(eopbook)) AS war
FROM {{mv_accounts}} GROUP BY ALL ORDER BY legacy_bank, product, subproductid
""").toPandas()
# accounts data is now included in Account_Review (03a) — no standalone accounts.xlsx
# Kept here: recon, dictionary, by_sub are displayed and available if needed.
print(f"{n:,} accounts | {len(by_sub):,} subproduct rows | accounts data -> Account_Review (03a)")

# COMMAND ----------

# DBTITLE 1,Step 7 header
# MAGIC %md
# MAGIC ## Step 7. Monthly history by subproduct (`mv_account_war`)
# MAGIC Every month of `accounts_all` summed by `sub_key`: balance, average balance and WAR = sum(rate x balance) / sum(balance). This is the series the betas are fitted on. Each month also carries the account count and the (unweighted, approximate) account rate distribution `rate_min`, `rate_p10` ... `rate_p90`, `rate_max`, used by the account review (03a). The snapshot month must tie to `mv_accounts` exactly (balance and accounts).

# COMMAND ----------

# DBTITLE 1,7. mv_account_war
sql("""
CREATE OR REPLACE TABLE {{mv_account_war}} AS
SELECT CONCAT_WS('|', legacy_bank, subproductid, COALESCE(product, 'UNMAPPED')) AS sub_key,
       legacy_bank, subproductid, COALESCE(product, 'UNMAPPED') AS product, eopdate,
       MAX(product_group) AS product_group, MAX(acct_typ_desc) AS acct_typ_desc,
       SUM(eopbook) AS eopbook, SUM(avgbook) AS avgbook, SUM(sumprod_eoprate) AS sumprod_eoprate,
       CASE WHEN SUM(eopbook) > 0 THEN SUM(sumprod_eoprate) / SUM(eopbook) END AS war,
       COUNT(*) AS accounts, SUM(CASE WHEN eopbook > 0 THEN 1 ELSE 0 END) AS accounts_with_balance,
       MIN(currcoupon) AS rate_min,
       PERCENTILE_APPROX(currcoupon, 0.10) AS rate_p10, PERCENTILE_APPROX(currcoupon, 0.25) AS rate_p25,
       PERCENTILE_APPROX(currcoupon, 0.50) AS rate_p50, PERCENTILE_APPROX(currcoupon, 0.75) AS rate_p75,
       PERCENTILE_APPROX(currcoupon, 0.90) AS rate_p90, MAX(currcoupon) AS rate_max
FROM accounts_all
GROUP BY legacy_bank, subproductid, COALESCE(product, 'UNMAPPED'), eopdate
""")
tie = sqlq("""
SELECT COUNT(*) AS sub_keys,
       SUM(CASE WHEN ABS(COALESCE(w.eopbook, 0) - a.eopbook) >= 0.01 OR COALESCE(w.accounts, 0) <> a.accounts THEN 1 ELSE 0 END) AS not_tied
FROM (SELECT sub_key, SUM(eopbook) AS eopbook, COUNT(*) AS accounts FROM {{mv_accounts}} GROUP BY 1) a
LEFT JOIN (SELECT sub_key, eopbook, accounts FROM {{mv_account_war}} WHERE eopdate = '{{snapshot_date}}') w ON a.sub_key = w.sub_key
""").first()
assert tie.not_tied == 0, f"{tie.not_tied} sub_keys do not tie to mv_accounts at the snapshot"
hist = sqlq("""
SELECT legacy_bank, COUNT(DISTINCT sub_key) AS sub_keys, COUNT(DISTINCT eopdate) AS months, MIN(eopdate) AS first_month, MAX(eopdate) AS last_month,
       SUM(CASE WHEN eopdate = '{{snapshot_date}}' THEN eopbook END) AS snapshot_balance
FROM {{mv_account_war}} GROUP BY 1 ORDER BY 1
""").toPandas()
print(f"Snapshot ties to mv_accounts on all {tie.sub_keys} sub_keys")
if S.table_exists("nmds_agg"):
    by_month = sqlq("""
    SELECT w.legacy_bank, w.eopdate, w.eopbook AS mv_balance, g.eopbook AS version_nmds_agg_balance, w.eopbook - g.eopbook AS difference
    FROM (SELECT legacy_bank, eopdate, SUM(eopbook) AS eopbook FROM {{mv_account_war}} GROUP BY 1, 2) w
    FULL OUTER JOIN (SELECT legacy_bank, eopdate, SUM(eopbook) AS eopbook FROM {{nmds_agg}} GROUP BY 1, 2) g
      ON w.legacy_bank = g.legacy_bank AND w.eopdate = g.eopdate
    """).toPandas()
    hist = hist.merge(by_month.assign(off=by_month.difference.abs() >= 1).groupby("legacy_bank", as_index=False)
                      .agg(months_differing_from_version=("off", "sum"), max_abs_difference=("difference", lambda s: s.abs().max())),
                      on="legacy_bank", how="left")
display(hist)