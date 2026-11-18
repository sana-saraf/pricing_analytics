# Databricks notebook source
# DBTITLE 1,Truncation 1 of 2: build the cohorts
# MAGIC %md
# MAGIC # Truncation analysis, part 1 of 2: build the cohorts (study version)
# MAGIC
# MAGIC **Question:** of the accounts that existed at a baseline date, how many are still open N months later, by final study product?
# MAGIC
# MAGIC | Bank | Month 0 | Cohort | Source |
# MAGIC |---|---|---|---|
# MAGIC | PNFP | `pnfp_baseline` | opened before `pnfp_open_before`, open, not internal | `src_pnfp_deposits` through `notebooks/sql/pnfp_src.sql` (the same rule as 01) |
# MAGIC | SNV | `snv_baseline` | the `vintage_years` vintages | `inputs/vintages/<year>Vintage.csv` (copied from `vintage_source_folder` the first time) |
# MAGIC
# MAGIC **Products** come from this version's mapping: subproduct -> `product` from `inputs/decisions/current/accounts.csv`,
# MAGIC then -> final `mv_product` from `mv_product_selection` (04). SNV vintage subproducts the study does not hold today fall back to
# MAGIC `inputs/reference/dim_nmd.csv`; anything still unmapped is listed and left out of the product results.
# MAGIC
# MAGIC Settings: `config/study_params.json` -> `truncation`. Output: `inputs/vintages/truncation_cohort_<tag>.csv`, read by 11.

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ../00_setup

# COMMAND ----------

# DBTITLE 1,Settings (config/study_params.json -> truncation)
import shutil
TR = {k: v for k, v in (S.params.get("truncation") or {}).items() if not str(k).startswith("_")}
VINTAGE_SRC = Path(TR.get("vintage_source_folder", ""))
VINTAGE_YEARS = [y.strip() for y in str(TR.get("vintage_years", "2015,2016,2017")).split(",") if y.strip()]
SNV_BASELINE, PNFP_BASELINE = TR.get("snv_baseline", "2019-05-31"), TR.get("pnfp_baseline", "2018-01-31")
PNFP_OPEN_BEFORE = TR.get("pnfp_open_before", "2018-02-01")
VINTAGES = INPUTS / "vintages"
VINTAGES.mkdir(parents=True, exist_ok=True)
COHORT_CSV = VINTAGES / f"{out_name('truncation_cohort')}.csv"
print(f"SNV vintages {VINTAGE_YEARS}, month 0 {SNV_BASELINE} | PNFP opened before {PNFP_OPEN_BEFORE}, month 0 {PNFP_BASELINE}")
print(f"cohort file: {COHORT_CSV}")

# COMMAND ----------

# DBTITLE 1,1. Load the vintage CSVs into inputs/vintages
# copy any <year>Vintage.csv not yet in the study (the study copy wins after that), then read the years in use
if VINTAGE_SRC.is_dir():
    for f in sorted(VINTAGE_SRC.glob("*Vintage.csv")):
        if not (VINTAGES / f.name).exists():
            shutil.copy2(f, VINTAGES / f.name)
            print("copied", f.name)
print("in inputs/vintages:", sorted(p.name for p in VINTAGES.glob("*Vintage.csv")))

def _month_end(v):
    """PRCS_DTE is an Excel serial (2015-2017 files) or m/d/yyyy text (2018 file)."""
    s = str(v).strip()
    d = pd.Timestamp("1899-12-30") + pd.Timedelta(days=int(float(s))) if s.replace(".", "").isdigit() else pd.to_datetime(s)
    return (d + pd.offsets.MonthEnd(0)).normalize()

vin = []
for y in VINTAGE_YEARS:
    p = VINTAGES / f"{y}Vintage.csv"
    assert p.exists(), f"missing {p}: put it in inputs/vintages or fix vintage_source_folder"
    d = pd.read_csv(p, dtype={"SubProductID": str})
    d["eopdate"] = d["PRCS_DTE"].map(_month_end)
    d["vintage"] = y
    vin.append(d)
    print(f"{y}: {len(d):,} rows | {d.eopdate.min():%Y-%m-%d} to {d.eopdate.max():%Y-%m-%d} | {d.SubProductID.nunique()} subproducts")
vin = pd.concat(vin, ignore_index=True)
assert not vin.duplicated(["vintage", "SubProductID", "eopdate"]).any(), "a vintage file has two rows for one subproduct and month"

# COMMAND ----------

# DBTITLE 1,2. This version's product mapping (current accounts -> mv_product)
cur = read_decision("current", "accounts")
sel = spark.table(S.table("mv_product_selection")).select("sub_key", "mv_product", "product_group").toPandas()
sel_map = dict(zip(sel.sub_key, sel.mv_product))

def to_mv(bank, sub, product):
    return sel_map.get(f"{bank}|{sub}|{product}") or None

# SNV: bdsp N keys (the vintages are not split by BDSP); fallback = inputs/reference/dim_nmd.csv
snv_cur = cur[(cur.legacy_bank == "SNV") & (cur.bdsp_flag == "N")].set_index("subproductid")
ref = pd.read_csv(REFERENCE / "dim_nmd.csv", dtype=str, keep_default_na=False)
ref.columns = [c.lower() for c in ref.columns]
if "legacy_bank" in ref.columns:
    ref = ref[ref.legacy_bank.str.upper() == "SNV"]
if "bdsp_flag" in ref.columns:
    ref = ref[ref.bdsp_flag.str.upper() != "Y"]
ref = ref.drop_duplicates("subproductid").set_index("subproductid")

rows = []
for sub in sorted(vin.SubProductID.unique()):
    if sub in snv_cur.index:
        prod, grp, src = snv_cur.at[sub, "product"], snv_cur.at[sub, "product_group"], "current"
    elif sub in ref.index and ref.at[sub, "product"]:
        prod, grp, src = ref.at[sub, "product"], ref.at[sub, "product_group"], "reference dim_nmd"
    else:
        prod, grp, src = None, None, "UNMAPPED"
    mv = to_mv("SNV", sub, prod) if prod else None
    rows.append({"subproductid": sub, "product": prod, "product_group": grp,
                 "mv_product": mv or prod, "map_source": src if mv or not prod else src + " (no mv_product, product name used)"})
snv_map = pd.DataFrame(rows)
display(snv_map.groupby("map_source", as_index=False).size())

# COMMAND ----------

# DBTITLE 1,3. SNV cohort from the vintages
snv = (vin.merge(snv_map, left_on="SubProductID", right_on="subproductid", how="left")
          .groupby(["eopdate", "subproductid", "product", "product_group", "mv_product", "map_source"], dropna=False, as_index=False)
          .agg(acct_cnt=("Acct_Cnt", "sum"), sum_balance=("SumBal", "sum")))
snv["legacy_bank"], snv["baseline"] = "SNV", pd.Timestamp(SNV_BASELINE)
print(f"SNV: {len(snv):,} rows | month 0 accounts {int(snv.loc[snv.eopdate == SNV_BASELINE, 'acct_cnt'].sum()):,}")

# COMMAND ----------

# DBTITLE 1,4. PNFP cohort from src_pnfp_deposits (same rule as 01)
# pnfp_src.sql with internal accounts out (they are not a customer cohort)
sql((STUDY_ROOT / "notebooks" / "sql" / "pnfp_src.sql").read_text(encoding="utf-8").replace("__INCLUDE_INTERNAL__", "N"))
# every account keeps its LATEST subproduct + pf flag for its whole history (a code change is not a close + open)
sql(f"""
CREATE OR REPLACE TEMP VIEW trunc_pnfp AS
WITH c AS (
  SELECT *, COALESCE(TRY_CAST(open_date AS DATE), TRY_TO_DATE(CAST(open_date AS STRING), 'yyyyMMdd')) AS opened
  FROM pnfp_src WHERE in_scope
),
latest AS (
  SELECT accountid, subproductid AS sub_latest, pf_flag AS pf_latest,
         ROW_NUMBER() OVER (PARTITION BY accountid ORDER BY eopdate DESC) AS rn
  FROM c
)
SELECT c.eopdate, l.sub_latest AS subproductid, l.pf_latest AS pf_flag,
       COUNT(DISTINCT c.accountid) AS acct_cnt, SUM(c.eopbook) AS sum_balance
FROM c JOIN latest l ON l.accountid = c.accountid AND l.rn = 1
WHERE c.opened < DATE'{PNFP_OPEN_BEFORE}' AND c.eopdate >= DATE'{PNFP_BASELINE}'
GROUP BY ALL
""")
chk = sqlq(f"""SELECT COUNT(*) AS rows_in_scope, COUNT_IF(opened IS NULL) AS no_open_date
             FROM (SELECT COALESCE(TRY_CAST(open_date AS DATE), TRY_TO_DATE(CAST(open_date AS STRING), 'yyyyMMdd')) AS opened
                   FROM pnfp_src WHERE in_scope AND eopdate = DATE'{PNFP_BASELINE}')""").toPandas().iloc[0]
print(f"PNFP at month 0: {int(chk.rows_in_scope):,} in-scope rows, {int(chk.no_open_date):,} with an open date that did not parse")
assert chk.no_open_date < 0.01 * chk.rows_in_scope, "open_date did not parse for over 1% of accounts: fix the date format in this cell"

pn = spark.table("trunc_pnfp").toPandas()
pn["eopdate"] = pd.to_datetime(pn.eopdate)
pnfp_cur = cur[cur.legacy_bank == "PNFP"].set_index(["subproductid", "pf_flag"])
def _pnfp_prod(r):
    k = (r.subproductid, r.pf_flag)
    if k not in pnfp_cur.index:
        return pd.Series([None, None, None, "UNMAPPED"])
    prod, grp = pnfp_cur.loc[k, "product"], pnfp_cur.loc[k, "product_group"]
    prod, grp = (prod.iloc[0], grp.iloc[0]) if isinstance(prod, pd.Series) else (prod, grp)
    mv = to_mv("PNFP", r.subproductid, prod)
    return pd.Series([prod, grp, mv or prod, "current" if mv else "current (no mv_product, product name used)"])
keys = pn[["subproductid", "pf_flag"]].drop_duplicates()
keys[["product", "product_group", "mv_product", "map_source"]] = keys.apply(_pnfp_prod, axis=1)
pnfp = pn.merge(keys, on=["subproductid", "pf_flag"], how="left").drop(columns="pf_flag")
pnfp["legacy_bank"], pnfp["baseline"] = "PNFP", pd.Timestamp(PNFP_BASELINE)
print(f"PNFP: {len(pnfp):,} rows | month 0 accounts {int(pnfp.loc[pnfp.eopdate == PNFP_BASELINE, 'acct_cnt'].sum()):,}")

# COMMAND ----------

# DBTITLE 1,5. Stack the banks, add N_Month, save
coh = pd.concat([pnfp, snv], ignore_index=True)
coh["N_Month"] = (coh.eopdate.dt.year - coh.baseline.dt.year) * 12 + (coh.eopdate.dt.month - coh.baseline.dt.month)
coh = coh[coh.N_Month >= 0]
cols = ["legacy_bank", "eopdate", "baseline", "N_Month", "subproductid", "product", "product_group", "mv_product",
        "map_source", "acct_cnt", "sum_balance"]
coh = coh[cols].sort_values(["legacy_bank", "N_Month", "mv_product", "subproductid"])
coh.to_csv(COHORT_CSV, index=False, date_format="%Y-%m-%d")

m0 = coh[coh.N_Month == 0]
summary = (coh.groupby("legacy_bank").agg(first=("eopdate", "min"), last=("eopdate", "max"), last_n_month=("N_Month", "max"))
              .join(m0.groupby("legacy_bank").agg(month_0_accounts=("acct_cnt", "sum"), month_0_balance=("sum_balance", "sum")))
              .join(m0[m0.map_source == "UNMAPPED"].groupby("legacy_bank").agg(month_0_unmapped_accounts=("acct_cnt", "sum"))))
display(summary.reset_index())
unm = (m0[m0.map_source == "UNMAPPED"].groupby(["legacy_bank", "subproductid"], as_index=False)
         .agg(month_0_accounts=("acct_cnt", "sum"), month_0_balance=("sum_balance", "sum")).sort_values("month_0_accounts", ascending=False))
log_issues("10 truncation", [("TRUNCATION_UNMAPPED", "MEDIUM", f"{r.legacy_bank} {r.subproductid}",
                              f"{int(r.month_0_accounts):,} month 0 accounts have no product; left out of product results", "", "")
                             for r in unm.itertuples()])
print(f"saved {COHORT_CSV} ({len(coh):,} rows) | unmapped subproducts at month 0: {len(unm)}")
display(unm)

# COMMAND ----------

