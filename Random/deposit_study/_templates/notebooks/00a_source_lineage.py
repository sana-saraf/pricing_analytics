# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 00a | Source lineage (check only)
# MAGIC Rebuilds the two model sources from their raw CSV files and compares them, month by month, to the tables the study reads. **Nothing is written to a table**: every step is a temp view. Results go to `outputs/reports/<source_lineage>_<tag>.md` / `.csv` and the known issues log.
# MAGIC
# MAGIC | Part | Raw files | Rebuilt as | Compared to |
# MAGIC |---|---|---|---|
# MAGIC | A1 | `dp/facts/dp_fact_YYYY-MM.csv` (2017-01 to 2019-01) | `dp_csv` | `finance.staging.alco_dp_fact` |
# MAGIC | A2-A4 | `alco_dp_fact` + ICS (`silver_prod.intrafi.deposits_ics`) + `dp/dp_txn_760.csv` + Empyrean maps | `snv_nmd_201701_201901` (business rules: balance + ICS, coupon, MTD income, SubProductID = type_rate_index, NMD map INC only) | 2019-01 history file |
# MAGIC | B | `dp/history/SNV_DA_History_YYYYMM.csv` (2019-01 onward) | `snv_nmd_history` | `src_lsnv_nmds` (`finance.pricing_analytics.lsnv_nmds`) |
# MAGIC | PNFP | `lpnfp_nmds/WAL Deps YYYYMMDD.csv` | `pnfp_files` | `finance.pricing_analytics.lpnfp_dp_history` (rows + hash) and `src_pnfp_deposits` (rows + ledger by month) |
# MAGIC
# MAGIC **End month:** `dp_history_end_date` in `config/study_params.json` (set by `install/execute_study`). Blank = the newest file in each folder. Files after the end month are ignored, and so are table months after it.
# MAGIC
# MAGIC The production loader is the draft *SNV NMD Data Lineage for Model Validation*; this notebook only reproduces and checks it.

# COMMAND ----------

# DBTITLE 1,Install openpyxl
# MAGIC %pip install -q openpyxl

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ./00_setup

# COMMAND ----------

# DBTITLE 1,Parameters and helpers
import os, json
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# Raw file folders and the read-only tables the rebuild uses. Override any of them with a "lineage" block in config/study_params.json.
LIN = {
    "facts_dir":   "/Volumes/finance/staging/commondrive_6800002_alco_extracts/dp/facts/",
    "history_dir": "/Volumes/finance/staging/commondrive_6800002_alco_extracts/dp/history/",
    "txn_760_file": "/Volumes/finance/staging/commondrive_6800002_alco_extracts/dp/dp_txn_760.csv",
    "pnfp_dir":    "/Volumes/finance/staging/lpnfp_alco_extracts/lpnfp_nmds/",
    "dp_fact":     "finance.staging.alco_dp_fact",
    "ics":         "silver_prod.intrafi.deposits_ics",
    "fcb_map":     "finance.empyrean.fcb",
    "nmd_map":     "finance.empyrean.nmd_map",
    "alm_org_map": "finance.empyrean.alm_org_map",
    "pnfp_history": "finance.pricing_analytics.lpnfp_dp_history",
}
LIN.update(S.params.get("lineage") or {})
LSNV = S.params["sources"]["src_lsnv_nmds"]
PNFP_SRC = S.params["sources"]["src_pnfp_deposits"]
SCHEMA_FILE = os.path.join(LIN["facts_dir"], "dp_fact_schema.json")
CSV_OPTS = {"header": "true", "timestampNTZFormat": "yyyy-MM-dd HH:mm:ss.SSSSSS", "multiLine": "true",
            "ignoreLeadingWhiteSpace": "false", "ignoreTrailingWhiteSpace": "false"}
DP_FIRST_MONTH, HISTORY_FIRST_MONTH = 201701, 201901

# End month: blank / missing = newest file
_end = str(S.params.get("dp_history_end_date") or "").strip()
hist_all = sorted(f for f in os.listdir(LIN["history_dir"]) if f.startswith("SNV_DA_History_") and f.endswith(".csv"))
pnfp_all = sorted(f for f in os.listdir(LIN["pnfp_dir"]) if f.startswith("WAL Deps ") and f.endswith(".csv"))
_hist_ym = lambda f: int(f[len("SNV_DA_History_"):][:6])
_pnfp_ym = lambda f: int(f[len("WAL Deps "):][:6])
END_YM = int(_end[:7].replace("-", "")) if _end else max(_hist_ym(f) for f in hist_all)
PNFP_END_YM = int(_end[:7].replace("-", "")) if _end else max(_pnfp_ym(f) for f in pnfp_all)
hist_files = [f for f in hist_all if _hist_ym(f) <= END_YM]
pnfp_files = [f for f in pnfp_all if _pnfp_ym(f) <= PNFP_END_YM]
print(f"dp_history_end_date = {_end or 'latest'} -> SNV to {END_YM} ({len(hist_files)} of {len(hist_all)} history files), "
      f"PNFP to {PNFP_END_YM} ({len(pnfp_files)} of {len(pnfp_all)} files)")

# lsnv_nmds layout
LSNV_SCHEMA = [
    ("EOPDate", "date"), ("YR_MTH_NBR", "bigint"), ("AccountID", "bigint"), ("ACCT_TYP_CDE", "bigint"),
    ("SubProductID", "string"), ("EOPBook", "double"), ("CurrCoupon", "double"), ("AcquisitionID", "bigint"),
    ("OpenDate", "date"), ("DeptCode", "string"), ("AvgBook", "double"), ("MTDIncome", "double"),
    ("CustomerID", "bigint"), ("BusinessUnit", "bigint"), ("Region", "string"), ("BranchCode", "bigint"),
    ("ZipCode", "string"), ("OfficerCode", "bigint"), ("InstrumentID", "bigint"), ("AsOfDate", "date"),
    ("LegalEntityID", "bigint"), ("GL_BS_Acct", "bigint"), ("GL_IS_Acct", "bigint"), ("ALMAccountNum", "bigint"),
    ("ALMOrgUnitID", "string"), ("file_path", "string"), ("upload_timestamp", "timestamp"),
]
LSNV_DATA_COLS = [c for c, _ in LSNV_SCHEMA if c not in ("file_path", "upload_timestamp")]

def fingerprint(df, cols, by=None):
    """Row count + order-independent hash of every row."""
    h = F.sum(F.xxhash64(*[F.col(c) for c in sorted(cols)]).cast("decimal(38,0)")).alias("row_hash_sum")
    agg = [F.count("*").alias("rows"), h]
    return df.groupBy(by).agg(*agg).orderBy(by) if by else df.agg(*agg)

def compare_by_month(left, right, cols, ln="left", rn="right"):
    a = fingerprint(left, cols, "YR_MTH_NBR").withColumnsRenamed({"rows": f"{ln}_rows", "row_hash_sum": "h1"})
    b = fingerprint(right, cols, "YR_MTH_NBR").withColumnsRenamed({"rows": f"{rn}_rows", "row_hash_sum": "h2"})
    return (a.join(b, "YR_MTH_NBR", "full_outer")
             .withColumn("identical", F.coalesce(F.col("h1") == F.col("h2"), F.lit(False)) & (F.col(f"{ln}_rows") == F.col(f"{rn}_rows")))
             .drop("h1", "h2").orderBy("YR_MTH_NBR"))

CHECKS = []          # (part, check, passed, detail)
def check(part, name, ok, detail=""):
    CHECKS.append((part, name, bool(ok), str(detail)))
    print(f"{'PASS' if ok else 'FAIL'}  {part}: {name}  {detail}")

# COMMAND ----------

# DBTITLE 1,A1. DP fact CSVs vs alco_dp_fact
dp_schema = StructType.fromJson(json.load(open(SCHEMA_FILE)))
dp_cols = [f.name for f in dp_schema.fields]
csv_files = sorted(f for f in os.listdir(LIN["facts_dir"]) if f.startswith("dp_fact_") and f.endswith(".csv"))
dp_csv = spark.read.schema(dp_schema).options(**CSV_OPTS).csv([os.path.join(LIN["facts_dir"], f) for f in csv_files])
a1 = compare_by_month(dp_csv, spark.table(LIN["dp_fact"]), dp_cols, "csv", "table")
display(a1)
n_bad = a1.filter(~F.col("identical")).count()
check("A1", "DP fact CSVs identical to alco_dp_fact", n_bad == 0, f"{len(csv_files)} files, {n_bad} months differ")

# COMMAND ----------

# DBTITLE 1,A2-A4. Business rules -> snv_nmd_201701_201901 (temp views)
(spark.read.schema("ACCT_NBR string, PRCS_DTE string, AMOUNT string").option("header", "true").csv(LIN["txn_760_file"])
      .select("ACCT_NBR", F.to_date(F.substring("PRCS_DTE", 1, 10), "yyyy-MM-dd").alias("PRCS_DTE"),
              F.col("AMOUNT").cast("decimal(18,2)").alias("AMOUNT"))
      .createOrReplaceTempView("dp_txn_760"))

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW bp_dp_fact AS
SELECT YR_MTH_NBR, BNK_NBR, ACCT_NBR, ACCT_TYP_CDE, ENTITY, OPEN_DTE, PRCS_DTE, last_day(PRCS_DTE) AS PRCS_DTE_EOM,
       INT_PLN_NBR, LGR_BAL_AMT, AVG_LGR_BAL_MTD_AMT, (CUR_INT_RTE + BNS_INT_RTE) / 100.0 AS CUR_INT_RTE,
       ACCRD_INT_MTD_AMT, CUST_CIS_NBR, CST_CNTR_NBR, CAST(NULL AS STRING) AS ST_CDE, BRNCH_NBR,
       CAST(NULL AS STRING) AS ZIP_5_CDE, OFFCR_NBR
FROM {LIN['dp_fact']}
WHERE RPT_FREQ_IND = 'M' AND ACCT_OPEN_IND = '1' AND ACCT_NBR != '01006550204' AND ENTITY != '165.00CLOSE'
""")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW bp_ics_fact AS
SELECT * FROM (
  SELECT right(concat('00000000000', Institution_Transaction_Account_No), 11) AS ACCT_NBR, Effective_Date,
         Principal_Balance, Average_Daily_Balance_MTD, Current_Program_Interest_Rate / 100.0 AS Current_Program_Interest_Rate,
         last_day(Effective_Date) AS PRCS_DTE_EOM
  FROM {LIN['ics']} WHERE Shadow_Account_No IS NOT NULL)
QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCT_NBR, PRCS_DTE_EOM ORDER BY Effective_Date DESC) = 1
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW bp_dp_tran_fact AS
SELECT ACCT_NBR, last_day(PRCS_DTE) AS PRCS_DTE_EOM, SUM(AMOUNT) AS AMOUNT, COUNT(*) AS TXN_COUNT
FROM dp_txn_760 GROUP BY ACCT_NBR, last_day(PRCS_DTE)
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW bp_dp_data_join AS
SELECT f.*,
  COALESCE(f.LGR_BAL_AMT, 0.0) + COALESCE(i.Principal_Balance, 0.0) AS EOPBook,
  IF(TRY_CAST(f.ACCT_TYP_CDE AS INT) IN (450, 451, 452, 453, 454, 455, 456, 457) AND i.Current_Program_Interest_Rate IS NOT NULL,
     i.Current_Program_Interest_Rate, f.CUR_INT_RTE) AS CurrCoupon,
  IF(f.ACCRD_INT_MTD_AMT = 0.0, COALESCE(t.AMOUNT, 0.0), f.ACCRD_INT_MTD_AMT) AS MTDIncome,
  COALESCE(f.AVG_LGR_BAL_MTD_AMT, 0.0) + COALESCE(i.Average_Daily_Balance_MTD, 0.0) AS AvgBook
FROM bp_dp_fact f
LEFT JOIN bp_ics_fact i ON f.ACCT_NBR = i.ACCT_NBR AND f.PRCS_DTE_EOM = i.PRCS_DTE_EOM
LEFT JOIN bp_dp_tran_fact t ON f.ACCT_NBR = t.ACCT_NBR AND f.PRCS_DTE_EOM = t.PRCS_DTE_EOM
""")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW bp_da_classified AS
SELECT j.*, m.entity_lvl,
  CASE WHEN j.INT_PLN_NBR = 82 THEN '082'
       WHEN TRY_CAST(j.BNK_NBR AS INT) = 165 AND j.ACCT_NBR IN ('01002923306', '00009975655') THEN '098'
       WHEN TRY_CAST(j.ACCT_TYP_CDE AS INT) = 450 AND j.CurrCoupon > 0 THEN '099'
       ELSE '000' END AS RateInd,
  CASE WHEN TRY_CAST(j.BNK_NBR AS INT) = 165 AND j.ACCT_NBR IN ('00000056022', '01007815721', '01007815713') THEN '01'
       WHEN j.INT_PLN_NBR IN (119, 120, 121, 122, 123, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 148,
                              149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 165, 169, 175) THEN '01'
       WHEN TRY_CAST(j.ACCT_TYP_CDE AS INT) IN (119, 961) THEN '01'
       ELSE '00' END AS IndexedInd
FROM bp_dp_data_join j LEFT JOIN {LIN['fcb_map']} m ON j.ENTITY = m.entity_id
""")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW da_data AS
WITH b AS (
  SELECT DISTINCT YR_MTH_NBR, concat(substring(BNK_NBR, 1, 3), ACCT_NBR) AS AccountID, ACCT_TYP_CDE,
         concat(ACCT_TYP_CDE, '_', RateInd, '_', IndexedInd) AS SubProductID, EOPBook, CurrCoupon,
         IF(entity_lvl IS NULL, NULL, '1') AS AcquisitionID, TRY_CAST(OPEN_DTE AS DATE) AS OpenDate,
         TRY_CAST(PRCS_DTE_EOM AS DATE) AS EOPDate, TRIM(ENTITY) AS DeptCode, CST_CNTR_NBR AS CostCenter, AvgBook, MTDIncome,
         CUST_CIS_NBR AS CustomerID, right(CST_CNTR_NBR, 4) AS BusinessUnit, ST_CDE AS Region, BRNCH_NBR AS BranchCode,
         ZIP_5_CDE AS ZipCode, concat(substring(BNK_NBR, 3), OFFCR_NBR) AS OfficerCode,
         concat(substring(BNK_NBR, 1, 3), ACCT_NBR) AS InstrumentID, TRY_CAST(PRCS_DTE_EOM AS DATE) AS AsOfDate, BNK_NBR AS LegalEntityID
  FROM bp_da_classified)
SELECT b.*, n.IncludeExclude, n.GL_BS_Acct, n.GL_IS_Acct, n.ALMAccountNum, a.ALMOrgUnitID
FROM b LEFT JOIN {LIN['nmd_map']} n ON b.SubProductID = n.SubProductID
       LEFT JOIN {LIN['alm_org_map']} a ON right(b.DeptCode, 4) = a.DeptCode
""")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW snv_nmd_201701_201901 AS
SELECT EOPDate, CAST(YR_MTH_NBR AS BIGINT) AS YR_MTH_NBR, CAST(AccountID AS BIGINT) AS AccountID,
       CAST(ACCT_TYP_CDE AS BIGINT) AS ACCT_TYP_CDE, SubProductID, EOPBook, CurrCoupon, CAST(AcquisitionID AS BIGINT) AS AcquisitionID,
       OpenDate, concat(substring(LegalEntityID, 1, 3), '_', CostCenter) AS DeptCode, AvgBook, MTDIncome,
       CAST(CustomerID AS BIGINT) AS CustomerID, CAST(BusinessUnit AS BIGINT) AS BusinessUnit, Region,
       CAST(BranchCode AS BIGINT) AS BranchCode, ZipCode, CAST(OfficerCode AS BIGINT) AS OfficerCode,
       CAST(InstrumentID AS BIGINT) AS InstrumentID, AsOfDate, CAST(LegalEntityID AS BIGINT) AS LegalEntityID,
       CAST(GL_BS_Acct AS BIGINT) AS GL_BS_Acct, CAST(GL_IS_Acct AS BIGINT) AS GL_IS_Acct,
       CAST(ALMAccountNum AS BIGINT) AS ALMAccountNum, ALMOrgUnitID,
       concat('{LIN['facts_dir']}dp_fact_', substring(YR_MTH_NBR, 1, 4), '-', substring(YR_MTH_NBR, 5, 2), '.csv') AS file_path,
       current_timestamp() AS upload_timestamp
FROM da_data
WHERE IncludeExclude = 'INC' AND CAST(YR_MTH_NBR AS BIGINT) BETWEEN {DP_FIRST_MONTH} AND {HISTORY_FIRST_MONTH}
""")
snv_dp = spark.table("snv_nmd_201701_201901")
check("A4", "Columns and types match lsnv_nmds", [(f.name, f.dataType.simpleString()) for f in snv_dp.schema.fields] == LSNV_SCHEMA)
display(snv_dp.groupBy("YR_MTH_NBR").agg(F.count("*").alias("rows"), F.countDistinct("AccountID").alias("accounts"),
                                         F.round(F.sum("EOPBook"), 2).alias("EOPBook"),
                                         F.round(F.sum(F.col("EOPBook") * F.col("CurrCoupon")) / F.sum("EOPBook"), 6).alias("war"))
        .orderBy("YR_MTH_NBR"))

# COMMAND ----------

# DBTITLE 1,B. History CSVs -> snv_nmd_history, vs lsnv_nmds
RENAMES = {"CurrBook": "EOPBook", "IssueDate": "OpenDate", "InstrumentID_in": "InstrumentID",
           "GLAccount": "GL_BS_Acct", "AccountNum": "ALMAccountNum"}

def to_type(c, t):
    col = F.col(c)
    if t == "date":
        return F.coalesce(F.try_to_date(col, F.lit("yyyy-M-d")), F.try_to_date(col, F.lit("M/d/yyyy")))
    if t == "bigint":
        return F.coalesce(col.try_cast("bigint"), col.try_cast("double").try_cast("bigint"))
    return col.try_cast(t)

parts = []
for f in hist_files:
    path = os.path.join(LIN["history_dir"], f)
    with open(path, encoding="utf-8-sig") as fh:
        header = [h.strip() for h in fh.readline().rstrip("\r\n").split(",")]
    renamed = [RENAMES.get(h, h) if RENAMES.get(h) not in header else h for h in header]
    df = spark.read.schema(StructType([StructField(h, StringType()) for h in renamed])).option("header", "true").csv(path)
    parts.append(df.select(*[to_type(c, t).alias(c) if c in renamed else F.lit(None).cast(t).alias(c) for c, t in LSNV_SCHEMA[:-2]],
                           F.lit(path).alias("file_path"), F.current_timestamp().alias("upload_timestamp")))
hist_df = parts[0]
for p in parts[1:]:
    hist_df = hist_df.unionByName(p)
hist_df.createOrReplaceTempView("snv_nmd_history")

lsnv_now = spark.table(LSNV).filter(F.col("YR_MTH_NBR") <= END_YM)
b_cmp = compare_by_month(hist_df, lsnv_now.filter(F.col("YR_MTH_NBR") >= HISTORY_FIRST_MONTH), LSNV_DATA_COLS, "files", "lsnv")
display(b_cmp)
bs = b_cmp.agg(F.count_if(F.col("identical")).alias("same"), F.count_if(F.col("lsnv_rows").isNull()).alias("not_in_table"),
               F.count_if(F.col("files_rows").isNull()).alias("no_file"),
               F.count_if(~F.col("identical") & F.col("lsnv_rows").isNotNull() & F.col("files_rows").isNotNull()).alias("different")).first()
check("B", "History files reproduce lsnv_nmds month by month", bs.different == 0 and bs.no_file == 0,
      f"identical {bs.same}, different {bs.different}, file missing {bs.no_file}, not yet in lsnv_nmds {bs.not_in_table}")

hq = (hist_df.groupBy("YR_MTH_NBR").agg(F.count("*").alias("rows"), F.countDistinct("AccountID").alias("accounts"),
                                        F.count_if(F.col("AccountID").isNull()).alias("null_accounts"))
      .filter((F.col("rows") != F.col("accounts")) | (F.col("null_accounts") > 0)))
bad = [r.YR_MTH_NBR for r in hq.collect()]
check("B", "One row per account, no broken AccountIDs", not bad, f"bad months: {bad}")

# 2019-01 is in both the DP build and the history file
dp_1901 = snv_dp.filter(F.col("YR_MTH_NBR") == HISTORY_FIRST_MONTH).select("AccountID", "DeptCode")
hi_1901 = hist_df.filter(F.col("YR_MTH_NBR") == HISTORY_FIRST_MONTH).select("AccountID", "DeptCode")
od, oh = dp_1901.exceptAll(hi_1901).count(), hi_1901.exceptAll(dp_1901).count()
check("A4", "2019-01: DP build = history file (accounts + DeptCode)", od == 0 and oh == 0, f"only DP build {od}, only history {oh}")

# COMMAND ----------

# DBTITLE 1,PNFP. WAL Deps CSVs vs lpnfp_dp_history and pnfp_deposits
PNFP_DATE_COLS = ["date7_opened", "first_dep", "load_date"]
PNFP_BALANCE_COLS = ["ledger", "avgbal", "current_balance", "accrued_interest"]

def pnfp_convert(c):
    if c in PNFP_DATE_COLS:
        return F.try_to_date(F.col(c), F.lit("M/d/yy"))
    if c in PNFP_BALANCE_COLS:
        return F.col(c).try_cast("double")
    if c == "rate":
        return F.col(c).try_cast("decimal(10,6)")
    return F.col(c)

headers = set()
for f in pnfp_files:
    with open(os.path.join(LIN["pnfp_dir"], f), encoding="utf-8-sig") as fh:
        fh.readline()                                     # line 1 is a dummy _c0,_c1,... line
        headers.add(fh.readline().rstrip("\r\n"))
check("PNFP", "All files share one header", len(headers) == 1, f"{len(headers)} headers")
pnfp_cols = sorted(headers)[0].split(",")
pnfp_txt = (spark.read.schema(StructType([StructField(c, StringType()) for c in pnfp_cols])).option("header", "false")
            .csv([os.path.join(LIN["pnfp_dir"], f) for f in pnfp_files])
            .filter(~F.col("account_number").isin("_c0", "account_number"))
            .withColumn("file_date", F.to_date(F.regexp_extract(F.col("_metadata.file_name"), r"(\d{8})\.csv$", 1), "yyyyMMdd"))
            .withColumn("file_name", F.url_decode(F.col("_metadata.file_name"))))
pnfp_df = pnfp_txt.select(*[pnfp_convert(c).alias(c) for c in pnfp_cols],
                          F.date_format("file_date", "yyyyMM").cast("int").alias("YR_MTH_NBR"), "file_name")
pnfp_df.createOrReplaceTempView("pnfp_files")

pm = (pnfp_txt.groupBy(F.date_format("file_date", "yyyyMM").cast("int").alias("YR_MTH_NBR"))
      .agg(F.count("*").alias("rows"), F.countDistinct("account_number", "account_type").alias("keys"),
           F.count_if(pnfp_convert("load_date") != F.col("file_date")).alias("load_date_not_file_date"),
           F.count_if(F.col("account_number").rlike("[Ee+]")).alias("sci_notation_accounts"),
           F.round(F.sum(pnfp_convert("ledger")), 2).alias("file_ledger")))
ps = pm.agg(F.count_if(F.col("rows") != F.col("keys")).alias("dup"), F.sum("load_date_not_file_date").alias("ld"),
            F.sum("sci_notation_accounts").alias("sci")).first()
check("PNFP", "One row per account + type, load_date = file date", ps.dup == 0 and ps.ld == 0,
      f"months with duplicate keys {ps.dup}, load_date mismatches {ps.ld}")
check("PNFP", "Account numbers not in scientific notation (known source issue)", ps.sci == 0, f"{ps.sci} account numbers lost")

# vs the loaded history table (hash of every converted column)
if spark.catalog.tableExists(LIN["pnfp_history"]):
    cols = [c for c in pnfp_cols]
    p_cmp = compare_by_month(pnfp_df, spark.table(LIN["pnfp_history"]).filter(F.col("YR_MTH_NBR") <= PNFP_END_YM), cols, "files", "table")
    n_diff = p_cmp.filter(~F.col("identical")).count()
    check("PNFP", f"Files identical to {LIN['pnfp_history']}", n_diff == 0, f"{n_diff} months differ")

# vs the study's PNFP source (different build: rows and ledger by month, for information)
src_m = (spark.table(PNFP_SRC).groupBy(F.date_format("load_date", "yyyyMM").cast("int").alias("YR_MTH_NBR"))
         .agg(F.count("*").alias("src_rows"), F.round(F.sum("ledger"), 2).alias("src_ledger")))
pnfp_vs_src = (pm.join(src_m, "YR_MTH_NBR", "full_outer")
               .withColumn("ledger_diff", F.round(F.col("file_ledger") - F.col("src_ledger"), 2)).orderBy("YR_MTH_NBR"))
display(pnfp_vs_src)

# COMMAND ----------

# DBTITLE 1,Summary: checks, report, known issues
chk = pd.DataFrame(CHECKS, columns=["part", "check", "passed", "detail"])
display(chk)
chk.to_csv(out("reports", "source_lineage", "csv"), index=False)

def md_table(df):
    cols = list(df.columns)
    rows = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    rows += ["| " + " | ".join("" if pd.isna(v) else str(v) for v in r) + " |" for r in df.itertuples(index=False)]
    return "\n".join(rows)

snv_months = b_cmp.toPandas()
pnfp_months = pnfp_vs_src.toPandas()
md = [f"# Source lineage {S.tag}", "",
      f"Check only (no table written). End month: SNV {END_YM}, PNFP {PNFP_END_YM} (`dp_history_end_date` = {_end or 'latest'}).", "",
      "| Part | Check | Passed | Detail |", "|---|---|---|---|"]
md += [f"| {r.part} | {r.check} | {'yes' if r.passed else '**no**'} | {r.detail.replace('|', '/')} |" for r in chk.itertuples()]
md += ["", "## SNV history files vs lsnv_nmds", "", md_table(snv_months), "",
       "## PNFP files vs the study source (rows, ledger)", "", md_table(pnfp_months)]
rp = out("reports", "source_lineage", "md")
rp.write_text("\n".join(md) + "\n", encoding="utf-8")
print("Written:", rp, "and", out("reports", "source_lineage", "csv"))

log_issues("00a lineage", [("LINEAGE_CHECK_FAILED", "HIGH" if r.part in ("A1", "B") else "MEDIUM", f"{r.part}: {r.check}", r.detail, "", "")
                           for r in chk.itertuples() if not r.passed])

# COMMAND ----------

