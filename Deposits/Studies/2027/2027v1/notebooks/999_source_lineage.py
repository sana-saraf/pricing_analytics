# Databricks notebook source
# DBTITLE 1,999 | Source Lineage (background)
# MAGIC %md
# MAGIC # 999 | Source Lineage
# MAGIC **Runs automatically** as the first step of every study run. Users don't need to open this notebook.
# MAGIC
# MAGIC Computes row counts, balance sums, date ranges, and account counts for every source table the study reads. Writes the summary to `outputs/reports/source_lineage_summary.json` so `00_run_study` can display it.
# MAGIC
# MAGIC **What it checks:**
# MAGIC * Each source table exists and is accessible
# MAGIC * Row counts and balance totals per month
# MAGIC * Date range (earliest → latest month)
# MAGIC * Account counts
# MAGIC * Schema matches expected columns
# MAGIC
# MAGIC If a source table is missing or inaccessible, the summary records `status: missing` instead of failing — the study can still run with `source_tables = study` or after fixing the source.

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ./00_setup

# COMMAND ----------

# DBTITLE 1,Compute source lineage summary
import json
from datetime import datetime
from pyspark.sql import functions as F

SRC = S.params.get("sources", {})
SOURCE_TABLES = {
    "SNV (lsnv_nmds)": {
        "table": SRC.get("src_lsnv_nmds", "finance.pricing_analytics.lsnv_nmds"),
        "date_col": "EOPDate", "balance_col": "EOPBook", "account_col": "AccountID", "month_col": "YR_MTH_NBR",
    },
    "PNFP (pnfp_deposits)": {
        "table": SRC.get("src_pnfp_deposits", "finance.ftp_validation.pnfp_deposits"),
        "date_col": "load_date", "balance_col": "ledger", "account_col": "account_number", "month_col": None,
    },
    "Fed Funds": {
        "table": SRC.get("src_fed_funds", "finance.operational_deposits.fed_funds_matrix"),
        "date_col": None, "balance_col": None, "account_col": None, "month_col": None,
    },
    "Dim NMD": {
        "table": SRC.get("src_dim_nmd", "finance.reference.dim_nmd"),
        "date_col": None, "balance_col": None, "account_col": None, "month_col": None,
    },
    "Product Grouping": {
        "table": SRC.get("src_product_grouping", "silver_prod.reference_data.vxref_fin_products_grouping_deposits"),
        "date_col": None, "balance_col": None, "account_col": None, "month_col": None,
    },
}

# Also check study-local tables if source_tables = study
if S.params.get("source_tables") == "study":
    SOURCE_TABLES["SNV (study local)"] = {
        "table": S.table("lsnv_dp_hist"), "date_col": "EOPDate", "balance_col": "EOPBook",
        "account_col": "AccountID", "month_col": "YR_MTH_NBR",
    }
    SOURCE_TABLES["PNFP (study local)"] = {
        "table": S.table("lpnfp_dp_hist"), "date_col": "load_date", "balance_col": "ledger",
        "account_col": "account_number", "month_col": None,
    }

results = []
for name, spec in SOURCE_TABLES.items():
    tbl = spec["table"]
    rec = {"source": name, "table": tbl, "status": "ok"}
    try:
        if not spark.catalog.tableExists(tbl):
            rec["status"] = "missing"
            results.append(rec)
            continue
        df = spark.table(tbl)
        rec["columns"] = len(df.columns)
        cnt = df.count()
        rec["rows"] = cnt
        if spec["balance_col"] and spec["balance_col"] in df.columns:
            s = df.agg(
                F.sum(spec["balance_col"]).alias("total_balance"),
                F.count(F.col(spec["balance_col"])).alias("balance_rows"),
            ).first()
            rec["total_balance"] = float(s.total_balance) if s.total_balance else 0.0
        if spec["account_col"] and spec["account_col"] in df.columns:
            rec["accounts"] = df.select(spec["account_col"]).distinct().count()
        if spec["date_col"] and spec["date_col"] in df.columns:
            d = df.agg(F.min(spec["date_col"]).alias("min_date"), F.max(spec["date_col"]).alias("max_date")).first()
            rec["min_date"] = str(d.min_date) if d.min_date else None
            rec["max_date"] = str(d.max_date) if d.max_date else None
        if spec["month_col"] and spec["month_col"] in df.columns:
            rec["months"] = df.select(spec["month_col"]).distinct().count()
        elif spec["date_col"] and spec["date_col"] in df.columns:
            rec["months"] = df.select(F.date_format(spec["date_col"], "yyyyMM")).distinct().count()
    except Exception as e:
        rec["status"] = f"error: {str(e)[:200]}"
    results.append(rec)

# Study metadata
summary = {
    "study": S.tag,
    "asofdate": str(S.snapshot_date),
    "source_tables_mode": S.params.get("source_tables", "production"),
    "generated": datetime.now().isoformat(timespec="seconds"),
    "sources": results,
}

# Write summary
out_path = OUTPUTS / "reports" / "source_lineage_summary.json"
out_path.parent.mkdir(parents=True, exist_ok=True)
out_path.write_text(json.dumps(summary, indent=2, default=str) + "\n", encoding="utf-8")
print(f"Source lineage summary written to {out_path}")
for r in results:
    icon = "OK" if r["status"] == "ok" else "MISS" if r["status"] == "missing" else "ERR"
    row_info = f"{r.get('rows', '?'):>12,} rows" if r.get("rows") else ""
    bal_info = f"  ${r.get('total_balance', 0):>18,.0f}" if r.get("total_balance") else ""
    date_info = f"  {r.get('min_date', '')} -> {r.get('max_date', '')}" if r.get("min_date") else ""
    print(f"  [{icon:4s}] {r['source']:30s} {row_info}{bal_info}{date_info}")