"""Spark-side helpers: load the version's CSV inputs as temp views / sandbox tables.

All inputs are read as strings (so codes like '025_000_00' and '083' are never mangled
by type inference) and cast explicitly where the original SQL used numbers.
"""
from __future__ import annotations
import pandas as pd

from .study import DIM_NMD_REL


def pandas_to_spark(spark, pdf: pd.DataFrame, types: dict | None = None, blank_to_null: bool = False):
    """All-string DataFrame, then cast the columns listed in ``types`` ({col: 'int'|'double'|...})."""
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    df = pdf.copy()
    if blank_to_null:
        df = df.apply(lambda s: s.map(lambda v: None if (v is None or str(v).strip() == "") else v))
    df = df.astype(object).where(df.notna(), None)
    schema = T.StructType([T.StructField(c, T.StringType(), True) for c in df.columns])
    sdf = spark.createDataFrame([tuple(r) for r in df.itertuples(index=False, name=None)], schema)
    for c, t in (types or {}).items():
        sdf = sdf.withColumn(c, F.trim(F.col(c)).cast(t))
    return sdf


def csv_to_view(study, rel: str, view: str, types: dict | None = None, blank_to_null: bool = False, log=print):
    """Load inputs/<rel> as temp view ``view``. Returns the Spark DataFrame."""
    pdf = study.read_input(rel)
    sdf = pandas_to_spark(study.spark, pdf, types, blank_to_null)
    sdf.createOrReplaceTempView(view)
    log(f"  loaded {rel} -> view {view} ({len(pdf):,} rows)")
    return sdf


def seed_dim_nmd_csv(study, log=print):
    """Make sure inputs/product_maps/dim_nmd.csv exists. First version: export production dim_nmd."""
    p = study.root / DIM_NMD_REL          # DIM_NMD_REL already starts with inputs/
    if p.exists():
        log(f"  dim_nmd.csv present ({p})")
        return p
    src = study.params["sources"]["src_dim_nmd"]
    log(f"  dim_nmd.csv missing, exporting {src}")
    pdf = study.spark.table(src).toPandas()
    p.parent.mkdir(parents=True, exist_ok=True)
    pdf.to_csv(p, index=False, encoding="utf-8-sig")
    log(f"  wrote {p} ({len(pdf):,} rows). Edit this file to test a different product mapping.")
    return p


def load_dim_nmd_table(study, log=print):
    """Load dim_nmd.csv into the version's sandbox dim_nmd table, keeping production column types."""
    from pyspark.sql import functions as F

    spark, tbl = study.spark, study.table("dim_nmd")
    src = study.params["sources"]["src_dim_nmd"]
    spark.sql(f"CREATE TABLE IF NOT EXISTS {tbl} AS SELECT * FROM {src} LIMIT 0")   # structure only
    schema = spark.table(tbl).schema
    pdf = study.read_input("product_maps/dim_nmd.csv")
    unknown = [c for c in pdf.columns if c not in schema.fieldNames()]
    if unknown:
        raise ValueError(f"dim_nmd.csv has columns not in {src}: {unknown}")
    sdf = pandas_to_spark(spark, pdf, blank_to_null=True)
    for f in schema.fields:
        if f.name in sdf.columns:
            sdf = sdf.withColumn(f.name, F.col(f.name).cast(f.dataType))
        else:
            sdf = sdf.withColumn(f.name, F.lit(None).cast(f.dataType))
    sdf.select(*schema.fieldNames()).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl)
    n = spark.table(tbl).count()
    log(f"  {tbl} loaded from dim_nmd.csv ({n:,} rows)")
    return n


def persist_view(study, view: str, table_key: str, log=print):
    """CREATE OR REPLACE sandbox table <table_key>_<tag> AS SELECT * FROM <view>."""
    tbl = study.table(table_key)
    study.spark.sql(f"CREATE OR REPLACE TABLE {tbl} AS SELECT * FROM {view}")
    n = study.spark.table(tbl).count()
    log(f"  saved {tbl} ({n:,} rows)")
    return tbl


def alias_view(study, view: str, table_key: str):
    """Temp view <view> over the sandbox table, so cells written against the view keep working."""
    study.spark.sql(f"CREATE OR REPLACE TEMP VIEW {view} AS SELECT * FROM {study.table(table_key)}")


def create_union_view(spark, schema: str, base: str, tags, view_name: str | None = None, log=print):
    """Persistent view that stacks <schema>.<base>_<tag> for every tag that exists.

    Adds a ``study`` column so versions can be compared side by side in SQL, a
    Databricks SQL dashboard or Genie. Only columns common to all tables are used.
    """
    view_name = view_name or f"{base}_all_versions"
    present = [t for t in tags if spark.catalog.tableExists(f"{schema}.{base}_{t}")]
    if not present:
        log(f"  no {base}_<tag> tables found, view not created")
        return None
    cols = None
    for t in present:
        c = [f.name for f in spark.table(f"{schema}.{base}_{t}").schema.fields]
        cols = c if cols is None else [x for x in cols if x in c]
    sel = ", ".join(f"`{c}`" for c in cols)
    sql = "\nUNION ALL\n".join(f"SELECT '{t}' AS study, {sel} FROM {schema}.{base}_{t}" for t in present)
    spark.sql(f"CREATE OR REPLACE VIEW {schema}.{view_name} AS\n{sql}")
    log(f"  view {schema}.{view_name} covers {len(present)} version(s): {', '.join(present)}")
    return f"{schema}.{view_name}"
