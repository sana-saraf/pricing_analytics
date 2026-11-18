"""Controlled promotion of sandbox tables to finance.pricing_analytics (or any target).

Flow
----
1. Edit inputs/config/promotion.csv (set include = Y for the tables you want to promote).
2. ``plan = dry_run(study)`` shows row counts, schema match and any blocking issue. Nothing is written.
3. ``execute(study, confirm="PROMOTE <tag>")`` backs up each existing target into the
   sandbox schema, then writes. Every attempt is appended to outputs/logs/promotion_log.csv.

Guards
------
* Only rows with include = Y are touched.
* Target must be a three-part name and must not be inside the sandbox schema.
* The confirm text must match exactly, so a stray "Run all" cannot overwrite production.
* mode = replace  -> CREATE OR REPLACE TABLE target AS SELECT * (same as the original notebooks)
  mode = overwrite -> INSERT OVERWRITE (fails if the schema differs; use when you want that safety)
"""
from __future__ import annotations
import csv
from datetime import datetime
from pathlib import Path

import pandas as pd

PLAN_COLS = ["sandbox_table", "target_table", "include", "mode", "notes"]
LOG_COLS = ["timestamp", "study", "user", "sandbox_table", "target_table", "mode", "rows_written", "backup", "status", "detail"]


def load_plan(study) -> pd.DataFrame:
    df = study.read_input("config/promotion.csv")
    missing = [c for c in PLAN_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"promotion.csv missing columns: {missing}")
    df = df.copy()
    df["include"] = df["include"].str.strip().str.upper()
    df["mode"] = df["mode"].str.strip().str.lower().replace("", "replace")
    df["source_table"] = df["sandbox_table"].map(lambda n: study.table(n.strip()))
    return df


def _count(spark, name):
    try:
        return spark.table(name).count()
    except Exception:
        return None


def _schema(spark, name):
    try:
        return [(f.name.lower(), f.dataType.simpleString()) for f in spark.table(name).schema.fields]
    except Exception:
        return None


def dry_run(study) -> pd.DataFrame:
    """Row counts, schema comparison and blocking issues. Writes nothing."""
    spark, plan, rows = study.spark, load_plan(study), []
    for _, r in plan.iterrows():
        issues = []
        src, tgt = r["source_table"], r["target_table"].strip()
        if len(tgt.split(".")) != 3:
            issues.append("target must be catalog.schema.table")
        if tgt.lower().startswith(study.sandbox_schema.lower() + "."):
            issues.append("target is inside the sandbox schema")
        if r["mode"] not in ("replace", "overwrite"):
            issues.append(f"unknown mode '{r['mode']}'")
        src_rows = _count(spark, src)
        if src_rows is None:
            issues.append("sandbox table does not exist (run the build notebooks)")
        elif src_rows == 0:
            issues.append("sandbox table is empty")
        tgt_rows = _count(spark, tgt)
        s_sch, t_sch = _schema(spark, src), _schema(spark, tgt)
        if s_sch and t_sch:
            match = s_sch == t_sch
            if not match and r["mode"] == "overwrite":
                issues.append("schema differs from target (overwrite will fail)")
        else:
            match = None
        rows.append(dict(sandbox_table=r["sandbox_table"], include=r["include"], mode=r["mode"],
                         source=src, target=tgt, source_rows=src_rows, target_rows=tgt_rows,
                         row_change=(src_rows - tgt_rows) if (src_rows is not None and tgt_rows is not None) else None,
                         schema_matches_target=match, issues="; ".join(issues), notes=r["notes"]))
    return pd.DataFrame(rows)


def _log(study, rec: dict):
    p = study.output("logs", "promotion_log", "csv")
    new = not p.exists()
    with open(p, "a", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=LOG_COLS)
        if new:
            w.writeheader()
        w.writerow({k: rec.get(k, "") for k in LOG_COLS})


def execute(study, confirm: str, backup: bool = True, log=print) -> pd.DataFrame:
    """Promote every row with include = Y. Requires confirm == 'PROMOTE <tag>'."""
    expected = f"PROMOTE {study.tag}"
    if confirm != expected:
        raise PermissionError(f"Confirmation text must be exactly '{expected}'. Nothing was written.")
    spark = study.spark
    plan = dry_run(study)
    todo = plan[plan["include"] == "Y"]
    if todo.empty:
        log("No rows have include = Y in promotion.csv. Nothing to do.")
        return plan.iloc[0:0]
    blocked = todo[todo["issues"] != ""]
    if len(blocked):
        raise RuntimeError("Blocking issues, nothing written:\n" + blocked[["sandbox_table", "issues"]].to_string(index=False))

    results, stamp = [], datetime.now().strftime("%Y%m%d%H%M")
    for _, r in todo.iterrows():
        src, tgt = r["source"], r["target"]
        rec = dict(timestamp=datetime.now().isoformat(timespec="seconds"), study=study.tag,
                   user=_user(spark), sandbox_table=r["sandbox_table"], target_table=tgt, mode=r["mode"])
        bak = ""
        try:
            if backup and r["target_rows"] is not None:
                bak = f"{study.sandbox_schema}.backup_{tgt.split('.')[-1]}_{stamp}"
                spark.sql(f"CREATE OR REPLACE TABLE {bak} DEEP CLONE {tgt}")
                log(f"  backup {tgt} -> {bak}")
            if r["mode"] == "overwrite":
                spark.sql(f"INSERT OVERWRITE {tgt} SELECT * FROM {src}")
            else:
                spark.sql(f"CREATE OR REPLACE TABLE {tgt} AS SELECT * FROM {src}")
            n = _count(spark, tgt)
            rec.update(rows_written=n, backup=bak, status="ok")
            log(f"  promoted {src} -> {tgt} ({n:,} rows)")
        except Exception as e:
            rec.update(backup=bak, status="FAILED", detail=str(e)[:400])
            log(f"  FAILED {src} -> {tgt}: {e}")
        _log(study, rec)
        results.append(rec)
    return pd.DataFrame(results)


def _user(spark) -> str:
    try:
        return spark.sql("SELECT current_user()").collect()[0][0]
    except Exception:
        return ""


def restore_command(backup_table: str, target_table: str) -> str:
    """SQL to put a backup back (run it yourself after checking)."""
    return f"CREATE OR REPLACE TABLE {target_table} AS SELECT * FROM {backup_table}"
