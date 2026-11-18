# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 00 | Setup (run by every notebook with `%run ./00_setup`)
# MAGIC
# MAGIC Nothing to edit here. This notebook:
# MAGIC
# MAGIC 1. Finds this MV study folder (the one holding `mv_study.json`) and the study version it was exported from.
# MAGIC 2. Loads the version (`S`) for tables and tokens, and lets this folder's `config/study_params.json` override the dates and sources.
# MAGIC 3. Reads `config/mv_study_config.xlsx` (method settings, cycles, overrides) and `config/rate_cycles.csv`.
# MAGIC 4. Defines the helpers every step uses: Excel writer, table writer, the known-issues log and the beta engine (`beta_grid`, `recommend`).
# MAGIC
# MAGIC | Folder | What lives there |
# MAGIC |---|---|
# MAGIC | `config/` | `mv_study_config.xlsx`, `study_params.json`, `rate_cycles.csv` |
# MAGIC | `inputs/reference/` | The originals from the version (`dim_nmd.csv`, `mv_config.xlsx`), copied at export |
# MAGIC | `inputs/` | Files the notebooks build from scratch: account mapping, account metrics, product assignment |
# MAGIC | `notebooks/` | 00 setup, 00 run study, 01 to 06 |
# MAGIC | `outputs/excel`, `outputs/html`, `outputs/reports` | Workbooks, the HTML dashboard, the MV report and the known-issues log |

# COMMAND ----------

# MAGIC %pip install openpyxl 
# MAGIC # DBTITLE 1,Install the deposit_utils library
# MAGIC # This is a one-time install for the clust

# COMMAND ----------

# DBTITLE 1,Find the study folder and load the study
# Finds this study folder (study.json; older MV studies also carry mv_study.json), the shared code and the study.
import os, sys, json, re
from pathlib import Path
import numpy as np
import pandas as pd

def _starts():
    s = [os.getcwd()]
    try:
        nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        s.insert(0, nb if nb.startswith("/Workspace") else "/Workspace" + nb)
    except Exception:
        pass
    return s

def _find_up(marker):
    for s in _starts():
        for q in [Path(s), *Path(s).parents]:
            if (q / marker).exists():
                return q
    return None

STUDY_ROOT = _find_up("study.json") or _find_up("mv_study.json")
if STUDY_ROOT is None:
    raise FileNotFoundError("study.json not found above this notebook. These notebooks run inside a study folder "
                            "(create one with studies/install/execute_study). The template folder itself does not run.")
MV_ROOT = STUDY_ROOT                                          # older cells use MV_ROOT
STUDIES_ROOT = _find_up("_shared/deposit_utils")
sys.path.insert(0, str(STUDIES_ROOT / "_shared"))
from deposit_utils.study import Study, MV_TABLES as _MV_TABLES
from deposit_utils import folders
from deposit_utils import betas as B

_sj = json.loads((STUDY_ROOT / "study.json").read_text(encoding="utf-8")) if (STUDY_ROOT / "study.json").exists() else {}
_mj = json.loads((STUDY_ROOT / "mv_study.json").read_text(encoding="utf-8")) if (STUDY_ROOT / "mv_study.json").exists() else {}
MANIFEST = {**_sj, **_mj}
MANIFEST.setdefault("source_version", _sj.get("study_name"))
S = Study.load(spark=spark, dbutils=dbutils, root=folders.tag_path(STUDIES_ROOT, MANIFEST["source_version"]))

# this folder's copy of study_params.json wins (snapshot date, source tables)
_p = json.loads((MV_ROOT / "config" / "study_params.json").read_text(encoding="utf-8"))
S.params["sources"].update(_p.pop("sources", {}))
S.params.update({k: v for k, v in _p.items() if not k.startswith("_")})
S.params["ftp_results_date"] = S.params.get("ftp_results_date") or S.params["snapshot_date"]

sql = S.sql                                                   # sql("...{{tokens}}...") runs several statements
sqlq = lambda text: S.spark.sql(S.render(text))               # returns a DataFrame

CONFIG, INPUTS, OUTPUTS = MV_ROOT / "config", MV_ROOT / "inputs", MV_ROOT / "outputs"
REFERENCE = INPUTS / "reference"
for _d in [CONFIG, INPUTS, REFERENCE, OUTPUTS / "excel", OUTPUTS / "html", OUTPUTS / "reports"]:
    _d.mkdir(parents=True, exist_ok=True)

NAMES = {}                                                    # output_names tab of the config workbook (filled in the next cell)

def out_name(stem):
    """The name chosen for an output in config / output_names. Always carries the tag ({tag} placeholder, or _<tag> added)."""
    n = str(NAMES.get(stem) or stem).strip()
    return n.replace("{tag}", S.tag) if "{tag}" in n else f"{n}_{S.tag}"

def inp(stem, ext="xlsx"):
    """inputs/<name>.<ext>: a file one notebook builds and the next one reads."""
    return INPUTS / f"{out_name(stem)}.{ext}"

def out(kind, stem, ext):
    """outputs/<excel|html|reports>/<name>.<ext>"""
    assert kind in ("excel", "html", "reports"), kind
    return OUTPUTS / kind / f"{out_name(stem)}.{ext}"

# COMMAND ----------

# DBTITLE 1,Config workbook and rate cycles
# config/study_config.xlsx (older studies: mv_study_config.xlsx): params, cycles, final_overrides, output_names.
CFG_PATH = next((CONFIG / f for f in ("study_config.xlsx", "mv_study_config.xlsx") if (CONFIG / f).exists()), CONFIG / "study_config.xlsx")
CFG_TABS = ["README", "params", "cycles", "final_overrides", "output_names"]

def read_cfg():
    raw = pd.read_excel(CFG_PATH, sheet_name=None, dtype=str, keep_default_na=False)
    return {k: raw.get(k, pd.DataFrame()) for k in CFG_TABS}

cfg = read_cfg()
P = dict(zip(cfg["params"]["key"], cfg["params"]["value"]))

# ---- output names: every table and file the study writes, named in config / output_names -----------------
# key = the name the code uses; name = what gets written (blank = key). File names always carry the tag.
OUTPUT_KEYS = [
    # ---- tables (12, unchanged) ----
    ("mv_accounts", "table", "01", "Accounts at the snapshot"),
    ("mv_account_war", "table", "01", "Monthly balance and WAR by subproduct"),
    ("mv_account_metrics", "table", "02", "Subproduct metrics at the snapshot"),
    ("mv_account_betas", "table", "03", "Betas by subproduct"),
    ("mv_product_selection", "table", "04", "Subproduct -> product assignment"),
    ("mv_product_map", "table", "05", "Accounts with their product"),
    ("mv_product_war", "table", "05", "Monthly balance and WAR by product"),
    ("mv_product_betas", "table", "05", "Betas by product"),
    ("mv_beta_review", "table", "06", "Recommended method by product"),
    ("mv_final_betas", "table", "06", "Final betas by product"),
    # ---- inputs (3, unchanged) ----
    ("account_mapping", "inputs", "01", "Account mapping workbook"),
    ("account_metrics", "inputs", "02", "Account metrics workbook"),
    ("product_assignment", "inputs", "04", "Product assignment workbook"),
    # ---- excel (8, consolidated from 12) ----
    ("account_betas", "excel", "03", "Subproduct betas workbook"),
    ("Account_Review", "excel", "03a", "Account Review: model config + accounts summary + account review (merged from 01 accounts)"),
    ("beta_review", "review", "06", "Beta review workbook"),
    ("Beta_Report", "excel", "08", "Beta Report: styled report + product map + product betas + final betas (merged from 05, 06)"),
    ("mv_summary", "excel", "06", "Study summary workbook"),
    ("Legacy_Assumptions", "excel", "06a", "Legacy Assumptions workbook"),
    ("wal", "excel", "15", "WAL and FTP workbook"),
    ("wal_review", "review", "15a", "WAL review workbook (InputsToDecide: pick the management WAL per product)"),
    # ---- html (6, unchanged) ----
    ("Account_Review_Dashboard", "html", "03a", "Account review dashboard"),
    ("mv_dashboard", "html", "06", "Study dashboard"),
    ("Fed_Funds_Cycles", "html", "08", "Fed funds cycles chart"),
    ("Product_Dashboard", "html", "08", "Product dashboard"),
    ("Product_Mapping_Dashboard", "html", "08", "Product mapping dashboard"),
    ("mv_flow", "html", "07", "Study flow page (HTML)"),
    # ---- reports (2, consolidated from 4) ----
    ("source_lineage", "reports", "999", "Source data lineage summary (999_source_lineage)"),
    ("deposit_study_overview", "reports", "07", "Deposit Study Overview: known issues + study report combined"),
    # ---- review (4) ----
    ("beta_review_wb", "review", "06b", "Beta review workbook (InputsToDecide: pick the management beta per product)"),
    ("warf_review_wb", "review", "12b", "WARF (Core Ratio) review workbook (InputsToDecide: pick the core ratio per product)"),
    ("warf_review", "review", "14", "WARF review workbook"),
    ("wal_trunc_review", "review", "16", "WAL & Truncation review workbook"),
    # ---- package (1, unchanged) ----
    ("deposit_study_package", "folder", "09", "Input files + mapping + betas + results + R script (outputs/<name>)"),
]
_DEFAULT_NAME = {"deposit_study_package": "{tag} Deposit Study"}

def _write_output_names(path, rows):
    from openpyxl import load_workbook
    wb = load_workbook(path)
    ws = wb["output_names"] if "output_names" in wb.sheetnames else wb.create_sheet("output_names")
    ws.delete_rows(1, ws.max_row)
    ws.append(["key", "kind", "notebook", "description", "name"])
    for r in rows:
        ws.append(list(r))
    for col, w in zip("ABCDE", (26, 14, 10, 60, 34)):
        ws.column_dimensions[col].width = w
    wb.save(path)

_on = cfg["output_names"]
if _on.empty or "key" not in _on.columns:
    # first run on an older study: add the tab with today's names, so you can rename from here on
    _write_output_names(CFG_PATH, [(k, kd, nb, d, _DEFAULT_NAME.get(k, k)) for k, kd, nb, d in OUTPUT_KEYS])
    cfg = read_cfg()
    _on = cfg["output_names"]
else:
    _missing = [r for r in OUTPUT_KEYS if r[0] not in set(_on["key"])]
    if _missing:
        _write_output_names(CFG_PATH, [tuple(x) for x in _on[["key", "kind", "notebook", "description", "name"]].values.tolist()]
                            + [(k, kd, nb, d, _DEFAULT_NAME.get(k, k)) for k, kd, nb, d in _missing])
        cfg = read_cfg()
        _on = cfg["output_names"]
NAMES.update({k: (str(n).strip() or k) for k, n in zip(_on["key"], _on["name"])})

# tables: S.table(key) and the {{key}} SQL tokens resolve to <sandbox>.<name>_<tag>
_TABLE_KEYS = {k for k, kd, _, _ in OUTPUT_KEYS if kd == "table"}
def _table(name, _S=S):
    base = NAMES.get(name, name) if name in _TABLE_KEYS else name
    return f"{_S.sandbox_schema}.{base}_{_S.tag}"
S.table = _table
PACKAGE_DIR = OUTPUTS / out_name("deposit_study_package")    # e.g. outputs/2027v10 Deposit Study
_csv = lambda v: [x.strip() for x in str(v).split(",") if x.strip()]

MODE = str(P.get("mode", "FINAL")).strip().upper()                                  # FINAL or PREVIEW
MAPPING_SOURCE = str(P.get("mapping_source", "REFERENCE")).strip().upper()          # REFERENCE, DERIVED, DERIVED_ELSE_REFERENCE
ASSIGNMENT_SOURCE = str(P.get("assignment_source", "REFERENCE")).strip().upper()    # REFERENCE, DERIVED, DERIVED_ELSE_REFERENCE
assert MODE in ("FINAL", "PREVIEW", "ANALYSIS", "MONTH-END", "OGM", "AD-HOC", "PRODUCTION", "VALIDATION"), MODE
for _k, _v in [("mapping_source", MAPPING_SOURCE), ("assignment_source", ASSIGNMENT_SOURCE)]:
    assert _v in ("REFERENCE", "DERIVED", "DERIVED_ELSE_REFERENCE", "REVIEWED", "READ_AND_RECOMMEND"), f"{_k} = {_v}"

LAGS = [int(x) for x in _csv(P["lags"])]
TTC = P["ttc_cycle"]
POLY_COL = P["poly_rate_col"]
assert POLY_COL in ("UP_FFR", "EFFR"), POLY_COL
TOL = float(P["avg_r2_tolerance"])
UD_LAG = int(P["updown_lag"])
assert UD_LAG in LAGS, "updown_lag must be one of lags"
MIN_MONTHS = int(P["min_months_ttc"])
MIN_BAL = float(P["min_balance"])
BDSP = _csv(P["snv_bdsp_accounts"])
EXCL = _csv(P["snv_excluded_subproducts"])
_in = lambda xs: ", ".join("'" + x.replace("'", "") + "'" for x in xs) or "''"

# fed funds (month-end) and the named cycles from this folder's rate_cycles.csv
RATE_CYCLES = pd.read_csv(CONFIG / "rate_cycles.csv", dtype=str, keep_default_na=False, encoding="utf-8-sig")
EFFR = B.load_effr_monthly(S)
NAMED = dict(B.build_named_cycles(EFFR, RATE_CYCLES))
assert TTC in NAMED, f"ttc_cycle '{TTC}' is not in config/rate_cycles.csv: {list(NAMED)}"
CYC = cfg["cycles"]
UPDOWN = CYC[(CYC["include"].str.upper() == "Y") & CYC["direction"].str.upper().isin(["UP", "DOWN"])].copy()
UPDOWN["direction"] = UPDOWN["direction"].str.upper()
assert set(UPDOWN["name"]) <= set(NAMED), "cycles tab names must match config/rate_cycles.csv"

# COMMAND ----------

# DBTITLE 1,Helpers: Excel, tables, temp views, known issues, product list check
from openpyxl.utils import get_column_letter
from pyspark.sql import functions as F

def write_xlsx(path, sheets):
    """Write {sheet_name: DataFrame} to .xlsx with frozen headers and sized columns."""
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        for name, df in sheets.items():
            sn = name[:31]
            df.to_excel(xw, sheet_name=sn, index=False)
            ws = xw.sheets[sn]
            ws.freeze_panes = "A2"
            for i, col in enumerate(df.columns, 1):
                n = max([len(str(col))] + [len(str(v)) for v in df[col].head(300).tolist()])
                ws.column_dimensions[get_column_letter(i)].width = min(max(10, n + 2), 60)
    return path

def _typed(pdf):
    pdf = pdf.copy()
    for c in pdf.columns:                      # all-null text columns: give Spark a type (string) instead of void
        if pdf[c].dtype == object and pdf[c].isna().all():
            pdf[c] = pdf[c].astype("string")
    return pdf

def save_table(pdf, name):
    """pandas -> the study's registered table <sandbox>.<name from output_names>_<tag> (overwrite)."""
    assert name in _MV_TABLES, name
    spark.createDataFrame(_typed(pdf)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(S.table(name))
    return S.table(name)

def to_view(pdf, name):
    """pandas -> temp view, so SQL in the next cells can join to an input file."""
    spark.createDataFrame(_typed(pdf)).createOrReplaceTempView(name)
    return name

def read_tab(path, tab):
    return pd.read_excel(path, sheet_name=tab, dtype=str, keep_default_na=False)

# Known issues: one log for the whole study (outputs/reports/known_issues_<tag>.csv). Each step replaces its own rows.
ISSUES_PATH = out("reports", "known_issues", "csv")
ISSUE_COLS = ["step", "code", "severity", "item", "detail", "accounts", "balance"]

def log_issues(step, rows):
    """rows: list of (code, severity, item, detail, accounts, balance). Severity HIGH, MEDIUM or INFO."""
    old = pd.read_csv(ISSUES_PATH, dtype=str, keep_default_na=False) if ISSUES_PATH.exists() else pd.DataFrame(columns=ISSUE_COLS)
    new = pd.DataFrame(rows, columns=ISSUE_COLS[1:])
    new.insert(0, "step", step)
    pd.concat([old[old["step"] != step], new.astype(str)], ignore_index=True)[ISSUE_COLS].to_csv(ISSUES_PATH, index=False)
    if len(new):
        print(f"KNOWN ISSUES ({step}): {len(new)}")
        display(new)
    else:
        print(f"No known issues in {step}.")
    return new

# Global product list: _shared/reference/products.csv is the master every new study copies (install/execute_study).
# Each study keeps its own copy (inputs/products.csv, inputs/product_lookup.csv) so old results stay reproducible;
# this check warns (known issues, MEDIUM) when the copy differs from the master. To change a product for all
# future studies, edit the master; to bring this study in line, copy the master's rows into its two files.
PRODUCT_MASTER = STUDIES_ROOT / "_shared" / "reference" / "products.csv"

def check_products_vs_master():
    """Rows (code, severity, item, detail, accounts, balance) where this study's product files differ from the master."""
    if not PRODUCT_MASTER.exists():
        return []
    _rd = lambda p: pd.read_csv(p, dtype=str, keep_default_na=False, encoding="utf-8-sig").apply(lambda c: c.str.strip())
    m, rows = _rd(PRODUCT_MASTER), []
    for f in ("products.csv", "product_lookup.csv"):
        p = INPUTS / f
        if not p.exists() or not len(s := _rd(p)):            # missing or empty (a scratch study): nothing to compare
            continue
        cols = [c for c in ("product_name", "product_code", "short_name", "client_group") if c in s.columns and c in m.columns]
        j = m[["product_id"] + cols].merge(s[["product_id"] + cols], on="product_id", how="outer", suffixes=("_master", "_study"), indicator=True)
        for r in j.to_dict("records"):
            item = f"inputs/{f} product_id {r['product_id']}"
            if r["_merge"] == "left_only":
                rows.append(("PRODUCT_NOT_IN_STUDY_LIST", "MEDIUM", item, f"in the master, missing here: {r['product_name_master']} ({r['product_code_master']})", "", ""))
            elif r["_merge"] == "right_only":
                rows.append(("PRODUCT_NOT_IN_MASTER", "MEDIUM", item, f"in this study only: {r['product_name_study']} ({r['product_code_study']})", "", ""))
            else:
                d = [f"{c}: study '{r[c + '_study']}' vs master '{r[c + '_master']}'" for c in cols if r[c + "_study"] != r[c + "_master"]]
                if d:
                    rows.append(("PRODUCT_DIFFERS_FROM_MASTER", "MEDIUM", item, "; ".join(d), "", ""))
    return rows

_prod_rows = check_products_vs_master()
if _prod_rows:
    print(f"Product list: {len(_prod_rows)} difference(s) from _shared/reference/products.csv")
    log_issues("00 product list", _prod_rows)
elif ISSUES_PATH.exists():                                      # clear rows from an earlier run, quietly
    _old = pd.read_csv(ISSUES_PATH, dtype=str, keep_default_na=False)
    if (_old["step"] == "00 product list").any():
        _old[_old["step"] != "00 product list"].to_csv(ISSUES_PATH, index=False)

# COMMAND ----------

# DBTITLE 1,Workbook style: xl_front, xl_table, xl_sheet, weighted percentiles
# Every workbook the study writes for people to read uses these helpers, so they all look and read the same:
#   row 1 title | rows 2-3: one or two lines on what the sheet is and how to read it | row 4 input-type badge + colour legend | table from row 6
# Input types (what the reader does with the sheet):
#   InputsToInform  business logic, notes, run modes, tables, sources                  -> read only
#   InputsToAssist  report size / depth, lineage, summary reports, comparison views    -> read to decide
#   InputsToDecide  final selections (betas, truncations, WALs, LRFs): fill in the yellow cells, upload to inputs/decisions/reviewed/
from openpyxl import Workbook as _XWB
from openpyxl.styles import Font as _XF, PatternFill as _XP, Alignment as _XA, Border as _XB, Side as _XS
from openpyxl.formatting.rule import ColorScaleRule as _XCS, DataBarRule as _XDB
from openpyxl.utils import get_column_letter as _xcol
from openpyxl.worksheet.datavalidation import DataValidation as _XDV

XL = dict(navy="1F3864", blue="0070C0", head="1F4E78", grey="F2F2F2", text="595959",
          edit="FFF2CC",      # yellow: you fill this in
          used="C6EFCE",      # green: what the rule / method uses
          alt="DDEBF7",       # light blue: other candidates worth a look
          override="FCE4D6",  # orange: override row
          warn="FFC7CE")      # red: check this
XL_KIND = {"inform": ("InputsToInform", "Read only: business logic, notes, settings, sources", "D9D9D9"),
           "assist": ("InputsToAssist", "Read to decide: summaries, comparisons, lineage", "DDEBF7"),
           "decide": ("InputsToDecide", "Fill in the yellow cells, then upload to inputs/decisions/reviewed/", "FFF2CC")}
XL_FMT = {"$": "$#,##0", "$m": '$#,##0.0,,"M"', "int": "#,##0", "beta": "0.0000", "b2": "0.00", "pct": "0.00%", "pct1": "0.0%",
          "m": "0.0", "r2": "0.0", "date": "yyyy-mm-dd"}
_XTHIN = _XB(*(_XS(style="thin", color="BFBFBF"),) * 4)
XL_TABLE_ROW = 6                                           # header row of the main table on every sheet

def _xv(v):
    if v is None or (not isinstance(v, str) and pd.isna(v)):
        return None
    return v.item() if hasattr(v, "item") else v

def xl_safe_name(name, used=()):
    """A valid, unique sheet name (31 chars, no []:*?/\\)."""
    sn = re.sub(r"[\[\]\:\*\?\/\\]", "", str(name))[:28] or "Sheet"
    k = 1
    while sn in used:
        sn, k = sn[:25] + f"_{k:02d}", k + 1
    return sn

def xl_sheet(wb, name, title, about, kind="assist", legend=None):
    """A sheet with the standard header: title, 1-2 lines `about` (str or [str, str]), the input-type badge and a colour legend.
    legend = [(colour key in XL, meaning)]. Returns the sheet; write the table at XL_TABLE_ROW."""
    first = wb.worksheets[0]
    fresh = len(wb.worksheets) == 1 and first.title == "Sheet" and first["A1"].value is None   # the empty sheet a new Workbook() has
    ws = first if fresh else wb.create_sheet(name)
    ws.title = name
    ws.cell(1, 1, f"{S.tag} | {title}").font = _XF(bold=True, size=13, color=XL["navy"])
    for i, line in enumerate(([about] if isinstance(about, str) else list(about))[:2], 2):
        ws.cell(i, 1, line).font = _XF(italic=True, color=XL["text"])
    lab, what, fill = XL_KIND[kind]
    c = ws.cell(4, 1, lab)
    c.font, c.fill, c.border = _XF(bold=True), _XP("solid", fgColor=fill), _XTHIN
    ws.cell(4, 2, what).font = _XF(color=XL["text"])
    for j, (key, meaning) in enumerate(legend or [], 4):
        c = ws.cell(4, 2 * j - 3, meaning)
        c.fill, c.border, c.font = _XP("solid", fgColor=XL.get(key, key)), _XTHIN, _XF(size=9)
    return ws

def xl_table(ws, df, row=XL_TABLE_ROW, col=1, fmt=None, widths=None, edit=(), fills=None, scales=(), bars=(), links=None,
             freeze=True, autofilter=True, bold=None):
    """Write df as a formatted table. fmt = {column: XL_FMT key}; edit = columns you fill in (yellow);
    fills = {row index: colour key} or a function(row) -> colour key / None (whole row); scales = columns with a red-white-green scale;
    bars = columns with data bars; links = {column: function(row) -> sheet name}; bold = function(row, column) -> True to bold a cell.
    Returns the last row written."""
    fmt, cols = fmt or {}, list(df.columns)
    for j, h in enumerate(cols, col):
        c = ws.cell(row, j, h)
        c.font, c.fill, c.border = _XF(bold=True, color="FFFFFF"), _XP("solid", fgColor=XL["head"]), _XTHIN
        c.alignment = _XA(wrap_text=True, vertical="center")
    for i, (idx, r) in enumerate(df.iterrows(), row + 1):
        key = fills(r) if callable(fills) else (fills or {}).get(idx)
        for j, h in enumerate(cols, col):
            c = ws.cell(i, j, _xv(r[h]))
            c.border = _XTHIN
            if h in fmt:
                c.number_format = XL_FMT.get(fmt[h], fmt[h])
            if h in edit:
                c.fill = _XP("solid", fgColor=XL["edit"])
            elif key:
                c.fill = _XP("solid", fgColor=XL.get(key, key))
            if bold and bold(r, h):
                c.font = _XF(bold=True)
            if links and h in links and links[h](r):
                c.hyperlink, c.font = f"#'{links[h](r)}'!A1", _XF(color="0563C1", underline="single")
    last = row + len(df)
    for h in scales:
        if h in cols and len(df):
            L = _xcol(col + cols.index(h))
            ws.conditional_formatting.add(f"{L}{row + 1}:{L}{last}", _XCS(start_type="min", start_color="F8696B", mid_type="percentile",
                                                                       mid_value=50, mid_color="FFFFFF", end_type="max", end_color="63BE7B"))
    for h in bars:
        if h in cols and len(df):
            L = _xcol(col + cols.index(h))
            ws.conditional_formatting.add(f"{L}{row + 1}:{L}{last}", _XDB(start_type="min", end_type="max", color="5B9BD5"))
    for j, h in enumerate(cols, col):
        w = (widths or {}).get(h) or min(max(len(str(h)) + 2, 10), 40)
        ws.column_dimensions[_xcol(j)].width = w
    if freeze:
        ws.freeze_panes = ws.cell(row + 1, col + 1)
    if autofilter and len(df):
        ws.auto_filter.ref = f"{_xcol(col)}{row}:{_xcol(col + len(cols) - 1)}{last}"
    return last

def xl_front(wb, title, kind, facts, steps, review_fields=True):
    """Front page: what the workbook is, its facts [(label, value)], the steps to read / review it, and (InputsToDecide) the sign-off cells.
    Returns the sheet; the sign-off is on rows 'Reviewed by', 'Review date', 'Reviewed and Confirmed' (column B)."""
    ws = xl_sheet(wb, "Review" if kind == "decide" else "About", title, [f"{title}: {S.tag}, snapshot {S.snapshot_date}.",
                  "Start here. Each product has its own tab; 'All products' links to every tab."], kind)
    r = XL_TABLE_ROW
    for a, b in list(facts) + [("Built", f"{pd.Timestamp.now():%Y-%m-%d %H:%M}")]:
        ws.cell(r, 1, a).font = _XF(bold=True)
        ws.cell(r, 2, _xv(b))
        r += 1
    if review_fields:
        r += 1
        for lab in ("Reviewed by", "Review date", "Reviewed and Confirmed"):
            ws.cell(r, 1, lab).font = _XF(bold=True)
            c = ws.cell(r, 2)
            c.fill, c.border = _XP("solid", fgColor=XL["edit"]), _XTHIN
            if lab == "Reviewed and Confirmed":
                dv = _XDV(type="list", formula1='"Y,N"', allow_blank=True)
                ws.add_data_validation(dv)
                dv.add(c.coordinate)
            r += 1
    r += 1
    ws.cell(r, 1, "How to read / review").font = _XF(bold=True)
    for i, s in enumerate(steps, 1):
        ws.cell(r + i, 1, str(i))
        ws.cell(r + i, 2, s)
    ws.column_dimensions["A"].width, ws.column_dimensions["B"].width = 26, 120
    return ws

def xl_read_signoff(path, sheet="Review"):
    """(reviewed_by, review_date, confirmed Y/N) from a reviewed workbook's front page."""
    f = pd.read_excel(path, sheet_name=sheet, header=None, dtype=str).fillna("")
    get = lambda lab: next((str(r[1]).strip() for _, r in f.iterrows() if str(r[0]).strip() == lab), "")
    return get("Reviewed by"), get("Review date"), get("Reviewed and Confirmed").upper()[:1]

def beta_method_rows(method):
    """The candidate row ids (06b row_id: LM_LAG<L>, POLY_LAG<L>, AVG) a beta method uses.
    'POLY_LAG3' -> {'POLY_LAG3'}; 'AVG(LM_LAG2,POLY_LAG3)' -> {'AVG', 'LM_LAG2', 'POLY_LAG3'}; MANUAL / blank -> set()."""
    m = str(method or "").upper()
    ids = set(re.findall(r"(?:LM|POLY)_LAG\d+", m))
    return ids | {"AVG"} if m.startswith("AVG") else ids

def xl_dist(df, value, weight=None, by="mv_product", pcts=(0.1, 0.25, 0.5, 0.75, 0.9)):
    """Distribution table by group: accounts, weighted average, min, percentiles, max (unweighted percentiles over accounts)."""
    out_ = []
    for g, d in df.groupby(by, dropna=False):
        x = pd.to_numeric(d[value], errors="coerce")
        w = pd.to_numeric(d[weight], errors="coerce") if weight else None
        m = x.notna() & (w.notna() if weight else True)
        row = {by: g, "accounts": int(m.sum()),
               f"{value}_wavg" if weight else f"{value}_avg": (float((x[m] * w[m]).sum() / w[m].sum()) if weight and w[m].sum() else (float(x[m].mean()) if m.any() else None)),
               "min": float(x[m].min()) if m.any() else None}
        for p in pcts:
            row[f"p{int(p * 100)}"] = float(x[m].quantile(p)) if m.any() else None
        row["max"] = float(x[m].max()) if m.any() else None
        out_.append(row)
    return pd.DataFrame(out_)

# COMMAND ----------

# DBTITLE 1,Decisions: source, derived, current (accounts, products, betas)
# =====================================================================================
# DECISIONS: three CSV copies of each decision under inputs/decisions/
#   source/<d>.csv   what the study started with (01 / 04: the reference files, rewritten each run; betas: the final_overrides tab
#                    when current/betas.csv was first made, written once)
#   derived/<d>.csv  what this run's rules produced (01 Step 5b, 04 Step 2b, 06 recommendation with no override), rewritten each run
#   current/<d>.csv  what the study USES. Made once from this run's pick (also on a cloned study, where it is copied in), then
#                    changed only by 00_read_and_recommend (or a hand edit). Every save keeps a copy in history/.
# After current/<d>.csv exists, mapping_source / assignment_source only decide the values of NEW keys.
# =====================================================================================
DECISIONS_DIR = INPUTS / "decisions"
DEC_KINDS = ("source", "derived", "current")
for _k in DEC_KINDS + ("history",):
    (DECISIONS_DIR / _k).mkdir(parents=True, exist_ok=True)
DEC_LOG = DECISIONS_DIR / "change_log.csv"
DEC_STATE = DECISIONS_DIR / "markup_state.json"

DECISIONS = {
    "accounts": dict(key="join_key", notebook="01_account_mapping", what="subproduct key -> product, group, segment, rate class",
                     info=["legacy_bank", "subproductid", "pf_flag", "bdsp_flag", "acct_typ_code", "months", "snapshot_accounts",
                           "snapshot_balance", "snapshot_war"],
                     edit=["acct_typ_desc", "product_group", "client_group", "rate_class", "product", "product_num", "dim_pf_flag"]),
    "products": dict(key="sub_key", notebook="04_product_assignment", what="subproduct -> final (new) product",
                     info=["legacy_bank", "subproductid", "current_product", "product_group", "snapshot_balance", "snapshot_accounts",
                           "months_ttc", "sub_recommended_method", "sub_recommended_beta", "sub_recommended_r2"],
                     edit=["new_product", "notes"]),
    "betas": dict(key="product", notebook="06_final_betas", what="product -> beta method override",
                  info=["recommended_method", "recommended_beta", "snapshot_balance", "months_ttc", "best_lm_beta", "best_lm_r2",
                        "best_poly_beta", "best_poly_r2"],
                  edit=["override_method", "override_lag", "manual_beta", "reason"]),
}

# RecommendCurrents: the study runs in stages; 00_read_and_recommend asks for each decision, then runs the next stage
STUDY_STEPS = ["01_account_mapping", "02_account_metrics", "03_account_betas", "03a_account_review", "04_product_assignment",
               "05_product_betas", "06_final_betas", "06a_legacy_assumptions", "06b_beta_review",
               "08_reports_dashboards", "07_study_flow",
               "10_truncation_cohorts", "11_truncation_points", "12_warf_prep", "13_warf_results", "12b_warf_review",
               "15_wal_ftp", "15a_wal_review_workbook", "15b_wal_review_workbook", "16_wal_trunc_review",
               "09a_assumptions_summary", "09_deposit_study_package"]
MARKUP_STAGES = {                       # stage -> notebooks run BEFORE that decision is asked
    "accounts": ["01_account_mapping"],
    "products": ["02_account_metrics", "03_account_betas", "03a_account_review", "04_product_assignment"],
    "betas": ["05_product_betas", "06_final_betas"],
    "done": ["08_reports_dashboards", "07_study_flow", "10_truncation_cohorts", "11_truncation_points", "12_warf_prep", "13_warf_results",
             "09_deposit_study_package"],
}
STAGE_ORDER = list(MARKUP_STAGES)

# module folder of each notebook under notebooks/ (core notebooks 00_* stay at the top)
STEP_DIRS = {
    "01_account_mapping": "1_accounts_products", "02_account_metrics": "1_accounts_products", "03_account_betas": "1_accounts_products",
    "03a_account_review": "1_accounts_products", "04_product_assignment": "1_accounts_products",
    "05_product_betas": "2_betas", "06_final_betas": "2_betas", "06a_legacy_assumptions": "2_betas", "06b_beta_review": "2_betas",
    "10_truncation_cohorts": "3_truncations", "11_truncation_points": "3_truncations",
    "12_warf_prep": "4_warfs", "13_warf_results": "4_warfs", "12b_warf_review": "4_warfs",
    "15_wal_ftp": "5_wals", "15a_wal_review_workbook": "5_wals", "15b_wal_review_workbook": "5_wals",
    "07_study_flow": "9_reports", "08_reports_dashboards": "9_reports", "16_wal_trunc_review": "9_reports", "09a_assumptions_summary": "9_reports", "09_deposit_study_package": "9_reports",
}

def step_file(nb):
    """/Workspace path of a study notebook (module folder from STEP_DIRS)."""
    return STUDY_ROOT / "notebooks" / STEP_DIRS.get(nb, "") / nb

def step_path(nb):
    """Workspace path for dbutils.notebook.run."""
    return str(step_file(nb)).replace("/Workspace", "", 1)

def dec_path(kind, decision):
    assert kind in DEC_KINDS and decision in DECISIONS, (kind, decision)
    return DECISIONS_DIR / kind / f"{decision}.csv"

def dec_cols(decision):
    d = DECISIONS[decision]
    return [d["key"]] + d["info"] + d["edit"]

def _dec_clean(df, decision):
    d = df.copy()
    for c in dec_cols(decision):
        if c not in d:
            d[c] = ""
    d = d[dec_cols(decision)].astype("string").fillna("")
    for c in d.columns:
        d[c] = d[c].str.strip()
        d.loc[d[c].isin(["nan", "None", "<NA>", "NaN"]), c] = ""
    return d[d[dec_cols(decision)[0]] != ""].drop_duplicates(DECISIONS[decision]["key"], keep="first").reset_index(drop=True)

def read_decision(kind, decision):
    p = dec_path(kind, decision)
    return _dec_clean(pd.read_csv(p, dtype=str, keep_default_na=False, encoding="utf-8-sig"), decision) if p.exists() else None

def write_decision(kind, decision, df, backup=True):
    """Writes inputs/decisions/<kind>/<decision>.csv. A current file is copied to history/ first."""
    p = dec_path(kind, decision)
    if backup and kind == "current" and p.exists():
        (DECISIONS_DIR / "history" / f"{decision}_{pd.Timestamp.now():%Y%m%d_%H%M%S}.csv").write_bytes(p.read_bytes())
    _dec_clean(df, decision).to_csv(p, index=False)
    return p

def decision_diff(a, b, decision):
    """One row per key and edit field where a and b differ (keys in both)."""
    d = DECISIONS[decision]
    if a is None or b is None:
        return pd.DataFrame(columns=[d["key"], "field", "a", "b"])
    m = a[[d["key"]] + d["edit"]].merge(b[[d["key"]] + d["edit"]], on=d["key"], suffixes=("_a", "_b"))
    rows = [dict(**{d["key"]: r[d["key"]]}, field=c, a=r[c + "_a"], b=r[c + "_b"])
            for _, r in m.iterrows() for c in d["edit"] if r[c + "_a"] != r[c + "_b"]]
    return pd.DataFrame(rows, columns=[d["key"], "field", "a", "b"])

def resolve_current(decision, picked):
    """picked = this run's values. Returns (current values for every picked key, info).
    current.csv wins for its keys (edit columns); the info columns are refreshed; new keys are added from picked;
    keys no longer in the data stay in the file."""
    d, k = DECISIONS[decision], DECISIONS[decision]["key"]
    picked = _dec_clean(picked, decision)
    cur = read_decision("current", decision)
    if cur is None:
        write_decision("current", decision, picked, backup=False)
        print(f"DECISIONS: current/{decision}.csv did not exist, made from this run ({len(picked):,} rows). From now on it is what the study uses.")
        return picked, dict(seeded=True, new=len(picked), gone=0, edited=0)
    new_keys = picked.loc[~picked[k].isin(cur[k]), k]
    gone = cur.loc[~cur[k].isin(picked[k])]
    out_ = picked.set_index(k)
    keep = cur.set_index(k)[d["edit"]]
    common = out_.index.intersection(keep.index)
    edited = int((out_.loc[common, d["edit"]] != keep.loc[common, d["edit"]]).any(axis=1).sum())
    out_.loc[common, d["edit"]] = keep.loc[common, d["edit"]]
    out_ = out_.reset_index()
    write_decision("current", decision, pd.concat([out_, gone], ignore_index=True), backup=False)
    print(f"DECISIONS: current/{decision}.csv used | {len(common):,} keys kept, {len(new_keys):,} new keys added from this run, "
          f"{len(gone):,} keys no longer in the data (kept in the file), {edited:,} differ from this run's pick")
    return out_, dict(seeded=False, new=len(new_keys), gone=len(gone), edited=edited)

def read_state():
    return json.loads(DEC_STATE.read_text(encoding="utf-8")) if DEC_STATE.exists() else {"stage": "", "history": []}

def write_state(stage, note=""):
    st = read_state()
    st["stage"] = stage
    st.setdefault("history", []).append(dict(at=f"{pd.Timestamp.now():%Y-%m-%d %H:%M:%S}", stage=stage, note=note))
    DEC_STATE.write_text(json.dumps(st, indent=2), encoding="utf-8")
    return st

def run_steps(steps, timeout=3600):
    """Runs study notebooks in order (dbutils.notebook.run), stops at the first failure. Returns the log."""
    import time
    log = []
    for nb in steps:
        t0 = time.time()
        try:
            dbutils.notebook.run(step_path(nb), timeout)
            log.append(dict(notebook=nb, status="OK", minutes=round((time.time() - t0) / 60, 1), error=""))
            print(f"OK   {nb} ({log[-1]['minutes']} min)")
        except Exception as e:
            log.append(dict(notebook=nb, status="FAILED", minutes=round((time.time() - t0) / 60, 1), error=str(e)[:500]))
            print(f"FAIL {nb}: {str(e)[:500]}")
            break
    return log

def alert(kind, title, lines=()):
    """kind: error (red), warn (amber), ok (green), info (blue)."""
    col = {"error": ("#fdecea", "#c62828"), "warn": ("#fff8e1", "#f9a825"), "ok": ("#e8f5e9", "#2e7d32"), "info": ("#e3f2fd", "#1565c0")}[kind]
    items = "".join(f"<li>{x}</li>" for x in list(lines)[:60])
    more = f"<li>... {len(lines) - 60} more</li>" if len(lines) > 60 else ""
    displayHTML(f'<div style="background:{col[0]};border-left:6px solid {col[1]};padding:10px 14px;font-family:Arial;font-size:13px">'
                f'<b>{title}</b>{"<ul>" + items + more + "</ul>" if items else ""}</div>')

def decisions_banner():
    """What source / derived / current mean, and the state of each decision file."""
    rows = []
    for dec, d in DECISIONS.items():
        r = {"decision": dec, "what": d["what"], "key": d["key"], "built by": d["notebook"]}
        for kind in DEC_KINDS:
            df, p = read_decision(kind, dec), dec_path(kind, dec)
            r[kind] = f"{len(df):,} rows ({pd.Timestamp(p.stat().st_mtime, unit='s'):%Y-%m-%d %H:%M} UTC)" if df is not None else "not made yet"
        dd = decision_diff(read_decision("current", dec), read_decision("derived", dec), dec)
        r["current vs derived"] = f"{dd.iloc[:, 0].nunique():,} keys differ" if len(dd) else ("same" if "not" not in r["current"] else "")
        rows.append(r)
    expl = ("<table style='border-collapse:collapse;font-family:Arial;font-size:12px'>"
            "<tr style='background:#1F4E79;color:white'><th style='padding:4px 8px'>Folder</th><th style='padding:4px 8px'>Holds</th>"
            "<th style='padding:4px 8px'>Written</th><th style='padding:4px 8px'>Used by the study</th></tr>"
            "<tr><td style='padding:4px 8px'><b>source/</b></td><td style='padding:4px 8px'>What the study started with: the reference files "
            "(<code>inputs/reference/dim_nmd.csv</code>, <code>mv_config.xlsx</code>); betas: the final_overrides tab when current was first made</td>"
            "<td style='padding:4px 8px'>01 / 04 every run; betas once</td><td style='padding:4px 8px'>No, for comparison</td></tr>"
            "<tr><td style='padding:4px 8px'><b>derived/</b></td><td style='padding:4px 8px'>What this run's rules produce (01 Step 5b, 04 Step 2b, "
            "06 recommended method with no override)</td><td style='padding:4px 8px'>01 / 04 / 06 every run</td>"
            "<td style='padding:4px 8px'>No, for comparison</td></tr>"
            "<tr style='background:#e8f5e9'><td style='padding:4px 8px'><b>current/</b></td><td style='padding:4px 8px'>The values the study runs on. "
            "Made once from the first run's pick (or copied in with a clone), then changed only in <b>00_read_and_recommend</b> "
            "(every save is kept in <code>history/</code> and <code>change_log.csv</code>). New keys are added with this run's pick.</td>"
            "<td style='padding:4px 8px'>When the file does not exist, and new keys each run</td><td style='padding:4px 8px'><b>Yes, always</b></td></tr></table>")
    displayHTML(f"<div style='font-family:Arial'><h3 style='color:#1F4E79;margin:4px 0'>Decisions: current is what this study uses</h3>"
                f"{expl}<p style='font-size:12px'>Folder: <code>{DECISIONS_DIR}</code>. Stage: <b>{read_state().get('stage') or 'not started'}</b></p>"
                f"{pd.DataFrame(rows).to_html(index=False)}</div>")

# COMMAND ----------

# DBTITLE 1,Beta engine: beta_grid and recommend (used by 03, 05, 06)
# Same functions as notebook 03 of the version (deposit_utils.betas):
#   B.lm_beta_lag        linear slope of WAR on fed funds over the cycle, lagged L months, with R2
#   B.poly_beta_lag      cubic fit WAR = b0 + b1 x + b2 x^2 + b3 x^3 on fed funds (POLY_COL); beta = derivative, with R2
#   B.endpoint_beta_lag  (WAR end - WAR start) / (fed funds end - fed funds start) over a cycle, lagged L months

def war_pivot(df, key):
    """Long (key, eopdate, war) -> one row per key, one column per month (the layout the beta functions expect)."""
    d = df[[key, "eopdate", "war"]].copy()
    d["eopdate"] = pd.to_datetime(d["eopdate"])
    p = d.pivot(index=key, columns="eopdate", values="war")
    p = p[sorted(p.columns)]
    p.columns = [c.strftime("%Y-%m-%d") for c in p.columns]
    return p.reset_index().rename(columns={key: "EMP_NAME"})

def _ren(df, names):
    df = df.copy()
    df.columns = ["EMP_NAME"] + names
    return df

UD_COLS = {"UP": {L: [] for L in LAGS}, "DOWN": {L: [] for L in LAGS}}

def beta_grid(pivot):
    """TTC linear / poly at every lag, endpoint betas for each UP / DOWN cycle at every lag, averages, TTC months."""
    g = pivot[["EMP_NAME"]].copy()
    ttc = NAMED[TTC]
    for L in LAGS:
        g = g.merge(_ren(B.lm_beta_lag(pivot, ttc, L, EFFR, TTC), [f"lm_{L}", f"lm_r2_{L}"]), on="EMP_NAME", how="left")
        g = g.merge(_ren(B.poly_beta_lag(pivot, ttc, L, EFFR, POLY_COL, TTC), [f"poly_{L}", f"poly_r2_{L}"]), on="EMP_NAME", how="left")
    for d in UD_COLS:
        for L in LAGS:
            UD_COLS[d][L] = []
    for _, c in UPDOWN.iterrows():
        code = re.sub(r"\W", "", c["short_code"])
        for L in LAGS:
            col = f"{c['direction'].lower()}_{code}_{L}"
            g = g.merge(_ren(B.endpoint_beta_lag(pivot, NAMED[c["name"]], L, EFFR, c["name"]), [col]), on="EMP_NAME", how="left")
            UD_COLS[c["direction"]][L].append(col)
    for L in LAGS:
        g[f"avg_up_{L}"] = g[UD_COLS["UP"][L]].mean(axis=1) if UD_COLS["UP"][L] else np.nan
        g[f"avg_down_{L}"] = g[UD_COLS["DOWN"][L]].mean(axis=1) if UD_COLS["DOWN"][L] else np.nan
    g["avg_up"], g["avg_down"] = g[f"avg_up_{UD_LAG}"], g[f"avg_down_{UD_LAG}"]
    dates = pd.to_datetime(pivot.columns[1:])
    in_ttc = (dates >= ttc[0]) & (dates <= ttc[-1])
    months = pd.DataFrame({"EMP_NAME": pivot["EMP_NAME"], "months_ttc": pivot.iloc[:, 1:].loc[:, in_ttc].notna().sum(axis=1).values})
    return g.merge(months, on="EMP_NAME", how="left")

def recommend(g):
    """Best-R2 linear lag vs best-R2 poly lag at TTC; AVG when the R2s are within TOL points."""
    rows = []
    for _, r in g.iterrows():
        def best(p):
            c = [(L, r[f"{p}_{L}"], r[f"{p}_r2_{L}"]) for L in LAGS if pd.notna(r[f"{p}_{L}"]) and pd.notna(r[f"{p}_r2_{L}"])]
            return max(c, key=lambda t: t[2]) if c else (None, np.nan, np.nan)
        lL, lb, lr = best("lm")
        pL, pb, pr = best("poly")
        if lL is None and pL is None:
            m, b, r2, why = "NONE", np.nan, np.nan, "no linear or poly fit (too little history in the TTC window)"
        elif pL is None:
            m, b, r2, why = f"LM_LAG{lL}", lb, lr, "no poly fit"
        elif lL is None:
            m, b, r2, why = f"POLY_LAG{pL}", pb, pr, "no linear fit"
        elif abs(lr - pr) <= TOL:
            m, b, r2, why = f"AVG(LM_LAG{lL},POLY_LAG{pL})", (lb + pb) / 2, (lr + pr) / 2, f"R2 within {TOL:g} pts ({lr:.1f} vs {pr:.1f})"
        elif lr > pr:
            m, b, r2, why = f"LM_LAG{lL}", lb, lr, f"linear R2 {lr:.1f} > poly {pr:.1f}"
        else:
            m, b, r2, why = f"POLY_LAG{pL}", pb, pr, f"poly R2 {pr:.1f} > linear {lr:.1f}"
        rows.append(dict(EMP_NAME=r["EMP_NAME"], best_lm_lag=lL, best_lm_beta=lb, best_lm_r2=lr,
                         best_poly_lag=pL, best_poly_beta=pb, best_poly_r2=pr,
                         avg_lm_poly_beta=(lb + pb) / 2 if lL is not None and pL is not None else np.nan,
                         recommended_method=m, recommended_beta=b, recommended_r2=r2, recommended_reason=why))
    return g.merge(pd.DataFrame(rows), on="EMP_NAME", how="left")

SETTINGS = pd.DataFrame([dict(setting=k, value=v) for k, v in P.items()])
print(f"MV study {MV_ROOT.name} | source {S.banner()}")
print(f"  mode {MODE} | mapping_source {MAPPING_SOURCE} | assignment_source {ASSIGNMENT_SOURCE}")
print(f"  TTC {TTC} ({NAMED[TTC][0]:%Y-%m} to {NAMED[TTC][-1]:%Y-%m}) | lags {LAGS} | poly on {POLY_COL} | AVG tolerance {TOL:g} R2 pts")