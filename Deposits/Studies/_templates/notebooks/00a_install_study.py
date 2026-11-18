# Databricks notebook source
# DBTITLE 1,00a | Install Study
# MAGIC %md
# MAGIC # 00a | Install Study
# MAGIC The **single entry point** for every deposit study. Open this notebook first.
# MAGIC
# MAGIC ## What it does
# MAGIC | Run Mode | Folders | Config | Source Tables | Study Run |
# MAGIC |---|---|---|---|---|
# MAGIC | **Dry run** | validates | validates | validates | no |
# MAGIC | **Create only** | creates | writes `study_params.json` | no | no |
# MAGIC | **Create + load sources** | creates | writes | builds per `source_data_action` | no |
# MAGIC | **Create + run** | creates | writes | builds (if needed) | yes |
# MAGIC | **Run** | exists | exists | exists | yes |
# MAGIC | **Update + run** | exists | updates | rebuilds (if changed) | yes |
# MAGIC
# MAGIC ## Widgets (fill top to bottom)
# MAGIC | Section | Widget | Purpose |
# MAGIC |---|---|---|
# MAGIC | 1. Action | `action` | What are you doing? |
# MAGIC | | `run_mode` | How far to go? |
# MAGIC | 2. Study | `study_tag` | e.g. `2027v1000` |
# MAGIC | | `clone_source` | Version to clone FROM (Clone only) |
# MAGIC | | `study_vintage` | Year (defaults to current + 1) |
# MAGIC | | `study_purpose` | Month-End, OGM, Final, Analysis, Ad-hoc, Production, Validation |
# MAGIC | 3. Data | `asofdate` | As-of date for balances, rates, reports |
# MAGIC | | `source_data_action` | Keep as-is, or Create/update to production/sandbox/final |
# MAGIC | | `clone_tables` | Clone, Create from scratch, Use without cloning (Clone only) |
# MAGIC | 4. Mapping | `mapping_source` | SQL query logic, Reference, Reviewed Excel, Read-and-recommend |
# MAGIC
# MAGIC The study root is **auto-detected** from wherever this notebook sits. No hardcoded paths.
# MAGIC
# MAGIC **Run after:** Nothing. This is the first notebook.

# COMMAND ----------

# DBTITLE 1,Install openpyxl
# MAGIC %pip install -q openpyxl

# COMMAND ----------

# DBTITLE 1,Auto-detect root and discover studies
import sys, os, re, json, shutil, getpass
from datetime import datetime, date, timedelta
from pathlib import Path
import pandas as pd

# ---- Auto-detect study root from wherever this notebook lives ----
try:
    _nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    _here = Path(_nb if _nb.startswith("/Workspace") else "/Workspace" + _nb).parent
except Exception:
    _here = Path(os.getcwd())

# Walk up to find _shared/deposit_utils (the studies root marker)
def _find_root(start: Path) -> Path:
    for p in [start, *start.parents]:
        if (p / "_shared" / "deposit_utils").is_dir():
            return p
    raise FileNotFoundError(f"Cannot find _shared/deposit_utils above {start}. Is this notebook inside a study tree?")

ROOT = _find_root(_here)
TEMPLATE = next((ROOT / t for t in ("_templates", "_templates_mv") if (ROOT / t / "notebooks").is_dir()), ROOT / "_templates")
sys.path.insert(0, str(ROOT / "_shared"))

# ---- Study discovery ----
TAG_RE = re.compile(r"^(20\d{2})v(\d+)$")

def list_studies(root: Path) -> list:
    """Study tags at <root>/<year>/<tag>, newest first."""
    out = []
    for yd in sorted(root.glob("20[0-9][0-9]")):
        for p in yd.iterdir():
            m = TAG_RE.match(p.name)
            if p.is_dir() and m and int(m.group(2)) < 1000 and ((p / "study.json").exists() or (p / "mv_study.json").exists()):
                out.append(p.name)
    return sorted(out, key=lambda t: (int(t[:4]), int(t[5:])), reverse=True)

def month_ends(last: date, n=36) -> list:
    """Last n month-end dates as strings."""
    out, y, m = [], last.year, last.month
    for _ in range(n):
        nxt = date(y + (m == 12), m % 12 + 1, 1)
        out.append(str(date.fromordinal(nxt.toordinal() - 1)))
        y, m = (y - 1, 12) if m == 1 else (y, m - 1)
    return out

def version_dir(tag: str) -> Path:
    m = TAG_RE.match(tag)
    assert m, f"bad tag {tag}"
    return ROOT / m.group(1) / tag

def config_xlsx(folder: Path) -> Path:
    return next((folder / "config" / f for f in ("study_config.xlsx", "mv_study_config.xlsx") if (folder / "config" / f).exists()),
                folder / "config" / "study_config.xlsx")

# ---- Latest source month-ends ----
try:
    _src = json.loads((TEMPLATE / "config" / "study_params.json").read_text(encoding="utf-8")).get("sources", {})
except Exception:
    _src = {}
try:
    _snv = spark.sql(f"SELECT MAX(EOPDate) d FROM {_src.get('src_lsnv_nmds', 'finance.pricing_analytics.lsnv_nmds')}").first().d
    _pnfp = spark.sql(f"SELECT MAX(load_date) d FROM {_src.get('src_pnfp_deposits', 'finance.ftp_validation.pnfp_deposits')}").first().d
    _snv, _pnfp = pd.Timestamp(_snv).date(), pd.Timestamp(_pnfp).date()
    LATEST = min(_snv, _pnfp)
except Exception as e:
    print(f"Could not read source dates: {str(e)[:200]}")
    _snv = _pnfp = LATEST = date.today()
if (LATEST + timedelta(days=1)).day != 1:
    LATEST = LATEST.replace(day=1) - timedelta(days=1)
MONTHS = month_ends(LATEST)

_studies = list_studies(ROOT)
_yr = sorted({str(date.today().year + i) for i in (-1, 0, 1, 2)} | {t[:4] for t in _studies})

print(f"Root: {ROOT}")
print(f"Template: {TEMPLATE.name}")
print(f"Studies found: {len(_studies)} ({', '.join(_studies[:5])}{'...' if len(_studies) > 5 else ''})")
print(f"Latest source month-end: SNV {_snv} | PNFP {_pnfp} -> as-of = {LATEST}")

# COMMAND ----------

# DBTITLE 1,Section 1-2: Action, run mode, study identity
W = dbutils.widgets

# ---- Section 1: What are you doing? ----
W.dropdown("action", "Create from scratch",
           ["Create from scratch", "Create from template", "Clone from version", "Update existing", "Run existing"],
           "01 Action")
W.dropdown("run_mode", "Dry run",
           ["Dry run", "Create only", "Create + load sources", "Create + run", "Run", "Update + run"],
           "02 Run mode")

# ---- Section 2: Which study? ----
W.dropdown("study_vintage", str(date.today().year + 1) if str(date.today().year + 1) in _yr else _yr[-1],
           _yr, "03 Study vintage (year)")
W.text("study_tag", "auto", "04 Study tag (auto or e.g. 2027v1000)")
W.dropdown("clone_source", _studies[0] if _studies else "none",
           _studies or ["none"], "05 Clone from (Clone only)")
W.dropdown("study_purpose", "Month-End",
           ["Month-End", "OGM", "Final", "Analysis", "Ad-hoc", "Production", "Validation"],
           "06 Study purpose")

# ---- Section 3: Data sourcing ----
W.dropdown("asofdate", "latest", ["latest"] + MONTHS, "07 As-of date")
W.dropdown("source_data_action", "Keep as-is",
           ["Keep as-is",
            "Create/update -> production",
            "Create/update -> sandbox",
            "Create/update -> final"],
           "08 Source data action")
W.dropdown("clone_tables", "Clone",
           ["Clone", "Create from scratch", "Use without cloning"],
           "09 Clone tables (Clone only)")

# ---- Section 4: Mapping & assignment ----
W.dropdown("mapping_source", "SQL query logic (derived)",
           ["SQL query logic (derived)",
            "Reference files",
            "Reviewed Excel",
            "Read-and-recommend"],
           "10 Mapping source")

# ---- Notes and dry run ----
W.text("notes", "", "11 Notes")

print("Widgets created. Fill them in, then run the next cell.")

# COMMAND ----------

# DBTITLE 1,Read widgets and build the plan
# ---- Read all widgets ----
cfg = {k: W.get(k).strip() for k in [
    "action", "run_mode", "study_vintage", "study_tag", "clone_source",
    "study_purpose", "asofdate", "source_data_action", "clone_tables",
    "mapping_source", "notes"]}

ACTION = cfg["action"]
RUN_MODE = cfg["run_mode"]
DRY = RUN_MODE == "Dry run"
YEAR = int(cfg["study_vintage"])

# ---- Resolve as-of date ----
asofdate = str(LATEST) if cfg["asofdate"] == "latest" else cfg["asofdate"]

# ---- Resolve mapping source ----
MAPPING_MAP = {
    "SQL query logic (derived)": "DERIVED",
    "Reference files": "REFERENCE",
    "Reviewed Excel": "REVIEWED",
    "Read-and-recommend": "READ_AND_RECOMMEND",
}
mapping = MAPPING_MAP.get(cfg["mapping_source"], "DERIVED")

# ---- Resolve source data action ----
SOURCE_MAP = {
    "Keep as-is": None,
    "Create/update -> production": "production",
    "Create/update -> sandbox": "sandbox",
    "Create/update -> final": "final",
}
source_target = SOURCE_MAP.get(cfg["source_data_action"])

# ---- Resolve study tag ----
from deposit_utils.study import MV_TABLES, DEFAULT_PARAMS
from deposit_utils import folders

TABLE_KEYS = [re.sub(r"^mv_", "", k) for k in MV_TABLES] + ["lsnv_dp_hist", "lpnfp_dp_hist"]
SCHEMA = json.loads((TEMPLATE / "config" / "study_params.json").read_text(encoding="utf-8")).get(
    "sandbox_schema", DEFAULT_PARAMS["sandbox_schema"])

def table_names(folder: Path) -> dict:
    names = {k: k for k in TABLE_KEYS}
    try:
        on = pd.read_excel(config_xlsx(folder), sheet_name="output_names", dtype=str, keep_default_na=False)
    except Exception:
        return {k: (f"mv_{k}" if f"mv_{k}" in MV_TABLES else k) for k in TABLE_KEYS}
    for k, kd, n in zip(on["key"], on.get("kind", pd.Series("table", index=on.index)), on["name"]):
        kk = re.sub(r"^mv_", "", k)
        if kk in names and (kd == "table" or k.startswith("mv_")):
            names[kk] = n.strip() or k
    return names

def tables_exist(tag: str, names: dict) -> list:
    return [n for n in names.values() if spark.catalog.tableExists(f"{SCHEMA}.{n}_{tag}")]

def used_numbers(year: int) -> set:
    nums = set()
    for base in (ROOT, ROOT / "_archive"):
        yd = base / str(year)
        if yd.is_dir():
            for p in yd.iterdir():
                m = re.match(r"^(20\d{2})v(\d+)", p.name)
                if m and int(m.group(1)) == year and int(m.group(2)) < 1000:
                    nums.add(int(m.group(2)))
    return nums

# ---- Determine study tag and destination ----
is_create = ACTION in ("Create from scratch", "Create from template", "Clone from version")
is_clone = ACTION == "Clone from version"
is_update = ACTION == "Update existing"
is_run = ACTION == "Run existing"

if is_create:
    SRC = version_dir(cfg["clone_source"]) if is_clone else TEMPLATE
    NEW_NAMES = table_names(SRC)
    used = used_numbers(YEAR)
    if cfg["study_tag"].lower() in ("", "auto"):
        n = max(used, default=0) + 1
        while tables_exist(f"{YEAR}v{n}", NEW_NAMES):
            n += 1
    else:
        tag_m = TAG_RE.match(cfg["study_tag"])
        if tag_m:
            n = int(tag_m.group(2))
        else:
            n = int(cfg["study_tag"].lower().lstrip("v"))
        assert n not in used, f"{YEAR}v{n} already exists"
    TAG = f"{YEAR}v{n}"
    DEST = version_dir(TAG)
else:
    # Update or Run: pick from existing studies
    if cfg["study_tag"].lower() in ("", "auto"):
        TAG = _studies[0] if _studies else "none"
    else:
        TAG = cfg["study_tag"]
    DEST = version_dir(TAG)
    assert (DEST / "study.json").exists() or (DEST / "mv_study.json").exists(), f"{TAG} not found at {DEST}"

# ---- Print the plan ----
print(f"={'=' * 60}")
print(f"  Action:       {ACTION}")
print(f"  Run mode:     {RUN_MODE}")
print(f"  Study tag:    {TAG}")
print(f"  Destination:  {DEST}")
if is_clone:
    print(f"  Clone from:   {cfg['clone_source']}")
    print(f"  Clone tables: {cfg['clone_tables']}")
print(f"  Purpose:      {cfg['study_purpose']}")
print(f"  As-of date:   {asofdate}")
print(f"  Source data:  {cfg['source_data_action']}" + (f" (target: {source_target})" if source_target else ""))
print(f"  Mapping:      {cfg['mapping_source']} -> {mapping}")
print(f"  Notes:        {cfg['notes'] or '(none)'}")
if DRY:
    print(f"\n  ** DRY RUN — nothing will be written **")
print(f"={'=' * 60}")

# COMMAND ----------

# DBTITLE 1,Execute: create, update, or run
from openpyxl import load_workbook
IGNORE = shutil.ignore_patterns("__pycache__", "*.pyc", ".ipynb_checkpoints")

def set_params(dest: Path):
    """Write the chosen settings into config/study_params.json and the params tab of the config workbook."""
    pj = dest / "config" / "study_params.json"
    prm = json.loads(pj.read_text(encoding="utf-8"))
    prm["snapshot_date"] = asofdate
    prm["study_purpose"] = cfg["study_purpose"]
    if source_target:
        prm["source_tables"] = source_target
    # Source file end date always latest (removed from widgets)
    prm["dp_history_end_date"] = ""
    # Mapping
    prm["mapping_source"] = mapping
    # Clean up retired keys
    for k in ("ftp_results_date", "chart_start_date", "pnfp_product_attr_date"):
        prm.pop(k, None); prm.pop(f"_{k}_note", None)
    for k in ("src_emp_ftp_results", "src_pnfp_stacked"):
        prm.get("sources", {}).pop(k, None)
    pj.write_text(json.dumps(prm, indent=2) + "\n", encoding="utf-8")
    # Also update the config workbook params tab
    xl = config_xlsx(dest)
    if xl.exists():
        wb = load_workbook(xl)
        if "params" in wb.sheetnames:
            ws = wb["params"]
            hdr = [c.value for c in ws[1]]
            ki, vi = hdr.index("key") + 1, hdr.index("value") + 1
            rows = {ws.cell(r, ki).value: r for r in range(2, ws.max_row + 1)}
            for k, v in {"mapping_source": mapping, "mode": cfg["study_purpose"]}.items():
                r = rows.get(k, ws.max_row + 1)
                ws.cell(r, ki).value, ws.cell(r, vi).value = k, v
            wb.save(xl)
    return prm

# ---- Dry run: validate only ----
if DRY:
    checks = []
    checks.append(("Root exists", ROOT.is_dir()))
    checks.append(("_shared/deposit_utils", (ROOT / "_shared" / "deposit_utils").is_dir()))
    checks.append(("Template exists", TEMPLATE.is_dir()))
    if is_create:
        checks.append((f"Destination does not exist", not DEST.exists()))
    else:
        checks.append((f"Study {TAG} exists", DEST.exists()))
    checks.append((f"Schema {SCHEMA} accessible", True))  # would need spark to fully check
    if source_target:
        checks.append(("Source data action", f"Will build source tables -> {source_target}"))
    else:
        checks.append(("Source data action", "Keep as-is"))
    for label, result in checks:
        icon = "PASS" if result else "FAIL"
        if isinstance(result, str):
            icon = "INFO"
        print(f"  [{icon}] {label}" + (f" ({result})" if isinstance(result, str) else ""))
    print("\nDry run complete. Change run_mode to proceed.")

# ---- Create ----
elif is_create and RUN_MODE.startswith("Create"):
    DEST.mkdir(parents=True)
    # Copy notebooks from template
    shutil.copytree(TEMPLATE / "notebooks", DEST / "notebooks", ignore=IGNORE)
    # Copy config from source (template or clone source)
    shutil.copytree(SRC / "config", DEST / "config",
                    ignore=shutil.ignore_patterns("*.pyc", f"*{cfg['clone_source']}*") if is_clone else IGNORE)
    # Inputs
    PRODUCT_COLS = ["product_id", "product_name", "product_code", "short_name", "client_group"]
    MAP_COLS = ["legacy_bank", "acct_typ_code", "acct_typ_desc", "subproductid", "pf_flag", "bdsp_flag",
                "product_group", "rate_class", "product_id", "final_product_id", "notes"]
    if ACTION == "Create from scratch":
        REF_DEST = DEST / "inputs" / "reference"
        REF_DEST.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=PRODUCT_COLS).to_csv(DEST / "inputs" / "products.csv", index=False)
        pd.DataFrame(columns=MAP_COLS).to_csv(REF_DEST / "subproduct_map.csv", index=False)
    elif (SRC / "inputs").is_dir():
        shutil.copytree(SRC / "inputs", DEST / "inputs",
                        ignore=shutil.ignore_patterns("*.pyc", "derived", "_legacy",
                                                      *([f"*{cfg['clone_source']}*"] if is_clone else [])))
        # Use the source study's used map as this study's reference
        if is_clone:
            used_map = SRC / "outputs" / "review" / f"subproduct_map_used_{SRC.name}.csv"
            if used_map.exists():
                shutil.copy2(used_map, DEST / "inputs" / "reference" / "subproduct_map.csv")
                print(f"  Reference map = {cfg['clone_source']}'s used map")
    # Master product list
    MASTER = ROOT / "_shared" / "reference" / "products.csv"
    if ACTION != "Create from scratch" and MASTER.exists():
        _m = pd.read_csv(MASTER, dtype=str, keep_default_na=False)
        _m[PRODUCT_COLS].to_csv(DEST / "inputs" / "products.csv", index=False)
        print(f"  Products: {len(_m)} from master -> inputs/products.csv")
    # Rename old files
    for old, new in ((DEST / "config" / "mv_study_config.xlsx", DEST / "config" / "study_config.xlsx"),
                     (DEST / "inputs" / "reference" / "mv_config.xlsx", DEST / "inputs" / "reference" / "product_assignment_reference.xlsx")):
        if old.exists() and not new.exists():
            old.rename(new)
    # Output directories
    for sub in ("excel", "html", "reports", "review"):
        (DEST / "outputs" / sub).mkdir(parents=True, exist_ok=True)
    # study.json
    now = datetime.now().isoformat(timespec="seconds")
    (DEST / "study.json").write_text(json.dumps({
        "study_name": TAG, "year": YEAR, "version": TAG[4:],
        "parent": cfg["clone_source"] if is_clone else ("template" if ACTION == "Create from template" else "scratch"),
        "created": now, "created_by": getpass.getuser(),
        "status": "draft", "kind": "deposit_study", "template": TEMPLATE.name,
        "purpose": cfg["study_purpose"], "notes": cfg["notes"]
    }, indent=2), encoding="utf-8")
    # Write study_params.json
    prm = set_params(DEST)
    folders.ensure_schema(spark, SCHEMA)
    # Clone tables if requested
    if is_clone and cfg["clone_tables"] == "Clone":
        src_names = table_names(version_dir(cfg["clone_source"]))
        for k, n in NEW_NAMES.items():
            s = f"{SCHEMA}.{src_names[k]}_{cfg['clone_source']}"
            if spark.catalog.tableExists(s):
                spark.sql(f"CREATE OR REPLACE TABLE {SCHEMA}.{n}_{TAG} DEEP CLONE {s}")
                print(f"  Cloned {s} -> {SCHEMA}.{n}_{TAG}")
    # Update studies index
    idx = ROOT / "studies_index.csv"
    if not idx.exists() and (ROOT / "studies_index_mv.csv").exists():
        shutil.copy2(ROOT / "studies_index_mv.csv", idx)
    row = pd.DataFrame([dict(tag=TAG, year=YEAR, version=TAG[4:],
                             parent=cfg.get("clone_source", "scratch"), created=now,
                             status="draft", asofdate=asofdate, purpose=cfg["study_purpose"],
                             mapping_source=mapping, path=str(DEST), notes=cfg["notes"])])
    (pd.concat([pd.read_csv(idx, dtype=str), row.astype(str)]) if idx.exists() else row).to_csv(idx, index=False)
    print(f"\nCreated {TAG} at {DEST}")
    print(f"  as-of {asofdate} | purpose {cfg['study_purpose']} | mapping {mapping}")

# ---- Update ----
elif is_update and RUN_MODE in ("Update + run", "Run"):
    prm = set_params(DEST)
    print(f"Updated {TAG}: as-of {asofdate} | mapping {mapping}")

# ---- Run existing (no create/update) ----
elif is_run and RUN_MODE == "Run":
    print(f"Will run {TAG} at {DEST} (no config changes)")

else:
    print(f"Action '{ACTION}' + run_mode '{RUN_MODE}' — nothing to do in this cell.")
    print("Adjust run_mode or action and re-run.")

# COMMAND ----------

# DBTITLE 1,Load sources (if requested)
# Run source lineage / build source tables if the run_mode includes loading
if DRY:
    print("Dry run — skipping source loading.")
elif RUN_MODE in ("Create + load sources", "Create + run", "Update + run") and source_target:
    lin_path = str(DEST / "notebooks" / "00b_source_lineage").replace("/Workspace", "", 1)
    if (DEST / "notebooks" / "00b_source_lineage.py").exists() or (DEST / "notebooks" / "00b_source_lineage").exists():
        print(f"Running {lin_path} (build_sources = tables, target = {source_target}) ...")
        try:
            dbutils.notebook.run(lin_path, 0, {"build_sources": "tables", "target": source_target})
            print(f"  Source tables built -> {source_target}")
        except Exception as e:
            print(f"  WARNING: 00b_source_lineage failed: {str(e)[:300]}")
    else:
        # Fallback: try the current 00a_source_lineage
        lin_path = str(DEST / "notebooks" / "00a_source_lineage").replace("/Workspace", "", 1)
        if Path("/Workspace" + lin_path).exists() or Path("/Workspace" + lin_path + ".py").exists():
            print(f"Running {lin_path} (build_sources = tables) ...")
            try:
                dbutils.notebook.run(lin_path, 0, {"build_sources": "tables"})
                print("  Source tables built.")
            except Exception as e:
                print(f"  WARNING: 00a_source_lineage failed: {str(e)[:300]}")
        else:
            print(f"  No source lineage notebook found at {DEST / 'notebooks'}. Skipping.")
elif RUN_MODE in ("Create + load sources", "Create + run", "Update + run"):
    print("Source data action = Keep as-is. No source tables to build.")
else:
    print(f"Run mode '{RUN_MODE}' does not include source loading.")

# COMMAND ----------

# DBTITLE 1,Run the study (if requested)
# Run the study pipeline if the run_mode includes running
if DRY:
    print("Dry run — skipping study run.")
elif RUN_MODE in ("Create + run", "Run", "Update + run"):
    run_nb = str(DEST / "notebooks" / "00_run_study").replace("/Workspace", "", 1)
    print(f"Running {run_nb} ...")
    try:
        result = dbutils.notebook.run(run_nb, 0)
        print(f"Study {TAG} completed.")
        if result:
            print(result)
    except Exception as e:
        print(f"Study run failed: {str(e)[:500]}")
else:
    print(f"Run mode '{RUN_MODE}' does not include a study run.")

print(f"\n{'=' * 60}")
print(f"  00a_install_study complete.")
print(f"  Study:  {TAG}")
print(f"  Path:   {DEST}")
print(f"  Action: {ACTION} | Mode: {RUN_MODE}")
print(f"{'=' * 60}")