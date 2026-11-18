# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # install_study
# MAGIC One-time (or occasional) setup of the deposit study layout. Run top to bottom; `dry_run = yes` (the default) only prints what it would do. Then create and run studies with **`execute_study`** (same folder).
# MAGIC
# MAGIC | Step | What it does |
# MAGIC |---|---|
# MAGIC | 1. Template | Builds `_templates/` (no `_mv`) from the study you pick in `seed_study` (default `2027v10`): its notebooks, `config/` (the config workbook is saved as `study_config.xlsx`), and the input files that are not tied to one tag (`inputs/reference/`, `inputs/product_lookup.csv`). Files built per tag (`*_2027v10.*`) are skipped. An older `inputs/reference/mv_config.xlsx` is saved as `product_assignment_reference.xlsx`. |
# MAGIC | 2. Output names | Writes the `output_names` tab of `_templates/config/study_config.xlsx`: every table and file a study writes, with the name new studies will use. Tables get `table_prefix` + the plain name (plain names), e.g. `accounts`, `final_betas`; with `table_prefix = dep` they become `dep_accounts`, `dep_final_betas` (the `_` is added). Edit the `name` column there (or in any study's own config) to rename anything. |
# MAGIC | 3. Tidy root | Moves the old root items (`install_studies_mv`, `setup_study_mv`, `_templates_mv`, `studies_index_mv.csv`) into `_archive/pre_rename_<date>/`. `studies_index_mv.csv` is copied to `studies_index.csv` first. Year folders, `_shared`, `_archive`, `_templates` and `install/` are never moved. |
# MAGIC
# MAGIC | Widget | Use |
# MAGIC |---|---|
# MAGIC | `seed_study` | The study the template is copied from. Pick the one whose notebooks you trust most |
# MAGIC | `template_overwrite` | `missing only` = never replace a template file; `notebooks` = refresh the notebooks from the seed; `everything` = notebooks + config + inputs |
# MAGIC | `table_prefix` | Put in front of every table name of **new** studies (blank = none, e.g. `finance.sandbox.accounts_2028v1`). Existing studies keep their names |
# MAGIC | `tidy_root` | `yes` = run step 3 |
# MAGIC | `dry_run` | `yes` = print the plan only |

# COMMAND ----------

# DBTITLE 1,Settings
import subprocess, sys
subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "openpyxl"])

import os, re, json, shutil
from datetime import date
from pathlib import Path
import pandas as pd

# studies root = the folder above install/ (where this notebook lives)
try:
    _nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    _here = Path(_nb if _nb.startswith("/Workspace") else "/Workspace" + _nb).parent
except Exception:
    _here = Path(os.getcwd())
DEFAULT_ROOT = str(_here.parent if _here.name == "install" else _here)
ROOT0 = Path(DEFAULT_ROOT)

TAG_RE = re.compile(r"^(20\d{2})v(\d+)$")
def list_studies(root: Path) -> list:
    """Study tags at <root>/<year>/<tag> (folders holding study.json or mv_study.json), newest first."""
    out = []
    for yd in sorted(root.glob("20[0-9][0-9]")):
        for p in yd.iterdir():
            m = TAG_RE.match(p.name)
            if p.is_dir() and m and int(m.group(2)) < 1000 and ((p / "study.json").exists() or (p / "mv_study.json").exists()):
                out.append(p.name)                                    # 2027v1030, 2027v10302026 ... are stray folders
    return sorted(out, key=lambda t: (int(t[:4]), int(t[5:])), reverse=True)

_studies = list_studies(ROOT0)
_has_templates = (ROOT0 / "_templates" / "notebooks").is_dir()
if not _studies and _has_templates:
    _studies = ["(skip - templates exist)"]
elif not _studies:
    _studies = ["none"]
_seed_default = "2027v10" if "2027v10" in _studies else _studies[0]

W = dbutils.widgets
W.text("studies_root", DEFAULT_ROOT, "Studies root")
W.dropdown("seed_study", _seed_default, _studies, "1. Seed template from")
W.dropdown("template_overwrite", "missing only", ["missing only", "notebooks", "everything"], "1. Replace template files")
W.text("table_prefix", "", "2. Table prefix (new studies)")
W.dropdown("tidy_root", "no", ["no", "yes"], "3. Archive old root items")
W.dropdown("dry_run", "yes", ["yes", "no"], "Dry run")

ROOT = Path(W.get("studies_root").strip())
ARCHIVE, TEMPLATE = ROOT / "_archive", ROOT / "_templates"
SEED_TAG = W.get("seed_study")
SKIP_SEED = SEED_TAG.startswith("(skip") or SEED_TAG == "none"
SEED = None if SKIP_SEED else ROOT / SEED_TAG[:4] / SEED_TAG
OVR = W.get("template_overwrite")
PREFIX = re.sub(r"[^A-Za-z0-9_]", "", W.get("table_prefix").strip())
PREFIX = PREFIX + "_" if PREFIX and not PREFIX.endswith("_") else PREFIX     # dep -> dep_accounts_<tag>
DRY = W.get("dry_run") == "yes"
assert (ROOT / "_shared" / "deposit_utils").is_dir(), f"_shared/deposit_utils not found in {ROOT}. Run the exports notebook extract step first."
if SKIP_SEED:
    assert (ROOT / "_templates" / "notebooks").is_dir(), f"No seed study and no _templates/ found in {ROOT}. Extract the ZIP first."
    print("Seed: SKIPPED (using existing _templates/)")
else:
    assert SEED.is_dir(), f"seed study not found: {SEED}"
IGNORE = shutil.ignore_patterns(".pytest_cache", "__pycache__", "*.pyc", ".ipynb_checkpoints")
log = []
print(f"Root {ROOT} | seed {'SKIP' if SKIP_SEED else SEED_TAG} | replace: {OVR} | table prefix '{PREFIX}' | dry run {DRY}")
print("Studies found:", ", ".join(s for s in _studies if not s.startswith("(")))

# COMMAND ----------

# DBTITLE 1,1. Build _templates from the seed study
def put(src: Path, dest: Path, label: str, kind: str):
    """Copy one file into the template. kind = notebook | data (decides whether template_overwrite replaces it)."""
    replace = OVR == "everything" or (OVR == "notebooks" and kind == "notebook")
    exists = dest.exists()
    status = "kept (exists)" if exists and not replace else ("would write" if DRY else ("replaced" if exists else "written"))
    if not DRY and (not exists or replace):
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
    log.append(dict(step="template", item=label, to=str(dest), status=status))
    print(f"  {status:14s} {label}")

if SKIP_SEED:
    print(f"SKIPPED step 1: using existing templates at {TEMPLATE}")
    _nbs = sum(1 for _ in (TEMPLATE / 'notebooks').rglob('*') if _.is_file()) if (TEMPLATE / 'notebooks').is_dir() else 0
    _cfg = sum(1 for _ in (TEMPLATE / 'config').rglob('*') if _.is_file()) if (TEMPLATE / 'config').is_dir() else 0
    _inp = sum(1 for _ in (TEMPLATE / 'inputs').rglob('*') if _.is_file()) if (TEMPLATE / 'inputs').is_dir() else 0
    print(f"  notebooks: {_nbs} files | config: {_cfg} files | inputs: {_inp} files")
else:
    print(f"Seed: {SEED}\nTemplate: {TEMPLATE}")
    # notebooks, including the module subfolders (1_accounts_products/, 2_betas/, ...) and notebooks/sql/
    for nb in sorted((SEED / "notebooks").rglob("*")):
        rel = nb.relative_to(SEED / "notebooks")
        if nb.is_file() and not any(part.startswith(".") or part == "__pycache__" for part in rel.parts):
            put(nb, TEMPLATE / "notebooks" / rel, f"notebooks/{rel}", "notebook")

    # config: the workbook is renamed study_config.xlsx
    cfg_src = next((SEED / "config" / f for f in ("study_config.xlsx", "mv_study_config.xlsx") if (SEED / "config" / f).exists()), None)
    assert cfg_src is not None, "no config workbook in the seed study"
    put(cfg_src, TEMPLATE / "config" / "study_config.xlsx", f"config/study_config.xlsx (from {cfg_src.name})", "data")
    for f in ("study_params.json", "rate_cycles.csv"):
        put(SEED / "config" / f, TEMPLATE / "config" / f, f"config/{f}", "data")

    # inputs that are not built per tag
    for p in sorted((SEED / "inputs").rglob("*")):
        if p.is_file() and SEED_TAG not in p.name and "__pycache__" not in str(p) and not {"derived", "_legacy"} & set(p.relative_to(SEED).parts):
            rel = p.relative_to(SEED)
            if p.name == "mv_config.xlsx":                       # renamed: the Level 2 reference workbook
                rel = rel.with_name("product_assignment_reference.xlsx")
            put(p, TEMPLATE / rel, str(rel), "data")
    # the template's reference map = the map the seed study used (when it has run with the products.csv layout)
    _used = SEED / "outputs" / "review" / f"subproduct_map_used_{SEED_TAG}.csv"
    if _used.exists():
        put(_used, TEMPLATE / "inputs" / "reference" / "subproduct_map.csv", "inputs/reference/subproduct_map.csv (seed's used map)", "data")

# COMMAND ----------

# DBTITLE 1,2. Output names for new studies (study_config.xlsx / output_names)
# key = the name the notebooks use (do not change); name = what new studies write. Same keys as OUTPUT_KEYS in 00_setup.
OUTPUT_KEYS = [
    ("lsnv_dp_hist", "table", "00a", "SNV DP history built from the raw files (only when 00a build_sources = tables)"),
    ("lpnfp_dp_hist", "table", "00a", "PNFP DP history built from the WAL Deps files (only when 00a build_sources = tables)"),
    ("accounts", "table", "01", "Accounts at the snapshot"),
    ("account_war", "table", "01", "Monthly balance and WAR by subproduct"),
    ("account_metrics", "table", "02", "Subproduct metrics at the snapshot"),
    ("account_betas", "table", "03", "Betas by subproduct"),
    ("product_selection", "table", "04", "Subproduct -> product assignment"),
    ("product_map", "table", "05", "Accounts with their product"),
    ("product_war", "table", "05", "Monthly balance and WAR by product"),
    ("product_betas", "table", "05", "Betas by product"),
    ("beta_review", "table", "06", "Recommended method by product"),
    ("final_betas", "table", "06", "Final betas by product"),
    ("account_mapping", "inputs", "01", "Account mapping workbook"),
    ("account_metrics", "inputs", "02", "Account metrics workbook"),
    ("product_assignment", "inputs", "04", "Product assignment workbook"),
    ("accounts", "excel", "01", "Accounts workbook"),
    ("account_betas", "excel", "03", "Subproduct betas workbook"),
    ("Account_Review", "excel", "03a", "Account review workbook"),
    ("product_map", "excel", "05", "Product map workbook"),
    ("product_beta_report", "excel", "05", "Product beta report"),
    ("beta_review", "excel", "06", "Beta review workbook"),
    ("final_betas", "excel", "06", "Final betas (csv)"),
    ("study_summary", "excel", "06", "Study summary workbook"),
    ("Beta_Report", "excel", "08", "Styled beta report"),
    ("Account_Review_Dashboard", "html", "03a", "Account review dashboard"),
    ("study_dashboard", "html", "06", "Study dashboard"),
    ("Fed_Funds_Cycles", "html", "08", "Fed funds cycles chart"),
    ("Product_Dashboard", "html", "08", "Product dashboard"),
    ("Product_Mapping_Dashboard", "html", "08", "Product mapping dashboard"),
    ("study_flow", "html, reports", "07", "Study flow page and markdown"),
    ("study_report", "reports", "06", "Study report (markdown)"),
    ("known_issues", "reports", "all", "Known issues log"),
    ("source_lineage", "reports", "00a", "Source data lineage checks"),
    ("deposit_study_package", "folder", "09", "Input files + mapping + betas + results + R script (outputs/<name>)"),
]
RENAME = {"deposit_study_package": "{tag} Deposit Study"}

def new_name(key, kind):
    if kind == "table":
        return PREFIX + key
    return RENAME.get(key, key)

names = pd.DataFrame([dict(key=k, kind=kd, notebook=nb, description=d, name=new_name(k, kd)) for k, kd, nb, d in OUTPUT_KEYS])
display(names)
xl = TEMPLATE / "config" / "study_config.xlsx"
if DRY or not xl.exists():
    print("would write the output_names tab of", xl)
    log.append(dict(step="output_names", item="config/study_config.xlsx / output_names", to=str(xl), status="would write"))
else:
    from openpyxl import load_workbook
    wb = load_workbook(xl)
    ws = wb["output_names"] if "output_names" in wb.sheetnames else wb.create_sheet("output_names")
    ws.delete_rows(1, ws.max_row)
    ws.append(list(names.columns))
    for r in names.itertuples(index=False):
        ws.append(list(r))
    for col, w in zip("ABCDE", (26, 14, 10, 60, 34)):
        ws.column_dimensions[col].width = w
    wb.save(xl)
    log.append(dict(step="output_names", item="config/study_config.xlsx / output_names", to=str(xl), status="written"))
    print("written: output_names tab of", xl)

# COMMAND ----------

# DBTITLE 1,3. Tidy the root (old *_mv items -> _archive)
OLD_ITEMS = ["install_studies_mv.ipynb", "install_studies_mv", "setup_study_mv.ipynb", "setup_study_mv", "_templates_mv", "studies_index_mv.csv"]
DEST = ARCHIVE / f"pre_rename_{date.today():%Y-%m-%d}"
if W.get("tidy_root") == "yes":
    idx_old, idx_new = ROOT / "studies_index_mv.csv", ROOT / "studies_index.csv"
    if idx_old.exists() and not idx_new.exists():
        status = "would copy" if DRY else "copied"
        if not DRY:
            shutil.copy2(idx_old, idx_new)
        log.append(dict(step="tidy", item="studies_index_mv.csv -> studies_index.csv", to=str(idx_new), status=status))
        print(f"  {status:14s} studies_index_mv.csv -> studies_index.csv")
    for name in OLD_ITEMS:
        p = ROOT / name
        if not p.exists():
            continue
        d = DEST / name
        status = "SKIP (already archived)" if d.exists() else ("would move" if DRY else "moved")
        if not DRY and not d.exists():
            DEST.mkdir(parents=True, exist_ok=True)
            shutil.move(str(p), str(d))
        log.append(dict(step="tidy", item=name, to=str(d), status=status))
        print(f"  {status:14s} {name} -> {d.relative_to(ROOT)}")
else:
    print("tidy_root = no, skipping.")

# COMMAND ----------

# DBTITLE 1,Result
display(pd.DataFrame(log))
if DRY:
    print("DRY RUN. Set dry_run = no and run again to apply.")
else:
    print("Root:", sorted(p.name for p in ROOT.iterdir()))
    print("Next: open install/execute_study.")

# COMMAND ----------

