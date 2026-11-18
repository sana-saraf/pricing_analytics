# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # execute_study
# MAGIC Creates, updates or runs a deposit study. Run `install_study` once first (it builds `_templates/`).
# MAGIC
# MAGIC **A new study** goes to `<studies_root>/<year>/<year>v<N>/` with `config/`, `inputs/`, `notebooks/` (from `_templates`) and `outputs/`. Its tables and files are named by the `output_names` tab of `config/study_config.xlsx` (plain names, e.g. `accounts_<tag>`). Rename anything there.
# MAGIC
# MAGIC | Widget | Use | Pick |
# MAGIC |---|---|---|
# MAGIC | `action` | `create` a new study, `update` a study's settings, `run` a study (00_run_study) | |
# MAGIC | `study` | The study to update or run. Ignored by `create` | e.g. `2027v10` |
# MAGIC | `year`, `version` | For `create`: the study year, and `auto` (next free number) or e.g. `v12` | `auto` |
# MAGIC | `snapshot_date` | Month-end for balances, WAR and the account reports. `latest` = the latest month both PNFP and SNV sources have | `latest` unless you need a fixed date |
# MAGIC | `dp_history_end_date` | Last month `00a_source_lineage` reads from the history / WAL Deps files. `latest` = the newest file in the folder | `latest` |
# MAGIC | `source_tables` | What 01 reads. `production` = `lsnv_nmds` / `pnfp_deposits`; `study` = the `lsnv_dp_hist` / `lpnfp_dp_hist` tables 00a builds from the raw files | `production` |
# MAGIC | `clone_from` (Start from) | `create` only. `template` = config and inputs from `_templates`. `scratch` = config from `_templates`, an **empty** `inputs/products.csv` and reference map; `mapping_source` / `assignment_source` are set to `DERIVED_ELSE_REFERENCE`, and 01 stops and lists what to fill. `<study>` = that study's config, `products.csv`, and the map it used as the new reference. Notebooks always come from the template | `template`, or the study you are changing |
# MAGIC | `clone_tables` | `yes` also DEEP CLONEs the source study's tables | `no` |
# MAGIC | `mapping_source` | Where account -> product comes from (01). `DERIVED` = the rules in 01 (SQL you can edit), `REFERENCE` = the csv files in `inputs/reference/`, `DERIVED_ELSE_REFERENCE` = rules first, csv for gaps. See `notebooks/00_control_panel` | `DERIVED` |
# MAGIC | `assignment_source` | Same choice for subproduct -> final product (04) | `DERIVED` |
# MAGIC | `mode` | `FINAL` = every subproduct must have a product (unassigned ones are flagged HIGH); `PREVIEW` = keep current products | `FINAL` for a delivered study |
# MAGIC | `run_source_check` | Run `notebooks/00a_source_lineage` first: `yes (look only)`, or `yes (build tables)` to (re)build `lsnv_dp_hist` / `lpnfp_dp_hist` | `no`; `yes (build tables)` once when `source_tables = study` |
# MAGIC | `run_study` | `yes` = run `notebooks/00_run_study` after create / update | |
# MAGIC | `notes` | One line on why this study exists | |
# MAGIC | `dry_run` | `yes` = show the plan, write nothing | start with `yes` |
# MAGIC
# MAGIC For `update`, `keep` leaves a setting unchanged. If a new study is missing from the `study` list, run the **Reset widgets** cell once and then run all.

# COMMAND ----------

# DBTITLE 1,Install openpyxl
# MAGIC %pip install -q openpyxl

# COMMAND ----------

# DBTITLE 1,Reset widgets (only when a dropdown list is out of date)
# Dropdown lists (studies, month-ends) are filled when the widgets are first created.
# Uncomment and run this cell once to rebuild them, then run the notebook from the top.
# dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Settings
import sys, os, re, json, shutil, getpass
from datetime import datetime, date, timedelta
from pathlib import Path
import pandas as pd

try:
    _nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    _here = Path(_nb if _nb.startswith("/Workspace") else "/Workspace" + _nb).parent
except Exception:
    _here = Path(os.getcwd())
DEFAULT_ROOT = str(_here.parent if _here.name == "install" else _here)
ROOT0 = Path(DEFAULT_ROOT)
TEMPLATE0 = next((ROOT0 / t for t in ("_templates", "_templates_mv") if (ROOT0 / t / "notebooks").is_dir()), ROOT0 / "_templates")

TAG_RE = re.compile(r"^(20\d{2})v(\d+)$")
def list_studies(root: Path) -> list:
    out = []
    for yd in sorted(root.glob("20[0-9][0-9]")):
        for p in yd.iterdir():
            m = TAG_RE.match(p.name)
            if p.is_dir() and m and int(m.group(2)) < 1000 and ((p / "study.json").exists() or (p / "mv_study.json").exists()):
                out.append(p.name)                                    # 2027v1030, 2027v10302026 ... are stray folders
    return sorted(out, key=lambda t: (int(t[:4]), int(t[5:])), reverse=True)

def month_ends(last: date, n=36) -> list:
    out, y, m = [], last.year, last.month
    for _ in range(n):
        nxt = date(y + (m == 12), m % 12 + 1, 1)
        out.append(str(date.fromordinal(nxt.toordinal() - 1)))
        y, m = (y - 1, 12) if m == 1 else (y, m - 1)
    return out

# latest month-end both model sources have (from the template's study_params.json)
_src = {}
try:
    _src = json.loads((TEMPLATE0 / "config" / "study_params.json").read_text(encoding="utf-8")).get("sources", {})
except Exception:
    pass
try:
    _snv = spark.sql(f"SELECT MAX(EOPDate) d FROM {_src.get('src_lsnv_nmds', 'finance.pricing_analytics.lsnv_nmds')}").first().d
    _pnfp = spark.sql(f"SELECT MAX(load_date) d FROM {_src.get('src_pnfp_deposits', 'finance.ftp_validation.pnfp_deposits')}").first().d
    _snv, _pnfp = pd.Timestamp(_snv).date(), pd.Timestamp(_pnfp).date()
    LATEST = min(_snv, _pnfp)
except Exception as e:
    print("could not read the source dates:", str(e)[:200])
    _snv = _pnfp = LATEST = date.today()
if (LATEST + timedelta(days=1)).day != 1:                     # not a month-end: use the month-end before it
    LATEST = LATEST.replace(day=1) - timedelta(days=1)
MONTHS = month_ends(LATEST)

_studies = list_studies(ROOT0)
_yr = sorted({str(date.today().year + i) for i in (-1, 0, 1, 2)} | {t[:4] for t in _studies})

W = dbutils.widgets
W.text("studies_root", DEFAULT_ROOT, "Studies root")
W.dropdown("action", "create", ["create", "update", "run"], "01 Action")
W.dropdown("study", _studies[0] if _studies else "none", _studies or ["none"], "02 Study (update / run)")
W.dropdown("year", str(date.today().year + 1) if str(date.today().year + 1) in _yr else _yr[-1], _yr, "03 Year (create)")
W.text("version", "auto", "04 Version (auto or vN)")
try:
    W.remove("ftp_results_date")                                # retired (no notebook used it)
except Exception:
    pass
W.dropdown("snapshot_date", "latest", ["latest", "keep"] + MONTHS, "05 Snapshot date")
W.dropdown("dp_history_end_date", "latest", ["latest", "keep"] + MONTHS, "06 Source files end (00a)")
W.dropdown("source_tables", "production", ["production", "study", "keep"], "07 Source tables (01)")
try:
    if W.get("clone_from") == "none":                         # the old default: same as template
        W.remove("clone_from")
except Exception:
    pass
W.dropdown("clone_from", "template", ["template", "scratch"] + _studies, "08 Start from (create)")
W.dropdown("clone_tables", "no", ["no", "yes"], "09 Clone tables too")
W.dropdown("mapping_source", "DERIVED", ["DERIVED", "DERIVED_ELSE_REFERENCE", "REFERENCE", "keep"], "10 Mapping source (01)")
W.dropdown("assignment_source", "DERIVED", ["DERIVED", "DERIVED_ELSE_REFERENCE", "REFERENCE", "keep"], "11 Assignment source (04)")
W.dropdown("mode", "FINAL", ["FINAL", "PREVIEW", "keep"], "12 Mode")
W.dropdown("run_source_check", "no", ["no", "yes (look only)", "yes (build tables)"], "13 Run 00a source check")
W.dropdown("run_study", "no", ["no", "yes"], "14 Run the study after")
W.text("notes", "", "15 Notes")
W.dropdown("dry_run", "yes", ["yes", "no"], "16 Dry run")
cfg = {k: W.get(k).strip() for k in ["studies_root", "action", "study", "year", "version", "snapshot_date", "dp_history_end_date",
                                     "source_tables", "clone_from", "clone_tables", "mapping_source", "assignment_source",
                                     "mode", "run_source_check", "run_study", "notes", "dry_run"]}

ROOT = Path(cfg["studies_root"])
ARCHIVE = ROOT / "_archive"
TEMPLATE = next((ROOT / t for t in ("_templates", "_templates_mv") if (ROOT / t / "notebooks").is_dir()), None)
assert TEMPLATE is not None, "_templates not found: run install/install_study first"
DRY = cfg["dry_run"] == "yes"
sys.path.insert(0, str(ROOT / "_shared"))
from deposit_utils.study import MV_TABLES, DEFAULT_PARAMS
from deposit_utils import folders
SCHEMA = json.loads((TEMPLATE / "config" / "study_params.json").read_text(encoding="utf-8")).get("sandbox_schema", DEFAULT_PARAMS["sandbox_schema"])

def version_dir(tag: str) -> Path:
    m = TAG_RE.match(tag)
    assert m, f"bad tag {tag}"
    return ROOT / m.group(1) / tag

def config_xlsx(folder: Path) -> Path:
    return next((folder / "config" / f for f in ("study_config.xlsx", "mv_study_config.xlsx") if (folder / "config" / f).exists()),
                folder / "config" / "study_config.xlsx")

# table keys (the names the notebooks use); 00a's two source tables only exist when built
TABLE_KEYS = [re.sub(r"^mv_", "", k) for k in MV_TABLES] + ["lsnv_dp_hist", "lpnfp_dp_hist"]

def table_names(folder: Path) -> dict:
    """key -> table base name for a study or the template, from the output_names tab (older keys mv_<key> are read as <key>).
    A study without the tab is an older one: its tables are mv_<key>."""
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
    for base in (ROOT, ARCHIVE):
        yd = base / str(year)
        if yd.is_dir():
            for p in yd.iterdir():
                m = re.match(r"^(20\d{2})v(\d+)", p.name)
                if m and int(m.group(1)) == year and int(m.group(2)) < 1000:   # 1030, 10302026 ... are stray folders, not versions
                    nums.add(int(m.group(2)))
    return nums

print(f"Root {ROOT} | template {TEMPLATE.name} | action {cfg['action']} | dry run {DRY} | schema {SCHEMA}")
print(f"Latest source month-end: SNV {_snv} | PNFP {_pnfp} -> latest = {LATEST}")

# COMMAND ----------

# DBTITLE 1,Plan
ACTION = cfg["action"]
pick = lambda v: None if v in ("keep", "") else v
snapshot = str(LATEST) if cfg["snapshot_date"] == "latest" else pick(cfg["snapshot_date"])
source_tables = pick(cfg["source_tables"])
RUN_00A = {"no": None, "yes (look only)": "none", "yes (build tables)": "tables"}[cfg["run_source_check"]]
dp_end = "latest" if cfg["dp_history_end_date"] == "latest" else pick(cfg["dp_history_end_date"])
START = "template" if cfg["clone_from"] in ("none", "template") else cfg["clone_from"]    # template, scratch or a study tag
clone_from = None if START in ("template", "scratch") else START
if ACTION == "create" and START == "scratch":
    # nothing to copy a mapping from: the rules derive what they can, 01 lists the rest for you to fill
    cfg["mapping_source"] = cfg["assignment_source"] = "DERIVED_ELSE_REFERENCE"

if ACTION == "create":
    year = int(cfg["year"])
    assert snapshot, "snapshot_date is required for create (pick latest or a month-end)"
    SRC = version_dir(clone_from) if clone_from else TEMPLATE
    NEW_NAMES = table_names(SRC)
    used = used_numbers(year)
    if cfg["version"].lower() in ("", "auto"):
        n = max(used, default=0) + 1
        while tables_exist(f"{year}v{n}", NEW_NAMES):
            n += 1
    else:
        n = int(cfg["version"].lower().lstrip("v"))
        assert n not in used, f"{year}v{n} is already used in the root or _archive"
        assert not tables_exist(f"{year}v{n}", NEW_NAMES), f"tables for {year}v{n} already exist"
    TAG = f"{year}v{n}"
    DEST = version_dir(TAG)
    print(f"Will create {TAG} at {DEST}")
    print(f"  start from: {START} | notebooks from {TEMPLATE / 'notebooks'}; config from {SRC}; " + (
        "inputs: empty products.csv and reference map (01 stops and lists what to fill)" if START == "scratch" else
        f"inputs from {SRC} (products.csv; reference map = its used map)"))
    print(f"  tables: {SCHEMA}.<name>_{TAG}, e.g. {NEW_NAMES['final_betas']}_{TAG}")
    print(f"  version numbers already used for {year}: {sorted(used) or 'none'}")
else:
    TAG = cfg["study"]
    DEST = version_dir(TAG)
    assert (DEST / "study.json").exists() or (DEST / "mv_study.json").exists(), f"{TAG} not found at {DEST}"
    print(f"Will {ACTION} {TAG} at {DEST}")
if ACTION in ("create", "update"):
    print(f"  snapshot {snapshot or 'keep'} | source files end {dp_end or 'keep'} | source tables {source_tables or 'keep'}")
    print(f"  mapping {cfg['mapping_source']} | assignment {cfg['assignment_source']} | mode {cfg['mode']} | run after: 00a {cfg['run_source_check']}, study {cfg['run_study']}")
if source_tables == "study" and RUN_00A != "tables":
    print("  NOTE: source_tables = study needs the 00a tables (lsnv_dp_hist, lpnfp_dp_hist): pick 'yes (build tables)' once, or build them in 00a.")
if DRY:
    print("\nDRY RUN. Set dry_run = no to apply.")

# COMMAND ----------

# DBTITLE 1,Create or update the study
from openpyxl import load_workbook
IGNORE = shutil.ignore_patterns("__pycache__", "*.pyc", ".ipynb_checkpoints")

def set_params(dest: Path):
    """Write the chosen settings into config/study_params.json and the params tab of the config workbook."""
    pj = dest / "config" / "study_params.json"
    prm = json.loads(pj.read_text(encoding="utf-8"))
    if snapshot:
        prm["snapshot_date"] = snapshot
    if dp_end:
        prm["dp_history_end_date"] = "" if dp_end == "latest" else dp_end
        prm["_dp_history_end_date_note"] = "Last month 00a_source_lineage reads from the history / WAL Deps files. Blank = the newest file."
    if source_tables:
        prm["source_tables"] = source_tables
        prm["_source_tables_note"] = ("production = 01 reads src_lsnv_nmds / src_pnfp_deposits; study = 01 reads the lsnv_dp_hist / "
                                      "lpnfp_dp_hist tables 00a_source_lineage builds from the raw files (build_sources = tables).")
    for k in ("ftp_results_date", "chart_start_date", "pnfp_product_attr_date"):   # settings no notebook uses
        prm.pop(k, None)
        prm.pop(f"_{k}_note", None)
    for k in ("src_emp_ftp_results", "src_pnfp_stacked"):
        prm.get("sources", {}).pop(k, None)
    pj.write_text(json.dumps(prm, indent=2) + "\n", encoding="utf-8")

    xl = config_xlsx(dest)
    wb = load_workbook(xl)
    ws = wb["params"]
    hdr = [c.value for c in ws[1]]
    ki, vi = hdr.index("key") + 1, hdr.index("value") + 1
    want = {k: cfg[w] for k, w in (("mapping_source", "mapping_source"), ("assignment_source", "assignment_source"), ("mode", "mode"))
            if cfg[w] != "keep"}
    rows = {ws.cell(r, ki).value: r for r in range(2, ws.max_row + 1)}
    for k, v in want.items():
        r = rows.get(k) or ws.max_row + 1
        ws.cell(r, ki).value, ws.cell(r, vi).value = k, v
    wb.save(xl)
    return prm, want

if not DRY and ACTION == "create":
    DEST.mkdir(parents=True)
    shutil.copytree(TEMPLATE / "notebooks", DEST / "notebooks", ignore=IGNORE)
    shutil.copytree(SRC / "config", DEST / "config", ignore=shutil.ignore_patterns("*.pyc", f"*{clone_from}*") if clone_from else IGNORE)
    REF_DEST = DEST / "inputs" / "reference"
    PRODUCT_COLS = ["product_id", "product_name", "product_code", "short_name", "client_group"]
    MAP_COLS = ["legacy_bank", "acct_typ_code", "acct_typ_desc", "subproductid", "pf_flag", "bdsp_flag", "product_group", "rate_class",
                "product_id", "final_product_id", "notes"]
    if START == "scratch":
        REF_DEST.mkdir(parents=True)
        pd.DataFrame(columns=PRODUCT_COLS).to_csv(DEST / "inputs" / "products.csv", index=False)
        pd.DataFrame(columns=MAP_COLS).to_csv(REF_DEST / "subproduct_map.csv", index=False)
        if (SRC / "inputs" / "reference" / "subproductid_suffix_guide.md").exists():
            shutil.copy2(SRC / "inputs" / "reference" / "subproductid_suffix_guide.md", REF_DEST)
    elif (SRC / "inputs" / "products.csv").exists():
        # new layout: the product list, plus the map the source study used as this study's reference
        shutil.copytree(SRC / "inputs", DEST / "inputs", ignore=shutil.ignore_patterns(
            "*.pyc", "derived", "_legacy", *([f"*{clone_from}*"] if clone_from else [])))
        used = SRC / "outputs" / "review" / f"subproduct_map_used_{SRC.name}.csv"
        if clone_from and used.exists():
            shutil.copy2(used, REF_DEST / "subproduct_map.csv")
            print(f"  reference map = the map {clone_from} used ({used.name})")
    else:
        # older layout (dim_nmd.csv, product_lookup.csv, ...): copied as is, 00_setup converts it on the first run
        shutil.copytree(SRC / "inputs", DEST / "inputs", ignore=shutil.ignore_patterns("*.pyc", f"*{clone_from}*") if clone_from else IGNORE)
    for old, new in ((DEST / "config" / "mv_study_config.xlsx", DEST / "config" / "study_config.xlsx"),
                     (DEST / "inputs" / "reference" / "mv_config.xlsx", DEST / "inputs" / "reference" / "product_assignment_reference.xlsx")):
        if old.exists() and not new.exists():
            old.rename(new)
    # the global product list (_shared/reference/products.csv) is the starting product list of every new study
    # (scratch keeps its empty list). Edit the master there; 00_setup warns when a study's copy differs.
    MASTER = ROOT / "_shared" / "reference" / "products.csv"
    if START != "scratch" and MASTER.exists():
        _m = pd.read_csv(MASTER, dtype=str, keep_default_na=False)
        _m[PRODUCT_COLS].to_csv(DEST / "inputs" / "products.csv", index=False)
        _m[["product_id", "product_name", "product_code", "short_name"]].to_csv(DEST / "inputs" / "product_lookup.csv", index=False)
        print(f"  products: {len(_m)} from {MASTER.relative_to(ROOT)} -> inputs/products.csv and inputs/product_lookup.csv")
    for sub in ("excel", "html", "reports", "review"):
        (DEST / "outputs" / sub).mkdir(parents=True, exist_ok=True)
    now = datetime.now().isoformat(timespec="seconds")
    (DEST / "study.json").write_text(json.dumps({
        "study_name": TAG, "year": int(TAG[:4]), "version": TAG[4:], "parent": clone_from or START, "created": now,
        "created_by": getpass.getuser(), "status": "draft", "kind": "deposit_study", "template": TEMPLATE.name,
        "source_version": TAG, "notes": cfg["notes"]}, indent=2), encoding="utf-8")
    prm, want = set_params(DEST)
    folders.ensure_schema(spark, SCHEMA)
    if clone_from and cfg["clone_tables"] == "yes":
        src_names = table_names(version_dir(clone_from))
        for k, n in NEW_NAMES.items():
            s = f"{SCHEMA}.{src_names[k]}_{clone_from}"
            if spark.catalog.tableExists(s):
                spark.sql(f"CREATE OR REPLACE TABLE {SCHEMA}.{n}_{TAG} DEEP CLONE {s}")
                print(f"  cloned {s} -> {SCHEMA}.{n}_{TAG}")
    idx = ROOT / "studies_index.csv"
    if not idx.exists() and (ROOT / "studies_index_mv.csv").exists():
        shutil.copy2(ROOT / "studies_index_mv.csv", idx)
    row = pd.DataFrame([dict(tag=TAG, year=int(TAG[:4]), version=TAG[4:], parent=clone_from or START, created=now, status="draft",
                             snapshot_date=prm["snapshot_date"], mapping_source=want.get("mapping_source"), mode=want.get("mode"),
                             path=str(DEST), notes=cfg["notes"])])
    (pd.concat([pd.read_csv(idx, dtype=str), row.astype(str)]) if idx.exists() else row).to_csv(idx, index=False)
    print(f"Created {TAG}: {DEST}\n  {want} | snapshot {prm['snapshot_date']}")

elif not DRY and ACTION == "update":
    prm, want = set_params(DEST)
    mj = DEST / "mv_study.json"
    if mj.exists() and "mode" in want:
        m = json.loads(mj.read_text(encoding="utf-8"))
        m["mode"] = want["mode"]
        mj.write_text(json.dumps(m, indent=2), encoding="utf-8")
    print(f"Updated {TAG}: {want} | snapshot {prm['snapshot_date']} | source files end {prm.get('dp_history_end_date') or 'latest'} | "
          f"source tables {prm.get('source_tables', 'production')}")
else:
    print("Nothing written (dry run or action = run).")

# COMMAND ----------

# DBTITLE 1,Run the study (00_run_study)
# 00a first (it builds the source tables a study with source_tables = study reads), then the study
lin = DEST / "notebooks" / "00a_source_lineage"
if not DRY and RUN_00A:
    lin_path = str(lin).replace("/Workspace", "", 1)
    print(f"Running {lin_path} (build_sources = {RUN_00A}) ...")
    dbutils.notebook.run(lin_path, 0, {"build_sources": RUN_00A})
    print("  report:", sorted(str(p.relative_to(DEST)) for p in (DEST / "outputs" / "reports").glob("source_lineage_*")))

run_it = not DRY and (ACTION == "run" or cfg["run_study"] == "yes")
nb = DEST / "notebooks" / "00_run_study"
nb_path = str(nb).replace("/Workspace", "", 1)
if run_it:
    print(f"Running {nb_path} ...")
    dbutils.notebook.run(nb_path, 0)
    print("Done. Outputs:", sorted(str(p.relative_to(DEST)) for p in (DEST / "outputs").rglob("*") if p.is_file()))
else:
    print("Study not run (run_study = no, or dry run).")
if not DRY:
    links = [("00_run_study", nb), ("00_control_panel", DEST / "notebooks" / "00_control_panel"), ("00a_source_lineage", lin), ("01_account_mapping", next((p for p in (DEST / "notebooks" / "1_accounts_products" / "01_account_mapping", DEST / "notebooks" / "01_account_mapping") if p.exists()), DEST / "notebooks"))]
    displayHTML(f"<h3>{TAG}</h3><p>" + " | ".join(f'<a href="{folders.ws_link(p)}" target="_blank">{n}</a>' for n, p in links) + "</p>")

# COMMAND ----------

# DBTITLE 1,Studies
idx = next((ROOT / f for f in ("studies_index.csv", "studies_index_mv.csv") if (ROOT / f).exists()), None)
display(pd.read_csv(idx, dtype=str)) if idx else print("No studies yet.")