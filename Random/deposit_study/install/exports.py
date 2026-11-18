# Databricks notebook source
# DBTITLE 1,Export Study Framework
# MAGIC %md
# MAGIC # Export Study Framework
# MAGIC
# MAGIC Packages the entire deposit study framework into a portable ZIP that can be imported into any Databricks workspace.
# MAGIC
# MAGIC | Folder in ZIP | Source | Contents |
# MAGIC |---|---|---|
# MAGIC | `_shared/` | `studies/_shared/` | `deposit_utils/` Python package, `reference/`, `tests/` |
# MAGIC | `_templates/` | `studies/_templates/` | Template notebooks, config, inputs, decisions, reference |
# MAGIC | `install/` | `studies/install/` | `install_study`, `execute_study`, `exports` (this notebook) |
# MAGIC
# MAGIC **Two modes:**
# MAGIC 1. **Export** (cells 1–5): Run here to build the ZIP from this workspace
# MAGIC 2. **Extract** (cell 6): Run in the **target** workspace to import the ZIP into a `studies/` folder
# MAGIC
# MAGIC Notebooks are exported as `.py` source files (Databricks format with `# Databricks notebook source` header). Files (CSV, JSON, XLSX, etc.) are copied as-is.

# COMMAND ----------

# DBTITLE 1,1. Configuration
from pathlib import Path
import requests, base64, zipfile, io, os, json

# ---- workspace paths ----
STUDIES_ROOT = Path("/Workspace/Users/joshuawaller@synovus.com/pricing_analytics/deposits/studies")
WS_PREFIX = "/Users/joshuawaller@synovus.com/pricing_analytics/deposits/studies"

# Folders to export (relative to studies/)
EXPORT_DIRS = ["_shared", "_templates", "install"]

# Output
OUTPUT_DIR = STUDIES_ROOT / "install"
ZIP_NAME = "deposit_study_export.zip"
ZIP_PATH = OUTPUT_DIR / ZIP_NAME

# API setup
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host = spark.conf.get("spark.databricks.workspaceUrl")
headers = {"Authorization": f"Bearer {token}"}
api = f"https://{host}/api/2.0"

print(f"Studies root: {STUDIES_ROOT}")
print(f"Export dirs: {EXPORT_DIRS}")
print(f"Output: {ZIP_PATH}")

# COMMAND ----------

# DBTITLE 1,2. Collect workspace tree
def list_workspace(ws_path):
    """Recursively list all objects under a workspace path via REST API."""
    items = []
    r = requests.get(f"{api}/workspace/list", headers=headers, params={"path": ws_path})
    if r.status_code != 200:
        print(f"  WARN: cannot list {ws_path}: {r.status_code}")
        return items
    for obj in r.json().get("objects", []):
        if obj["object_type"] == "DIRECTORY":
            items += list_workspace(obj["path"])
        else:
            items.append(obj)
    return items

# Collect all objects
all_objects = []
for d in EXPORT_DIRS:
    ws_path = f"{WS_PREFIX}/{d}"
    objs = list_workspace(ws_path)
    all_objects.extend(objs)
    print(f"{d}: {len(objs)} objects")

print(f"\nTotal objects to export: {len(all_objects)}")
for t in ('NOTEBOOK', 'FILE'):
    n = sum(1 for o in all_objects if o['object_type'] == t)
    print(f"  {t}: {n}")

# COMMAND ----------

# DBTITLE 1,3. Export to ZIP
LANG_EXT = {"PYTHON": ".py", "SQL": ".sql", "R": ".r", "SCALA": ".scala"}

def rel_path(ws_path):
    """Convert workspace path to relative path inside the ZIP."""
    return ws_path.replace(WS_PREFIX + "/", "")

def export_notebook(ws_path):
    """Export a notebook as SOURCE format, return (content_bytes, extension)."""
    r = requests.get(f"{api}/workspace/export", headers=headers,
                     params={"path": ws_path, "format": "SOURCE"})
    if r.status_code != 200:
        return None, None
    data = r.json()
    content = base64.b64decode(data["content"])
    ext = LANG_EXT.get(data.get("language", "PYTHON"), ".py")
    return content, ext

def export_file(ws_path):
    """Export a workspace file as raw bytes."""
    # Try direct filesystem read first (faster for files in /Workspace)
    local = Path("/Workspace" + ws_path)
    if local.exists():
        return local.read_bytes()
    # Fallback to API
    r = requests.get(f"{api}/workspace/export", headers=headers,
                     params={"path": ws_path, "format": "AUTO"})
    if r.status_code != 200:
        return None
    return base64.b64decode(r.json()["content"])

# Build ZIP
buf = io.BytesIO()
stats = {"notebooks": 0, "files": 0, "errors": 0}

with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
    for obj in all_objects:
        ws_path = obj["path"]
        rp = rel_path(ws_path)

        if obj["object_type"] == "NOTEBOOK":
            content, ext = export_notebook(ws_path)
            if content:
                zf.writestr(rp + ext, content)
                stats["notebooks"] += 1
            else:
                print(f"  ERROR exporting notebook: {rp}")
                stats["errors"] += 1

        elif obj["object_type"] == "FILE":
            content = export_file(ws_path)
            if content:
                zf.writestr(rp, content)
                stats["files"] += 1
            else:
                print(f"  ERROR exporting file: {rp}")
                stats["errors"] += 1

# Write ZIP
ZIP_PATH.parent.mkdir(parents=True, exist_ok=True)
ZIP_PATH.write_bytes(buf.getvalue())
size_mb = len(buf.getvalue()) / 1024 / 1024

print(f"\n✓ {ZIP_PATH}")
print(f"  Size: {size_mb:.1f} MB")
print(f"  Notebooks: {stats['notebooks']} | Files: {stats['files']} | Errors: {stats['errors']}")

# COMMAND ----------

# DBTITLE 1,4. Verify ZIP contents
with zipfile.ZipFile(ZIP_PATH, 'r') as zf:
    names = sorted(zf.namelist())
    # Group by top-level folder
    from collections import Counter
    top = Counter(n.split('/')[0] for n in names)
    print(f"ZIP: {len(names)} entries in {len(top)} top-level folders\n")
    for folder, count in sorted(top.items()):
        print(f"  {folder}/  ({count} files)")
        folder_items = [n for n in names if n.startswith(folder + '/')]
        for n in folder_items[:8]:
            info = zf.getinfo(n)
            print(f"    {n}  ({info.file_size:,} bytes)")
        if len(folder_items) > 8:
            print(f"    ... and {len(folder_items) - 8} more")
    print(f"\nDownload: {ZIP_PATH}")

# COMMAND ----------

# DBTITLE 1,5. Copy to UC Volume for download
import shutil

VOLUME_PATH = Path("/Volumes/finance/pricing_analytics/flat_files/deposit_study_exports")
VOLUME_PATH.mkdir(parents=True, exist_ok=True)

volume_zip = VOLUME_PATH / ZIP_NAME
shutil.copy2(ZIP_PATH, volume_zip)

print(f"\u2713 Copied to volume: {volume_zip}")
print(f"  Size: {volume_zip.stat().st_size / 1024 / 1024:.1f} MB")
print(f"\nVolume path: /Volumes/finance/pricing_analytics/flat_files/deposit_study_exports/{ZIP_NAME}")
print("\nDownload from the Volumes UI or use:")
print(f"  databricks fs cp 'dbfs:{volume_zip}' ./deposit_study_export.zip")

# Clickable link in notebook
displayHTML(f'''
<div style="margin:10px 0; padding:12px; background:#e8f4fd; border-radius:6px; border-left:4px solid #1F4E79;">
  <b>Download ZIP:</b><br>
  <a href="/explore/data/volumes/finance/pricing_analytics/flat_files" target="_blank">
    📦 Open Volume in Catalog Explorer → deposit_study_exports/{ZIP_NAME}
  </a>
</div>
''')

# COMMAND ----------

# DBTITLE 1,5. Import instructions
# MAGIC %md
# MAGIC ## Importing to another workspace
# MAGIC
# MAGIC ### Quick method (use the Extract cell below)
# MAGIC 1. In the **target** workspace, create a folder for your studies (e.g. `studies/`)
# MAGIC 2. Upload just **two files** into `studies/install/`:
# MAGIC    * `deposit_study_export.zip`
# MAGIC    * `exports.py` (this notebook)
# MAGIC 3. Open `exports` in the target workspace
# MAGIC 4. Set widget **target_root** to the `studies/` path, **extract_mode** = `extract`
# MAGIC 5. **Run cell 6** — it imports all notebooks (with proper cell boundaries), files (CSV, JSON, XLSX), and folder structure
# MAGIC 6. Then run `install/999_install_study` to create your first study
# MAGIC
# MAGIC ### What gets extracted
# MAGIC ```
# MAGIC studies/
# MAGIC   _shared/deposit_utils/    ← Python package (betas, config, study, etc.)
# MAGIC   _shared/reference/        ← shared reference data
# MAGIC   _shared/tests/            ← unit tests
# MAGIC   _templates/notebooks/     ← all template notebooks
# MAGIC   _templates/config/        ← study_config.xlsx, study_params.json, rate_cycles.csv
# MAGIC   _templates/inputs/        ← products, decisions, reference, vintages
# MAGIC   install/                  ← 999_install_study, execute_study, exports
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,6. Extract ZIP into target workspace
# ============================================================
# EXTRACT MODE — run this cell in the TARGET workspace.
# Place the ZIP file somewhere accessible (workspace or volume),
# set TARGET_ROOT to where you want studies/ to live,
# and set ZIP_SOURCE to the ZIP location.
# ============================================================
import subprocess, sys
subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "openpyxl"])

import os, base64, zipfile, requests
from pathlib import Path
from collections import Counter

# ---- CONFIGURE THESE ----
# Where should the study framework live in this workspace?
# e.g. "/Users/you@company.com/pricing_analytics/deposits/studies"
try:
    _nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    _here = Path(_nb if _nb.startswith("/Workspace") else "/Workspace" + _nb).parent
except Exception:
    _here = Path(os.getcwd())
DEFAULT_TARGET = str(_here.parent if _here.name == "install" else _here)

W = dbutils.widgets
W.text("target_root", DEFAULT_TARGET, "Target studies root (workspace path)")
W.dropdown("extract_mode", "dry_run", ["dry_run", "extract"], "Mode")

TARGET_ROOT = W.get("target_root").strip()
DRY = W.get("extract_mode") == "dry_run"

# Find the ZIP: check workspace install/ folder, then volume
ZIP_CANDIDATES = [
    Path(f"/Workspace{TARGET_ROOT}/install/deposit_study_export.zip"),
    Path("/Volumes/finance/pricing_analytics/flat_files/deposit_study_exports/deposit_study_export.zip"),
]
ZIP_FILE = next((p for p in ZIP_CANDIDATES if p.exists()), None)
if ZIP_FILE is None:
    print("No ZIP found at:")
    for p in ZIP_CANDIDATES:
        print(f"  {p}")
    print("\nUpload deposit_study_export.zip to one of these locations first.")
    dbutils.notebook.exit("ZIP not found")

print(f"ZIP source: {ZIP_FILE}")
print(f"Target root: {TARGET_ROOT}")
print(f"Mode: {'DRY RUN (preview only)' if DRY else 'EXTRACT (will write files)'}")

# ---- API setup ----
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host = spark.conf.get("spark.databricks.workspaceUrl")
headers = {"Authorization": f"Bearer {token}"}
api_url = f"https://{host}/api/2.0"

EXT_TO_LANG = {".py": "PYTHON", ".sql": "SQL", ".r": "R", ".scala": "SCALA"}
NB_HEADER = "# Databricks notebook source"

stats = {"notebooks": 0, "files": 0, "dirs_created": 0, "skipped": 0, "errors": 0}

def ensure_dir(ws_path):
    """Create workspace directory if it doesn't exist."""
    r = requests.post(f"{api_url}/workspace/mkdirs", headers=headers, json={"path": ws_path})
    if r.status_code == 200:
        stats["dirs_created"] += 1
    return r.status_code == 200

def import_notebook(ws_path, content_bytes, lang):
    """Import a notebook from source bytes."""
    r = requests.post(f"{api_url}/workspace/import", headers=headers, json={
        "path": ws_path, "format": "SOURCE", "language": lang,
        "content": base64.b64encode(content_bytes).decode(), "overwrite": True
    })
    return r.status_code == 200

def import_file(ws_path, content_bytes):
    """Import a regular file."""
    r = requests.post(f"{api_url}/workspace/import", headers=headers, json={
        "path": ws_path, "format": "AUTO",
        "content": base64.b64encode(content_bytes).decode(), "overwrite": True
    })
    return r.status_code == 200

# ---- Extract ----
with zipfile.ZipFile(ZIP_FILE, 'r') as zf:
    names = sorted(zf.namelist())
    top = Counter(n.split('/')[0] for n in names)
    print(f"\nZIP: {len(names)} entries in {len(top)} folders: {', '.join(sorted(top.keys()))}\n")

    for name in names:
        content = zf.read(name)
        _, ext = os.path.splitext(name)
        ws_dest = f"{TARGET_ROOT}/{name}"

        # Detect if it's a notebook (has Databricks header and a recognized extension)
        is_notebook = False
        if ext.lower() in EXT_TO_LANG:
            try:
                text = content.decode('utf-8', errors='ignore')
                if text.strip().startswith(NB_HEADER):
                    is_notebook = True
            except Exception:
                pass

        if is_notebook:
            lang = EXT_TO_LANG[ext.lower()]
            nb_path = ws_dest[:-len(ext)]  # strip the .py/.sql extension for notebook path
            if DRY:
                print(f"  [notebook] {name} -> {nb_path}")
            else:
                parent = '/'.join(nb_path.split('/')[:-1])
                ensure_dir(parent)
                if import_notebook(nb_path, content, lang):
                    stats["notebooks"] += 1
                    print(f"  \u2713 notebook: {name} -> {nb_path}")
                else:
                    stats["errors"] += 1
                    print(f"  \u2717 FAILED notebook: {name}")
        else:
            if DRY:
                print(f"  [file]     {name} -> {ws_dest}")
            else:
                parent = '/'.join(ws_dest.split('/')[:-1])
                ensure_dir(parent)
                if import_file(ws_dest, content):
                    stats["files"] += 1
                    print(f"  \u2713 file: {name} -> {ws_dest}")
                else:
                    stats["errors"] += 1
                    print(f"  \u2717 FAILED file: {name}")

print(f"\n{'DRY RUN complete' if DRY else 'EXTRACT complete'}")
if not DRY:
    print(f"  Notebooks: {stats['notebooks']} | Files: {stats['files']} | Dirs: {stats['dirs_created']} | Errors: {stats['errors']}")
    print(f"\nNext: open {TARGET_ROOT}/install/999_install_study and run it.")
else:
    print(f"  Would import {len(names)} items. Set extract_mode = 'extract' to run for real.")