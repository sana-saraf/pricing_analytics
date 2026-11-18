"""Folder scaffolding for Deposit Studies.

Layout (created by setup_study.py)::

    Deposit Studies/
      _shared/deposit_utils/        shared python package (single copy)
      _templates/                   seed content for a brand-new (non-cloned) version
      studies_index.csv             one row per version (parent, created, status)
      2027/
        comparisons/                version-vs-version workbooks
        2027v1/
          study.json                identity + lineage of this version
          requirements.txt  README.md
          notebooks/  inputs/  outputs/  docs/  archive/
        2027v2/                     = a copy of 2027v1 with its own inputs + notebooks

Works on any filesystem path (Databricks /Workspace/... or a local folder).
"""
from __future__ import annotations
import csv
import getpass
import json
import re
import shutil
from datetime import datetime
from pathlib import Path

TAG_RE = re.compile(r"^(20\d{2})v(\d+)$")
OUTPUT_SUBDIRS = ["excel", "html", "flatfiles", "intermediate", "logs"]
COPY_DIRS = ["notebooks", "inputs", "docs", "archive"]
COPY_FILES = ["requirements.txt"]
INDEX_COLS = ["tag", "year", "version", "parent", "created", "created_by", "status", "path", "notes"]
IGNORE = shutil.ignore_patterns("__pycache__", "*.pyc", ".ipynb_checkpoints", ".DS_Store")


def parse_tag(tag: str):
    m = TAG_RE.match(tag.strip())
    if not m:
        raise ValueError(f"'{tag}' is not a valid study tag. Expected like 2027v1.")
    return int(m.group(1)), f"v{int(m.group(2))}"


def tag_path(studies_root: Path, tag: str) -> Path:
    year, _ = parse_tag(tag)
    return Path(studies_root) / str(year) / tag


def list_versions(studies_root, year: int | None = None) -> list[str]:
    root = Path(studies_root)
    out = []
    for ydir in sorted(p for p in root.glob("20??") if p.is_dir()):
        if year is not None and ydir.name != str(year):
            continue
        for v in sorted(ydir.iterdir()):
            if v.is_dir() and TAG_RE.match(v.name) and (v / "study.json").exists():
                out.append(v.name)
    return sorted(out, key=lambda t: (parse_tag(t)[0], int(parse_tag(t)[1][1:])))


def next_version(studies_root, year: int) -> str:
    nums = [int(parse_tag(t)[1][1:]) for t in list_versions(studies_root, year)]
    return f"v{(max(nums) + 1) if nums else 1}"


def _copy_tree(src: Path, dst: Path):
    shutil.copytree(src, dst, ignore=IGNORE, dirs_exist_ok=True)


def create_version(studies_root, year: int, version: str | None = None,
                   clone_from: str | None = None, notes: str = "",
                   log=print, dry_run: bool = False) -> Path:
    """Create Deposit Studies/<year>/<year><version> and return its path.

    clone_from  tag of an existing version (for example '2027v1') to copy notebooks,
                inputs and docs from. Outputs are NOT copied (a new version starts
                with empty outputs so nothing stale is mistaken for new results).
                If None, the version is seeded from _templates/.
    """
    root = Path(studies_root)
    version = version or next_version(root, year)
    if not re.match(r"^v\d+$", version):
        raise ValueError(f"version must look like v1, v2 ... got '{version}'")
    tag = f"{year}{version}"
    dest = root / str(year) / tag
    if dest.exists():
        raise FileExistsError(f"{dest} already exists. Pick another version or delete it first.")

    if clone_from:
        src = tag_path(root, clone_from)
        if not (src / "study.json").exists():
            raise FileNotFoundError(f"clone_from={clone_from}: {src} is not a study version.")
        parent = clone_from
    else:
        src = root / "_templates"
        if not src.exists():
            raise FileNotFoundError(
                f"{src} not found. Upload the _templates folder next to setup_study.py first.")
        parent = None

    log(f"Creating {tag}  <-  {'clone of ' + clone_from if clone_from else 'templates'}")
    if dry_run:
        log("  (dry run: nothing written)")
        return dest

    (root / str(year) / "comparisons").mkdir(parents=True, exist_ok=True)
    dest.mkdir(parents=True)
    for d in COPY_DIRS:
        if (src / d).exists():
            _copy_tree(src / d, dest / d)
        else:
            (dest / d).mkdir(exist_ok=True)
    for f in COPY_FILES:
        if (src / f).exists():
            shutil.copy2(src / f, dest / f)
    for sub in OUTPUT_SUBDIRS:
        (dest / "outputs" / sub).mkdir(parents=True, exist_ok=True)
    (dest / "archive").mkdir(exist_ok=True)

    manifest = {
        "study_name": tag,
        "year": year,
        "version": version,
        "parent": parent,
        "created": datetime.now().isoformat(timespec="seconds"),
        "created_by": _user(),
        "status": "draft",
        "notes": notes,
    }
    (dest / "study.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    if clone_from and (dest / "inputs" / "inputs").exists():
        log("  parent contained a stray inputs/inputs folder, repairing in the new version")
        repair_version(root, tag, apply=True, log=log)

    _append_changelog(dest, tag, parent, notes)
    write_readme(dest, manifest)
    write_start_here(dest, manifest, root)
    _update_index(root)
    log(f"  created {dest}")
    return dest


def _user() -> str:
    try:
        return getpass.getuser()
    except Exception:
        return "unknown"


def _append_changelog(dest: Path, tag: str, parent, notes: str):
    cl = dest / "docs" / "change_log.md"
    cl.parent.mkdir(exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    line = f"- {stamp}  created {tag}" + (f" as a copy of {parent}" if parent else " from templates")
    if notes:
        line += f"  ({notes})"
    existing = cl.read_text(encoding="utf-8") if cl.exists() else "# Change log\n\nRecord every input or method change you make in this version.\n\n"
    cl.write_text(existing.rstrip("\n") + "\n" + line + "\n", encoding="utf-8")



# ----------------------------------------------------------------- repair
def repair_version(studies_root, tag: str, apply: bool = False, log=print) -> list:
    """Fix known folder problems in an existing version. Dry run unless apply=True.

    1. A stray inputs/inputs/ tree (created by an old bug that wrote dim_nmd.csv to
       inputs/inputs/product_maps/) is merged into inputs/ and removed. If a file exists in both
       places, the one in inputs/ wins and the stray copy is kept in archive/stray_inputs_backup/.
    2. Input files that exist in _templates/inputs but are missing from the version are restored.
       (dim_nmd.csv is not in the templates; notebook 01 creates it.)

    Returns the list of actions taken (or that would be taken).
    """
    root = Path(studies_root)
    d = tag_path(root, tag)
    if not (d / "study.json").exists():
        raise FileNotFoundError(f"{d} is not a study version.")
    actions = []
    stray = d / "inputs" / "inputs"
    if stray.exists():
        for f in sorted(p for p in stray.rglob("*") if p.is_file()):
            rel = f.relative_to(stray)
            dest = d / "inputs" / rel
            if not dest.exists():
                actions.append(f"move   inputs/inputs/{rel.as_posix()}  ->  inputs/{rel.as_posix()}")
                if apply:
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(f), str(dest))
            elif dest.read_bytes() == f.read_bytes():
                actions.append(f"delete inputs/inputs/{rel.as_posix()}  (identical copy already in inputs/)")
                if apply:
                    f.unlink()
            else:
                bak = d / "archive" / "stray_inputs_backup" / rel
                actions.append(f"backup inputs/inputs/{rel.as_posix()}  ->  archive/stray_inputs_backup/{rel.as_posix()}  (inputs/ copy kept)")
                if apply:
                    bak.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(f), str(bak))
        if apply:
            shutil.rmtree(stray, ignore_errors=True)
        actions.append("remove inputs/inputs/")

    tpl = root / "_templates" / "inputs"
    if tpl.exists():
        for f in sorted(p for p in tpl.rglob("*") if p.is_file()):
            rel = f.relative_to(tpl)
            dest = d / "inputs" / rel
            if not dest.exists():
                actions.append(f"restore inputs/{rel.as_posix()}  (missing, copied from _templates)")
                if apply:
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(f, dest)
    if apply and actions:
        _append_changelog_line(d, "repair: " + "; ".join(a.split("  ")[0] for a in actions[:6]))
    for a in actions:
        log(("  " if apply else "  [dry run] ") + a)
    if not actions:
        log("  nothing to repair")
    return actions


def _append_changelog_line(dest: Path, text: str):
    cl = dest / "docs" / "change_log.md"
    cl.parent.mkdir(exist_ok=True)
    existing = cl.read_text(encoding="utf-8") if cl.exists() else "# Change log\n\n"
    cl.write_text(existing.rstrip("\n") + f"\n- {datetime.now().strftime('%Y-%m-%d %H:%M')}  {text}\n", encoding="utf-8")

# ----------------------------------------------------------------- index
def _update_index(root: Path):
    rows = []
    for tag in list_versions(root):
        d = tag_path(root, tag)
        m = json.loads((d / "study.json").read_text(encoding="utf-8"))
        rows.append({
            "tag": tag, "year": m["year"], "version": m["version"],
            "parent": m.get("parent") or "", "created": m.get("created", ""),
            "created_by": m.get("created_by", ""), "status": m.get("status", ""),
            "path": str(d), "notes": m.get("notes", ""),
        })
    with open(root / "studies_index.csv", "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=INDEX_COLS)
        w.writeheader()
        w.writerows(rows)


def set_status(studies_root, tag: str, status: str):
    """draft -> review -> final. Updates study.json and the index."""
    d = tag_path(studies_root, tag)
    m = json.loads((d / "study.json").read_text(encoding="utf-8"))
    m["status"] = status
    (d / "study.json").write_text(json.dumps(m, indent=2), encoding="utf-8")
    _update_index(Path(studies_root))


# ----------------------------------------------------------------- links
def ws_link(path: Path | str) -> str:
    """Databricks workspace deep link (the part after #workspace is the path without /Workspace)."""
    p = str(path).replace("\\", "/")
    if p.startswith("/Workspace"):
        p = p[len("/Workspace"):]
    return f"#workspace{p}"


def _links_md(dest: Path, root: Path) -> str:
    def L(label, rel):
        return f"- [{label}]({ws_link(dest / rel)})"

    nb = [
        ("01 Build sandbox tables", "notebooks/01_build_tables.ipynb"),
        ("02 Product mapping", "notebooks/02_product_mapping.ipynb"),
        ("03 Beta models", "notebooks/03_beta_models.ipynb"),
        ("04 Legacy / Empyrean betas", "notebooks/04_legacy_betas.ipynb"),
        ("05 Beta report (Excel)", "notebooks/05_beta_report.ipynb"),
        ("06 Dashboards", "notebooks/06_dashboards.ipynb"),
        ("07 Account to product analysis", "notebooks/07_account_to_product.ipynb"),
        ("08 Version compare", "notebooks/08_version_compare.ipynb"),
        ("09 Promote to pricing_analytics", "notebooks/09_promote_to_pricing_analytics.ipynb"),
    ]
    inputs = [
        ("study_params.json (snapshot date, schema, source tables)", "inputs/config/study_params.json"),
        ("rate_cycles.csv", "inputs/config/rate_cycles.csv"),
        ("product_config.csv (beta method per product)", "inputs/config/product_config.csv"),
        ("promotion.csv (sandbox to pricing_analytics)", "inputs/config/promotion.csv"),
        ("legacy_betas_snv.csv", "inputs/betas/legacy_betas_snv.csv"),
        ("legacy_betas_pnfp.csv", "inputs/betas/legacy_betas_pnfp.csv"),
        ("truncations.csv", "inputs/betas/truncations.csv"),
        ("pnfp_acct_map.csv", "inputs/product_maps/pnfp_acct_map.csv"),
        ("pnfp_products.csv", "inputs/product_maps/pnfp_products.csv"),
        ("snv_acct_map.csv", "inputs/product_maps/snv_acct_map.csv"),
        ("snv_product_map.csv", "inputs/product_maps/snv_product_map.csv"),
        ("future_products_map.csv", "inputs/product_maps/future_products_map.csv"),
        ("dim_nmd.csv (created by notebook 01)", "inputs/product_maps/dim_nmd.csv"),
    ]
    folders = [
        ("This version folder", ""),
        ("notebooks", "notebooks"), ("inputs", "inputs"),
        ("outputs/excel", "outputs/excel"), ("outputs/html", "outputs/html"),
        ("outputs/flatfiles", "outputs/flatfiles"), ("docs", "docs"),
    ]
    parts = [
        f"# {dest.name}  |  START HERE",
        f"Parent: **{json.loads((dest / 'study.json').read_text())['parent'] or 'none (from templates)'}**"
        f"  |  Sandbox tables: `finance.sandbox.*_{dest.name}`",
        "",
        "## Run order",
        "01 build tables, then 02 and 03, then 04, then 05 / 06 / 07. Run 08 to compare with another version. "
        "Run 09 only when the version is final.",
        "",
        "## Notebooks", *[L(a, b) for a, b in nb], "",
        "## Inputs you edit", *[L(a, b) for a, b in inputs], "",
        "## Folders", *[L(a, b) for a, b in folders], "",
        "## Other",
        f"- [studies_index.csv (all versions)]({ws_link(root / 'studies_index.csv')})",
        f"- [Comparisons folder]({ws_link(dest.parent / 'comparisons')})",
        f"- [README]({ws_link(dest / 'README.md')})",
        f"- [Change log]({ws_link(dest / 'docs' / 'change_log.md')})",
        f"- [Optional logic, commented out (Standard Plus, product mapping)]({ws_link(dest / 'docs' / 'optional_logic.md')})",
    ]
    return "\n".join(parts)


def write_start_here(dest: Path, manifest: dict, root: Path):
    md = _links_md(dest, root)
    nb = {
        "cells": [{"cell_type": "markdown", "metadata": {}, "source": md.splitlines(True)}],
        "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
                     "language_info": {"name": "python"}},
        "nbformat": 4, "nbformat_minor": 5,
    }
    (dest / "notebooks" / "00_START_HERE.ipynb").write_text(json.dumps(nb, indent=1), encoding="utf-8")


def write_readme(dest: Path, manifest: dict):
    tpl = dest / "README.md"
    parent = manifest.get("parent") or "none (created from templates)"
    text = f"""# Deposit Study {manifest['study_name']}

| | |
|---|---|
| Year / version | {manifest['year']} / {manifest['version']} |
| Parent | {parent} |
| Created | {manifest['created']} by {manifest['created_by']} |
| Status | {manifest['status']} |
| Sandbox tables | `finance.sandbox.<table>_{manifest['study_name']}` |

Open `notebooks/00_START_HERE` for links to every notebook and input file.

## Rules for this version
1. Edit inputs in `inputs/` (CSV / JSON). Do not hardcode dates, paths or table names in notebooks.
2. Record every change in `docs/change_log.md`.
3. Outputs land in `outputs/` and carry the version tag in the file name.
4. To start the next iteration, create a new version from this one with `setup_study.py` (clone_from = {manifest['study_name']}).
"""
    tpl.write_text(text, encoding="utf-8")


# ------------------------------------------------------- spark-side helpers
def ensure_schema(spark, schema: str, log=print) -> bool:
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        log(f"  schema ready: {schema}")
        return True
    except Exception as e:  # permissions are the usual cause
        log(f"  could not create schema {schema}: {e}")
        log("  ask for CREATE SCHEMA on the finance catalog, or point sandbox_schema in study_params.json at a schema you own.")
        return False


def clone_tables(spark, schema: str, from_tag: str, to_tag: str, names, log=print) -> list:
    """DEEP CLONE <schema>.<name>_<from_tag> to <schema>.<name>_<to_tag>. Skips tables that do not exist."""
    done = []
    for n in names:
        src, dst = f"{schema}.{n}_{from_tag}", f"{schema}.{n}_{to_tag}"
        try:
            if not spark.catalog.tableExists(src):
                log(f"  skip (missing): {src}")
                continue
            spark.sql(f"CREATE OR REPLACE TABLE {dst} DEEP CLONE {src}")
            log(f"  cloned {src} -> {dst}")
            done.append(dst)
        except Exception as e:
            log(f"  clone failed for {src}: {e}")
    return done
