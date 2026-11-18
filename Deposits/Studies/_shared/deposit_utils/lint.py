"""Hardcoding checker for study notebooks.

Finds the things that break versioning: a literal study tag, a literal table name,
a literal date, a literal workspace path. Run it after ANY edit (yours, Genie Code's, mine)::

    from deposit_utils import lint
    lint.scan_version(S.root)            # prints findings, returns the list

Only code cells are checked. Comment-only lines are skipped.
"""
from __future__ import annotations
import json
import re
from pathlib import Path

RULES = [
    ("literal study tag",
     r"(?<![\w{<])20\d\dv\d+(?![\w}>])",
     "Use S.tag, S.table('<name>') or the {{token}} instead of typing the version."),
    ("literal table name",
     r"\bfinance\.(?:sandbox|pricing_analytics|reference|ftp_validation|operational_deposits)\.\w+",
     "Use S.table('<name>'), a {{token}} (see tokens list in the skill) or S.params['sources']."),
    ("literal date",
     r"['\"]20\d\d-\d\d(?:-\d\d)?['\"]",
     "Use S.snapshot_date / {{snapshot_date}} / {{snapshot_month}} / {{ftp_results_date}}."),
    ("literal workspace path",
     r"/Workspace/|/Users/|/dbfs/|S:\\\\",
     "Use S.root, S.input(...) or S.output(...)."),
    ("literal user or domain",
     r"@synovus\.com|@pinnacle",
     "Remove it. Nothing should depend on one person's account."),
]
_COMPILED = [(n, re.compile(p, re.I), fix) for n, p, fix in RULES]


def _code_lines_ipynb(path: Path):
    nb = json.loads(path.read_text(encoding="utf-8"))
    for ci, c in enumerate(nb.get("cells", [])):
        if c.get("cell_type") != "code":
            continue
        for li, line in enumerate("".join(c["source"]).split("\n"), 1):
            yield f"cell {ci} line {li}", line


def _code_lines_py(path: Path):
    in_md = False
    cell = 0
    for li, raw in enumerate(path.read_text(encoding="utf-8").split("\n"), 1):
        if raw.startswith("# COMMAND ----------"):
            cell += 1
            in_md = False
            continue
        line = raw
        if raw.startswith("# MAGIC"):
            line = raw[len("# MAGIC"):].lstrip(" ")
            if line.startswith("%md"):
                in_md = True
            if in_md:
                continue
        yield f"cell {cell} line {li}", line


def scan_file(path) -> list:
    p = Path(path)
    lines = _code_lines_ipynb(p) if p.suffix == ".ipynb" else _code_lines_py(p)
    out = []
    for where, line in lines:
        s = line.strip()
        if not s or s.startswith(("#", "--", "//")):
            continue
        for name, rx, fix in _COMPILED:
            m = rx.search(line)
            if m:
                out.append(dict(file=p.name, where=where, rule=name, found=m.group(0), line=s[:110], fix=fix))
    return out


def scan_version(root, log=print) -> list:
    """Scan every notebook (.ipynb or .py) in <root>/notebooks. Returns the findings."""
    nb_dir = Path(root) / "notebooks"
    files = sorted(list(nb_dir.glob("*.ipynb")) + list(nb_dir.glob("*.py")))
    findings = []
    for f in files:
        if f.name.startswith("00_START_HERE"):
            continue
        findings += scan_file(f)
    if not findings:
        log(f"OK: no hardcoded tags, tables, dates or paths in {len(files)} notebook file(s).")
    else:
        log(f"{len(findings)} hardcoded value(s) found:")
        for x in findings:
            log(f"  {x['file']}  {x['where']}  [{x['rule']}]  {x['found']}")
            log(f"      {x['line']}")
            log(f"      fix: {x['fix']}")
    return findings
