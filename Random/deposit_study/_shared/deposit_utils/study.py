"""Study context for one version folder (for example Deposit Studies/2027/2027v2).

A notebook never hardcodes a path, a date, or a table name. It calls
``Study.load(spark, dbutils)`` which walks up from the notebook's own location to
the folder containing ``study.json`` and reads everything else from there. That is
what makes a cloned version work without editing any code.
"""
from __future__ import annotations
import json
import os
from pathlib import Path

import pandas as pd

from . import sqlrun

MANIFEST = "study.json"
PARAMS_REL = "inputs/config/study_params.json"
CYCLES_REL = "inputs/config/rate_cycles.csv"
PRODUCT_CONFIG_REL = "inputs/config/product_config.csv"
PROMOTION_REL = "inputs/config/promotion.csv"
DIM_NMD_REL = "inputs/product_maps/dim_nmd.csv"

# Sandbox tables created per version: <schema>.<name>_<tag>
SANDBOX_TABLES = [
    "dim_nmd",
    "lpnfp_nmds",
    "lpnfp_nmds_agg",
    "lsnv_nmds_agg",
    "nmds_agg",
    "ccalc_beta_results",
    "legacy_betas",
    "legacy_consolid_betas",
    "beta_summary",
]

# Model validation tables written by studies/mv_report: <schema>.<name>_<tag>.
# Kept out of SANDBOX_TABLES on purpose so setup_study does not deep-clone a parent's
# MV results into a new version (outputs are not copied on clone for the same reason).
# Stages: accounts -> account metrics -> account (subproduct) betas -> product selection
#         -> product map -> product betas -> beta review -> final betas
MV_TABLES = [
    "mv_accounts",
    "mv_account_metrics",
    "mv_account_war",
    "mv_account_betas",
    "mv_product_selection",
    "mv_product_map",
    "mv_product_war",
    "mv_product_betas",
    "mv_beta_review",
    "mv_final_betas",
]

DEFAULT_PARAMS = {
    "snapshot_date": "2026-06-30",
    "ftp_results_date": None,                      # defaults to snapshot_date
    "chart_start_date": "2015-01-01",              # first date shown on the fed funds chart (06_dashboards)
    "sandbox_schema": "finance.sandbox",
    "production_schema": "finance.pricing_analytics",
    "sources": {
        "src_pnfp_deposits": "finance.ftp_validation.pnfp_deposits",
        "src_lsnv_nmds": "finance.pricing_analytics.lsnv_nmds",
        "src_emp_ftp_results": "finance.ftp_validation.emp_ftp_results",
        "src_fed_funds": "finance.operational_deposits.fed_funds_matrix",
        "src_dim_nmd": "finance.reference.dim_nmd",
    },
}


def find_study_root(start=None, dbutils=None) -> Path:
    """Walk up from the notebook location (or ``start``) to the folder with study.json."""
    cands = []
    if start:
        cands.append(str(start))
    if dbutils is not None:
        try:
            nb = (dbutils.notebook.entry_point.getDbutils().notebook()
                  .getContext().notebookPath().get())
            cands.append(nb if nb.startswith("/Workspace") else "/Workspace" + nb)
        except Exception:
            pass
    cands.append(os.getcwd())
    for c in cands:
        p = Path(c)
        for q in [p, *p.parents]:
            if (q / MANIFEST).exists():
                return q
    raise FileNotFoundError(
        f"Could not find {MANIFEST} above any of: {cands}. "
        "Open the notebook from inside a study version folder, or pass root=..."
    )


def read_csv(path, **kw) -> pd.DataFrame:
    """CSV reader used for every input file: all strings, blanks stay blank."""
    kw.setdefault("dtype", str)
    kw.setdefault("keep_default_na", False)
    kw.setdefault("encoding", "utf-8-sig")
    return pd.read_csv(path, **kw)


class Study:
    def __init__(self, root, spark=None, dbutils=None):
        self.root = Path(root)
        self.spark = spark
        self.dbutils = dbutils
        self.manifest = json.loads((self.root / MANIFEST).read_text(encoding="utf-8"))
        self.tag = self.manifest["study_name"]          # e.g. 2027v1
        self.year = int(self.manifest["year"])
        self.version = self.manifest["version"]         # e.g. v1
        self.parent = self.manifest.get("parent")
        self.params = self._load_params()
        self.sandbox_schema = self.params["sandbox_schema"]

    # ------------------------------------------------------------------ load
    @classmethod
    def load(cls, spark=None, dbutils=None, root=None) -> "Study":
        r = Path(root) if root else find_study_root(dbutils=dbutils)
        return cls(r, spark=spark, dbutils=dbutils)

    def _load_params(self) -> dict:
        p = json.loads(json.dumps(DEFAULT_PARAMS))
        f = self.root / PARAMS_REL
        if f.exists():
            user = json.loads(f.read_text(encoding="utf-8"))
            src = {**p["sources"], **user.pop("sources", {})}
            p.update({k: v for k, v in user.items() if not k.startswith("_")})
            p["sources"] = src
        if not p.get("ftp_results_date"):
            p["ftp_results_date"] = p["snapshot_date"]
        return p

    # ----------------------------------------------------------------- paths
    @property
    def inputs(self) -> Path: return self.root / "inputs"
    @property
    def outputs(self) -> Path: return self.root / "outputs"
    @property
    def docs(self) -> Path: return self.root / "docs"
    @property
    def notebooks(self) -> Path: return self.root / "notebooks"
    @property
    def year_dir(self) -> Path: return self.root.parent
    @property
    def comparisons(self) -> Path: return self.year_dir / "comparisons"
    @property
    def studies_root(self) -> Path: return self.year_dir.parent

    def input(self, rel: str) -> Path:
        return self.inputs / rel

    def output(self, kind: str, stem: str, ext: str) -> Path:
        """outputs/<kind>/<stem>_<tag>.<ext>, folder created if missing."""
        d = self.outputs / kind
        d.mkdir(parents=True, exist_ok=True)
        return d / f"{stem}_{self.tag}.{ext.lstrip('.')}"

    def read_input(self, rel: str, **kw) -> pd.DataFrame:
        return read_csv(self.input(rel), **kw)

    # ------------------------------------------------------------ rate cycles
    @property
    def cycles(self) -> pd.DataFrame:
        """rate_cycles.csv as a DataFrame (name, short_code, config_acronym, start, end)."""
        return self.read_input("config/rate_cycles.csv")

    @property
    def cycle_lookup(self) -> dict:
        c = self.cycles
        return dict(zip(c["config_acronym"], c["name"]))

    # ----------------------------------------------------------------- tables
    def table(self, name: str) -> str:
        """Fully qualified versioned sandbox table, e.g. finance.sandbox.nmds_agg_2027v1."""
        return f"{self.sandbox_schema}.{name}_{self.tag}"

    def tokens(self) -> dict:
        t = dict(self.params["sources"])
        t.update({n: self.table(n) for n in SANDBOX_TABLES + MV_TABLES})
        t["snapshot_date"] = self.params["snapshot_date"]
        t["snapshot_month"] = self.params["snapshot_date"][:7]
        t["ftp_results_date"] = self.params["ftp_results_date"]
        t["tag"] = self.tag
        t["sandbox_schema"] = self.sandbox_schema
        t["production_schema"] = self.params["production_schema"]
        return t

    @property
    def snapshot_date(self) -> str: return self.params["snapshot_date"]

    # -------------------------------------------------------------------- sql
    def sql(self, text: str, show: bool = False, quiet: bool = False):
        """Run templated SQL (multiple statements ok). Mirrors a %sql cell when show=True."""
        if self.spark is None:
            raise RuntimeError("Study was loaded without spark=")
        disp = None
        if show:
            try:
                import builtins
                disp = getattr(builtins, "display", None)
            except Exception:
                disp = None
        return sqlrun.run(self.spark, text, self.tokens(), show=show, display=disp, quiet=quiet)

    def render(self, text: str) -> str:
        return sqlrun.render(text, self.tokens())

    def table_exists(self, name: str) -> bool:
        return bool(self.spark.catalog.tableExists(self.table(name)))

    # --------------------------------------------------------------- banners
    def banner(self) -> str:
        return (f"Study {self.tag} | parent: {self.parent or 'none'} | "
                f"snapshot {self.snapshot_date} | tables {self.sandbox_schema}.*_{self.tag}")
