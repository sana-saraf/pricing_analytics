"""PRODUCT_CONFIG: which beta method / lag / cycle is used per product.

The configuration now lives in ``inputs/config/product_config.csv`` (editable in
Excel, diff-able between versions). ``describe`` and ``resolve`` keep the exact
behaviour of the original ``describe_config`` / ``resolve_config`` cell.

CSV columns:  product, typ, m1, m2, lag, cyc, beta, note
"""
from __future__ import annotations
import pandas as pd

METHOD_LOOKUP = {
    "EP":   {"label": "Endpoint",          "beta": "Endpoint_Beta",  "r2": None},
    "LR":   {"label": "Linear Regression", "beta": "Lm_Coef",        "r2": "R_Square"},
    "POLY": {"label": "Polynomial",        "beta": "Poly_Beta",      "r2": "Poly_R_Square"},
    "PFFE": {"label": "Poly FFE",          "beta": "Poly_Beta_FFE",  "r2": "Poly_R_Square_FFE"},
}

CALC_TYPE_LOOKUP = {
    "AVG_M": "Averaged between Methods",     # rec = calc = avg(m1, m2)
    "AVG_E": "Averaged with Empyrean",       # calc = method, rec = avg(calc, empyrean)
    "PURE":  "Pure from Calc",               # rec = calc = method result
    "EMP":   "Empyrean Override",            # rec = empyrean
    "MAN":   "Manual Override",              # rec = specified value
    "AUTO":  "Auto-select best R\u00b2",     # rec = calc = best R2 method
}

# Which fields each calc type needs
_REQUIRED = {
    "AVG_M": ("m1", "m2", "lag", "cyc"),
    "AVG_E": ("m1", "lag", "cyc"),
    "PURE":  ("m1", "lag", "cyc"),
    "EMP":   (),
    "MAN":   ("beta",),
    "AUTO":  (),
}


def _blank(v) -> bool:
    return v is None or (isinstance(v, float) and pd.isna(v)) or str(v).strip() == ""


def load_product_config_df(df: pd.DataFrame) -> dict:
    """product_config.csv rows -> PRODUCT_CONFIG dict (same shape as the old notebook dict)."""
    cfg = {}
    for _, r in df.iterrows():
        prod = str(r["product"]).strip()
        if not prod:
            continue
        d = {"typ": (str(r.get("typ", "")).strip().upper() or "AUTO")}
        for k in ("m1", "m2", "cyc"):
            if not _blank(r.get(k)):
                d[k] = str(r[k]).strip()
        if not _blank(r.get("lag")):
            d["lag"] = int(float(r["lag"]))
        if not _blank(r.get("beta")):
            d["beta"] = float(r["beta"])
        if not _blank(r.get("note")):
            d["note"] = str(r["note"]).strip()
        cfg[prod] = d
    return cfg


def validate(cfg: dict, cycle_lookup: dict, known_products=None) -> list:
    """Return a list of human-readable problems (empty list = clean)."""
    errs = []
    for prod, c in cfg.items():
        typ = c.get("typ", "AUTO")
        if typ not in CALC_TYPE_LOOKUP:
            errs.append(f"{prod}: unknown typ '{typ}' (use {', '.join(CALC_TYPE_LOOKUP)})")
            continue
        for f in _REQUIRED[typ]:
            if f not in c:
                errs.append(f"{prod}: typ {typ} requires '{f}'")
        for m in ("m1", "m2"):
            if m in c and c[m] not in METHOD_LOOKUP:
                errs.append(f"{prod}: {m}='{c[m]}' not in {list(METHOD_LOOKUP)}")
        if "cyc" in c and c["cyc"] not in cycle_lookup:
            errs.append(f"{prod}: cyc='{c['cyc']}' not in {list(cycle_lookup)}")
        if "lag" in c and not 0 <= c["lag"] <= 3:
            errs.append(f"{prod}: lag={c['lag']} outside 0-3")
    if known_products is not None:
        kp = set(known_products)
        for prod in cfg:
            if prod not in kp:
                errs.append(f"{prod}: not found in nmds_agg products (renamed or removed?)")
        for prod in sorted(kp - set(cfg)):
            errs.append(f"{prod}: product has no row in product_config.csv (will default to AUTO)")
    return errs


class ProductConfig:
    """Holds PRODUCT_CONFIG and provides describe() / resolve() like the old cell."""

    def __init__(self, cfg: dict, cycle_lookup: dict):
        self.cfg = cfg
        self.cycle_lookup = cycle_lookup
        self.notes = {p: self.describe(p) for p in cfg}

    @classmethod
    def from_study(cls, study) -> "ProductConfig":
        df = study.read_input("config/product_config.csv")
        return cls(load_product_config_df(df), study.cycle_lookup)

    def describe(self, prod: str) -> str:
        cfg = self.cfg.get(prod)
        if not cfg:
            return ""
        typ = cfg.get("typ", "AUTO")
        m1, m2 = cfg.get("m1"), cfg.get("m2")
        lag, note = cfg.get("lag"), cfg.get("note")
        m1_lbl = METHOD_LOOKUP[m1]["label"] if m1 else None
        m2_lbl = METHOD_LOOKUP[m2]["label"] if m2 else None
        lag_s = f"at {lag}M lag" if lag is not None else ""
        if typ == "AVG_M":
            desc = f"Average of {m1_lbl} & {m2_lbl} Method {lag_s}"
        elif typ == "AVG_E":
            desc = f"Average of Empyrean Model Beta & {m1_lbl} Method {lag_s}"
        elif typ == "PURE":
            desc = f"{m1_lbl} Method {lag_s}"
        elif typ == "EMP":
            desc = "Empyrean Model Beta Override"
        elif typ == "MAN":
            desc = f"Manual Override \u2014 {cfg.get('beta', '')}"
        else:
            desc = "Auto-select best R\u00b2"
        if note:
            desc = f"{desc} \u2014 {note}" if typ != "AUTO" else note
        return desc.strip()

    def resolve(self, prod: str):
        """Resolve config to the dict the report cells expect, or None for AUTO."""
        cfg = self.cfg.get(prod)
        if not cfg:
            return None
        typ = cfg.get("typ", "AUTO")
        if typ == "AUTO":
            return None
        cyc_acr = cfg.get("cyc", "TTC")
        cycle_int = self.cycle_lookup.get(cyc_acr, cyc_acr)
        cycle_dsp = cyc_acr
        if typ == "EMP":
            return dict(mode="empyrean", method="Empyrean Override", cycle="", cycle_disp="")
        if typ == "MAN":
            return dict(mode="manual", value=cfg["beta"], method="Manual Override",
                        cycle="", cycle_disp="")
        lag = cfg.get("lag", 0)
        m1i = METHOD_LOOKUP[cfg["m1"]]
        if typ == "AVG_M":
            m2i = METHOD_LOOKUP[cfg["m2"]]
            return dict(
                mode="blend_poly_lm", lag=lag,
                poly_col=f"{m1i['beta']} Lag {lag} {cycle_int}",
                lm_col=f"{m2i['beta']} Lag {lag} {cycle_int}",
                r2_poly=f"{m1i['r2']} Lag {lag} {cycle_int}" if m1i["r2"] else None,
                r2_lm=f"{m2i['r2']} Lag {lag} {cycle_int}" if m2i["r2"] else None,
                method=f"Blend {cfg['m1']}/{cfg['m2']} Lag {lag}",
                cycle=cycle_int, cycle_disp=cycle_dsp,
            )
        if typ in ("AVG_E", "PURE"):
            return dict(
                mode="standard" if typ == "AVG_E" else "pure",
                beta_col=f"{m1i['beta']} Lag {lag} {cycle_int}",
                r2_col=f"{m1i['r2']} Lag {lag} {cycle_int}" if m1i["r2"] else None,
                method=f"{m1i['label']} Lag {lag}",
                cycle=cycle_int, cycle_disp=cycle_dsp,
            )
        return None

    def summary_table(self) -> pd.DataFrame:
        rows = []
        for prod, c in self.cfg.items():
            rows.append(dict(product=prod, typ=c.get("typ", "AUTO"), m1=c.get("m1", ""),
                             m2=c.get("m2", ""), lag=c.get("lag", ""), cyc=c.get("cyc", ""),
                             beta=c.get("beta", ""), description=self.notes.get(prod, "")))
        return pd.DataFrame(rows)
