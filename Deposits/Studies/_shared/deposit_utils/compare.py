"""Version-to-version comparison.

Everything here is plain pandas on files, so it also runs without Spark.

What is compared
----------------
1. Inputs      every CSV / JSON in inputs/ (product maps, dim_nmd, legacy betas,
               truncations, product_config, rate_cycles, study_params)
2. Mapping     product / product-group / account-type classification changes
3. Methods     product_config changes plus the method / lag / cycle each product ended up using
4. Results     recommended, calculated and Empyrean betas, balances, WAR, by product and by group
5. Method grid every beta column in final_results (all methods x lags x cycles)
"""
from __future__ import annotations
import json
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

from .study import read_csv

# file (relative to inputs/) -> key columns used to match rows between versions
INPUT_KEYS = {
    "product_maps/dim_nmd.csv": ["legacy_bank", "subproductid", "pf_flag", "bdsp_flag"],
    "product_maps/pnfp_acct_map.csv": ["src_ac_tp_code"],
    "product_maps/snv_acct_map.csv": ["src_ac_tp_code"],
    "product_maps/snv_product_map.csv": ["subproductid", "acct_typ_code"],
    "product_maps/pnfp_products.csv": ["subproductid"],
    "product_maps/future_products_map.csv": ["legacy_bank", "acct_typ_code"],
    "betas/truncations.csv": ["product_group"],
    "betas/legacy_betas_snv.csv": ["Product", "AccountNum"],
    "betas/legacy_betas_pnfp.csv": ["Product", "AccountNum"],
    "config/product_config.csv": ["product"],
    "config/rate_cycles.csv": ["name"],
}
MAPPING_FILES = [k for k in INPUT_KEYS if k.startswith("product_maps/")]
METHOD_FILES = ["config/product_config.csv", "config/rate_cycles.csv"]
BETA_COLS = ["rec_beta", "calc_beta", "emp_beta"]


# ------------------------------------------------------------------ generic diff
def _norm(df: pd.DataFrame) -> pd.DataFrame:
    return df.astype(str).apply(lambda s: s.str.strip())


def diff_frames(a: pd.DataFrame, b: pd.DataFrame, key: list[str]) -> dict:
    """Row-level diff. Returns dict(added, removed, changed, mode).

    If the key is not unique in either frame the diff compares whole rows as a multiset.
    Leftover rows that share a key are then paired into "changed" records; anything that
    cannot be paired is reported as added or removed.
    """
    a, b = _norm(a), _norm(b)
    cols = [c for c in a.columns if c in b.columns]
    key = [k for k in key if k in cols]
    empty = pd.DataFrame(columns=cols)
    if not key or a.duplicated(key).any() or b.duplicated(key).any():
        # Duplicate keys: compare whole rows as a multiset, then pair leftovers that share a key
        ca = Counter(map(tuple, a[cols].values.tolist()))
        cb = Counter(map(tuple, b[cols].values.tolist()))
        rem = list((ca - cb).elements())
        add = list((cb - ca).elements())
        changed_rows = []
        if key:
            kpos = [cols.index(k) for k in key]
            by_key_rem, by_key_add = {}, {}
            for r in rem:
                by_key_rem.setdefault(tuple(r[i] for i in kpos), []).append(r)
            for r in add:
                by_key_add.setdefault(tuple(r[i] for i in kpos), []).append(r)
            rem, add = [], []
            for k in set(by_key_rem) | set(by_key_add):
                rr, aa = by_key_rem.get(k, []), by_key_add.get(k, [])
                n = min(len(rr), len(aa))
                for old_r, new_r in zip(rr[:n], aa[:n]):
                    for ci, c in enumerate(cols):
                        if c not in key and old_r[ci] != new_r[ci]:
                            changed_rows.append({**dict(zip(key, k)), "column": c, "old": old_r[ci], "new": new_r[ci]})
                rem.extend(rr[n:])
                add.extend(aa[n:])
        return dict(mode="multiset",
                    added=pd.DataFrame(add, columns=cols) if add else empty,
                    removed=pd.DataFrame(rem, columns=cols) if rem else empty,
                    changed=pd.DataFrame(changed_rows, columns=key + ["column", "old", "new"]))
    m = a.merge(b, on=key, how="outer", suffixes=("_a", "_b"), indicator=True)
    added = m[m["_merge"] == "right_only"][key + [c + "_b" for c in cols if c not in key]]
    added.columns = key + [c for c in cols if c not in key]
    removed = m[m["_merge"] == "left_only"][key + [c + "_a" for c in cols if c not in key]]
    removed.columns = key + [c for c in cols if c not in key]
    both = m[m["_merge"] == "both"]
    rows = []
    for c in cols:
        if c in key:
            continue
        diff = both[both[c + "_a"] != both[c + "_b"]]
        for _, r in diff.iterrows():
            rows.append({**{k: r[k] for k in key}, "column": c, "old": r[c + "_a"], "new": r[c + "_b"]})
    return dict(mode="keyed", added=added.reset_index(drop=True),
                removed=removed.reset_index(drop=True),
                changed=pd.DataFrame(rows, columns=key + ["column", "old", "new"]))


# ------------------------------------------------------------------ inputs
def compare_inputs(root_a, root_b, files=None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returns (summary, detail). detail has one row per changed/added/removed record."""
    files = files or list(INPUT_KEYS)
    summ, det = [], []
    for rel in files:
        pa, pb = Path(root_a) / "inputs" / rel, Path(root_b) / "inputs" / rel
        if not pa.exists() and not pb.exists():
            continue
        if not pa.exists() or not pb.exists():
            summ.append(dict(file=rel, status="only in " + ("B" if pb.exists() else "A"),
                             added=0, removed=0, changed=0))
            continue
        d = diff_frames(read_csv(pa), read_csv(pb), INPUT_KEYS[rel])
        summ.append(dict(file=rel, status="same" if (d["added"].empty and d["removed"].empty and d["changed"].empty) else "different",
                         added=len(d["added"]), removed=len(d["removed"]),
                         changed=d["changed"][d["changed"].columns[:-3]].drop_duplicates().shape[0] if len(d["changed"]) else 0))
        for kind in ("added", "removed"):
            for _, r in d[kind].iterrows():
                det.append(dict(file=rel, change=kind, record=" | ".join(r.astype(str).tolist()),
                                column="", old="", new=""))
        for _, r in d["changed"].iterrows():
            det.append(dict(file=rel, change="changed",
                            record=" | ".join(str(r[k]) for k in INPUT_KEYS[rel] if k in r.index),
                            column=r["column"], old=r["old"], new=r["new"]))
    # study params (json)
    pa, pb = Path(root_a) / "inputs/config/study_params.json", Path(root_b) / "inputs/config/study_params.json"
    if pa.exists() and pb.exists():
        ja, jb = json.loads(pa.read_text("utf-8")), json.loads(pb.read_text("utf-8"))
        flat = lambda d, p="": {f"{p}{k}": v for k, v in d.items() if not k.startswith("_") and not isinstance(v, dict)} | \
            {kk: vv for k, v in d.items() if isinstance(v, dict) and not k.startswith("_") for kk, vv in flat(v, f"{k}.").items()}
        fa, fb = flat(ja), flat(jb)
        n = 0
        for k in sorted(set(fa) | set(fb)):
            if fa.get(k) != fb.get(k):
                n += 1
                det.append(dict(file="config/study_params.json", change="changed", record=k,
                                column="value", old=fa.get(k, ""), new=fb.get(k, "")))
        summ.append(dict(file="config/study_params.json", status="same" if n == 0 else "different",
                         added=0, removed=0, changed=n))
    return pd.DataFrame(summ), pd.DataFrame(det)


# ------------------------------------------------------------------ results
def load_beta_summary(study_root, tag=None) -> pd.DataFrame:
    root = Path(study_root)
    tag = tag or json.loads((root / "study.json").read_text("utf-8"))["study_name"]
    p = root / "outputs" / "flatfiles" / f"beta_summary_{tag}.csv"
    if not p.exists():
        raise FileNotFoundError(f"{p} not found. Run 05_beta_report in {tag} first.")
    df = pd.read_csv(p)
    for c in BETA_COLS + ["r2", "war", "curr_bal", "avg_bal"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def compare_beta_summaries(a: pd.DataFrame, b: pd.DataFrame, tol: float = 1e-9) -> dict:
    """Product-level and group-level comparison of two beta_summary frames."""
    m = a.merge(b, on="product", how="outer", suffixes=("_a", "_b"), indicator=True)
    out = pd.DataFrame({"product": m["product"]})
    out["group_a"], out["group_b"] = m.get("pg_a"), m.get("pg_b")
    out["status"] = m["_merge"].map({"left_only": "removed in B", "right_only": "added in B", "both": "in both"})
    out["reclassified"] = (m["_merge"] == "both") & (m["pg_a"] != m["pg_b"])
    for c in BETA_COLS:
        out[c + "_a"], out[c + "_b"] = m[c + "_a"], m[c + "_b"]
        out[c + "_delta"] = m[c + "_b"] - m[c + "_a"]
    for c in ("method", "cycle"):
        out[c + "_a"], out[c + "_b"] = m[c + "_a"], m[c + "_b"]
    out["method_changed"] = (m["_merge"] == "both") & ((m["method_a"] != m["method_b"]) | (m["cycle_a"] != m["cycle_b"]))
    out["bal_a"], out["bal_b"] = m["curr_bal_a"], m["curr_bal_b"]
    out["bal_delta"] = out["bal_b"] - out["bal_a"]
    out["war_a"], out["war_b"] = m["war_a"], m["war_b"]
    out["war_delta"] = out["war_b"] - out["war_a"]
    out["beta_changed"] = out["rec_beta_delta"].abs() > tol
    out = out.sort_values(["group_b", "group_a", "product"], na_position="last").reset_index(drop=True)

    def wtd(df, col, bal, pgcol):
        d = df.dropna(subset=[col])
        d = d[d[bal] > 0]
        num = (d[col] * d[bal]).groupby(d[pgcol]).sum()
        den = d[bal].groupby(d[pgcol]).sum()
        return num / den

    def side(df):
        g = pd.DataFrame({"products": df.groupby("pg")["product"].nunique(),
                          "balance": df.groupby("pg")["curr_bal"].sum(),
                          "rec_beta": wtd(df, "rec_beta", "curr_bal", "pg"),
                          "calc_beta": wtd(df, "calc_beta", "curr_bal", "pg")})
        return g

    ga, gb = side(a), side(b)
    grp = ga.join(gb, how="outer", lsuffix="_a", rsuffix="_b")
    for c in ("products", "balance", "rec_beta", "calc_beta"):
        grp[c + "_delta"] = grp[c + "_b"] - grp[c + "_a"]
    grp = grp.reset_index().rename(columns={"index": "group", "pg": "group"})
    return dict(products=out, groups=grp)


def compare_final_results(fa: pd.DataFrame, fb: pd.DataFrame, tol: float = 0.005) -> pd.DataFrame:
    """Long list of (product, column, a, b, delta) for every beta column that moved."""
    cols = [c for c in fa.columns if c in fb.columns and c != "EMP_NAME"]
    m = fa.set_index("EMP_NAME")[cols].join(fb.set_index("EMP_NAME")[cols], how="inner", lsuffix="_a", rsuffix="_b")
    rows = []
    for c in cols:
        d = m[c + "_b"] - m[c + "_a"]
        hit = d.abs() > tol
        for prod in m.index[hit.fillna(False)]:
            rows.append(dict(product=prod, column=c, a=m.at[prod, c + "_a"], b=m.at[prod, c + "_b"], delta=d[prod]))
    only_a = sorted(set(fa.columns) - set(fb.columns))
    only_b = sorted(set(fb.columns) - set(fa.columns))
    res = pd.DataFrame(rows, columns=["product", "column", "a", "b", "delta"])
    res.attrs["columns_only_in_a"], res.attrs["columns_only_in_b"] = only_a, only_b
    return res.sort_values("delta", key=lambda s: s.abs(), ascending=False).reset_index(drop=True)


# ------------------------------------------------------------------ orchestration
def compare_versions(root_a, root_b, include_results: bool = True, log=print) -> dict:
    """Run every comparison. root_a is the baseline ('A'), root_b the new one ('B')."""
    ra, rb = Path(root_a), Path(root_b)
    ta = json.loads((ra / "study.json").read_text("utf-8"))["study_name"]
    tb = json.loads((rb / "study.json").read_text("utf-8"))["study_name"]
    log(f"Comparing {ta} (A) -> {tb} (B)")
    isum, idet = compare_inputs(ra, rb)
    res = dict(tag_a=ta, tag_b=tb, input_summary=isum, input_detail=idet)
    res["mapping_detail"] = idet[idet["file"].isin(MAPPING_FILES)] if len(idet) else idet
    res["method_detail"] = idet[idet["file"].isin(METHOD_FILES)] if len(idet) else idet
    if include_results:
        try:
            sa, sb = load_beta_summary(ra, ta), load_beta_summary(rb, tb)
            r = compare_beta_summaries(sa, sb)
            res.update(products=r["products"], groups=r["groups"])
        except FileNotFoundError as e:
            log(f"  results skipped: {e}")
        try:
            fa = pd.read_csv(ra / "outputs" / "intermediate" / f"final_results_{ta}.csv")
            fb = pd.read_csv(rb / "outputs" / "intermediate" / f"final_results_{tb}.csv")
            res["method_grid"] = compare_final_results(fa, fb)
        except FileNotFoundError as e:
            log(f"  method grid skipped: {e}")
    return res


def write_comparison_workbook(res: dict, path) -> str:
    """Style-consistent Excel workbook (dark blue headers, same as the beta report)."""
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
    from openpyxl.utils import get_column_letter

    hdr_fill = PatternFill("solid", fgColor="1F4E79")
    hdr_font = Font(name="Arial", bold=True, color="FFFFFF", size=9)
    body = Font(name="Arial", size=9)
    pos, neg = PatternFill("solid", fgColor="E2F0D9"), PatternFill("solid", fgColor="FCE4D6")
    side = Side(style="thin", color="BFBFBF")
    border = Border(left=side, right=side, top=side, bottom=side)

    sheets = {}
    ta, tb = res["tag_a"], res["tag_b"]
    ov = [("A (baseline)", ta), ("B (new)", tb)]
    if "products" in res:
        p = res["products"]
        ov += [("Products in both", int((p["status"] == "in both").sum())),
               ("Products added in B", int((p["status"] == "added in B").sum())),
               ("Products removed in B", int((p["status"] == "removed in B").sum())),
               ("Products reclassified", int(p["reclassified"].sum())),
               ("Products with method / cycle change", int(p["method_changed"].sum())),
               ("Products with recommended beta change", int(p["beta_changed"].sum()))]
    sheets["Overview"] = pd.DataFrame(ov, columns=["Item", "Value"])
    sheets["Input Files"] = res["input_summary"]
    if len(res["input_detail"]):
        sheets["Input Changes"] = res["input_detail"]
    if "products" in res:
        sheets["Beta by Product"] = res["products"]
        sheets["Beta by Group"] = res["groups"]
    if "method_grid" in res and len(res["method_grid"]):
        sheets["Method Grid"] = res["method_grid"].head(5000)

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        for name, df in sheets.items():
            df.to_excel(xw, sheet_name=name[:31], index=False)
            ws = xw.sheets[name[:31]]
            for ci, col in enumerate(df.columns, 1):
                c = ws.cell(row=1, column=ci)
                c.font, c.fill, c.border = hdr_font, hdr_fill, border
                c.alignment = Alignment(horizontal="center", wrap_text=True)
                width = min(max(len(str(col)), *(len(str(v)) for v in df[col].head(200))) + 3, 48) if len(df) else len(str(col)) + 3
                ws.column_dimensions[get_column_letter(ci)].width = width
                is_delta = str(col).endswith("_delta") or col == "delta"
                is_beta = str(col).startswith(("rec_beta", "calc_beta", "emp_beta")) or col in ("a", "b", "rec_beta", "calc_beta")
                for ri in range(2, len(df) + 2):
                    cell = ws.cell(row=ri, column=ci)
                    cell.font, cell.border = body, border
                    v = cell.value
                    if isinstance(v, (int, float)) and not isinstance(v, bool):
                        if "bal" in str(col) or col == "balance":
                            cell.number_format = "#,##0"
                        elif "war" in str(col):
                            cell.number_format = "0.0000%"
                        elif is_beta or is_delta:
                            cell.number_format = "0.00"
                        if is_delta and abs(v) > 1e-9 and "bal" not in str(col):
                            cell.fill = pos if v > 0 else neg
            ws.freeze_panes = "A2"
    return str(path)
