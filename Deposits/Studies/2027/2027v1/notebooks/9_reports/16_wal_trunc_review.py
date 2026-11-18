# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 16 | WAL & Truncation review workbook (InputsToDecide)
# MAGIC
# MAGIC Builds `outputs/review/wal_trunc_review_<tag>.xlsx` in the study workbook style (00_setup):
# MAGIC * **Review** (front page): study, snapshot, sign-off cells.
# MAGIC * **All products**: one row per product with the pre-selected WAL and truncation, linked to each product tab.
# MAGIC * **One tab per product** with two pick sections:
# MAGIC   * **Pick the WAL**: CURRENT, CALCULATED, each loaded month, OVERRIDE. Put `x` in `final` (exactly one); for OVERRIDE type the WAL and the reason.
# MAGIC   * **Pick the truncation**: one row per level (mv_product, product_group, legacy, …). Put `x` in `final`; for OVERRIDE type months in `override_months`.
# MAGIC   * Below: account survival curve (% of baseline by month).
# MAGIC
# MAGIC **Colours:** green = pre-marked final; blue = Current (compare); yellow = you fill in; orange = OVERRIDE.
# MAGIC
# MAGIC **After the review.** Upload the filled-in workbook to `inputs/decisions/reviewed/`; in `00_read_and_recommend` set `action = final review WAL+Trunc`, Run all, check, then `confirm = yes, I checked`.
# MAGIC
# MAGIC **Run after:** 11_truncation_points and 15_wal_ftp.

# COMMAND ----------

# DBTITLE 1,Install openpyxl
# MAGIC %pip install -q openpyxl

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ../00_setup

# COMMAND ----------

# DBTITLE 1,1. Load WAL + truncation data and build candidate rows
import shutil, numpy as np

# ---- WAL ----
WAL_SUM_PATH = Path(out("excel", "wal_summary", "csv"))
assert WAL_SUM_PATH.exists(), f"{WAL_SUM_PATH.name} missing: run 15_wal_ftp first"
wsum = pd.read_csv(WAL_SUM_PATH).rename(columns={"avg_balance_calc_month": "avg_balance"}).set_index("mv_product")
WCFG = S.params.get("wal", {}) or {}
_ls = lambda s: [x.strip() for x in str(s or "").split(",") if x.strip()]
CUR_M, CALC_M = _ls(WCFG.get("current_months")), str(WCFG.get("calculated_month") or "").strip()
WAL_M = sorted(c for c in wsum.columns if c.startswith("wal_2"))  # wal_2026-06-30 etc.
WALA_CSV = REFERENCE / "wal_assumptions.csv"
wala = pd.read_csv(WALA_CSV, dtype=str, keep_default_na=False).set_index("mv_product") if WALA_CSV.exists() else pd.DataFrame()

# ---- Truncation ----
TCFG = S.params.get("truncation", {}) or {}
TRUNC_XLSX = OUTPUTS / "excel" / f"{out_name('truncation')}.xlsx"
assert TRUNC_XLSX.exists(), f"{TRUNC_XLSX.name} missing: run 11_truncation_points first"
THR = [int(x) for x in str(TCFG.get("thresholds", "30,25,20")).split(",") if x.strip()]
FINAL_SRC = TCFG.get("final_source", "legacy")
LEVELS = [l.strip() for l in str(TCFG.get("levels", "mv_product,product_group")).split(",") if l.strip() and l.strip() != "legacy_bank"]

tdata = {}
for lv in LEVELS:
    try:
        df = pd.read_excel(TRUNC_XLSX, sheet_name=f"By {lv}")
        for c in df.columns:
            if c != lv: df[c] = pd.to_numeric(df[c], errors="coerce")
        tdata[lv] = df.set_index(lv)
    except Exception: pass

pdata = {}
try:
    df = pd.read_excel(TRUNC_XLSX, sheet_name="Pct mv_product")
    pdata["mv_product"] = df.set_index("mv_product").apply(pd.to_numeric, errors="coerce")
except Exception: pass

# Product -> group mapping from cohort
COHORT_PATH = INPUTS / "vintages" / f"truncation_cohort_{S.tag}.csv"
ccols = pd.read_csv(COHORT_PATH, nrows=0).columns.tolist()
mcols = ["mv_product"] + [c for c in ["product_group", "product_group_pf", "mv_product_pf"] if c in ccols]
PMAP = pd.read_csv(COHORT_PATH, dtype=str, usecols=mcols).drop_duplicates("mv_product").set_index("mv_product")

# Legacy / old truncations
LEG_CSV = REFERENCE / "legacy_truncations.csv"
leg_t = pd.read_csv(LEG_CSV, dtype=str, keep_default_na=False).set_index("product_group") if LEG_CSV.exists() else pd.DataFrame()
for c in leg_t.columns: leg_t[c] = pd.to_numeric(leg_t[c], errors="coerce")
OLD_CSV = REFERENCE / "old_truncations.csv"
old_t = pd.read_csv(OLD_CSV, dtype=str, keep_default_na=False).set_index("product_name") if OLD_CSV.exists() else pd.DataFrame()
for c in old_t.columns: old_t[c] = pd.to_numeric(old_t[c], errors="coerce")

# New crosswalk legacy truncation (per-product, balance-weighted from 06a)
XWALK_CSV = OUTPUTS / "reports" / f"legacy_assumptions_by_product_{S.tag}.csv"
xwalk_t = pd.read_csv(XWALK_CSV).set_index("product") if XWALK_CSV.exists() else pd.DataFrame()
print(f"Crosswalk legacy truncation: {XWALK_CSV.name} {'found' if XWALK_CSV.exists() else 'NOT found (run 06a)'}")

# Products sorted by balance
prods = wsum.sort_values("avg_balance", ascending=False)

# ---- Candidate row builders ----
WAL_COLS = ["row_id", "basis", "wal", "final", "note"]
TRUNC_COLS = ["row_id", "level", "level_key", "accts", "R2"] + [f"m{t}" for t in THR] + ["extrap", "final", "override_months", "note"]

def wal_picks(p):
    r = wsum.loc[p] if p in wsum.index else pd.Series(dtype=float)
    rows = [dict(row_id="CURRENT", basis=f"Current (avg {', '.join(CUR_M)})", wal=r.get("wal_current")),
            dict(row_id="CALCULATED", basis=f"Calculated ({CALC_M})", wal=r.get("wal_calculated"))]
    for mc in WAL_M:
        rows.append(dict(row_id=f"MONTH_{mc[4:]}", basis=f"Month {mc[4:]}", wal=r.get(mc)))
    ov = pd.to_numeric(wala.get("management_wal", pd.Series(dtype=str)).get(p, ""), errors="coerce") if len(wala) else np.nan
    rows.append(dict(row_id="OVERRIDE", basis="Management override", wal=ov,
                     note=(wala.get("notes", pd.Series(dtype=str)).get(p, "") if len(wala) else "")))
    df = pd.DataFrame(rows).reindex(columns=WAL_COLS)
    df["final"] = ""
    df["note"] = df.note.fillna("")
    return df

def trunc_picks(p):
    pm = PMAP.loc[p] if p in PMAP.index else pd.Series(dtype=str)
    pg = pm.get("product_group", "") if isinstance(pm, pd.Series) else ""
    rows = []
    for lv in tdata:
        key = p if lv == "mv_product" else pm.get(lv, "") if isinstance(pm, pd.Series) else ""
        if not key or key not in tdata[lv].index: continue
        tr = tdata[lv].loc[key]
        rows.append(dict(row_id=lv.upper(), level=lv, level_key=key,
                         accts=tr.get("Baseline_Acct_Cnt"), R2=tr.get("R_squared"),
                         **{f"m{t}": tr.get(f"Month y={t}%") for t in THR},
                         extrap="Y" if any(tr.get(f"Extrap y={t}%", 0) == 1 for t in THR) else ""))
    # Crosswalk legacy truncation (per-product, balance-weighted from legacy_assumptions_by_account.csv)
    if len(xwalk_t) and p in xwalk_t.index and pd.notna(xwalk_t.loc[p].get("legacy_trunc")):
        lt = xwalk_t.loc[p].legacy_trunc
        rows.append(dict(row_id="LEGACY", level="legacy (crosswalk)", level_key=p, accts=np.nan, R2=np.nan,
                         **{f"m{t}": lt for t in THR}, extrap=""))
    # Fallback: legacy truncation by product_group (old file)
    elif pg and pg in leg_t.index:
        lr = leg_t.loc[pg]
        rows.append(dict(row_id="LEGACY", level="legacy (by group)", level_key=pg, accts=np.nan, R2=np.nan,
                         **{f"m{t}": lr.get(f"predicted_trunc_{t}_pct", lr.get("truncation_point")) for t in THR}, extrap=""))
    rows.append(dict(row_id="OVERRIDE", level="override", level_key="", accts=np.nan, R2=np.nan,
                     **{f"m{t}": np.nan for t in THR}, extrap=""))
    df = pd.DataFrame(rows).reindex(columns=TRUNC_COLS)
    df["final"] = ""
    df["override_months"] = ""
    df["note"] = ""
    return df

print(f"Products: {len(prods)} | WAL: Current {CUR_M}, Calc {CALC_M}, months {[c[4:] for c in WAL_M]}")
print(f"Truncation levels: {list(tdata.keys())} | Thresholds: {THR} | final_source: {FINAL_SRC}")
print(f"Legacy truncations: {len(leg_t)} groups | Old truncations: {len(old_t)} products")
display(wal_picks(prods.index[0]))
display(trunc_picks(prods.index[0]))

# COMMAND ----------

# DBTITLE 1,2. Write outputs/review/wal_trunc_review_<tag>.xlsx
REVIEW_DIR = OUTPUTS / "review"
REVIEW_DIR.mkdir(parents=True, exist_ok=True)
XLSX = REVIEW_DIR / f"{out_name('wal_trunc_review')}.xlsx"
LEGEND = [("used", "green = pre-marked final"), ("alt", "blue = current (compare)"), ("edit", "yellow = you fill in"), ("override", "orange = override")]

wb = _XWB()
xl_front(wb, "WAL & Truncation review", "decide",
         [("Study", S.tag), ("Snapshot date", str(S.snapshot_date)),
          ("WAL current months", ", ".join(CUR_M)), ("WAL calculated month", CALC_M),
          ("Truncation thresholds", ", ".join(map(str, THR))),
          ("Truncation final_source", FINAL_SRC), ("Products", len(prods))],
         ["Each product has its own tab with two pick sections: WAL (top) and Truncation (below).",
          "WAL: put x in 'final' on the row you want. For OVERRIDE: type the WAL (months) and the reason in 'note'.",
          "Truncation: pick the level whose months to use (mv_product, product_group, legacy, etc.). For OVERRIDE: type months in 'override_months'.",
          "Fill Reviewed by / Review date / Reviewed and Confirmed (Y), save, upload to inputs/decisions/reviewed/."])

# ---- All Products summary ----
used_names = {"Review", "All products"}
sheet_of = {}
allp = []
for p in prods.index:
    sn = xl_safe_name(p, used_names); used_names.add(sn); sheet_of[p] = sn
    w = wal_picks(p).set_index("row_id")
    t = trunc_picks(p).set_index("row_id")
    wc, wk = w.wal.get("CURRENT"), w.wal.get("CALCULATED")
    td = t[t.final == "x"]
    tm = td[f"m{THR[0]}"].iloc[0] if len(td) else np.nan
    allp.append(dict(product=p, tab=sn, avg_balance=prods.avg_balance.get(p),
                     wal_current=wc, wal_calculated=wk,
                     wal_change=((wk / wc - 1) if pd.notna(wc) and pd.notna(wk) and wc else np.nan),
                     trunc_level=(td.level.iloc[0] if len(td) else ""),
                     trunc_m=tm, trunc_R2=(td.R2.iloc[0] if len(td) else np.nan)))
ALLP = pd.DataFrame(allp)
ws = xl_sheet(wb, "All products", "WAL & Truncation: all products",
              [f"WAL: Current = avg of {', '.join(CUR_M)}; Calculated = {CALC_M}. Trunc: pre-selected level + months to {THR[0]}%.",
               "Red = WAL moved > 25%. Click tab to jump."], "assist", [("warn", "red = moved > 25%")])
xl_table(ws, ALLP, fmt={"avg_balance": "$", "wal_current": "m", "wal_calculated": "m", "wal_change": "pct1",
                         "trunc_m": "int", "trunc_R2": "r2"},
         widths={"product": 34, "tab": 26, "trunc_level": 18},
         fills=lambda r: "warn" if pd.notna(r.wal_change) and abs(r.wal_change) > 0.25 else None,
         bars=("avg_balance",), links={"tab": lambda r: r.tab})

# ---- Per-product tabs ----
for p in prods.index:
    sn = sheet_of[p]
    wpk, tpk = wal_picks(p), trunc_picks(p)
    ws = xl_sheet(wb, sn, f"WAL & Truncation: {p}",
                  [f"Pick the WAL and truncation for {p}. One x in 'final' per section.",
                   f"WAL in months. Truncation = months until {'/'.join(str(t)+'%' for t in THR)} of baseline accounts remain."],
                  "decide", LEGEND)
    ws.cell(5, 1, "product").font = _XF(bold=True)
    ws.cell(5, 2, p).font = _XF(bold=True)
    ws.cell(5, 4, f"avg_balance ${prods.avg_balance.get(p, 0):,.0f}").font = _XF(italic=True, color=XL["text"])

    # ---- WAL pick table ----
    ws.cell(7, 1, "Pick the WAL").font = _XF(bold=True, color=XL["navy"])
    wr = xl_table(ws, wpk, row=8, fmt={"wal": "m"}, widths={"row_id": 22, "basis": 36, "wal": 12, "note": 50},
                  edit=("final", "note"),
                  fills=lambda r: "override" if r.row_id == "OVERRIDE" else ("used" if r.final == "x" else ("alt" if r.row_id == "CURRENT" else None)),
                  bold=lambda r, c: c == "wal" and (r.final == "x" or r.row_id == "CURRENT"))
    ws.cell(wr, WAL_COLS.index("wal") + 1).fill = _XP("solid", fgColor=XL["edit"])  # OVERRIDE wal editable
    dv = _XDV(type="list", formula1='"x"', allow_blank=True)
    ws.add_data_validation(dv)
    fc = _xcol(WAL_COLS.index("final") + 1)
    dv.add(f"{fc}9:{fc}{wr}")

    # ---- Truncation pick table ----
    tr0 = wr + 3
    ws.cell(tr0 - 1, 1, "Pick the truncation").font = _XF(bold=True, color=XL["navy"])
    tr_last = xl_table(ws, tpk, row=tr0,
                       fmt={"accts": "int", "R2": "r2", "override_months": "int", **{f"m{t}": "int" for t in THR}},
                       widths={"row_id": 22, "level": 18, "level_key": 34, "note": 50},
                       edit=("final", "override_months", "note"),
                       fills=lambda r: "override" if r.row_id == "OVERRIDE" else ("used" if r.final == "x" else None),
                       bold=lambda r, c: r.final == "x" and c.startswith("m"))
    for t in THR:  # OVERRIDE months cells editable
        ws.cell(tr_last, TRUNC_COLS.index(f"m{t}") + 1).fill = _XP("solid", fgColor=XL["edit"])
    dv2 = _XDV(type="list", formula1='"x"', allow_blank=True)
    ws.add_data_validation(dv2)
    fc2 = _xcol(TRUNC_COLS.index("final") + 1)
    dv2.add(f"{fc2}{tr0 + 1}:{fc2}{tr_last}")

    # ---- Survival curve (if available) ----
    if "mv_product" in pdata and p in pdata["mv_product"].index:
        cr0 = tr_last + 3
        ws.cell(cr0 - 1, 1, "Account survival (% of baseline by month)").font = _XF(bold=True, color=XL["navy"])
        curve = pdata["mv_product"].loc[[p]].T.reset_index()
        curve.columns = ["month", "pct"]
        curve["month"] = pd.to_numeric(curve["month"], errors="coerce")
        curve["pct"] = pd.to_numeric(curve["pct"], errors="coerce")
        xl_table(ws, curve, row=cr0, fmt={"month": "int", "pct": "pct"}, freeze=False, autofilter=False)

    ws.freeze_panes = ws.cell(9, 2)

wb.save(XLSX)
DETAIL = OUTPUTS / "Deposit Study Artifacts" / "Detailed Reports"
DETAIL.mkdir(parents=True, exist_ok=True)
shutil.copy2(XLSX, DETAIL / XLSX.name)
print(f"{len(prods)} product tabs -> {XLSX}")
print(f"Sheets: {', '.join(wb.sheetnames[:4])}, ...")
display(ALLP)