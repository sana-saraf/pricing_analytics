# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 06b | Beta Review Workbook
# MAGIC
# MAGIC Produces `outputs/review/beta_review_<tag>.xlsx` — the reviewer picks the final beta for each product.
# MAGIC
# MAGIC * **Review** (front page): study info and sign-off cells.
# MAGIC * **All products**: one row per product with Legacy, Recommended, Final betas, the change from legacy, and a link to each product tab.
# MAGIC * **Per-product tabs**: pick section (Legacy, Recommended, Current Final, OVERRIDE) plus method detail.
# MAGIC
# MAGIC **Run after:** 06_final_betas and 06a_legacy_assumptions.

# COMMAND ----------

# DBTITLE 1,Install openpyxl
# MAGIC %pip install -q openpyxl

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ../00_setup

# COMMAND ----------

# DBTITLE 1,1. Load betas, product-level detail, and legacy assumptions
import numpy as np

# Study's final betas
betas = sqlq("SELECT * FROM {{mv_final_betas}} ORDER BY snapshot_balance DESC").toPandas()

# Product-level beta detail (LM/POLY per lag)
pbetas = sqlq("SELECT * FROM {{mv_product_betas}}").toPandas()

# Legacy assumptions (from 06a)
LEGACY_CSV = OUTPUTS / "reports" / f"{out_name('legacy_assumptions_by_product')}.csv"
if not LEGACY_CSV.exists():
    LEGACY_CSV = OUTPUTS / "reports" / f"legacy_assumptions_by_product_{S.tag}.csv"
assert Path(LEGACY_CSV).exists(), f"Run 06a_legacy_assumptions first: {LEGACY_CSV}"
legacy = pd.read_csv(LEGACY_CSV)

# Merge
df = betas.merge(legacy[['product', 'legacy_beta', 'legacy_beta_balance']], left_on='mv_product', right_on='product', how='left').drop(columns=['product'])
df = df.merge(pbetas[['mv_product'] + [c for c in pbetas.columns if c.startswith(('lm_', 'poly_'))]], on='mv_product', how='left')
df['change_from_legacy'] = df.apply(lambda r: (r.final_beta / r.legacy_beta - 1) if pd.notna(r.legacy_beta) and r.legacy_beta != 0 else None, axis=1)

# Determine LAGS from columns
LAGS = sorted(set(int(c.split('_')[-1]) for c in pbetas.columns if c.startswith('lm_') and not c.startswith('lm_r2')))
TTC_CYCLE = S.params.get('ttc_cycle', '')

def _round_beta(b):
    """Round to nearest 0.05."""
    if pd.isna(b): return None
    return round(round(b / 0.05) * 0.05, 2)

print(f"Products: {len(df)} | With legacy beta: {df.legacy_beta.notna().sum()} | Lags: {LAGS}")
display(df[['mv_product', 'legacy_beta', 'recommended_beta', 'final_beta', 'final_method', 'change_from_legacy', 'snapshot_balance']].head(10))

# COMMAND ----------

# DBTITLE 1,2. Build outputs/review/beta_review_<tag>.xlsx
REVIEW_DIR = OUTPUTS / "review"
REVIEW_DIR.mkdir(parents=True, exist_ok=True)
BETA_XLSX = REVIEW_DIR / f"beta_review_{S.tag}.xlsx"

PICK_COLS = ["row_id", "cycle", "method", "lag", "beta", "rounded", "r2", "support", "final", "note"]
LEGEND = [("alt", "blue = legacy (compare)"),
          ("edit", "yellow = you fill in"), ("override", "orange = override")]

def pick_rows(r):
    """Build the detailed pick-section DataFrame for one product row r."""
    rows = []
    # LM rows for every lag
    for L in LAGS:
        b = r.get(f"lm_{L}")
        r2 = r.get(f"lm_r2_{L}")
        rows.append(dict(row_id=f"LM_LAG{L}", cycle=TTC_CYCLE, method="LM", lag=L,
                         beta=b, rounded=_round_beta(b), r2=r2, support=""))
    # POLY rows for every lag
    for L in LAGS:
        b = r.get(f"poly_{L}")
        r2 = r.get(f"poly_r2_{L}")
        rows.append(dict(row_id=f"POLY_LAG{L}", cycle=TTC_CYCLE, method="POLY", lag=L,
                         beta=b, rounded=_round_beta(b), r2=r2, support=""))
    # AVG_UP / AVG_DOWN
    if pd.notna(r.avg_up):
        rows.append(dict(row_id="AVG_UP", cycle="UP cycles", method="AVG", lag="",
                         beta=r.avg_up, rounded=_round_beta(r.avg_up), r2=None, support=""))
    if pd.notna(r.avg_down):
        rows.append(dict(row_id="AVG_DOWN", cycle="DOWN cycles", method="AVG", lag="",
                         beta=r.avg_down, rounded=_round_beta(r.avg_down), r2=None, support=""))
    # LEGACY
    rows.append(dict(row_id="LEGACY", cycle="Empyrean", method="Legacy", lag="",
                     beta=r.legacy_beta, rounded=_round_beta(r.legacy_beta), r2=None, support=""))
    # OVERRIDE
    rows.append(dict(row_id="OVERRIDE", cycle="", method="Override", lag="",
                     beta=None, rounded=None, r2=None, support=""))
    pk = pd.DataFrame(rows)
    pk["final"] = ""
    pk["note"] = ""
    return pk[PICK_COLS]

wb = _XWB()
xl_front(wb, "Beta review", "decide",
         [("Study", S.tag), ("Snapshot date", str(S.snapshot_date)),
          ("TTC cycle", TTC_CYCLE), ("Products", len(df)),
          ("Beta method", "TTC through-the-cycle regression (LM + POLY at each lag)"),
          ("Legacy source", "legacy_assumptions_by_account.csv, balance-weighted via FTP to new products")],
         ["'All products' lists every product: Legacy vs Current Final, the change (red > 25%), recommended, and cycle betas.",
          "On each product tab: every LM and POLY lag row is shown. Put 'x' in 'final' for your pick. For OVERRIDE, type the beta and note.",
          "Fill Reviewed by, Review date, put Y in Confirmed, save, upload to inputs/decisions/reviewed/."])

# All products sheet
used = set(["Review", "All products"])
sheet_of = {}
for _, r in df.iterrows():
    sheet_of[r.mv_product] = xl_safe_name(r.mv_product, used)
    used.add(sheet_of[r.mv_product])

allp = df.copy()
allp["tab"] = allp.mv_product.map(sheet_of)
ALL_COLS = ["mv_product", "tab", "snapshot_balance", "legacy_beta", "recommended_beta", "final_beta",
            "change_from_legacy", "final_method", "avg_up", "avg_down", "recommended_r2"]
ws = xl_sheet(wb, "All products", "Beta review: all products",
              [f"One row per product (largest first). Legacy = balance-weighted Empyrean. Red = final moved > 25% from legacy."],
              "assist", [("warn", "red = moved > 25%")])
xl_table(ws, allp[ALL_COLS], fmt={"snapshot_balance": "$", "change_from_legacy": "pct1", "recommended_r2": "pct",
                                  **{c: "beta" for c in ("legacy_beta", "recommended_beta", "final_beta", "avg_up", "avg_down")}},
         widths={"mv_product": 34, "tab": 26, "final_method": 20},
         fills=lambda r: "warn" if pd.notna(r.change_from_legacy) and abs(r.change_from_legacy) > 0.25 else None,
         bars=("snapshot_balance",), links={"tab": lambda r: r.tab})

# Per-product tabs
for _, r in df.iterrows():
    sn = sheet_of[r.mv_product]
    pk = pick_rows(r)
    ws = xl_sheet(wb, sn, f"Beta review: {r.mv_product}",
                  [f"Pick the beta for {r.mv_product}: put x in 'final' for your pick. Balance ${r.snapshot_balance:,.0f}.",
                   f"Current final: {r.final_method} = {r.final_beta:.4f}" + (f" | Override reason: {r.override_reason}" if pd.notna(r.get('override_reason')) and r.override_reason else "")],
                  "decide", LEGEND)
    ws.cell(5, 1, "product").font = _XF(bold=True)
    ws.cell(5, 2, r.mv_product).font = _XF(bold=True)
    last = xl_table(ws, pk, fmt={"beta": "beta", "rounded": "b2", "r2": "r2", "lag": "int"},
                    widths={"row_id": 16, "cycle": 14, "method": 10, "lag": 6, "beta": 12, "rounded": 10, "r2": 8, "support": 14, "final": 8, "note": 40},
                    edit=("final", "note", "support"),
                    fills=lambda row: "override" if row.row_id == "OVERRIDE" else ("alt" if row.row_id == "LEGACY" else None),
                    bold=lambda row, c: c == "beta" and row.row_id == "LEGACY")
    # OVERRIDE beta + rounded cells editable
    ov_row = last  # OVERRIDE is the last row
    ws.cell(ov_row, PICK_COLS.index("beta") + 1).fill = _XP("solid", fgColor=XL["edit"])
    ws.cell(ov_row, PICK_COLS.index("rounded") + 1).fill = _XP("solid", fgColor=XL["edit"])
    dv = _XDV(type="list", formula1='"x"', allow_blank=True)
    ws.add_data_validation(dv)
    dv.add(f"{_xcol(PICK_COLS.index('final') + 1)}{XL_TABLE_ROW + 1}:{_xcol(PICK_COLS.index('final') + 1)}{last}")
    ws.freeze_panes = ws.cell(XL_TABLE_ROW + 1, 2)

wb.save(BETA_XLSX)
print(f"{len(df)} product tabs -> {BETA_XLSX}")
moved = allp[allp.change_from_legacy.abs() > 0.25]
print(f"Products moved > 25% from legacy: {len(moved)}")
if len(moved):
    display(moved[['mv_product', 'legacy_beta', 'final_beta', 'change_from_legacy']])