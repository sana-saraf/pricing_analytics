# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 12b | WARF (Core Ratio) Review Workbook
# MAGIC
# MAGIC Produces `outputs/review/warf_review_<tag>.xlsx` — the reviewer picks the final core ratio for each product.
# MAGIC
# MAGIC Core ratio = 1 − runoff share = the fraction of the balance that is "core" (stable). Same concept as beta but for balance stability, not rate sensitivity.
# MAGIC
# MAGIC * **Review** (front page): study info and sign-off cells.
# MAGIC * **All products**: Legacy core ratio vs Calculated (latest month, all counterparties), the change, FSE share, and per-product tab links.
# MAGIC * **Per-product tabs**: pick section (Legacy, Calculated All, Calculated ex-FSE, Calculated FSE-only, OVERRIDE).
# MAGIC
# MAGIC **Run after:** 12_warf_prep / 13_warf_results and 06a_legacy_assumptions.

# COMMAND ----------

# DBTITLE 1,Install openpyxl
# MAGIC %pip install -q openpyxl

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ../00_setup

# COMMAND ----------

# DBTITLE 1,1. Load WARF summary and legacy core ratios
# WARF summary (from 12_warf_prep / 13_warf_results)
WSUM_CSV = OUTPUTS / "excel" / f"warf_summary_{S.tag}.csv"
assert WSUM_CSV.exists(), f"Run 12_warf_prep first: {WSUM_CSV}"
wsum_raw = pd.read_csv(WSUM_CSV)

# Latest month, product-level rows
latest = wsum_raw[wsum_raw.basis == 'latest'].copy()
latest = latest[latest.mv_product != '(not in study)'].copy()

# Legacy assumptions
LEGACY_CSV = OUTPUTS / "reports" / f"legacy_assumptions_by_product_{S.tag}.csv"
assert LEGACY_CSV.exists(), f"Run 06a_legacy_assumptions first: {LEGACY_CSV}"
legacy = pd.read_csv(LEGACY_CSV)

# Merge
df = latest.merge(legacy[['product', 'legacy_core_ratio', 'legacy_core_ratio_balance']],
                  left_on='mv_product', right_on='product', how='left').drop(columns=['product'])

# Core ratio = 1 - runoff, so warf_all is runoff share; core = 1 - warf_all
df['calculated_core_ratio'] = 1 - df['warf_all']
df['calculated_core_ex_fse'] = 1 - df['warf_ex_fse']
df['calculated_core_fse'] = 1 - df['warf_fse']
df['change_from_legacy'] = df.apply(
    lambda r: (r.calculated_core_ratio / r.legacy_core_ratio - 1)
    if pd.notna(r.legacy_core_ratio) and r.legacy_core_ratio != 0 else None, axis=1)
df = df.sort_values('balance', ascending=False).reset_index(drop=True)
print(f"Products: {len(df)} | With legacy core ratio: {df.legacy_core_ratio.notna().sum()}")
display(df[['mv_product', 'legacy_core_ratio', 'calculated_core_ratio', 'calculated_core_ex_fse',
            'fse_share', 'change_from_legacy', 'balance']].head(10))

# COMMAND ----------

# DBTITLE 1,2. Build outputs/review/warf_review_<tag>.xlsx
REVIEW_DIR = OUTPUTS / "review"
REVIEW_DIR.mkdir(parents=True, exist_ok=True)
WARF_XLSX = REVIEW_DIR / f"warf_review_{S.tag}.xlsx"

PICK_COLS = ["row_id", "basis", "core_ratio", "runoff_share", "fse_share", "final", "note"]
LEGEND = [("used", "green = pre-marked final"), ("alt", "blue = legacy (compare)"),
          ("edit", "yellow = you fill in"), ("override", "orange = override")]

def pick_rows_warf(r):
    rows = [
        dict(row_id="LEGACY", basis="Legacy (balance-weighted Empyrean)",
             core_ratio=r.legacy_core_ratio, runoff_share=1 - r.legacy_core_ratio if pd.notna(r.legacy_core_ratio) else None, fse_share=None),
        dict(row_id="CALCULATED_ALL", basis="Calculated (all counterparties)",
             core_ratio=r.calculated_core_ratio, runoff_share=r.warf_all, fse_share=r.fse_share),
        dict(row_id="CALCULATED_EX_FSE", basis="Calculated (ex-FSE)",
             core_ratio=r.calculated_core_ex_fse, runoff_share=r.warf_ex_fse, fse_share=None),
        dict(row_id="CALCULATED_FSE", basis="Calculated (FSE only)",
             core_ratio=r.calculated_core_fse, runoff_share=r.warf_fse, fse_share=1.0),
        dict(row_id="OVERRIDE", basis="Management override",
             core_ratio=None, runoff_share=None, fse_share=None),
    ]
    pk = pd.DataFrame(rows)
    # Pre-mark CALCULATED_ALL as the default pick
    pk["final"] = pk.row_id.apply(lambda x: "x" if x == "CALCULATED_ALL" else "")
    pk["note"] = ""
    return pk[PICK_COLS]

wb = _XWB()
xl_front(wb, "WARF (Core Ratio) review", "decide",
         [("Study", S.tag), ("Snapshot date", str(S.snapshot_date)),
          ("Products", len(df)),
          ("Core ratio", "1 - runoff share (the stable fraction of balances)"),
          ("Legacy source", "legacy_assumptions_by_account.csv, balance-weighted via FTP to new products"),
          ("WARF source", f"warf_summary_{S.tag}.csv (latest month)")],
         ["'All products' lists every product: Legacy vs Calculated core ratio, the change (red > 10%), FSE share.",
          "On each product tab, 'Pick the core ratio' (top): move the x in 'final'. For OVERRIDE, type the value and reason.",
          "Fill Reviewed by, Review date, put Y in Confirmed, save, upload to inputs/decisions/reviewed/."])

# All products sheet
used = set(["Review", "All products"])
sheet_of = {}
for _, r in df.iterrows():
    sheet_of[r.mv_product] = xl_safe_name(r.mv_product, used)
    used.add(sheet_of[r.mv_product])

allp = df.copy()
allp["tab"] = allp.mv_product.map(sheet_of)
ALL_COLS = ["mv_product", "tab", "balance", "legacy_core_ratio", "calculated_core_ratio",
            "calculated_core_ex_fse", "change_from_legacy", "fse_share", "accounts"]
ws = xl_sheet(wb, "All products", "WARF review: all products",
              ["One row per product (largest first). Core ratio = 1 - runoff. Red = calculated moved > 10% from legacy."],
              "assist", [("warn", "red = moved > 10%")])
xl_table(ws, allp[ALL_COLS],
         fmt={"balance": "$", "accounts": "int", "change_from_legacy": "pct1", "fse_share": "pct1",
              **{c: "pct" for c in ("legacy_core_ratio", "calculated_core_ratio", "calculated_core_ex_fse")}},
         widths={"mv_product": 34, "tab": 26},
         fills=lambda r: "warn" if pd.notna(r.change_from_legacy) and abs(r.change_from_legacy) > 0.10 else None,
         bars=("balance",), links={"tab": lambda r: r.tab})

# Per-product tabs
for _, r in df.iterrows():
    sn = sheet_of[r.mv_product]
    pk = pick_rows_warf(r)
    ws = xl_sheet(wb, sn, f"WARF review: {r.mv_product}",
                  [f"Pick the core ratio for {r.mv_product}: exactly one x in 'final'. Balance ${r.balance:,.0f}.",
                   f"FSE share: {r.fse_share:.1%}" if pd.notna(r.fse_share) else ""],
                  "decide", LEGEND)
    ws.cell(5, 1, "product").font = _XF(bold=True)
    ws.cell(5, 2, r.mv_product).font = _XF(bold=True)
    last = xl_table(ws, pk,
                    fmt={"core_ratio": "pct", "runoff_share": "pct", "fse_share": "pct1"},
                    widths={"row_id": 22, "basis": 34, "core_ratio": 14, "runoff_share": 14, "fse_share": 12, "note": 60},
                    edit=("final", "note"),
                    fills=lambda row: "override" if row.row_id == "OVERRIDE" else ("used" if row.final == "x" else ("alt" if row.row_id == "LEGACY" else None)),
                    bold=lambda row, c: c == "core_ratio" and (row.final == "x" or row.row_id == "LEGACY"))
    ws.cell(last, PICK_COLS.index("core_ratio") + 1).fill = _XP("solid", fgColor=XL["edit"])
    dv = _XDV(type="list", formula1='"x"', allow_blank=True)
    ws.add_data_validation(dv)
    dv.add(f"{_xcol(PICK_COLS.index('final') + 1)}{XL_TABLE_ROW + 1}:{_xcol(PICK_COLS.index('final') + 1)}{last}")
    ws.freeze_panes = ws.cell(XL_TABLE_ROW + 1, 2)

wb.save(WARF_XLSX)
print(f"{len(df)} product tabs -> {WARF_XLSX}")
moved = allp[allp.change_from_legacy.abs() > 0.10]
print(f"Products moved > 10% from legacy: {len(moved)}")
if len(moved):
    display(moved[['mv_product', 'legacy_core_ratio', 'calculated_core_ratio', 'change_from_legacy']])