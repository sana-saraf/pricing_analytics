# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 09a | Assumptions Summary
# MAGIC
# MAGIC Consolidated workbook for all NMD assumptions: betas, WALs, WARFs, and truncations.
# MAGIC
# MAGIC | Sheet | Content |
# MAGIC |---|---|
# MAGIC | Top of House | Aggregate summary across all NMD products |
# MAGIC | Assumptions Summary | One row per product: Avg Balance, WAR, Beta, WAL, WARF, Modified WARF, Truncation |
# MAGIC | Beta Detail | Per-product beta detail: LM/POLY all lags, legacy, recommended, final |
# MAGIC | WAL WARF Detail | Per-product and per-group WAL + WARF detail |
# MAGIC
# MAGIC **Excludes:** Internal, Brokered, Repo products (not non-maturing deposits).\
# MAGIC **Modified WARFs** = WARF w/o FSE × 0.7.\
# MAGIC **Run after:** 06a, 06b, 12b, 13, 15, 16 (needs all assumption notebooks to have run).

# COMMAND ----------

# DBTITLE 1,Install openpyxl
# MAGIC %pip install -q openpyxl

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ../00_setup

# COMMAND ----------

# DBTITLE 1,1. Load all assumption data and merge
import numpy as np

# ---- Exclusions: not NMDs ----
EXCLUDE = {'Brokered DDA', 'Brokered MMA', 'Repo Commercial', 'Repo Public Funds',
           'NIB DDA Internal', '(not in study)', 'UNMATCHED'}
_keep = lambda df, col='mv_product': df[~df[col].isin(EXCLUDE)].copy()

# ---- Final betas ----
betas = sqlq("SELECT * FROM {{mv_final_betas}}").toPandas()
betas = _keep(betas)

# ---- Product betas (LM/POLY per lag) ----
pbetas = sqlq("SELECT * FROM {{mv_product_betas}}").toPandas()
pbetas = _keep(pbetas)
LAGS = sorted(set(int(c.split('_')[-1]) for c in pbetas.columns if c.startswith('lm_') and not c.startswith('lm_r2')))

# ---- WAL summary ----
wal = pd.read_csv(OUTPUTS / "excel" / f"{out_name('wal_summary')}.csv")
wal = wal.rename(columns={'avg_balance_calc_month': 'avg_balance'})
wal = _keep(wal)

# ---- WARF summary ----
warf = pd.read_csv(OUTPUTS / "excel" / f"{out_name('warf_summary')}.csv")
warf = _keep(warf)
warf['modified_warf'] = warf['warf_ex_fse'] * 0.7

# ---- Truncation ----
TRUNC_XLSX = OUTPUTS / "excel" / f"{out_name('truncation')}.xlsx"
trunc = pd.read_excel(TRUNC_XLSX, sheet_name="By mv_product")
trunc = _keep(trunc)

trunc_pg = pd.read_excel(TRUNC_XLSX, sheet_name="By product_group")

# ---- Legacy crosswalk ----
LEGACY_CSV = OUTPUTS / "reports" / f"legacy_assumptions_by_product_{S.tag}.csv"
legacy = pd.read_csv(LEGACY_CSV) if LEGACY_CSV.exists() else pd.DataFrame()
if len(legacy):
    legacy = _keep(legacy, 'product')

# ---- Product group mapping (from legacy or betas) ----
if len(legacy) and 'product_group' in legacy.columns:
    pg_map = legacy[['product', 'product_group']].drop_duplicates('product').rename(columns={'product': 'mv_product'})
else:
    pg_map = pd.DataFrame(columns=['mv_product', 'product_group'])

# ---- Master merge ----
df = wal[['mv_product', 'avg_balance', 'wal_current', 'wal_calculated']].copy()
df = df.merge(betas[['mv_product', 'final_beta', 'recommended_beta', 'final_method', 'recommended_r2', 'avg_up', 'avg_down', 'months_ttc']], on='mv_product', how='left')
df = df.merge(warf[['mv_product', 'accounts', 'balance', 'fse_share', 'warf_all', 'warf_ex_fse', 'warf_fse', 'war', 'modified_warf']], on='mv_product', how='left')
df = df.merge(trunc[['mv_product', 'R_squared', 'Month y=30%', 'Month y=25%', 'Month y=20%']], on='mv_product', how='left')
if len(legacy):
    df = df.merge(legacy[['product', 'legacy_beta', 'legacy_trunc', 'legacy_core_ratio']].rename(columns={'product': 'mv_product'}), on='mv_product', how='left')
df = df.merge(pg_map, on='mv_product', how='left')
df = df.sort_values('avg_balance', ascending=False).reset_index(drop=True)

print(f"Products: {len(df)} (excluded {len(EXCLUDE)} non-NMD categories)")
print(f"Total avg balance: ${df.avg_balance.sum():,.0f}")
display(df[['mv_product', 'product_group', 'avg_balance', 'war', 'final_beta', 'wal_current', 'warf_ex_fse', 'modified_warf', 'Month y=30%']].head(10))

# COMMAND ----------

# DBTITLE 1,2. Build Assumptions Summary Excel
REVIEW_DIR = OUTPUTS / "review"
REVIEW_DIR.mkdir(parents=True, exist_ok=True)
SUMMARY_XLSX = REVIEW_DIR / f"Assumptions_Summary_{S.tag}.xlsx"

wb = _XWB()

# ======================================================================
# Sheet 1: Top of House
# ======================================================================
ws = xl_sheet(wb, "Top of House", f"{S.tag} Deposit Study | Non-Maturing Deposits",
              [f"Snapshot: {S.snapshot_date} | Excludes Internal, Brokered, Repo products.",
               "Modified WARFs = WARF w/o FSE × 0.7."],
              "assist", [])

toh = pd.DataFrame([
    dict(metric="Total Products", value=len(df)),
    dict(metric="Total Accounts", value=df.accounts.sum()),
    dict(metric="Total Avg Balance", value=df.avg_balance.sum()),
    dict(metric="Wtd Avg WAR (bps)", value=np.average(df.war.dropna(), weights=df.loc[df.war.notna(), 'avg_balance']) * 100 if df.war.notna().any() else None),
    dict(metric="Wtd Avg Beta", value=np.average(df.final_beta.dropna(), weights=df.loc[df.final_beta.notna(), 'avg_balance']) if df.final_beta.notna().any() else None),
    dict(metric="Wtd Avg WAL (Current, months)", value=np.average(df.wal_current.dropna(), weights=df.loc[df.wal_current.notna(), 'avg_balance']) if df.wal_current.notna().any() else None),
    dict(metric="Wtd Avg WAL (Calculated, months)", value=np.average(df.wal_calculated.dropna(), weights=df.loc[df.wal_calculated.notna(), 'avg_balance']) if df.wal_calculated.notna().any() else None),
    dict(metric="Wtd Avg WARF (all)", value=np.average(df.warf_all.dropna(), weights=df.loc[df.warf_all.notna(), 'avg_balance']) if df.warf_all.notna().any() else None),
    dict(metric="Wtd Avg WARF (ex FSE)", value=np.average(df.warf_ex_fse.dropna(), weights=df.loc[df.warf_ex_fse.notna(), 'avg_balance']) if df.warf_ex_fse.notna().any() else None),
    dict(metric="Wtd Avg Modified WARF", value=np.average(df.modified_warf.dropna(), weights=df.loc[df.modified_warf.notna(), 'avg_balance']) if df.modified_warf.notna().any() else None),
    dict(metric="Wtd Avg Truncation (months to 30%)", value=np.average(df['Month y=30%'].dropna(), weights=df.loc[df['Month y=30%'].notna(), 'avg_balance']) if df['Month y=30%'].notna().any() else None),
])
xl_table(ws, toh, fmt={"value": "num"}, widths={"metric": 40, "value": 20})

# Group-level summary below
grp = df.groupby('product_group', as_index=False).agg(
    products=('mv_product', 'count'), avg_balance=('avg_balance', 'sum'),
    accounts=('accounts', 'sum'), war=('war', lambda x: np.average(x.dropna(), weights=df.loc[x.dropna().index, 'avg_balance']) if x.notna().any() else None),
).sort_values('avg_balance', ascending=False)
grp_start = toh.shape[0] + XL_TABLE_ROW + 3
ws.cell(grp_start, 1, "By Product Group").font = _XF(bold=True, size=12)
for ci, col in enumerate(grp.columns, 1):
    ws.cell(grp_start + 1, ci, col).font = _XF(bold=True)
    for ri, val in enumerate(grp[col], 2):
        c = ws.cell(grp_start + ri, ci, val)
        if col == 'avg_balance' and isinstance(val, (int, float)):
            c.number_format = '#,##0'

# ======================================================================
# Sheet 2: Assumptions Summary (one row per product)
# ======================================================================
SUM_COLS = ['product_group', 'mv_product', 'avg_balance', 'accounts', 'war']
if 'legacy_beta' in df.columns:
    SUM_COLS += ['legacy_beta']
SUM_COLS += ['final_beta', 'final_method', 'wal_current', 'wal_calculated',
             'warf_all', 'warf_ex_fse', 'modified_warf', 'fse_share']
if 'legacy_trunc' in df.columns:
    SUM_COLS += ['legacy_trunc']
SUM_COLS += ['Month y=30%', 'Month y=25%', 'Month y=20%', 'R_squared']

ws2 = xl_sheet(wb, "Assumptions Summary", f"{S.tag} | Assumptions Summary",
               ["One row per NMD product, sorted by Avg Balance. Modified WARF = WARF w/o FSE × 0.7."],
               "assist", [])
xl_table(ws2, df[[c for c in SUM_COLS if c in df.columns]],
         fmt={'avg_balance': '$', 'accounts': 'int', 'war': 'pct', 'final_beta': 'beta',
              'legacy_beta': 'beta', 'wal_current': 'n1', 'wal_calculated': 'n1',
              'warf_all': 'pct', 'warf_ex_fse': 'pct', 'modified_warf': 'pct', 'fse_share': 'pct',
              'legacy_trunc': 'n1', 'Month y=30%': 'int', 'Month y=25%': 'int', 'Month y=20%': 'int', 'R_squared': 'pct'},
         widths={'product_group': 22, 'mv_product': 32, 'avg_balance': 16, 'final_method': 18},
         bars=('avg_balance',))

# ======================================================================
# Sheet 3: Beta Detail
# ======================================================================
beta_detail = pbetas[['mv_product', 'snapshot_balance', 'snapshot_war', 'months_ttc']].copy()
beta_detail = beta_detail.rename(columns={'snapshot_balance': 'avg_balance'})
# merge avg_balance from WAL instead
beta_detail = beta_detail.drop(columns=['avg_balance']).merge(wal[['mv_product', 'avg_balance']], on='mv_product', how='left')
# Add all LM/POLY columns
for L in LAGS:
    for pfx in ['lm', 'poly']:
        for sfx in ['', '_r2']:
            col = f"{pfx}{sfx}_{L}"
            if col in pbetas.columns:
                beta_detail[col] = pbetas.set_index('mv_product')[col].reindex(beta_detail.mv_product).values
beta_detail = beta_detail.merge(betas[['mv_product', 'avg_up', 'avg_down', 'recommended_method', 'recommended_beta', 'recommended_r2', 'final_method', 'final_beta', 'final_r2']], on='mv_product', how='left')
if len(legacy):
    beta_detail = beta_detail.merge(legacy[['product', 'legacy_beta']].rename(columns={'product': 'mv_product'}), on='mv_product', how='left')
beta_detail = _keep(beta_detail).sort_values('avg_balance', ascending=False)

BETA_COLS = ['mv_product', 'avg_balance', 'months_ttc']
for L in LAGS:
    BETA_COLS += [f'lm_{L}', f'lm_r2_{L}']
for L in LAGS:
    BETA_COLS += [f'poly_{L}', f'poly_r2_{L}']
BETA_COLS += ['avg_up', 'avg_down']
if 'legacy_beta' in beta_detail.columns:
    BETA_COLS += ['legacy_beta']
BETA_COLS += ['recommended_method', 'recommended_beta', 'recommended_r2', 'final_method', 'final_beta', 'final_r2']
beta_cols_exist = [c for c in BETA_COLS if c in beta_detail.columns]

ws3 = xl_sheet(wb, "Beta Detail", f"{S.tag} | Beta Assumptions Detail",
               [f"LM and POLY betas at each lag (0-{max(LAGS)}). TTC cycle: {S.params.get('ttc_cycle', '')}."],
               "assist", [])
fmt3 = {'avg_balance': '$', 'months_ttc': 'int', 'recommended_r2': 'pct', 'final_r2': 'pct',
        **{c: 'beta' for c in beta_cols_exist if c.startswith(('lm_', 'poly_')) and 'r2' not in c},
        **{c: 'beta' for c in ('legacy_beta', 'recommended_beta', 'final_beta', 'avg_up', 'avg_down') if c in beta_cols_exist},
        **{c: 'pct' for c in beta_cols_exist if 'r2' in c}}
xl_table(ws3, beta_detail[beta_cols_exist], fmt=fmt3,
         widths={'mv_product': 32, 'avg_balance': 16, 'recommended_method': 18, 'final_method': 18},
         bars=('avg_balance',))

# ======================================================================
# Sheet 4: WAL WARF Detail (product + group summaries)
# ======================================================================
ww = df[['product_group', 'mv_product', 'avg_balance', 'accounts', 'war',
         'wal_current', 'wal_calculated',
         'warf_all', 'warf_ex_fse', 'warf_fse', 'modified_warf', 'fse_share',
         'Month y=30%', 'Month y=25%', 'Month y=20%', 'R_squared']].copy()

# Group-level rollup
ww_grp = ww.groupby('product_group', as_index=False).agg(
    avg_balance=('avg_balance', 'sum'), accounts=('accounts', 'sum'),
    war=('war', lambda x: np.average(x.dropna(), weights=ww.loc[x.dropna().index, 'avg_balance']) if x.notna().any() else None),
    wal_current=('wal_current', lambda x: np.average(x.dropna(), weights=ww.loc[x.dropna().index, 'avg_balance']) if x.notna().any() else None),
    wal_calculated=('wal_calculated', lambda x: np.average(x.dropna(), weights=ww.loc[x.dropna().index, 'avg_balance']) if x.notna().any() else None),
    warf_all=('warf_all', lambda x: np.average(x.dropna(), weights=ww.loc[x.dropna().index, 'avg_balance']) if x.notna().any() else None),
    warf_ex_fse=('warf_ex_fse', lambda x: np.average(x.dropna(), weights=ww.loc[x.dropna().index, 'avg_balance']) if x.notna().any() else None),
    modified_warf=('modified_warf', lambda x: np.average(x.dropna(), weights=ww.loc[x.dropna().index, 'avg_balance']) if x.notna().any() else None),
    fse_share=('fse_share', lambda x: np.average(x.dropna(), weights=ww.loc[x.dropna().index, 'avg_balance']) if x.notna().any() else None),
).sort_values('avg_balance', ascending=False)
ww_grp.insert(1, 'mv_product', '(Group Total)')

WW_COLS = list(ww.columns)
ws4 = xl_sheet(wb, "WAL WARF Detail", f"{S.tag} | WAL, WARF & Truncation Detail",
               ["Product-level detail followed by product-group rollups. Modified WARF = WARF w/o FSE × 0.7."],
               "assist", [])
fmt4 = {'avg_balance': '$', 'accounts': 'int', 'war': 'pct',
        'wal_current': 'n1', 'wal_calculated': 'n1',
        'warf_all': 'pct', 'warf_ex_fse': 'pct', 'warf_fse': 'pct', 'modified_warf': 'pct', 'fse_share': 'pct',
        'Month y=30%': 'int', 'Month y=25%': 'int', 'Month y=20%': 'int', 'R_squared': 'pct'}
last_prod = xl_table(ws4, ww[WW_COLS], fmt=fmt4,
                     widths={'product_group': 22, 'mv_product': 32, 'avg_balance': 16},
                     bars=('avg_balance',))

# Add group rollup section below
grp_hdr_row = last_prod + 2
ws4.cell(grp_hdr_row, 1, "Product Group Summary").font = _XF(bold=True, size=12)
GRP_COLS = [c for c in ww_grp.columns if c in WW_COLS or c == 'mv_product']
for ci, col in enumerate(GRP_COLS, 1):
    c = ws4.cell(grp_hdr_row + 1, ci, col)
    c.font = _XF(bold=True)
for ri, (_, row) in enumerate(ww_grp[GRP_COLS].iterrows(), 2):
    for ci, col in enumerate(GRP_COLS, 1):
        c = ws4.cell(grp_hdr_row + ri, ci, row[col])
        if col == 'avg_balance' and isinstance(row[col], (int, float)):
            c.number_format = '#,##0'
        elif col in ('warf_all', 'warf_ex_fse', 'modified_warf', 'fse_share', 'war') and isinstance(row[col], (int, float)):
            c.number_format = '0.00%'
        elif col in ('wal_current', 'wal_calculated') and isinstance(row[col], (int, float)):
            c.number_format = '0.0'

wb.save(SUMMARY_XLSX)
print(f"Assumptions Summary: {SUMMARY_XLSX}")
print(f"Sheets: {wb.sheetnames}")
print(f"Products: {len(df)} | Groups: {len(ww_grp)}")