# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 08 | Formatted reports and HTML dashboards
# MAGIC
# MAGIC **What this builds.** The presentation layer on top of 01 to 06, in the house style of the earlier beta reports (navy headers, product-group bands, totals, dollar / percent formats, color scales on betas and R2).
# MAGIC
# MAGIC | Step | Output |
# MAGIC |---|---|
# MAGIC | 1 | Restyles every workbook 01 to 06 wrote (`inputs/*_<tag>.xlsx`, `outputs/excel/*_<tag>.xlsx`) |
# MAGIC | 2 | `outputs/excel/Beta_Report_<tag>.xlsx`: Summary by product group (with group subtotals and a grand total), Product Groups, Bank Split, Lag Detail, Up Down Cycles, Product Map, Product Lookup |
# MAGIC | 3 | `outputs/html/Fed_Funds_Cycles_<tag>.html`, `outputs/html/Product_Dashboard_<tag>.html`, `outputs/html/Product_Mapping_Dashboard_<tag>.html` |
# MAGIC
# MAGIC Product IDs, codes and short names come from `inputs/product_lookup.csv`. **Run after:** 06.

# COMMAND ----------

# DBTITLE 1,Install openpyxl and plotly
# MAGIC %pip install -q openpyxl plotly

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ../00_setup

# COMMAND ----------

# DBTITLE 1,House style (shared by every workbook)
import re, datetime as _dt
from decimal import Decimal
from openpyxl import Workbook, load_workbook
from openpyxl.styles import PatternFill, Font, Border, Side, Alignment
from openpyxl.formatting.rule import ColorScaleRule, CellIsRule
from openpyxl.utils import get_column_letter

ARIAL, NAVY = "Arial", "1F4E79"
HDR_FILL, HDR_FONT = PatternFill("solid", fgColor=NAVY), Font(name=ARIAL, bold=True, color="FFFFFF", size=9)
STD_FONT, TITLE_FONT = Font(name=ARIAL, size=9), Font(name=ARIAL, bold=True, size=12, color=NAVY)
SUB_FONT = Font(name=ARIAL, size=9, italic=True, color="555555")
GRP_FILL, GRP_FONT = PatternFill("solid", fgColor="D6E4F0"), Font(name=ARIAL, size=9, bold=True, color=NAVY)
TOT_FILL, TOT_FONT = PatternFill("solid", fgColor="E8EDF3"), Font(name=ARIAL, size=9, bold=True)
BAND_FILL, FLAG_FILL = PatternFill("solid", fgColor="F7F9FC"), PatternFill("solid", fgColor="FCE4D6")
SIDE = Side(style="thin", color="BFBFBF")
BORDER = Border(left=SIDE, right=SIDE, top=SIDE, bottom=SIDE)
CENTER = Alignment(horizontal="center", vertical="center", wrap_text=True)
FMT_DOLLAR, FMT_PCT, FMT_R2, FMT_INT = '"$"#,##0', "0.00%", "0.0", "#,##0"     # R2 is in points (0-100)
BIG_SHEET = 20000          # sheets longer than this get header / widths / filter only (cell styling is slow)
R2_SCALE = dict(start_type="num", start_value=0, start_color="F8696B", mid_type="num", mid_value=50,
                mid_color="FFEB84", end_type="num", end_value=100, end_color="63BE7B")
BETA_SCALE = dict(start_type="min", start_color="FFFFFF", end_type="max", end_color="5B9BD5")

def col_format(name):
    """Number format from the column name (betas, WAR and shares as %, balances as $, R2 as 0.000)."""
    n = str(name or "").lower()
    if re.search(r"(^|_| )r2($|_| )", n): return FMT_R2
    if "share" in n: return FMT_PCT
    if re.search(r"balance|eopbook|avgbook|sumprod", n): return FMT_DOLLAR
    if re.search(r"beta|^lm_\d|^poly_\d|^avg_up|^avg_down|^up_|^down_|war|coupon|^rate_(min|max|p\d+|std)", n): return FMT_PCT
    if re.search(r"accounts|months|subproducts|^n_|count", n): return FMT_INT
    return None

def scale_for(name):
    n = str(name or "").lower()
    if re.search(r"(^|_| )r2($|_| )", n): return R2_SCALE
    if re.search(r"beta|^lm_\d|^poly_\d|^avg_up|^avg_down", n): return BETA_SCALE
    return None

def xl(v):
    """Python / numpy / Spark values -> something openpyxl writes."""
    if v is None: return None
    if isinstance(v, Decimal): return float(v)
    if isinstance(v, (np.bool_, bool)): return bool(v)
    if isinstance(v, np.integer): return int(v)
    if isinstance(v, (np.floating, float)): return None if np.isnan(v) else float(v)
    if isinstance(v, (pd.Timestamp, _dt.date, _dt.datetime)): return str(v)[:10]
    try:
        if pd.isna(v): return None
    except (TypeError, ValueError):
        pass
    return v if isinstance(v, (str, int)) else str(v)

def style_sheet(ws, header_row=1, first_data=None, last_data=None):
    """House style for a tabular sheet whose header is on header_row."""
    if ws.max_row < header_row or ws.max_column < 1:
        return
    hdr = [c.value for c in ws[header_row]]
    first = first_data or header_row + 1
    last = last_data or ws.max_row
    big = (last - first) > BIG_SHEET
    for c in ws[header_row]:
        c.fill, c.font, c.alignment, c.border = HDR_FILL, HDR_FONT, CENTER, BORDER
    ws.row_dimensions[header_row].height = 30
    for j, name in enumerate(hdr, 1):
        L, fmt = get_column_letter(j), col_format(name)
        if not big:
            for i in range(first, last + 1):
                cell = ws.cell(i, j)
                cell.font, cell.border = (cell.font if cell.font.bold else STD_FONT), BORDER
                if (i - first) % 2 == 1 and cell.fill.fgColor.rgb in (None, "00000000"):
                    cell.fill = BAND_FILL
                if fmt and isinstance(cell.value, (int, float)) and not isinstance(cell.value, bool):
                    cell.number_format = fmt
        w = max([len(str(name or ""))] + [len(str(ws.cell(i, j).value or "")) for i in range(first, min(last, first + 300) + 1)])
        ws.column_dimensions[L].width = min(max(9, w + 2), 48)
        if last > first and not big:
            rng = f"{L}{first}:{L}{last}"
            sc = scale_for(name)
            if sc:
                ws.conditional_formatting.add(rng, ColorScaleRule(**sc))
            elif str(name or "").lower().startswith("flag"):
                ws.conditional_formatting.add(rng, CellIsRule(operator="equal", formula=["TRUE"], fill=FLAG_FILL))
    ws.freeze_panes = ws.cell(header_row + 1, 1)
    ws.auto_filter.ref = f"A{header_row}:{get_column_letter(len(hdr))}{last}"
    ws.sheet_view.showGridLines = False

def style_readme(ws):
    for c in ws[1]:
        c.fill, c.font, c.alignment = HDR_FILL, HDR_FONT, CENTER
    for row in ws.iter_rows(min_row=2):
        for c in row:
            c.font, c.alignment = STD_FONT, Alignment(wrap_text=True, vertical="top")
    ws.column_dimensions["A"].width, ws.column_dimensions["B"].width = 28, 110
    ws.sheet_view.showGridLines = False

def restyle(path):
    wb = load_workbook(path)
    for ws in wb.worksheets:
        style_readme(ws) if ws.title.upper() == "README" else style_sheet(ws)
    wb.save(path)

def df_to_ws(wb, title, df, note=None, sub=None):
    """DataFrame -> new styled sheet; an optional title and subtitle sit above the header."""
    ws = wb.create_sheet(title[:31])
    hr = 1
    if note:
        ws.cell(1, 1, note).font = TITLE_FONT
        if sub:
            ws.cell(2, 1, sub).font = SUB_FONT
        hr = 4
    for j, c in enumerate(df.columns, 1):
        ws.cell(hr, j, c)
    for i, row in enumerate(df.itertuples(index=False), hr + 1):
        for j, v in enumerate(row, 1):
            ws.cell(i, j, xl(v))
    style_sheet(ws, hr)
    return ws

print("House style ready.")

# COMMAND ----------

# DBTITLE 1,Step 1 header
# MAGIC %md
# MAGIC ## Step 1. Restyle the workbooks 01 to 06 wrote
# MAGIC Every `*_<tag>.xlsx` in `inputs/` and `outputs/excel/` gets navy headers, banded rows, borders, number formats by column name, color scales on betas and R2, flags highlighted, frozen headers and filters. The data is not changed.

# COMMAND ----------

# DBTITLE 1,1. Restyle workbooks
INPUTS_DIR = MV_ROOT / "inputs"
books = sorted(set((OUTPUTS / "excel").glob(f"*_{S.tag}.xlsx")) | set(INPUTS_DIR.glob(f"*_{S.tag}.xlsx")))
books = [p for p in books if not p.name.startswith("Beta_Report_")]
for p in books:
    restyle(p)
    print(f"  styled {p.relative_to(MV_ROOT)}")
print(f"{len(books)} workbooks styled")

# COMMAND ----------

# DBTITLE 1,Step 2 header
# MAGIC %md
# MAGIC ## Step 2. Beta report workbook
# MAGIC One row per final product, grouped by product group (the group with the largest balance first). Group rows and the grand total carry the balance and the balance-weighted WAR, final beta, average up and average down betas.

# COMMAND ----------

# DBTITLE 1,2a. Report data
assert S.table_exists("mv_final_betas"), "run 06 first"
final = spark.table(S.table("mv_final_betas")).toPandas()
review = spark.table(S.table("mv_beta_review")).toPandas()
pw = spark.table(S.table("mv_product_war")).toPandas()
pmap = sqlq("""
SELECT legacy_bank, product_group, mv_product, subproductid, acct_typ_code, acct_typ_desc, product AS current_product,
       COUNT(*) AS accounts, SUM(eopbook) AS balance, TRY_DIVIDE(SUM(sumprod_eoprate), SUM(eopbook)) AS war
FROM {{mv_product_map}}
WHERE eopdate = '{{snapshot_date}}'
GROUP BY ALL
""").toPandas()
for d in (final, review, pw, pmap):
    for c in d.columns:
        if d[c].dtype == object and d[c].map(lambda v: isinstance(v, Decimal)).any():
            d[c] = d[c].astype(float)
pmap[["product_group", "mv_product"]] = pmap[["product_group", "mv_product"]].fillna("(unmapped)")

LOOKUP_PATH = INPUTS_DIR / "product_lookup.csv"
lookup = (pd.read_csv(LOOKUP_PATH, dtype=str, keep_default_na=False) if LOOKUP_PATH.exists()
          else pd.DataFrame(columns=["product_id", "product_name", "product_code", "short_name"]))
lookup["product_id"] = pd.to_numeric(lookup["product_id"], errors="coerce")

# product group of each final product = the group holding most of its balance
pg = pmap.sort_values("balance").groupby("mv_product").product_group.last().rename("product_group")
bank = pmap.pivot_table(index="mv_product", columns="legacy_bank", values="balance", aggfunc="sum").add_prefix("bal_")
flag_cols = [c for c in final.columns if c.startswith("flag_")]
summary = (final.merge(pg, left_on="mv_product", right_index=True, how="left")
                .merge(bank, left_on="mv_product", right_index=True, how="left")
                .merge(lookup, left_on="mv_product", right_on="product_name", how="left"))
summary["product_group"] = summary["product_group"].fillna("(no group)")
summary["share"] = summary.snapshot_balance / summary.snapshot_balance.sum()
summary["flag_list"] = summary[flag_cols].apply(lambda r: ", ".join(c[5:] for c in flag_cols if bool(r[c]) and r[c] == r[c]), axis=1)
for b in ("bal_PNFP", "bal_SNV"):
    if b not in summary:
        summary[b] = np.nan
no_code = summary[summary.product_code.isna()].mv_product.tolist()
print(f"{len(summary)} products | ${summary.snapshot_balance.sum():,.0f} | without a product code in product_lookup.csv: {no_code or 'none'}")
if no_code:
    log_issues("08 reports", [("PRODUCT_NOT_IN_LOOKUP", "MEDIUM", p, "final product has no row in inputs/product_lookup.csv (no id / code / short name)",
                               "", float(summary.loc[summary.mv_product == p, "snapshot_balance"].sum())) for p in no_code])

# COMMAND ----------

# DBTITLE 1,2b. Beta_Report workbook (expanded)
def wavg(d, col, w="snapshot_balance"):
    x = d[[col, w]].dropna()
    return float((x[col] * x[w]).sum() / x[w].sum()) if len(x) and x[w].sum() else None

COLS = [("Product Group", "product_group", None), ("ID", "product_id", FMT_INT), ("Code", "product_code", None),
        ("Short Name", "short_name", None), ("Product", "mv_product", None), ("PNFP Balance", "bal_PNFP", FMT_DOLLAR),
        ("SNV Balance", "bal_SNV", FMT_DOLLAR), ("Total Balance", "snapshot_balance", FMT_DOLLAR), ("Share", "share", FMT_PCT),
        ("WAR", "snapshot_war", FMT_PCT), ("TTC Months", "months_ttc", FMT_INT), ("Final Method", "final_method", None),
        ("Final Beta", "final_beta", FMT_PCT), ("R2", "final_r2", FMT_R2), ("Avg Up Beta", "avg_up", FMT_PCT),
        ("Avg Down Beta", "avg_down", FMT_PCT), ("Source", "final_source", None), ("Flags", "flag_list", None)]
WAVG = ["snapshot_war", "final_beta", "avg_up", "avg_down"]
SUMS = ["bal_PNFP", "bal_SNV", "snapshot_balance", "share"]

def put_row(ws, r, values, font=STD_FONT, fill=None):
    for j, (_, key, fmt) in enumerate(COLS, 1):
        c = ws.cell(r, j, xl(values.get(key)))
        c.font, c.border = font, BORDER
        if fill: c.fill = fill
        if fmt and isinstance(c.value, (int, float)): c.number_format = fmt

wb = Workbook()
ws = wb.active
ws.title = "Summary"
ws.cell(1, 1, f"Deposit Beta Report | {S.tag}").font = TITLE_FONT
ws.cell(2, 1, f"Snapshot {S.snapshot_date} | mode {MODE} | mapping {MAPPING_SOURCE} | TTC cycle {TTC} | lags {LAGS} | "
              f"built {_dt.datetime.now():%Y-%m-%d %H:%M}").font = SUB_FONT
HR = 4
for j, (h, _, _) in enumerate(COLS, 1):
    c = ws.cell(HR, j, h)
    c.fill, c.font, c.alignment, c.border = HDR_FILL, HDR_FONT, CENTER, BORDER
ws.row_dimensions[HR].height = 30
r = HR + 1
prod_rows = []
groups = summary.groupby("product_group").snapshot_balance.sum().sort_values(ascending=False).index
for g in groups:
    gd = summary[summary.product_group == g].sort_values("snapshot_balance", ascending=False)
    tot = {k: gd[k].sum() for k in SUMS} | {k: wavg(gd, k) for k in WAVG}
    put_row(ws, r, {"product_group": g, "mv_product": f"{len(gd)} products", **tot}, font=GRP_FONT, fill=GRP_FILL)
    r += 1
    for rec in gd.to_dict("records"):
        put_row(ws, r, {**rec, "product_group": ""}, fill=BAND_FILL if (r % 2) else None)
        if rec.get("flag_list"):
            ws.cell(r, len(COLS)).fill = FLAG_FILL
        prod_rows.append(r)
        r += 1
gt = {k: summary[k].sum() for k in SUMS} | {k: wavg(summary, k) for k in WAVG}
put_row(ws, r, {"product_group": "TOTAL", "mv_product": f"{len(summary)} products", **gt}, font=TOT_FONT, fill=TOT_FILL)
for key, sc in (("final_beta", BETA_SCALE), ("avg_up", BETA_SCALE), ("avg_down", BETA_SCALE), ("final_r2", R2_SCALE)):
    L = get_column_letter([k for _, k, _ in COLS].index(key) + 1)
    ws.conditional_formatting.add(f"{L}{HR + 1}:{L}{r - 1}", ColorScaleRule(**sc))
for j, (h, key, _) in enumerate(COLS, 1):
    w = max([len(h)] + [len(str(v)) for v in summary[key].astype(str).head(300)]) if key in summary else len(h)
    ws.column_dimensions[get_column_letter(j)].width = min(max(9, w + 2), 42)
ws.freeze_panes = ws.cell(HR + 1, 6)
ws.sheet_view.showGridLines = False

# group, bank, lag and cycle tabs
grp = (summary.groupby("product_group").apply(lambda d: pd.Series({
          "products": len(d), "pnfp_balance": d.bal_PNFP.sum(), "snv_balance": d.bal_SNV.sum(), "snapshot_balance": d.snapshot_balance.sum(),
          "share": d.share.sum(), "war": wavg(d, "snapshot_war"), "final_beta": wavg(d, "final_beta"),
          "avg_up_beta": wavg(d, "avg_up"), "avg_down_beta": wavg(d, "avg_down")}))
       .reset_index().sort_values("snapshot_balance", ascending=False))
df_to_ws(wb, "Product Groups", grp, "Product groups", "balance-weighted WAR and betas")
bs = (pmap.groupby(["mv_product", "legacy_bank"]).apply(lambda d: pd.Series({
          "subproducts": d.subproductid.nunique(), "accounts": d.accounts.sum(), "balance": d.balance.sum(),
          "war": float((d.war * d.balance).sum() / d.balance.sum()) if d.balance.sum() else None}))
      .reset_index().merge(pg, left_on="mv_product", right_index=True, how="left")
      .sort_values(["product_group", "mv_product", "legacy_bank"]))
df_to_ws(wb, "Bank Split", bs[["product_group", "mv_product", "legacy_bank", "subproducts", "accounts", "balance", "war"]],
         "Balances by legacy bank", f"snapshot {S.snapshot_date}")
lag_cols = [c for L in LAGS for c in (f"lm_{L}", f"lm_r2_{L}", f"poly_{L}", f"poly_r2_{L}")]
best_cols = ["best_lm_lag", "best_lm_beta", "best_lm_r2", "best_poly_lag", "best_poly_beta", "best_poly_r2",
             "avg_lm_poly_beta", "recommended_method", "recommended_beta", "recommended_r2", "recommended_reason"]
lag = review[["mv_product", "snapshot_balance", "months_ttc"] + [c for c in lag_cols + best_cols if c in review]].sort_values("snapshot_balance", ascending=False)
df_to_ws(wb, "Lag Detail", lag, "TTC linear and polynomial betas by lag", f"cycle {TTC}; R2 in points (0-100)")
cyc_cols = [c for c in review.columns if re.match(r"^(up|down)_.+_\d+$", c) or re.match(r"^avg_(up|down)(_\d+)?$", c)]
df_to_ws(wb, "Up Down Cycles", review[["mv_product", "snapshot_balance"] + cyc_cols].sort_values("snapshot_balance", ascending=False),
         "Endpoint betas by cycle and lag", f"averages at lag {UD_LAG}")
df_to_ws(wb, "Product Map", pmap.sort_values(["product_group", "mv_product", "legacy_bank", "balance"], ascending=[True, True, True, False]),
         "Subproduct to final product", f"accounts at {S.snapshot_date}")
df_to_ws(wb, "Product Lookup", lookup, "Product lookup", "inputs/product_lookup.csv")

# ---- Merged from former product_map.xlsx (05) ----
_sel = spark.table(S.table("mv_product_selection")).toPandas()
_prod_summary = sqlq("""
SELECT COALESCE(mv_product, 'UNASSIGNED') AS mv_product,
       CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(legacy_bank))) AS banks,
       COUNT(DISTINCT sub_key) AS subproducts, COUNT(*) AS accounts, SUM(eopbook) AS eopbook,
       TRY_DIVIDE(SUM(sumprod_eoprate), SUM(eopbook)) AS war,
       CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(COALESCE(product, 'UNMAPPED')))) AS current_products
FROM {{mv_product_map}} WHERE eopdate = '{{snapshot_date}}' GROUP BY 1 ORDER BY eopbook DESC
""").toPandas()
_flow = sqlq("""
SELECT legacy_bank, COALESCE(product, 'UNMAPPED') AS current_product, COALESCE(mv_product, 'UNASSIGNED') AS mv_product,
       COUNT(DISTINCT sub_key) AS subproducts, COUNT(*) AS accounts, SUM(eopbook) AS eopbook
FROM {{mv_product_map}} WHERE eopdate = '{{snapshot_date}}' GROUP BY ALL ORDER BY legacy_bank, current_product, eopbook DESC
""").toPandas()
df_to_ws(wb, "Product Summary", _prod_summary, "Product summary at snapshot", f"{S.snapshot_date}")
df_to_ws(wb, "Current to New Product", _flow, "Current product to new MV product", f"accounts at {S.snapshot_date}")
df_to_ws(wb, "Subproduct Assignment", _sel.sort_values("snapshot_balance", ascending=False),
         "Subproduct to MV product assignment", f"{len(_sel)} subproducts")

# ---- Merged from former final_betas.csv (06) ----
df_to_ws(wb, "Final Betas", final.sort_values("snapshot_balance", ascending=False),
         "Final betas by product (all columns)", f"{len(final)} products")

BETA_REPORT = out("excel", "Beta_Report", "xlsx")
wb.save(BETA_REPORT)
print(f"Wrote {BETA_REPORT.relative_to(MV_ROOT)}: {', '.join(wb.sheetnames)}")

# COMMAND ----------

# DBTITLE 1,Step 3 header
# MAGIC %md
# MAGIC ## Step 3. HTML dashboards
# MAGIC Self-contained pages (Plotly from the CDN), openable from the workspace or a browser:
# MAGIC * **Fed funds cycles**: fed funds history with the study's rate cycles shaded (up green, down red, TTC outlined).
# MAGIC * **Product dashboard**: headline numbers, a balance treemap colored by beta, final betas, beta vs R2, WAR against fed funds for any product (dropdown), up vs down betas and the summary table.
# MAGIC * **Product mapping dashboard**: bank, product group, product and subproduct by balance, plus the mapping table.

# COMMAND ----------

# DBTITLE 1,3. HTML dashboards
import html as _html
import plotly.graph_objects as go
import plotly.express as px
import plotly.io as pio

CSS = """<style>
body{font-family:Arial,Helvetica,sans-serif;margin:0;background:#f4f6f9;color:#222}
header{background:#1F4E79;color:#fff;padding:16px 28px}header h1{margin:0;font-size:22px}header p{margin:4px 0 0;font-size:13px;opacity:.85}
.wrap{padding:18px 28px}.card{background:#fff;border-radius:8px;box-shadow:0 1px 3px rgba(0,0,0,.12);padding:12px;margin-bottom:18px}
.kpis{display:flex;gap:14px;flex-wrap:wrap}.kpi{flex:1;min-width:160px;background:#fff;border-left:5px solid #1F4E79;border-radius:6px;padding:10px 14px;box-shadow:0 1px 3px rgba(0,0,0,.12)}
.kpi b{display:block;font-size:20px;color:#1F4E79}.kpi span{font-size:12px;color:#555}
table{border-collapse:collapse;width:100%;font-size:12px}th{background:#1F4E79;color:#fff;padding:6px;position:sticky;top:0}
td{padding:5px 6px;border-bottom:1px solid #e3e7ee}tr:nth-child(even) td{background:#f7f9fc}tr.grp td{background:#D6E4F0;font-weight:bold;color:#1F4E79}
.scroll{max-height:560px;overflow:auto}
</style>"""

def page(path, title, sub, parts):
    """parts: plotly figures or raw HTML strings, in order."""
    body, first = [], True
    for p in parts:
        if isinstance(p, go.Figure):
            p.update_layout(template="plotly_white", font=dict(family="Arial"), margin=dict(l=50, r=30, t=60, b=60))
            body.append('<div class="card">' + pio.to_html(p, full_html=False, include_plotlyjs="cdn" if first else False) + "</div>")
            first = False
        else:
            body.append(p)
    doc = (f"<!doctype html><html><head><meta charset='utf-8'><title>{_html.escape(title)}</title>{CSS}</head><body>"
           f"<header><h1>{_html.escape(title)}</h1><p>{_html.escape(sub)}</p></header><div class='wrap'>{''.join(body)}</div></body></html>")
    path.write_text(doc, encoding="utf-8")
    return path

def fmt_cell(key, v):
    v = xl(v)
    if v is None: return ""
    f = dict((k, fm) for _, k, fm in COLS).get(key) or col_format(key)
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return f"${v:,.0f}" if f == FMT_DOLLAR else f"{v:.2%}" if f == FMT_PCT else f"{v:.1f}" if f == FMT_R2 else f"{v:,.0f}" if f == FMT_INT else f"{v:,.4f}"
    return _html.escape(str(v))

def html_table(df, cols, group_col=None):
    head = "".join(f"<th>{_html.escape(h)}</th>" for h, _ in cols)
    rows, last = [], None
    for rec in df.to_dict("records"):
        if group_col and rec[group_col] != last:
            last = rec[group_col]
            rows.append(f"<tr class='grp'><td colspan='{len(cols)}'>{_html.escape(str(last))}</td></tr>")
        rows.append("<tr>" + "".join(f"<td>{fmt_cell(k, rec.get(k))}</td>" for _, k in cols) + "</tr>")
    return f"<div class='card scroll'><table><tr>{head}</tr>{''.join(rows)}</table></div>"

SUB = f"{S.tag} | snapshot {S.snapshot_date} | mode {MODE} | built {_dt.datetime.now():%Y-%m-%d %H:%M}"
effr = EFFR.copy()
effr["effective_date"] = pd.to_datetime(effr["effective_date"])
direction = dict(zip(UPDOWN["name"], UPDOWN["direction"]))

# 1. fed funds cycles
f = go.Figure()
for rc in RATE_CYCLES.to_dict("records"):
    d = direction.get(rc["name"])
    if str(rc["short_code"]).upper().startswith("TTC"):
        f.add_vrect(x0=rc["start"], x1=rc["end"], line=dict(color="#1F4E79", dash="dot"), fillcolor="rgba(0,0,0,0)",
                    annotation_text=rc["name"], annotation_position="top left")
    else:
        f.add_vrect(x0=rc["start"], x1=rc["end"], line_width=0, opacity=0.18,
                    fillcolor={"UP": "#63BE7B", "DOWN": "#F8696B"}.get(d, "#BFBFBF"), annotation_text=rc["name"], annotation_position="top left")
for col, color in (("UP_FFR", "#1F4E79"), ("EFFR", "#c8532f")):
    if col in effr:
        f.add_trace(go.Scatter(x=effr.effective_date, y=effr[col], name=col, line=dict(color=color, width=2)))
f.update_layout(title="Fed funds and the study's rate cycles", yaxis_title="rate (%)", height=520, hovermode="x unified")
cyc_tbl = RATE_CYCLES.assign(direction=RATE_CYCLES["name"].map(direction).fillna(""))
FF = page(out("html", "Fed_Funds_Cycles", "html"), "Fed Funds and Rate Cycles", SUB,
          [f, html_table(cyc_tbl, [("Cycle", "name"), ("Code", "short_code"), ("Direction", "direction"), ("Start", "start"),
                                   ("End", "end"), ("Description", "description")])])

# 2. product dashboard
s = summary.sort_values("snapshot_balance", ascending=False)
kpi = lambda v, t: f"<div class='kpi'><b>{v}</b><span>{t}</span></div>"
kpis = ("<div class='kpis card' style='background:none;box-shadow:none;padding:0'>"
        + kpi(f"${s.snapshot_balance.sum() / 1e9:,.2f}B", "balance at snapshot") + kpi(f"{len(s)}", "final products")
        + kpi(f"{wavg(s, 'final_beta'):.1%}" if wavg(s, "final_beta") is not None else "n/a", "balance-weighted final beta")
        + kpi(f"{wavg(s, 'snapshot_war'):.2%}" if wavg(s, "snapshot_war") is not None else "n/a", "balance-weighted WAR")
        + kpi(f"{int((s.flag_list != '').sum())}", "products with flags") + "</div>")
tm = px.treemap(s.assign(beta=s.final_beta.fillna(0)), path=[px.Constant("All products"), "product_group", "mv_product"],
                values="snapshot_balance", color="beta", color_continuous_scale="Blues", title="Balance by product group and product (color = final beta)")
tm.update_layout(height=560)
bars = px.bar(s, x="mv_product", y="final_beta", color="product_group", hover_data=["final_method", "final_r2", "snapshot_balance"],
              title="Final beta by product")
bars.update_layout(height=520, xaxis_tickangle=-45, yaxis_tickformat=".0%")
sc = px.scatter(s, x="final_r2", y="final_beta", size=s.snapshot_balance.clip(lower=1), color="product_group", hover_name="mv_product",
                title="Final beta vs R2 (bubble = balance)")
sc.update_layout(height=500, yaxis_tickformat=".0%")
war = go.Figure()
prods = s.mv_product.tolist()
for i, p in enumerate(prods):
    d = pw[pw.mv_product == p].sort_values("eopdate")
    war.add_trace(go.Scatter(x=pd.to_datetime(d.eopdate), y=d.war * 100, name=f"{p} WAR", visible=(i == 0), line=dict(color="#1F4E79", width=2)))
war.add_trace(go.Scatter(x=effr.effective_date, y=effr[POLY_COL] if POLY_COL in effr else effr.iloc[:, 1], name=f"Fed funds ({POLY_COL})",
                         line=dict(color="#c8532f", dash="dot")))
buttons = [dict(label=p, method="update", args=[{"visible": [j == i for j in range(len(prods))] + [True]}, {"title": f"WAR vs fed funds: {p}"}])
           for i, p in enumerate(prods)]
war.update_layout(title=f"WAR vs fed funds: {prods[0] if prods else ''}", yaxis_title="rate (%)", height=520, hovermode="x unified",
                  updatemenus=[dict(buttons=buttons, direction="down", x=0, xanchor="left", y=1.15, yanchor="top")])
ud = go.Figure([go.Bar(x=s.mv_product, y=s.avg_up, name="Average up beta", marker_color="#63BE7B"),
                go.Bar(x=s.mv_product, y=s.avg_down, name="Average down beta", marker_color="#F8696B")])
ud.update_layout(barmode="group", title=f"Average up and down endpoint betas (lag {UD_LAG})", height=500, xaxis_tickangle=-45, yaxis_tickformat=".0%")
tbl = summary.sort_values(["product_group", "snapshot_balance"], ascending=[True, False])
PD = page(out("html", "Product_Dashboard", "html"), "Deposit Product Dashboard", SUB,
          [kpis, tm, bars, sc, war, ud, html_table(tbl, [(h, k) for h, k, _ in COLS if k != "product_group"], group_col="product_group")])

# 3. product mapping dashboard
m = pmap[pmap.balance > 0].copy()
sb = px.sunburst(m, path=["legacy_bank", "product_group", "mv_product", "subproductid"], values="balance", color="legacy_bank",
                 color_discrete_map={"PNFP": "#1F4E79", "SNV": "#c8532f"}, title="Bank, product group, product and subproduct by balance")
sb.update_layout(height=720)
mp = (pmap.groupby(["product_group", "mv_product", "legacy_bank"]).agg(subproducts=("subproductid", "nunique"),
          accounts=("accounts", "sum"), balance=("balance", "sum")).reset_index()
      .merge(lookup[["product_name", "product_code", "short_name"]], left_on="mv_product", right_on="product_name", how="left")
      .sort_values(["product_group", "mv_product", "legacy_bank"]))
sub_tbl = pmap.sort_values(["product_group", "mv_product", "legacy_bank", "balance"], ascending=[True, True, True, False])
MD = page(out("html", "Product_Mapping_Dashboard", "html"), "Deposit Product Mapping", SUB,
          [sb, html_table(mp, [("Product", "mv_product"), ("Code", "product_code"), ("Short Name", "short_name"), ("Bank", "legacy_bank"),
                               ("Subproducts", "subproducts"), ("Accounts", "accounts"), ("Balance", "balance")], group_col="product_group"),
           html_table(sub_tbl, [("Product", "mv_product"), ("Bank", "legacy_bank"), ("Subproduct", "subproductid"), ("Acct Type", "acct_typ_code"),
                                ("Description", "acct_typ_desc"), ("Current Product", "current_product"), ("Accounts", "accounts"),
                                ("Balance", "balance"), ("WAR", "war")], group_col="product_group")])
for p in (FF, PD, MD):
    print(f"Wrote {p.relative_to(MV_ROOT)}")
displayHTML(" | ".join(f'<a href="{folders.ws_link(p)}" target="_blank">{p.name}</a>' for p in (BETA_REPORT, FF, PD, MD)))