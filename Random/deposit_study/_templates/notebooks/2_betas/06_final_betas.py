# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 06 | Beta review, final betas and the MV report
# MAGIC
# MAGIC **What this builds.**
# MAGIC 1. `mv_beta_review`: every candidate per new product (linear and poly at each lag with R2, the best of each, their average, average up / down) and the recommended method, plus flags. Workbook `outputs/excel/beta_review_<tag>.xlsx` has one tab per product.
# MAGIC 2. `mv_final_betas`: the recommended method unless the `final_overrides` tab of `config/mv_study_config.xlsx` says otherwise. `override_method` is LM, POLY or AVG (with an optional `override_lag`) or MANUAL (with `manual_beta`), and `reason` is required.
# MAGIC 3. The MV package: `outputs/excel/mv_summary_<tag>.xlsx`, `outputs/reports/mv_report_<tag>.md` (known issues first) and `outputs/html/mv_dashboard_<tag>.html`.
# MAGIC
# MAGIC **Run after:** 05.

# COMMAND ----------

# DBTITLE 1,Install openpyxl and plotly
# MAGIC %pip install -q openpyxl plotly

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ../00_setup

# COMMAND ----------

# DBTITLE 1,Step 1 header
# MAGIC %md
# MAGIC ## Step 1. Beta review
# MAGIC The recommendation rule from `00_setup` (`recommend`) applied to each new product. Flags: `flag_thin_history` (fewer than `min_months_ttc` TTC months), `flag_low_balance` (below `min_balance`), `flag_unassigned` (holds subproducts with no new product), `flag_preview` (PREVIEW mode only).

# COMMAND ----------

# DBTITLE 1,1. mv_beta_review, review workbook, override rows
assert S.table_exists("mv_product_betas"), "run 05 first"
prod_betas = spark.table(S.table("mv_product_betas")).toPandas()
sel = spark.table(S.table("mv_product_selection")).toPandas()
for d in UD_COLS:                                  # rebuild the up / down column lists from the table
    for L in LAGS:
        UD_COLS[d][L] = [c for c in prod_betas.columns if re.fullmatch(fr"{d.lower()}_\w+_{L}", c)]

review = recommend(prod_betas.rename(columns={"mv_product": "EMP_NAME"})).rename(columns={"EMP_NAME": "mv_product"})
unassigned_products = set(sel.loc[sel.assignment_source == "UNASSIGNED", "mv_product"])
review["flag_thin_history"] = review["months_ttc"] < MIN_MONTHS
review["flag_low_balance"] = review["snapshot_balance"] < MIN_BAL
review["flag_preview"] = review.mv_product.isin(sel.loc[sel.assignment_source == "PREVIEW_CURRENT_PRODUCT", "mv_product"])
review["flag_unassigned"] = review.mv_product.isin(unassigned_products)
review = review.sort_values("snapshot_balance", ascending=False)
save_table(review, "mv_beta_review")

def candidates(r):
    rows = [dict(candidate=f"LM lag {L}", beta=r[f"lm_{L}"], r2=r[f"lm_r2_{L}"]) for L in LAGS]
    rows += [dict(candidate=f"POLY lag {L}", beta=r[f"poly_{L}"], r2=r[f"poly_r2_{L}"]) for L in LAGS]
    rows += [dict(candidate="AVG(best LM, best POLY)", beta=r["avg_lm_poly_beta"], r2=np.nanmean([r["best_lm_r2"], r["best_poly_r2"]]))]
    rows += [dict(candidate=f"Avg UP lag {L}", beta=r[f"avg_up_{L}"], r2=np.nan) for L in LAGS]
    rows += [dict(candidate=f"Avg DOWN lag {L}", beta=r[f"avg_down_{L}"], r2=np.nan) for L in LAGS]
    rows += [dict(candidate=f"{c} lag {L}", beta=r[c], r2=np.nan) for d in UD_COLS for L in LAGS for c in UD_COLS[d][L]]
    df = pd.DataFrame(rows)
    df["recommended"] = ""
    m = r["recommended_method"]
    for i, cand in df.candidate.items():
        if (m.startswith("AVG") and cand.startswith("AVG")) or cand.replace(" lag ", "_LAG") == m:
            df.loc[i, "recommended"] = "<-- recommended"
    return df

REVIEW_COLS = ["mv_product", "snapshot_balance", "months_ttc", "best_lm_lag", "best_lm_beta", "best_lm_r2", "best_poly_lag", "best_poly_beta",
               "best_poly_r2", "avg_lm_poly_beta", "avg_up", "avg_down", "recommended_method", "recommended_beta", "recommended_r2",
               "recommended_reason", "flag_thin_history", "flag_low_balance", "flag_unassigned", "flag_preview"]
sheets = {"All products": review[REVIEW_COLS]}
used = set()
for _, r in review.iterrows():
    sn = re.sub(r"[\[\]\:\*\?\/\\]", "", str(r.mv_product))[:28]
    while sn in used:
        sn = sn[:26] + f"{len(used) % 100:02d}"
    used.add(sn)
    sheets[sn] = candidates(r)
write_xlsx(out("excel", "beta_review", "xlsx"), sheets)

# one override row per product in config/mv_study_config.xlsx (your existing overrides are kept)
cfg = read_cfg()
OV_COLS = ["product", "recommended_method", "recommended_beta", "override_method", "override_lag", "manual_beta", "reason"]
ov_old = cfg["final_overrides"].reindex(columns=OV_COLS).fillna("")
ov = review[["mv_product", "recommended_method", "recommended_beta"]].rename(columns={"mv_product": "product"})
ov["recommended_beta"] = ov["recommended_beta"].round(2)
ov = ov.merge(ov_old[["product", "override_method", "override_lag", "manual_beta", "reason"]], on="product", how="left").fillna("")
ov = pd.concat([ov, ov_old[~ov_old["product"].isin(ov["product"]) & (ov_old["override_method"].str.strip() != "")]], ignore_index=True)

# ---- decisions (00_setup): derived = the rule with no override, source = the tab when current was first made, current = what is used ----
_info = review[["mv_product", "snapshot_balance", "months_ttc", "best_lm_beta", "best_lm_r2", "best_poly_beta", "best_poly_r2"]].rename(columns={"mv_product": "product"})
_picked = ov.merge(_info, on="product", how="left")
write_decision("derived", "betas", _picked.assign(override_method="", override_lag="", manual_beta="", reason=""))
if not dec_path("source", "betas").exists():
    write_decision("source", "betas", _picked)
_cur, CUR_INFO = resolve_current("betas", _picked)
ov = _cur
# the final_overrides tab now mirrors inputs/decisions/current/betas.csv (edit current in 00_read_and_recommend, not the tab)
cfg["final_overrides"] = ov[OV_COLS]
write_xlsx(CFG_PATH, {k: cfg[k] for k in CFG_TABS})
print(f"{len(review)} products reviewed ->", out("excel", "beta_review", "xlsx"))
display(sheets["All products"])

# COMMAND ----------

# DBTITLE 1,Step 2 header
# MAGIC %md
# MAGIC ## Step 2. Final betas (`mv_final_betas`)
# MAGIC The default is the rule (`final_source = RULE`). Any row in `inputs/decisions/current/betas.csv` (mirrored to the `final_overrides` tab) with an `override_method` replaces it (`final_source = OVERRIDE`). An override without a reason is flagged.

# COMMAND ----------

# DBTITLE 1,2. mv_final_betas and the summary workbook
ov = read_decision("current", "betas").set_index("product")       # inputs/decisions/current/betas.csv (the final_overrides tab mirrors it)
final = []
for _, r in review.iterrows():
    o = ov.loc[r.mv_product] if r.mv_product in ov.index else None
    om = str(o["override_method"]).strip().upper() if o is not None else ""
    lag = str(o["override_lag"]).strip() if o is not None else ""
    row = dict(mv_product=r.mv_product, snapshot_balance=r.snapshot_balance, snapshot_war=r.snapshot_war, months_ttc=r.months_ttc,
               recommended_method=r.recommended_method, recommended_beta=r.recommended_beta, recommended_r2=r.recommended_r2,
               avg_up=r.avg_up, avg_down=r.avg_down, override_method=om, override_reason=(str(o["reason"]).strip() if o is not None else ""))
    if om == "":
        fm, fb, fr, src = r.recommended_method, r.recommended_beta, r.recommended_r2, "RULE"
    elif om == "MANUAL":
        fm, fb, fr, src = "MANUAL", float(o["manual_beta"]), np.nan, "OVERRIDE"
    elif om in ("LM", "POLY", "AVG"):
        lL = int(lag) if lag else r.best_lm_lag
        pL = int(lag) if lag else r.best_poly_lag
        if om == "LM":
            fm, fb, fr = f"LM_LAG{int(lL)}", r[f"lm_{int(lL)}"], r[f"lm_r2_{int(lL)}"]
        elif om == "POLY":
            fm, fb, fr = f"POLY_LAG{int(pL)}", r[f"poly_{int(pL)}"], r[f"poly_r2_{int(pL)}"]
        else:
            fm = f"AVG(LM_LAG{int(lL)},POLY_LAG{int(pL)})"
            fb = (r[f"lm_{int(lL)}"] + r[f"poly_{int(pL)}"]) / 2
            fr = (r[f"lm_r2_{int(lL)}"] + r[f"poly_r2_{int(pL)}"]) / 2
        src = "OVERRIDE"
    else:
        raise ValueError(f"{r.mv_product}: override_method '{om}' must be LM, POLY, AVG or MANUAL")
    row.update(final_method=fm, final_beta=fb, final_r2=fr, final_source=src,
               flag_override_without_reason=(src == "OVERRIDE" and not row["override_reason"]),
               flag_thin_history=r.flag_thin_history, flag_low_balance=r.flag_low_balance, flag_preview=r.flag_preview,
               study=S.tag, snapshot_date=S.snapshot_date, ttc_cycle=TTC, flag_unassigned=r.flag_unassigned, mode=MODE)
    final.append(row)
final = pd.DataFrame(final)
save_table(final, "mv_final_betas")
final.to_csv(out("excel", "final_betas", "csv"), index=False)

acc_tot = sqlq("SELECT legacy_bank, COUNT(*) AS accounts, SUM(eopbook) AS balance, SUM(CASE WHEN NOT mapped THEN eopbook ELSE 0 END) AS unmapped_balance FROM {{mv_accounts}} GROUP BY 1 ORDER BY 1").toPandas()
bal = sel.snapshot_balance.astype(float)
assigned = sel.assignment_source == "ASSIGNED"
w = final.dropna(subset=["final_beta"])
wavg = float((w.final_beta * w.snapshot_balance).sum() / w.snapshot_balance.sum()) if w.snapshot_balance.sum() else np.nan
summary = pd.DataFrame([
    dict(item="MV study folder", value=MV_ROOT.name), dict(item="Source version", value=S.tag), dict(item="Parent", value=S.parent or "none"),
    dict(item="Snapshot date", value=S.snapshot_date), dict(item="Mode", value=MODE),
    dict(item="Mapping values", value=MAPPING_SOURCE), dict(item="Product assignment", value=ASSIGNMENT_SOURCE),
    dict(item="TTC cycle", value=f"{TTC} ({NAMED[TTC][0]:%Y-%m} to {NAMED[TTC][-1]:%Y-%m})"),
    dict(item="UP cycles", value=", ".join(UPDOWN.loc[UPDOWN.direction == "UP", "name"])),
    dict(item="DOWN cycles", value=", ".join(UPDOWN.loc[UPDOWN.direction == "DOWN", "name"])),
    dict(item="Recommended method rule", value=f"best-R2 linear lag vs best-R2 poly lag ({POLY_COL}); AVG when within {TOL:g} R2 pts"),
    dict(item="Accounts / balance", value=f"{int(acc_tot.accounts.sum()):,} / ${acc_tot.balance.sum():,.0f}"),
    dict(item="Unmapped balance", value=f"${acc_tot.unmapped_balance.sum():,.2f}"),
    dict(item="Subproducts assigned", value=f"{int(assigned.sum())} of {len(sel)} ({bal[assigned].sum() / bal.sum():.1%} of balance)"),
    dict(item="Products", value=len(final)), dict(item="Overrides", value=int((final.final_source == "OVERRIDE").sum())),
    dict(item="Balance-weighted final beta", value=f"{wavg:.3f}"),
])
issues_all = pd.read_csv(ISSUES_PATH, dtype=str, keep_default_na=False) if ISSUES_PATH.exists() else pd.DataFrame(columns=ISSUE_COLS)
write_xlsx(out("excel", "mv_summary", "xlsx"), {
    "Summary": summary, "Known issues": issues_all, "Final betas": final, "Beta review": sheets["All products"],
    "Product betas": prod_betas, "Product assignment": sel, "Accounts": acc_tot, "Settings": SETTINGS, "Cycles": CYC})
print(f"{len(final)} final betas ({int((final.final_source == 'OVERRIDE').sum())} overridden) ->", out("excel", "mv_summary", "xlsx"))
rows = []
if final.flag_override_without_reason.any():
    rows.append(("OVERRIDE_WITHOUT_REASON", "HIGH", ", ".join(final.loc[final.flag_override_without_reason, "mv_product"]), "add a reason in final_overrides", "", ""))
nob = final[final.final_beta.isna() & (final.snapshot_balance > 0)]
if len(nob):
    rows.append(("NO_FINAL_BETA", "HIGH", ", ".join(nob.mv_product), "no fit and no override", "", float(nob.snapshot_balance.sum())))
issues_final = log_issues("06 final betas", rows)
display(final)

# COMMAND ----------

# DBTITLE 1,Step 3 header
# MAGIC %md
# MAGIC ## Step 3. MV report and HTML dashboard
# MAGIC `outputs/reports/mv_report_<tag>.md` opens with every known issue (highest severity first), then explains how each answer was reached and shows the results. `outputs/html/mv_dashboard_<tag>.html` is a single self-contained file, so it can be shared without opening Databricks.

# COMMAND ----------

# DBTITLE 1,3. MV report (markdown)
from datetime import datetime

def md_table(df, cols=None, money=(), pct=(), dec=()):
    d = df[cols or list(df.columns)].copy()
    for c in d.columns:
        if c in money:
            d[c] = d[c].map(lambda v: "" if pd.isna(v) or v == "" else f"${float(v):,.0f}")
        elif c in pct:
            d[c] = d[c].map(lambda v: "" if pd.isna(v) else f"{float(v):.1%}")
        elif c in dec:
            d[c] = d[c].map(lambda v: "" if pd.isna(v) else f"{float(v):.3f}")
    lines = ["| " + " | ".join(d.columns) + " |", "|" + "---|" * len(d.columns)]
    lines += ["| " + " | ".join(str(v).replace("|", "/") for v in r) + " |" for r in d.itertuples(index=False)]
    return "\n".join(lines)

SEV = {"HIGH": 0, "MEDIUM": 1, "INFO": 2}
issues_all = pd.read_csv(ISSUES_PATH, dtype=str, keep_default_na=False)
issues_all = issues_all.sort_values(by=["severity", "step"], key=lambda s: s.map(SEV) if s.name == "severity" else s)
nb_list = [("01_account_mapping", "Source tables -> PNFP rate-class rule -> mapping keys -> reference / derived mapping values",
            f"inputs/account_mapping_{S.tag}.xlsx; tables mv_accounts, mv_account_war"),
           ("02_account_metrics", "Balances, WAR, rate distribution and history by subproduct", f"inputs/account_metrics_{S.tag}.xlsx; table mv_account_metrics"),
           ("03_account_betas", "TTC linear / poly by lag, up / down endpoint betas, recommended method by subproduct", "table mv_account_betas"),
           ("04_product_assignment", "Subproduct -> new product (reference / derived), consolidated names", f"inputs/product_assignment_{S.tag}.xlsx; table mv_product_selection"),
           ("05_product_betas", "Accounts -> new product, product WAR, product betas, engine check vs the version's 03", "tables mv_product_map, mv_product_war, mv_product_betas"),
           ("06_final_betas", "Recommended method per product, overrides, final betas", "tables mv_beta_review, mv_final_betas")]
md = [f"# MV report | {S.tag}", "",
      f"Generated {datetime.now():%Y-%m-%d %H:%M} from `{MV_ROOT.name}` (exported from study version {S.tag}, parent {S.parent or 'none'}). "
      f"Snapshot {S.snapshot_date}. Mode **{MODE}**; mapping values **{MAPPING_SOURCE}**; product assignment **{ASSIGNMENT_SOURCE}**.", "",
      "## 1. Known issues (read first)", "",
      md_table(issues_all, ["severity", "step", "code", "item", "detail", "accounts", "balance"], money=("balance",)) if len(issues_all) else "None.", "",
      "## 2. How the answers were derived", "",
      "Each notebook in `notebooks/` runs start to finish from the previous one's output. The input files are rebuilt from scratch on every run.", "",
      md_table(pd.DataFrame(nb_list, columns=["notebook", "what it does", "writes"])), "",
      "## 3. Summary", "", md_table(summary), "",
      "## 4. Accounts at the snapshot", "", md_table(acc_tot, money=("balance", "unmapped_balance")), "",
      "## 5. Product assignment", "",
      md_table(sel.assign(balance=bal).groupby(["assignment_source", "assigned_from"], as_index=False)
               .agg(subproducts=("sub_key", "count"), balance=("balance", "sum")), money=("balance",)), "",
      "## 6. Final betas", "",
      md_table(final, ["mv_product", "snapshot_balance", "months_ttc", "final_method", "final_beta", "final_r2", "avg_up", "avg_down",
                       "final_source", "flag_unassigned", "flag_thin_history", "flag_low_balance"],
               money=("snapshot_balance",), dec=("final_beta", "final_r2", "avg_up", "avg_down")), "",
      "## 7. Method settings", "", md_table(SETTINGS), ""]
out("reports", "mv_report", "md").write_text("\n".join(md), encoding="utf-8")
print("->", out("reports", "mv_report", "md"))
display(issues_all)

# COMMAND ----------

# DBTITLE 1,3. HTML dashboard
import html as _html
import plotly.graph_objects as go
import plotly.io as pio

fb = final.sort_values("snapshot_balance", ascending=False)
meth = fb.final_method.str.extract(r"^(\w+?)(?:_LAG|\(|$)")[0].fillna("NONE")
figs = []
f1 = go.Figure(go.Bar(x=fb.mv_product, y=fb.final_beta, marker_color=["#c8532f" if u else "#2f7fd6" for u in fb.flag_unassigned],
                      text=fb.final_method, hovertemplate="%{x}<br>beta %{y:.3f}<br>%{text}<extra></extra>"))
f1.update_layout(title="Final beta by product (orange = holds unassigned subproducts)", height=460, margin=dict(b=180))
figs.append(f1)
mb = pd.DataFrame({"method": meth, "balance": fb.snapshot_balance}).groupby("method", as_index=False).balance.sum()
figs.append(go.Figure(go.Pie(labels=mb.method, values=mb.balance, hole=0.45)).update_layout(title="Final method by balance", height=380))
top = fb.head(10).mv_product.tolist()
pw = spark.table(S.table("mv_product_war")).toPandas()
f3 = go.Figure()
for p in top:
    s = pw[pw.mv_product == p].sort_values("eopdate")
    f3.add_trace(go.Scatter(x=s.eopdate, y=s.war, name=p, mode="lines"))
f3.add_trace(go.Scatter(x=EFFR.effective_date, y=EFFR.UP_FFR, name="Fed funds (UP_FFR)", line=dict(color="black", dash="dot")))
f3.update_layout(title="Monthly WAR, 10 largest products, and fed funds", height=480)
figs.append(f3)
ud_df = fb[["mv_product", "avg_up", "avg_down"]]
f4 = go.Figure([go.Bar(x=ud_df.mv_product, y=ud_df.avg_up, name="Average up beta"), go.Bar(x=ud_df.mv_product, y=ud_df.avg_down, name="Average down beta")])
f4.update_layout(title=f"Average up / down endpoint beta (lag {UD_LAG})", barmode="group", height=460, margin=dict(b=180))
figs.append(f4)

def html_table(df, money=(), dec=()):
    d = df.copy()
    for c in money:
        d[c] = d[c].map(lambda v: "" if v == "" or pd.isna(v) else f"${float(v):,.0f}")
    for c in dec:
        d[c] = d[c].map(lambda v: "" if pd.isna(v) else f"{float(v):.3f}")
    return d.to_html(index=False, escape=True, border=0, classes="t")

cards = [("Accounts", f"{int(acc_tot.accounts.sum()):,}"), ("Balance", f"${acc_tot.balance.sum() / 1e9:,.2f}B"),
         ("Products", f"{len(final)}"), ("Assigned balance", f"{bal[assigned].sum() / bal.sum():.1%}"),
         ("Overrides", f"{int((final.final_source == 'OVERRIDE').sum())}"), ("Balance-weighted beta", f"{wavg:.3f}")]
body = [f"<h1>MV dashboard | {_html.escape(S.tag)}</h1>",
        f"<p class='sub'>Snapshot {S.snapshot_date} | mode {MODE} | mapping {MAPPING_SOURCE} | assignment {ASSIGNMENT_SOURCE} | "
        f"generated {datetime.now():%Y-%m-%d %H:%M}</p>",
        "<div class='cards'>" + "".join(f"<div class='c'><div class='l'>{a}</div><div class='v'>{b}</div></div>" for a, b in cards) + "</div>",
        "<h2>Known issues (read first)</h2>", html_table(issues_all[["severity", "step", "code", "item", "detail", "accounts", "balance"]], money=("balance",)) if len(issues_all) else "<p>None.</p>"]
body += [pio.to_html(f, full_html=False, include_plotlyjs=(i == 0)) for i, f in enumerate(figs)]
body += ["<h2>Final betas</h2>", html_table(final[["mv_product", "snapshot_balance", "months_ttc", "final_method", "final_beta", "final_r2",
                                                  "avg_up", "avg_down", "final_source", "flag_unassigned", "flag_thin_history"]],
                                           money=("snapshot_balance",), dec=("final_beta", "final_r2", "avg_up", "avg_down")),
         "<h2>Summary</h2>", html_table(summary)]
CSS = ("body{font-family:-apple-system,Segoe UI,Roboto,sans-serif;margin:24px;color:#1a1a2e;background:#f8f9fa}h1{margin:0}"
       ".sub{color:#64748b}.cards{display:flex;gap:10px;flex-wrap:wrap;margin:14px 0}.c{background:#fff;border:1px solid #e2e8f0;border-radius:10px;padding:12px 16px;min-width:150px}"
       ".l{font-size:11px;text-transform:uppercase;color:#64748b}.v{font-size:22px;font-weight:700}"
       "table.t{border-collapse:collapse;background:#fff;font-size:12px;margin-bottom:18px}table.t th,table.t td{border:1px solid #e2e8f0;padding:4px 8px;text-align:left}"
       "table.t th{background:#eef2f7}")
out("html", "mv_dashboard", "html").write_text(f"<!DOCTYPE html><html><head><meta charset='utf-8'><title>MV dashboard {S.tag}</title><style>{CSS}</style></head>"
                                               f"<body>{''.join(body)}</body></html>", encoding="utf-8")
print("->", out("html", "mv_dashboard", "html"))
displayHTML("".join(body[:4]))