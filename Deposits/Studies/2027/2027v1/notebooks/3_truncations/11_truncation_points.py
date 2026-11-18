# Databricks notebook source
# DBTITLE 1,Truncation 2 of 2: decay and truncation points
# MAGIC %md
# MAGIC # Truncation analysis, part 2 of 2: decay and truncation points (study version)
# MAGIC
# MAGIC Reads `inputs/vintages/truncation_cohort_<tag>.csv` from 10 and answers: **how many months until only 30%, 25% or 20% of the
# MAGIC month 0 accounts are left**, by final study product (`mv_product`), product group and bank.
# MAGIC
# MAGIC 1. Add up accounts by group and `N_Month`. 2. Divide by month 0 (percent of baseline). 3. Fit `percent = intercept + slope * N_Month`.
# MAGIC 4. Solve `month = (target - intercept) / slope`. `R_squared` = how straight the decay is; low means the month is rough.
# MAGIC
# MAGIC `max_n_month` blank = the shorter bank's last month, so one bank running out of history is not read as decay.
# MAGIC Outputs: `outputs/excel/truncation_<tag>.xlsx`, `outputs/html/truncation_<tag>.html`, and a copy of both in
# MAGIC `outputs/Deposit Study Artifacts/Detailed Reports/`.

# COMMAND ----------

# DBTITLE 1,Install openpyxl
# MAGIC %pip install openpyxl --quiet

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ../00_setup

# COMMAND ----------

# DBTITLE 1,Settings and the cohort
import numpy as np, io, base64, html as _html, shutil
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

TR = {k: v for k, v in (S.params.get("truncation") or {}).items() if not str(k).startswith("_")}
THRESHOLDS = [int(x) for x in str(TR.get("thresholds", "30,25,20")).split(",") if x.strip()]
LEVELS = [x.strip() for x in str(TR.get("levels", "mv_product,product_group,legacy_bank")).split(",") if x.strip()]
COHORT_CSV = INPUTS / "vintages" / f"{out_name('truncation_cohort')}.csv"
assert COHORT_CSV.exists(), f"{COHORT_CSV} not found: run 10_truncation_cohorts first"
coh = pd.read_csv(COHORT_CSV, dtype={"subproductid": str})
mapped = coh[coh.map_source != "UNMAPPED"]

coverage = coh.groupby("legacy_bank", as_index=False).agg(first_n_month=("N_Month", "min"), last_n_month=("N_Month", "max"))
shortest = int(coverage.last_n_month.min())
MAX_N = int(TR.get("max_n_month") or shortest)
if MAX_N > shortest:
    print(f"WARNING: max_n_month {MAX_N} is past the shorter bank's last month {shortest}; the combined decay will look steeper")
display(coverage)
print(f"levels {LEVELS} | N_Month 0 to {MAX_N} | thresholds {THRESHOLDS} | "
      f"unmapped month 0 accounts left out: {int(coh[(coh.N_Month == 0) & (coh.map_source == 'UNMAPPED')].acct_cnt.sum()):,}")

# COMMAND ----------

# DBTITLE 1,Functions: percent of baseline, straight-line fit, months to threshold
def pct_of_baseline(df, level):
    """Accounts at each N_Month as a percent of N_Month 0. Returns (wide, long)."""
    pivot = df.pivot_table(index=level, columns="N_Month", values="acct_cnt", aggfunc="sum")
    assert 0 in pivot.columns, "no rows at N_Month 0: check the baselines in config/study_params.json"
    pivot = pivot[pivot[0] > 0]
    pct = (pivot.div(pivot[0], axis=0) * 100).sort_index()
    long = pct.reset_index().melt(id_vars=level, var_name="N_Month", value_name="pct").dropna(subset=["pct"])
    long["N_Month"] = long["N_Month"].astype(int)
    return pct, long

def fit_line(sub):
    x, y = sub["N_Month"].to_numpy(float), sub["pct"].to_numpy(float)
    if len(x) < 2:
        return None
    slope, intercept = np.polyfit(x, y, 1)
    ss_res, ss_tot = ((y - (intercept + slope * x)) ** 2).sum(), ((y - y.mean()) ** 2).sum()
    return intercept, slope, (1 - ss_res / ss_tot) if ss_tot else np.nan

def decay_summary(df, level):
    pct, long = pct_of_baseline(df, level)
    base = df[df.N_Month == 0].groupby(level).agg(Baseline_Acct_Cnt=("acct_cnt", "sum"), Baseline_Balance=("sum_balance", "sum"))
    last = long.sort_values("N_Month").groupby(level).tail(1).set_index(level)
    rows = []
    for key, sub in long.groupby(level):
        f = fit_line(sub)
        if f is None:
            continue
        a, b, r2 = f
        r = {level: key, "Intercept": round(a, 4), "Slope": round(b, 4), "R_squared": round(r2, 4),
             "Last_N_Month": int(last.at[key, "N_Month"]), "Pct_left_at_last": round(float(last.at[key, "pct"]), 2)}
        for t in THRESHOLDS:
            m = round((t - a) / b) if b < 0 else None
            r[f"Month y={t}%"] = m if m and m > 0 else None
        rows.append(r)
    out = pd.DataFrame(rows).merge(base.reset_index(), on=level, how="left")
    cols = [level, "Baseline_Acct_Cnt", "Baseline_Balance", "Intercept", "Slope", "R_squared", "Last_N_Month", "Pct_left_at_last"] + \
           [f"Month y={t}%" for t in THRESHOLDS]
    return out[cols].sort_values("Baseline_Acct_Cnt", ascending=False, ignore_index=True), pct, long

# check on a case with a known answer: 100% falling 1 point a month -> 30% at month 70
_t = pd.DataFrame({"g": "x", "N_Month": range(0, 50), "acct_cnt": [1000 - 10 * i for i in range(50)], "sum_balance": 1.0})
_r = decay_summary(_t, "g")[0].iloc[0]
assert _r["Month y=30%"] == 70 and abs(_r.Slope + 1) < 1e-9, _r

# COMMAND ----------

# DBTITLE 1,Run each grouping
use = mapped[mapped.N_Month <= MAX_N]
_missing = [lv for lv in LEVELS if lv not in use.columns]
if _missing:
    print(f"WARNING: skipping levels not in cohort: {_missing}")
_levels = [lv for lv in LEVELS if lv in use.columns]
results, wide, longs = {}, {}, {}
for level in _levels:
    d = use[use[level].notna()]
    results[level], wide[level], longs[level] = decay_summary(d, level)
    print(f"{level}: {len(results[level])} groups")
for level in _levels:
    print(level); display(results[level])

# COMMAND ----------

# DBTITLE 1,Excel and HTML outputs
XLSX = OUTPUTS / "excel" / f"{out_name('truncation')}.xlsx"
HTML = OUTPUTS / "html" / f"{out_name('truncation')}.html"
settings = pd.DataFrame([("study", S.tag), ("cohort file", str(COHORT_CSV)), ("max_n_month", MAX_N),
                         ("thresholds", ",".join(map(str, THRESHOLDS)))] + [(k, v) for k, v in TR.items()], columns=["setting", "value"])
unmapped = (coh[(coh.N_Month == 0) & (coh.map_source == "UNMAPPED")].groupby(["legacy_bank", "subproductid"], as_index=False)
              .agg(month_0_accounts=("acct_cnt", "sum"), month_0_balance=("sum_balance", "sum"))
              .sort_values("month_0_accounts", ascending=False))
sheets = {f"By {lv}"[:31]: results[lv] for lv in _levels}
sheets.update({f"Pct {lv}"[:31]: wide[lv].round(2).reset_index() for lv in _levels})
sheets.update({"Coverage": coverage, "Unmapped": unmapped, "Settings": settings,
               "Cohort": coh.groupby(["legacy_bank", "N_Month", "eopdate", "mv_product"], dropna=False, as_index=False)[["acct_cnt", "sum_balance"]].sum()})
write_xlsx(XLSX, sheets)

def _png(level, top=9):
    res, long = results[level], longs[level]
    keys = list(res[level].head(top))
    cols = 3; rows = -(-len(keys) // cols)
    fig, axes = plt.subplots(rows, cols, figsize=(15, 3.6 * rows), squeeze=False)
    for ax, key in zip(axes.flat, keys):
        sub, r = long[long[level] == key], res[res[level] == key].iloc[0]
        ax.scatter(sub.N_Month, sub.pct, s=8)
        x = np.array([0, MAX_N]); ax.plot(x, r.Intercept + r.Slope * x, color="#c00000")
        for t in THRESHOLDS:
            ax.axhline(t, ls=":", color="grey", lw=0.8)
        ax.set_title(f"{key}\nR2 {r.R_squared:.2f} | 30% at {r.get('Month y=30%')}", fontsize=9); ax.set_ylim(0, 110)
    for ax in list(axes.flat)[len(keys):]:
        ax.axis("off")
    fig.tight_layout(); buf = io.BytesIO(); fig.savefig(buf, format="png", dpi=90); plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode()

parts = [f"<h1>{S.tag} truncation analysis</h1><p>Months until 30/25/20% of month 0 accounts remain. "
         f"N_Month 0 to {MAX_N}. PNFP month 0 {TR.get('pnfp_baseline')}, SNV month 0 {TR.get('snv_baseline')} "
         f"(vintages {TR.get('vintage_years')}). Products from this version's mapping.</p>", coverage.to_html(index=False)]
for lv in _levels:
    parts += [f"<h2>By {lv}</h2>", results[lv].to_html(index=False, na_rep="", float_format=lambda v: f"{v:,.2f}"),
              f'<img style="max-width:100%" src="data:image/png;base64,{_png(lv)}">']
parts += ["<h2>Unmapped subproducts at month 0 (left out)</h2>", unmapped.to_html(index=False, float_format=lambda v: f"{v:,.0f}")]
HTML.write_text("<html><head><meta charset='utf-8'><style>body{font-family:Segoe UI,Arial;margin:24px}table{border-collapse:collapse;font-size:12px}"
                "td,th{border:1px solid #ccc;padding:3px 6px}th{background:#1f4e79;color:#fff}</style></head><body>"
                + "".join(parts) + "</body></html>", encoding="utf-8")

DETAIL = OUTPUTS / "Deposit Study Artifacts" / "Detailed Reports"
DETAIL.mkdir(parents=True, exist_ok=True)
for f in (XLSX, HTML):
    shutil.copy2(f, DETAIL / f.name)
print(f"wrote {XLSX}\nwrote {HTML}\ncopied both to {DETAIL}")

# COMMAND ----------

