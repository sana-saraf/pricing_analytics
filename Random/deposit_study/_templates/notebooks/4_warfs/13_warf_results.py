# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 13 | WARF results: by counterparty, product and product group, with and without FSE
# MAGIC
# MAGIC Reads `mv_warf_monthly` (built by 12). **WARF** = sum(runoff_amt) / sum(warf_balance). Every view is shown three ways:
# MAGIC
# MAGIC | Column | Meaning |
# MAGIC |---|---|
# MAGIC | `warf_all` | all balances |
# MAGIC | `warf_ex_fse` | without FSE balances (`fse_flag = 'N'`) |
# MAGIC | `warf_fse` | FSE balances only |
# MAGIC
# MAGIC Latest month and snapshot month tables, monthly history and charts.
# MAGIC
# MAGIC **Outputs:**
# MAGIC * `outputs/excel/warf_<tag>.xlsx` and `outputs/html/warf_<tag>.html`
# MAGIC * a copy of both in `outputs/Deposit Study Artifacts/Detailed Reports/WARFs/`
# MAGIC * `outputs/excel/warf_summary_<tag>.csv`, the by-product table the Final Report's WARFs tab reads
# MAGIC
# MAGIC **Run after:** 12_warf_prep.

# COMMAND ----------

# DBTITLE 1,Install openpyxl
# MAGIC %pip install openpyxl --quiet

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ../00_setup

# COMMAND ----------

# DBTITLE 1,1. Load mv_warf_monthly
import numpy as np, io, base64, html as _html, shutil
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

assert S.table_exists("mv_warf_monthly"), "run 12_warf_prep first"
wm = spark.table(S.table("mv_warf_monthly")).toPandas()
wm["month"] = pd.to_datetime(wm["month"])
assert "op_deposit_balance" in wm, "mv_warf_monthly is from an older 12: re-run 12_warf_prep"
for c in ["accounts", "ledger_balance", "warf_balance", "runoff_amt", "war_amt", "op_deposit_balance"]:
    wm[c] = pd.to_numeric(wm[c], errors="coerce").fillna(0.0)
wm["fse"] = np.where(wm.fse_flag.astype(str).str.upper() == "Y", "FSE", "Non-FSE")

# month settings (config/study_params.json -> warf)
WCFG = S.params.get("warf", {}) or {}
_ym = lambda s: [x.strip() for x in str(s or "").split(",") if x.strip()]
EXCL_M = set(_ym(WCFG.get("exclude_months")))
OPS_ONLY = str(WCFG.get("ops_months_only", "Y")).strip().upper().startswith("Y")
_ops = wm.pivot_table(index="month", columns="legacy_bank", values="op_deposit_balance", aggfunc="sum").fillna(0)
OPS_MONTHS = set(_ops.index[(_ops > 0).all(axis=1)])
ALL_MONTHS = sorted(pd.to_datetime(wm.month.unique()))
KEPT = [m for m in ALL_MONTHS if f"{m:%Y-%m}" not in EXCL_M and (not OPS_ONLY or m in OPS_MONTHS)]
DROPPED = {f"{m:%Y-%m}": ("exclude_months" if f"{m:%Y-%m}" in EXCL_M else "no operational deposits loaded for a bank")
           for m in ALL_MONTHS if m not in KEPT}
assert KEPT, "no months left: check warf.exclude_months and warf.ops_months_only in config/study_params.json"
wm = wm[wm.month.isin(KEPT)].copy()
RM = (WCFG.get("report_month") or "").strip()
LATEST = (pd.Timestamp(RM + "-01" if len(RM) == 7 else RM) + pd.offsets.MonthEnd(0)) if RM else wm.month.max()
assert LATEST in set(wm.month), f"warf.report_month {RM} is not among the months kept: {[f'{m:%Y-%m}' for m in KEPT]}"
if DROPPED:
    alert("info", f"{len(DROPPED)} months left out (ops_months_only = {'Y' if OPS_ONLY else 'N'})", [f"{k}: {v}" for k, v in DROPPED.items()])
SNAP = pd.Timestamp(S.snapshot_date) + pd.offsets.MonthEnd(0)
SNAP = SNAP if SNAP in set(wm.month) else LATEST
print(f"{len(wm):,} rows | months kept {wm.month.min():%Y-%m} to {wm.month.max():%Y-%m} ({wm.month.nunique()}) | "
      f"report month {LATEST:%Y-%m} | snapshot month {SNAP:%Y-%m}")

# COMMAND ----------

# DBTITLE 1,2. WARF helpers
def _w(num, den):
    return np.where(den > 0, num / den.where(den > 0, 1), np.nan)

def warf_table(df, by):
    """Balance, WARF all / ex-FSE / FSE-only and WAR for each group in `by`."""
    by = [by] if isinstance(by, str) else list(by)
    d = df.assign(fse_bal=np.where(df.fse == "FSE", df.ledger_balance, 0.0),
                  nf_wb=np.where(df.fse == "Non-FSE", df.warf_balance, 0.0), nf_ro=np.where(df.fse == "Non-FSE", df.runoff_amt, 0.0),
                  f_wb=np.where(df.fse == "FSE", df.warf_balance, 0.0), f_ro=np.where(df.fse == "FSE", df.runoff_amt, 0.0))
    g = d.groupby(by, dropna=False).agg(accounts=("accounts", "sum"), balance=("ledger_balance", "sum"), fse_balance=("fse_bal", "sum"),
                                        warf_balance=("warf_balance", "sum"), runoff=("runoff_amt", "sum"), war_amt=("war_amt", "sum"),
                                        nf_wb=("nf_wb", "sum"), nf_ro=("nf_ro", "sum"), f_wb=("f_wb", "sum"), f_ro=("f_ro", "sum")).reset_index()
    g["fse_share"] = _w(g.fse_balance, g.balance)
    g["warf_all"] = _w(g.runoff, g.warf_balance)
    g["warf_ex_fse"] = _w(g.nf_ro, g.nf_wb)
    g["warf_fse"] = _w(g.f_ro, g.f_wb)
    g["war"] = _w(g.war_amt, g.balance)
    return g.drop(columns=["nf_wb", "nf_ro", "f_wb", "f_ro", "war_amt"])

def at(df, month):
    return df[df.month == month]

VIEWS = {"bank": ["legacy_bank"], "counterparty": ["counterparty_type"], "product": ["mv_product"], "product_group": ["product_group"],
         "lcr_rollup": ["lcr_rollup_1"]}

# COMMAND ----------

# DBTITLE 1,3. Latest and snapshot month tables
TABLES = {}
for name, by in VIEWS.items():
    t = pd.concat([warf_table(at(wm, LATEST), by).assign(month=f"{LATEST:%Y-%m}", basis="latest"),
                   warf_table(at(wm, SNAP), by).assign(month=f"{SNAP:%Y-%m}", basis="snapshot")], ignore_index=True)
    TABLES[name] = t.sort_values(["basis", "balance"], ascending=[True, False], ignore_index=True)

tot = pd.concat([warf_table(at(wm, m).assign(k="Total"), "k").assign(month=f"{m:%Y-%m}") for m in sorted({LATEST, SNAP})])
bybank = pd.concat([warf_table(at(wm, m), "legacy_bank").rename(columns={"legacy_bank": "k"}).assign(month=f"{m:%Y-%m}") for m in sorted({LATEST, SNAP})])
summary = pd.concat([bybank, tot], ignore_index=True).rename(columns={"k": "bank"})
print(f"Latest month {LATEST:%Y-%m}, snapshot month {SNAP:%Y-%m}")
display(summary[["month", "bank", "balance", "fse_balance", "fse_share", "warf_all", "warf_ex_fse", "warf_fse", "war"]])
display(TABLES["counterparty"][TABLES["counterparty"].basis == "latest"])

# COMMAND ----------

# DBTITLE 1,4. Monthly history
HIST = {}
for name, by in VIEWS.items():
    HIST[name] = warf_table(wm, ["month"] + by).sort_values(["month"] + by, ignore_index=True)
HIST["total"] = warf_table(wm.assign(k="Total"), ["month", "k"]).drop(columns="k")

# months where the source's LCR rollup mix shifts (a large month-on-month WARF move): flagged in every output
h = HIST["total"].set_index("month")
jump = h.warf_all.diff().abs()
SHIFT_MONTHS = [m for m in jump.index if jump[m] > 0.02]
if SHIFT_MONTHS:
    alert("warn", "WARF moves by more than 2 points month on month",
          [f"{m:%Y-%m}: {h.warf_all.shift(1)[m]:.1%} -> {h.warf_all[m]:.1%}" for m in SHIFT_MONTHS] +
          ["Check the LCR rollup mix in the source (By LCR rollup tab): these are source classification changes, not study mapping"])
display(HIST["total"][["month", "balance", "fse_balance", "warf_all", "warf_ex_fse", "warf_fse"]])

# COMMAND ----------

# DBTITLE 1,5. Charts
CHARTS = {}
def _png(fig):
    b = io.BytesIO(); fig.savefig(b, format="png", dpi=110, bbox_inches="tight"); plt.close(fig)
    return base64.b64encode(b.getvalue()).decode()

def _pct(ax):
    ax.yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1.0, decimals=0)); ax.grid(alpha=0.3)
    for m in SHIFT_MONTHS:
        ax.axvline(m, color="#c62828", lw=0.6, ls=":")

# 1. total and each bank, all vs without FSE
fig, ax = plt.subplots(figsize=(10, 4))
t = HIST["total"]; ax.plot(t.month, t.warf_all, lw=2.2, color="black", label="Total, all"); ax.plot(t.month, t.warf_ex_fse, lw=2.2, ls="--", color="black", label="Total, without FSE")
for bank, col in (("PNFP", "#1f77b4"), ("SNV", "#ff7f0e")):
    b = HIST["bank"][HIST["bank"].legacy_bank == bank]
    ax.plot(b.month, b.warf_all, color=col, label=f"{bank}, all"); ax.plot(b.month, b.warf_ex_fse, color=col, ls="--", label=f"{bank}, without FSE")
_pct(ax); ax.set_title("WARF by bank, with and without FSE"); ax.legend(ncol=3, fontsize=8)
CHARTS["bank"] = _png(fig)

# 2. balance: FSE vs non-FSE
fse_b = wm.groupby(["month", "fse"]).ledger_balance.sum().unstack().fillna(0) / 1e9
fig, ax = plt.subplots(figsize=(10, 3.5))
ax.stackplot(fse_b.index, [fse_b.get("Non-FSE", 0), fse_b.get("FSE", 0)], labels=["Non-FSE", "FSE"], colors=["#9ecae1", "#de2d26"], alpha=0.85)
ax.set_ylabel("$ billions"); ax.grid(alpha=0.3); ax.legend(loc="upper left"); ax.set_title("Balance: FSE and non-FSE")
CHARTS["fse_balance"] = _png(fig)

# 3-5. history by counterparty, product group and the largest products (all and without FSE)
def _lines(name, col, title, top=None):
    d = HIST[name]
    keep = d[d.month == LATEST].nlargest(top or 99, "balance")[col].tolist()
    fig, axes = plt.subplots(1, 2, figsize=(14, 4.2), sharey=True)
    for ax, metric, lab in ((axes[0], "warf_all", "all balances"), (axes[1], "warf_ex_fse", "without FSE")):
        for k in keep:
            s = d[d[col] == k]
            ax.plot(s.month, s[metric], label=str(k), lw=1.4)
        _pct(ax); ax.set_title(f"{title}: {lab}")
    axes[1].legend(fontsize=7, bbox_to_anchor=(1.01, 1), loc="upper left")
    return _png(fig)

CHARTS["counterparty"] = _lines("counterparty", "counterparty_type", "WARF by counterparty")
CHARTS["product_group"] = _lines("product_group", "product_group", "WARF by product group")
CHARTS["product"] = _lines("product", "mv_product", "WARF, 10 largest products", top=10)
displayHTML("".join(f'<h4>{k}</h4><img src="data:image/png;base64,{v}" style="max-width:100%">' for k, v in CHARTS.items()))

# COMMAND ----------

# DBTITLE 1,6. Excel and HTML outputs
def _wide(name, col, metric):
    d = HIST[name].pivot_table(index=col, columns="month", values=metric, aggfunc="sum")
    d.columns = [f"{c:%Y-%m}" for c in d.columns]
    return d.reset_index()

cov = wm.groupby(["month", "legacy_bank", "mapping_basis"], as_index=False).agg(accounts=("accounts", "sum"), balance=("ledger_balance", "sum"))
cov["month"] = cov.month.dt.strftime("%Y-%m")
settings = pd.DataFrame([("source", S.params.get("warf", {}).get("source_table", "finance.staging.depositsplusLCR")),
                         ("rows", "eobm_flag = 'Y' (month-end)"), ("start", f"{wm.month.min():%Y-%m}"), ("latest month", f"{LATEST:%Y-%m}"),
                         ("snapshot month", f"{SNAP:%Y-%m}"), ("WARF", "sum(runoff_amt) / sum(warf_balance)"), ("FSE", "fse_flag = 'Y'"),
                         ("source shift months", ", ".join(f"{m:%Y-%m}" for m in SHIFT_MONTHS) or "none"),
                         ("report_month", WCFG.get("report_month") or f"blank = newest kept ({LATEST:%Y-%m})"),
                         ("exclude_months", WCFG.get("exclude_months") or "none"), ("ops_months_only", "Y" if OPS_ONLY else "N"),
                         ("months kept", ", ".join(f"{m:%Y-%m}" for m in KEPT)),
                         ("months left out", "; ".join(f"{k} ({v})" for k, v in DROPPED.items()) or "none")], columns=["setting", "value"])
hist_long = HIST["total"].assign(view="Total", item="Total")
for name, by in VIEWS.items():
    hist_long = pd.concat([hist_long, HIST[name].rename(columns={by[0]: "item"}).assign(view=name)], ignore_index=True)
hist_long["month"] = pd.to_datetime(hist_long.month).dt.strftime("%Y-%m")

XLSX = out("excel", "warf", "xlsx")
write_xlsx(XLSX, {
    "Summary": summary, "By counterparty": TABLES["counterparty"], "By product": TABLES["product"], "By product group": TABLES["product_group"],
    "By LCR rollup": TABLES["lcr_rollup"], "Hist WARF all by product": _wide("product", "mv_product", "warf_all"),
    "Hist WARF exFSE by product": _wide("product", "mv_product", "warf_ex_fse"),
    "Hist WARF by counterparty": _wide("counterparty", "counterparty_type", "warf_all"),
    "Hist WARF by product group": _wide("product_group", "product_group", "warf_all"),
    "Hist LCR rollup balance": _wide("lcr_rollup", "lcr_rollup_1", "warf_balance"),
    "Monthly (long)": hist_long, "Coverage": cov, "Settings": settings})

# the by-product table the Final Report reads (latest month)
TABLES["product"][TABLES["product"].basis == "latest"].to_csv(out("excel", "warf_summary", "csv"), index=False)

def _tbl(df, pct=("fse_share", "warf_all", "warf_ex_fse", "warf_fse", "war")):
    f = {c: (lambda v: "" if pd.isna(v) else f"{v:.2%}") for c in pct if c in df}
    f.update({c: (lambda v: f"${v:,.0f}") for c in ("balance", "fse_balance", "warf_balance", "runoff") if c in df})
    return df.to_html(index=False, formatters=f, border=0, classes="t")
lat = lambda k: TABLES[k][TABLES[k].basis == "latest"].drop(columns=["basis"])
shift_note = (f'<p style="color:#c62828"><b>Source shift:</b> WARF moves more than 2 points in {", ".join(f"{m:%Y-%m}" for m in SHIFT_MONTHS)} '
              "(red dotted lines). That comes from the LCR rollup mix in the source, not from the study mapping.</p>") if SHIFT_MONTHS else ""
HTML = f"""<html><head><meta charset="utf-8"><title>WARF {S.tag}</title><style>body{{font-family:Segoe UI,Arial;margin:24px}}
.t{{border-collapse:collapse;font-size:12px}}.t td,.t th{{padding:3px 8px;border-bottom:1px solid #ddd;text-align:right}}.t th{{background:#1F4E79;color:#fff}}</style></head><body>
<h2>{S.tag} Deposit Study | WARF | latest {LATEST:%Y-%m} | snapshot {SNAP:%Y-%m}</h2>
<p>WARF = runoff / WARF balance. Shown for all balances, without FSE (fse_flag = N) and FSE only.</p>{shift_note}
<h3>Summary</h3>{_tbl(summary)}
<img src="data:image/png;base64,{CHARTS['bank']}"><br><img src="data:image/png;base64,{CHARTS['fse_balance']}">
<h3>By counterparty ({LATEST:%Y-%m})</h3>{_tbl(lat('counterparty'))}<img src="data:image/png;base64,{CHARTS['counterparty']}">
<h3>By product group ({LATEST:%Y-%m})</h3>{_tbl(lat('product_group'))}<img src="data:image/png;base64,{CHARTS['product_group']}">
<h3>By product ({LATEST:%Y-%m})</h3>{_tbl(lat('product'))}<img src="data:image/png;base64,{CHARTS['product']}">
</body></html>"""
HTML_PATH = out("html", "warf", "html")
HTML_PATH.write_text(HTML, encoding="utf-8")

DET = OUTPUTS / "Deposit Study Artifacts" / "Detailed Reports" / "WARFs"
DET.mkdir(parents=True, exist_ok=True)
for p in (XLSX, HTML_PATH):
    shutil.copy2(p, DET / p.name)
print("Wrote:", XLSX, HTML_PATH, out("excel", "warf_summary", "csv"), f"(copies in {DET})", sep="\n  ")

# COMMAND ----------

