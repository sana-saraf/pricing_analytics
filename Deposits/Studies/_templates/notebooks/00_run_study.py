# Databricks notebook source
# DBTITLE 1,00 | Run Study — Control Panel
# MAGIC %md
# MAGIC # 00 | Run Study — Control Panel
# MAGIC The **study dashboard**. When you open this notebook you see the source data lineage summary and choose what to run.
# MAGIC
# MAGIC ## Flow
# MAGIC 1. `999_source_lineage` runs first (automatically) — checks every source table and writes a summary
# MAGIC 2. The lineage view displays below: rows, balances, accounts, date ranges
# MAGIC 3. Pick your run action and output scope, then execute
# MAGIC
# MAGIC ## Run Actions
# MAGIC | Action | What it does |
# MAGIC |---|---|
# MAGIC | **Full study** | Runs all notebooks 01 → 09 in order |
# MAGIC | **Read-and-recommend** | Runs 01, then opens `00_read_and_recommend` for interactive decisions at each stage |
# MAGIC | **Select notebooks** | Pick specific notebooks or a profile (betas, truncation, warf, reports) |
# MAGIC | **Outputs only** | Re-generate outputs without re-running the model (07, 08, 09) |
# MAGIC
# MAGIC ## Output Scope
# MAGIC | Scope | Includes |
# MAGIC |---|---|
# MAGIC | **All** | Everything below |
# MAGIC | **Final** | `09_deposit_study_package` (the deliverable) |
# MAGIC | **Detailed** | Beta report, product dashboards, MV summary |
# MAGIC | **Excel** | All `.xlsx` workbooks |
# MAGIC | **HTML** | All `.html` dashboards |

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ./00_setup

# COMMAND ----------

# DBTITLE 1,Run 999_source_lineage and display lineage summary
import json as _json

# ---- Run 999_source_lineage silently ----
_lin_nb = str(STUDY_ROOT / "notebooks" / "999_source_lineage").replace("/Workspace", "", 1)
try:
    dbutils.notebook.run(_lin_nb, timeout_seconds=600)
    print("999_source_lineage completed.")
except Exception as e:
    print(f"999_source_lineage skipped: {str(e)[:200]}")

# ---- Display the lineage summary ----
_lin_path = OUTPUTS / "reports" / "source_lineage_summary.json"
if _lin_path.exists():
    _lin = _json.loads(_lin_path.read_text(encoding="utf-8"))
    _rows = []
    for s in _lin.get("sources", []):
        _rows.append(f"""<tr>
            <td style='padding:4px 8px;font-weight:bold'>{s['source']}</td>
            <td style='padding:4px 8px;font-family:monospace;font-size:11px'>{s['table']}</td>
            <td style='padding:4px 8px;text-align:center'>{'\u2705' if s['status'] == 'ok' else '\u274c ' + s['status']}</td>
            <td style='padding:4px 8px;text-align:right'>{s.get('rows', 0):,}</td>
            <td style='padding:4px 8px;text-align:right'>{s.get('accounts', 0):,}</td>
            <td style='padding:4px 8px;text-align:right'>{'${:,.0f}'.format(s['total_balance']) if s.get('total_balance') else ''}</td>
            <td style='padding:4px 8px;text-align:right'>{s.get('months', '')}</td>
            <td style='padding:4px 8px'>{s.get('min_date', '')} \u2192 {s.get('max_date', '')}</td>
        </tr>""")
    _html = f"""<div style='font-family:Arial;margin:10px 0'>
    <h3 style='color:#1F4E79;margin:4px 0'>Source Data Lineage \u2014 {_lin.get('study', '')} (as-of {_lin.get('asofdate', '')})</h3>
    <p style='font-size:11px;color:#666'>Mode: <b>{_lin.get('source_tables_mode', 'production')}</b> | Generated: {_lin.get('generated', '')}</p>
    <table style='border-collapse:collapse;font-size:12px'>
    <tr style='background:#1F4E79;color:white'>
        <th style='padding:4px 8px'>Source</th><th style='padding:4px 8px'>Table</th><th style='padding:4px 8px'>Status</th>
        <th style='padding:4px 8px'>Rows</th><th style='padding:4px 8px'>Accounts</th><th style='padding:4px 8px'>Balance</th>
        <th style='padding:4px 8px'>Months</th><th style='padding:4px 8px'>Date Range</th>
    </tr>
    {''.join(_rows)}
    </table></div>"""
    displayHTML(_html)
else:
    print("No lineage summary found. Run 999_source_lineage first.")

# ---- Decisions banner ----
decisions_banner()

# COMMAND ----------

# DBTITLE 1,Run action, profile, output scope
W = dbutils.widgets

# ---- Run action ----
W.dropdown("run_action", "Full study",
           ["Full study", "Read-and-recommend", "Select notebooks", "Outputs only"],
           "1. Run action")

# ---- Run profile (for Select notebooks) ----
_rp = {k: v for k, v in (S.params.get("run_profiles") or {}).items() if not str(k).startswith("_")}
PROFILES = {"full": list(STUDY_STEPS), **{k: [s.strip() for s in str(v).split(",") if s.strip()] for k, v in _rp.items() if k != "full"}}
W.dropdown("run_profile", "full", list(PROFILES), "2. Profile (Select notebooks)")

# ---- Skip notebooks ----
W.multiselect("skip_steps", "(none)", ["(none)"] + list(STUDY_STEPS), "3. Skip notebooks")

# ---- Output scope ----
W.dropdown("output_scope", "All",
           ["All", "Final", "Detailed", "Excel", "HTML"],
           "4. Output scope")

# ---- Read widgets ----
RUN_ACTION = W.get("run_action")
RUN_PROFILE = W.get("run_profile")
SKIP = [s for s in W.get("skip_steps").split(",") if s != "(none)"]
OUTPUT_SCOPE = W.get("output_scope")

print(f"Run action: {RUN_ACTION} | Profile: {RUN_PROFILE} | Skip: {SKIP or 'none'} | Outputs: {OUTPUT_SCOPE}")

# ---- Build the step list ----
if RUN_ACTION == "Full study":
    STEPS = [s for s in STUDY_STEPS if s not in SKIP]
elif RUN_ACTION == "Read-and-recommend":
    STEPS = [s for s in MARKUP_STAGES["accounts"] if s not in SKIP]
elif RUN_ACTION == "Select notebooks":
    STEPS = [s for s in STUDY_STEPS if s in PROFILES[RUN_PROFILE] and s not in SKIP]
elif RUN_ACTION == "Outputs only":
    _out_steps = [s for s in STUDY_STEPS if s.startswith(("07_", "08_", "09_"))]
    STEPS = [s for s in _out_steps if s not in SKIP]

if RUN_ACTION != "Full study" and RUN_PROFILE != "full":
    _need = [t for t in ("mv_product_map", "mv_final_betas") if not S.table_exists(t)]
    if _need:
        print(f"WARNING: {[S.table(t) for t in _need]} missing. Run 'Full study' first.")

print(f"Notebooks to run ({len(STEPS)}): {', '.join(STEPS)}")

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Execute and display results
# ---- Run the notebooks ----
assert STEPS, "Nothing to run. Adjust run_action, profile, or skip_steps."
log = run_steps(STEPS)
display(pd.DataFrame(log))

_ok = all(r["status"] == "OK" for r in log) and len(log) == len(STEPS)
if not _ok:
    print("\nThe study stopped at a failure. Open the failed notebook and run it top to bottom.")

if RUN_ACTION == "Read-and-recommend":
    write_state("accounts", "00_run_study: 01 ran, waiting for the accounts decision")
    _url = "/#workspace" + str(STUDY_ROOT / "notebooks" / "00_read_and_recommend").replace("/Workspace", "", 1)
    alert("info", "Stage 1 ready: open 00_read_and_recommend to set the accounts decision.",
          [f"<a href='{_url}' target='_blank'>00_read_and_recommend</a>: decision = accounts, action = review, Run all",
           "Each save there runs the next stage (products, betas, reports)."])
elif _ok:
    write_state("done", f"00_run_study: {RUN_ACTION}")

# ---- List outputs by scope ----
host = "/#workspace"
_scope_map = {
    "All": lambda p: True,
    "Final": lambda p: "deposit_study_package" in p.name or "09_" in p.name,
    "Detailed": lambda p: p.suffix in (".xlsx", ".html") and "package" not in p.name,
    "Excel": lambda p: p.suffix == ".xlsx",
    "HTML": lambda p: p.suffix == ".html",
}
_filt = _scope_map.get(OUTPUT_SCOPE, _scope_map["All"])
files = sorted(p for d in ("inputs", "outputs") for p in (STUDY_ROOT / d).rglob(f"*{S.tag}*")
               if p.is_file() and _filt(p))
if files:
    displayHTML(f"<h3 style='color:#1F4E79'>Outputs ({OUTPUT_SCOPE}): {len(files)} files</h3><ul>" +
               "".join(f'<li><a href="{host}{str(p).replace("/Workspace", "", 1)}" target="_blank">{p.relative_to(STUDY_ROOT)}</a></li>' for p in files) +
               "</ul>")
else:
    print(f"No outputs matching scope '{OUTPUT_SCOPE}' found.")

print(f"\n{'=' * 60}")
print(f"  Study {S.tag} | {RUN_ACTION} | {len(log)} notebooks | {OUTPUT_SCOPE} outputs")
print(f"{'=' * 60}")