# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 00 | Read and recommend: review the decisions, set `current`, continue the study
# MAGIC
# MAGIC Used when `00_run_study` runs with **run_mode = RecommendCurrents**. The study stops before each decision; this notebook shows the data for it, takes your changes and then runs the next part of the study.
# MAGIC
# MAGIC | Stage | Decision (`inputs/decisions/current/<decision>.csv`) | Key | Ready after | After you save, this notebook runs |
# MAGIC |---|---|---|---|---|
# MAGIC | 1 | `accounts`: subproduct key -> product, group, segment, rate class | `join_key` | 01 | 01 again (if you changed it), 02, 03, 03a, 04 |
# MAGIC | 2 | `products`: subproduct -> final product | `sub_key` | 04 | 04 again (if changed), 05, 06 |
# MAGIC | 3 | `betas`: product -> method override | `product` | 06 | 06 again (if changed), 08, 07, 09 |
# MAGIC
# MAGIC **How to use it**
# MAGIC 1. Set `decision` to the stage shown in the banner, leave `action = review`, press **Run all**. Read the alerts, the table and the chart.
# MAGIC 2. To change values: copy lines from the "Lines to copy" cell into the **Your edits** cell, change the value, set `action = apply edits`. To take the rules as they are: `action = push derived as is` (or `push source as is` for the reference values). To change nothing: `keep current`.
# MAGIC 3. Run all with `confirm = no` first: it shows every change and every alert. When they are right, set `confirm = yes, I checked` and Run all again. That saves `current`, logs the change, and (with `continue_study = yes`) runs the next stage.
# MAGIC 4. Repeat for the next decision. After `betas` the study is complete.
# MAGIC
# MAGIC `current` is always what the study uses, also on a cloned study. `derived` and `source` are only for comparison and are rewritten each run (see the banner).

# COMMAND ----------

# DBTITLE 1,Install openpyxl and plotly
# MAGIC %pip install -q openpyxl plotly

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ./00_setup

# COMMAND ----------

# DBTITLE 1,Widgets, stage and the decisions banner
_stage = read_state().get("stage", "")
W = dbutils.widgets
W.dropdown("decision", _stage if _stage in DECISIONS else "accounts", list(DECISIONS), "1. Decision")
W.dropdown("action", "review", ["review", "apply edits", "push derived as is", "push source as is", "keep current"], "2. Action")
W.dropdown("confirm", "no", ["no", "yes, I checked"], "3. Confirm (saves current)")
W.dropdown("continue_study", "yes", ["yes", "no"], "4. Continue the study after saving")
W.dropdown("show", "flagged", ["flagged", "all"], "5. Rows to show")
W.text("chart_item", "", "6. Chart: product (betas), sub_key (products) or all")

DEC = W.get("decision")
ACTION = W.get("action")
CONFIRM = W.get("confirm") == "yes, I checked"
CONTINUE = W.get("continue_study") == "yes"
SHOW_ALL = W.get("show") == "all"
CHART_ITEM = W.get("chart_item").strip()
D, K = DECISIONS[DEC], DECISIONS[DEC]["key"]

decisions_banner()
if _stage in DECISIONS and _stage != DEC:
    alert("warn", f"The study is waiting for the '{_stage}' decision, but decision = '{DEC}'.",
          [f"Set decision = {_stage} to continue the study in order. Saving '{DEC}' now still works; the runner then continues from after '{DEC}'."])
elif _stage == "done":
    alert("ok", "The study has run through every decision.", ["You can still change a decision; saving it re-runs the study from that point."])
elif not _stage:
    alert("info", "No staged run yet.", ["Run 00_run_study with run_mode = RecommendCurrents, or review a decision that already has a current file."])
print(f"decision = {DEC} | action = {ACTION} | confirm = {CONFIRM} | continue_study = {CONTINUE}")

# COMMAND ----------

# DBTITLE 1,Key: fields, allowed values, line format
# MAGIC %md
# MAGIC ## Key
# MAGIC Each change is one line in the **Your edits** cell: `key | field | new value | why`. Lines starting with `#` are ignored. An empty value blanks the field.
# MAGIC
# MAGIC | Decision | `key` is | Editable `field`s | Allowed values |
# MAGIC |---|---|---|---|
# MAGIC | `accounts` | `join_key`: PNFP `PNFP\|subproductid\|pf_flag\|bdsp_flag`, SNV `SNV\|subproductid\|*\|bdsp_flag` | `product` | a product name; not in `inputs/products.csv` = warning |
# MAGIC | | | `product_num` | digits (the product code; should match `inputs/product_lookup.csv` for the product) |
# MAGIC | | | `product_group`, `client_group`, `rate_class` | a value already used; a new one = warning |
# MAGIC | | | `acct_typ_desc` | free text |
# MAGIC | | | `dim_pf_flag` | `Y`, `N` or empty |
# MAGIC | `products` | `sub_key` (as in `mv_product_selection`) | `new_product` | a product name; empty = unassigned (stays on its current product, flagged in FINAL mode); not known = warning |
# MAGIC | | | `notes` | free text |
# MAGIC | `betas` | `product` (the final product name, `mv_product`) | `override_method` | empty (use the rule), `LM`, `POLY`, `AVG`, `MANUAL` |
# MAGIC | | | `override_lag` | empty (best lag) or one of the `lags` in the config (`params` tab) |
# MAGIC | | | `manual_beta` | a number; required with `MANUAL` |
# MAGIC | | | `reason` | required with any override |
# MAGIC
# MAGIC **Alerts.** Red = must be fixed, nothing is saved. Amber = check it; it is saved only with `confirm = yes, I checked`. Every saved change goes to `inputs/decisions/change_log.csv`, and the previous file to `inputs/decisions/history/`.
# MAGIC
# MAGIC **Status in the table.** accounts / products: `NOT IN THIS RUN` (key in current only), `UNMAPPED` / `UNASSIGNED`, `EDITED` (current differs from derived), `DERIVED DIFFERS FROM SOURCE`, `ok`. betas: `OVERRIDE, NO REASON`, `OVERRIDE`, `THIN HISTORY`, `LOW BALANCE`, `ok`.

# COMMAND ----------

# DBTITLE 1,Indicative view: current vs derived vs source
cur, der, src = read_decision("current", DEC), read_decision("derived", DEC), read_decision("source", DEC)
assert cur is not None, f"inputs/decisions/current/{DEC}.csv is not made yet: run {D['notebook']} first (00_run_study, run_mode = RecommendCurrents)"

view = cur.copy()
for kind, df in (("derived", der), ("source", src)):
    other = df if df is not None else pd.DataFrame(columns=[K] + D["edit"])
    view = view.merge(other[[K] + D["edit"]].rename(columns={c: f"{c}_{kind}" for c in D["edit"]}), on=K, how="left")
    for c in D["edit"]:
        view[f"{c}_{kind}"] = view[f"{c}_{kind}"].astype("string").fillna("")
view["in_this_run"] = view[K].isin(der[K]) if der is not None else True
# a blank derived value means the rule gave nothing (the reference filled it): not an edit
view["vs_derived"] = view.apply(lambda r: ", ".join(c for c in D["edit"] if r.in_this_run and r[f"{c}_derived"] != "" and r[c] != r[f"{c}_derived"]), axis=1)
view["vs_source"] = view.apply(lambda r: ", ".join(c for c in D["edit"] if r[f"{c}_derived"] != r[f"{c}_source"]), axis=1)
for c in ["snapshot_balance", "snapshot_accounts", "months", "months_ttc", "snapshot_war", "sub_recommended_beta", "sub_recommended_r2",
          "recommended_beta", "best_lm_beta", "best_lm_r2", "best_poly_beta", "best_poly_r2"]:
    if c in view:
        view[c + "_n"] = pd.to_numeric(view[c], errors="coerce")

RED = {"NOT IN THIS RUN", "UNMAPPED", "UNASSIGNED", "OVERRIDE, NO REASON"}
def _status(r):
    if DEC == "betas":
        if r.override_method and not r.reason:
            return "OVERRIDE, NO REASON"
        if r.override_method:
            return "OVERRIDE"
        if (r.get("months_ttc_n") or 0) < MIN_MONTHS:
            return "THIN HISTORY"
        if (r.get("snapshot_balance_n") or 0) < MIN_BAL:
            return "LOW BALANCE"
        return "ok"
    if not r.in_this_run:
        return "NOT IN THIS RUN"
    main = r.product if DEC == "accounts" else r.new_product
    if main == "":
        return "UNMAPPED" if DEC == "accounts" else "UNASSIGNED"
    if r.vs_derived:
        return "EDITED"
    if r.vs_source:
        return "DERIVED DIFFERS FROM SOURCE"
    return "ok"
view["status"] = view.apply(_status, axis=1)
view["severity"] = np.where(view.status.isin(RED), "red", np.where(view.status == "ok", "green", "amber"))
view = view.sort_values(["severity", "snapshot_balance_n"], key=lambda s: s.map({"red": 0, "amber": 1, "green": 2}) if s.name == "severity" else -s.fillna(0),
                        ignore_index=True)
to_view(view.drop(columns=[c for c in view.columns if c.endswith("_n")]), f"decision_{DEC}")

counts = view.groupby(["severity", "status"], as_index=False).agg(rows=(K, "count"), balance=("snapshot_balance_n", "sum"))
display(counts)
red = view[view.severity == "red"]
diff = decision_diff(cur, der, DEC)
diff = diff[diff.b != ""]                     # ignore fields the rule left blank
if len(red):
    alert("error", f"{len(red):,} {DEC} rows must be adjusted and double checked (${red.snapshot_balance_n.sum():,.0f} snapshot balance)",
          [f"{r[K]}: {r.status}" for _, r in red.iterrows()])
if len(diff):
    alert("warn", f"current does not match derived for {diff[K].nunique():,} keys: check each one is intended",
          [f"{r[K]}: {r.field} current '{r.a}' vs derived '{r.b}'" for _, r in diff.iterrows()])
if not len(red) and not len(diff):
    alert("ok", f"current/{DEC}.csv matches derived and has nothing to fix.")
print(f"temp view decision_{DEC}: {len(view):,} rows (current, with _derived and _source columns)")

# COMMAND ----------

# DBTITLE 1,Decision table (conditional formatting) and chart
import plotly.express as px

COLS = {
    "accounts": [
        "status",
        K,
        "legacy_bank",
        "subproductid",
        "product",
        "product_derived",
        "product_source",
        "product_group",
        "rate_class",
        "vs_derived",
        "snapshot_accounts_n",
        "snapshot_balance_n",
        "months_n",
    ],
    "products": [
        "status",
        K,
        "legacy_bank",
        "current_product",
        "new_product",
        "new_product_derived",
        "new_product_source",
        "sub_recommended_method",
        "sub_recommended_beta_n",
        "sub_recommended_r2_n",
        "snapshot_balance_n",
        "months_ttc_n",
    ],
    "betas": [
        "status",
        K,
        "recommended_method",
        "recommended_beta_n",
        "override_method",
        "override_lag",
        "manual_beta",
        "reason",
        "best_lm_beta_n",
        "best_lm_r2_n",
        "best_poly_beta_n",
        "best_poly_r2_n",
        "snapshot_balance_n",
        "months_ttc_n",
    ],
}[DEC]
t = (view if SHOW_ALL else view[view.severity != "green"])[COLS].head(300).copy()
if "snapshot_balance_n" in t:
    t["snapshot_balance_n"] = t["snapshot_balance_n"] / 1e6
t = t.rename(columns={c: c[:-2] for c in t.columns if c.endswith("_n")}).rename(
    columns={"snapshot_balance": "balance_mm"}
)

SEV = {
    "red": "background-color:#f8d7da;color:#842029;font-weight:bold",
    "amber": "background-color:#fff3cd;color:#664d03",
    "green": "background-color:#d1e7dd;color:#0f5132",
}
sev_of = lambda s: "red" if s in RED else ("green" if s == "ok" else "amber")
r2_style = (
    lambda v: ""
    if pd.isna(v)
    else (
        "background-color:#f8d7da"
        if v < 50
        else ("background-color:#fff3cd" if v < 80 else "background-color:#d1e7dd")
    )
)
months_style = (
    lambda v: "background-color:#fff3cd" if pd.notna(v) and v < MIN_MONTHS else ""
)

sty = t.style
_map = sty.map if hasattr(sty, "map") else sty.applymap
sty = _map(lambda s: SEV[sev_of(s)], subset=["status"])
_map = sty.map if hasattr(sty, "map") else sty.applymap
for c in [c for c in t.columns if c.endswith("_r2")]:
    sty = _map(r2_style, subset=[c])
for c in [c for c in t.columns if c in ("months", "months_ttc")]:
    sty = _map(months_style, subset=[c])
for a, b in [("product", "product_derived"), ("new_product", "new_product_derived")]:
    if a in t and b in t:
        sty = sty.apply(
            lambda col, a=a, b=b: [
                "background-color:#fff3cd" if y != "" and x != y else ""
                for x, y in zip(t[a], t[b])
            ],
            subset=[a],
        )
if "balance_mm" in t:
    sty = sty.bar(subset=["balance_mm"], color="#9ecae1")
sty = sty.format(
    {c: "{:,.1f}" for c in ["balance_mm"] if c in t}
    | {c: "{:.2f}" for c in t.columns if c.endswith("_beta") and pd.api.types.is_numeric_dtype(t[c])}
    | {c: "{:.1f}" for c in t.columns if c.endswith("_r2") and pd.api.types.is_numeric_dtype(t[c])},
    na_rep="",
).hide(axis="index")
print(
    f"{len(t):,} rows shown ({'all' if SHOW_ALL else 'flagged only'}; max 300). R2: red < 50, amber < 80. Months amber < {MIN_MONTHS}."
)
# a cell only keeps its LAST displayHTML, so the table and charts are collected and shown together at the end
_html = [f"<div style='max-height:600px;overflow:auto;font-family:Arial;font-size:12px'>{sty.to_html()}</div>"]

# chart: balance by product and status, plus WAR history for one item
grp = {"accounts": "product", "products": "new_product", "betas": K}[DEC]
b = (
    view.assign(_p=view[grp].replace("", "(blank)"))
    .groupby(["_p", "status"], as_index=False)
    .snapshot_balance_n.sum()
)
_n_bars = None if SHOW_ALL else 30          # show = all -> every product, else the top 30 by balance
_tot = b.groupby("_p").snapshot_balance_n.sum().sort_values(ascending=False)
top = _tot.index if _n_bars is None else _tot.head(_n_bars).index
fig = px.bar(
    b[b._p.isin(top)],
    x="_p",
    y="snapshot_balance_n",
    color="status",
    title=f"{DEC}: snapshot balance by {grp} and status ({'all ' + str(len(top)) if _n_bars is None else 'top 30'})",
    labels={"_p": grp, "snapshot_balance_n": "snapshot balance"},
    color_discrete_map={
        s: {"red": "#c62828", "amber": "#f9a825", "green": "#2e7d32"}[sev_of(s)]
        for s in b.status.unique()
    },
)
fig.update_layout(height=450, xaxis_tickangle=-45)
_html.append(fig.to_html(include_plotlyjs="cdn", full_html=False))

# chart_item that is blank, the placeholder text or not a key of this decision -> the largest-balance key
_ci = (CHART_ITEM or "").strip()
_default_item = view.loc[view.snapshot_balance_n.idxmax(), K] if len(view) else None
CHART_ALL = _ci.lower() == "all" and DEC in ("betas", "products")
if CHART_ALL:
    # chart_item = all -> WAR history for every final product, one line each (click the legend to hide / isolate)
    wa = spark.table(S.table("mv_product_war")).select("mv_product", "eopdate", "war").toPandas().sort_values(["mv_product", "eopdate"])
    _order = list(view.sort_values("snapshot_balance_n", ascending=False)[K]) if DEC == "betas" else sorted(wa.mv_product.unique())
    f3 = px.line(wa, x="eopdate", y="war", color="mv_product", category_orders={"mv_product": _order},
                 title=f"WAR by month: all {wa.mv_product.nunique()} final products (double-click a name to isolate it)")
    f3.update_layout(height=650, legend=dict(font=dict(size=10)))
    _html.append(f3.to_html(include_plotlyjs=False, full_html=False))
    _ci = ""
elif _ci and _ci not in set(view[K]):
    print(f"chart_item '{_ci}' is not a {K} in {DEC}; charting the largest: {_default_item}")
    _ci = ""
if DEC == "betas":
    item = _ci or _default_item
    w = (
        spark.table(S.table("mv_product_war"))
        .where(F.col("mv_product") == item)
        .select("eopdate", "war", "eopbook")
        .toPandas()
    )
elif DEC == "products":
    item = _ci or _default_item
    w = (
        spark.table(S.table("mv_account_war"))
        .where(F.col("sub_key") == item)
        .select("eopdate", "war", "eopbook")
        .toPandas()
    )
else:
    item, w = None, pd.DataFrame()
if CHART_ALL:
    item = None                               # the all-products chart is already shown above
if item is not None:
    if len(w):
        f2 = px.line(
            w.sort_values("eopdate"),
            x="eopdate",
            y="war",
            title=f"WAR by month: {item}",
        )
        f2.update_layout(height=350)
        _html.append(f2.to_html(include_plotlyjs=False, full_html=False))
    else:
        _html.append(f"<div style='background:#fff3cd;color:#664d03;padding:8px;font-family:Arial'>No WAR history found for '{item}'. "
                     "Check the chart_item widget (betas: product name, products: sub_key, or all).</div>")
displayHTML("".join(_html))

# COMMAND ----------

# DBTITLE 1,Lines to copy into Your edits
# Ready-made lines for the flagged rows, with today's current value. Copy the ones to change into the next cell and edit the value.
MAIN = {"accounts": ["product"], "products": ["new_product"], "betas": ["override_method", "override_lag", "manual_beta", "reason"]}[DEC]
lines = []
for _, r in view[view.severity != "black"].head(60).iterrows():
    fields = [f.strip() for f in r.vs_derived.split(",") if f.strip()] or MAIN
    for f in fields:
        hint = f"   # derived: {r[f + '_derived']}" if r[f + "_derived"] != r[f] else ""
        lines.append(f"{r[K]} | {f} | {r[f]} | {hint}")
print(f"# {DEC}: key | field | new value | why")
print("\n".join(lines) if lines else "# nothing flagged: set show = all and use the table above for keys")

# COMMAND ----------

# DBTITLE 1,Your edits (one line per change)
# =============================== YOUR EDITS (for the decision in the widget) ===============================
# One line per change:      key | field | new value | why
#   key    accounts: join_key (PNFP|1B_000_00|N|N)    products: sub_key    betas: product name
#   field  an editable field of the decision (see the Key cell above)
#   value  the new value; leave it empty to blank the field (e.g. remove an override)
#   why    free text, saved to inputs/decisions/change_log.csv
# Lines starting with # are ignored. Use | only as the separator. Then set action = apply edits.
EDITS = """

# SNV|050_000_00|*|N | product | Savings Consumer | unmapped; savings product
# SNV|050_000_00|*|N | product_group | Savings | match SNV Savings Consumer
# SNV|050_000_00|*|N | client_group | Consumer | match SNV Savings Consumer
# SNV|050_000_00|*|N | product_num | 20014000 | Savings Consumer code
# SNV|050_000_00|*|N | dim_pf_flag | N | not public funds


# SNV|050_082_00|*|N | product | Savings Consumer | unmapped; savings product
# SNV|050_082_00|*|N | product_group | Savings | match SNV Savings Consumer
# SNV|050_082_00|*|N | client_group | Consumer | match SNV Savings Consumer
# SNV|050_082_00|*|N | product_num | 20014000 | Savings Consumer code
# SNV|050_082_00|*|N | dim_pf_flag | N | not public funds


# # MMA Consumer Standard | override_method | POLY | poly R2 higher and in line with history
# # MMA Consumer Standard | reason | poly R2 higher and in line with history |
"""

_rows = []
for i, ln in enumerate(EDITS.splitlines(), 1):
    s = ln.split("   #")[0].strip()
    if not s or s.startswith("#"):
        continue
    # keys contain | themselves (SNV|050_000_00|*|N), so the key is everything before the first editable field name
    parts = [p.strip() for p in s.split("|")]
    fi = next((j for j, p in enumerate(parts) if j > 0 and p in D["edit"]), None)
    if fi is None:
        _rows.append(dict(line=i, key="|".join(parts), field="(no editable field found)", value="", why=""))
        continue
    rest = parts[fi + 1:] + [""]
    _rows.append(dict(line=i, key="|".join(parts[:fi]), field=parts[fi], value=rest[0], why=" | ".join(p for p in rest[1:] if p)))
edits = pd.DataFrame(_rows, columns=["line", "key", "field", "value", "why"])
if len(edits):
    to_view(edits.astype(str), "markup_edits")
else:
    spark.createDataFrame([], "line string, key string, field string, value string, why string").createOrReplaceTempView("markup_edits")
print(f"{len(edits)} edit lines (temp view markup_edits)")
if len(edits):
    display(edits)

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Check the edits (alerts) and the proposed current
errors, warns = [], []
products_csv = INPUTS / "products.csv"
PRODUCT_NAMES = set(pd.read_csv(products_csv, dtype=str, keep_default_na=False)["product_name"].str.strip()) if products_csv.exists() else set()
lookup_csv = INPUTS / "product_lookup.csv"
LOOKUP = (dict(pd.read_csv(lookup_csv, dtype=str, keep_default_na=False)[["product_name", "product_code"]].values) if lookup_csv.exists() else {})
seen = {c: set().union(*[set(x[c]) for x in (cur, der, src) if x is not None]) - {""} for c in D["edit"]}

proposed = cur.copy().set_index(K)
for e in edits.itertuples():
    where = f"line {e.line} ({e.key} | {e.field})"
    if e.key not in proposed.index:
        errors.append(f"{where}: key not in current/{DEC}.csv")
        continue
    if e.field not in D["edit"]:
        errors.append(f"{where}: '{e.field}' is not editable; use one of {D['edit']}")
        continue
    v = e.value.upper() if e.field == "override_method" else e.value
    if DEC == "accounts":
        if e.field == "product" and v and v not in PRODUCT_NAMES | seen["product"]:
            warns.append(f"{where}: '{v}' is not in inputs/products.csv or any decision file")
        if e.field == "product_num" and v and not v.isdigit():
            errors.append(f"{where}: product_num must be digits")
        if e.field in ("product_group", "client_group", "rate_class") and v and v not in seen[e.field]:
            warns.append(f"{where}: '{v}' is a new {e.field}")
        if e.field == "dim_pf_flag" and v not in ("", "Y", "N"):
            errors.append(f"{where}: dim_pf_flag must be Y, N or empty")
    elif DEC == "products":
        if e.field == "new_product" and v == "":
            warns.append(f"{where}: blank new_product = unassigned (kept on its current product)")
        elif e.field == "new_product" and v not in PRODUCT_NAMES | seen["new_product"]:
            warns.append(f"{where}: '{v}' is a new product name")
    else:
        if e.field == "override_method" and v not in ("", "LM", "POLY", "AVG", "MANUAL"):
            errors.append(f"{where}: override_method must be empty, LM, POLY, AVG or MANUAL")
        if e.field == "override_lag" and v and not (v.isdigit() and int(v) in LAGS):
            errors.append(f"{where}: override_lag must be empty or one of {LAGS}")
        if e.field == "manual_beta" and v:
            try:
                float(v)
            except ValueError:
                errors.append(f"{where}: manual_beta must be a number")
    proposed.loc[e.key, e.field] = v
proposed = proposed.reset_index()

# row checks on every edited key
for key in set(edits.key) & set(proposed[K]):
    r = proposed.loc[proposed[K] == key].iloc[0]
    if DEC == "betas" and r.override_method:
        if not r.reason:
            errors.append(f"{key}: an override needs a reason")
        if r.override_method == "MANUAL" and not r.manual_beta:
            errors.append(f"{key}: MANUAL needs manual_beta")
        if r.override_method != "MANUAL" and r.manual_beta:
            warns.append(f"{key}: manual_beta is ignored unless override_method = MANUAL")
    if DEC == "accounts" and r.product in LOOKUP and r.product_num and LOOKUP[r.product] != r.product_num:
        warns.append(f"{key}: product_num {r.product_num} is not the code of '{r.product}' in product_lookup.csv ({LOOKUP[r.product]})")

changes = decision_diff(cur, proposed, DEC).rename(columns={"a": "current", "b": "proposed"})
to_view(proposed, f"decision_{DEC}_proposed")
if len(edits):
    print(f"{len(changes)} field changes from {len(edits)} lines (temp view decision_{DEC}_proposed):")
    if len(changes):
        display(changes)
if errors:
    alert("error", f"{len(errors)} problems: fix the lines in Your edits. Nothing will be saved.", errors)
if warns:
    alert("warn", f"{len(warns)} values to double check: saved only with confirm = yes, I checked", warns)
if len(edits) and not errors and not warns:
    alert("ok", "All edit lines are valid.")

# COMMAND ----------

# DBTITLE 1,Save current and continue the study
def _from(other):
    """current with the edit columns of another decision file (derived / source) for the keys it has."""
    new = cur.copy().set_index(K)
    o = other.set_index(K)[D["edit"]]
    common = new.index.intersection(o.index)
    new.loc[common, D["edit"]] = o.loc[common]
    return new.reset_index()

if ACTION == "review":
    alert("info", "action = review: nothing saved.",
          ["apply edits = save the lines in Your edits", "push derived as is = current takes the derived values",
           "push source as is = current takes the source values", "keep current = change nothing and continue the study"])
else:
    if ACTION == "apply edits":
        new, why = proposed, dict(zip(edits.key + "|" + edits.field, edits.why))
    elif ACTION == "push derived as is":
        assert der is not None, f"derived/{DEC}.csv is not made yet"
        new, why = _from(der), {}
    elif ACTION == "push source as is":
        assert src is not None, f"source/{DEC}.csv is not made yet"
        new, why = _from(src), {}
    else:
        new, why = cur, {}
    ch = decision_diff(cur, new, DEC)
    blocked = (ACTION == "apply edits" and errors)
    need_confirm = len(ch) > 0 or (ACTION == "apply edits" and warns)
    if blocked:
        alert("error", "Not saved: fix the red problems in the check cell first.", errors)
    elif need_confirm and not CONFIRM:
        alert("warn", f"{len(ch)} changes ready ({ACTION}). Check them, then set confirm = yes, I checked and run all again.",
              [f"{r[K]}: {r.field} '{r.a}' -> '{r.b}'" for _, r in ch.iterrows()] or ["no field changes; the warnings above still need a check"])
    else:
        if len(ch):
            write_decision("current", DEC, new)
            user = spark.sql("SELECT current_user()").first()[0]
            at = f"{pd.Timestamp.now():%Y-%m-%d %H:%M:%S}"
            logrows = pd.DataFrame([dict(at=at, user=user, decision=DEC, action=ACTION, key=r[K], field=r.field, old=r.a, new=r.b,
                                         why=why.get(f"{r[K]}|{r.field}", ACTION)) for _, r in ch.iterrows()])
            logrows.to_csv(DEC_LOG, mode="a", header=not DEC_LOG.exists(), index=False)
            alert("ok", f"Saved inputs/decisions/current/{DEC}.csv: {len(ch)} field changes ({ch[K].nunique()} keys). Logged in change_log.csv.")
        else:
            alert("ok", f"current/{DEC}.csv unchanged ({ACTION}).")
        if CONTINUE:
            nxt = STAGE_ORDER[STAGE_ORDER.index(DEC) + 1]
            steps = ([D["notebook"]] if len(ch) else []) + MARKUP_STAGES[nxt]
            print(f"Continuing the study: {' -> '.join(steps)}")
            log = run_steps(steps)
            display(pd.DataFrame(log))
            if all(r["status"] == "OK" for r in log) and len(log) == len(steps):
                write_state(nxt, f"{DEC} saved ({ACTION}, {len(ch)} changes)")
                if nxt == "done":
                    alert("ok", "Study complete: every decision is set and all notebooks ran.", ["Results: outputs/excel, outputs/html, outputs/reports."])
                else:
                    alert("info", f"Next decision: {nxt}. Set decision = {nxt}, action = review, confirm = no, then Run all.")
            else:
                bad = [r for r in log if r["status"] != "OK"]
                alert("error", f"The study stopped at {bad[0]['notebook'] if bad else '?'}",
                      [bad[0]["error"] if bad else "", "Open that notebook, run it top to bottom, fix it, then save this decision again (keep current)."])
        else:
            alert("info", "continue_study = no: the study was not run.", [f"Run {D['notebook']} and the notebooks after it to use the new current."])