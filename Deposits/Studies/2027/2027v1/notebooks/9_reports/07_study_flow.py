# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 07 | Study flow: how we get from accounts to final betas
# MAGIC
# MAGIC An interactive Mermaid flow of the whole MV study, built from the tables and files of the last run. Click a box (or the buttons above the diagram, or Prev / Next) to read what each step does, what it reads and writes, and the numbers it produced.
# MAGIC
# MAGIC | Output | What it is |
# MAGIC |---|---|
# MAGIC | `outputs/html/mv_flow_<tag>.html` | The interactive page (Mermaid diagram + read-along panel). Open it in a browser; it loads Mermaid and Marked from a CDN |
# MAGIC | `outputs/reports/mv_flow_<tag>.md` | The same content as Markdown with a `mermaid` block, for any Markdown viewer that renders Mermaid |
# MAGIC
# MAGIC Run after 06 (or as the last step of `00_run_study`). Nothing here writes tables.

# COMMAND ----------

# DBTITLE 1,Install openpyxl
# MAGIC %pip install -q openpyxl

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ../00_setup

# COMMAND ----------

# DBTITLE 1,1. Gather the numbers from the last run
import json

fB = lambda x: "n/a" if pd.isna(x) else ("$0" if x == 0 else (f"${x / 1e9:,.2f}B" if abs(x) >= 1e9 else f"${x / 1e6:,.2f}M"))
fL = lambda x: "n/a" if pd.isna(x) else str(int(x))
fN = lambda x: f"{int(x):,}"
fR = lambda x, d=2: "n/a" if pd.isna(x) else f"{float(x):,.{d}f}"
fam = lambda m: str(m).split("(")[0].split("_")[0]          # AVG / POLY / LM / NONE

def mdt(df):
    """DataFrame -> GitHub Markdown table."""
    cols = list(df.columns)
    lines = ["| " + " | ".join(map(str, cols)) + " |", "|" + "---|" * len(cols)]
    for r in df.itertuples(index=False):
        lines.append("| " + " | ".join("" if (isinstance(v, float) and pd.isna(v)) else str(v).replace("|", "\\|") for v in r) + " |")
    return "\n".join(lines)

T = lambda n: spark.table(S.table(n)).toPandas()

acc = sqlq("""
SELECT legacy_bank, mapped, COUNT(*) AS accounts, SUM(eopbook) AS balance,
       TRY_DIVIDE(SUM(sumprod_eoprate), SUM(eopbook)) AS war
FROM {{mv_accounts}} GROUP BY ALL ORDER BY 1, 2""").toPandas()
rc = sqlq("""
SELECT CASE WHEN subproductid LIKE '%\_INT' THEN 'Internal (_INT)'
            ELSE CASE RIGHT(subproductid, 6) WHEN '000_01' THEN 'Indexed (000_01)' WHEN '000_00' THEN 'Standard / ZeroRate (000_00)'
                 WHEN '070_00' THEN 'Standard Plus (070_00)' WHEN '081_00' THEN 'DNC (081_00)' ELSE 'other' END END AS rate_class,
       COUNT(DISTINCT product) AS products, COUNT(*) AS accounts, SUM(eopbook) AS balance
FROM {{mv_accounts}} WHERE legacy_bank = 'PNFP' GROUP BY ALL ORDER BY 1""").toPandas()
plus = sqlq("""
SELECT COUNT(DISTINCT product) AS products, COUNT(*) AS accounts, COALESCE(SUM(eopbook), 0) AS balance FROM {{mv_accounts}}
WHERE legacy_bank = 'PNFP'""").toPandas().iloc[0]
bdsp = sqlq("""SELECT COUNT(*) AS accounts, COALESCE(SUM(eopbook), 0) AS balance FROM {{mv_accounts}}
WHERE legacy_bank = 'SNV' AND bdsp_flag = 'Y'""").toPandas().iloc[0]
war_s = sqlq("""
SELECT legacy_bank, COUNT(DISTINCT sub_key) AS sub_keys, COUNT(DISTINCT eopdate) AS months,
       CAST(MIN(eopdate) AS STRING) AS first_month, CAST(MAX(eopdate) AS STRING) AS last_month
FROM {{mv_account_war}} GROUP BY 1 ORDER BY 1""").toPandas()
pm = sqlq("""
SELECT COUNT(*) AS accounts, SUM(eopbook) AS balance, COUNT(DISTINCT mv_product) AS products, COUNT(DISTINCT sub_key) AS sub_keys
FROM {{mv_product_map}}""").toPandas().iloc[0]

map_used = read_tab(inp("account_mapping"), "mapping_used")
for c in ["snapshot_accounts", "snapshot_balance"]:
    map_used[c] = pd.to_numeric(map_used[c], errors="coerce").fillna(0)
met, ab, sel = T("mv_account_metrics"), T("mv_account_betas"), T("mv_product_selection")
pw, pb, rev, fb = T("mv_product_war"), T("mv_product_betas"), T("mv_beta_review"), T("mv_final_betas")
KI_FILE = out("reports", "known_issues", "csv")
ki = pd.read_csv(KI_FILE, dtype=str, keep_default_na=False) if KI_FILE.exists() else pd.DataFrame(columns=ISSUE_COLS)
ki["balance"] = pd.to_numeric(ki["balance"], errors="coerce").fillna(0)

TOT_ACC, TOT_BAL = acc.accounts.sum(), acc.balance.sum()
N_HIGH = int((ki.severity == "HIGH").sum())
print(f"{fN(TOT_ACC)} accounts | {fB(TOT_BAL)} | {len(ab)} sub_keys | {len(fb)} products | {len(ki)} known issues ({N_HIGH} HIGH)")
display(fb.sort_values("snapshot_balance", ascending=False)[["mv_product", "snapshot_balance", "final_method", "final_beta", "final_r2", "final_source"]].head(5))

# COMMAND ----------

# DBTITLE 1,2. The read-along text for every step (Markdown)
# Every box in the diagram gets one Markdown panel. Numbers come from the tables and files of the last run (Step 1).
SRC_T = S.params["sources"]
ttc_rng = f"{NAMED[TTC][0]:%Y-%m} to {NAMED[TTC][-1]:%Y-%m}"
bank = acc.groupby("legacy_bank").agg(accounts=("accounts", "sum"), balance=("balance", "sum")).reset_index()
unm = acc[~acc.mapped.astype(bool)]
UNM_ACC, UNM_BAL = (unm.accounts.sum(), unm.balance.sum()) if len(unm) else (0, 0.0)
bank_tbl = mdt(pd.DataFrame({"bank": bank.legacy_bank, "accounts": bank.accounts.map(fN), "balance": bank.balance.map(fB)}))
rc_tbl = mdt(pd.DataFrame({"rate class": rc.rate_class, "products": rc.products, "accounts": rc.accounts.map(fN), "balance": rc.balance.map(fB)}))
war_tbl = mdt(war_s)
vs = map_used.groupby(["legacy_bank", "value_source"], as_index=False).agg(keys=("join_key", "count"), accounts=("snapshot_accounts", "sum"), balance=("snapshot_balance", "sum"))
vs_tbl = mdt(vs.assign(accounts=vs.accounts.map(fN), balance=vs.balance.map(fB)))

met_bal = met[met.eopbook.astype(float) != 0]
thin = met[met.months_history.astype(float) < MIN_MONTHS]

def fam_tbl(df, bal_col):
    g = df.assign(family=df.recommended_method.map(fam)).groupby("family", as_index=False).agg(rows=("recommended_method", "count"), balance=(bal_col, "sum"))
    g = g.sort_values("balance", ascending=False)
    return mdt(g.assign(balance=g.balance.map(fB)))

ab_tbl = fam_tbl(ab, "snapshot_balance")
sa = sel.groupby("assignment_source", as_index=False).agg(subproducts=("sub_key", "count"), balance=("snapshot_balance", "sum"))
sa_tbl = mdt(sa.assign(balance=sa.balance.map(fB)))
rev_tbl = fam_tbl(rev, "snapshot_balance")
fm = fb.groupby("final_method", as_index=False).agg(products=("mv_product", "count"), balance=("snapshot_balance", "sum")).sort_values("balance", ascending=False)
fm_tbl = mdt(fm.assign(balance=fm.balance.map(fB)))
fs = fb.final_source.value_counts().to_dict()
n_ovr = int((fb.override_method.astype("string").fillna("").str.strip() != "").sum())
top = fb.sort_values("snapshot_balance", ascending=False).head(15)
top_tbl = mdt(pd.DataFrame({"product": top.mv_product, "balance": top.snapshot_balance.map(fB), "method": top.final_method,
                            "beta": top.final_beta.map(lambda v: fR(v, 3)), "R2": top.final_r2.map(lambda v: fR(v, 1)),
                            "avg up": top.avg_up.map(lambda v: fR(v, 3)), "avg down": top.avg_down.map(lambda v: fR(v, 3))}))
ud_tbl = mdt(UPDOWN[["name", "direction", "start", "end"]]) if len(UPDOWN) else "_no UP / DOWN cycles_"
ki_g = ki.groupby(["step", "severity", "code"], as_index=False).agg(items=("code", "count"), balance=("balance", "sum"))
ki_tbl = mdt(ki_g.assign(balance=ki_g.balance.map(fB))) if len(ki) else "_no known issues logged_"
ki_high = ki[ki.severity == "HIGH"]
ki_high_txt = "\n".join(f"* **{r.code}** ({r.step}) `{r.item}`: {r.detail}" for r in ki_high.itertuples()) or "* none"

# worked example: the largest product with a non-zero final beta, followed from its subproducts to its final beta
_ex = top[top.final_beta.fillna(0).astype(float) != 0]
EXP = (_ex if len(_ex) else top).iloc[0]
ex_name = EXP.mv_product
ex_sel = sel[sel.mv_product == ex_name]
ex_rev = rev[rev.mv_product == ex_name].iloc[0]
ex_war = pw[pw.mv_product == ex_name].sort_values("eopdate")
ex_sub = mdt(ex_sel.sort_values("snapshot_balance", ascending=False).head(8).assign(
    snapshot_balance=lambda d: d.snapshot_balance.map(fB))[["sub_key", "current_product", "snapshot_balance", "assignment_source"]])
ex_grid = mdt(pd.DataFrame({"lag": LAGS,
    "linear beta": [fR(ex_rev.get(f"lm_{L}"), 3) for L in LAGS], "linear R2": [fR(ex_rev.get(f"lm_r2_{L}"), 1) for L in LAGS],
    "poly beta": [fR(ex_rev.get(f"poly_{L}"), 3) for L in LAGS], "poly R2": [fR(ex_rev.get(f"poly_r2_{L}"), 1) for L in LAGS]}))
ex_last = ex_war.tail(1)
ex_war_txt = (f"{len(ex_war)} months of WAR history ({str(ex_war.eopdate.min())[:10]} to {str(ex_war.eopdate.max())[:10]}); "
              f"last month WAR {fR(float(ex_last.war.iloc[0]) * 100 if len(ex_last) and pd.notna(ex_last.war.iloc[0]) else np.nan, 3)}%") if len(ex_war) else "no WAR history"
ex_fb = fb[fb.mv_product == ex_name].iloc[0]

SECTIONS = {}
SECTIONS["OV"] = ("Overview", f"""## How to read this page
This page follows the **{MV_ROOT.name}** study from the raw account rows to the final beta of every product. Click any box in the diagram, a button above it, or step through with **Prev / Next**. Each panel says what the step does, what it reads and writes, and what it produced in the last run.

| | |
|---|---|
| Source version | `{S.tag}` (parent `{MANIFEST.get('parent', '')}`) |
| Snapshot | `{S.snapshot_date}` |
| Mode | `{MODE}` |
| Mapping source | `{MAPPING_SOURCE}` |
| Assignment source | `{ASSIGNMENT_SOURCE}` |
| TTC cycle | `{TTC}` ({ttc_rng}), lags {LAGS}, poly on `{POLY_COL}` |

### The result in one line
**{fN(TOT_ACC)} accounts** worth **{fB(TOT_BAL)}** roll up into **{len(ab)} subproducts** and **{len(fb)} products**; every product gets one final beta. **{N_HIGH} HIGH** known issues are open (see *Known issues*).

### The chain
1. **01** builds the subproduct of every account and maps it to a product (`mv_accounts`, `mv_account_war`).
2. **02** describes every subproduct (balance, rate spread, history).
3. **03** fits betas per subproduct, so you can judge each one before grouping.
4. **04** assigns every subproduct to a new product.
5. **05** re-aggregates accounts to the new products and fits product betas.
6. **06** recommends a method per product, applies your overrides, and writes the final betas.
""")

SECTIONS["SRC"] = ("Production sources", f"""## Production sources (read only)
| Bank | Table | Grain |
|---|---|---|
| PNFP | `{SRC_T.get('src_pnfp_deposits', '')}` | one row per account per `load_date` |
| SNV | `{SRC_T.get('src_lsnv_nmds', '')}` | one row per account per `EOPDate` |
| both | `{SRC_T.get('src_dim_nmd', '')}` | production mapping, **comparison only** |

**Scope rules**
* PNFP: closed accounts are dropped; internal accounts are kept as *Internal Deposits* when `pnfp_include_internal = Y` (this run: `{S.params.get('pnfp_include_internal', 'Y')}`).
* SNV: subproducts in `snv_excluded_subproducts` are dropped: {', '.join(EXCL) or 'none'}.
* Every month is kept, not only the snapshot, because the betas need history.

**At the snapshot ({S.snapshot_date})**

{bank_tbl}

**Next:** PNFP accounts get a subproduct from the rate-class rule; SNV accounts already carry one.
""")

SECTIONS["CFG"] = ("Study config", f"""## Study config (`config/`)
| File | What it controls |
|---|---|
| `mv_study_config.xlsx` / params | mode, mapping and assignment source, lags, TTC cycle, poly rate, AVG tolerance, thin-history and low-balance thresholds, BDSP accounts, SNV exclusions |
| `mv_study_config.xlsx` / cycles | the TTC, UP and DOWN windows used by the betas |
| `mv_study_config.xlsx` / final_overrides | your per-product method choice and reason (06) |
| `rate_cycles.csv` | month-end fed funds (EFFR, LB_FFR, UP_FFR) and the named cycles |
| `study_params.json` | snapshot date and source tables |

**Settings in this run**
* TTC `{TTC}` = {ttc_rng}; lags {LAGS}; poly on `{POLY_COL}`.
* AVG when linear and poly R2 are within **{TOL:g} pts**.
* UP / DOWN endpoint betas at lag **{UD_LAG}**.
* Thin history below **{MIN_MONTHS}** TTC months; low balance below **{fB(MIN_BAL)}**.

**UP / DOWN cycles**

{ud_tbl}
""")

SECTIONS["REF"] = ("Reference files", f"""## Reference files (`inputs/reference/`)
Copied from the version on every export, so they are the originals the version used.
* `dim_nmd.csv`: subproduct to product mapping. Used by **01** when `mapping_source` is `REFERENCE` (this run: `{MAPPING_SOURCE}`).
* `mv_config.xlsx`: `product_assignment` and `product_names` tabs. Used by **04** when `assignment_source` is `REFERENCE` (this run: `{ASSIGNMENT_SOURCE}`).

To change them, edit the version's files and export again, or write your own rule in 01 Step 5b / 04 Step 2b (the `DERIVED` options).
""")

SECTIONS["S1"] = ("01 PNFP product rule", f"""## 01 Step 2: PNFP rate class and the 33-product rule
Every PNFP account gets its product straight from the source.

| Rate class (first match wins) | Condition | Suffix |
|---|---|---|
| DNC | override rate 787 | `081_00` |
| ZeroRate | override 100 and prime not 100 | `000_00` |
| Indexed | override description WSJ PRIME / Fed Funds Target / Fed Funds Effective / Pinnacle Index, or prime 657 / 659 | `000_01` |
| Standard | service charge AG / EG | `000_00` |
| Standard Plus | override not 0 and not equal to prime | `070_00` |
| Standard | anything else | `000_00` |

Type, group and segment come from the service charge code in `{SRC_T.get('src_product_grouping', '')}`. The product is *Internal Deposits*, *ICS group segment*, *group IOLTA*, *Brokered DDA / MMA*, or *group segment rate label* (no label for public funds, NIB DDA, Savings, Repo and Hybrid; ZeroRate -> Standard; DNC -> Exception; Standard Plus -> Exception for MMA and IB DDA Consumer). Internal accounts get the suffix `_INT`.

**PNFP at the snapshot**

{rc_tbl}

**{fN(plus.products)}** PNFP products, **{fN(plus.accounts)}** accounts, **{fB(plus.balance)}**. Product IDs, codes and short names: `inputs/product_lookup.csv`.

**Checks (01 Step 2):** every subproduct key gives one product, and every product is in the lookup.
""")

SECTIONS["S2"] = ("01 SNV subproducts", f"""## 01 Step 3: SNV subproducts
SNV already carries `SubProductID`, so there is no rate-class rule.
* **BDSP** accounts ({', '.join(BDSP) or 'none'}) map to their own `dim_nmd` rows: **{fN(bdsp.accounts)}** accounts, **{fB(bdsp.balance)}** at the snapshot.
* Excluded subproducts are dropped: {', '.join(EXCL) or 'none'}.
* SNV is joined on bank + subproduct + BDSP flag; its public-funds flag comes from the mapping.
""")

SECTIONS["S3"] = ("01 Mapping", f"""## 01 Step 4-5: the mapping (`inputs/account_mapping_{S.tag}.xlsx`)
**Keys.** One row per key the accounts are joined on, from **every month**:
* PNFP: `bank | subproduct | pf_flag | bdsp_flag`
* SNV: `bank | subproduct | * | bdsp_flag`

**Values.** For each key: account type description, product group, client group, rate class, product, product number. Three sources sit side by side in the workbook: **reference** (`dim_nmd.csv`), **derived** (your SQL in Step 5b), **production** `dim_nmd`. `mapping_source = {MAPPING_SOURCE}` picks what is used.

**Where the values came from**

{vs_tbl}

Keys with no product group are **UNMAPPED**. Keys that only appear in history don't affect the snapshot, but their months show up as UNMAPPED sub_keys in the history.
""")

SECTIONS["S4"] = ("01 Accounts at snapshot", f"""## 01 Step 6: accounts at the snapshot (`mv_accounts`)
Every in-scope account at `{S.snapshot_date}`, joined to the mapping once (duplicate reference keys keep their first row, so nothing is double counted).

`sub_key = bank | subproductid | product`: the unit the betas are fitted on.

| Bank | Mapped | Accounts | Balance | WAR |
|---|---|---|---|---|
""" + "\n".join(f"| {r.legacy_bank} | {'yes' if r.mapped else 'no'} | {fN(r.accounts)} | {fB(r.balance)} | {fR(r.war * 100, 3)}% |" for r in acc.itertuples()) + f"""

**Total:** {fN(TOT_ACC)} accounts, {fB(TOT_BAL)}. Unmapped: {fN(UNM_ACC)} accounts, {fB(UNM_BAL)}.

**Tie-out:** balance by bank vs the version's `nmds_agg`. Any difference is explained in *Known issues* (`VERSION_DOUBLE_COUNT`).

File: `outputs/excel/accounts_{S.tag}.xlsx`.
""")

SECTIONS["S5"] = ("01 Monthly history", f"""## 01 Step 7: monthly history by subproduct (`mv_account_war`)
The same rule and mapping applied to **every month**, aggregated by `sub_key` and month:

`WAR = SUM(rate x balance) / SUM(balance)`

{war_tbl}

**Check:** the snapshot month ties to `mv_accounts` on every sub_key. Each month is also compared with the version's `nmds_agg`.
""")

SECTIONS["S6"] = ("02 Subproduct metrics", f"""## 02: subproduct metrics (`mv_account_metrics`, `inputs/account_metrics_{S.tag}.xlsx`)
One row per sub_key that holds accounts at the snapshot: accounts, balance, WAR, the rate distribution (min, P10 to P90, max, std), the share of balance at a zero rate, the share of its product's balance, and months of history.

* **{len(met)}** sub_keys at the snapshot, **{len(met_bal)}** with a non-zero balance.
* **{len(thin)}** with fewer than {MIN_MONTHS} months of history.

Use it to judge which subproducts behave alike before you group them in 04.
""")

SECTIONS["S7"] = ("03 Subproduct betas", f"""## 03: betas per subproduct (`mv_account_betas`)
The beta engine runs on each sub_key's monthly WAR against fed funds:

| Method | What it is |
|---|---|
| **Linear** `lm_L` | slope of WAR on fed funds over the TTC window, fed funds lagged L months; with R2 |
| **Poly** `poly_L` | cubic fit WAR = b0 + b1 x + b2 x^2 + b3 x^3 on `{POLY_COL}`; beta = derivative; with R2 |
| **Endpoint** | (WAR end - WAR start) / (fed funds end - fed funds start) for each UP / DOWN cycle, giving `avg_up`, `avg_down` at lag {UD_LAG} |

**Recommendation rule:** take the best-R2 linear lag and the best-R2 poly lag. If their R2s are within {TOL:g} pts, use **AVG**. Otherwise use the higher one. If neither fits, **NONE**.

**{len(ab)} sub_keys:**

{ab_tbl}

File: `outputs/excel/account_betas_{S.tag}.xlsx`.
""")

SECTIONS["S8"] = ("04 Product assignment", f"""## 04: subproduct to new product (`mv_product_selection`, `inputs/product_assignment_{S.tag}.xlsx`)
Every sub_key gets a `new_product`, either from the reference `product_assignment` tab or from your rule (Step 2b). `assignment_source = {ASSIGNMENT_SOURCE}`. `product_names` can rename it to a consolidated name, which becomes `mv_product`.

**Mode `{MODE}`:** an unassigned sub_key keeps its current product and is flagged.

{sa_tbl}

**{sel.mv_product.nunique()}** products after assignment.
""")

SECTIONS["S9"] = ("05 Product map", f"""## 05 Step 1: accounts to products (`mv_product_map`)
`mv_accounts` joined to the selection on `sub_key`: every account carries its `mv_product`.

* **{fN(pm.accounts)}** accounts, **{fB(pm.balance)}**, **{fN(pm.sub_keys)}** sub_keys, **{fN(pm.products)}** products.
* **Tie-out:** accounts and balance equal `mv_accounts` (an assert stops the run if not).
* {fN(pm.products)} products hold accounts at the snapshot; {sel.mv_product.nunique()} exist in the history (a product can have history but no balance today).

File: `outputs/excel/product_map_{S.tag}.xlsx` (with the current to new product flow).
""")

SECTIONS["S10"] = ("05 Product history", f"""## 05 Step 2: monthly WAR per product (`mv_product_war`)
The subproduct history re-aggregated to `mv_product` by month, again `WAR = SUM(rate x balance) / SUM(balance)`, so a product's history is balance-weighted across its subproducts.

* **{pw.mv_product.nunique()}** products, **{len(pw):,}** product-months.
* **Check:** the snapshot month ties to the product map.
""")

SECTIONS["S11"] = ("05 Product betas", f"""## 05 Step 3: product betas (`mv_product_betas`)
The same beta engine as 03, now on each product's WAR history: linear and poly at lags {LAGS}, UP / DOWN endpoints, and TTC months.

* **{len(pb)}** products.
* **Engine check:** compared with the version's notebook 03 (`ccalc_beta_results`). Differences are logged as `DIFFERS_FROM_VERSION_03`.

File: `outputs/excel/product_beta_report_{S.tag}.xlsx`.
""")

SECTIONS["S12"] = ("06 Beta review", f"""## 06 Step 1: recommended method per product (`mv_beta_review`)
The 03 rule applied per product: best linear lag vs best poly lag, AVG within {TOL:g} R2 pts. Each product is flagged for thin history (< {MIN_MONTHS} TTC months) and low balance (< {fB(MIN_BAL)}).

{rev_tbl}

The recommendation is written into `config/mv_study_config.xlsx` / `final_overrides`. Overrides you already entered are kept.

File: `outputs/excel/beta_review_{S.tag}.xlsx` (one tab per product with every candidate).
""")

SECTIONS["S13"] = ("06 Overrides", f"""## 06 Step 2: your overrides (`final_overrides`)
For any product, set `override_method`:

| Value | Final beta |
|---|---|
| `LM_LAGn` / `POLY_LAGn` | that fit at that lag |
| `AVG(LM_LAGa,POLY_LAGb)` | mean of the two |
| `MANUAL` + value | your number |
| blank | the recommendation (`final_source = RULE`) |

An override without a reason is flagged (`flag_override_without_reason`).

**This run:** {n_ovr} overrides; final_source {fs}.
""")

SECTIONS["S14"] = ("06 Final betas", f"""## 06 Step 2: final betas (`mv_final_betas`)
One row per product: the recommendation, your override, and the **final method, beta and R2** with its source, plus the study, snapshot and TTC cycle.

**By final method**

{fm_tbl}

**The 15 largest products**

{top_tbl}

Files: `outputs/excel/mv_summary_{S.tag}.xlsx`, `outputs/excel/final_betas_{S.tag}.csv`.
""")

SECTIONS["EX"] = ("Worked example", f"""## Worked example: **{ex_name}**
The largest product with a non-zero beta ({fB(EXP.snapshot_balance)}), followed through every step.

**1. Accounts and subproducts (01, 04).** {len(ex_sel)} sub_keys are assigned to it (largest first):

{ex_sub}

**2. History (05).** {ex_war_txt}.

**3. Fits over `{TTC}` ({ttc_rng}).**

{ex_grid}

**4. Recommendation (06).** Best linear: lag {fL(ex_rev.best_lm_lag)}, beta {fR(ex_rev.best_lm_beta, 3)}, R2 {fR(ex_rev.best_lm_r2, 1)}. Best poly: lag {fL(ex_rev.best_poly_lag)}, beta {fR(ex_rev.best_poly_beta, 3)}, R2 {fR(ex_rev.best_poly_r2, 1)}. So **{ex_rev.recommended_method}**: {ex_rev.recommended_reason}.

**5. Final.** {ex_fb.final_method}, beta **{fR(ex_fb.final_beta, 3)}**, R2 {fR(ex_fb.final_r2, 1)} (source {ex_fb.final_source}). Endpoint averages: up {fR(ex_fb.avg_up, 3)}, down {fR(ex_fb.avg_down, 3)}.
""")

SECTIONS["KI"] = ("Known issues", f"""## Known issues (`outputs/reports/known_issues_{S.tag}.csv`)
Each step logs what it finds as it runs. **{len(ki)}** issues, **{N_HIGH}** HIGH.

**HIGH**

{ki_high_txt}

**All, by step**

{ki_tbl}
""")

SECTIONS["OUT"] = ("Outputs", f"""## Outputs
| Folder | Files |
|---|---|
| `inputs/` | `account_mapping_{S.tag}.xlsx`, `account_metrics_{S.tag}.xlsx`, `product_assignment_{S.tag}.xlsx` |
| `outputs/excel/` | accounts, account_betas, product_map, product_beta_report, beta_review, mv_summary, final_betas.csv |
| `outputs/html/` | `mv_dashboard_{S.tag}.html`, `mv_flow_{S.tag}.html` (this page) |
| `outputs/reports/` | `mv_report_{S.tag}.md`, `mv_flow_{S.tag}.md`, `known_issues_{S.tag}.csv` |

Tables: `mv_accounts`, `mv_account_war`, `mv_account_metrics`, `mv_account_betas`, `mv_product_selection`, `mv_product_map`, `mv_product_war`, `mv_product_betas`, `mv_beta_review`, `mv_final_betas` (all `_{S.tag}` in the sandbox).
""")
print(f"{len(SECTIONS)} sections:", ", ".join(v[0] for v in SECTIONS.values()))

# COMMAND ----------

# DBTITLE 1,3. The Mermaid diagram
def lab(*lines):
    return "<br/>".join(str(x).replace('"', "'") for x in lines)

LABELS = {
    "SRC": lab("Production sources", f"PNFP + SNV, every month"),
    "CFG": lab("Study config", f"{TTC}, lags {','.join(map(str, LAGS))}", f"mode {MODE}"),
    "REF": lab("Reference files", "dim_nmd.csv, mv_config.xlsx"),
    "S1": lab("PNFP rate-class rule", f"{fN(rc.accounts.sum())} accounts"),
    "S2": lab("SNV subproducts", f"BDSP {fN(bdsp.accounts)}, excluded {len(EXCL)}"),
    "S3": lab("Mapping", f"{len(map_used)} keys, {MAPPING_SOURCE}"),
    "S4": lab("Accounts at snapshot", f"{fN(TOT_ACC)} accounts", fB(TOT_BAL)),
    "S5": lab("Monthly history", f"{int(war_s.sub_keys.sum())} sub_keys"),
    "S6": lab("Subproduct metrics", f"{len(met)} sub_keys"),
    "S7": lab("Subproduct betas", f"{len(ab)} sub_keys"),
    "S8": lab("Product assignment", f"{int((sel.assignment_source == 'ASSIGNED').sum())} of {len(sel)} assigned"),
    "S9": lab("Product map", f"{fN(pm.accounts)} accounts", f"{fN(pm.products)} products"),
    "S10": lab("Product history", f"{len(pw):,} product-months"),
    "S11": lab("Product betas", f"{len(pb)} products"),
    "S12": lab("Beta review", "recommended method"),
    "S13": lab("Overrides", f"{n_ovr} overrides"),
    "S14": lab("Final betas", f"{len(fb)} products", fB(fb.snapshot_balance.sum())),
    "EX": lab("Worked example", ex_name),
    "KI": lab("Known issues", f"{len(ki)} logged, {N_HIGH} HIGH"),
    "OUT": lab("Outputs", "workbooks, report, dashboards"),
}
GROUPS = [("INP", "Inputs", ["SRC", "CFG", "REF"]),
          ("N01", "01 Account mapping", ["S1", "S2", "S3", "S4", "S5"]),
          ("N02", "02 Metrics", ["S6"]),
          ("N03", "03 Subproduct betas", ["S7"]),
          ("N04", "04 Assignment", ["S8"]),
          ("N05", "05 Product betas", ["S9", "S10", "S11"]),
          ("N06", "06 Final betas", ["S12", "S13", "S14"]),
          ("NOUT", "Results", ["EX", "OUT", "KI"])]
EDGES = [("SRC", "S1", "PNFP"), ("SRC", "S2", "SNV"), ("S1", "S3", ""), ("S2", "S3", ""), ("REF", "S3", "dim_nmd.csv"),
         ("S3", "S4", "join once per key"), ("S3", "S5", "all months"), ("S4", "S6", ""), ("S5", "S6", ""),
         ("S5", "S7", "WAR by sub_key"), ("CFG", "S7", "cycles + fed funds"), ("S6", "S8", ""), ("S7", "S8", ""),
         ("REF", "S8", "product_assignment"), ("S8", "S9", "sub_key to mv_product"), ("S4", "S9", ""),
         ("S5", "S10", ""), ("S8", "S10", ""), ("S10", "S11", "WAR by product"), ("CFG", "S11", ""),
         ("S11", "S12", ""), ("S12", "S13", "recommended"), ("CFG", "S13", "final_overrides"), ("S13", "S14", ""),
         ("S14", "EX", ""), ("S14", "OUT", "")]
ISSUE_EDGES = ["S3", "S4", "S8", "S11"]
CLASS = {"INP": "inp", "N01": "s01", "N02": "s02", "N03": "s03", "N04": "s04", "N05": "s05", "N06": "s06", "NOUT": "res"}

mm = ["flowchart TD"]
for gid, title, ids in GROUPS:
    mm.append(f'  subgraph {gid}["{title}"]')
    shape = {"SRC": ('[("', '")]'), "KI": ('{{"', '"}}'), "EX": ('(["', '"])')}
    for i in ids:
        a, b = shape.get(i, ('["', '"]'))
        mm.append(f"    {i}{a}{LABELS[i]}{b}")
    mm.append("  end")
for a, b, t in EDGES:
    mm.append(f"  {a} -->|{t}| {b}" if t else f"  {a} --> {b}")
for a in ISSUE_EDGES:
    mm.append(f"  {a} -.-> KI")
mm += ["  classDef inp fill:#f1f5f9,stroke:#475569,color:#0f172a",
       "  classDef s01 fill:#dbeafe,stroke:#1d4ed8,color:#1e3a8a",
       "  classDef s02 fill:#e0e7ff,stroke:#4338ca,color:#312e81",
       "  classDef s03 fill:#ede9fe,stroke:#6d28d9,color:#4c1d95",
       "  classDef s04 fill:#fce7f3,stroke:#be185d,color:#831843",
       "  classDef s05 fill:#ffedd5,stroke:#c2410c,color:#7c2d12",
       "  classDef s06 fill:#dcfce7,stroke:#15803d,color:#14532d",
       "  classDef res fill:#fef9c3,stroke:#a16207,color:#713f12",
       "  classDef iss fill:#fee2e2,stroke:#b91c1c,color:#7f1d1d"]
for gid, _, ids in GROUPS:
    mm.append(f"  class {','.join(i for i in ids if i != 'KI')} {CLASS[gid]}")
mm.append("  class KI iss")
MERMAID = "\n".join(mm)
ORDER = ["OV", "SRC", "CFG", "REF", "S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9", "S10", "S11", "S12", "S13", "S14", "EX", "KI", "OUT"]
assert set(ORDER) == set(SECTIONS), set(ORDER) ^ set(SECTIONS)
print(MERMAID)

# COMMAND ----------

# DBTITLE 1,4. Write the HTML, Markdown, and Deposit Study Overview
STEP_NO = {"S1": "01", "S2": "01", "S3": "01", "S4": "01", "S5": "01", "S6": "02", "S7": "03", "S8": "04",
           "S9": "05", "S10": "05", "S11": "05", "S12": "06", "S13": "06", "S14": "06"}
payload = {
    "title": f"MV study flow | {MV_ROOT.name}",
    "subtitle": f"source {S.tag} | snapshot {S.snapshot_date} | mode {MODE} | cycle {TTC} ({ttc_rng}) | built {pd.Timestamp.now():%Y-%m-%d %H:%M}",
    "kpis": [["Accounts", fN(TOT_ACC)], ["Balance", fB(TOT_BAL)], ["Subproducts", str(len(ab))], ["Products", str(len(fb))],
             ["Assigned", f"{int((sel.assignment_source == 'ASSIGNED').sum())} / {len(sel)}"], ["HIGH issues", str(N_HIGH)]],
    "mermaid": MERMAID,
    "order": ORDER,
    "nodes": [{"id": k, "title": SECTIONS[k][0], "step": STEP_NO.get(k, ""), "md": SECTIONS[k][1]} for k in ORDER],
}

HTML = r"""<!doctype html>
<html><head><meta charset="utf-8"><title>MV study flow</title>
<style>
  body{font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:0;background:#f8fafc;color:#0f172a}
  header{padding:14px 20px;background:#0f172a;color:#fff}
  header h1{margin:0;font-size:20px} header .sub{opacity:.75;font-size:12px;margin-top:4px}
  .kpis{display:flex;gap:10px;flex-wrap:wrap;padding:12px 20px}
  .kpi{background:#fff;border:1px solid #e2e8f0;border-radius:10px;padding:8px 14px;min-width:110px}
  .kpi .v{font-size:18px;font-weight:700} .kpi .k{font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:.04em}
  .chips{display:flex;flex-wrap:wrap;gap:6px;padding:0 20px 10px}
  .chip{border:1px solid #cbd5e1;background:#fff;border-radius:999px;padding:5px 11px;font-size:12px;cursor:pointer}
  .chip .n{color:#64748b;margin-right:4px;font-weight:600}
  .chip:hover{background:#eef2ff} .chip.active{background:#1d4ed8;color:#fff;border-color:#1d4ed8} .chip.active .n{color:#bfdbfe}
  .wrap{display:grid;grid-template-columns:minmax(0,1.25fr) minmax(0,1fr);gap:14px;padding:0 20px 20px}
  .card{background:#fff;border:1px solid #e2e8f0;border-radius:12px;height:820px;overflow:auto}
  #diagram{padding:10px;text-align:center} #diagram svg{max-width:100%;height:auto}
  #diagram g.node{cursor:pointer} #diagram g.node:hover{filter:brightness(.95)}
  #diagram g.node.active rect,#diagram g.node.active polygon,#diagram g.node.active path,#diagram g.node.active circle{stroke:#e11d48 !important;stroke-width:4px !important}
  .nav{position:sticky;top:0;background:#fff;border-bottom:1px solid #e2e8f0;padding:10px 14px;display:flex;align-items:center;gap:8px;z-index:2}
  .nav button{border:1px solid #cbd5e1;background:#f8fafc;border-radius:8px;padding:6px 12px;cursor:pointer;font-size:13px}
  .nav button:hover{background:#e2e8f0} .nav .pos{flex:1;font-size:13px;color:#334155;font-weight:600}
  #panel{padding:4px 18px 24px;font-size:14px;line-height:1.5}
  #panel h2{font-size:18px;margin:14px 0 8px;color:#1e3a8a} #panel h3{font-size:15px}
  #panel table{border-collapse:collapse;margin:8px 0;font-size:12.5px;width:100%}
  #panel th,#panel td{border:1px solid #e2e8f0;padding:4px 8px;text-align:left;vertical-align:top}
  #panel th{background:#f1f5f9} #panel code{background:#f1f5f9;padding:1px 4px;border-radius:4px;font-size:12px}
  #panel hr{border:none;border-top:2px dashed #cbd5e1;margin:22px 0}
  .hint{font-size:12px;color:#64748b;padding:0 20px 8px}
</style></head>
<body>
<header><h1 id="t"></h1><div class="sub" id="st"></div></header>
<div class="kpis" id="kpis"></div>
<div class="hint">Click a box in the diagram or a button below to read that step. Prev / Next walks the study in run order; Read all shows every step in one page.</div>
<div class="chips" id="chips"></div>
<div class="wrap">
  <div class="card"><div id="diagram">Loading the diagram...</div></div>
  <div class="card">
    <div class="nav"><button id="prev">&#9664; Prev</button><span class="pos" id="pos"></span>
      <button id="all">Read all</button><button id="next">Next &#9654;</button></div>
    <div id="panel"></div>
  </div>
</div>
<script src="https://cdn.jsdelivr.net/npm/marked@12/marked.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
<script>
const D = __DATA__;
const byId = Object.fromEntries(D.nodes.map(n => [n.id, n]));
let cur = 0;
const esc = s => s.replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
const md = t => (window.marked ? marked.parse(t) : '<pre style="white-space:pre-wrap">' + esc(t) + '</pre>');
const nid = g => { const m = (g.id || '').match(/^flowchart-(.+?)-\d+$/); return m ? m[1] : null; };
document.getElementById('t').textContent = D.title;
document.getElementById('st').textContent = D.subtitle;
document.getElementById('kpis').innerHTML = D.kpis.map(k => `<div class="kpi"><div class="v">${esc(k[1])}</div><div class="k">${esc(k[0])}</div></div>`).join('');
document.getElementById('chips').innerHTML = D.order.map(id => {
  const n = byId[id]; return `<button class="chip" data-id="${id}">${n.step ? '<span class="n">' + n.step + '</span>' : ''}${esc(n.title)}</button>`; }).join('');
document.querySelectorAll('.chip').forEach(c => c.addEventListener('click', () => show(c.dataset.id)));
function show(id) {
  const i = D.order.indexOf(id); if (i >= 0) cur = i;
  const n = byId[id];
  document.getElementById('panel').innerHTML = md(n.md);
  document.getElementById('pos').textContent = (cur + 1) + ' / ' + D.order.length + '  ' + (n.step ? n.step + ' | ' : '') + n.title;
  document.querySelectorAll('#diagram g.node').forEach(g => g.classList.toggle('active', nid(g) === id));
  document.querySelectorAll('.chip').forEach(c => c.classList.toggle('active', c.dataset.id === id));
  document.getElementById('panel').parentElement.scrollTop = 0;
}
document.getElementById('prev').onclick = () => show(D.order[(cur - 1 + D.order.length) % D.order.length]);
document.getElementById('next').onclick = () => show(D.order[(cur + 1) % D.order.length]);
document.getElementById('all').onclick = () => {
  document.getElementById('panel').innerHTML = D.order.map(id => md(byId[id].md)).join('<hr>');
  document.getElementById('pos').textContent = 'All steps';
  document.querySelectorAll('.chip').forEach(c => c.classList.remove('active'));
  document.querySelectorAll('#diagram g.node').forEach(g => g.classList.remove('active'));
};
(async () => {
  const el = document.getElementById('diagram');
  try {
    mermaid.initialize({startOnLoad: false, securityLevel: 'loose', theme: 'base', flowchart: {htmlLabels: true, curve: 'basis', nodeSpacing: 28, rankSpacing: 42}});
    const r = await mermaid.render('flowsvg', D.mermaid);
    el.innerHTML = r.svg;
    el.querySelectorAll('g.node').forEach(g => { const id = nid(g); if (byId[id]) g.addEventListener('click', () => show(id)); });
  } catch (e) {
    el.innerHTML = '<p>The diagram needs Mermaid from cdn.jsdelivr.net. The buttons and text still work.</p><pre style="text-align:left;white-space:pre-wrap">' + esc(D.mermaid) + '</pre>';
  }
  show(D.order[0]);
})();
</script>
</body></html>"""

page = HTML.replace("__DATA__", json.dumps(payload).replace("</", "<\\/"))
FLOW_HTML = out("html", "mv_flow", "html")
FLOW_HTML.write_text(page, encoding="utf-8")

# ---- Deposit Study Overview: combines known_issues + study report ----
overview_parts = []
overview_parts.append(f"# Deposit Study Overview | {S.tag}")
overview_parts.append(f"")
overview_parts.append(f"Snapshot **{S.snapshot_date}** | mode **{MODE}** | mapping **{MAPPING_SOURCE}** | TTC cycle **{TTC}** | built {pd.Timestamp.now():%Y-%m-%d %H:%M}")
overview_parts.append(f"")
overview_parts.append(f"---")
overview_parts.append(f"")

# Part 1: Study summary (from the flow sections)
overview_parts.append(f"## Study Summary")
overview_parts.append(f"")
overview_parts.append(f"| Metric | Value |")
overview_parts.append(f"|---|---|")
overview_parts.append(f"| Accounts | {fN(TOT_ACC)} |")
overview_parts.append(f"| Balance | {fB(TOT_BAL)} |")
overview_parts.append(f"| Subproducts | {len(ab)} |")
overview_parts.append(f"| Products | {len(fb)} |")
overview_parts.append(f"| Assigned | {int((sel.assignment_source == 'ASSIGNED').sum())} / {len(sel)} |")
overview_parts.append(f"| HIGH issues | {N_HIGH} |")
overview_parts.append(f"")

# Part 2: Top products
overview_parts.append(f"## Top 15 Products by Balance")
overview_parts.append(f"")
overview_parts.append(top_tbl)
overview_parts.append(f"")

# Part 3: Method summary
overview_parts.append(f"## Final Method Distribution")
overview_parts.append(f"")
overview_parts.append(fm_tbl)
overview_parts.append(f"")

# Part 4: Known Issues
overview_parts.append(f"---")
overview_parts.append(f"")
overview_parts.append(f"## Known Issues ({len(ki)} logged, {N_HIGH} HIGH)")
overview_parts.append(f"")
if len(ki):
    overview_parts.append(f"### HIGH priority")
    overview_parts.append(f"")
    overview_parts.append(ki_high_txt)
    overview_parts.append(f"")
    overview_parts.append(f"### All issues by step")
    overview_parts.append(f"")
    overview_parts.append(ki_tbl)
else:
    overview_parts.append(f"No known issues logged.")
overview_parts.append(f"")

# Part 5: Worked example
overview_parts.append(f"---")
overview_parts.append(f"")
overview_parts.append(f"## Worked Example: {ex_name}")
overview_parts.append(f"")
overview_parts.append(SECTIONS["EX"][1].strip())
overview_parts.append(f"")

OVERVIEW_MD = out("reports", "deposit_study_overview", "md")
OVERVIEW_MD.write_text("\n".join(overview_parts), encoding="utf-8")
print("->", FLOW_HTML, f"({FLOW_HTML.stat().st_size / 1024:,.0f} KB)")
print("->", OVERVIEW_MD)
displayHTML(page)

# COMMAND ----------

