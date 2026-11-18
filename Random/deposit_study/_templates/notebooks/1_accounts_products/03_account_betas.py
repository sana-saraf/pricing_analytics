# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 03 | Account (subproduct) betas
# MAGIC
# MAGIC **What this builds.** For every `sub_key`, from its monthly WAR and month-end fed funds:
# MAGIC
# MAGIC * **TTC linear** at lags 0 to 3. The slope of WAR on fed funds over the `ttc_cycle` window, plus R2.
# MAGIC * **TTC poly** at lags 0 to 3. A cubic fit of WAR on fed funds (`poly_rate_col`); the beta is the derivative. Plus R2.
# MAGIC * **Up / down endpoint betas.** For each cycle tagged UP or DOWN in the config, (WAR end - WAR start) / (fed funds end - fed funds start) at every lag. These are then averaged across the UP cycles and across the DOWN cycles.
# MAGIC * **Recommended method.** Compare the best-R2 linear lag with the best-R2 poly lag. If the two R2s are within `avg_r2_tolerance` points, recommend the average of the two betas (AVG); otherwise recommend the one with the higher R2.
# MAGIC
# MAGIC Writes the table `mv_account_betas` and `outputs/excel/account_betas_<tag>.xlsx`. These betas feed the product assignment (04), which shows them next to every subproduct.
# MAGIC
# MAGIC **Run after:** 01 and 02.

# COMMAND ----------

# DBTITLE 1,Install openpyxl
# MAGIC %pip install -q openpyxl

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ../00_setup

# COMMAND ----------

# DBTITLE 1,Step 1 header
# MAGIC %md
# MAGIC ## Step 1. Inputs: monthly WAR, fed funds, cycles
# MAGIC WAR by `sub_key` and month comes from `mv_account_war` (01). Fed funds is the month-end series from production `fed_funds_matrix`. The cycles come from `config/rate_cycles.csv`, and the UP / DOWN tags come from the `cycles` tab of `config/mv_study_config.xlsx`.

# COMMAND ----------

# DBTITLE 1,1. Load WAR, show the cycles
assert S.table_exists("mv_account_war") and S.table_exists("mv_account_metrics"), "run 01 and 02 first"
war_long = spark.table(S.table("mv_account_war")).orderBy("sub_key", "eopdate").toPandas()
metrics = spark.table(S.table("mv_account_metrics")).toPandas()

_f = lambda d: float(EFFR.loc[EFFR["effective_date"] == d, "UP_FFR"].iloc[0])
cycle_view = pd.DataFrame([dict(name=n, start=f"{d[0]:%Y-%m}", end=f"{d[-1]:%Y-%m}", months=len(d),
                                fed_funds_start=_f(d[0]), fed_funds_end=_f(d[-1]),
                                use=("TTC (linear and poly)" if n == TTC else
                                     ", ".join(UPDOWN.loc[UPDOWN.name == n, "direction"]) or "not used"))
                           for n, d in NAMED.items()])
print(f"{war_long.sub_key.nunique():,} sub_keys | {war_long.eopdate.nunique()} months of WAR")
display(cycle_view)

# COMMAND ----------

# DBTITLE 1,Step 2 header
# MAGIC %md
# MAGIC ## Step 2. The beta functions
# MAGIC The exact code used (from `_shared/deposit_utils/betas.py`, the same code notebook 03 of the version uses), printed here so the method can be read in full. `beta_grid` and `recommend` are in `00_setup`.

# COMMAND ----------

# DBTITLE 1,2. Print the beta functions
import inspect
for fn in (B.lm_beta_lag, B.poly_beta_lag, B.endpoint_beta_lag):
    print("#" * 100)
    print(inspect.getsource(fn))

# COMMAND ----------

# DBTITLE 1,Step 3 header
# MAGIC %md
# MAGIC ## Step 3. Betas by subproduct and the recommended method

# COMMAND ----------

# DBTITLE 1,3. mv_account_betas
sub_pivot = war_pivot(war_long, "sub_key")
sub_grid = recommend(beta_grid(sub_pivot)).rename(columns={"EMP_NAME": "sub_key"})
keys = (war_long.sort_values("eopdate").groupby("sub_key", as_index=False)
        .agg(legacy_bank=("legacy_bank", "last"), subproductid=("subproductid", "last"), product=("product", "last"),
             product_group=("product_group", "last"), acct_typ_desc=("acct_typ_desc", "last")))
snapm = metrics[["sub_key", "accounts", "eopbook", "war"]].rename(columns={"accounts": "snapshot_accounts", "eopbook": "snapshot_balance", "war": "snapshot_war"})
acct_betas = keys.merge(snapm, on="sub_key", how="left").merge(sub_grid, on="sub_key", how="left")
acct_betas[["snapshot_accounts", "snapshot_balance"]] = acct_betas[["snapshot_accounts", "snapshot_balance"]].fillna(0)
acct_betas["flag_thin_history"] = acct_betas["months_ttc"] < MIN_MONTHS
acct_betas["flag_low_balance"] = acct_betas["snapshot_balance"] < MIN_BAL
acct_betas = acct_betas.sort_values(["legacy_bank", "product", "snapshot_balance"], ascending=[True, True, False])
save_table(acct_betas, "mv_account_betas")
assert acct_betas.sub_key.is_unique and len(acct_betas) == war_long.sub_key.nunique()

meth = acct_betas.recommended_method.str.extract(r"^(\w+?)(?:_LAG|\(|$)")[0]
print(f"{len(acct_betas):,} sub_keys | recommended: " + ", ".join(f"{k} {v}" for k, v in meth.value_counts().items()))
write_xlsx(out("excel", "account_betas", "xlsx"), {"Subproduct betas": acct_betas, "Cycles": cycle_view, "Settings": SETTINGS})
print("->", out("excel", "account_betas", "xlsx"))
display(acct_betas[["sub_key", "snapshot_balance", "months_ttc"] + [f"lm_{L}" for L in LAGS] + [f"poly_{L}" for L in LAGS]
                   + ["avg_up", "avg_down", "recommended_method", "recommended_beta", "recommended_r2"]].head(50))

# COMMAND ----------

# DBTITLE 1,3. Known issues in the subproduct betas
rows = []
none = acct_betas[acct_betas.recommended_method == "NONE"]
if len(none):
    rows.append(("NO_FIT", "HIGH" if none.snapshot_balance.sum() else "INFO", f"{len(none)} sub_keys",
                 "no linear or poly fit in the TTC window: " + ", ".join(none.sub_key.head(15)) + (" ..." if len(none) > 15 else ""),
                 int(none.snapshot_accounts.sum()), float(none.snapshot_balance.sum())))
r2cols = [f"lm_r2_{L}" for L in LAGS] + [f"poly_r2_{L}" for L in LAGS]
neg = acct_betas[(acct_betas["best_lm_r2"] < 0) | (acct_betas["best_poly_r2"] < 0)]
if len(neg):
    rows.append(("NEGATIVE_R2", "MEDIUM", f"{len(neg)} sub_keys",
                 f"best linear or poly R2 below 0 (min {np.nanmin(neg[['best_lm_r2', 'best_poly_r2']].values):,.1f}); "
                 "usually a short or flat series in the TTC window: " + ", ".join(neg.sub_key.head(15)) + (" ..." if len(neg) > 15 else ""),
                 int(neg.snapshot_accounts.sum()), float(neg.snapshot_balance.sum())))
thin = acct_betas[acct_betas.flag_thin_history & (acct_betas.snapshot_balance > 0)]
if len(thin):
    rows.append(("THIN_TTC_HISTORY", "MEDIUM", f"{len(thin)} sub_keys", f"fewer than {MIN_MONTHS} WAR months in the TTC window",
                 int(thin.snapshot_accounts.sum()), float(thin.snapshot_balance.sum())))
issues_betas = log_issues("03 account betas", rows)
display(acct_betas.loc[acct_betas.sub_key.isin(set(none.sub_key) | set(neg.sub_key)),
                       ["sub_key", "snapshot_balance", "months_ttc", "best_lm_r2", "best_poly_r2", "recommended_method", "recommended_reason"]])