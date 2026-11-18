# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 05 | Product map and product betas
# MAGIC
# MAGIC **What this builds.**
# MAGIC 1. `mv_product_map`: every snapshot account with its new product (from `inputs/product_assignment_<tag>.xlsx` via `mv_product_selection`). It ties to `mv_accounts`.
# MAGIC 2. `mv_product_war`: the monthly balance and WAR by new product. Each month is the sum of its subproducts' rate x balance, divided by the sum of their balances.
# MAGIC 3. `mv_product_betas`: the same beta grid as 03 (TTC linear and poly at lags 0 to 3 with R2, up / down endpoint betas, the averages), at the new-product level.
# MAGIC
# MAGIC It also checks the engine against the version's notebook 03 for every product whose makeup is identical to a current product.
# MAGIC
# MAGIC Writes `outputs/excel/product_map_<tag>.xlsx` and `outputs/excel/product_beta_report_<tag>.xlsx`.
# MAGIC
# MAGIC **Run after:** 04.

# COMMAND ----------

# DBTITLE 1,Install openpyxl
# MAGIC %pip install -q openpyxl

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ../00_setup

# COMMAND ----------

# DBTITLE 1,Step 1 header
# MAGIC %md
# MAGIC ## Step 1. Product map (accounts -> new product)

# COMMAND ----------

# DBTITLE 1,1. mv_product_map, summary and tie-out
assert S.table_exists("mv_product_selection"), "run 04 first"
sel = spark.table(S.table("mv_product_selection")).toPandas()
sql("""
CREATE OR REPLACE TABLE {{mv_product_map}} AS
SELECT a.*, s.new_product, s.mv_product, s.assignment_source
FROM {{mv_accounts}} a
LEFT JOIN {{mv_product_selection}} s ON a.sub_key = s.sub_key
""")
prod_summary = sqlq("""
SELECT COALESCE(mv_product, 'UNASSIGNED') AS mv_product,
       CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(legacy_bank))) AS banks,
       COUNT(DISTINCT sub_key) AS subproducts, COUNT(*) AS accounts, SUM(eopbook) AS eopbook,
       TRY_DIVIDE(SUM(sumprod_eoprate), SUM(eopbook)) AS war,
       CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(COALESCE(product, 'UNMAPPED')))) AS current_products,
       CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(assignment_source))) AS assignment_source
FROM {{mv_product_map}} GROUP BY 1 ORDER BY eopbook DESC
""").toPandas()
flow = sqlq("""
SELECT legacy_bank, COALESCE(product, 'UNMAPPED') AS current_product, COALESCE(mv_product, 'UNASSIGNED') AS mv_product, assignment_source,
       COUNT(DISTINCT sub_key) AS subproducts, COUNT(*) AS accounts, SUM(eopbook) AS eopbook
FROM {{mv_product_map}} GROUP BY ALL ORDER BY legacy_bank, current_product, eopbook DESC
""").toPandas()
tot_acc = spark.table(S.table("mv_accounts")).agg(F.count("*").alias("n"), F.sum("eopbook").alias("b")).first()
assert prod_summary.accounts.sum() == tot_acc.n and abs(prod_summary.eopbook.sum() - tot_acc.b) < 1, "product map does not tie to mv_accounts"
print(f"Ties to mv_accounts: {tot_acc.n:,} accounts, ${tot_acc.b:,.2f} | {len(prod_summary)} products")
# product_map data is now included in Beta_Report (08) — no standalone product_map.xlsx
print(f"Product map data -> Beta_Report (08)")
display(prod_summary)

# COMMAND ----------

# DBTITLE 1,Step 2 header
# MAGIC %md
# MAGIC ## Step 2. Monthly WAR by new product (`mv_product_war`)
# MAGIC The subproduct history (`mv_account_war`) summed by `mv_product`: WAR = sum(rate x balance) / sum(balance) over every subproduct assigned to the product in that month.

# COMMAND ----------

# DBTITLE 1,2. mv_product_war
sql("""
CREATE OR REPLACE TABLE {{mv_product_war}} AS
SELECT s.mv_product, w.eopdate,
       COUNT(DISTINCT w.sub_key) AS subproducts, SUM(w.eopbook) AS eopbook, SUM(w.avgbook) AS avgbook,
       SUM(w.sumprod_eoprate) AS sumprod_eoprate,
       CASE WHEN SUM(w.eopbook) > 0 THEN SUM(w.sumprod_eoprate) / SUM(w.eopbook) END AS war
FROM {{mv_account_war}} w
JOIN {{mv_product_selection}} s ON w.sub_key = s.sub_key
WHERE s.mv_product IS NOT NULL
GROUP BY s.mv_product, w.eopdate
""")
prod_war = spark.table(S.table("mv_product_war")).orderBy("mv_product", "eopdate").toPandas()
snap = prod_war[pd.to_datetime(prod_war.eopdate) == pd.Timestamp(S.snapshot_date)].set_index("mv_product").eopbook
ps = prod_summary.set_index("mv_product").eopbook
assert (snap.reindex(ps.index).fillna(0) - ps).abs().max() < 1, "product WAR at the snapshot does not tie to the product map"
print(f"{prod_war.mv_product.nunique()} products x {prod_war.eopdate.nunique()} months; the snapshot ties to the product map")

# COMMAND ----------

# DBTITLE 1,Step 3 header
# MAGIC %md
# MAGIC ## Step 3. Product betas (`mv_product_betas`) and the product beta report

# COMMAND ----------

# DBTITLE 1,3. mv_product_betas and the report
prod_grid = beta_grid(war_pivot(prod_war, "mv_product")).rename(columns={"EMP_NAME": "mv_product"})
prod_betas = (prod_summary[prod_summary.mv_product != "UNASSIGNED"][["mv_product", "banks", "subproducts", "accounts", "eopbook", "war", "current_products"]]
              .rename(columns={"eopbook": "snapshot_balance", "war": "snapshot_war"}).merge(prod_grid, on="mv_product", how="right"))
prod_betas["snapshot_balance"] = prod_betas["snapshot_balance"].fillna(0)
save_table(prod_betas, "mv_product_betas")

rep_cols = (["mv_product", "snapshot_balance", "snapshot_war", "months_ttc"]
            + [c for L in LAGS for c in (f"lm_{L}", f"lm_r2_{L}")] + [c for L in LAGS for c in (f"poly_{L}", f"poly_r2_{L}")]
            + [f"avg_up_{L}" for L in LAGS] + [f"avg_down_{L}" for L in LAGS])
report = prod_betas[rep_cols].sort_values("snapshot_balance", ascending=False)
ud = prod_betas[["mv_product"] + [c for d in UD_COLS for L in LAGS for c in UD_COLS[d][L]]]
# product_beta_report data is now included in Beta_Report (08) — no standalone product_beta_report.xlsx
print(f"{len(prod_betas)} products | product beta data -> Beta_Report (08)")
display(report)

# COMMAND ----------

# DBTITLE 1,3b. Engine check against the version's notebook 03
# A new product made of exactly the same subproducts as a current product must get the same TTC linear / poly and endpoint
# betas as the version's ccalc_beta_results. Differences mean the inputs diverged (for example the duplicated dim_nmd key).
_code = re.sub(r"[ ]", "_", TTC).replace("-", "")
_pb = "pb" if POLY_COL == "UP_FFR" else "pbe"
engine_check = pd.DataFrame()
if S.table_exists("ccalc_beta_results"):
    cc = spark.table(S.table("ccalc_beta_results")).toPandas()
    same = sel.groupby("mv_product").current_product.agg(lambda s: set(s)).reset_index()
    full = sel.groupby("current_product").mv_product.agg(lambda s: set(s.dropna())).to_dict()
    same = same[same.apply(lambda r: r.current_product == {r.mv_product} and full.get(r.mv_product) == {r.mv_product}, axis=1)]
    chk = prod_betas[prod_betas.mv_product.isin(same.mv_product)].merge(cc, left_on="mv_product", right_on="EMP_NAME", how="inner")
    pairs = []
    for L in LAGS:
        pairs += [(f"lm_{L}", f"lm_{L}_{_code}"), (f"poly_{L}", f"{_pb}_{L}_{_code}")]
        for _, c in UPDOWN.iterrows():
            pairs.append((f"{c['direction'].lower()}_{re.sub(r'[^A-Za-z0-9_]', '', c['short_code'])}_{L}",
                          f"ed_{L}_{c['name'].replace(' ', '_').replace('-', '')}"))
    res = []
    for mine, theirs in pairs:
        if mine in chk and theirs in chk:
            a, b = chk[mine], chk[theirs]
            both = a.notna() & b.notna()
            diff = (a[both] - b[both]).abs()
            res.append(dict(mv_column=mine, column_03=theirs, compared=int(both.sum()),
                            null_mismatch=int((a.isna() != b.isna()).sum()),
                            max_abs_diff=float(diff.max()) if both.any() else 0.0,
                            products_off=", ".join(chk.loc[diff[diff > 0.011].index, "mv_product"])))
    engine_check = pd.DataFrame(res)
    print(f"Compared {len(chk)} products with the version's ccalc_beta_results (TTC {TTC})")
    display(engine_check)
else:
    print("The version has no ccalc_beta_results; skipped")

# COMMAND ----------

# DBTITLE 1,3. Known issues in the product betas
rows = []
if len(engine_check):
    off = engine_check[(engine_check.max_abs_diff > 0.011) | (engine_check.null_mismatch > 0)]
    if len(off):
        prods = sorted(set(", ".join(off.products_off).split(", ")) - {""})
        rows.append(("DIFFERS_FROM_VERSION_03", "MEDIUM", f"{len(off)} beta columns",
                     f"products: {', '.join(prods)}; max diff {off.max_abs_diff.max():.3f}. Expected where the version's nmds_agg double-counts "
                     "a duplicated dim_nmd key (see 01) or where 03 is older than nmds_agg", "", ""))
mix = sel[sel.flag_unassigned.astype(str).str.lower() == "true"].groupby("mv_product").snapshot_balance.apply(lambda s: s.astype(float).sum())
if len(mix):
    rows.append(("PRODUCT_HOLDS_UNASSIGNED", "HIGH" if MODE == "FINAL" else "INFO", f"{len(mix)} products",
                 "products that contain unassigned subproducts (kept on their current product)", "", float(mix.sum())))
issues_products = log_issues("05 product betas", rows)