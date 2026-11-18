# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 04 | Product assignment: subproduct -> new product
# MAGIC
# MAGIC **What this builds.** The new (future-state) product for every `sub_key`. It writes the input file **`inputs/product_assignment_<tag>.xlsx`** and the table `mv_product_selection`.
# MAGIC
# MAGIC | Step | What it does |
# MAGIC |---|---|
# MAGIC | 1 | Lists every subproduct to assign, with its balance, history and subproduct betas (02 and 03) |
# MAGIC | 2a | **Reference:** your assignments in the version's `mv_config.xlsx` (copied to `inputs/reference/`) |
# MAGIC | 2b | **Derived:** your SQL rule (edit the cell) |
# MAGIC | 2c | Picks the assignment by `assignment_source` in `config/mv_study_config.xlsx`: `REFERENCE`, `DERIVED` or `DERIVED_ELSE_REFERENCE`. Then it applies the consolidated names and writes the file |
# MAGIC
# MAGIC **Final mode (`mode = FINAL`).** Every subproduct still runs through to the final betas. Any subproduct without a new product stays on its **current product**, with `assignment_source = UNASSIGNED` and `flag_unassigned = True`. It is listed as a known issue, and every product it lands in is flagged in 06.
# MAGIC
# MAGIC **Run after:** 03.

# COMMAND ----------

# DBTITLE 1,Install openpyxl
# MAGIC %pip install -q openpyxl

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ../00_setup

# COMMAND ----------

# DBTITLE 1,Step 1 header
# MAGIC %md
# MAGIC ## Step 1. The subproducts to assign
# MAGIC One row per `sub_key` in the history (the betas exist for all of them), largest balance first within each current product. The temp view `subproducts_to_assign` is what Step 2b can use.

# COMMAND ----------

# DBTITLE 1,1. subproducts_to_assign
assert S.table_exists("mv_account_betas"), "run 03 first"
acct_betas = spark.table(S.table("mv_account_betas")).toPandas()
PA_COLS = ["sub_key", "legacy_bank", "subproductid", "current_product", "product_group", "acct_typ_desc",
           "snapshot_balance", "snapshot_accounts", "months_ttc", "sub_recommended_method", "sub_recommended_beta", "sub_recommended_r2"]
base = acct_betas.rename(columns={"product": "current_product", "recommended_method": "sub_recommended_method",
                                  "recommended_beta": "sub_recommended_beta", "recommended_r2": "sub_recommended_r2"})[PA_COLS]
base = base.sort_values(["legacy_bank", "current_product", "snapshot_balance"], ascending=[True, True, False]).reset_index(drop=True)
to_view(base, "subproducts_to_assign")
print(f"{len(base):,} subproducts | {base.current_product.nunique()} current products | ${base.snapshot_balance.astype(float).sum():,.0f}")
display(base.head(50))

# COMMAND ----------

# DBTITLE 1,Step 2a header
# MAGIC %md
# MAGIC ## Step 2a. Reference: your original assignments
# MAGIC `product_assignment` and `product_names` tabs of `inputs/reference/mv_config.xlsx`. This is a copy of the version's `inputs/mv/mv_config.xlsx`, refreshed on every export. To change the originals, edit the version's workbook and export again, or write the rule in Step 2b.

# COMMAND ----------

# DBTITLE 1,2a. Reference assignments
REF_CFG = REFERENCE / "mv_config.xlsx"
ref_pa = read_tab(REF_CFG, "product_assignment") if REF_CFG.exists() else pd.DataFrame(columns=["sub_key", "new_product", "notes"])
ref_names = read_tab(REF_CFG, "product_names") if REF_CFG.exists() else pd.DataFrame(columns=["new_product", "consolidated_name", "notes"])
ref_pa = ref_pa[ref_pa.sub_key.astype(str).str.strip() != ""][["sub_key", "new_product", "notes"]].copy()
ref_pa["new_product"] = ref_pa["new_product"].astype(str).str.strip()
ref_gone = ref_pa[~ref_pa.sub_key.isin(base.sub_key) & (ref_pa.new_product != "")]
print(f"Reference: {int((ref_pa.new_product != '').sum())} of {len(ref_pa)} rows have a new_product | "
      f"{len(ref_names)} product_names rows | {len(ref_gone)} assignments refer to subproducts no longer in the data")

# COMMAND ----------

# DBTITLE 1,Step 2b header
# MAGIC %md
# MAGIC ## Step 2b. Derived: the product from 01
# MAGIC Every mapped subproduct keeps its 01 product as its new product (PNFP: the 33-product rule; SNV: the reference mapping). `setup_study_mv` sets `assignment_source = DERIVED`, so this is what 05 and 06 use. Unmapped subproducts stay unassigned and are flagged. Edit the CASE to merge or rename products.
# MAGIC
# MAGIC Return `sub_key`, `new_product` and an optional `notes` for the subproducts your rule covers; NULL or missing = not assigned by the rule. `subproducts_to_assign` has the bank, subproduct, current product, product group, description, balance, TTC months and the subproduct's recommended beta. You can also join `{{mv_account_metrics}}` or `{{mv_accounts}}`.

# COMMAND ----------

# DBTITLE 1,2b. assign_derived (edit this cell)
sql(r"""
-- ============================== YOUR ASSIGNMENT RULE ==============================
CREATE OR REPLACE TEMP VIEW assign_derived AS
SELECT
  s.sub_key,
  -- the product from 01 is the final product: PNFP = the 33-product rule, SNV = the reference dim_nmd.csv
  -- (to merge or rename products, return a different new_product here, or use product_names in inputs/reference/mv_config.xlsx)
  CASE WHEN s.current_product IS NOT NULL AND s.current_product <> 'UNMAPPED' THEN s.current_product END AS new_product,
  CASE WHEN s.legacy_bank = 'PNFP' THEN 'current product (PNFP 33-product rule)'
       ELSE 'current product (SNV reference mapping)' END AS notes
FROM subproducts_to_assign s
""")
der_pa = spark.table("assign_derived").toPandas()
der_pa["new_product"] = der_pa["new_product"].astype("string").fillna("").str.strip()
der_pa["notes"] = der_pa["notes"].astype("string").fillna("")
assert der_pa.sub_key.is_unique, "assign_derived returns a sub_key more than once"
print(f"Derived: {int((der_pa.new_product != '').sum())} of {len(base)} subproducts assigned by the rule")

# COMMAND ----------

# DBTITLE 1,Step 2c header
# MAGIC %md
# MAGIC ## Step 2c. Pick, name, write `inputs/product_assignment_<tag>.xlsx`
# MAGIC `mv_product` = `consolidated_name` from `product_names` when given, else `new_product`. In FINAL mode, an unassigned subproduct keeps its current product and is flagged.

# COMMAND ----------

# DBTITLE 1,2c. mv_product_selection and the input workbook
pa = (base.merge(ref_pa.rename(columns={"new_product": "new_product_ref", "notes": "notes_ref"}), on="sub_key", how="left")
          .merge(der_pa.rename(columns={"new_product": "new_product_der", "notes": "notes_der"}), on="sub_key", how="left"))
for c in ["new_product_ref", "notes_ref", "new_product_der", "notes_der"]:
    pa[c] = pa[c].astype("string").fillna("")
r, d = pa.new_product_ref, pa.new_product_der
if ASSIGNMENT_SOURCE == "REFERENCE":
    pa["new_product"], pa["assigned_from"] = r, np.where(r != "", "REFERENCE", "")
elif ASSIGNMENT_SOURCE == "DERIVED":
    pa["new_product"], pa["assigned_from"] = d, np.where(d != "", "DERIVED", "")
else:
    pa["new_product"] = d.where(d != "", r)
    pa["assigned_from"] = np.where(d != "", "DERIVED", np.where(r != "", "REFERENCE", ""))
pa["notes"] = np.where(pa.assigned_from == "DERIVED", pa.notes_der, pa.notes_ref)

# ---- decisions (00_setup): source = reference, derived = Step 2b, current = what is used (current wins for its keys) ----
_ids = ["sub_key", "legacy_bank", "subproductid", "current_product", "product_group", "snapshot_balance", "snapshot_accounts",
        "months_ttc", "sub_recommended_method", "sub_recommended_beta", "sub_recommended_r2"]
write_decision("source", "products", pa[_ids].assign(new_product=pa.new_product_ref, notes=pa.notes_ref))
write_decision("derived", "products", pa[_ids].assign(new_product=pa.new_product_der, notes=pa.notes_der))
_cur, CUR_INFO = resolve_current("products", pa[_ids + ["new_product", "notes"]])
_cur = _cur.set_index("sub_key")
_picked = pa["new_product"].astype("string").fillna("")
pa["new_product"] = pa.sub_key.map(_cur["new_product"]).fillna(_picked).astype("string")
pa["notes"] = pa.sub_key.map(_cur["notes"]).fillna(pd.Series(pa["notes"], index=pa.index)).astype("string")
pa.loc[pa.new_product != _picked, "assigned_from"] = "CURRENT (edited)"
pa["ref_vs_derived"] = np.where((r != "") & (d != "") & (r != d), "differs", np.where((r != "") & (d != ""), "same", ""))

# names: reference product_names, plus any new name your rule introduced
names = pd.DataFrame({"new_product": pd.Series(sorted(set(pa.loc[pa.new_product != "", "new_product"])), dtype=object)})
names = names.merge(ref_names[["new_product", "consolidated_name", "notes"]].astype(object), on="new_product", how="left").fillna("")
consol = {x.new_product: (str(x.consolidated_name).strip() or x.new_product) for x in names.itertuples()}

assigned = pa.new_product != ""
pa["assignment_source"] = np.where(assigned, "ASSIGNED", np.where(MODE == "PREVIEW", "PREVIEW_CURRENT_PRODUCT", "UNASSIGNED"))
pa["mv_product"] = np.where(assigned, pa.new_product.map(consol), pa.current_product)
pa["flag_unassigned"] = ~assigned

sel = pa[["sub_key", "legacy_bank", "subproductid", "current_product", "product_group", "snapshot_balance", "new_product", "notes",
          "assignment_source", "mv_product", "assigned_from", "flag_unassigned"]].copy()
save_table(sel, "mv_product_selection")

bal = sel.snapshot_balance.astype(float)
README_PA = pd.DataFrame({"tab": ["assignment_used", "compare", "reference", "derived", "product_names", "unassigned", "reference_not_in_data"],
    "what": [f"The assignment used: inputs/decisions/current/products.csv (assignment_source = {ASSIGNMENT_SOURCE} only fills new subproducts; mode = {MODE}); mv_product is the name carried to 05 and 06",
             "Reference vs derived new_product for every subproduct", "The original product_assignment tab (inputs/reference/mv_config.xlsx)",
             "Your Step 2b rule", "new_product -> consolidated_name", "Subproducts with no new product (kept on their current product)",
             "Reference assignments whose subproduct is no longer in the data"]})
PA_FILE = inp("product_assignment")
write_xlsx(PA_FILE, {"README": README_PA, "assignment_used": pa[PA_COLS + ["new_product", "notes", "assigned_from", "assignment_source", "mv_product", "flag_unassigned"]],
                     "compare": pa[["sub_key", "current_product", "snapshot_balance", "new_product_ref", "new_product_der", "ref_vs_derived"]],
                     "reference": ref_pa, "derived": der_pa, "product_names": names,
                     "unassigned": pa.loc[~assigned, PA_COLS], "reference_not_in_data": ref_gone})
print(f"assigned {int(assigned.sum())} of {len(sel)} subproducts | {bal[assigned].sum() / bal.sum():.1%} of the snapshot balance | "
      f"{sel.loc[assigned, 'mv_product'].nunique()} new products | {sel.mv_product.nunique()} products in total ->", PA_FILE)
display(sel.groupby(["assignment_source", "assigned_from"], as_index=False).agg(subproducts=("sub_key", "count"), balance=("snapshot_balance", lambda s: s.astype(float).sum())))

# COMMAND ----------

# DBTITLE 1,2c. Known issues in the assignment
rows = []
ua = pa[~assigned]
if len(ua):
    rows.append(("UNASSIGNED_SUBPRODUCTS", "HIGH" if MODE == "FINAL" else "INFO", f"{len(ua)} of {len(pa)} subproducts",
                 f"no new product; kept on the current product ({ua.current_product.nunique()} current products). "
                 f"{bal[~assigned].sum() / bal.sum():.1%} of the snapshot balance",
                 int(ua.snapshot_accounts.astype(float).sum()), float(bal[~assigned].sum())))
diff = pa[pa.ref_vs_derived == "differs"]
if len(diff):
    rows.append(("DERIVED_DIFFERS_FROM_REFERENCE", "MEDIUM", f"{len(diff)} subproducts", "see the compare tab",
                 int(diff.snapshot_accounts.astype(float).sum()), float(diff.snapshot_balance.astype(float).sum())))
if len(ref_gone):
    rows.append(("ASSIGNMENT_NOT_IN_DATA", "INFO", f"{len(ref_gone)} rows", ", ".join(ref_gone.sub_key.head(10)), 0, 0.0))
nofit = pa[assigned & (pa.sub_recommended_method == "NONE") & (pa.snapshot_balance.astype(float) > 0)]
if len(nofit):
    rows.append(("ASSIGNED_WITHOUT_SUBPRODUCT_FIT", "INFO", f"{len(nofit)} subproducts",
                 "assigned but have no subproduct-level fit; they still feed the product WAR", int(nofit.snapshot_accounts.astype(float).sum()),
                 float(nofit.snapshot_balance.astype(float).sum())))
issues_assignment = log_issues("04 assignment", rows)