# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 00 | Control panel: how to make a product change
# MAGIC Run the cells below to see this study's current settings and what is in each file. The text explains where a product change goes and which route to pick.
# MAGIC
# MAGIC **Two decisions decide every account's product**
# MAGIC
# MAGIC | Level | Question | Notebook | Setting (config / params) |
# MAGIC |---|---|---|---|
# MAGIC | 1. Mapping | Which product does each legacy account (subproduct) belong to? | `01_account_mapping` | `mapping_source` |
# MAGIC | 2. Assignment | Which final (new) product does each subproduct roll into, and under what name? | `04_product_assignment` | `assignment_source` |
# MAGIC
# MAGIC **Each level has two routes**
# MAGIC
# MAGIC | Route | Setting | Where you edit | What happens to new accounts |
# MAGIC |---|---|---|---|
# MAGIC | **Derived** (edit the query) | `DERIVED` | Level 1: `01` cell **5b. map_derived (edit this cell)** (PNFP rule details are in cell **2. pnfp_src**). Level 2: `04` cell **2b. assign_derived (edit this cell)** | Picked up automatically: the rule runs on every month of source data |
# MAGIC | **Reference** (edit the csv / workbook) | `REFERENCE` | Level 1: `inputs/reference/dim_nmd.csv` (one row per bank + subproduct + pf / bdsp flag). Level 2: `inputs/reference/mv_config.xlsx`, tabs `product_assignment` (sub_key -> new_product) and `product_names` (new_product -> consolidated_name) | Not picked up: a subproduct missing from the file shows as UNMAPPED / UNASSIGNED in the known issues |
# MAGIC | Both | `DERIVED_ELSE_REFERENCE` | Both of the above | The rule first; the file fills the gaps |
# MAGIC
# MAGIC New product names and codes go in `inputs/product_lookup.csv` in both routes (`product_name`, `product_code` = `product_num`, `short_name`).

# COMMAND ----------

# DBTITLE 1,Which route to pick
# MAGIC %md
# MAGIC ## Which route to pick
# MAGIC
# MAGIC | The change looks like | Pick | Why |
# MAGIC |---|---|---|
# MAGIC | A **rule**: "accounts with service charge code X / override rate Y / rate class Z are product P", a new indexed or exception class, a threshold | **DERIVED**: edit the query | One line of SQL covers every account in every month, including accounts opened after the study. MV can read the rule and trace it to source fields. |
# MAGIC | A **short list**: "these 5 subproducts move to P", a management overlay, a one-off fix of a bad mapping row (e.g. the duplicate PNFP `CP_000_00` row) | **REFERENCE**: edit the csv | Explicit and easy to review line by line. No SQL. |
# MAGIC | A **rename or consolidation**: "IB DDA Commercial Standard Plus is reported as IB DDA Commercial Standard" | `product_names` tab (`consolidated_name`) | Leaves the mapping alone; only the reported product changes, so betas can be compared before / after. |
# MAGIC | A rule for most accounts plus a few exceptions | **DERIVED_ELSE_REFERENCE** | Keep the rule clean; the csv holds only the exceptions. |
# MAGIC
# MAGIC **Default for a delivered study:** `mapping_source = DERIVED` (PNFP from the 33-product rule, SNV from the reference), `assignment_source = DERIVED`, consolidations in `product_names`. Use REFERENCE when you need to reproduce an older study exactly.
# MAGIC
# MAGIC ## Steps for a product change
# MAGIC 1. **Make a new version**, so the current results stay: `install/execute_study`, `action = create`, `clone_from = <this study>`, `clone_tables = no`, `dry_run = no`.
# MAGIC 2. In the new version, set the route (`execute_study`, `action = update`, `mapping_source` / `assignment_source`) and make the edit from the table above.
# MAGIC 3. Add any new product to `inputs/product_lookup.csv`.
# MAGIC 4. Run `00_run_study` (or `execute_study`, `action = run`).
# MAGIC 5. Check: `outputs/reports/known_issues_<tag>.csv` (UNMAPPED, DUPLICATE_MAPPING_KEY, UNASSIGNED rows), the `compare` tab of `inputs/account_mapping_<tag>.xlsx` (reference vs derived vs production for every key), and `outputs/html/Product_Mapping_Dashboard_<tag>.html`.

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ./00_setup

# COMMAND ----------

# DBTITLE 1,Current settings and files
def _rows(p, **kw):
    try:
        return len(pd.read_csv(p, dtype=str, keep_default_na=False, **kw)) if p.suffix == ".csv" else len(read_tab(p, kw["tab"]))
    except Exception:
        return "missing"

REF_CFG = REFERENCE / "mv_config.xlsx"
settings = pd.DataFrame([
    ("mapping_source (01)", MAPPING_SOURCE, "DERIVED = cell 5b of 01, REFERENCE = inputs/reference/dim_nmd.csv"),
    ("assignment_source (04)", ASSIGNMENT_SOURCE, "DERIVED = cell 2b of 04, REFERENCE = inputs/reference/mv_config.xlsx"),
    ("mode", MODE, "FINAL = unassigned subproducts are HIGH issues; PREVIEW = keep current products"),
    ("snapshot_date", S.params["snapshot_date"], "config/study_params.json"),
    ("config workbook", CFG_PATH.name, "params, cycles, final_overrides, output_names"),
], columns=["setting", "value", "where"])
display(settings)

files = pd.DataFrame([
    ("inputs/product_lookup.csv", _rows(INPUTS / "product_lookup.csv"), "product names, codes, short names (both routes)"),
    ("inputs/reference/dim_nmd.csv", _rows(REFERENCE / "dim_nmd.csv"), "Level 1 reference mapping rows"),
    ("inputs/reference/mv_config.xlsx / product_assignment", _rows(REF_CFG, tab="product_assignment"), "Level 2 reference: sub_key -> new_product"),
    ("inputs/reference/mv_config.xlsx / product_names", _rows(REF_CFG, tab="product_names"), "new_product -> consolidated_name"),
], columns=["file", "rows", "use"])
display(files)

links = [("01_account_mapping (cell 5b)", step_file("01_account_mapping")),
         ("04_product_assignment (cell 2b)", step_file("04_product_assignment")),
         ("00_run_study", MV_ROOT / "notebooks" / "00_run_study"),
         ("inputs/reference/", REFERENCE), ("inputs/product_lookup.csv", INPUTS / "product_lookup.csv")]
displayHTML("<p>" + " | ".join(f'<a href="{folders.ws_link(p)}" target="_blank">{n}</a>' for n, p in links) + "</p>")

# COMMAND ----------

# DBTITLE 1,Mapping and assignment issues from the last run
if ISSUES_PATH.exists():
    ki = pd.read_csv(ISSUES_PATH, dtype=str, keep_default_na=False)
    ki = ki[ki["step"].str.match(r"^(01|04)") | (ki["code"] == "PRODUCT_NOT_IN_LOOKUP")]   # mapping, assignment, missing lookup rows
    display(ki) if len(ki) else print("No mapping or assignment issues in the last run.")
else:
    print("No known issues log yet: run 00_run_study.")

# COMMAND ----------

