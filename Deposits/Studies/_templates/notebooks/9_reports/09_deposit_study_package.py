# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 09 | Deposit study package (inputs, mapping, betas, results + R)
# MAGIC Writes one self-contained folder, **`outputs/<deposit_study_package>`** (default `outputs/<tag> Deposit Study/`), that someone can open without Databricks: the input series, the mapping, the betas, the final and raw results, and an R script that rebuilds the betas from the csv files and checks them against this study's numbers.
# MAGIC
# MAGIC | Folder | Files |
# MAGIC |---|---|
# MAGIC | `1_inputs/` | `fed_funds_monthly.csv`, `rate_cycles.csv`, `settings.csv`, `war_by_product.csv`, `war_by_subproduct.csv` |
# MAGIC | `2_mapping/` | `product_lookup.csv`, `account_mapping.csv` (the mapping 01 used), `product_assignment.csv` (subproduct -> product, 04) |
# MAGIC | `3_betas/` | `subproduct_betas.csv` (03), `product_betas.csv` (05 / 06, every lag and cycle) |
# MAGIC | `4_results/` | `final_results.csv` (method, beta, R2, up / down, overrides, flags, parameters), `raw_results.csv` (every fit), `study_summary.md` |
# MAGIC | `R/` | `deposit_betas.R`: base R only, no packages. Writes `4_results/r_*.csv` and `4_results/r_vs_python.csv` |
# MAGIC
# MAGIC **R runs locally** (R or RStudio on your machine): copy the folder, then `Rscript R/deposit_betas.R "<folder>"`, or in RStudio `setwd("<folder>"); source("R/deposit_betas.R")`.

# COMMAND ----------

# DBTITLE 1,Install openpyxl
# MAGIC %pip install -q openpyxl

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ../00_setup

# COMMAND ----------

# DBTITLE 1,1. Inputs
import shutil
# outputs/Deposit Study Artifacts/: StudyRecreation-R (this package), Final Reports, Detailed Reports, About
ARTIFACTS = OUTPUTS / "Deposit Study Artifacts"
ART = {k: ARTIFACTS / k for k in ("StudyRecreation-R", "Final Reports", "Detailed Reports", "About")}
for _d in ART.values():
    _d.mkdir(parents=True, exist_ok=True)
PKG = ART["StudyRecreation-R"]
D = {k: PKG / k for k in ("1_inputs", "2_mapping", "3_betas", "4_results", "R")}
for d in D.values():
    d.mkdir(parents=True, exist_ok=True)
for t in ("mv_product_war", "mv_account_war", "mv_beta_review", "mv_account_betas", "mv_final_betas", "mv_product_selection"):
    assert S.table_exists(t), f"{S.table(t)} missing: run 01 to 06 first"
tbl = lambda t: spark.table(S.table(t)).toPandas()

# fed funds, one row per month (last observation), exactly what the beta functions use
ff = EFFR[["effective_date", "month_end", "UP_FFR", "EFFR"]].copy()
ff["effective_date"] = ff["effective_date"].dt.strftime("%Y-%m-%d")
ff["month_end"] = ff["month_end"].dt.strftime("%Y-%m-%d")
ff.to_csv(D["1_inputs"] / "fed_funds_monthly.csv", index=False)

# named cycles + how the study uses them (TTC, UP / DOWN endpoint cycles)
rc = RATE_CYCLES.merge(CYC[["name", "direction", "include"]], on="name", how="left").fillna({"direction": "", "include": ""})
rc["is_ttc"] = (rc["name"] == TTC).map({True: "Y", False: "N"})
if "short_code" in CYC.columns:          # the endpoint column names use the cycles tab's short_code
    rc = rc.drop(columns=["short_code"], errors="ignore").merge(CYC[["name", "short_code"]], on="name", how="left")
rc.to_csv(D["1_inputs"] / "rate_cycles.csv", index=False)

pd.DataFrame([("study", S.tag), ("snapshot_date", S.params["snapshot_date"]), ("lags", ",".join(map(str, LAGS))),
              ("ttc_cycle", TTC), ("poly_rate_col", POLY_COL), ("avg_r2_tolerance", TOL), ("updown_lag", UD_LAG),
              ("min_months_ttc", MIN_MONTHS), ("min_balance", MIN_BAL), ("mode", MODE),
              ("mapping_source", MAPPING_SOURCE), ("assignment_source", ASSIGNMENT_SOURCE)],
             columns=["setting", "value"]).to_csv(D["1_inputs"] / "settings.csv", index=False)

pw = tbl("mv_product_war").rename(columns={"mv_product": "key"})
aw = tbl("mv_account_war").rename(columns={"sub_key": "key"})
for df, f in ((pw, "war_by_product.csv"), (aw, "war_by_subproduct.csv")):
    df["eopdate"] = pd.to_datetime(df["eopdate"]).dt.strftime("%Y-%m-%d")
    df.to_csv(D["1_inputs"] / f, index=False)
print(f"1_inputs: {len(ff)} fed funds months, {len(rc)} cycles, {pw['key'].nunique()} products, {aw['key'].nunique()} subproducts")

# COMMAND ----------

# DBTITLE 1,2. Mapping
shutil.copy2(INPUTS / "product_lookup.csv", D["2_mapping"] / "product_lookup.csv")
try:
    read_tab(inp("account_mapping"), "mapping_used").to_csv(D["2_mapping"] / "account_mapping.csv", index=False)
except Exception as e:
    print("account mapping workbook has no mapping_used tab:", str(e)[:120])
tbl("mv_product_selection").to_csv(D["2_mapping"] / "product_assignment.csv", index=False)
print("2_mapping:", sorted(p.name for p in D["2_mapping"].iterdir()))

# COMMAND ----------

# DBTITLE 1,3. Betas and 4. Results
sb = tbl("mv_account_betas")
pb = tbl("mv_beta_review")
sb.to_csv(D["3_betas"] / "subproduct_betas.csv", index=False)
pb.to_csv(D["3_betas"] / "product_betas.csv", index=False)

fb = tbl("mv_final_betas")
keep = [c for c in ["mv_product", "final_method", "final_beta", "final_r2", "final_source", "recommended_method", "recommended_beta",
                    "recommended_r2", "avg_up", "avg_down", "override_method", "override_reason", "snapshot_balance", "snapshot_war",
                    "months_ttc", "flag_thin_history", "flag_low_balance", "flag_unassigned", "flag_preview",
                    "study", "snapshot_date", "ttc_cycle", "mode"] if c in fb.columns]
final = fb[keep].copy()
for c in ("final_beta", "final_r2", "recommended_beta", "recommended_r2", "avg_up", "avg_down"):
    if c in final:
        final[c] = pd.to_numeric(final[c], errors="coerce").round(2)
final["how_calculated"] = ("TTC " + TTC + ", lags " + ",".join(map(str, LAGS)) + ": best-R2 linear vs best-R2 cubic poly on " + POLY_COL
                           + f"; AVG of the two when R2s are within {TOL:g} pts. Up / down = mean endpoint beta at lag {UD_LAG}.")
final.sort_values("snapshot_balance", ascending=False).to_csv(D["4_results"] / "final_results.csv", index=False)
fb.merge(pb, on="mv_product", how="left", suffixes=("", "_review")).to_csv(D["4_results"] / "raw_results.csv", index=False)

bal = pd.to_numeric(final["snapshot_balance"], errors="coerce")
summ = [f"# {S.tag} Deposit Study", "",
        f"Snapshot {S.params['snapshot_date']} | mode {MODE} | mapping {MAPPING_SOURCE} | assignment {ASSIGNMENT_SOURCE} | TTC {TTC}", "",
        f"{len(final)} products, balance ${bal.sum() / 1e9:,.2f}B.", "",
        "| Product | Method | Beta | R2 | Avg up | Avg down | Balance ($M) |", "|---|---|---|---|---|---|---|"]
for r in final.sort_values("snapshot_balance", ascending=False).itertuples():
    g = lambda c: getattr(r, c, "")
    summ.append(f"| {g('mv_product')} | {g('final_method') or g('recommended_method')} | {g('final_beta')} | {g('final_r2')} | "
                f"{g('avg_up')} | {g('avg_down')} | {float(g('snapshot_balance') or 0) / 1e6:,.1f} |")
summ += ["", "Files: see README.md. R check: run R/deposit_betas.R, then open 4_results/r_vs_python.csv."]
(D["4_results"] / "study_summary.md").write_text("\n".join(summ) + "\n", encoding="utf-8")
print(f"3_betas: {len(sb)} subproducts, {len(pb)} products | 4_results: {len(final)} final rows")

# COMMAND ----------

# DBTITLE 1,5. R script and README
R_SCRIPT = r'''# deposit_betas.R  -- study {{TAG}}
# Rebuilds the study betas in base R (no packages) from the csv files in this folder and compares them with the study.
# Run:   Rscript R/deposit_betas.R "<path to this folder>"
# or in RStudio:  setwd("<path to this folder>"); source("R/deposit_betas.R")
# Same definitions as deposit_utils/betas.py and 00_setup (beta_grid, recommend):
#   linear  WAR(t + L) ~ UP_FFR(t) over the TTC cycle, beta = slope x 100, R2 x 100
#   poly    WAR(t + L) ~ x + x^2 + x^3 (x = poly_rate_col), beta = derivative at the last x, x 100
#   endpoint (WAR end - WAR start + L) / (FFR end - L - FFR start) x 100, per UP / DOWN cycle; avg_up / avg_down = mean at updown_lag

args <- commandArgs(trailingOnly = TRUE)
root <- if (length(args) >= 1) args[1] else "."
p <- function(...) file.path(root, ...)
rd <- function(...) read.csv(p(...), stringsAsFactors = FALSE, check.names = FALSE)

st <- rd("1_inputs", "settings.csv"); S <- setNames(as.character(st$value), st$setting)
LAGS <- as.integer(strsplit(S[["lags"]], ",")[[1]])
TTC <- S[["ttc_cycle"]]; POLY_COL <- S[["poly_rate_col"]]
TOL <- as.numeric(S[["avg_r2_tolerance"]]); UD_LAG <- as.integer(S[["updown_lag"]])

ff <- rd("1_inputs", "fed_funds_monthly.csv"); ff$effective_date <- as.Date(ff$effective_date)
ff <- ff[order(ff$effective_date), ]
cycles <- rd("1_inputs", "rate_cycles.csv")
cycle_dates <- function(nm) {
  r <- cycles[cycles$name == nm, ][1, ]
  sort(ff$effective_date[ff$effective_date >= as.Date(r$start) & ff$effective_date <= as.Date(r$end)])
}
updown <- cycles[toupper(cycles$include) == "Y" & toupper(cycles$direction) %in% c("UP", "DOWN"), ]

make_pivot <- function(war) {
  war$eopdate <- as.Date(war$eopdate)
  dates <- sort(unique(war$eopdate)); keys <- sort(unique(war$key))
  m <- matrix(NA_real_, length(keys), length(dates), dimnames = list(keys, as.character(dates)))
  m[cbind(match(war$key, keys), match(war$eopdate, dates))] <- as.numeric(war$war)
  list(m = m, dates = dates)
}
win <- function(cyc, L) list(start = cyc[1], end = cyc[length(cyc)], start_adj = cyc[L + 1], end_adj = cyc[length(cyc) - L])
xvals <- function(w, col) as.numeric(ff[[col]][ff$effective_date >= w$start & ff$effective_date <= w$end_adj])
r2_of <- function(y, pred) { ss_res <- sum((y - pred)^2); ss_tot <- sum((y - mean(y))^2)
  if (ss_tot > 0) 1 - ss_res / ss_tot else if (ss_res == 0) 1 else 0 }
rows_apply <- function(M, f) t(vapply(seq_len(nrow(M)), function(i) f(M[i, ]), numeric(2)))

lm_beta <- function(P, cyc, L) {
  w <- win(cyc, L); x <- xvals(w, "UP_FFR"); M <- P$m[, P$dates >= w$start_adj & P$dates <= w$end, drop = FALSE]
  rows_apply(M, function(y) {
    if (length(y) != length(x)) return(c(NA, NA))
    v <- !is.na(y) & !is.na(x); if (sum(v) < 2) return(c(NA, NA))
    f <- lm(y[v] ~ x[v]); b <- unname(coef(f)[2]); if (is.na(b)) b <- 0
    c(round(b * 100, 2), round(r2_of(y[v], fitted(f)) * 100, 2))
  })
}
poly_beta <- function(P, cyc, L, col) {
  w <- win(cyc, L); x <- xvals(w, col); x_end <- if (length(x)) x[length(x)] else NA
  M <- P$m[, P$dates >= w$start_adj & P$dates <= w$end, drop = FALSE]
  rows_apply(M, function(y) {
    if (length(y) != length(x)) return(c(NA, NA))
    v <- !is.na(y) & !is.na(x); if (sum(v) < 4) return(c(NA, NA))
    xv <- x[v]; yv <- y[v]; X <- cbind(1, xv, xv^2, xv^3)
    cf <- qr.coef(qr(X), yv); cf[is.na(cf)] <- 0
    pred <- as.vector(X %*% cf); ss_tot <- sum((yv - mean(yv))^2)
    r2 <- if (ss_tot > 0) round((1 - sum((yv - pred)^2) / ss_tot) * 100, 2) else NA
    c(round((cf[2] + 2 * cf[3] * x_end + 3 * cf[4] * x_end^2) * 100, 2), r2)
  })
}
endpoint_beta <- function(P, cyc, L) {
  w <- win(cyc, L)
  ch <- ff$UP_FFR[ff$effective_date == w$end_adj][1] - ff$UP_FFR[ff$effective_date == w$start][1]
  col <- function(d) { s <- as.character(d); if (s %in% colnames(P$m)) P$m[, s] else rep(NA_real_, nrow(P$m)) }
  ws <- col(w$start_adj); we <- col(w$end)
  ifelse(!is.na(ws) & !is.na(we) & ch != 0, round((we - ws) / ch * 100, 2), NA)
}

beta_grid <- function(war) {
  P <- make_pivot(war); ttc <- cycle_dates(TTC)
  g <- data.frame(key = rownames(P$m), stringsAsFactors = FALSE)
  for (L in LAGS) {
    a <- lm_beta(P, ttc, L); g[[paste0("lm_", L)]] <- a[, 1]; g[[paste0("lm_r2_", L)]] <- a[, 2]
    b <- poly_beta(P, ttc, L, POLY_COL); g[[paste0("poly_", L)]] <- b[, 1]; g[[paste0("poly_r2_", L)]] <- b[, 2]
  }
  ud <- list(UP = list(), DOWN = list())
  for (i in seq_len(nrow(updown))) {
    c0 <- updown[i, ]; code <- gsub("[^A-Za-z0-9_]", "", c0$short_code); dir <- toupper(c0$direction)
    for (L in LAGS) {
      nm <- paste0(tolower(dir), "_", code, "_", L)
      g[[nm]] <- endpoint_beta(P, cycle_dates(c0$name), L)
      ud[[dir]][[as.character(L)]] <- c(ud[[dir]][[as.character(L)]], nm)
    }
  }
  for (L in LAGS) for (dir in c("UP", "DOWN")) {
    cols <- ud[[dir]][[as.character(L)]]
    g[[paste0("avg_", tolower(dir), "_", L)]] <- if (length(cols)) rowMeans(g[, cols, drop = FALSE], na.rm = TRUE) else NA
  }
  g$avg_up <- g[[paste0("avg_up_", UD_LAG)]]; g$avg_down <- g[[paste0("avg_down_", UD_LAG)]]
  g$avg_up[is.nan(g$avg_up)] <- NA; g$avg_down[is.nan(g$avg_down)] <- NA
  g$months_ttc <- rowSums(!is.na(P$m[, P$dates >= ttc[1] & P$dates <= ttc[length(ttc)], drop = FALSE]))
  recommend(g)
}

recommend <- function(g) {
  best <- function(r, pfx) {
    b <- sapply(LAGS, function(L) r[[paste0(pfx, "_", L)]]); q <- sapply(LAGS, function(L) r[[paste0(pfx, "_r2_", L)]])
    ok <- !is.na(b) & !is.na(q); if (!any(ok)) return(list(L = NA, b = NA, r = NA))
    i <- which(ok)[which.max(q[ok])]; list(L = LAGS[i], b = b[i], r = q[i])
  }
  out <- lapply(seq_len(nrow(g)), function(i) {
    r <- g[i, ]; l <- best(r, "lm"); p <- best(r, "poly")
    if (is.na(l$L) && is.na(p$L)) { m <- "NONE"; b <- NA; q <- NA }
    else if (is.na(p$L)) { m <- paste0("LM_LAG", l$L); b <- l$b; q <- l$r }
    else if (is.na(l$L)) { m <- paste0("POLY_LAG", p$L); b <- p$b; q <- p$r }
    else if (abs(l$r - p$r) <= TOL) { m <- sprintf("AVG(LM_LAG%d,POLY_LAG%d)", l$L, p$L); b <- (l$b + p$b) / 2; q <- (l$r + p$r) / 2 }
    else if (l$r > p$r) { m <- paste0("LM_LAG", l$L); b <- l$b; q <- l$r }
    else { m <- paste0("POLY_LAG", p$L); b <- p$b; q <- p$r }
    data.frame(best_lm_lag = l$L, best_lm_beta = l$b, best_lm_r2 = l$r, best_poly_lag = p$L, best_poly_beta = p$b,
               best_poly_r2 = p$r, recommended_method = m, recommended_beta = b, recommended_r2 = q, stringsAsFactors = FALSE)
  })
  cbind(g, do.call(rbind, out))
}

compare <- function(r, py, key, level) {
  py$key <- py[[key]]; m <- merge(r, py, by = "key", suffixes = c("_r", "_py"))
  num <- intersect(setdiff(names(r), c("key", "recommended_method")), names(py))
  rows <- lapply(num, function(c0) { d <- abs(as.numeric(m[[paste0(c0, "_r")]]) - as.numeric(m[[paste0(c0, "_py")]]))
    na_mismatch <- sum(is.na(m[[paste0(c0, "_r")]]) != is.na(as.numeric(m[[paste0(c0, "_py")]])))
    data.frame(level = level, column = c0, rows = nrow(m), max_abs_diff = suppressWarnings(max(d, na.rm = TRUE)),
               rows_over_0.011 = sum(d > 0.011, na.rm = TRUE), na_mismatch = na_mismatch) })
  meth <- data.frame(level = level, column = "recommended_method", rows = nrow(m), max_abs_diff = NA,
                     rows_over_0.011 = sum(m$recommended_method_r != m$recommended_method_py, na.rm = TRUE),
                     na_mismatch = sum(is.na(m$recommended_method_r) != is.na(m$recommended_method_py)))
  rbind(do.call(rbind, rows), meth)
}

prod_r <- beta_grid(rd("1_inputs", "war_by_product.csv"))
sub_r <- beta_grid(rd("1_inputs", "war_by_subproduct.csv"))
write.csv(prod_r, p("4_results", "r_product_betas.csv"), row.names = FALSE)
write.csv(sub_r, p("4_results", "r_subproduct_betas.csv"), row.names = FALSE)
cmp <- rbind(compare(prod_r, rd("3_betas", "product_betas.csv"), "mv_product", "product"),
             compare(sub_r, rd("3_betas", "subproduct_betas.csv"), "sub_key", "subproduct"))
write.csv(cmp, p("4_results", "r_vs_python.csv"), row.names = FALSE)
bad <- cmp[cmp$rows_over_0.011 > 0 | cmp$na_mismatch > 0, ]
cat(sprintf("R rebuilt %d products and %d subproducts. Columns that differ from the study: %d\n", nrow(prod_r), nrow(sub_r), nrow(bad)))
if (nrow(bad)) print(bad) else cat("R matches the study (to 0.01) on every beta, R2 and method.\n")
'''.replace("{{TAG}}", S.tag)
(D["R"] / "deposit_betas.R").write_text(R_SCRIPT, encoding="utf-8")

README = f"""# {S.tag} Deposit Study

Everything needed to read, check or rerun the {S.tag} deposit betas outside Databricks. Built by `notebooks/09_deposit_study_package`.

## Where the numbers come from
1. Accounts: PNFP `{S.params['sources']['src_pnfp_deposits']}`, SNV `{S.params['sources']['src_lsnv_nmds']}` (lineage checks: `outputs/reports/` source lineage report).
2. Mapping (01, `{MAPPING_SOURCE}`): account -> product, `2_mapping/account_mapping.csv`, names and codes in `2_mapping/product_lookup.csv`.
3. WAR = sum(rate x balance) / sum(balance) by month: `1_inputs/war_by_subproduct.csv`, `1_inputs/war_by_product.csv`.
4. Betas (03 subproduct, 05 product): `3_betas/`. Fed funds and cycles: `1_inputs/fed_funds_monthly.csv`, `1_inputs/rate_cycles.csv`.
5. Final (06): `4_results/final_results.csv` (method, beta, R2, up / down, overrides, flags, parameters). Every fit: `4_results/raw_results.csv`.

## Settings
Snapshot {S.params['snapshot_date']}, TTC {TTC}, lags {LAGS}, poly on {POLY_COL}, AVG tolerance {TOL:g} R2 pts, up / down lag {UD_LAG}, mode {MODE}. All in `1_inputs/settings.csv`.

## Rerun in R (local)
1. Copy this folder to your machine.
2. `Rscript R/deposit_betas.R "<this folder>"` (or RStudio: `setwd("<this folder>"); source("R/deposit_betas.R")`). Base R only.
3. Open `4_results/r_vs_python.csv`: every beta, R2 and method compared with the study (rows_over_0.011 and na_mismatch should be 0).

To test a change in R, edit the csv files in `1_inputs/` (e.g. a cycle's dates or `lags`) and rerun.
"""
(PKG / "README.md").write_text(README, encoding="utf-8")
files = sorted(str(p.relative_to(PKG)) for p in PKG.rglob("*") if p.is_file())
print(f"{PKG}\n  " + "\n  ".join(files))
displayHTML(f'<a href="{folders.ws_link(PKG)}" target="_blank">Open {PKG.name}</a>')

# COMMAND ----------

# DBTITLE 1,6. Final Reports: Final Deposits Study workbook
# outputs/Deposit Study Artifacts/Final Reports/Final Deposits Study <tag>.xlsx
#   Betas                    one row per product: name, code, balance, WAR, old beta, calc beta, recommended beta, method, notes
#   Truncations              months until 30 / 25 / 20% of month 0 accounts remain, by final product (from 11_truncation_points)
#   Product & Subproduct     each product with its subproducts underneath (click + / - on the left to open or close a product)
# OLD BETA comes from inputs/reference/old_betas.csv (product_name, old_beta). It is made with blanks the first time: fill it in.
# Notes come from inputs/decisions/current/betas.csv (notes, else reason), so write them in 00_read_and_recommend.
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

fb = tbl("mv_final_betas")
OLD_PATH = REFERENCE / "old_betas.csv"
if not OLD_PATH.exists():
    pd.DataFrame({"product_name": sorted(fb.mv_product), "old_beta": ""}).to_csv(OLD_PATH, index=False)
    print(f"Made {OLD_PATH} with blank old_beta: fill it in and re-run this cell")
old = pd.read_csv(OLD_PATH, dtype=str, keep_default_na=False)
old_map = dict(zip(old["product_name"].str.strip(), pd.to_numeric(old["old_beta"], errors="coerce")))
prods = pd.read_csv(INPUTS / "products.csv", dtype=str, keep_default_na=False)
code_map = dict(zip(prods["product_name"].str.strip(), prods["product_code"].str.strip()))
cb = read_decision("current", "betas")
cb = cb.set_index("product") if cb is not None else pd.DataFrame()

def _note(p):
    if p not in cb.index:
        return ""
    r = cb.loc[p]
    n, why = str(r.get("notes", "") or ""), str(r.get("reason", "") or "")
    return " | ".join(x for x in dict.fromkeys([n, why]) if x)

num = lambda s: pd.to_numeric(s, errors="coerce")
rep = pd.DataFrame({
    "Product Name": fb.mv_product,
    "Product Code": fb.mv_product.map(code_map).fillna(""),
    "Balance": num(fb.snapshot_balance),
    "WAR": num(fb.snapshot_war),
    "OLD BETA": fb.mv_product.map(old_map),
    "CALC BETA": num(fb.recommended_beta),          # the rule (best-R2 linear / poly / AVG)
    "Recommended Beta": num(fb.final_beta),         # after your override in current/betas.csv
    "Calc Method": fb.final_method.fillna(""),
    "Notes": fb.mv_product.map(_note),
}).sort_values("Balance", ascending=False, ignore_index=True)

sel = tbl("mv_product_selection")[["sub_key", "legacy_bank", "subproductid", "mv_product", "snapshot_balance"]]
ab = tbl("mv_account_betas")[["sub_key", "acct_typ_desc", "snapshot_accounts", "snapshot_war"]]
sub = sel.merge(ab, on="sub_key", how="left")
sub["snapshot_balance"], sub["snapshot_accounts"], sub["snapshot_war"] = num(sub.snapshot_balance), num(sub.snapshot_accounts), num(sub.snapshot_war)

HDR = PatternFill("solid", fgColor="1F4E79")
PROD = PatternFill("solid", fgColor="DDEBF7")
TOT = PatternFill("solid", fgColor="FFF2CC")
thin = Border(bottom=Side(style="thin", color="BFBFBF"))
FMT = {"Balance": "$#,##0", "WAR": "0.00%", "OLD BETA": "0.00", "CALC BETA": "0.00", "Recommended Beta": "0.00",
       "Accounts": "#,##0"}

def _header(ws, cols, widths):
    ws.append(cols)
    for i, (c, w) in enumerate(zip(cols, widths), 1):
        cell = ws.cell(row=ws.max_row, column=i)
        cell.font, cell.fill = Font(bold=True, color="FFFFFF"), HDR
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        ws.column_dimensions[get_column_letter(i)].width = w

def _fmt_row(ws, cols, fill=None, bold=False):
    for i, c in enumerate(cols, 1):
        cell = ws.cell(row=ws.max_row, column=i)
        cell.number_format = FMT.get(c, "General")
        cell.border = thin
        if fill:
            cell.fill = fill
        if bold:
            cell.font = Font(bold=True)

wb = Workbook()
ws = wb.active
ws.title = "Betas"
ws.append([f"{S.tag} Deposit Study | Final betas | snapshot {S.snapshot_date} | TTC {TTC} | mode {MODE}"])
ws["A1"].font = Font(bold=True, size=13, color="1F4E79")
ws.append([])
cols = list(rep.columns)
_header(ws, cols, [36, 14, 18, 9, 11, 11, 16, 26, 60])
for r in rep.itertuples(index=False):
    ws.append([None if pd.isna(v) else v for v in r])
    _fmt_row(ws, cols)
    ws.cell(row=ws.max_row, column=9).alignment = Alignment(wrap_text=True, vertical="top")
b = rep.Balance.fillna(0)
wavg = lambda c: float((rep[c] * b).sum() / b[rep[c].notna()].sum()) if b[rep[c].notna()].sum() else None
ws.append(["Total / balance-weighted", "", float(b.sum()), wavg("WAR"), wavg("OLD BETA"), wavg("CALC BETA"), wavg("Recommended Beta"), "", ""])
_fmt_row(ws, cols, TOT, True)
ws.freeze_panes = "B4"
ws.auto_filter.ref = f"A3:{get_column_letter(len(cols))}{ws.max_row - 1}"

# Truncations: 11_truncation_points writes outputs/excel/truncation_<tag>.xlsx; its 'By mv_product' tab is copied here
TRUNC_XLSX = OUTPUTS / "excel" / f"{out_name('truncation')}.xlsx"
wt = wb.create_sheet("Truncations")
if TRUNC_XLSX.exists():
    tr = pd.read_excel(TRUNC_XLSX, sheet_name="By mv_product")
    tr.insert(1, "Product Code", tr["mv_product"].map(code_map).fillna(""))
    tr = tr.rename(columns={"mv_product": "Product Name", "Baseline_Acct_Cnt": "Month 0 Accounts", "Baseline_Balance": "Month 0 Balance",
                            "R_squared": "R Squared", "Last_N_Month": "Last Month Observed", "Pct_left_at_last": "Pct Left at Last Month"})
    tr = tr.drop(columns=[c for c in ("Intercept", "Slope") if c in tr])
    _ts = pd.Timestamp(TRUNC_XLSX.stat().st_mtime, unit="s").strftime("%Y-%m-%d %H:%M")
    wt.append([f"{S.tag} Deposit Study | Truncation points by final product | from {TRUNC_XLSX.name} (built {_ts} UTC)"])
    wt["A1"].font = Font(bold=True, size=13, color="1F4E79")
    wt.append(["Months until only 30 / 25 / 20% of the month 0 accounts are still open (straight-line fit of percent of month 0). "
               "Low R Squared = rough estimate."])
    wt["A2"].font = Font(italic=True, color="595959")
    tcols = list(tr.columns)
    FMT.update({"Month 0 Accounts": "#,##0", "Month 0 Balance": "$#,##0", "R Squared": "0.00", "Pct Left at Last Month": "0.0"})
    _header(wt, tcols, [36, 14, 16, 18] + [14] * (len(tcols) - 4))
    for r in tr.itertuples(index=False):
        wt.append([None if pd.isna(v) else v for v in r])
        _fmt_row(wt, tcols)
    wt.freeze_panes = "B4"
    wt.auto_filter.ref = f"A3:{get_column_letter(len(tcols))}{wt.max_row}"
    print(f"Truncations: {len(tr)} products from {TRUNC_XLSX.name} (built {_ts} UTC)")
else:
    wt.append([f"No truncation results yet: run 10_truncation_cohorts and 11_truncation_points, then re-run this cell ({TRUNC_XLSX.name})."])
    print(f"Truncations tab is empty: {TRUNC_XLSX} not found (run 10 and 11)")

wd = wb.create_sheet("Product & Subproduct")
wd.sheet_properties.outlinePr.summaryBelow = False          # the + / - sits on the product row
wd.append(["Each product row opens to its subproducts: click + on the left (or the 1 / 2 buttons at the top left for all)."])
wd["A1"].font = Font(italic=True, color="595959")
dcols = ["Product / Subproduct", "Bank", "Sub key", "Account Type", "Balance", "Accounts", "WAR"]
_header(wd, dcols, [40, 8, 30, 30, 18, 12, 9])
war_p = dict(zip(rep["Product Name"], rep["WAR"]))
for p in rep["Product Name"]:
    s = sub[sub.mv_product == p].sort_values("snapshot_balance", ascending=False)
    wd.append([p, ", ".join(sorted(s.legacy_bank.dropna().unique())), f"{len(s)} subproducts", "",
               float(s.snapshot_balance.sum()), float(s.snapshot_accounts.sum()), war_p.get(p)])
    _fmt_row(wd, dcols, PROD, True)
    for x in s.itertuples():
        wd.append([f"    {x.subproductid}", x.legacy_bank, x.sub_key, x.acct_typ_desc or "",
                   None if pd.isna(x.snapshot_balance) else float(x.snapshot_balance),
                   None if pd.isna(x.snapshot_accounts) else float(x.snapshot_accounts),
                   None if pd.isna(x.snapshot_war) else float(x.snapshot_war)])
        _fmt_row(wd, dcols)
        wd.row_dimensions[wd.max_row].outline_level = 1
        wd.row_dimensions[wd.max_row].hidden = True
wd.freeze_panes = "B3"

wr = wb.create_sheet("README")
for row in [("Tab", "What it shows"),
            ("Betas", "One row per final product. CALC BETA = the study rule; Recommended Beta = the beta used (the rule, or your override); "
                              "Calc Method = how the Recommended Beta was set; OLD BETA from inputs/reference/old_betas.csv; "
                              "Notes from inputs/decisions/current/betas.csv (00_read_and_recommend)"),
            ("Truncations", "Months until 30 / 25 / 20% of month 0 accounts remain, by final product, from 11_truncation_points "
                            "(PNFP month 0 and SNV vintages set in config/study_params.json -> truncation)"),
            ("Product & Subproduct", "Each product with its subproducts (click + to open). Balance, accounts and WAR at the snapshot"),
            ("Units", "Balance in dollars; WAR shown as a percent; betas as decimals (0.45 = 45% pass-through)")]:
    wr.append(row)
wr["A1"].font = wr["B1"].font = Font(bold=True)
wr.column_dimensions["A"].width, wr.column_dimensions["B"].width = 18, 120

FINAL_XLSX = ART["Final Reports"] / f"Final Deposits Study {S.tag}.xlsx"
wb.save(FINAL_XLSX)
missing_old = int(rep["OLD BETA"].isna().sum())
print(f"{FINAL_XLSX} | {len(rep)} products, {len(sub)} subproducts | {missing_old} products without an OLD BETA "
      f"| {int((rep.Notes != '').sum())} with notes")
displayHTML(f'<a href="{folders.ws_link(ART["Final Reports"])}" target="_blank">Open Final Reports</a>')
display(rep)

# COMMAND ----------

# DBTITLE 1,6. Final Reports: Final Deposits Study workbook
# outputs/Deposit Study Artifacts/Final Reports/Final Deposits Study <tag>.xlsx
#   Betas | Betas Detail | Truncations | Truncations Detail | WARFs | WARFs Detail |
#   Product Group Summary | Product Summary | Bank Summary | Study Summary
# Reads: mv_final_betas, mv_product_betas, mv_product_selection, mv_account_betas (this study),
#        outputs/excel/truncation_<tag>.xlsx (11) and outputs/excel/warf_<tag>.xlsx (13). A module not run yet leaves its columns blank.
# OLD BETA: inputs/reference/old_betas.csv (product_name, old_beta). Notes: inputs/decisions/current/betas.csv (notes, else reason).
import re
import numpy as np
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

num = lambda s: pd.to_numeric(s, errors="coerce")

# ---------- betas and the subproduct base ----------
fb = tbl("mv_final_betas")
pb = tbl("mv_product_betas")
OLD_PATH = REFERENCE / "old_betas.csv"
if not OLD_PATH.exists():
    pd.DataFrame({"product_name": sorted(fb.mv_product), "old_beta": ""}).to_csv(OLD_PATH, index=False)
    print(f"Made {OLD_PATH} with blank old_beta: fill it in and re-run this cell")
old = pd.read_csv(OLD_PATH, dtype=str, keep_default_na=False)
old_map = dict(zip(old["product_name"].str.strip(), num(old["old_beta"])))
prods = pd.read_csv(INPUTS / "products.csv", dtype=str, keep_default_na=False)
code_map = dict(zip(prods["product_name"].str.strip(), prods["product_code"].str.strip()))
cb = read_decision("current", "betas")
cb = cb.set_index("product") if cb is not None else pd.DataFrame()

def _note(p):
    if p not in cb.index:
        return ""
    r = cb.loc[p]
    n, why = str(r.get("notes", "") or ""), str(r.get("reason", "") or "")
    return " | ".join(x for x in dict.fromkeys([n, why]) if x)

sel = tbl("mv_product_selection")[["sub_key", "legacy_bank", "subproductid", "mv_product", "product_group", "snapshot_balance"]]
ab = tbl("mv_account_betas")[["sub_key", "acct_typ_desc", "snapshot_accounts", "snapshot_war"]]
sub = sel.merge(ab, on="sub_key", how="left")
for c in ("snapshot_balance", "snapshot_accounts", "snapshot_war"):
    sub[c] = num(sub[c])
sub["snapshot_balance"] = sub.snapshot_balance.fillna(0)
sub["final_beta"] = sub.mv_product.map(dict(zip(fb.mv_product, num(fb.final_beta))))
GROUP = (sub.groupby(["mv_product", "product_group"]).snapshot_balance.sum().reset_index()
         .sort_values("snapshot_balance").drop_duplicates("mv_product", keep="last").set_index("mv_product").product_group)

def roll(by):
    """Subproducts, accounts, balance, WAR and the balance-weighted final beta by `by` (each subproduct carries its product's beta)."""
    d = sub.copy()
    b = d.snapshot_balance
    d["_wb"], d["_ww"] = b * d.snapshot_war.fillna(0), np.where(d.snapshot_war.notna(), b, 0)
    d["_bb"], d["_bw"] = b * d.final_beta.fillna(0), np.where(d.final_beta.notna(), b, 0)
    r = d.groupby(by).agg(subproducts=("sub_key", "nunique"), accounts=("snapshot_accounts", "sum"), balance=("snapshot_balance", "sum"),
                          _wb=("_wb", "sum"), _ww=("_ww", "sum"), _bb=("_bb", "sum"), _bw=("_bw", "sum")).reset_index()
    r["war"], r["beta"] = r._wb / r._ww.replace(0, np.nan), r._bb / r._bw.replace(0, np.nan)
    return r.drop(columns=["_wb", "_ww", "_bb", "_bw"])

# ---------- truncations (11) ----------
TR_XLSX = OUTPUTS / "excel" / f"{out_name('truncation')}.xlsx"
TR = {}
if TR_XLSX.exists():
    _tx = pd.ExcelFile(TR_XLSX)
    TR = {k: _tx.parse(k) for k in _tx.sheet_names}
else:
    print(f"No truncation results ({TR_XLSX.name}): run 10 and 11. Truncation columns stay blank.")

def trunc(level, key):
    d = TR.get(f"By {level}")
    if d is None:
        return pd.DataFrame(columns=[key])
    d = d.rename(columns={level: key, "Baseline_Acct_Cnt": "month0_accounts", "Baseline_Balance": "month0_balance", "R_squared": "trunc_r2",
                          "Last_N_Month": "last_month", "Pct_left_at_last": "pct_left_at_last", "Intercept": "intercept", "Slope": "slope"})
    return d.rename(columns={c: "months_to_" + re.sub(r"\D", "", c) for c in d.columns if str(c).startswith("Month y=")})

TH = [c for c in trunc("mv_product", "k").columns if str(c).startswith("months_to_")]

# ---------- WARFs (13) ----------
WX_XLSX = OUTPUTS / "excel" / f"{out_name('warf')}.xlsx"
WX = {}
if WX_XLSX.exists():
    _wx = pd.ExcelFile(WX_XLSX)
    WX = {k: _wx.parse(k) for k in _wx.sheet_names}
else:
    print(f"No WARF results ({WX_XLSX.name}): run 12 and 13. WARF columns stay blank.")
WSET = dict(zip(WX["Settings"].setting, WX["Settings"].value.astype(str))) if "Settings" in WX else {}
WMONTH = WSET.get("latest month", "")
WCOLS = ["warf_month_balance", "fse_balance", "fse_share", "warf_all", "warf_ex_fse", "modified_warf", "warf_fse"]

def warf(sheet, key):
    d = WX.get(sheet)
    if d is None:
        return pd.DataFrame(columns=[key] + WCOLS)
    d = d[d.basis == "latest"] if "basis" in d else d
    d = d.rename(columns={d.columns[0]: key, "balance": "warf_month_balance"})
    d["modified_warf"] = d["warf_ex_fse"] * 0.7
    return d[[key] + [c for c in WCOLS if c in d.columns]]

WSUM = WX.get("Summary", pd.DataFrame(columns=["bank", "month"] + WCOLS)).rename(columns={"balance": "warf_month_balance"})
if "warf_ex_fse" in WSUM.columns:
    WSUM["modified_warf"] = WSUM["warf_ex_fse"] * 0.7
WSUM_M = WSUM[WSUM.month.astype(str) == WMONTH] if len(WSUM) else WSUM

# ---------- WALs (15) ----------
WAL_CSV = OUTPUTS / "excel" / f"{out_name('wal_summary')}.csv"
if WAL_CSV.exists():
    _wal = pd.read_csv(WAL_CSV).rename(columns={'avg_balance_calc_month': 'avg_balance'})
else:
    _wal = pd.DataFrame(columns=['mv_product', 'avg_balance', 'wal_current', 'wal_calculated'])
    print(f"No WAL results ({WAL_CSV.name}): run 15. WAL columns stay blank.")
wal_avg_map = dict(zip(_wal.mv_product, _wal.avg_balance))
wal_cur_map = dict(zip(_wal.mv_product, _wal.wal_current))
wal_calc_map = dict(zip(_wal.mv_product, _wal.wal_calculated))

# ---------- display names and formats ----------
NAMES = {"mv_product": "Product", "product_group": "Product Group", "legacy_bank": "Bank", "balance": "Balance", "accounts": "Accounts",
         "subproducts": "Subproducts", "war": "WAR", "beta": "Beta (balance-weighted)", "month0_accounts": "Month 0 Accounts",
         "month0_balance": "Month 0 Balance", "trunc_r2": "Truncation R²", "last_month": "Last Month Observed",
         "pct_left_at_last": "% Left at Last Month", "intercept": "Intercept", "slope": "Slope", "warf_month_balance": "WARF Month Balance",
         "fse_balance": "FSE Balance", "fse_share": "FSE Share", "warf_all": "WARF (all)", "warf_ex_fse": "WARF without FSE",
         "warf_fse": "WARF FSE only", "modified_warf": "Modified WARF",
         "wal_current": "WAL Current (months)", "wal_calculated": "WAL Calculated (months)", "avg_balance": "Avg Balance", "snapshot_balance": "Balance", "months_ttc": "TTC Months", "counterparty_type": "Counterparty",
         "lcr_rollup_1": "LCR Rollup", "bank": "Bank", "runoff": "Runoff Balance", "warf_balance": "WARF Balance", "month": "Month"}
pretty = lambda df: df.rename(columns=lambda c: NAMES.get(c, f"Months to {c[10:]}%" if str(c).startswith("months_to_") else c))

def FMT(c):
    c = str(c)
    if c.isdigit():                                   # truncation detail: % of month 0 accounts left at month N
        return "0.0"
    if re.fullmatch(r"\d{4}-\d{2}", c):               # WARF history months
        return "0.00%"
    if c.endswith("Balance"):
        return "$#,##0"
    if c in ("Accounts", "Subproducts", "Month 0 Accounts", "Last Month Observed", "TTC Months") or c.startswith("Months to"):
        return "#,##0"
    if c.startswith("WAL"):
        return "0.0"
    if c in ("WAR", "FSE Share") or c.startswith("WARF") or c.startswith("Modified"):
        return "0.00%"
    if c in ("Truncation R²", "Intercept", "Slope"):
        return "0.00"
    if "_r2_" in c or c.endswith("R² (%)") or c == "% Left at Last Month":
        return "0.0"
    if "Beta" in c or "BETA" in c or re.match(r"(lm|poly|up|down|avg)_", c):
        return "0.00"
    return "General"

HDR, PRODF, TOT = PatternFill("solid", fgColor="1F4E79"), PatternFill("solid", fgColor="DDEBF7"), PatternFill("solid", fgColor="FFF2CC")
SEC = Font(bold=True, size=11, color="1F4E79")
thin = Border(bottom=Side(style="thin", color="BFBFBF"))
_v = lambda v: None if (not isinstance(v, str) and pd.isna(v)) else (v.item() if hasattr(v, "item") else v)

def title(ws, text, note=None):
    ws.append([text]); ws.cell(ws.max_row, 1).font = Font(bold=True, size=13, color="1F4E79")
    if note:
        ws.append([note]); ws.cell(ws.max_row, 1).font = Font(italic=True, color="595959")
    ws.append([])

def table(ws, df, label=None, total=None):
    if label:
        ws.append([label]); ws.cell(ws.max_row, 1).font = SEC
    cols = [str(c) for c in df.columns]
    ws.append(cols)
    hr = ws.max_row
    for i in range(1, len(cols) + 1):
        cell = ws.cell(hr, i)
        cell.font, cell.fill = Font(bold=True, color="FFFFFF"), HDR
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
    rows = [list(r) for r in df.itertuples(index=False)] + ([list(total)] if total is not None else [])
    for j, r in enumerate(rows):
        ws.append([_v(v) for v in r])
        last = total is not None and j == len(rows) - 1
        for i, c in enumerate(cols, 1):
            cell = ws.cell(ws.max_row, i)
            cell.number_format, cell.border = FMT(c), thin
            if last:
                cell.fill, cell.font = TOT, Font(bold=True)
    ws.append([])
    return hr

def widths(ws, first=34, rest=14, n=40):
    ws.column_dimensions["A"].width = first
    for i in range(2, n + 1):
        ws.column_dimensions[get_column_letter(i)].width = rest

def total_row(df, label, sums=(), wavg=(), w="Balance", fixed=None):
    row = {c: None for c in df.columns}
    row[df.columns[0]] = label
    b = num(df[w]).fillna(0) if w in df else pd.Series(0, index=df.index)
    for c in sums:
        row[c] = float(num(df[c]).sum())
    for c in wavg:
        x = num(df[c]); m = x.notna() & (b > 0)
        row[c] = float((x[m] * b[m]).sum() / b[m].sum()) if m.any() else None
    row.update(fixed or {})
    return [row[c] for c in df.columns]

WT = WSUM_M[WSUM_M.bank == "Total"]
WARF_TOTAL = {NAMES.get(k, k): float(WT.iloc[0][k]) for k in WCOLS if len(WT) and k in WT.columns} if len(WT) else {}
HEAD = f"{S.tag} Deposit Study | snapshot {S.snapshot_date} | TTC {TTC} | mode {MODE}" + (f" | WARF month {WMONTH}" if WMONTH else "")

# ---------- frames ----------
bet = pd.DataFrame({
    "Product": fb.mv_product, "Product Code": fb.mv_product.map(code_map).fillna(""), "Product Group": fb.mv_product.map(GROUP).fillna(""),
    "Balance": num(fb.snapshot_balance), "WAR": num(fb.snapshot_war), "OLD BETA": fb.mv_product.map(old_map),
    "CALC BETA": num(fb.recommended_beta), "Recommended Beta": num(fb.final_beta), "Calc Method": fb.final_method.fillna(""),
    "Beta R² (%)": num(fb.final_r2), "TTC Months": num(fb.months_ttc), "Avg Balance": fb.mv_product.map(wal_avg_map),
    "WAL Current (months)": fb.mv_product.map(wal_cur_map),
    "WAL Calculated (months)": fb.mv_product.map(wal_calc_map),
    "Notes": fb.mv_product.map(_note),
}).sort_values("Balance", ascending=False, ignore_index=True)
ORDER = bet.Product.tolist()

grid = [c for c in pb.columns if re.match(r"(lm|poly|up|down|avg)_", c)]
bdet = pb[["mv_product", "snapshot_balance", "months_ttc"] + grid].copy()
for c in ["snapshot_balance", "months_ttc"] + grid:
    bdet[c] = num(bdet[c])
bdet = pretty(bdet.sort_values("snapshot_balance", ascending=False, ignore_index=True))

tp = trunc("mv_product", "mv_product")
if len(tp):
    tp.insert(1, "product_code", tp.mv_product.map(code_map).fillna("")); tp.insert(2, "product_group", tp.mv_product.map(GROUP).fillna(""))
tcond_cols = ["mv_product", "product_code", "product_group", "month0_accounts", "month0_balance", "trunc_r2"] + TH + ["last_month", "pct_left_at_last"]
tcond = pretty(tp[[c for c in tcond_cols if c in tp]]).rename(columns={"product_code": "Product Code"}) if len(tp) else pd.DataFrame()

wp = warf("By product", "mv_product")
wp.insert(1, "Product Code", wp.mv_product.map(code_map).fillna("")); wp.insert(2, "product_group", wp.mv_product.map(GROUP).fillna(""))
wp["_out"] = wp.mv_product.eq("(not in study)")                    # accounts outside the study go last
wcond = pretty(wp.sort_values(["_out", "warf_month_balance"], ascending=[True, False], ignore_index=True).drop(columns="_out"))

# product group / product / bank summaries: snapshot balance, WAR and weighted beta + truncation + WARF
TKEEP = TH + ["trunc_r2"]
WKEEP = ["fse_share", "warf_all", "warf_ex_fse", "modified_warf", "warf_fse"]
def summary(level):
    r = roll(level)
    t = trunc(level, level)
    r = r.merge(t[[level] + [c for c in TKEEP if c in t]], on=level, how="left") if len(t) else r
    w_ = {"product_group": warf("By product group", level), "mv_product": warf("By product", level),
          "legacy_bank": WSUM_M.rename(columns={"bank": level})}[level]
    r = r.merge(w_[[level] + [c for c in WKEEP if c in w_]], on=level, how="left") if len(w_) else r
    for c in TKEEP + WKEEP:
        if c not in r:
            r[c] = np.nan
    return r[[level, "subproducts", "accounts", "balance", "war", "beta"] + TKEEP + WKEEP].sort_values("balance", ascending=False, ignore_index=True)

gsum, bsum = pretty(summary("product_group")), pretty(summary("legacy_bank"))
psum = summary("mv_product")

# ---------- workbook ----------
wb = Workbook()

ws = wb.active; ws.title = "Betas"
title(ws, HEAD, "Final beta per product. CALC BETA = the study rule; Recommended Beta = the beta used (the rule, or your override in 00_read_and_recommend).")
table(ws, bet, total=total_row(bet, "Total / balance-weighted", sums=["Balance"], wavg=["WAR", "OLD BETA", "CALC BETA", "Recommended Beta"]))
widths(ws); ws.column_dimensions["L"].width = 60; ws.freeze_panes = "B5"

ws = wb.create_sheet("Betas Detail")
title(ws, HEAD, "Every fit per product: TTC linear (lm) and polynomial (poly) beta and R² (%) at lags 0 to 3, the up / down cycle betas and their averages.")
table(ws, bdet); widths(ws, rest=11); ws.freeze_panes = "B5"

ws = wb.create_sheet("Truncations")
title(ws, HEAD, "Months until only 30 / 25 / 20% of the month 0 accounts are still open (straight-line fit). Low Truncation R² = rough estimate.")
if len(tcond):
    table(ws, tcond)
else:
    ws.append(["No truncation results yet: run 10_truncation_cohorts and 11_truncation_points, then 09."])
widths(ws); ws.freeze_panes = "B5"

ws = wb.create_sheet("Truncations Detail")
title(ws, HEAD, "The fitted line and the % of month 0 accounts still open at each month (N_Month), by product, product group and bank.")
for lvl, key in (("mv_product", "Product"), ("product_group", "Product Group"), ("legacy_bank", "Bank")):
    t = trunc(lvl, lvl)
    if len(t):
        table(ws, pretty(t[[c for c in [lvl, "month0_accounts", "month0_balance", "intercept", "slope", "trunc_r2"] + TH + ["last_month", "pct_left_at_last"] if c in t]]),
              label=f"Fitted line by {key.lower()}")
for lvl, key in (("mv_product", "product"), ("product_group", "product group"), ("legacy_bank", "bank")):
    p = TR.get(f"Pct {lvl}")
    if p is not None:
        table(ws, p.rename(columns={lvl: NAMES[lvl]}), label=f"% of month 0 accounts still open, by {key} (columns = months since month 0)")
widths(ws, rest=9); ws.freeze_panes = "B4"

ws = wb.create_sheet("WARFs")
title(ws, HEAD, f"WARF = runoff / WARF balance at {WMONTH or 'the WARF month'}, for all balances, without FSE (fse_flag = N) and FSE only. "
               "Study-mapped balances only; the Bank Summary includes everything.")
if len(wcond):
    table(ws, wcond, total=["Total (all balances, incl. not in study)", "", ""] + [WARF_TOTAL.get(NAMES[k]) for k in WCOLS])
else:
    ws.append(["No WARF results yet: run 12_warf_prep and 13_warf_results, then 09."])
widths(ws); ws.freeze_panes = "B5"

ws = wb.create_sheet("WARFs Detail")
title(ws, HEAD, "WARF by bank, counterparty and LCR rollup, and the monthly WARF history by product and product group. Months kept: " + WSET.get("months kept", ""))
if len(WSUM):
    table(ws, pretty(WSUM[["month", "bank"] + WCOLS]), label="By bank (report and snapshot months)")
for sheet, key, lab in (("By counterparty", "counterparty_type", "By counterparty"), ("By LCR rollup", "lcr_rollup_1", "By LCR rollup")):
    if sheet in WX:
        table(ws, pretty(warf(sheet, key)).sort_values("WARF Month Balance", ascending=False), label=f"{lab} ({WMONTH})")
for sheet, lab in (("Hist WARF all by product", "WARF (all) by month, by product"), ("Hist WARF exFSE by product", "WARF without FSE by month, by product"),
                   ("Hist WARF by product group", "WARF (all) by month, by product group"), ("Hist WARF by counterparty", "WARF (all) by month, by counterparty")):
    if sheet in WX:
        table(ws, pretty(WX[sheet]), label=lab)
widths(ws, first=40, rest=11); ws.freeze_panes = "B4"

ws = wb.create_sheet("WALs")
title(ws, HEAD, "Weighted Average Life per product. WAL Current = management WAL, WAL Calculated = this study's derived WAL.")
if len(_wal):
    _wshow = _wal[['mv_product', 'avg_balance', 'wal_current', 'wal_calculated']].copy()
    _wshow = _wshow[~_wshow.mv_product.isin({'(not in study)', 'UNMATCHED'})].sort_values('avg_balance', ascending=False, ignore_index=True)
    _wshow = pretty(_wshow)
    table(ws, _wshow, total=total_row(_wshow, 'Total / balance-weighted', sums=['Avg Balance'], wavg=['WAL Current (months)', 'WAL Calculated (months)'], w='Avg Balance'))
else:
    ws.append(['No WAL results yet: run 15_wal_ftp, then 09.'])
widths(ws); ws.freeze_panes = 'B5'

ws = wb.create_sheet("Product Group Summary")
title(ws, HEAD, "By product group: snapshot balance and WAR, the balance-weighted final beta, the truncation points and WARF. "
               "Group beta = each subproduct's balance x its product's final beta. The total row's WARF covers all balances (incl. accounts not in the study).")
WT_FIX = {NAMES[k]: WARF_TOTAL.get(NAMES[k]) for k in WKEEP}
table(ws, gsum, total=total_row(gsum, "Total / balance-weighted", sums=["Subproducts", "Accounts", "Balance"], wavg=["WAR", "Beta (balance-weighted)"], fixed=WT_FIX))
widths(ws); ws.freeze_panes = "B5"

ws = wb.create_sheet("Product Summary")
ws.sheet_properties.outlinePr.summaryBelow = False
title(ws, HEAD, "Every final metric per product. Click + on the left (or the 1 / 2 buttons) to open a product's subproducts.")
pcols = ["Product / Subproduct", "Bank", "Product Group", "Product Code", "Account Type", "Subproducts", "Accounts", "Balance", "WAR",
         "Recommended Beta", "Calc Method", "Avg Balance", "WAL Current (months)", "WAL Calculated (months)"] + [NAMES.get(c, f"Months to {c[10:]}%") for c in TH] + ["Truncation R²", "WARF (all)", "WARF without FSE", "Modified WARF", "FSE Share"]
ws.append(pcols)
for i in range(1, len(pcols) + 1):
    cell = ws.cell(ws.max_row, i)
    cell.font, cell.fill = Font(bold=True, color="FFFFFF"), HDR
    cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
ps = psum.set_index("mv_product")
meth = dict(zip(fb.mv_product, fb.final_method.fillna("")))
for p in ORDER + [x for x in ps.index if x not in ORDER]:
    r = ps.loc[p] if p in ps.index else pd.Series(dtype=float)
    s = sub[sub.mv_product == p].sort_values("snapshot_balance", ascending=False)
    vals = [p, ", ".join(sorted(s.legacy_bank.dropna().unique())), GROUP.get(p, ""), code_map.get(p, ""), "", r.get("subproducts"), r.get("accounts"),
            r.get("balance"), r.get("war"), r.get("beta"), meth.get(p, ""),
            wal_avg_map.get(p), wal_cur_map.get(p), wal_calc_map.get(p)] + [r.get(c) for c in TH] + [r.get("trunc_r2")] + [r.get("warf_all"), r.get("warf_ex_fse"), (r.get("warf_ex_fse") or 0) * 0.7 if r.get("warf_ex_fse") is not None else None, r.get("fse_share")]
    ws.append([_v(v) for v in vals])
    for i, c in enumerate(pcols, 1):
        cell = ws.cell(ws.max_row, i); cell.number_format, cell.border, cell.fill, cell.font = FMT(c), thin, PRODF, Font(bold=True)
    for x in s.itertuples():
        ws.append([_v(v) for v in [f"    {x.subproductid}", x.legacy_bank, x.product_group, "", x.acct_typ_desc or "", None, x.snapshot_accounts,
                                    x.snapshot_balance, x.snapshot_war]])
        for i, c in enumerate(pcols, 1):
            ws.cell(ws.max_row, i).number_format = FMT(c)
        ws.row_dimensions[ws.max_row].outline_level = 1
        ws.row_dimensions[ws.max_row].hidden = True
widths(ws); ws.column_dimensions["E"].width = 28; ws.freeze_panes = "B5"

ws = wb.create_sheet("Bank Summary")
title(ws, HEAD, "By legacy bank: snapshot balance and WAR, the balance-weighted final beta, the truncation points and WARF (WARF covers all balances).")
table(ws, bsum, total=total_row(bsum, "Total / balance-weighted", sums=["Subproducts", "Accounts", "Balance"], wavg=["WAR", "Beta (balance-weighted)"], fixed=WT_FIX))
widths(ws); ws.freeze_panes = "B5"

ws = wb.create_sheet("Study Summary")
title(ws, f"{S.tag} Deposit Study | Study summary", f"Built {pd.Timestamp.now():%Y-%m-%d %H:%M} from {STUDY_ROOT}")
kv = [("Study", S.tag), ("Snapshot date", S.snapshot_date), ("Mode", MODE), ("TTC cycle", TTC), ("Products", len(bet)),
      ("Snapshot balance", float(bet.Balance.sum())), ("Balance-weighted final beta", total_row(bet, "", wavg=["Recommended Beta"])[7]),
      ("WARF month", WMONTH or "WARF not run"), ("WARF (all) / without FSE", " / ".join(f"{WARF_TOTAL[k]:.2%}" for k in ("WARF (all)", "WARF without FSE") if WARF_TOTAL.get(k) is not None)),
      ("Truncation results", TR_XLSX.name if TR else "not run"), ("WARF results", WX_XLSX.name if WX else "not run")]
table(ws, pd.DataFrame(kv, columns=["Item", "Value"]), label="Study")
flat = []
for k, v in S.params.items():
    if str(k).startswith("_"):
        continue
    if isinstance(v, dict):
        flat += [(f"{k}.{kk}", str(vv)) for kk, vv in v.items() if not str(kk).startswith("_")]
    else:
        flat.append((k, str(v)))
table(ws, pd.DataFrame(flat, columns=["Setting", "Value"]), label="config/study_params.json")
table(ws, pd.read_csv(CONFIG / "rate_cycles.csv", dtype=str, keep_default_na=False), label="config/rate_cycles.csv")
if "Settings" in TR:
    table(ws, TR["Settings"], label="Truncations (11) settings")
if "Settings" in WX:
    table(ws, WX["Settings"], label="WARFs (13) settings")
tabs = [("Betas", "Final beta per product with the old beta, the rule's beta, method, R² and notes"),
        ("Betas Detail", "Every beta fit per product (lags 0 to 3, linear and poly, cycle betas)"),
        ("Truncations", "Months to 30 / 25 / 20% of month 0 accounts, per product"),
        ("Truncations Detail", "Fitted lines and the % of accounts still open by month, by product, group and bank"),
        ("WARFs", "WARF all / without FSE / FSE only per product at the WARF month"),
        ("WARFs Detail", "WARF by bank, counterparty, LCR rollup and the monthly history"),
        ("WALs", "Weighted Average Life: current and calculated per product"),
        ("Product Group Summary", "Balance, WAR, beta, truncation and WARF by product group"),
        ("Product Summary", "Every final metric per product, subproducts underneath"),
        ("Bank Summary", "Balance, WAR, beta, truncation and WARF by legacy bank"),
        ("Units", "Balances in dollars; WAR and WARF as percents; betas as decimals (0.45 = 45% pass-through); beta R² in percent; truncation R² 0 to 1")]
table(ws, pd.DataFrame(tabs, columns=["Tab", "What it shows"]), label="Tabs")
ws.column_dimensions["A"].width, ws.column_dimensions["B"].width = 36, 110
for i in range(3, 8):
    ws.column_dimensions[get_column_letter(i)].width = 16

FINAL_XLSX = ART["Final Reports"] / f"Final Deposits Study {S.tag}.xlsx"
wb.save(FINAL_XLSX)
print(f"{FINAL_XLSX}\n  tabs: {', '.join(wb.sheetnames)}\n  {len(bet)} products, {len(sub)} subproducts | truncation: {'yes' if TR else 'no'} | "
      f"WARF: {WMONTH or 'no'} | {int(bet['OLD BETA'].isna().sum())} products without an OLD BETA")
displayHTML(f'<a href="{folders.ws_link(ART["Final Reports"])}" target="_blank">Open Final Reports</a>')
display(pretty(psum))