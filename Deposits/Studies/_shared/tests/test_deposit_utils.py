"""Local tests (no Spark, no Databricks). Run from the Deposit Studies folder:  pytest _shared/tests -q"""
import ast, json, re, shutil, sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(ROOT / "_shared"))

from deposit_utils import betas, compare, config, folders, promote, sqlrun  # noqa: E402
from deposit_utils.study import Study  # noqa: E402


@pytest.fixture()
def studies(tmp_path):
    root = tmp_path / "Deposit Studies"
    shutil.copytree(ROOT / "_templates", root / "_templates")
    shutil.copytree(ROOT / "_shared", root / "_shared", ignore=shutil.ignore_patterns("tests", "__pycache__"))
    return root


# ------------------------------------------------------------------ sql
def test_split_statements_ignores_semicolons_in_strings_and_comments():
    sql = "-- a;b\nSELECT 'x;y' AS s; /* ; */ SELECT 2;\n-- only comment;\n"
    assert len(sqlrun.split_statements(sql)) == 2


def test_render_tokens_and_unknown_token_fails():
    assert sqlrun.render("FROM {{t}}", {"t": "finance.sandbox.x_2027v1"}) == "FROM finance.sandbox.x_2027v1"
    with pytest.raises(KeyError):
        sqlrun.render("{{missing}}", {})


# ------------------------------------------------------------------ folders
def test_create_first_version_from_templates(studies):
    d = folders.create_version(studies, 2027, log=lambda *_: None)
    assert d.name == "2027v1" and d.parent.name == "2027"
    for sub in ("notebooks", "inputs", "outputs/excel", "outputs/html", "docs", "archive"):
        assert (d / sub).is_dir()
    assert (d / "notebooks" / "00_START_HERE.ipynb").exists()
    assert json.loads((d / "study.json").read_text())["parent"] is None
    idx = pd.read_csv(studies / "studies_index.csv")
    assert list(idx["tag"]) == ["2027v1"]


def test_clone_copies_inputs_and_notebooks_but_not_outputs(studies):
    v1 = folders.create_version(studies, 2027, log=lambda *_: None)
    (v1 / "outputs" / "excel" / "report_2027v1.xlsx").write_text("x")
    (v1 / "inputs" / "config" / "product_config.csv").write_text("product,typ\nA,AUTO\n")
    v2 = folders.create_version(studies, 2027, clone_from="2027v1", notes="test", log=lambda *_: None)
    assert v2.name == "2027v2"
    assert (v2 / "inputs" / "config" / "product_config.csv").read_text() == "product,typ\nA,AUTO\n"
    assert not any((v2 / "outputs" / "excel").iterdir())
    m = json.loads((v2 / "study.json").read_text())
    assert m["parent"] == "2027v1" and m["notes"] == "test"
    assert "2027v1" in (v2 / "docs" / "change_log.md").read_text()
    assert list(pd.read_csv(studies / "studies_index.csv")["tag"]) == ["2027v1", "2027v2"]


def test_duplicate_version_and_bad_clone_source_are_rejected(studies):
    folders.create_version(studies, 2027, log=lambda *_: None)
    with pytest.raises(FileExistsError):
        folders.create_version(studies, 2027, version="v1", log=lambda *_: None)
    with pytest.raises(FileNotFoundError):
        folders.create_version(studies, 2027, clone_from="2027v9", log=lambda *_: None)


def test_dry_run_writes_nothing(studies):
    folders.create_version(studies, 2027, dry_run=True, log=lambda *_: None)
    assert not (studies / "2027").exists()


def test_next_version_and_tag_validation(studies):
    assert folders.next_version(studies, 2027) == "v1"
    folders.create_version(studies, 2027, log=lambda *_: None)
    folders.create_version(studies, 2027, log=lambda *_: None)
    assert folders.next_version(studies, 2027) == "v3"
    assert folders.next_version(studies, 2028) == "v1"
    with pytest.raises(ValueError):
        folders.parse_tag("2027-1")


def test_start_here_links_point_at_this_version(studies):
    d = folders.create_version(studies, 2027, log=lambda *_: None)
    nb = json.loads((d / "notebooks" / "00_START_HERE.ipynb").read_text())
    text = "".join(nb["cells"][0]["source"])
    assert "03_beta_models.ipynb" in text and "product_config.csv" in text and "#workspace" in text


# ------------------------------------------------------------------ study
def test_study_loads_from_nested_path_and_names_tables(studies):
    d = folders.create_version(studies, 2027, log=lambda *_: None)
    S = Study.load(root=d)
    assert S.tag == "2027v1" and S.table("nmds_agg") == "finance.sandbox.nmds_agg_2027v1"
    assert S.tokens()["snapshot_month"] == "2026-06"
    assert S.output("excel", "Report", "xlsx").name == "Report_2027v1.xlsx"
    from deposit_utils.study import find_study_root
    assert find_study_root(start=d / "notebooks") == d


def test_params_edit_changes_tokens(studies):
    d = folders.create_version(studies, 2027, log=lambda *_: None)
    p = d / "inputs" / "config" / "study_params.json"
    j = json.loads(p.read_text()); j["snapshot_date"] = "2027-06-30"; j["ftp_results_date"] = ""
    p.write_text(json.dumps(j))
    S = Study.load(root=d)
    assert S.tokens()["snapshot_date"] == "2027-06-30" and S.tokens()["ftp_results_date"] == "2027-06-30"


# ------------------------------------------------------------------ config
def test_shipped_product_config_is_valid(studies):
    d = folders.create_version(studies, 2027, log=lambda *_: None)
    S = Study.load(root=d)
    pc = config.ProductConfig.from_study(S)
    assert len(pc.cfg) == 36
    assert config.validate(pc.cfg, S.cycle_lookup) == []


def test_config_validation_catches_mistakes():
    df = pd.DataFrame([dict(product="P1", typ="AVG_M", m1="POLY", m2="", lag="1", cyc="TTC", beta="", note=""),
                       dict(product="P2", typ="MAN", m1="", m2="", lag="", cyc="", beta="", note=""),
                       dict(product="P3", typ="PURE", m1="LR", m2="", lag="7", cyc="ZZZ", beta="", note="")])
    errs = config.validate(config.load_product_config_df(df), {"TTC": "TTC PD"})
    joined = " | ".join(errs)
    assert "P1: typ AVG_M requires 'm2'" in joined
    assert "P2: typ MAN requires 'beta'" in joined
    assert "lag=7" in joined and "cyc='ZZZ'" in joined


# ------------------------------------------------------------------ betas
def _synthetic():
    dates = pd.date_range("2018-12-01", "2026-08-31", freq="MS")
    r, rates = 2.5, []
    for d in dates:
        if pd.Timestamp("2019-08-01") <= d < pd.Timestamp("2020-04-01"): r = max(0.25, r - 0.25)
        if pd.Timestamp("2022-03-01") <= d < pd.Timestamp("2023-08-01"): r = min(5.5, r + 0.25)
        if pd.Timestamp("2024-09-01") <= d < pd.Timestamp("2026-06-01"): r = max(3.5, r - 0.1)
        rates.append(r)
    raw = pd.DataFrame({"effective_date": dates + pd.offsets.MonthEnd(0), "UP_FFR": rates,
                        "LB_FFR": np.array(rates) - .25, "EFFR": np.array(rates) - .1})
    return betas.effr_monthly_from_raw(raw)


def test_beta_engine_recovers_planted_betas(studies):
    em = _synthetic()
    S = Study.load(root=folders.create_version(studies, 2027, log=lambda *_: None))
    nc = betas.build_named_cycles(em, S.cycles)
    assert [n for n, _ in nc] == list(S.cycles["name"])
    up = em.set_index("effective_date")["UP_FFR"]
    war = pd.DataFrame({"EMP_NAME": ["A", "B"]})
    for d in em["effective_date"]:
        war.loc[0, d.strftime("%Y-%m-%d")] = 0.40 * up[d] + 0.1
        war.loc[1, d.strftime("%Y-%m-%d")] = 0.10 * up[d]
    final, short = betas.run_all_betas(war, nc, em)
    assert final.shape[1] == 141
    a = short.set_index("EMP_NAME").loc["A"]
    assert a["lm_0_TTC_PD"] == pytest.approx(40.0, abs=0.01) and a["r2_0_TTC_PD"] == pytest.approx(100.0, abs=0.01)
    assert a["pb_0_TTC_PD"] == pytest.approx(40.0, abs=0.05)
    assert short.set_index("EMP_NAME").loc["B", "lm_0_TTC_PD"] == pytest.approx(10.0, abs=0.01)


def test_changing_a_cycle_window_changes_months(studies):
    em = _synthetic()
    d = folders.create_version(studies, 2027, log=lambda *_: None)
    f = d / "inputs" / "config" / "rate_cycles.csv"
    df = pd.read_csv(f, dtype=str); df.loc[df.name == "500bpsUP", "end"] = "2023-05-31"; df.to_csv(f, index=False)
    nc = dict(betas.build_named_cycles(em, Study.load(root=d).cycles))
    assert len(nc["500bpsUP"]) == 15


# ------------------------------------------------------------------ compare
def _fake_outputs(root, tag, rows):
    df = pd.DataFrame(rows, columns=["study", "pg", "product", "method", "cycle", "cycle_disp", "r2", "emp_beta",
                                     "calc_beta", "rec_beta", "war", "curr_bal", "avg_bal", "notes"])
    p = root / "outputs" / "flatfiles"; p.mkdir(parents=True, exist_ok=True)
    df.to_csv(p / f"beta_summary_{tag}.csv", index=False)


def test_compare_versions_end_to_end(studies, tmp_path):
    v1 = folders.create_version(studies, 2027, log=lambda *_: None)
    v2 = folders.create_version(studies, 2027, clone_from="2027v1", log=lambda *_: None)
    # change method for one product, add a manual override, change one mapping row, change the snapshot date
    pc = v2 / "inputs" / "config" / "product_config.csv"
    df = pd.read_csv(pc, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    df.loc[df["product"] == "MMA Consumer Standard", "lag"] = "2"
    df.to_csv(pc, index=False, encoding="utf-8-sig")
    fm = v2 / "inputs" / "product_maps" / "future_products_map.csv"
    fdf = pd.read_csv(fm, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    fdf.loc[0, "recommended_product_group"] = "Commercial"
    fdf = pd.concat([fdf, pd.DataFrame([dict(zip(fdf.columns, ["SNV", "999", "New", "999", "New", "Checking", "Consumer", "Consumer"]))])])
    fdf.to_csv(fm, index=False, encoding="utf-8-sig")
    sp = v2 / "inputs" / "config" / "study_params.json"
    j = json.loads(sp.read_text()); j["snapshot_date"] = "2027-06-30"; sp.write_text(json.dumps(j))

    cols = ("2027v1", "MMA", "MMA Consumer Standard", "Linear Regression Lag 1", "TTC PD", "TTC", 90.0, 0.30, 0.25, 0.275, 0.03, 1000.0, 990.0, "")
    _fake_outputs(v1, "2027v1", [cols, ("2027v1", "Savings", "Savings Consumer", "Manual Override", "", "", None, 0.05, 0.05, 0.05, 0.01, 500.0, 500.0, ""),
                                 ("2027v1", "MMA", "Gone Product", "AUTO", "TTC PD", "TTC", 80.0, .2, .2, .2, .02, 10.0, 10.0, "")])
    _fake_outputs(v2, "2027v2", [("2027v2", "MMA", "MMA Consumer Standard", "Linear Regression Lag 2", "TTC PD", "TTC", 92.0, 0.30, 0.31, 0.305, 0.03, 1100.0, 1090.0, ""),
                                 ("2027v2", "Savings", "Savings Consumer", "Manual Override", "", "", None, 0.05, 0.05, 0.05, 0.01, 500.0, 500.0, ""),
                                 ("2027v2", "ICS MMA", "New Product", "AUTO", "TTC PD", "TTC", 70.0, .3, .3, .3, .03, 5.0, 5.0, "")])
    res = compare.compare_versions(v1, v2, log=lambda *_: None)
    summ = res["input_summary"].set_index("file")
    assert summ.loc["product_maps/future_products_map.csv", "status"] == "different"
    assert summ.loc["product_maps/future_products_map.csv", "added"] == 1       # the new account type
    assert summ.loc["product_maps/future_products_map.csv", "changed"] == 1     # edited row is paired, not removed + added
    assert summ.loc["product_maps/snv_acct_map.csv", "status"] == "same"
    assert summ.loc["config/study_params.json", "changed"] == 1
    det = res["input_detail"]
    assert ((det.file == "config/product_config.csv") & (det.column == "lag") & (det.old == "1") & (det.new == "2")).any()
    p = res["products"].set_index("product")
    assert p.loc["MMA Consumer Standard", "rec_beta_delta"] == pytest.approx(0.03)
    assert bool(p.loc["MMA Consumer Standard", "method_changed"]) and not bool(p.loc["Savings Consumer", "beta_changed"])
    assert p.loc["Gone Product", "status"] == "removed in B" and p.loc["New Product", "status"] == "added in B"
    out = compare.write_comparison_workbook(res, studies / "2027" / "comparisons" / "x.xlsx")
    names = pd.ExcelFile(out).sheet_names
    assert {"Overview", "Input Files", "Input Changes", "Beta by Product", "Beta by Group"} <= set(names)


def test_diff_frames_handles_duplicate_keys():
    a = pd.DataFrame({"k": ["1", "1", "2"], "v": ["a", "b", "c"]})
    b = pd.DataFrame({"k": ["1", "1", "2"], "v": ["a", "b", "z"]})
    d = compare.diff_frames(a, b, ["k"])
    assert d["mode"] == "multiset" and len(d["added"]) == 0 and len(d["removed"]) == 0
    assert d["changed"].to_dict("records") == [{"k": "2", "column": "v", "old": "c", "new": "z"}]
    d2 = compare.diff_frames(a, pd.concat([b, pd.DataFrame({"k": ["3"], "v": ["n"]})]), ["k"])
    assert len(d2["added"]) == 1


# ------------------------------------------------------------------ promote (fake spark)
class _FakeDF:
    def __init__(self, n, schema): self._n, self.schema = n, schema
    def count(self): return self._n


class _Field:
    def __init__(self, name, t): self.name, self.dataType = name, type("T", (), {"simpleString": lambda s: t})()


class _FakeSpark:
    def __init__(self, tables): self.tables, self.sql_log = tables, []
    def table(self, name):
        if name not in self.tables: raise KeyError(name)
        n, cols = self.tables[name]
        return _FakeDF(n, type("S", (), {"fields": [_Field(c, t) for c, t in cols]})())
    def sql(self, q):
        self.sql_log.append(q)
        return type("R", (), {"collect": lambda s: [["tester"]]})()


def _promo_study(studies, include):
    d = folders.create_version(studies, 2027, log=lambda *_: None)
    f = d / "inputs" / "config" / "promotion.csv"
    df = pd.read_csv(f, dtype=str, keep_default_na=False); df["include"] = "N"
    df.loc[df.sandbox_table == "nmds_agg", "include"] = include
    df.to_csv(f, index=False)
    cols = [("a", "int"), ("b", "string")]
    spark = _FakeSpark({"finance.sandbox.nmds_agg_2027v1": (100, cols), "finance.pricing_analytics.nmds_agg": (90, cols)})
    return Study.load(root=d, spark=spark)


def test_promote_requires_exact_confirmation_and_never_runs_on_dry_run(studies):
    S = _promo_study(studies, "Y")
    plan = promote.dry_run(S)
    row = plan[plan.sandbox_table == "nmds_agg"].iloc[0]
    assert row.source_rows == 100 and row.target_rows == 90 and row.row_change == 10 and row.schema_matches_target
    with pytest.raises(PermissionError):
        promote.execute(S, confirm="yes", log=lambda *_: None)
    assert not any("CREATE OR REPLACE TABLE finance.pricing_analytics" in q for q in S.spark.sql_log)


def test_promote_backs_up_then_writes_and_logs(studies):
    S = _promo_study(studies, "Y")
    res = promote.execute(S, confirm="PROMOTE 2027v1", log=lambda *_: None)
    log = [q for q in S.spark.sql_log if q.startswith("CREATE OR REPLACE TABLE")]
    assert "DEEP CLONE finance.pricing_analytics.nmds_agg" in log[0]
    assert log[1].startswith("CREATE OR REPLACE TABLE finance.pricing_analytics.nmds_agg AS SELECT * FROM finance.sandbox.nmds_agg_2027v1")
    assert list(res.status) == ["ok"] and S.output("logs", "promotion_log", "csv").exists()


def test_promote_blocks_sandbox_target_and_skips_excluded(studies):
    S = _promo_study(studies, "N")
    assert promote.execute(S, confirm="PROMOTE 2027v1", log=lambda *_: None).empty
    S2 = _promo_study(studies / "again", "Y") if False else None
    f = S.input("config/promotion.csv")
    df = pd.read_csv(f, dtype=str, keep_default_na=False)
    df.loc[df.sandbox_table == "nmds_agg", ["include", "target_table"]] = ["Y", "finance.sandbox.oops"]
    df.to_csv(f, index=False)
    with pytest.raises(RuntimeError):
        promote.execute(S, confirm="PROMOTE 2027v1", log=lambda *_: None)


# ------------------------------------------------------------------ notebooks
NB = sorted((ROOT / "_templates" / "notebooks").glob("*.ipynb"))


@pytest.mark.parametrize("path", NB, ids=[p.name for p in NB])
def test_notebook_cells_compile_and_have_no_hardcoded_study_values(path):
    nb = json.loads(path.read_text(encoding="utf-8"))
    for i, c in enumerate(nb["cells"]):
        if c["cell_type"] != "code":
            continue
        src = "".join(c["source"])
        ast.parse("\n".join(l for l in src.split("\n") if not l.startswith("%pip")))
        assert "synovus.com" not in src and "/Workspace/Users" not in src, f"{path.name} cell {i}"
        assert not re.search(r"(?<![\w{])'2026-\d\d-\d\d'", src), f"{path.name} cell {i}: hardcoded date"


def test_dim_nmd_seed_writes_to_inputs_product_maps_not_nested(studies):
    from deposit_utils import spark_io

    class _Tbl:
        def toPandas(self): return pd.DataFrame({"subproductid": ["a"], "product": ["p"]})

    class _Sp:
        def table(self, name): return _Tbl()

    d = folders.create_version(studies, 2027, log=lambda *_: None)
    S = Study.load(root=d, spark=_Sp())
    out = spark_io.seed_dim_nmd_csv(S, log=lambda *_: None)
    assert out == d / "inputs" / "product_maps" / "dim_nmd.csv" and out.exists()
    assert not (d / "inputs" / "inputs").exists()
    assert list(S.read_input("product_maps/dim_nmd.csv")["product"]) == ["p"]
    assert spark_io.seed_dim_nmd_csv(S, log=lambda *_: None) == out      # second call leaves it alone


# ------------------------------------------------------------------ repair
def test_repair_merges_stray_nested_inputs_and_restores_missing(studies):
    d = folders.create_version(studies, 2027, log=lambda *_: None)
    stray = d / "inputs" / "inputs" / "product_maps"
    stray.mkdir(parents=True)
    (stray / "dim_nmd.csv").write_text("subproductid\nA\n")                 # only exists in the stray tree
    (stray / "snv_acct_map.csv").write_text("DIFFERENT\n")                  # also exists in inputs/, differs
    (d / "inputs" / "product_maps" / "pnfp_acct_map.csv").unlink()           # missing file to restore
    dry = folders.repair_version(studies, "2027v1", apply=False, log=lambda *_: None)
    assert (d / "inputs" / "inputs").exists() and len(dry) >= 3              # dry run changes nothing
    folders.repair_version(studies, "2027v1", apply=True, log=lambda *_: None)
    assert not (d / "inputs" / "inputs").exists()
    assert (d / "inputs" / "product_maps" / "dim_nmd.csv").read_text() == "subproductid\nA\n"
    assert (d / "archive" / "stray_inputs_backup" / "product_maps" / "snv_acct_map.csv").exists()
    assert (d / "inputs" / "product_maps" / "snv_acct_map.csv").read_text() != "DIFFERENT\n"
    assert (d / "inputs" / "product_maps" / "pnfp_acct_map.csv").exists()
    assert folders.repair_version(studies, "2027v1", apply=False, log=lambda *_: None) == []   # idempotent


def test_clone_of_a_version_with_stray_folder_comes_out_clean(studies):
    v1 = folders.create_version(studies, 2027, log=lambda *_: None)
    (v1 / "inputs" / "inputs" / "product_maps").mkdir(parents=True)
    (v1 / "inputs" / "inputs" / "product_maps" / "dim_nmd.csv").write_text("x\n")
    v2 = folders.create_version(studies, 2027, clone_from="2027v1", log=lambda *_: None)
    assert not (v2 / "inputs" / "inputs").exists()
    assert (v2 / "inputs" / "product_maps" / "dim_nmd.csv").exists()


# ------------------------------------------------------------------ lint
def _nb(path, code):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"cells": [{"cell_type": "code", "metadata": {}, "outputs": [], "execution_count": None,
                                           "source": code.splitlines(True)}], "nbformat": 4, "nbformat_minor": 5, "metadata": {}}))


def test_lint_flags_the_hardcodes_that_broke_v4_and_passes_tokens(tmp_path):
    from deposit_utils import lint
    bad = tmp_path / "bad" / "notebooks" / "04.ipynb"
    _nb(bad, 'x = spark.sql("SELECT * FROM finance.sandbox.ccalc_beta_results_2027v2 WHERE d = \'2026-06-30\'")\n'
             'p = "/Workspace/Users/someone/out.xlsx"\n# finance.sandbox.ignored_2027v1 in a comment\n')
    found = lint.scan_version(tmp_path / "bad", log=lambda *_: None)
    rules = {f["rule"] for f in found}
    assert {"literal table name", "literal date", "literal workspace path"} <= rules
    good = tmp_path / "good" / "notebooks" / "04.ipynb"
    _nb(good, 'x = sqlq("SELECT * FROM {{ccalc_beta_results}} WHERE d = \'{{snapshot_date}}\'")\nS.table("nmds_agg")\n')
    assert lint.scan_version(tmp_path / "good", log=lambda *_: None) == []


def test_shipped_templates_pass_their_own_lint():
    from deposit_utils import lint
    assert lint.scan_version(ROOT / "_templates", log=lambda *_: None) == []


def test_optional_logic_blocks_are_present_and_fully_commented():
    """The user's Standard Plus / product-mapping logic ships commented out, so enabling is uncommenting."""
    expect = {"01_build_tables": ["OPTION 1", "OPTION 2a", "OPTION 2b", "OPTION 2c", "OPTION 3"],
              "02_product_mapping": ["OPTION P", "OPTION S"]}
    for name, markers in expect.items():
        nb = json.loads((ROOT / "_templates" / "notebooks" / f"{name}.ipynb").read_text(encoding="utf-8"))
        text = "\n".join("".join(c["source"]) for c in nb["cells"] if c["cell_type"] == "code")
        for m in markers:
            assert m in text, f"{name} missing {m}"
        for line in text.split("\n"):
            if "OPTION" in line:
                assert line.strip().startswith("--"), f"{name}: OPTION line is not a comment: {line.strip()[:80]}"
    assert (ROOT / "_templates" / "docs" / "optional_logic.md").exists()
    text01 = "\n".join("".join(c["source"]) for c in json.loads((ROOT / "_templates" / "notebooks" / "01_build_tables.ipynb").read_text())["cells"] if c["cell_type"] == "code")
    # live logic is untouched: the original dnc column, the AND 787 guard and the plain dn.product line are still live
    live = [l.strip() for l in text01.split("\n") if not l.strip().startswith("--")]
    assert "nmds.dnc AS dnc_flag," in live and "AND nmds.prime_rate_num <> '787'" in live and "dn.product," in live
