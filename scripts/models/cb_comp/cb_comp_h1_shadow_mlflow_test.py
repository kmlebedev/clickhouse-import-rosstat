#!/usr/bin/env python3
"""Офлайн-тесты MLflow-зеркала shadow `wk_mix_ex@v1` (P0.6a #462).

MLflow — реальный `MlflowClient` на временном SQLite tracking URI
(`sqlite:///<tmp>/mlflow.db`), артефакты — во временном каталоге. Сети,
ClickHouse и `build/mlflow` нет.

Запуск: `make cb-comp-h1-shadow-mlflow-test`.
"""

import contextlib
import io
import json
import math
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from mlflow.tracking import MlflowClient

from models.cb_comp import cb_comp_h1_shadow_mlflow as m

SHA = "a" * 64
PANEL_SHA = "b" * 64


def payload(**overrides) -> dict:
    base = {
        "version": m.VERSION,
        "month": "2026-10",
        "cutoff": "2026-11-08",
        "run_date": "2026-10-31",
        "git_sha": "64c1a9b",
        "v1_code_sha": "264ce73",
        "spec": "docs/superpowers/specs/2026-10-31-cb-comp-h1-shadow-p06-design.md",
        "unit": "п.п., м/м SA ИПЦ без КУ (h1-nowcast)",
        "models": {
            "combo3": 0.4966762829675247,
            "wk_mix_ex": 0.4556346483403635,
            "wk_mix_d": 0.6703750277911844,
            "live_cb_wk": 0.41460677159114545,
        },
        "components": {
            "live_cb": 0.3979680798451001,
            "bs_econ16": 0.5675301420602414,
            "bs_rozn16": 0.5245306269972325,
            "wk_agg_ex": 0.41459301371320223,
            "wk_agg_d": 0.8440737726148442,
        },
        "served": {"model": "wk_mix_ex", "value": 0.4556346483403635, "fallback_level": 0},
        "weekly_features": {
            "agg_cov": 0.8387096774193549,
            "agg_mtd": 0.9266054895529452,
            "util_cov": 0.8292395693135935,
            "util_mtd": 9.800967458881882,
            "w_zhku": 0.05944,
            "wkagg_ex_mtd": 0.3905068894408791,
        },
        "asof": {
            "panel_last_month": "2026-09-01",
            "weekly_last_point": "2026-10-26",
            "weekly_aggregate_last_point": "2026-10-26",
            "survey_price_expectations_sa_last": "2026-09-01",
            "public_panel_last": "2026-09-01",
            "bs_regressor_month": "2026-09-01",
            "utilities_last_point": {
                "Водоотведение, м3": "2026-10-26",
                "Газ сжиженный, м3": "2025-12-22",
            },
        },
        "inputs_sha256": {
            "panel_csv": PANEL_SHA,
            "public_panel_csv": "c" * 64,
        },
        "s0": {
            "months": ["2026-07-01", "2026-08-01", "2026-09-01"],
            "identical": True,
            "combo3": {"n": 3, "max_abs_diff": 0.0},
            "wk_mix_ex": {"n": 3, "max_abs_diff": 0.0},
        },
        "truth": {
            "primary": "T_LIVE_rt[1]: SA ИПЦ без КУ из 88 групп",
            "secondary": "T_LIVE[1]: финальная SA",
        },
        "actual": None,
    }
    base.update(overrides)
    return base


def write_json(path: Path, data: dict) -> str:
    text = json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    Path(path).write_text(text, encoding="utf-8")
    return text


class SchemaTest(unittest.TestCase):
    def test_valid_payload(self):
        m.validate_payload(payload())

    def test_invalid_payloads(self):
        broken = payload()
        del broken["asof"]
        cases = [
            broken,
            payload(version="wk_mix_ex@v2"),
            payload(run_date="2026-11-09"),
            payload(month="2026-13"),
            payload(models={"combo3": 0.2, "wk_mix_ex": 0.2, "wk_mix_d": 0.2,
                           "live_cb_wk": float("nan")}),
            payload(served={"model": "wk_agg_ex", "value": 0.1, "fallback_level": 0}),
            payload(served={"model": "wk_mix_ex", "value": 0.1, "fallback_level": 3}),
            payload(inputs_sha256={"panel_csv": "abc"}),
            payload(actual={"t_live": 0.5}),
            payload(actual=0.5),
            payload(truth={"primary": ""}),
        ]
        for item in cases:
            with self.subTest(item=item.get("version")):
                with self.assertRaises(m.ShadowJsonError):
                    m.validate_payload(item)

    def test_load_payload_sha_and_errors(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "2026-10.json"
            write_json(path, payload())
            got, text, sha = m.load_payload(path)
            self.assertEqual(got["month"], "2026-10")
            self.assertEqual(sha, m.sha256_bytes(path.read_bytes()))
            self.assertEqual(len(sha), 64)
            self.assertEqual(text, path.read_text(encoding="utf-8"))

            bad = Path(tmp) / "bad.json"
            bad.write_text("{", encoding="utf-8")
            with self.assertRaises(m.ShadowJsonError):
                m.load_payload(bad)
            bad.write_text("[]", encoding="utf-8")
            with self.assertRaises(m.ShadowJsonError):
                m.load_payload(bad)

    def test_real_2026_10_payload(self):
        path = m.ROOT / "docs" / "research" / "cb-comp-h1-shadow" / "2026-10.json"
        if not path.exists():
            self.skipTest("нет shadow-JSON 2026-10")
        data, _text, sha = m.load_payload(path)
        plan = m.build_plan(data, path, sha)
        self.assertEqual(plan["run_name"], "shadow__2026-10__wk_mix_ex@v1")
        self.assertEqual(plan["tags"]["json_sha256"], sha)
        self.assertAlmostEqual(plan["metrics"]["pred.wk_mix_ex"], 0.4556346483, places=9)
        self.assertNotIn("error_bp", plan["metrics"])

    def test_safe_keys_are_mlflow_compatible(self):
        allowed = __import__("re").compile(r"^[\w.\-/ ]+$")
        plan = m.build_plan(payload(), Path("2026-10.json"), SHA)
        for key in list(plan["params"]) + list(plan["metrics"]):
            self.assertRegex(key, allowed)
        self.assertIn("asof.utilities_last_point.Водоотведение_ м3", plan["params"])


class PlanTest(unittest.TestCase):
    def test_tags_and_params(self):
        plan = m.build_plan(payload(), Path("docs/research/cb-comp-h1-shadow/2026-10.json"), SHA)
        tags = plan["tags"]
        self.assertEqual(tags["decision"], "shadow")
        self.assertEqual(tags["stage"], "paper_shadow")
        self.assertEqual(tags["champion"], "combo3")
        self.assertEqual(tags["challenger"], "wk_mix_ex@v1")
        self.assertEqual(tags["model_version"], "wk_mix_ex@v1")
        self.assertEqual(tags["source"], "json")
        self.assertEqual(tags["json_sha256"], SHA)
        self.assertEqual(tags["month"], "2026-10")
        self.assertEqual(tags["resolved"], "0")
        params = plan["params"]
        self.assertEqual(params["month"], "2026-10")
        self.assertEqual(params["cutoff"], "2026-11-08")
        self.assertEqual(params["input.panel_csv_sha256"], PANEL_SHA)
        self.assertEqual(params["source_path"], "docs/research/cb-comp-h1-shadow/2026-10.json")
        self.assertEqual(params["served.model"], "wk_mix_ex")
        self.assertEqual(params["s0.identical"], "1")
        metrics = plan["metrics"]
        self.assertAlmostEqual(metrics["pred.combo3"], 0.496676283, places=9)
        self.assertAlmostEqual(metrics["pred.bs_econ16"], 0.567530142, places=9)
        self.assertAlmostEqual(metrics["served.value"], 0.455634648, places=9)
        self.assertAlmostEqual(metrics["weekly_features.util_mtd"], 9.800967459, places=9)
        self.assertEqual(metrics["s0.max_abs_diff.combo3"], 0.0)
        self.assertNotIn("actual.t_live_rt", metrics)

    def test_actual_metrics(self):
        plan = m.build_plan(
            payload(actual={"t_live_rt": 0.5, "t_live": 0.55}),
            Path("2026-10.json"), SHA,
        )
        metrics = plan["metrics"]
        self.assertAlmostEqual(metrics["actual.t_live_rt"], 0.5, places=12)
        self.assertAlmostEqual(metrics["actual.t_live"], 0.55, places=12)
        self.assertAlmostEqual(metrics["error_bp"], -4.43653516596365, places=9)
        self.assertAlmostEqual(metrics["abs_error_bp"], 4.43653516596365, places=9)
        self.assertEqual(plan["tags"]["resolved"], "1")
        self.assertAlmostEqual(plan["summary"]["errors_bp"]["served"], -4.43653516596365, places=9)
        self.assertAlmostEqual(plan["summary"]["errors_bp"]["combo3"], -0.33237170324753, places=9)

    def test_actual_upper_case_alias(self):
        plan = m.build_plan(
            payload(actual={"T_LIVE_rt": 0.5, "T_LIVE": 0.55}),
            Path("2026-10.json"), SHA,
        )
        self.assertAlmostEqual(plan["metrics"]["actual.t_live_rt"], 0.5, places=12)

    def test_display_path_outside_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "2026-10.json"
            self.assertEqual(m.display_path(path), "2026-10.json")

    def test_collect_paths_rules(self):
        self.assertEqual(m.collect_paths(month="2026-10")[0].name, "2026-10.json")
        self.assertEqual(m.collect_paths(path="x.json")[0], Path("x.json"))
        with self.assertRaises(m.ShadowJsonError):
            m.collect_paths()
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(m, "SHADOW_DIR", Path(tmp)):
            with self.assertRaises(m.ShadowJsonError):
                m.collect_paths(backfill=True)


class MLflowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.old_cwd = os.getcwd()
        os.chdir(self.root)
        self.addCleanup(os.chdir, self.old_cwd)
        self.uri = f"sqlite:///{self.root / 'mlflow.db'}"
        self.client = MlflowClient(tracking_uri=self.uri)
        self.path = self.root / "2026-10.json"
        self.text = write_json(self.path, payload())

    def runs(self):
        experiment = self.client.get_experiment_by_name(m.EXPERIMENT)
        if experiment is None:
            return []
        return self.client.search_runs([experiment.experiment_id])

    def test_created_schema_and_artifacts(self):
        result = m.log_path(self.path, self.uri)
        self.assertEqual(result["status"], "created")
        experiment = self.client.get_experiment_by_name(m.EXPERIMENT)
        self.assertIsNotNone(experiment)
        self.assertEqual(result["experiment_id"], experiment.experiment_id)
        runs = self.runs()
        self.assertEqual(len(runs), 1)
        run = runs[0]
        self.assertEqual(run.info.run_name, "shadow__2026-10__wk_mix_ex@v1")
        self.assertEqual(run.info.status, "FINISHED")
        tags = run.data.tags
        self.assertEqual(tags["decision"], "shadow")
        self.assertEqual(tags["stage"], "paper_shadow")
        self.assertEqual(tags["champion"], "combo3")
        self.assertEqual(tags["challenger"], "wk_mix_ex@v1")
        self.assertEqual(tags["model_version"], "wk_mix_ex@v1")
        self.assertEqual(tags["source"], "json")
        self.assertEqual(tags["json_sha256"], m.sha256_bytes(self.path.read_bytes()))
        self.assertEqual(tags["resolved"], "0")
        params = run.data.params
        self.assertEqual(params["month"], "2026-10")
        self.assertEqual(params["input.panel_csv_sha256"], PANEL_SHA)
        self.assertAlmostEqual(run.data.metrics["pred.wk_mix_ex"], 0.4556346483, places=9)
        self.assertNotIn("error_bp", run.data.metrics)

        names = {item.path for item in self.client.list_artifacts(run.info.run_id)}
        self.assertEqual(names, {"shadow.json", "run_summary.json"})
        with tempfile.TemporaryDirectory() as tmp:
            got = self.client.download_artifacts(run.info.run_id, "shadow.json", tmp)
            self.assertEqual(Path(got).read_bytes(), self.path.read_bytes())
            summary_path = self.client.download_artifacts(
                run.info.run_id, "run_summary.json", tmp
            )
            summary = json.loads(Path(summary_path).read_text(encoding="utf-8"))
        self.assertEqual(summary["json_sha256"], tags["json_sha256"])
        self.assertEqual(summary["tags"]["source"], "json")
        self.assertEqual(summary["errors_bp"], {})
        self.assertEqual(summary["source_path"], "2026-10.json")

    def test_idempotency_and_force(self):
        first = m.log_path(self.path, self.uri)
        second = m.log_path(self.path, self.uri)
        self.assertEqual(second["status"], "skipped")
        self.assertEqual(second["run_id"], first["run_id"])
        self.assertEqual(len(self.runs()), 1)

        forced = m.log_path(self.path, self.uri, force=True)
        self.assertEqual(forced["status"], "created")
        self.assertNotEqual(forced["run_id"], first["run_id"])
        self.assertEqual(len(self.runs()), 2)

    def test_dry_run_no_write_without_uri(self):
        with mock.patch.dict(os.environ, {m.TRACKING_URI_ENV: ""}, clear=False):
            result = m.log_path(self.path, None, dry_run=True)
        self.assertEqual(result["status"], "would-create")
        self.assertEqual(result["plan"]["run_name"], "shadow__2026-10__wk_mix_ex@v1")
        self.assertIsNone(self.client.get_experiment_by_name(m.EXPERIMENT))
        self.assertEqual(self.runs(), [])

    def test_missing_tracking_uri(self):
        with mock.patch.dict(os.environ, {m.TRACKING_URI_ENV: ""}, clear=False):
            with self.assertRaises(m.MissingTrackingUriError) as ctx:
                m.log_path(self.path, None)
        self.assertIn("staging", str(ctx.exception))
        self.assertIn("--tracking-uri", str(ctx.exception))
        self.assertIn("build/mlflow", str(ctx.exception))
        self.assertIsNone(self.client.get_experiment_by_name(m.EXPERIMENT))
        self.assertEqual(self.runs(), [])

    def test_resolve_tracking_uri_prefers_argument_then_env(self):
        with mock.patch.dict(os.environ, {m.TRACKING_URI_ENV: "http://127.0.0.1:15001"},
                             clear=False):
            self.assertEqual(m.resolve_tracking_uri(None), "http://127.0.0.1:15001")
            self.assertEqual(m.resolve_tracking_uri("sqlite:///x.db"), "sqlite:///x.db")
        with mock.patch.dict(os.environ, {m.TRACKING_URI_ENV: ""}, clear=False):
            self.assertIsNone(m.resolve_tracking_uri(None))

    def test_actual_metrics_logged(self):
        write_json(self.path, payload(actual={"t_live_rt": 0.5, "t_live": 0.55}))
        result = m.log_path(self.path, self.uri)
        self.assertEqual(result["status"], "created")
        metrics = self.runs()[0].data.metrics
        self.assertAlmostEqual(metrics["actual.t_live_rt"], 0.5, places=12)
        self.assertAlmostEqual(metrics["actual.t_live"], 0.55, places=12)
        self.assertAlmostEqual(metrics["error_bp"], -4.43653516596365, places=9)
        self.assertAlmostEqual(metrics["abs_error_bp"], 4.43653516596365, places=9)
        self.assertEqual(self.runs()[0].data.tags["resolved"], "1")

    def test_month_mismatch(self):
        with self.assertRaises(m.ShadowJsonError):
            m.log_path(self.path, self.uri, expected_month="2026-11")
        self.assertEqual(self.runs(), [])

    def test_main_backfill_and_repeat(self):
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(m, "SHADOW_DIR", Path(tmp)):
            write_json(Path(tmp) / "2026-10.json", payload())
            write_json(
                Path(tmp) / "2026-11.json",
                payload(month="2026-11", cutoff="2026-12-08", run_date="2026-11-15"),
            )
            with contextlib.redirect_stdout(io.StringIO()) as out:
                self.assertEqual(m.main(["--backfill", "--tracking-uri", self.uri]), 0)
            self.assertEqual(len(self.runs()), 2)
            self.assertIn("shadow__", out.getvalue())
            with contextlib.redirect_stdout(io.StringIO()) as out:
                self.assertEqual(m.main(["--backfill", "--tracking-uri", self.uri]), 0)
            self.assertEqual(len(self.runs()), 2)
            self.assertIn("skipped", out.getvalue())

    def test_main_month_dry_run_without_uri(self):
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.object(m, "SHADOW_DIR", Path(tmp)), \
                mock.patch.dict(os.environ, {m.TRACKING_URI_ENV: ""}, clear=False):
            write_json(Path(tmp) / "2026-10.json", payload())
            with contextlib.redirect_stdout(io.StringIO()) as out:
                self.assertEqual(
                    m.main(["--month", "2026-10", "--dry-run"]), 0
                )
            self.assertIn("shadow__2026-10__wk_mix_ex@v1", out.getvalue())
            self.assertIsNone(self.client.get_experiment_by_name(m.EXPERIMENT))

    def test_main_requires_tracking_uri(self):
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.object(m, "SHADOW_DIR", Path(tmp)), \
                mock.patch.dict(os.environ, {m.TRACKING_URI_ENV: ""}, clear=False):
            write_json(Path(tmp) / "2026-10.json", payload())
            with contextlib.redirect_stderr(io.StringIO()) as err:
                self.assertEqual(m.main(["--month", "2026-10"]), 1)
            self.assertIn("staging", err.getvalue())
            self.assertIn("--tracking-uri", err.getvalue())
            self.assertEqual(self.runs(), [])

    def test_main_uses_tracking_uri_env(self):
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.object(m, "SHADOW_DIR", Path(tmp)), \
                mock.patch.dict(os.environ, {m.TRACKING_URI_ENV: self.uri}, clear=False):
            write_json(Path(tmp) / "2026-10.json", payload())
            self.assertEqual(m.main(["--month", "2026-10"]), 0)
        self.assertEqual(len(self.runs()), 1)

    def test_main_backfill_name_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(m, "SHADOW_DIR", Path(tmp)):
            write_json(Path(tmp) / "2026-10.json", payload(month="2026-11",
                                                           cutoff="2026-12-08",
                                                           run_date="2026-11-15"))
            with contextlib.redirect_stderr(io.StringIO()) as err:
                self.assertEqual(m.main(["--backfill", "--tracking-uri", self.uri]), 1)
            self.assertIn("не совпадает", err.getvalue())
            self.assertEqual(self.runs(), [])

    def test_main_invalid_json_continues(self):
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(m, "SHADOW_DIR", Path(tmp)):
            write_json(Path(tmp) / "2026-10.json", payload())
            (Path(tmp) / "2026-11.json").write_text("{", encoding="utf-8")
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(m.main(["--backfill", "--tracking-uri", self.uri]), 1)
            self.assertEqual(len(self.runs()), 1)

    def test_cli_requires_source(self):
        with self.assertRaises(SystemExit):
            m._parse_args([])
        with self.assertRaises(SystemExit):
            m._parse_args(["--month", "2026-13"])

    def test_run_summary_jsonable(self):
        plan = m.build_plan(payload(actual={"t_live_rt": 0.5}), Path("2026-10.json"), SHA)
        text = json.dumps(plan["summary"], ensure_ascii=False)
        self.assertIn('"json_sha256"', text)
        self.assertTrue(math.isfinite(plan["summary"]["metrics"]["error_bp"]))


if __name__ == "__main__":
    unittest.main()
