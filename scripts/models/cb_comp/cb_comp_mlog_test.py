#!/usr/bin/env python3
"""Офлайн-тесты `cb_comp_mlog` (P0.5): без ClickHouse и без MLflow."""

import os
import sys
import unittest
from datetime import date
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lib import ml_experiment as mlx
from models.cb_comp import cb_comp_mlog as mlog

MONTHS = [date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)]


def aggregated():
    return {
        "combo3": {1: {m: 1.0 + i for i, m in enumerate(MONTHS)},
                   3: {m: 2.0 + i for i, m in enumerate(MONTHS)}},
        "live_cb": {1: {m: 0.5 + i for i, m in enumerate(MONTHS)},
                    3: {m: 1.5 + i for i, m in enumerate(MONTHS)}},
    }


def truth():
    return {1: {m: 1.0 for m in MONTHS}, 3: {m: 2.0 for m in MONTHS}}


class BuildFoldRowsTest(unittest.TestCase):
    def test_rows_cover_models_horizons_months(self):
        rows = mlog.build_fold_rows(aggregated(), truth(), MONTHS,
                                    ("combo3", "live_cb"), (1, 3), version="2010")
        self.assertEqual(len(rows), 2 * 2 * 3)
        self.assertEqual({row.model for row in rows}, {"combo3", "live_cb"})
        self.assertEqual({row.window for row in rows}, {"h1", "h3"})
        self.assertEqual({row.unit for row in rows}, {mlog.UNIT})
        self.assertEqual({row.target_date for row in rows}, set(MONTHS))

    def test_error_is_predicted_minus_actual(self):
        rows = mlog.build_fold_rows({"combo3": {1: {MONTHS[0]: 1.5}}},
                                    {1: {MONTHS[0]: 1.0}}, [MONTHS[0]],
                                    ("combo3",), (1,), version="2010")
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0].error, 0.5)

    def test_meta_carries_horizon_version_and_t_live(self):
        rows = mlog.build_fold_rows(aggregated(), truth(), MONTHS, ("combo3",), (1,),
                                    t_live={1: {MONTHS[0]: 7.5}}, version="2009")
        row = rows[0]
        self.assertEqual(row.meta["horizon"], 1)
        self.assertEqual(row.meta["version"], "2009")
        self.assertEqual(row.meta["truth"], "target_full")
        self.assertEqual(row.meta["normal"], 1)
        self.assertAlmostEqual(row.meta["t_live"], 7.5)
        self.assertIsNone(rows[1].meta["t_live"])

    def test_missing_and_non_finite_are_skipped(self):
        rows = mlog.build_fold_rows(
            {"combo3": {1: {MONTHS[0]: float("nan"), MONTHS[1]: 1.0}}},
            {1: {MONTHS[0]: 1.0, MONTHS[1]: 1.0, MONTHS[2]: float("inf")}},
            MONTHS, ("combo3",), (1,), version="2010")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].target_date, MONTHS[1])

    def test_unknown_model_or_horizon_gives_no_rows(self):
        rows = mlog.build_fold_rows(aggregated(), truth(), MONTHS, ("nope",), (1,))
        self.assertEqual(rows, [])


class LogRunTest(unittest.TestCase):
    def setUp(self):
        self._env = mock.patch.dict(os.environ, {}, clear=False)
        self._env.start()
        os.environ.pop("ML_LOG", None)

    def tearDown(self):
        self._env.stop()

    def test_disabled_without_ml_log(self):
        with mock.patch.object(mlx, "start_run") as start_run:
            status = mlog.log_run(
                run_name="combo3", config={}, data_asof=date(2024, 7, 1),
                sample_meta={}, sample=[], spec_path="spec.md",
                aggregated=aggregated(), truth=truth(), months=MONTHS,
                models=("combo3",), horizons=(1,))
        self.assertEqual(status, "disabled")
        start_run.assert_not_called()

    def test_enabled_passes_line_contract(self):
        recorder = mock.Mock()
        recorder.finish.return_value = "clickhouse"
        os.environ["ML_LOG"] = "1"
        with mock.patch.object(mlx, "start_run", return_value=recorder) as start_run:
            status = mlog.log_run(
                run_name="combo3__h1-h6__normal54__target_full__2010",
                config={"seed": 1}, data_asof=date(2024, 7, 1),
                sample_meta={"normal": 3}, sample=[{"month": "2024-01-01"}],
                spec_path="docs/spec.md",
                aggregated=aggregated(), truth=truth(), months=MONTHS,
                models=("combo3",), horizons=(1, 3), version="2010",
                metrics={"primary_metric": "rmse_pp"}, artifacts=[], notes="ok")
        self.assertEqual(status, "clickhouse")
        kwargs = start_run.call_args.kwargs
        self.assertEqual(kwargs["experiment"], "cb_comp_88_groups")
        self.assertTrue(kwargs["run_name"].startswith("combo3__"))
        self.assertTrue(kwargs["enabled"])
        folds = recorder.log_predictions.call_args.args[0]
        self.assertEqual(len(folds), 6)
        recorder.log_metrics.assert_called_once_with({"primary_metric": "rmse_pp"})
        recorder.finish.assert_called_once_with("finished", "ok")

    def test_receiver_failure_is_isolated(self):
        recorder = mock.Mock()
        recorder.log_predictions.side_effect = RuntimeError("boom")
        os.environ["ML_LOG"] = "1"
        with mock.patch.object(mlx, "start_run", return_value=recorder):
            status = mlog.log_run(
                run_name="combo3", config={}, data_asof=date(2024, 7, 1),
                sample_meta={}, sample=[], spec_path="spec.md",
                aggregated=aggregated(), truth=truth(), months=MONTHS,
                models=("combo3",), horizons=(1,))
        self.assertEqual(status, "failed")
        recorder.finish.assert_called_once()
        self.assertEqual(recorder.finish.call_args.args[0], "failed")

    def test_start_failure_reports_failed(self):
        os.environ["ML_LOG"] = "1"
        with mock.patch.object(mlx, "start_run", side_effect=RuntimeError("no api")):
            status = mlog.log_run(
                run_name="combo3", config={}, data_asof=date(2024, 7, 1),
                sample_meta={}, sample=[], spec_path="spec.md",
                aggregated=aggregated(), truth=truth(), months=MONTHS,
                models=("combo3",), horizons=(1,))
        self.assertEqual(status, "failed")


class HashStabilityTest(unittest.TestCase):
    def test_config_hash_ignores_key_order(self):
        left = {"seed": 1, "models": ["combo3"], "version": "2010"}
        right = {"version": "2010", "models": ["combo3"], "seed": 1}
        self.assertEqual(mlx.sha256_text(mlx.canonical_json(left)),
                         mlx.sha256_text(mlx.canonical_json(right)))


if __name__ == "__main__":
    unittest.main()
