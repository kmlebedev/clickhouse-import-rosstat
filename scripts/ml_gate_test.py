#!/usr/bin/env python3
"""Офлайн-тесты gate промоушена (infra/4.3)."""

import json
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

import ml_gate as mg

CHAMPION = "11111111-1111-4111-8111-111111111111"
CHALLENGER = "22222222-2222-4222-8222-222222222222"


def fold(run_id, day, error, model="ordered_logit", window="expanding", unit="bp"):
    return mg.Fold(
        run_id=run_id,
        experiment="key_rate_model",
        model=model,
        window=window,
        target_date=day,
        error=error,
        unit=unit,
    )


def folds(run_id, errors, model="ordered_logit", window="expanding"):
    return [
        fold(run_id, f"2025-{index // 28 + 1:02d}-{index % 28 + 1:02d}", error, model, window)
        for index, error in enumerate(errors)
    ]


class AlignTest(unittest.TestCase):
    def test_align_common_dates(self):
        left = folds(CHAMPION, [1.0, 2.0, 3.0])
        right = folds(CHALLENGER, [2.0, 1.0, 4.0])
        a, b, _ = mg.align(left, right, "ordered_logit", "expanding")
        self.assertEqual([row.target_date for row in a], [row.target_date for row in b])
        self.assertEqual(len(a), 3)

    def test_align_no_common_folds(self):
        left = folds(CHAMPION, [1.0])
        right = [fold(CHALLENGER, "2030-01-01", 2.0)]
        with self.assertRaises(mg.GateError):
            mg.align(left, right, "ordered_logit", "expanding")

    def test_align_unit_mismatch(self):
        left = folds(CHAMPION, [1.0])
        right = [fold(CHALLENGER, "2025-01-01", 2.0, unit="pp")]
        with self.assertRaises(mg.GateError):
            mg.align(left, right, "ordered_logit", "expanding")


class WindowResultTest(unittest.TestCase):
    def test_promote_when_better_and_significant(self):
        champion = folds(CHAMPION, [10.0, 12.0, 9.0, 11.0] * 4)
        challenger = folds(CHALLENGER, [4.0, 5.0, 3.0, 6.0] * 4)
        result = mg.window_result("expanding", champion, challenger, mg.MIN_PRIMARY_FOLDS)
        self.assertTrue(result.challenger_better)
        self.assertTrue(result.promote)
        self.assertLess(result.dm_p, mg.DM_ALPHA)
        self.assertLess(result.bootstrap_high, 0)

    def test_iterate_when_not_significant(self):
        champion = folds(CHAMPION, [10.0, 12.0, 9.0, 11.0] * 4)
        challenger = folds(CHALLENGER, [10.0, 12.0, 9.0, 11.0] * 4)
        result = mg.window_result("expanding", champion, challenger, mg.MIN_PRIMARY_FOLDS)
        self.assertFalse(result.promote)
        self.assertFalse(result.significant_worse)
        decision, reason = mg.decide(result, [])
        self.assertEqual((decision, reason), ("iterate", "no_improvement"))

    def test_reject_when_significantly_worse(self):
        champion = folds(CHAMPION, [4.0, 5.0, 3.0, 6.0] * 4)
        challenger = folds(CHALLENGER, [10.0, 12.0, 9.0, 11.0] * 4)
        result = mg.window_result("expanding", champion, challenger, mg.MIN_PRIMARY_FOLDS)
        self.assertTrue(result.significant_worse)
        self.assertEqual(mg.decide(result, [])[0], "reject")

    def test_insufficient_folds(self):
        champion = folds(CHAMPION, [1.0] * 5)
        challenger = folds(CHALLENGER, [2.0] * 5)
        result = mg.window_result("expanding", champion, challenger, mg.MIN_PRIMARY_FOLDS)
        self.assertIn("insufficient_folds", result.note)
        self.assertEqual(mg.decide(result, [])[0], "iterate")

    def test_robustness_significant_worse_rejects(self):
        primary = mg.WindowResult(
            window="expanding", folds=20, rmse_champion=10.0, rmse_challenger=8.0,
            dm_stat=-2.0, dm_p=0.01, bootstrap_low=-4.0, bootstrap_high=-1.0,
            bootstrap_p=0.0, challenger_better=True, significant_worse=False,
            promote=True,
        )
        robustness = mg.WindowResult(
            window="rolling40", folds=20, rmse_champion=8.0, rmse_challenger=12.0,
            dm_stat=2.0, dm_p=0.01, bootstrap_low=1.0, bootstrap_high=5.0,
            bootstrap_p=0.0, challenger_better=False, significant_worse=True,
            promote=False,
        )
        decision, reason = mg.decide(primary, [robustness])
        self.assertEqual(decision, "reject")
        self.assertIn("robustness_significantly_worse", reason)


class McsTest(unittest.TestCase):
    def test_mcs_requires_three_candidates(self):
        with self.assertRaises(mg.GateError):
            mg._mcs_included(
                [(CHAMPION, folds(CHAMPION, [1.0] * 12)),
                 (CHALLENGER, folds(CHALLENGER, [2.0] * 12))],
                "ordered_logit", "expanding",
            )

    def test_mcs_included_wiring(self):
        class FakeMCS:
            def __init__(self, *args, **kwargs):
                self.included = [0, 1]
                self.pvalues = [0.01, 0.02, 0.5]

            def compute(self):
                return None

        fake_arch = types.ModuleType("arch")
        fake_bootstrap = types.ModuleType("arch.bootstrap")
        fake_bootstrap.MCS = FakeMCS
        fake_arch.bootstrap = fake_bootstrap
        third = "33333333-3333-4333-8333-333333333333"
        candidates = [
            (CHAMPION, folds(CHAMPION, [1.0] * 12)),
            (CHALLENGER, folds(CHALLENGER, [2.0] * 12)),
            (third, folds(third, [3.0] * 12)),
        ]
        with mock.patch.dict(sys.modules, {"arch": fake_arch, "arch.bootstrap": fake_bootstrap}):
            included, pvalues = mg._mcs_included(candidates, "ordered_logit", "expanding")
        self.assertEqual(included, {CHAMPION, CHALLENGER})
        self.assertEqual(pvalues[third], 0.5)


class BacktestCliTest(unittest.TestCase):
    def test_cli_writes_artifact(self):
        champion = folds(CHAMPION, [10.0, 12.0, 9.0, 11.0] * 4)
        challenger = folds(CHALLENGER, [4.0, 5.0, 3.0, 6.0] * 4)

        def fake_query(sql):
            return [as_dict(row) for row in champion] if CHAMPION in sql else [
                as_dict(row) for row in challenger
            ]

        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(mg, "ch_query", side_effect=fake_query), \
                    mock.patch.dict(os.environ, {}, clear=False):
                os.environ.pop("MLFLOW_TRACKING_URI", None)
                code = mg.main([
                    "--experiment", "key_rate_model",
                    "--champion-run", CHAMPION,
                    "--challenger-run", CHALLENGER,
                    "--model", "ordered_logit",
                    "--window", "expanding",
                    "--out", directory,
                ])
                self.assertEqual(code, 0)
                artifact = Path(directory) / f"{CHALLENGER}.json"
                self.assertTrue(artifact.exists())
                payload = json.loads(artifact.read_text(encoding="utf-8"))
                self.assertEqual(payload["decision"], "promote")
                self.assertEqual(payload["mode"], "backtest")
                self.assertFalse(payload["gate"]["mcs"])


def as_dict(row):
    return {
        "run_id": row.run_id,
        "experiment": row.experiment,
        "model": row.model,
        "window": row.window,
        "target_date": row.target_date,
        "error": row.error,
        "unit": row.unit,
    }


class LiveTest(unittest.TestCase):
    def test_latest_by_date_keeps_latest_forecast(self):
        rows = [
            {"target_date": "2026-09-11", "forecast_date": "2026-09-01", "abs_error_bp": 50.0},
            {"target_date": "2026-09-11", "forecast_date": "2026-09-05", "abs_error_bp": 20.0},
            {"target_date": "2026-07-24", "forecast_date": "2026-07-01", "abs_error_bp": 10.0},
        ]
        latest = mg._latest_by_date(rows)
        self.assertEqual(len(latest), 2)
        by_day = {row["target_date"]: row for row in latest}
        self.assertEqual(by_day["2026-09-11"]["abs_error_bp"], 20.0)

    def test_wait_until_shadow_meetings(self):
        rows = {
            "cbr_rate:ordered_logit:shadow": [
                {"target_date": "2026-09-11", "abs_error_bp": 50.0, "is_resolved": 1}
            ],
            "cbr_rate:ordered_logit:canary": [],
        }
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(mg, "_live_rows", side_effect=lambda metric: rows[metric]):
                code = mg.main([
                    "--live", "--live-track", "cbr_rate:ordered_logit",
                    "--out", directory,
                ])
                self.assertEqual(code, 0)
                payload = json.loads(
                    (Path(directory) / "live-cbr_rate-ordered_logit.json").read_text(
                        encoding="utf-8"
                    )
                )
                self.assertEqual(payload["decision"], "wait")

    def test_promote_after_shadow_and_canary(self):
        rows = {
            "cbr_rate:ordered_logit:shadow": [
                {"target_date": "2026-09-11", "abs_error_bp": 50.0, "is_resolved": 1},
                {"target_date": "2026-10-23", "abs_error_bp": 40.0, "is_resolved": 1},
            ],
            "cbr_rate:ordered_logit:canary": [
                {"target_date": "2026-12-18", "abs_error_bp": 30.0, "is_resolved": 1}
            ],
        }
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(mg, "_live_rows", side_effect=lambda metric: rows[metric]):
                code = mg.main([
                    "--live", "--live-track", "cbr_rate:ordered_logit",
                    "--out", directory,
                ])
                self.assertEqual(code, 0)
                payload = json.loads(
                    (Path(directory) / "live-cbr_rate-ordered_logit.json").read_text(
                        encoding="utf-8"
                    )
                )
                self.assertEqual(payload["decision"], "promote")
                self.assertEqual(payload["shadow_meetings"], 2)
                self.assertEqual(payload["canary_meetings"], 1)

    def test_rollback_on_large_canary_error(self):
        rows = {
            "cbr_rate:ordered_logit:shadow": [
                {"target_date": "2026-09-11", "abs_error_bp": 20.0, "is_resolved": 1},
                {"target_date": "2026-10-23", "abs_error_bp": 20.0, "is_resolved": 1},
            ],
            "cbr_rate:ordered_logit:canary": [
                {"target_date": "2026-12-18", "abs_error_bp": 250.0, "is_resolved": 1}
            ],
        }
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(mg, "_live_rows", side_effect=lambda metric: rows[metric]):
                code = mg.main([
                    "--live", "--live-track", "cbr_rate:ordered_logit",
                    "--out", directory,
                ])
                self.assertEqual(code, 0)
                payload = json.loads(
                    (Path(directory) / "live-cbr_rate-ordered_logit.json").read_text(
                        encoding="utf-8"
                    )
                )
                self.assertEqual(payload["decision"], "rollback")


if __name__ == "__main__":
    unittest.main()
