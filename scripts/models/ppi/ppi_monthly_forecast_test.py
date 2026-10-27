#!/usr/bin/env python3
"""Офлайн-тесты research-прогноза CPI/PPI по ценовым ожиданиям."""

import contextlib
import io
import json
import math
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta
from unittest import mock
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from models.ppi import ppi_monthly_forecast as pm


class CalendarTest(unittest.TestCase):
    def test_shift_month_handles_year_and_leap_february(self):
        self.assertEqual(pm.shift_month(date(2025, 12, 1), 1), date(2026, 1, 1))
        self.assertEqual(pm.shift_month(date(2024, 1, 31), 1), date(2024, 2, 29))
        self.assertEqual(pm.shift_month(date(2024, 2, 29), -1), date(2024, 1, 29))

    def test_cutoff_is_eighth_of_next_month(self):
        self.assertEqual(
            pm.cutoff_for(date(2026, 8, 1)),
            datetime(2026, 9, 8, 23, 59, 59),
        )

    def test_publication_at_cutoff_is_not_available(self):
        origin = date(2026, 8, 1)
        cutoff = pm.cutoff_for(origin)
        values = {("price_expectations", date(2026, 7, 1)): 12.0}
        publications = [
            {"series": "price_expectations", "period_date": "2026-07-01",
             "published_at": cutoff, "quality": "exact_time"},
        ]
        self.assertEqual(
            pm.latest_survey_features(origin, values, publications, ("price_expectations",)),
            {},
        )
        publications[0]["published_at"] = cutoff - timedelta(seconds=1)
        features = pm.latest_survey_features(
            origin, values, publications, ("price_expectations",)
        )
        self.assertEqual(features["price_expectations_lag0"], 12.0)
        for lag in (1, 2, 3):
            self.assertTrue(math.isnan(features[f"price_expectations_lag{lag}"]))

    def test_publication_quality_is_preserved_in_audit(self):
        origin = date(2026, 8, 1)
        publications = [
            {"series": "price_expectations", "period_date": "2026-07-01",
             "published_at": "2026-08-15", "quality": "date"},
            {"series": "price_expectations", "period_date": "2026-06-01",
             "published_at": "2026-07-15", "quality": "fallback"},
        ]
        audit = pm.survey_publication_audit(origin, publications, ("price_expectations",))
        self.assertEqual([row["quality"] for row in audit], ["date", "fallback"])
        self.assertEqual(audit[0]["published_at"], datetime(2026, 8, 15, 23, 59, 59))
        self.assertEqual(audit[1]["published_at"], datetime(2026, 7, 31, 23, 59, 59))

    def test_missing_publication_row_fails_closed(self):
        values = {("price_expectations", date(2026, 7, 1)): 12.0}
        self.assertEqual(
            pm.latest_survey_features(
                date(2026, 8, 1), values, [], ("price_expectations",)
            ),
            {},
        )

    def test_missing_industry_calendar_fails_closed(self):
        values = {("industry_food_price_expectations", date(2026, 7, 1)): 8.0}
        features = pm.latest_survey_features(
            date(2026, 8, 1), values, [], ("industry_food_price_expectations",)
        )
        self.assertEqual(features, {})


class InputParsingTest(unittest.TestCase):
    @staticmethod
    def ppi_rows():
        return [
            {"series": series, "date": "2026-08-31", "mm_sa": 0.4}
            for series in ("total", "food", "nonfood", "core")
        ]

    def test_targets_normalize_month_and_convert_cpi_to_pp(self):
        cpi = [{"name": pm.CPI_ALL_NAME, "date": "2026-08-31", "percent": 100.7}]
        targets = pm.parse_targets(cpi, self.ppi_rows())
        self.assertEqual(set(targets), {"cpi_all", "ppi_total", "ppi_food", "ppi_nonfood", "ppi_core"})
        self.assertEqual(list(targets["cpi_all"]), [date(2026, 8, 1)])
        self.assertAlmostEqual(targets["cpi_all"][date(2026, 8, 1)], 0.7)
        self.assertEqual(targets["ppi_total"], {date(2026, 8, 1): 0.4})

    def test_duplicate_survey_point_is_rejected(self):
        rows = [
            {"series": "price_expectations", "date": "2026-08-01", "value": 1.0},
            {"series": "price_expectations", "date": "2026-08-31", "value": 2.0},
        ]
        with self.assertRaisesRegex(ValueError, "дубль"):
            pm.parse_survey_values(rows)

    def test_nonfinite_target_is_rejected(self):
        rows = self.ppi_rows()
        rows[0]["mm_sa"] = float("inf")
        with self.assertRaisesRegex(ValueError, "finite"):
            pm.parse_targets([], rows)

    def test_missing_required_target_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "cpi_all"):
            pm.parse_targets([], self.ppi_rows())


class DirectFoldTest(unittest.TestCase):
    @staticmethod
    def rows(count=48):
        return [
            pm.Row(
                target="cpi_all",
                origin=pm.shift_month(date(2020, 1, 1), index),
                actual=float(index),
                base_features={"y_lag1": float(index - 1)},
                expectation_features={"price_expectations_lag0": float(index)},
            )
            for index in range(count)
        ]

    def test_direct_fold_respects_horizon_and_embargo(self):
        rows = self.rows()
        for horizon in (1, 3, 6):
            with self.subTest(horizon=horizon):
                folds = pm.direct_folds(rows, horizon)
                first = folds[0]
                self.assertEqual(len(first.train), 36)
                self.assertEqual(first.test.origin, pm.shift_month(date(2020, 1, 1), 36 + horizon))
                self.assertEqual(
                    max(row.origin for row in first.train),
                    pm.shift_month(first.test.origin, -horizon - 1),
                )
                self.assertEqual(first.target_date, pm.shift_month(first.test.origin, horizon))
                self.assertGreater(len(folds[1].train), len(first.train))
                self.assertNotIn(first.test.origin, {row.origin for row in first.train})

    def test_pair_sample_starts_on_common_available_month(self):
        rows = self.rows()
        without_early_expectations = [
            pm.Row(row.target, row.origin, row.actual, row.base_features, {})
            for row in rows
        ]
        base_folds = pm.direct_folds(rows, 3)
        expectation_folds = pm.direct_folds(without_early_expectations, 3)
        self.assertEqual(
            [fold.test.origin for fold in base_folds],
            [fold.test.origin for fold in expectation_folds],
        )

    def test_direct_folds_reject_noncontiguous_months(self):
        rows = self.rows()
        del rows[20]
        with self.assertRaisesRegex(ValueError, "разрыв"):
            pm.direct_folds(rows, 1)


class ModelTest(unittest.TestCase):
    @staticmethod
    def rows(count=38):
        return [
            pm.Row(
                target="cpi_all",
                origin=pm.shift_month(date(2020, 1, 1), index),
                actual=math.sin(index / 3.0),
                base_features={"y_lag1": math.sin((index - 1) / 3.0)},
                expectation_features={
                    "price_expectations_lag0": float(index % 7),
                    "price_expectations_lag1": float((index - 1) % 7),
                    "price_expectations_lag2": float((index - 2) % 7),
                    "price_expectations_lag3": float((index - 3) % 7),
                },
            )
            for index in range(count)
        ]

    def test_b1_returns_last_actual(self):
        rows = self.rows()
        predictions = pm.walk_forward(rows, horizons=(1,))
        b1 = [row for row in predictions if row.model == "b1_last"]
        self.assertEqual(len(b1), 1)
        self.assertEqual(b1[0].y_pred, rows[35].actual)

    def test_b2_uses_ar1_and_month_dummies(self):
        row = self.rows()[36]
        features = pm.feature_values(row, horizon=1, expectations=False)
        self.assertIn("y_lag1", features)
        dummies = {name for name in features if name.startswith("month_")}
        self.assertEqual(len(dummies), 11)
        target_month = pm.shift_month(row.origin, 1).month
        active = [name for name in dummies if features[name] == 1.0]
        self.assertEqual(active, [f"month_{target_month:02d}"] if target_month != 1 else [])

    def test_b3_pair_differs_only_by_expectation_columns(self):
        row = self.rows()[36]
        base = pm.feature_values(row, horizon=3, expectations=False)
        with_expectations = pm.feature_values(row, horizon=3, expectations=True)
        self.assertEqual({key: with_expectations[key] for key in base}, base)
        self.assertEqual(
            set(with_expectations) - set(base),
            set(row.expectation_features),
        )

    def test_future_survey_does_not_change_prediction(self):
        rows = self.rows()
        before = [
            row for row in pm.walk_forward(rows, horizons=(1,))
            if row.model == "b3_enet_expectations"
        ][0]
        future = rows[-1]
        rows[-1] = pm.Row(
            future.target, future.origin, future.actual, future.base_features,
            {key: value + 1000.0 for key, value in future.expectation_features.items()},
        )
        after = [
            row for row in pm.walk_forward(rows, horizons=(1,))
            if row.model == "b3_enet_expectations"
        ][0]
        self.assertEqual(before.y_pred, after.y_pred)

    def test_imputation_and_scaling_are_train_only(self):
        train = [
            pm.Row("cpi_all", pm.shift_month(date(2020, 1, 1), index), float(index),
                   {"x": value}, {})
            for index, value in enumerate([1.0, 2.0, float("nan"), 3.0, 4.0, 5.0])
        ]
        imputer, scaler, _ = pm._fit_b3(train, horizon=1, expectations=False)
        self.assertEqual(imputer.statistics_[0], 3.0)
        self.assertAlmostEqual(scaler.mean_[0], 3.0)

    def test_degenerate_train_fold_is_reported_not_silently_dropped(self):
        rows = self.rows()
        rows = [
            pm.Row(row.target, row.origin, row.actual, {"y_lag1": float("nan")}, {})
            for row in rows
        ]
        predictions = [
            row for row in pm.walk_forward(rows, horizons=(1,))
            if row.model == "b2_ar1"
        ]
        self.assertEqual(len(predictions), 1)
        self.assertEqual(predictions[0].status, "skipped")
        self.assertTrue(predictions[0].diagnostic)


class EvaluationTest(unittest.TestCase):
    @staticmethod
    def prediction(model, origin_month, y_pred, y_true=0.0, target="ppi_food", horizon=1):
        origin = pm.shift_month(date(2020, 1, 1), origin_month)
        return pm.Prediction(
            target=target, horizon=horizon, origin=origin, model=model,
            y_true=y_true, y_pred=y_pred, train_start=date(2017, 1, 1),
            train_end=pm.shift_month(origin, -horizon - 1),
        )

    def test_pair_metrics_use_identical_origins(self):
        predictions = [
            self.prediction("b2_ar1", 0, 1.0), self.prediction("b2_ar1", 1, 1.0),
            self.prediction("b2_ar1", 2, 1.0), self.prediction("b2_ar1", 3, 1.0),
            self.prediction("b2_ar1_expectations", 1, 0.5),
            self.prediction("b2_ar1_expectations", 2, 0.5),
            self.prediction("b2_ar1_expectations", 3, 0.5),
        ]
        result = pm.evaluate(predictions, n=50)
        pair = [row for row in result["pairs"] if row["base_model"] == "b2_ar1"][0]
        self.assertEqual(pair["n"], 3)
        self.assertEqual([row.isoformat() for row in pair["origins"]], ["2020-02-01", "2020-03-01", "2020-04-01"])

    def test_delta_sign_and_manual_rmse_mae(self):
        predictions = [
            self.prediction("b3_enet", 0, 1.0), self.prediction("b3_enet", 1, -1.0),
            self.prediction("b3_enet_expectations", 0, 0.5),
            self.prediction("b3_enet_expectations", 1, -0.5),
        ]
        result = pm.evaluate(predictions, n=50)
        pair = [row for row in result["pairs"] if row["base_model"] == "b3_enet"][0]
        self.assertAlmostEqual(pair["base_rmse"], 1.0)
        self.assertAlmostEqual(pair["base_mae"], 1.0)
        self.assertAlmostEqual(pair["expectations_rmse"], 0.5)
        self.assertAlmostEqual(pair["delta"], 0.5)

    def test_identical_errors_are_not_significant(self):
        predictions = []
        for index in range(4):
            predictions.extend([
                self.prediction("b2_ar1", index, 1.0),
                self.prediction("b2_ar1_expectations", index, 1.0),
            ])
        pair = pm.evaluate(predictions, n=50)["pairs"][0]
        self.assertEqual(pair["delta"], 0.0)
        self.assertEqual(pair["dm_pvalue"], 1.0)
        self.assertEqual((pair["ci_low"], pair["ci_high"]), (0.0, 0.0))

    def test_bootstrap_is_deterministic(self):
        predictions = []
        for index in range(8):
            predictions.extend([
                self.prediction("b2_ar1", index, 1.0 + index / 10),
                self.prediction("b2_ar1_expectations", index, 0.7 + index / 20),
            ])
        first = pm.evaluate(predictions, n=200)
        second = pm.evaluate(predictions, n=200)
        self.assertEqual(
            [(row["ci_low"], row["ci_high"], row["dm_pvalue"]) for row in first["pairs"]],
            [(row["ci_low"], row["ci_high"], row["dm_pvalue"]) for row in second["pairs"]],
        )

    def test_mde_requires_two_pairs(self):
        self.assertIsNone(pm.mde_ratio([1.0], [1.0]))
        self.assertEqual(pm.mde_ratio([1.0, 2.0], [1.0, 2.0]), 0.0)

    def test_holm_adjustment_is_ordered(self):
        adjusted = pm.holm_adjust([0.01, 0.03, 0.04])
        self.assertEqual(adjusted, [0.03, 0.06, 0.06])

    def test_verdict_requires_positive_delta_dm_and_ci(self):
        base = {
            "target": "ppi_food", "horizon": 1, "primary": True, "n": 12,
            "delta": 0.5, "holm_pvalue": 0.01, "ci_low": 0.1, "dm_pvalue": 0.01,
        }
        self.assertEqual(pm.research_verdict([base]), "promote")
        self.assertEqual(pm.research_verdict([{**base, "ci_low": 0.0}]), "iterate")
        self.assertEqual(pm.research_verdict([{**base, "delta": -0.1}]), "reject")
        self.assertEqual(pm.research_verdict([{**base, "n": 11}]), "INSUFFICIENT")


class ArtifactTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        root = Path(cls.temp.name)
        cls.out1 = root / "run1"
        cls.out2 = root / "run2"
        cls.targets, cls.values, cls.publications = cls.synthetic()
        cls.result = pm.run_research(
            cls.targets, cls.values, cls.publications,
            out_dir=cls.out1, horizons=(1,), min_train=36, logger_enabled=False,
        )
        cls.second = pm.run_research(
            cls.targets, cls.values, cls.publications,
            out_dir=cls.out2, horizons=(1,), min_train=36, logger_enabled=False,
        )
        cls.report = (cls.out1 / f"{pm.ARTIFACT_STEM}.md").read_text()
        cls.data = json.loads((cls.out1 / f"{pm.ARTIFACT_STEM}.json").read_text())

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    @staticmethod
    def synthetic():
        targets = {name: {} for name in ("cpi_all", "ppi_total", "ppi_food", "ppi_nonfood", "ppi_core")}
        values = {}
        publications = []
        for index in range(44):
            month = pm.shift_month(date(2019, 1, 1), index)
            for offset, name in enumerate(targets):
                targets[name][month] = math.sin((index + offset) / 4.0)
            for series_index, series in enumerate(pm.AGGREGATE_SURVEY_SERIES):
                values[(series, month)] = 10.0 + series_index + math.cos((index + series_index) / 5.0)
                publications.append({
                    "series": series,
                    "period_date": month,
                    "published_at": pm.shift_month(month, 1).replace(day=1) - timedelta(days=1),
                    "quality": "date",
                })
        return targets, values, publications

    def test_artifacts_and_passport_are_complete(self):
        for suffix in (".md", ".json", "-predictions.csv"):
            self.assertTrue((self.out1 / f"{pm.ARTIFACT_STEM}{suffix}").is_file())
        for key in (
            "schema_version", "run_id", "experiment", "run_name", "git", "spec_path",
            "plan_path", "config", "config_hash", "data_asof", "sample_meta",
            "sample_hash", "data_passport", "fold_counts", "predictions_schema",
            "evaluation", "robustness", "limitations", "verdict", "artifacts",
        ):
            self.assertIn(key, self.data)
        self.assertEqual(self.data["verdict"], "INSUFFICIENT")
        self.assertIn("latest-vintage", self.report)
        self.assertIn("RMSE", self.report)
        self.assertIn("INSUFFICIENT", self.report)
        self.assertIn("Сравнение с запиской", self.report)
        for row in self.data["fold_counts"]:
            self.assertIn("first_target", row)
            self.assertIn("last_target", row)
        self.assertEqual(
            self.data["data_passport"]["secondary_consumer"]["status"],
            "FAIL_CLOSED",
        )
        hashes = {entry["name"]: entry["sha256"] for entry in self.data["artifacts"]}
        self.assertIn(f"{pm.ARTIFACT_STEM}-predictions.csv", hashes)
        self.assertIn(f"{pm.ARTIFACT_STEM}.md", hashes)

    def test_csv_round_trip_reproduces_metrics(self):
        path = self.out1 / f"{pm.ARTIFACT_STEM}-predictions.csv"
        predictions = pm.read_predictions_csv(path)
        evaluation = pm.evaluate(predictions, n=200)
        official = pm.evaluate(self.result["predictions"], n=200)
        fields = ("target", "horizon", "base_model", "n", "delta", "dm_pvalue", "ci_low", "ci_high", "mde")
        self.assertEqual(
            [[pair.get(field) for field in fields] for pair in evaluation["pairs"]],
            [[pair.get(field) for field in fields] for pair in official["pairs"]],
        )

    def test_replay_matches_predictions_and_metrics(self):
        second = json.loads((self.out2 / f"{pm.ARTIFACT_STEM}.json").read_text())
        fields = ("target", "horizon", "origin", "model", "y_true", "y_pred", "train_start", "train_end", "status")
        self.assertEqual(
            [[row.get(field) for field in fields] for row in self.data["predictions"]],
            [[row.get(field) for field in fields] for row in second["predictions"]],
        )
        self.assertEqual(self.data["evaluation"]["models"], second["evaluation"]["models"])
        self.assertEqual(
            [{key: value for key, value in pair.items() if key != "origins"} for pair in self.data["evaluation"]["pairs"]],
            [{key: value for key, value in pair.items() if key != "origins"} for pair in second["evaluation"]["pairs"]],
        )

    def test_predictions_csv_uses_lf_line_endings(self):
        path = Path(self.temp.name) / "line-endings.csv"
        row = EvaluationTest.prediction("b2_ar1", 0, 0.5)
        pm.write_predictions_csv(path, [row])
        self.assertNotIn(b"\r", path.read_bytes())

    def test_csv_round_trip_preserves_skipped_prediction(self):
        path = Path(self.temp.name) / "skipped.csv"
        row = EvaluationTest.prediction("b2_ar1", 0, float("nan"), y_true=1.0)
        skipped = pm.Prediction(
            row.target, row.horizon, row.origin, row.model, row.y_true,
            row.y_pred, row.train_start, row.train_end, "skipped", "degenerate",
        )
        pm.write_predictions_csv(path, [skipped])
        loaded = pm.read_predictions_csv(path)[0]
        self.assertEqual(loaded.status, "skipped")
        self.assertEqual(loaded.diagnostic, "degenerate")
        self.assertTrue(math.isnan(loaded.y_pred))

    def test_missing_target_writes_no_success_artifact(self):
        broken = {name: dict(points) for name, points in self.targets.items()}
        broken["ppi_core"] = {}
        out = Path(self.temp.name) / "broken"
        with self.assertRaises(pm.DataError):
            pm.run_research(
                broken, self.values, self.publications,
                out_dir=out, horizons=(1,), min_train=36, logger_enabled=False,
            )
        self.assertFalse((out / f"{pm.ARTIFACT_STEM}.json").exists())

    def test_logged_models_are_target_qualified(self):
        captured = {}
        original = pm.mlx.start_run

        def wrapped(*args, **kwargs):
            kwargs["enabled"] = False
            logger = original(*args, **kwargs)
            captured["logger"] = logger
            return logger

        with mock.patch.object(pm.mlx, "start_run", side_effect=wrapped):
            pm.run_research(
                self.targets, self.values, self.publications,
                out_dir=Path(self.temp.name) / "logging",
                horizons=(1,), min_train=36,
            )
        names = {row.model for row in captured["logger"].predictions}
        self.assertTrue(names)
        self.assertTrue(all("::" in name for name in names), names)
        self.assertEqual(len(names), 5 * 5)


class SecondaryStatusTest(unittest.TestCase):
    def test_missing_industry_calendar_fails_closed(self):
        status = pm.secondary_consumer_status([])
        self.assertEqual(status["status"], "FAIL_CLOSED")
        self.assertIn("calendar", status["reason"])

    def test_industry_calendar_presence_is_reported(self):
        status = pm.secondary_consumer_status([
            {"series": "industry_manufacturing_consumer_price_expectations",
             "period_date": "2026-08-01", "quality": "date"},
        ])
        self.assertEqual(status["status"], "CALENDAR_PRESENT")


class CliBoundaryTest(unittest.TestCase):
    def test_horizon_rows_honour_custom_min_train(self):
        targets, values, publications = ArtifactTest.synthetic()
        horizon = pm.shift_month(date(2019, 1, 1), 30)
        points = {day: value for day, value in targets["cpi_all"].items() if day < horizon}
        survey = {key: value for key, value in values.items() if key[1] < horizon}
        pubs = [row for row in publications if row["period_date"] < horizon]
        rows = pm.build_horizon_rows(
            "cpi_all", points, survey, pubs, horizon=1, min_train=24
        )
        self.assertEqual(len(rows), 28)

    def test_main_turns_data_value_error_into_clean_failure(self):
        stderr = io.StringIO()
        with mock.patch.object(pm, "load_inputs", side_effect=ValueError("bad data")):
            with contextlib.redirect_stderr(stderr):
                code = pm.main([])
        self.assertEqual(code, 1)
        self.assertIn("bad data", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
