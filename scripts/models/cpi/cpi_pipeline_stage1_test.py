#!/usr/bin/env python3
"""Офлайн-тесты `scripts/models/cpi/cpi_pipeline_stage1.py`.

Запуск из корня репозитория:

    make cpi-pipeline-stage1-test

ClickHouse и сеть не нужны: таргеты, USD, ставка, FAO и недельная панель
подменяются синтетикой; календарь публикаций читается из версионированной
фикстуры `scripts/testdata/publication_calendar.csv`. Все времена — наивные,
московское время; заседание считается моментом 13:30.
"""

import math
import sys
import unittest
from dataclasses import FrozenInstanceError
from datetime import date, timedelta
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))

from lib.publication_calendar import PublicationCalendar  # noqa: E402
from models.cpi import cpi_pipeline_stage1 as stage1  # noqa: E402

CAL = PublicationCalendar.from_csv(
    ROOT / "scripts" / "testdata" / "publication_calendar.csv"
)

TARGET_CODES = ("cpi_all", "cpi_food", "cpi_nonfood", "cpi_services")


def shift_month(month: date, delta: int) -> date:
    index = month.year * 12 + (month.month - 1) + delta
    return date(index // 12, index % 12 + 1, 1)


def month_end(month: date) -> date:
    return shift_month(month, 1) - timedelta(days=1)


def months_between(first: date, last: date) -> list:
    result = []
    month = first
    while month <= last:
        result.append(month)
        month = shift_month(month, 1)
    return result


def make_targets(values: dict | None = None) -> dict:
    """Синтетические таргеты 2024-01…2026-12 уже в m/m (как `cm.load_targets`)."""
    values = values or {}
    result = {}
    for code in TARGET_CODES:
        points = []
        for month in months_between(date(2024, 1, 1), date(2026, 12, 1)):
            value = values.get(month, 0.4)
            points.append((month_end(month), value))
        result[code] = points
    return result


def monthly_points(first: date, last: date, value: float) -> list:
    """Одна точка на первое число месяца — достаточно для средних/пар."""
    return [(month, value) for month in months_between(first, last)]


def panel_from_rows(rows: list) -> tuple:
    """`[(date, series, value)]` → `(sorted_dates, {date: {series: value}})`."""
    by_date: dict = {}
    for day, series, value in rows:
        by_date.setdefault(day, {})[series] = value
    return sorted(by_date), by_date


def sample_rows(n: int = 30) -> list:
    """Синтетические `Stage1Row` для smoke-тестов моделей и walk-forward."""
    rows = []
    month = date(2024, 1, 1)
    for index in range(n):
        row = stage1.Stage1Row(
            meeting_date=date(2024, 1, 15) + timedelta(days=30 * index),
            m0=shift_month(month, 1),
            m1=shift_month(month, 2),
            features={
                "lag1": 0.3 + 0.01 * index,
                "lag2": 0.2 + 0.01 * index,
                "lag3": 0.1 + 0.01 * index,
                "wk_share_rising": 0.5,
                "wk_median": 0.2,
                "wk_median4": 0.25,
                "wk_gasoline4": 0.1,
                "usd_mm": 0.0,
                "key_rate": 16.0,
                "fao_mm": 0.1,
                "month": month.month,
            },
            y0=0.3 + 0.02 * index,
            y1=0.4 + 0.02 * index,
        )
        rows.append(row)
        month = shift_month(month, 1)
    return rows


class Stage1TestBase(unittest.TestCase):
    """Подменяет все загрузчики: тесты не трогают ClickHouse и сеть."""

    def setUp(self):
        self.targets = make_targets()
        self.usd = monthly_points(date(2023, 1, 1), date(2026, 12, 1), 90.0)
        self.key_rate = [
            (date(2023, 1, 1), 16.0),
            (date(2026, 1, 1), 18.0),
        ]
        self.fao = monthly_points(date(2023, 1, 1), date(2026, 12, 1), 100.0)
        self.panel = ([], {})
        patchers = [
            mock.patch.object(
                stage1, "load_targets", lambda: self.targets
            ),
            mock.patch.object(stage1, "load_usd", lambda: self.usd),
            mock.patch.object(stage1, "load_key_rate", lambda: self.key_rate),
            mock.patch.object(stage1, "load_fao", lambda: self.fao),
            mock.patch.object(stage1, "load_weekly_panel", lambda: self.panel),
        ]
        for patcher in patchers:
            patcher.start()
            self.addCleanup(patcher.stop)


class TestClock(Stage1TestBase):
    def test_clock_sep11(self):
        row = stage1.build_rows(
            [date(2026, 9, 11)], CAL, date(2026, 9, 11)
        )[0]
        self.assertEqual(row.m0, date(2026, 9, 1))
        self.assertEqual(row.m1, date(2026, 10, 1))

    def test_clock_oct23(self):
        row = stage1.build_rows(
            [date(2026, 10, 23)], CAL, date(2026, 10, 23)
        )[0]
        self.assertEqual(row.m0, date(2026, 10, 1))
        self.assertEqual(row.m1, date(2026, 11, 1))

    def test_monthly_release_on_meeting_day_is_not_available_at_1330(self):
        # Сентябрьский ИПЦ выходит 2026-10-09 в 19:00; в 13:30 он ещё не вышел.
        row = stage1.build_rows(
            [date(2026, 10, 9)], CAL, date(2026, 10, 9)
        )[0]
        self.assertEqual(row.m0, date(2026, 9, 1))

    def test_meetings_are_sorted_and_unique(self):
        rows = stage1.build_rows(
            [date(2026, 10, 23), date(2026, 9, 11), date(2026, 10, 23)],
            CAL,
            date(2026, 10, 23),
        )
        self.assertEqual(
            [row.meeting_date for row in rows],
            [date(2026, 9, 11), date(2026, 10, 23)],
        )

    def test_assert_no_lookahead_rejects_published_month(self):
        rows = stage1.build_rows(
            [date(2026, 9, 11)], CAL, date(2026, 9, 11)
        )
        stage1.assert_no_lookahead(rows, CAL)
        bad = stage1.Stage1Row(
            meeting_date=rows[0].meeting_date,
            m0=date(2026, 8, 1),  # август уже опубликован 2026-09-09
            m1=date(2026, 9, 1),
            features=rows[0].features,
            y0=None,
            y1=None,
        )
        with self.assertRaises(AssertionError):
            stage1.assert_no_lookahead([bad], CAL)


class TestFeaturesAndTargets(Stage1TestBase):
    def test_no_expectations_sa_mp(self):
        names = set(
            stage1.build_rows(
                [date(2026, 10, 23)], CAL, date(2026, 10, 23)
            )[0].features
        )
        self.assertFalse(
            names
            & {
                "infl_exp_expected",
                "price_expectations",
                "pi_saar",
                "cpi_saar",
            }
        )
        self.assertFalse(any("mp_" in name for name in names))
        self.assertEqual(names, set(stage1.FEATURE_NAMES))

    def test_feature_names_order(self):
        self.assertEqual(
            stage1.FEATURE_NAMES,
            [
                "lag1",
                "lag2",
                "lag3",
                "wk_share_rising",
                "wk_median",
                "wk_median4",
                "wk_gasoline4",
                "usd_mm",
                "key_rate",
                "fao_mm",
                "month",
            ],
        )

    def test_lags_use_latest_published_months(self):
        self.targets = make_targets(
            {
                date(2026, 6, 1): 0.3,
                date(2026, 7, 1): 0.5,
                date(2026, 8, 1): 0.7,
            }
        )
        row = stage1.build_rows(
            [date(2026, 9, 11)], CAL, date(2026, 9, 11)
        )[0]
        self.assertAlmostEqual(row.features["lag1"], 0.7)
        self.assertAlmostEqual(row.features["lag2"], 0.5)
        self.assertAlmostEqual(row.features["lag3"], 0.3)
        self.assertEqual(row.features["month"], 9)

    def test_missing_lag_is_nan_not_zero(self):
        row = stage1.build_rows(
            [date(2024, 2, 15)], CAL, date(2024, 2, 15)
        )[0]
        # m0 = 2024-02: lag1 = 2024-01, lag2 = 2023-12 (нет в синтетике).
        self.assertTrue(math.isfinite(row.features["lag1"]))
        self.assertTrue(math.isnan(row.features["lag2"]))
        self.assertTrue(math.isnan(row.features["lag3"]))

    def test_targets_none_until_published(self):
        row = stage1.build_rows(
            [date(2026, 9, 11)], CAL, date(2026, 9, 11)
        )[0]
        self.assertIsNone(row.y0)  # сентябрь выходит 2026-10-09 19:00
        self.assertIsNone(row.y1)

    def test_y0_observed_at_as_of_after_release(self):
        self.targets = make_targets({date(2026, 9, 1): 0.9})
        row = stage1.build_rows(
            [date(2026, 9, 11)], CAL, date(2026, 10, 23)
        )[0]
        self.assertAlmostEqual(row.y0, 0.9)
        self.assertIsNone(row.y1)  # октябрь выходит 2026-11-09

    def test_y1_observed_at_later_as_of(self):
        self.targets = make_targets(
            {date(2026, 9, 1): 0.9, date(2026, 10, 1): 0.6}
        )
        row = stage1.build_rows(
            [date(2026, 9, 11)], CAL, date(2026, 11, 10)
        )[0]
        self.assertAlmostEqual(row.y0, 0.9)
        self.assertAlmostEqual(row.y1, 0.6)

    def test_as_of_on_release_day_is_1330_not_eod(self):
        # 2026-10-09 19:00 — релиз сентября; срез в 13:30 его ещё не видит.
        self.targets = make_targets({date(2026, 9, 1): 0.9})
        self.assertIsNone(
            stage1.build_rows(
                [date(2026, 9, 11)], CAL, date(2026, 10, 9)
            )[0].y0
        )
        self.assertAlmostEqual(
            stage1.build_rows(
                [date(2026, 9, 11)], CAL, date(2026, 10, 10)
            )[0].y0,
            0.9,
        )

    def test_usd_key_rate_fao_features(self):
        row = stage1.build_rows(
            [date(2026, 10, 23)], CAL, date(2026, 10, 23)
        )[0]
        self.assertAlmostEqual(row.features["usd_mm"], 0.0)
        self.assertAlmostEqual(row.features["key_rate"], 18.0)
        self.assertTrue(math.isfinite(row.features["fao_mm"]))


class TestWeeklyAsOf(Stage1TestBase):
    def test_weekly_asof_excludes_future_week(self):
        # Доступная неделя 2026-09-28 даёт медиану 1.0; будущая 2026-10-05
        # (релиз 2026-10-07 19:00) дала бы 999.0. Заседание в 13:30 не должно
        # видеть будущую неделю.
        panel = panel_from_rows(
            [
                (date(2026, 9, 28), "series_a", 101.0),
                (date(2026, 10, 5), "series_a", 1099.0),
            ]
        )
        with mock.patch.object(stage1, "load_weekly_panel", lambda: panel):
            row = stage1.build_rows(
                [date(2026, 10, 7)], CAL, date(2026, 10, 7)
            )[0]
        self.assertEqual(row.features["wk_median"], 1.0)
        self.assertEqual(row.features["wk_share_rising"], 1.0)

    def test_no_weekly_panel_gives_nan(self):
        row = stage1.build_rows(
            [date(2026, 10, 23)], CAL, date(2026, 10, 23)
        )[0]
        self.assertTrue(math.isnan(row.features["wk_median"]))
        self.assertTrue(math.isnan(row.features["wk_median4"]))


class TestModels(Stage1TestBase):
    def test_b1_last_value_forecasts_lag1(self):
        rows = sample_rows()
        row = rows[-1]
        prediction = stage1.fit_predict(
            rows[:-1], row, "b1_last_value"
        )
        self.assertEqual(set(prediction), {"y0", "y1"})
        self.assertAlmostEqual(prediction["y0"], row.features["lag1"])
        self.assertAlmostEqual(prediction["y1"], row.features["lag1"])

    def test_catboost_and_enet_predict_both_targets(self):
        rows = sample_rows()
        for model in ("catboost_direct", "b3_elastic_net"):
            prediction = stage1.fit_predict(rows[:-1], rows[-1], model)
            self.assertEqual(set(prediction), {"y0", "y1"})
            for target in ("y0", "y1"):
                self.assertTrue(math.isfinite(prediction[target]))

    def test_unknown_model_rejected(self):
        rows = sample_rows()
        with self.assertRaises(ValueError):
            stage1.fit_predict(rows[:-1], rows[-1], "no_such_model")

    def test_degenerate_train_window(self):
        rows = sample_rows(5)
        with self.assertRaises(stage1.cm.DegenerateWindowError):
            stage1.fit_predict(rows[:-1], rows[-1], "b3_elastic_net")

    def test_stage1_row_is_frozen(self):
        row = sample_rows(1)[0]
        with self.assertRaises(FrozenInstanceError):
            row.y0 = 1.0  # type: ignore[misc]


class TestWalkForwardAndEvaluate(Stage1TestBase):
    def test_walk_forward_skips_rows_with_both_targets_missing(self):
        rows = sample_rows(10)
        skipped = stage1.Stage1Row(
            meeting_date=rows[-1].meeting_date + timedelta(days=40),
            m0=shift_month(rows[-1].m0, 1),
            m1=shift_month(rows[-1].m1, 1),
            features=dict(rows[-1].features),
            y0=None,
            y1=None,
        )
        rows.append(skipped)
        predictions = stage1.walk_forward(
            rows, start=6, models={"b1_last_value": stage1.fit_b1}
        )
        self.assertNotIn(
            skipped.meeting_date,
            {item.meeting_date for item in predictions},
        )
        self.assertEqual(len(predictions), 8)  # строки 6, 7, 8, 9 × 2 таргета
        for item in predictions:
            self.assertIn(item.target, {"y0", "y1"})

    def test_evaluate_reports_rmse_mae_and_pairs(self):
        predictions = []
        for index, meeting in enumerate(
            [date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)]
        ):
            for target, actual in (("y0", 1.0), ("y1", 2.0)):
                predictions.append(
                    stage1.Stage1Prediction(
                        meeting_date=meeting,
                        target_month=shift_month(meeting, 1),
                        model="catboost_direct",
                        target=target,
                        y_true=actual,
                        y_pred=actual + 0.5 + 0.1 * index,
                    )
                )
                predictions.append(
                    stage1.Stage1Prediction(
                        meeting_date=meeting,
                        target_month=shift_month(meeting, 1),
                        model="b1_last_value",
                        target=target,
                        y_true=actual,
                        y_pred=actual + 1.0,
                    )
                )
        result = stage1.evaluate(predictions)
        summary = result["summary"]
        self.assertEqual(summary["catboost_direct"]["y0"]["n"], 3)
        self.assertAlmostEqual(
            summary["catboost_direct"]["y0"]["mae"], 0.6, places=12
        )
        self.assertEqual(summary["b1_last_value"]["y1"]["n"], 3)
        pair = result["pairs"]["catboost_direct vs b1_last_value:y0"]
        self.assertEqual(pair["n"], 3)
        self.assertIn("dm_p", pair)
        self.assertEqual(len(pair["delta_ci"]), 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
