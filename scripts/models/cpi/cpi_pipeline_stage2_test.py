#!/usr/bin/env python3
"""Офлайн-тесты `scripts/models/cpi/cpi_pipeline_stage2.py`.

Запуск из корня репозитория:

    make cpi-pipeline-stage2-test

ClickHouse и сеть не нужны: CPI-ряды, ожидания и МП подменяются синтетикой,
календарь публикаций читается из версионированной фикстуры
`scripts/testdata/publication_calendar.csv`. Все времена — наивные, московское
время; заседание считается моментом 13:30.
"""

import math
import sys
import tempfile
import unittest
from dataclasses import FrozenInstanceError, replace
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))

from lib.publication_calendar import PublicationCalendar  # noqa: E402
from models.cpi import cpi_monthly as cm  # noqa: E402
from models.cpi import cpi_pipeline_stage1 as stage1  # noqa: E402
from models.cpi import cpi_pipeline_stage2 as stage2  # noqa: E402

CAL = PublicationCalendar.from_csv(
    ROOT / "scripts" / "testdata" / "publication_calendar.csv"
)
AS_OF = date(2027, 6, 30)
MEETING = date(2026, 10, 23)

# Синтетическая аддитивная сезонность: SA-ряд должен быть близок к константе.
SEASONAL = {
    1: -0.3,
    2: -0.2,
    3: -0.1,
    4: 0.0,
    5: 0.1,
    6: 0.2,
    7: 0.3,
    8: 0.4,
    9: 0.3,
    10: 0.2,
    11: 0.1,
    12: 0.0,
}

MONTHLY_FEATURES = (
    "wk_share_rising",
    "wk_median",
    "wk_median4",
    "wk_gasoline4",
)


def shift_month(day: date, months: int) -> date:
    """Первое число месяца, сдвинутого на `months` от месяца дня `day`."""
    index = day.year * 12 + (day.month - 1) + months
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


def compound(values: list) -> float:
    product = 1.0
    for value in values:
        product *= 1.0 + float(value) / 100.0
    return (product - 1.0) * 100.0


def cpi_points(
    first: date = date(2021, 1, 1),
    last: date = date(2027, 5, 1),
    base: float = 0.4,
) -> list:
    """NSA m/m: сезонность плюс медленная несезонная волна (SA не константа)."""
    return [
        (
            month_end(month),
            base
            + SEASONAL[month.month]
            + 0.15 * math.sin(2.0 * math.pi * index / 7.0),
        )
        for index, month in enumerate(months_between(first, last))
    ]


def flat_points(first: date, last: date, value: float = 1.0) -> list:
    return [(month_end(month), value) for month in months_between(first, last)]


def make_series(
    first: date = date(2021, 1, 1), last: date = date(2027, 5, 1)
) -> dict:
    """Четыре синтетических ряда: общий ИПЦ и три компонента bottom-up."""
    total = cpi_points(first, last)
    return {
        "cpi_all": total,
        "cpi_food": [
            (day, value + 0.03 * ((day.month % 3) - 1))
            for day, value in total
        ],
        "cpi_nonfood": [
            (day, value - 0.02 * ((day.month % 4) - 2))
            for day, value in total
        ],
        "cpi_services": [
            (day, value + 0.01 * ((day.month % 2) - 1))
            for day, value in total
        ],
    }


def make_meetings() -> list:
    """Тридцать шесть заседаний 2024-01-15 … 2026-12-15."""
    months = months_between(date(2024, 1, 1), date(2026, 12, 1))
    return [date(month.year, month.month, 15) for month in months]


def stage1_row(meeting: date) -> stage1.Stage1Row:
    m0 = CAL.next_unpublished_month(
        stage1.SOURCE, stage1.SERIES, stage1.meeting_datetime(meeting)
    )
    return stage1.Stage1Row(
        meeting_date=meeting,
        m0=m0,
        m1=shift_month(m0, 1),
        features={
            "lag1": 0.3,
            "lag2": 0.2,
            "lag3": 0.1,
            "wk_share_rising": 0.5,
            "wk_median": 0.2,
            "wk_median4": 0.25,
            "wk_gasoline4": 0.1,
            "usd_mm": 0.0,
            "key_rate": 16.0,
            "fao_mm": 0.1,
            "month": m0.month,
        },
        y0=0.3,
        y1=0.4,
    )


def make_stage1_rows(meetings: list) -> list:
    return [stage1_row(meeting) for meeting in meetings]


def make_oof(meetings: list) -> dict:
    return {
        meeting: {"m0": 0.30 + 0.05 * index, "m1": 0.40 + 0.05 * index}
        for index, meeting in enumerate(meetings)
    }


def build_rows(
    meetings=None,
    oof=None,
    series=None,
    expectations=None,
    mp=None,
    as_of: date = AS_OF,
) -> list:
    meetings = [MEETING] if meetings is None else meetings
    return stage2.build_rows(
        make_stage1_rows(meetings),
        make_oof(meetings) if oof is None else oof,
        make_series() if series is None else series,
        expectations if expectations is not None else {},
        mp if mp is not None else {},
        CAL,
        as_of,
    )


def row_for(rows: list, meeting: date = MEETING) -> stage2.Stage2Row:
    for row in rows:
        if row.meeting_date == meeting:
            return row
    raise AssertionError(f"нет строки {meeting}")


def raw_nsa_values(series=None) -> dict:
    """Сырые NSA m/m синтетического ряда: `{месяц: значение}` (не из строки)."""
    series = make_series() if series is None else series
    return {
        date(day.year, day.month, 1): value
        for day, value in series["cpi_all"]
    }


def actual_nsa_path(row: stage2.Stage2Row, series=None) -> dict:
    """Фактический NSA-путь из исходного ряда, а не из `sa_path`+факторов."""
    raw = raw_nsa_values(series)
    return {
        month: raw[month] for month in stage2.target_months(row.m0)
    }


def actual_nsa_cum(
    row: stage2.Stage2Row, horizon: int, series=None
) -> float:
    months = stage2.target_months(row.m0)
    path = actual_nsa_path(row, series)
    return compound([path[month] for month in months[:horizon]])


def predictions_for(
    row: stage2.Stage2Row,
    model: str = "catboost_direct",
    delta: float = 0.0,
    true_path=None,
    pred_path=None,
) -> list:
    """Синтетические прогнозы; `true_path`/`pred_path` можно задать явно."""
    months = stage2.target_months(row.m0)
    if true_path is None:
        true_path = {month: row.sa_path[month] for month in months}
    if pred_path is None:
        pred_path = {
            month: true_path[month] + delta for month in months
        }
    return [
        stage2.Stage2Prediction(
            meeting_date=row.meeting_date,
            target_month=month,
            model=model,
            horizon=index + 1,
            y_true=true_path[month],
            y_pred=pred_path[month],
            cpi_sa_available=int(row.features["cpi_sa_available"]),
        )
        for index, month in enumerate(months)
    ]


class TestTargetPath(unittest.TestCase):
    def test_target_path_m0_m5(self):
        row = build_rows()[0]
        months = [shift_month(row.m0, index) for index in range(6)]
        self.assertEqual(sorted(row.sa_path), months)
        self.assertEqual(sorted(row.cum), [1, 3, 6])
        self.assertEqual(sorted(row.seasonal), list(range(1, 13)))
        raw = {
            date(day.year, day.month, 1): value
            for day, value in make_series()["cpi_all"]
        }
        # Независимая проверка: SA-путь = сырой NSA минус сезонный фактор
        # месяца, cumulative — компаундирование этого же пути.
        expected = {
            month: raw[month] - row.seasonal[month.month] for month in months
        }
        for month in months:
            self.assertAlmostEqual(
                row.sa_path[month], expected[month], places=12
            )
        for horizon in (1, 3, 6):
            self.assertAlmostEqual(
                row.cum[horizon],
                compound([expected[month] for month in months[:horizon]]),
                places=12,
            )

    def test_stage1_oof_not_actual(self):
        # Постоянный NSA-ряд 1.0: SA-цель M0 равна 1.0, а OOF stage 1 — 0.123.
        series = {"cpi_all": flat_points(date(2023, 1, 1), date(2027, 5, 1))}
        row = build_rows(
            series=series, oof={MEETING: {"m0": 0.123, "m1": 0.456}}
        )[0]
        self.assertEqual(row.features["stage1_m0"], 0.123)
        self.assertEqual(row.features["stage1_m1"], 0.456)
        self.assertAlmostEqual(row.sa_path[row.m0], 1.0, places=6)
        self.assertNotEqual(
            row.features["stage1_m0"], row.sa_path[row.m0]
        )
        self.assertAlmostEqual(
            row.features["stage1_m0_sa"], 0.123, places=6
        )

    def test_no_future_sa(self):
        # Точка после горизонта M0+5 не должна менять ни признаки, ни путь,
        # ни сезонные снимки (as-of и целевой).
        base = cpi_points(date(2021, 1, 1), date(2027, 3, 1))
        future = base + [(month_end(date(2027, 5, 1)), 9999.0)]
        base_row = build_rows(series={"cpi_all": base})[0]
        future_row = build_rows(series={"cpi_all": future})[0]
        self.assertEqual(base_row.features, future_row.features)
        self.assertEqual(base_row.sa_path, future_row.sa_path)
        self.assertEqual(base_row.cum, future_row.cum)
        self.assertEqual(base_row.seasonal, future_row.seasonal)
        self.assertEqual(base_row.seasonal_asof, future_row.seasonal_asof)

    def test_future_inside_path_does_not_change_features_or_asof(self):
        # Точка внутри (заседание, M0+5] — это метка будущего месяца: она
        # может менять целевой путь, но НЕ признаки и НЕ сезонный снимок
        # заседания (`seasonal_asof`).
        base = cpi_points(date(2021, 1, 1), date(2027, 3, 1))
        inside = month_end(date(2027, 1, 1))
        future = [
            (day, 9999.0 if day == inside else value)
            for day, value in base
        ]
        base_row = build_rows(series={"cpi_all": base})[0]
        future_row = build_rows(series={"cpi_all": future})[0]
        self.assertEqual(base_row.features, future_row.features)
        self.assertEqual(base_row.seasonal_asof, future_row.seasonal_asof)
        self.assertNotEqual(base_row.sa_path, future_row.sa_path)

    def test_current_meeting_has_no_target_path(self):
        row = build_rows(as_of=MEETING)[0]
        self.assertIsNone(row.sa_path)
        self.assertIsNone(row.cum)
        # Признаки при этом доступны: SA-история и OOF stage 1 есть.
        self.assertEqual(row.features["cpi_sa_available"], 1)

    def test_short_sa_history_has_no_path(self):
        series = {
            "cpi_all": cpi_points(date(2025, 9, 1), date(2026, 9, 1))
        }
        row = build_rows(series=series)[0]
        self.assertIsNone(row.sa_path)
        self.assertIsNone(row.cum)
        self.assertEqual(row.features["cpi_sa_available"], 0)
        for name in ("sa_lag1", "sa_lag2", "sa_lag3"):
            self.assertIsNone(row.features[name])

    def test_assert_no_lookahead_rejects_wrong_m0(self):
        row = build_rows()[0]
        stage2.assert_no_lookahead([row], CAL)
        bad = replace(row, m0=shift_month(row.m0, 1))
        with self.assertRaises(AssertionError):
            stage2.assert_no_lookahead([bad], CAL)

    def test_assert_no_lookahead_rejects_wrong_path(self):
        row = build_rows()[0]
        path = dict(row.sa_path)
        path.pop(next(iter(path)))
        with self.assertRaises(AssertionError):
            stage2.assert_no_lookahead([replace(row, sa_path=path)], CAL)

    def test_row_is_frozen(self):
        row = build_rows()[0]
        with self.assertRaises(FrozenInstanceError):
            row.m0 = date(2024, 1, 1)  # type: ignore[misc]


class TestFeatures(unittest.TestCase):
    def test_feature_names_contract(self):
        self.assertEqual(
            list(stage2.FEATURE_NAMES),
            [
                "stage1_m0",
                "stage1_m1",
                "stage1_m0_sa",
                "stage1_m1_sa",
                "sa_lag1",
                "sa_lag2",
                "sa_lag3",
                "cbr_sa",
                "cbr_sa_chg",
                "cbr_sa_available",
                "infl_exp_expected",
                "infl_exp_expected_chg",
                "infl_exp_observed",
                "infl_exp_observed_chg",
                "price_expectations",
                "price_expectations_chg",
                "costs",
                "costs_chg",
                "ibk",
                "ibk_chg",
                "wk_share_rising",
                "wk_median",
                "wk_median4",
                "wk_gasoline4",
                "cpi_sa_available",
                "month",
            ],
        )

    def test_no_policy_block(self):
        forbidden = (
            "unemployment",
            "credit",
            "budget",
            "m2",
            "usdrub",
            "ruonia",
            "wage",
            "savings",
            "text",
        )
        for name in stage2.FEATURE_NAMES:
            for prefix in forbidden:
                self.assertNotIn(prefix, name)
        self.assertFalse(
            any(name.endswith("_chg") for name in MONTHLY_FEATURES)
        )

    def test_sa_lags_and_stage1_transform(self):
        row = build_rows()[0]
        self.assertEqual(row.features["cpi_sa_available"], 1)
        for name in ("sa_lag1", "sa_lag2", "sa_lag3"):
            self.assertTrue(math.isfinite(row.features[name]))
        # SA-трансформация OOF — NSA минус сезонный фактор месяца, снимок
        # строго на заседание (последний опубликованный месяц), а не на конец
        # целевого пути.
        self.assertEqual(sorted(row.seasonal_asof), list(range(1, 13)))
        stage1_m0 = row.features["stage1_m0"]
        self.assertAlmostEqual(
            row.features["stage1_m0_sa"],
            stage1_m0 - row.seasonal_asof[row.m0.month],
            places=12,
        )
        self.assertNotEqual(row.seasonal, row.seasonal_asof)

    def test_cbr_sa_available_per_calendar(self):
        expectations = {
            "cpi": [
                (month_end(date(2026, 7, 1)), 0.30),
                (month_end(date(2026, 8, 1)), 0.40),
                (month_end(date(2026, 9, 1)), 0.50),
                (month_end(date(2026, 10, 1)), 0.90),
            ]
        }
        row = build_rows(expectations=expectations)[0]
        # 2026-09 опубликован 2026-10-16 15:00; 2026-10 ещё не вышел.
        self.assertAlmostEqual(row.features["cbr_sa"], 0.50)
        self.assertAlmostEqual(row.features["cbr_sa_chg"], 0.10)
        self.assertEqual(row.features["cbr_sa_available"], 1)

    def test_cbr_sa_missing_is_none(self):
        row = build_rows(expectations={})[0]
        self.assertIsNone(row.features["cbr_sa"])
        self.assertIsNone(row.features["cbr_sa_chg"])
        self.assertEqual(row.features["cbr_sa_available"], 0)

    def test_cbr_infl_exp_levels_and_changes(self):
        expectations = {
            "infl_exp_expected": [
                (date(2026, 8, 1), 12.5),
                (date(2026, 9, 1), 12.0),
                (date(2026, 10, 1), 11.5),
                (date(2026, 11, 1), 10.0),
            ],
            "infl_exp_observed": [
                (date(2026, 9, 1), 15.0),
                (date(2026, 10, 1), 14.0),
            ],
        }
        row = build_rows(expectations=expectations)[0]
        # Октябрьские ожидания опубликованы 2026-10-21 15:00 — до заседания
        # 2026-10-23 13:30; ноябрьские ещё нет.
        self.assertAlmostEqual(row.features["infl_exp_expected"], 11.5)
        self.assertAlmostEqual(row.features["infl_exp_expected_chg"], -0.5)
        self.assertAlmostEqual(row.features["infl_exp_observed"], 14.0)
        self.assertAlmostEqual(row.features["infl_exp_observed_chg"], -1.0)

    def test_mp_features_levels_and_changes(self):
        mp = {
            "price_expectations_sa": [
                (date(2026, 7, 1), 10.0),
                (date(2026, 8, 1), 11.0),
            ],
            "business_climate_sa": [
                (date(2026, 7, 1), -1.0),
                (date(2026, 8, 1), -2.0),
            ],
            "price_growth_expected_sa": [
                (date(2026, 7, 1), 5.0),
                (date(2026, 8, 1), 6.0),
            ],
        }
        row = build_rows(mp=mp)[0]
        self.assertAlmostEqual(row.features["price_expectations"], 11.0)
        self.assertAlmostEqual(row.features["price_expectations_chg"], 1.0)
        self.assertAlmostEqual(row.features["ibk"], -2.0)
        self.assertAlmostEqual(row.features["ibk_chg"], -1.0)
        self.assertAlmostEqual(row.features["costs"], 6.0)
        self.assertAlmostEqual(row.features["costs_chg"], 1.0)

    def test_missing_features_are_none_not_zero(self):
        row = build_rows(expectations={}, mp={})[0]
        for name in (
            "cbr_sa",
            "cbr_sa_chg",
            "infl_exp_expected",
            "infl_exp_expected_chg",
            "infl_exp_observed",
            "infl_exp_observed_chg",
            "price_expectations",
            "price_expectations_chg",
            "costs",
            "costs_chg",
            "ibk",
            "ibk_chg",
        ):
            self.assertIsNone(row.features[name], name)

    def test_no_oof_zeroes_availability(self):
        row = build_rows(oof={})[0]
        self.assertIsNotNone(row.sa_path)
        self.assertEqual(row.features["cpi_sa_available"], 0)
        self.assertIsNone(row.features["stage1_m0"])
        self.assertIsNone(row.features["stage1_m1"])
        self.assertIsNone(row.features["stage1_m0_sa"])
        self.assertIsNone(row.features["stage1_m1_sa"])

    def test_weekly_diffusion_copied_from_stage1(self):
        row = build_rows()[0]
        self.assertEqual(row.features["wk_share_rising"], 0.5)
        self.assertEqual(row.features["wk_median"], 0.2)
        self.assertEqual(row.features["wk_median4"], 0.25)
        self.assertEqual(row.features["wk_gasoline4"], 0.1)
        self.assertEqual(row.features["month"], row.m0.month)

    def test_returns_only_first_published_month_at_1330(self):
        # Релиз сентября 2026 — 2026-10-09 19:00; заседание 2026-10-09 в 13:30
        # ещё видит август, значит M0 = сентябрь.
        row = build_rows(meetings=[date(2026, 10, 9)])[0]
        self.assertEqual(row.m0, date(2026, 9, 1))


class TestModels(unittest.TestCase):
    def test_min_train_is_24(self):
        self.assertEqual(stage2.MIN_TRAIN, 24)
        self.assertEqual(stage2.DEFAULT_START, 24)

    def test_fit_predict_paths_and_ensemble(self):
        rows = build_rows(meetings=make_meetings())
        train, row = rows[:30], rows[30]
        months = stage2.target_months(row.m0)
        direct = stage2.fit_predict(train, row, "catboost_direct")
        bottomup = stage2.fit_predict(train, row, "catboost_bottomup")
        ensemble = stage2.fit_predict(train, row, "ensemble")
        for values in (direct, bottomup, ensemble):
            self.assertEqual(set(values), set(months))
            for value in values.values():
                self.assertTrue(math.isfinite(value))
        for month in months:
            self.assertAlmostEqual(
                ensemble[month],
                0.5 * (direct[month] + bottomup[month]),
                places=12,
            )

    def test_unknown_model_rejected(self):
        rows = build_rows(meetings=make_meetings())
        with self.assertRaises(ValueError):
            stage2.fit_predict(rows[:10], rows[10], "no_such_model")

    def test_degenerate_train_window(self):
        rows = build_rows(meetings=make_meetings())
        with self.assertRaises(cm.DegenerateWindowError):
            stage2.fit_predict(rows[:2], rows[10], "catboost_direct")

    def test_walk_forward_outputs_all_models_and_horizons(self):
        rows = build_rows(meetings=make_meetings())
        predictions = stage2.walk_forward(rows, start=30, calendar=CAL)
        self.assertEqual(
            {item.model for item in predictions},
            {"catboost_direct", "catboost_bottomup", "ensemble"},
        )
        direct = {
            (item.meeting_date, item.target_month): item
            for item in predictions
            if item.model == "catboost_direct"
        }
        bottomup = {
            (item.meeting_date, item.target_month): item
            for item in predictions
            if item.model == "catboost_bottomup"
        }
        ensemble = {
            (item.meeting_date, item.target_month): item
            for item in predictions
            if item.model == "ensemble"
        }
        self.assertEqual(set(direct), set(bottomup))
        self.assertEqual(set(direct), set(ensemble))
        self.assertEqual(
            {item.horizon for item in predictions}, {1, 2, 3, 4, 5, 6}
        )
        for key, item in ensemble.items():
            self.assertAlmostEqual(
                item.y_pred,
                0.5 * (direct[key].y_pred + bottomup[key].y_pred),
                places=12,
            )
            self.assertEqual(item.cpi_sa_available, 1)

    def test_walk_forward_skips_unavailable_rows(self):
        rows = build_rows(meetings=make_meetings())
        rows[-1] = replace(rows[-1], sa_path=None, cum=None)
        features = dict(rows[-2].features)
        features["cpi_sa_available"] = 0
        rows[-2] = replace(rows[-2], features=features)
        predictions = stage2.walk_forward(
            rows,
            start=30,
            models={"catboost_direct": stage2.fit_catboost_direct},
            calendar=CAL,
        )
        skipped = {rows[-1].meeting_date, rows[-2].meeting_date}
        self.assertFalse(
            {item.meeting_date for item in predictions} & skipped
        )
        self.assertTrue(predictions)
        self.assertEqual(len(predictions), 4 * 6)

    def test_walk_forward_reports_skipped_folds(self):
        rows = build_rows(meetings=make_meetings())
        rows[-1] = replace(rows[-1], sa_path=None, cum=None)
        features = dict(rows[-2].features)
        features["cpi_sa_available"] = 0
        rows[-2] = replace(rows[-2], features=features)
        counts = {}
        stage2.walk_forward(
            rows,
            start=30,
            models={"catboost_direct": stage2.fit_catboost_direct},
            calendar=CAL,
            skipped=counts,
        )
        self.assertEqual(counts["fold_target_unavailable"], 2)

    def test_walk_forward_excludes_unpublished_train_rows(self):
        rows = build_rows(meetings=make_meetings())
        last = rows[-1]
        self.assertFalse(
            stage2.target_published(
                last, CAL, stage2.meeting_datetime(date(2026, 9, 11))
            )
        )
        self.assertTrue(
            stage2.target_published(
                last, CAL, stage2.meeting_datetime(date(2027, 6, 30))
            )
        )
        # Заседание rows[30] (2026-07-15): train-строка rows[29] (M0=2026-06,
        # конец 2026-11) ещё не опубликована, а rows[23] (конец 2026-05) — да.
        test_at = stage2.meeting_datetime(rows[30].meeting_date)
        self.assertFalse(stage2.target_published(rows[29], CAL, test_at))
        self.assertTrue(stage2.target_published(rows[23], CAL, test_at))
        counts = {}
        stage2.walk_forward(
            rows,
            start=30,
            models={"catboost_direct": stage2.fit_catboost_direct},
            calendar=CAL,
            skipped=counts,
        )
        self.assertGreater(counts["train_rows_excluded_unpublished"], 0)

    def test_walk_forward_requires_calendar(self):
        rows = build_rows(meetings=make_meetings())
        with self.assertRaises(ValueError):
            stage2.walk_forward(rows, start=30)

    def test_walk_forward_rejects_unsorted_rows(self):
        rows = build_rows(meetings=make_meetings())
        rows[3], rows[4] = rows[4], rows[3]
        with self.assertRaises(ValueError):
            stage2.walk_forward(rows, start=30, calendar=CAL)


class TestEvaluate(unittest.TestCase):
    @staticmethod
    def _predictions(errors: dict, meetings: int = 3) -> list:
        result = []
        for index in range(meetings):
            meeting = date(2024, 1 + index, 15)
            m0 = date(2024, 1 + index, 1)
            true_path = [0.4, 0.5, 0.3, 0.2, 0.4, 0.6]
            for model, error in errors.items():
                for position, value in enumerate(true_path):
                    result.append(
                        stage2.Stage2Prediction(
                            meeting_date=meeting,
                            target_month=shift_month(m0, position),
                            model=model,
                            horizon=position + 1,
                            y_true=value,
                            y_pred=value + error,
                            cpi_sa_available=1,
                        )
                    )
        return result

    def test_summary_and_cumulative(self):
        result = stage2.evaluate(
            self._predictions(
                {"catboost_direct": 0.1, "catboost_bottomup": 0.2}
            )
        )
        direct = result["summary"]["catboost_direct"]
        self.assertEqual(direct[1]["n"], 3)
        self.assertAlmostEqual(direct[1]["rmse"], 0.1, places=12)
        cumulative = result["cumulative"]["catboost_direct"]
        self.assertEqual(cumulative[1]["n"], 3)
        self.assertAlmostEqual(cumulative[1]["rmse"], 0.1, places=12)
        self.assertEqual(cumulative[3]["n"], 3)
        self.assertEqual(cumulative[6]["n"], 3)
        # cumulative согласован с путём: ошибка накопленного h1 = ошибке M0.
        self.assertAlmostEqual(cumulative[1]["rmse"], 0.1, places=12)

    def test_cumulative_matches_path_compounding(self):
        predictions = self._predictions({"catboost_direct": 0.1})
        result = stage2.evaluate(predictions)
        true_path = [0.4, 0.5, 0.3, 0.2, 0.4, 0.6]
        expected = compound(
            [value + 0.1 for value in true_path[:3]]
        ) - compound(true_path[:3])
        self.assertAlmostEqual(
            result["cumulative"]["catboost_direct"][3]["rmse"],
            abs(expected),
            places=12,
        )

    def test_pairs_structure(self):
        result = stage2.evaluate(
            self._predictions(
                {
                    "catboost_direct": 0.1,
                    "catboost_bottomup": 0.2,
                    "ensemble": 0.15,
                }
            )
        )
        pair = result["pairs"]["catboost_direct vs catboost_bottomup:h3"]
        self.assertEqual(pair["n"], 3)
        self.assertIn("dm_p", pair)
        self.assertEqual(len(pair["delta_ci"]), 2)
        self.assertLess(pair["delta"], 0.0)

    def test_ensemble_not_worse_flag(self):
        better = stage2.evaluate(
            self._predictions(
                {
                    "catboost_direct": 0.2,
                    "catboost_bottomup": -0.2,
                    "ensemble": 0.0,
                }
            )
        )
        self.assertTrue(better["ensemble"][1]["not_worse"])
        worse = stage2.evaluate(
            self._predictions(
                {
                    "catboost_direct": 0.1,
                    "catboost_bottomup": 0.2,
                    "ensemble": 0.15,
                }
            )
        )
        self.assertFalse(worse["ensemble"][1]["not_worse"])


class TestNsaBacktransform(unittest.TestCase):
    def test_backtransform_recovers_actual_nsa(self):
        row = build_rows()[0]
        back = stage2.nsa_backtransform(row, row.sa_path)
        self.assertEqual(set(back), set(row.sa_path))
        actual = actual_nsa_path(row)
        for month, value in back.items():
            self.assertAlmostEqual(value, actual[month], places=12)
        self.assertAlmostEqual(
            stage2.nsa_backtransform(row, row.sa_path)[row.m0],
            actual[row.m0],
            places=12,
        )

    def test_backtransform_shifts_prediction(self):
        row = build_rows()[0]
        predicted = {
            month: value + 0.1 for month, value in row.sa_path.items()
        }
        back = stage2.nsa_backtransform(row, predicted)
        for month, value in back.items():
            self.assertAlmostEqual(
                value, actual_nsa_path(row)[month] + 0.1, places=12
            )

    def test_backtransform_requires_factors(self):
        row = build_rows()[0]
        with self.assertRaises(ValueError):
            stage2.nsa_backtransform(replace(row, seasonal={}), row.sa_path)


class TestCbrSaBacktest(unittest.TestCase):
    def test_compares_published_cbr_sa(self):
        row = build_rows()[0]
        months = stage2.target_months(row.m0)
        predictions = predictions_for(row)
        official = {
            month: 0.1 * index for index, month in enumerate(months)
        }
        points = [
            (month_end(month), official[month]) for month in months
        ]
        result = stage2.cbr_sa_backtest(
            predictions, [row], points, CAL, AS_OF
        )
        expected = abs(row.sa_path[months[0]] - official[months[0]])
        self.assertEqual(result["h1"]["n"], 1)
        self.assertAlmostEqual(result["h1"]["rmse"], expected, places=12)
        self.assertEqual(result["h6"]["n"], 1)
        self.assertAlmostEqual(
            result["h6"]["rmse"],
            abs(row.sa_path[months[5]] - official[months[5]]),
            places=12,
        )

    def test_unpublished_cbr_sa_is_excluded(self):
        row = build_rows()[0]
        predictions = predictions_for(row)
        month = stage2.target_months(row.m0)[0]
        points = [(month_end(month), 1.0)]
        # Сезонный снимок ЦБ за октябрь 2026 выходит 2026-11-18 (fallback).
        result = stage2.cbr_sa_backtest(
            predictions, [row], points, CAL, date(2026, 10, 31)
        )
        self.assertEqual(result["h1"]["n"], 0)


class TestGate2(unittest.TestCase):
    @staticmethod
    def _expected_error(row: stage2.Stage2Row, horizon: int, delta: float):
        """Ожидаемая ошибка NSA-back-transform cum при прогнозе SA+delta."""
        months = stage2.target_months(row.m0)
        actual = actual_nsa_path(row)
        predicted = {month: actual[month] + delta for month in months}
        return compound(
            [predicted[month] for month in months[:horizon]]
        ) - compound([actual[month] for month in months[:horizon]])

    def _setup(self, delta: float = 0.05):
        meetings = make_meetings()[:6]
        rows = build_rows(meetings=meetings)
        predictions = []
        live3 = {}
        for row in rows:
            predictions.extend(predictions_for(row, delta=delta))
            live3[row.meeting_date] = {
                "h1": actual_nsa_cum(row, 1) + 0.3,
                "h3": actual_nsa_cum(row, 3) + 0.3,
                "h6": actual_nsa_cum(row, 6) + 0.3,
                "origin": shift_month(row.m0, -1),
            }
        return rows, predictions, live3

    def test_pass_when_stage2_within_live3_plus_five_percent(self):
        rows, predictions, live3 = self._setup(delta=0.05)
        result = stage2.gate2_report(predictions, rows, live3)
        self.assertEqual(result["decision"], "PASS")
        self.assertEqual(result["n"], 6)
        for horizon in (1, 3, 6):
            item = result["horizons"][f"h{horizon}"]
            expected = [
                self._expected_error(row, horizon, 0.05) for row in rows
            ]
            rmse_expected = math.sqrt(
                sum(error * error for error in expected) / len(expected)
            )
            self.assertAlmostEqual(
                item["rmse_stage2"], rmse_expected, places=9
            )
            self.assertAlmostEqual(item["rmse_live3"], 0.3, places=12)
            self.assertLessEqual(item["rmse_stage2"], item["threshold"])
            self.assertAlmostEqual(
                item["threshold"], item["rmse_live3"] * 1.05
            )

    def test_fail_when_stage2_worse(self):
        rows, predictions, live3 = self._setup(delta=0.5)
        result = stage2.gate2_report(predictions, rows, live3)
        self.assertEqual(result["decision"], "FAIL")
        for horizon in (1, 3, 6):
            item = result["horizons"][f"h{horizon}"]
            self.assertGreater(item["rmse_stage2"], item["threshold"])

    def test_origin_mismatch_is_excluded(self):
        rows, predictions, live3 = self._setup()
        first = sorted(live3)[0]
        live3[first]["origin"] = sorted(live3)[0]
        result = stage2.gate2_report(predictions, rows, live3)
        self.assertEqual(result["n"], 5)
        self.assertEqual(
            result["excluded_target_mismatch"], [first.isoformat()]
        )

    def test_insufficient_common_meetings(self):
        rows, predictions, live3 = self._setup()
        only = sorted(live3)[0]
        result = stage2.gate2_report(
            predictions, rows, {only: live3[only]}
        )
        self.assertEqual(result["decision"], "INSUFFICIENT")
        self.assertEqual(result["n"], 1)

    def test_gate2_reports_live3_missing_skip(self):
        rows, predictions, live3 = self._setup()
        first = sorted(live3)[0]
        live3[first] = {
            "h1": None,
            "h3": None,
            "h6": None,
            "origin": shift_month(first, -1),
        }
        result = stage2.gate2_report(predictions, rows, live3)
        self.assertEqual(result["n"], 5)
        self.assertEqual(result["skipped"]["live3_missing"], 1)

    def test_nsa_backtest_reports_row_missing_skip(self):
        row = build_rows()[0]
        result = stage2.nsa_backtest(predictions_for(row), [])
        self.assertEqual(result["skipped"]["row_missing"], 1)
        self.assertEqual(result["h1"]["n"], 0)

    def test_live3_cache_is_reused(self):
        calls = []

        def builder(meetings, as_of):
            calls.append((tuple(meetings), as_of))
            return {
                day: {
                    "h1": 0.1,
                    "h3": 0.2,
                    "h6": 0.3,
                    "origin": shift_month(day, -1),
                }
                for day in meetings
            }

        meetings = [date(2024, 1, 1), date(2024, 2, 1)]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "live3.json"
            first = stage2.load_live3_paths(
                meetings, AS_OF, cache_path=path, builder=builder
            )
            second = stage2.load_live3_paths(
                meetings, AS_OF, cache_path=path, builder=builder
            )
            third = stage2.load_live3_paths(
                meetings, date(2027, 7, 31), cache_path=path, builder=builder
            )
        self.assertEqual(first, second)
        self.assertEqual(len(calls), 2)
        self.assertEqual(first[date(2024, 1, 1)]["h1"], 0.1)
        self.assertEqual(third[date(2024, 1, 1)]["h1"], 0.1)
        self.assertEqual(
            first[date(2024, 1, 1)]["origin"], date(2023, 12, 1)
        )


class TestStage1Oof(unittest.TestCase):
    def test_stage1_oof_from_predictions(self):
        predictions = [
            stage1.Stage1Prediction(
                meeting_date=date(2024, 1, 15),
                target_month=date(2024, 2, 1),
                model="catboost_direct",
                target="y0",
                y_true=0.3,
                y_pred=0.31,
            ),
            stage1.Stage1Prediction(
                meeting_date=date(2024, 1, 15),
                target_month=date(2024, 3, 1),
                model="catboost_direct",
                target="y1",
                y_true=0.4,
                y_pred=0.41,
            ),
            stage1.Stage1Prediction(
                meeting_date=date(2024, 1, 15),
                target_month=date(2024, 2, 1),
                model="b1_last_value",
                target="y0",
                y_true=0.3,
                y_pred=0.3,
            ),
        ]
        result = stage2.stage1_oof_from_predictions(predictions)
        self.assertEqual(
            result, {date(2024, 1, 15): {"m0": 0.31, "m1": 0.41}}
        )


class TestArtifacts(unittest.TestCase):
    def test_write_predictions_and_json(self):
        rows = build_rows(meetings=make_meetings()[:6])
        predictions = predictions_for(rows[0])
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "predictions.csv"
            json_path = Path(tmp) / "result.json"
            stage2.write_predictions(csv_path, predictions)
            stage2.write_json(json_path, {"as_of": "2027-06-30"})
            text = csv_path.read_text(encoding="utf-8")
            self.assertIn("meeting_date,target_month,model,horizon", text)
            self.assertIn("catboost_direct", text)
            self.assertIn("2027-06-30", json_path.read_text(encoding="utf-8"))

    def test_report_contains_gate2_and_limitations(self):
        rows = build_rows(meetings=make_meetings()[:6])
        predictions = predictions_for(rows[0])
        result = stage2.evaluate(predictions)
        payload = {
            "as_of": AS_OF.isoformat(),
            "start": 24,
            "seed": stage2.SEED,
            "n_meetings": len(rows),
            "n_predictions": len(predictions),
            "first_meeting": rows[0].meeting_date.isoformat(),
            "last_meeting": rows[-1].meeting_date.isoformat(),
            "features": list(stage2.FEATURE_NAMES),
            "models": list(stage2.MODELS),
            "summary": result["summary"],
            "cumulative": result["cumulative"],
            "pairs": result["pairs"],
            "ensemble": result["ensemble"],
            "nsa_backtest": {},
            "gate2": None,
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "report.md"
            stage2.write_report(path, payload)
            text = path.read_text(encoding="utf-8")
        self.assertIn("NSA back-transform", text)
        self.assertIn("Gate 2", text)
        self.assertIn("не рассчитан", text)
        self.assertIn("Что не проверено", text)


if __name__ == "__main__":
    unittest.main(verbosity=2)
