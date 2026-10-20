#!/usr/bin/env python3
"""Офлайн-тесты `scripts/cb_comp_parity.py` (issue #190).

Запуск из корня репозитория (pytest не нужен):

    make cb-comp-parity-test

Набор ОФЛАЙН: ClickHouse не спрашивается, архив CB_COMP не нужен — все данные
синтетические, xlsx-книги собираются во временном каталоге openpyxl. Живой
прогон — отдельная цель `make cb-comp-parity`.

Что пинуется:

* предзарегистрированные константы (окна, горизонты, seed, margin, пины RMSE);
* формула накопленного прироста и нормализация месяцев;
* нормальный набор статьи (54 месяца, 2020-04 и 2022 исключены);
* чтение `TARGET_FULL`/`StfCpiCB`/`catboostVCB_IBK.xlsx` и сверка флага
  `Учитывать` с правилом статьи;
* DM-HLN и TOST на синтетике (знак, эквивалентность, детерминизм бутстрапа);
* отсутствие утечки: тестовый месяц не в train, cutoff раньше публикации метки,
  таргет не попадает в признаки;
* smoke CatBoost direct на маленькой выборке.
"""

import math
import sys
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd
from openpyxl import Workbook

# Модуль и тест лежат рядом, запуск прямой (`python3 scripts/...`).
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from models.cb_comp import cb_comp_parity as parity
from models.cpi import cpi_monthly as cm
from lib import eval_utils as eu


def _month_range(first: date, last: date) -> list:
    return parity.month_range(first, last)


def _write_book(path: Path, sheets: dict) -> None:
    """Записать xlsx из словаря `имя листа → (DataFrame | list[list])`."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, content in sheets.items():
            if isinstance(content, pd.DataFrame):
                content.to_excel(writer, sheet_name=name, index=False)
            else:
                frame = pd.DataFrame(content)
                frame.to_excel(writer, sheet_name=name, index=False, header=False)


def _make_target(months: list) -> pd.DataFrame:
    """Синтетический TARGET_FULL: h-колонки — накопление из месячного уровня.

    Колонки строятся из одного SA-уровня (100 = предыдущий месяц), поэтому
    `target_consistency` на такой книге обязан дать ~0.
    """
    levels = np.array([100.5 + 0.1 * math.sin(index / 2) for index in range(len(months))])
    log_levels = np.log(levels / 100.0)
    rows = []
    for index, month in enumerate(months):
        row = {"дата": pd.Timestamp(month)}
        for horizon in parity.ALL_HORIZONS:
            if index < horizon - 1:
                row[horizon] = np.nan
            else:
                row[horizon] = (
                    math.exp(float(log_levels[index - horizon + 1 : index + 1].sum())) - 1.0
                ) * 100.0
        rows.append(row)
    return pd.DataFrame(rows)


def _make_cb_sheet(months: list, target: pd.DataFrame, errors: list, normal: set, sheet_title: str) -> list:
    """Лист `StfCpiCB`-формата: строка заголовка + данные."""
    rows = [
        ["", "", sheet_title, None, None, None, None],
        ["Учитывать при расчете ошибки", "дата", 1, 2, 3, 6],
    ]
    for index, month in enumerate(months):
        rows.append(
            [
                month in normal,
                pd.Timestamp(month),
                target.iloc[index][1] + errors[index],
                target.iloc[index][2] + errors[index],
                target.iloc[index][3] + errors[index],
                target.iloc[index][6] + errors[index],
            ]
        )
    return rows


def _make_cb_xlsx(months: list, target: pd.DataFrame, errors: list) -> pd.DataFrame:
    rows = []
    for index, month in enumerate(months):
        rows.append(
            {
                "model_cpi_group": "1 - 9470",
                "model_region": "643->643004.АГ",
                "дата": pd.Timestamp(month),
                "1": target.iloc[index][1] + errors[index],
                "2": target.iloc[index][2] + errors[index],
                "3": target.iloc[index][3] + errors[index],
                "4": target.iloc[index][4] + errors[index],
                "5": target.iloc[index][5] + errors[index],
                "6": target.iloc[index][6] + errors[index],
            }
        )
    return pd.DataFrame(rows)


def _make_archive(root: Path, with_dm: bool = False) -> Path:
    """Полный синтетический архив: target, StfCpiCB, StfCpiCB_nomp, xlsx, DM."""
    archive = root / parity.ARCHIVE_ROOT_NAME
    archive.mkdir(parents=True)
    months = _month_range(parity.TEST_START, parity.TEST_END)
    normal = set(parity.normal_months())
    # TARGET_FULL начинается раньше теста: h2…h6 на 2019-01 должны иметь
    # историю, иначе синтетический архив не воспроизводит форму настоящего.
    target_months = _month_range(date(2018, 1, 1), parity.TEST_END)
    target = _make_target(target_months)
    target_test = target[target["дата"].isin([pd.Timestamp(month) for month in months])].reset_index(drop=True)
    errors_mp = [0.05 * math.sin(index) + 0.02 * math.cos(index / 3) for index in range(len(months))]
    errors_nomp = [0.10 * math.cos(index) + 0.03 * math.sin(index / 2) for index in range(len(months))]

    sheets = {
        "TARGET_FULL": target,
        "StfCpiCB": _make_cb_sheet(
            months, target_test, errors_mp, normal, "PREDICTION (Все товары и услуги без коммунальных услуг)"
        ),
        "StfCpiCB_nomp": _make_cb_sheet(
            months, target_test, errors_nomp, normal, "PREDICTION (Все товары и услуги без коммунальных услуг)"
        ),
    }
    if with_dm:
        dm_values = {}
        for horizon in parity.HORIZONS:
            errors_a = [errors_mp[index] for index in range(len(months))]
            errors_b = [errors_nomp[index] for index in range(len(months))]
            # В DM-таблицу пишем воспроизведение формулы авторов на нормальных
            # месяцах — именно это проверяет `check_archive`.
            normal_indices = [index for index, month in enumerate(months) if month in normal]
            dm_values[f"h{horizon}_StfCpiCB"] = parity.authors_dm_pvalue(
                [errors_a[index] for index in normal_indices],
                [errors_b[index] for index in normal_indices],
            )
        dm_frame = pd.DataFrame(
            {"DM": list(dm_values.values()), "id": list(dm_values.keys())}
        )
        _write_book(archive / parity.DM_XLSX, {parity.DM_SHEET: dm_frame})

    _write_book(archive / parity.TARGET_XLSX, sheets)
    _write_book(
        archive / parity.CB_XLSX,
        {"Sheet1": _make_cb_xlsx(months, target_test, errors_mp)},
    )
    return archive


class TestPinnedDesign(unittest.TestCase):
    """Константы, зафиксированные до прогона (спека §5.2)."""

    def test_horizons_and_windows(self):
        self.assertEqual(parity.HORIZONS, (1, 2, 3, 6))
        self.assertEqual(parity.HORIZONS_EXTRA, (4, 5))
        self.assertEqual(parity.TRAIN_START, date(2010, 3, 1))
        self.assertEqual(parity.TRAIN_END, date(2018, 12, 1))
        self.assertEqual(parity.TEST_START, date(2019, 1, 1))
        self.assertEqual(parity.TEST_END, date(2024, 7, 1))

    def test_seed_and_margin(self):
        self.assertEqual(parity.SEED, 20261018)
        self.assertEqual(parity.MARGIN_FRACTION, 0.10)
        self.assertEqual(parity.BOOTSTRAP_BLOCK, 3)
        self.assertEqual(parity.BOOTSTRAP_N, 2000)

    def test_cb_rmse_pins(self):
        expected = {1: 0.158805, 2: 0.296655, 3: 0.443623, 6: 0.971459}
        self.assertEqual(parity.CB_RMSE_PINNED, expected)

    def test_train_outlier_months(self):
        self.assertEqual(
            parity.TRAIN_OUTLIER_MONTHS,
            {date(2014, 12, 1), date(2015, 1, 1), date(2015, 2, 1), date(2015, 3, 1)},
        )
        self.assertTrue(parity.OUTLIER_MONTHS.isdisjoint(parity.normal_months()))

    def test_margin_is_ten_percent_of_pinned_rmse(self):
        for horizon, rmse in parity.CB_RMSE_PINNED.items():
            self.assertAlmostEqual(parity.MARGIN_FRACTION * rmse, 0.10 * rmse, places=12)
        self.assertAlmostEqual(parity.MARGIN_FRACTION * 0.158805, 0.0158805, places=12)
        self.assertAlmostEqual(parity.MARGIN_FRACTION * 0.971459, 0.0971459, places=12)

    def test_catboost_defaults(self):
        params = parity.CATBOOST_DEFAULTS
        self.assertEqual(params["loss_function"], "RMSE")
        self.assertEqual(params["random_seed"], parity.SEED)
        self.assertFalse(params["verbose"])
        self.assertFalse(params["allow_writing_files"])
        self.assertEqual(parity.CATBOOST_CAT_FEATURES, ("month",))
        names = cm.feature_names(with_mp=True)
        self.assertEqual(parity.catboost_params(names)["cat_features"], [len(names) - 1])

    def test_feature_names_contract(self):
        base = ["y_lag1", "y_lag2", "usd_mm", "key_rate", "fao_mm", "month"]
        self.assertEqual(cm.feature_names(with_mp=False), base)
        with_mp = cm.feature_names(with_mp=True)
        self.assertEqual(with_mp[:5], base[:5])
        self.assertEqual(with_mp[-1], "month")
        self.assertEqual(len(with_mp), len(base) + 7)
        self.assertIn("price_expectations_sa", with_mp)
        self.assertIn("credit_conditions", with_mp)


    def test_forecast_origin_matches_cb_comp(self):
        target = date(2024, 7, 1)
        self.assertEqual(parity.forecast_origin(target, 1), date(2024, 7, 1))
        self.assertEqual(parity.forecast_origin(target, 2), date(2024, 6, 1))
        self.assertEqual(parity.forecast_origin(target, 3), date(2024, 5, 1))
        self.assertEqual(parity.forecast_origin(target, 6), date(2024, 2, 1))

    def test_rows_for_horizon_take_origin_features(self):
        feature_by_month = {
            date(2024, 2, 1): {"marker": "origin"},
            date(2024, 7, 1): {"marker": "target"},
        }
        target_row = parity.ParityRow(
            month=date(2024, 7, 1),
            features={"marker": "target"},
            targets={1: 1.0, 6: 6.0},
        )
        rows = parity.rows_for_horizon([target_row], feature_by_month, 6)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].month, date(2024, 7, 1))
        self.assertEqual(rows[0].features["marker"], "origin")
        self.assertEqual(rows[0].targets[6], 6.0)

    def test_leakage_origin_check_accepts_valid(self):
        row = parity.ParityRow(
            month=date(2024, 7, 1),
            features={"marker": "origin"},
            targets={6: 6.0},
        )
        parity.assert_no_leakage_origin([row], 6)


class TestCalendar(unittest.TestCase):
    def test_month_start_variants(self):
        self.assertEqual(parity.month_start(datetime(2024, 7, 19, 12, 0)), date(2024, 7, 1))
        self.assertEqual(parity.month_start(date(2024, 7, 19)), date(2024, 7, 1))
        self.assertEqual(parity.month_start("2024-07-19"), date(2024, 7, 1))

    def test_month_range(self):
        self.assertEqual(
            parity.month_range(date(2020, 11, 1), date(2021, 2, 1)),
            [date(2020, 11, 1), date(2020, 12, 1), date(2021, 1, 1), date(2021, 2, 1)],
        )

    def test_normal_months_are_54(self):
        normal = parity.normal_months()
        self.assertEqual(len(normal), 54)
        self.assertEqual(normal[0], date(2019, 1, 1))
        self.assertEqual(normal[-1], date(2024, 7, 1))

    def test_outliers_excluded(self):
        normal = set(parity.normal_months())
        self.assertNotIn(date(2020, 4, 1), normal)
        for month in range(1, 13):
            self.assertNotIn(date(2022, month, 1), normal)
        for month in (12,):
            self.assertNotIn(date(2014, month, 1), normal)
        for month in (1, 2, 3):
            self.assertNotIn(date(2015, month, 1), normal)

    def test_test_window_has_67_months(self):
        self.assertEqual(len(parity.month_range(parity.TEST_START, parity.TEST_END)), 67)


class TestCumulativeGrowth(unittest.TestCase):
    def test_constant_level(self):
        index = [date(2024, month, 1) for month in range(1, 9)]
        levels = pd.Series([101.0] * 8, index=index)
        result = parity.cumulative_growth(levels, horizons=(1, 2, 6))
        self.assertAlmostEqual(result.loc[index[0], 1], 1.0, places=10)
        self.assertTrue(math.isnan(result.loc[index[0], 2]))
        self.assertAlmostEqual(result.loc[index[1], 1], 1.0, places=10)
        self.assertAlmostEqual(result.loc[index[1], 2], 2.01, places=10)
        self.assertAlmostEqual(result.loc[index[6], 6], 6.1520150601, places=8)

    def test_first_h_minus_one_rows_are_nan(self):
        index = [date(2024, month, 1) for month in range(1, 5)]
        levels = pd.Series([100.5, 100.6, 100.7, 100.8], index=index)
        result = parity.cumulative_growth(levels, horizons=(1, 2, 3))
        self.assertFalse(math.isnan(result.loc[index[0], 1]))
        self.assertTrue(math.isnan(result.loc[index[0], 2]))
        self.assertTrue(math.isnan(result.loc[index[1], 3]))

    def test_non_positive_level_rejected(self):
        with self.assertRaises(ValueError):
            parity.cumulative_growth(pd.Series([100.0, 0.0]), horizons=(1,))


class TestTargetConsistency(unittest.TestCase):
    """`TARGET_FULL[h]` — накопление `TARGET_FULL[1]` (проверка до обучения)."""

    def test_consistent_target(self):
        months = parity.month_range(date(2024, 1, 1), date(2024, 8, 1))
        target = _make_target(months).set_index("дата")
        target.index = [parity.month_start(value) for value in target.index]
        self.assertLess(parity.target_consistency(target), 1e-12)

    def test_broken_column_detected(self):
        months = parity.month_range(date(2024, 1, 1), date(2024, 8, 1))
        target = _make_target(months).set_index("дата")
        target.index = [parity.month_start(value) for value in target.index]
        target[3] = target[3] + 1.0
        self.assertGreater(parity.target_consistency(target), 1e-3)


class TestArchiveReaders(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.archive = _make_archive(self.root)

    def tearDown(self):
        self.tmp.cleanup()

    def test_read_target_full(self):
        target = parity.read_target_full(self.archive)
        self.assertIn(date(2019, 1, 1), target.index)
        self.assertIn(date(2024, 7, 1), target.index)
        self.assertEqual(list(target.columns), [1, 2, 3, 4, 5, 6])
        self.assertGreater(float(target.loc[date(2019, 1, 1), 1]), 0.0)
        self.assertFalse(pd.isna(target.loc[date(2019, 1, 1), 2]))

    def test_read_cb_sheet_and_flags(self):
        sheet = parity.read_cb_sheet(self.archive)
        self.assertIn("normal", sheet.columns)
        self.assertEqual(parity.normal_flag_months(sheet), parity.normal_months())
        self.assertFalse(bool(sheet.at[date(2022, 5, 1), "normal"]))
        self.assertTrue(bool(sheet.at[date(2023, 5, 1), "normal"]))

    def test_assert_normal_flags_rejects_mismatch(self):
        with self.assertRaises(ValueError):
            parity.assert_normal_flags([date(2019, 1, 1)], [date(2019, 1, 1), date(2019, 2, 1)])

    def test_read_cb_xlsx(self):
        frame = parity.read_cb_xlsx(self.archive)
        self.assertEqual(list(frame.columns), [1, 2, 3, 4, 5, 6])
        self.assertIn(date(2019, 1, 1), frame.index)

    def test_source_max_diff_detects_mismatch(self):
        sheet = parity.read_cb_sheet(self.archive)
        xlsx = parity.read_cb_xlsx(self.archive)
        self.assertLessEqual(parity.source_max_diff(sheet, xlsx, parity.normal_months()), 1e-12)
        broken = xlsx.copy()
        broken.at[date(2019, 1, 1), 1] += 1.0
        self.assertGreater(parity.source_max_diff(sheet, broken, parity.normal_months()), 0.5)

    def test_resolve_archive_explicit(self):
        self.assertEqual(parity.resolve_archive(str(self.archive)), self.archive)

    def test_resolve_archive_missing(self):
        with self.assertRaises(SystemExit):
            parity.resolve_archive(str(self.root / "nope"))


class TestCheckArchive(unittest.TestCase):
    """Полный `check_archive` на синтетическом архиве с подменёнными пинами."""

    def test_check_archive_passes_with_matching_pins(self):
        with tempfile.TemporaryDirectory() as tmp:
            archive = _make_archive(Path(tmp), with_dm=True)
            target = parity.read_target_full(archive)
            sheet = parity.read_cb_sheet(archive)
            normal = parity.normal_months()
            pinned = {
                horizon: eu.rmse(parity._cb_errors(sheet, target, horizon, normal))
                for horizon in parity.HORIZONS
            }
            original_pins = parity.CB_RMSE_PINNED
            try:
                parity.CB_RMSE_PINNED = pinned
                data = parity.check_archive(archive)
            finally:
                parity.CB_RMSE_PINNED = original_pins
            self.assertEqual(data["normal"], normal)
            for horizon in parity.HORIZONS:
                self.assertAlmostEqual(data["rmse_cb"][horizon], pinned[horizon], places=12)
                self.assertAlmostEqual(
                    data["margins"][horizon], 0.10 * pinned[horizon], places=12
                )

    def test_check_archive_rejects_wrong_rmse_pin(self):
        with tempfile.TemporaryDirectory() as tmp:
            archive = _make_archive(Path(tmp), with_dm=True)
            original_pins = parity.CB_RMSE_PINNED
            try:
                parity.CB_RMSE_PINNED = {1: 999.0, 2: 999.0, 3: 999.0, 6: 999.0}
                with self.assertRaises(ValueError):
                    parity.check_archive(archive)
            finally:
                parity.CB_RMSE_PINNED = original_pins


class TestBuildAndLeakage(unittest.TestCase):
    def _feature_row(self, month: date) -> parity.FeatureRow:
        return parity.FeatureRow(month=month, features={"y_lag1": 0.2, "month": month.month})

    def _target(self) -> pd.DataFrame:
        months = [date(2018, 12, 1), date(2019, 1, 1), date(2019, 2, 1)]
        return pd.DataFrame(
            {1: [0.1, 0.2, 0.3], 2: [0.2, 0.3, 0.4]},
            index=pd.Index(months, name="month"),
        )

    def test_build_rows_attaches_target(self):
        rows, missing = parity.build_rows(
            [self._feature_row(date(2018, 12, 1)), self._feature_row(date(2019, 1, 1))],
            self._target(),
            horizons=(1, 2),
        )
        self.assertEqual(missing, [])
        self.assertEqual(rows[0].targets, {1: 0.1, 2: 0.2})
        self.assertEqual(rows[1].targets, {1: 0.2, 2: 0.3})

    def test_build_rows_reports_missing_without_filling(self):
        target = self._target()
        target.loc[date(2019, 1, 1), 2] = np.nan
        rows, missing = parity.build_rows(
            [self._feature_row(date(2019, 1, 1))], target, horizons=(1, 2)
        )
        self.assertEqual(rows, [])
        self.assertEqual(missing[0][1], "нет TARGET_FULL h2")

    def test_split_rows(self):
        rows = [
            parity.ParityRow(date(2010, 3, 1), {}, {1: 0.0}),
            parity.ParityRow(date(2014, 12, 1), {}, {1: 0.0}),
            parity.ParityRow(date(2015, 1, 1), {}, {1: 0.0}),
            parity.ParityRow(date(2018, 12, 1), {}, {1: 0.0}),
            parity.ParityRow(date(2019, 1, 1), {}, {1: 0.0}),
            parity.ParityRow(date(2024, 7, 1), {}, {1: 0.0}),
            parity.ParityRow(date(2025, 1, 1), {}, {1: 0.0}),
        ]
        train, test = parity.split_rows(rows)
        self.assertEqual([row.month for row in train], [date(2010, 3, 1), date(2018, 12, 1)])
        self.assertEqual([row.month for row in test], [date(2019, 1, 1), date(2024, 7, 1)])

    def test_no_leakage_on_valid_rows(self):
        rows = [
            parity.ParityRow(date(2018, 12, 1), {"month": 12}, {1: 0.1}),
            parity.ParityRow(date(2019, 1, 1), {"month": 1}, {1: 0.2}),
        ]
        parity.assert_no_leakage(rows)

    def test_duplicate_month_rejected(self):
        rows = [
            parity.ParityRow(date(2019, 1, 1), {"month": 1}, {1: 0.2}),
            parity.ParityRow(date(2019, 1, 1), {"month": 1}, {1: 0.3}),
        ]
        with self.assertRaises(AssertionError):
            parity.assert_no_leakage(rows)

    def test_train_test_overlap_rejected(self):
        rows = [
            parity.ParityRow(date(2018, 12, 1), {"month": 12}, {1: 0.1}),
            parity.ParityRow(date(2019, 1, 1), {"month": 1}, {1: 0.2}),
        ]
        original = parity.TRAIN_END
        try:
            parity.TRAIN_END = date(2019, 6, 1)
            with self.assertRaises(AssertionError):
                parity.assert_no_leakage(rows)
        finally:
            parity.TRAIN_END = original

    def test_target_key_in_features_rejected(self):
        rows = [
            parity.ParityRow(date(2018, 12, 1), {"month": 12, "y": 0.1}, {1: 0.1}),
            parity.ParityRow(date(2019, 1, 1), {"month": 1}, {1: 0.2}),
        ]
        with self.assertRaises(AssertionError):
            parity.assert_no_leakage(rows)

    def test_cutoff_after_label_publication_rejected(self):
        rows = [
            parity.ParityRow(date(2018, 12, 1), {"month": 12}, {1: 0.1}),
            parity.ParityRow(date(2019, 1, 1), {"month": 1}, {1: 0.2}),
        ]
        original = parity.cm.available_from_mes
        try:
            parity.cm.available_from_mes = lambda period_end: parity.cm.cutoff_for(period_end)
            with self.assertRaises(AssertionError):
                parity.assert_no_leakage(rows)
        finally:
            parity.cm.available_from_mes = original


class TestDieboldMariano(unittest.TestCase):
    def test_identical_errors(self):
        errors = [0.1, -0.2, 0.3, -0.1] * 10
        statistic, p_value = eu.diebold_mariano(errors, list(errors), 1)
        self.assertEqual(statistic, 0.0)
        self.assertEqual(p_value, 1.0)

    def test_worse_forecast_has_positive_statistic(self):
        base = [0.1, -0.1, 0.2, -0.2, 0.05, -0.05] * 9
        worse = [value + 0.4 for value in base]
        statistic, p_value = eu.diebold_mariano(worse, base, 1)
        self.assertGreater(statistic, 0.0)
        self.assertLess(p_value, 0.05)

    def test_authors_dm_is_symmetric_and_bounded(self):
        err_a = [0.1, -0.2, 0.3, -0.4, 0.5, -0.6, 0.2, -0.1, 0.4, -0.3]
        err_b = [0.05, 0.1, -0.1, 0.2, -0.3, 0.4, -0.2, 0.3, -0.4, 0.1]
        forward = parity.authors_dm_pvalue(err_a, err_b)
        backward = parity.authors_dm_pvalue(err_b, err_a)
        self.assertAlmostEqual(forward, backward, places=12)
        self.assertGreaterEqual(forward, 0.0)
        self.assertLessEqual(forward, 1.0)
        self.assertEqual(parity.authors_dm_pvalue(err_a, err_a), 1.0)


class TestTost(unittest.TestCase):
    def _errors(self):
        return [0.1, -0.2, 0.3, -0.1, 0.05, -0.15] * 9

    def test_identical_errors_are_equivalent(self):
        errors = self._errors()
        bootstrap = parity.block_bootstrap_delta_ratio(errors, list(errors), margin=0.05)
        result = parity.tost(bootstrap, margin=0.05)
        self.assertAlmostEqual(bootstrap.delta, 0.0, places=12)
        self.assertAlmostEqual(bootstrap.ratio, 1.0, places=12)
        self.assertEqual([bootstrap.delta_low, bootstrap.delta_high], [0.0, 0.0])
        self.assertTrue(result.equivalent)

    def test_doubled_errors_are_not_equivalent(self):
        base = self._errors()
        doubled = [2.0 * value for value in base]
        margin = 0.10 * eu.rmse(base)
        bootstrap = parity.block_bootstrap_delta_ratio(doubled, base, margin=margin)
        result = parity.tost(bootstrap, margin=margin)
        self.assertGreater(bootstrap.delta, margin)
        self.assertGreater(bootstrap.ratio, 1.8)
        self.assertFalse(result.equivalent)

    def test_bootstrap_is_deterministic(self):
        base = self._errors()
        worse = [value + 0.1 for value in base]
        first = parity.block_bootstrap_delta_ratio(worse, base, margin=0.05)
        second = parity.block_bootstrap_delta_ratio(worse, base, margin=0.05)
        self.assertEqual(first, second)

    def test_quantile_matches_eval_utils_convention(self):
        values = list(range(100))
        self.assertEqual(parity._quantile(values, 0.05), 5)
        self.assertEqual(parity._quantile(values, 0.95), 94)


class TestVerdicts(unittest.TestCase):
    def _result(self, horizon, verdict):
        return {"model": parity.MODEL_MP, "horizon": horizon, "verdict": verdict}

    def test_horizon_verdicts(self):
        equivalent = parity.TostResult(0.0, 0.1, -0.01, 0.01, 0.0, 0.0, True)
        not_equivalent = parity.TostResult(0.2, 0.1, 0.1, 0.3, 0.0, 0.0, False)
        self.assertEqual(parity.horizon_verdict(0.5, 0.0, equivalent), "equivalent")
        self.assertEqual(parity.horizon_verdict(0.01, 0.2, not_equivalent), "ours worse")
        self.assertEqual(parity.horizon_verdict(0.01, -0.2, not_equivalent), "ours better")
        self.assertEqual(parity.horizon_verdict(0.5, 0.2, not_equivalent), "inconclusive")

    def test_overall_verdict_requires_all_primary_horizons(self):
        results = [self._result(horizon, "equivalent") for horizon in parity.HORIZONS]
        self.assertEqual(parity.overall_verdict(results, parity.MODEL_MP), "близок")
        results[-1] = self._result(6, "inconclusive")
        self.assertEqual(parity.overall_verdict(results, parity.MODEL_MP), "не близок")

    def test_overall_verdict_requires_all_horizons_present(self):
        results = [self._result(horizon, "equivalent") for horizon in (1, 2, 3)]
        with self.assertRaises(ValueError):
            parity.overall_verdict(results, parity.MODEL_MP)


class TestModelSmoke(unittest.TestCase):
    def test_fit_and_predict_small_sample(self):
        months = _month_range(date(2017, 1, 1), date(2019, 6, 1))
        names = ["y_lag1", "y_lag2", "usd_mm", "key_rate", "fao_mm", "month"]
        rows = []
        for index, month in enumerate(months):
            features = {
                "y_lag1": 0.2 + 0.01 * index,
                "y_lag2": 0.1 + 0.01 * index,
                "usd_mm": 0.5 * math.sin(index / 3),
                "key_rate": 7.5,
                "fao_mm": 0.3 * math.cos(index / 4),
                "month": month.month,
            }
            rows.append(
                parity.ParityRow(
                    month=month,
                    features=features,
                    targets={1: 0.5 + 0.05 * math.sin(index)},
                )
            )
        model = parity.fit_direct(rows, names, 1, iterations=5, thread_count=1)
        predictions = parity.predict_direct(model, rows, names)
        self.assertEqual(len(predictions), len(rows))
        self.assertTrue(all(math.isfinite(value) for value in predictions))


class TestFeatureModeNames(unittest.TestCase):
    def test_mean_keeps_legacy_names(self):
        self.assertEqual(
            parity.bottomup_model_name(parity.bu.MODEL_MP, "mean"),
            parity.bu.MODEL_MP,
        )
        self.assertEqual(
            parity.bottomup_model_name(parity.bu.MODEL_NOMP, "mean"),
            parity.bu.MODEL_NOMP,
        )

    def test_non_mean_gets_suffix(self):
        self.assertEqual(
            parity.bottomup_model_name(parity.bu.MODEL_MP, "per_bsu"),
            "bottomup_mp__per_bsu",
        )
        self.assertEqual(
            parity.bottomup_model_name(parity.bu.MODEL_NOMP, "full"),
            "bottomup_nomp__full",
        )


class TestReportArtifacts(unittest.TestCase):
    def test_predictions_csv_and_json_are_written(self):
        payload = {
            "design": {
                "train_start": "2010-03-01",
                "train_end": "2018-12-01",
                "test_start": "2019-01-01",
                "test_end": "2024-07-01",
                "seed": 20261018,
                "horizons": [1, 2, 3, 6],
                "horizons_extra": [4, 5],
                "margin_fraction": 0.10,
                "bootstrap_block": 3,
                "bootstrap_n": 2000,
                "cb_rmse_pinned": {"1": 0.158805},
            },
            "archive": {
                "path": "/tmp/archive",
                "rmse_cb": {"1": 0.158805},
                "extra_rmse_cb": {},
                "margins": {"1": 0.0158805},
                "max_source_diff": 0.0,
                "archive_dm": {"1": {"computed": 0.032952, "archive": 0.032952}},
            },
            "sample": {
                "n_train": 63,
                "train_actual": "2013-10-01…2018-12-01",
                "n_test": 67,
                "n_normal": 54,
                "n_skipped": 43,
                "skipped_reasons": [["нет доступной ключевой ставки на cutoff (key_rate)", 43]],
                "train_outliers_excluded": [
                    "2014-12-01",
                    "2015-01-01",
                    "2015-02-01",
                    "2015-03-01",
                ],
            },
            "results": [
                {
                    "model": parity.MODEL_MP,
                    "horizon": 1,
                    "n": 54,
                    "rmse_ours": 0.3,
                    "rmse_cb": 0.158805,
                    "mae_ours": 0.2,
                    "mae_cb": 0.1,
                    "delta": 0.14,
                    "ratio": 1.9,
                    "margin": 0.0158805,
                    "delta_ci90": [0.1, 0.2],
                    "ratio_ci90": [1.7, 2.1],
                    "dm_stat_h1": 2.0,
                    "dm_p_h1": 0.01,
                    "dm_stat_h": 2.0,
                    "dm_p_h": 0.01,
                    "tost_equivalent": False,
                    "tost_tail_low": 0.0,
                    "tost_tail_high": 0.9,
                    "verdict": "ours worse",
                }
            ],
            "overall": {parity.MODEL_MP: "не близок", parity.MODEL_NOMP: "не близок"},
        }
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            rows = [
                {
                    "month": "2019-01-01",
                    "horizon": 1,
                    "model": parity.MODEL_MP,
                    "sample": "test",
                    "normal": 1,
                    "y_true": 0.2,
                    "y_pred": 0.3,
                    "error": 0.1,
                }
            ]
            parity.write_predictions_csv(out / parity.ARTIFACT_PREDICTIONS, rows)
            parity.write_result_json(out / parity.ARTIFACT_RESULT, payload)
            self.assertIn("month,horizon,model", (out / parity.ARTIFACT_PREDICTIONS).read_text())
            self.assertIn("не близок", (out / parity.ARTIFACT_RESULT).read_text())


if __name__ == "__main__":
    unittest.main()
