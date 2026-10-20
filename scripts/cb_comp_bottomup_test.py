#!/usr/bin/env python3
"""Офлайн-тесты `scripts/cb_comp_bottomup.py` (issue #190, спека §4.1).

Запуск из корня репозитория:

    make cb-comp-parity-test

Тесты не требуют R, ClickHouse и распакованного архива: `model.xlsx`,
`dictionary_cpi.xlsx` и `cpi_weights_tovar.xlsx` собираются во временном
каталоге openpyxl. Проверяются групповой таргет, веса с лагом год, ЖКУ вне
компонент, агрегат с нормировкой, as-of МП и отсутствие утечки.
"""

import math
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from openpyxl import Workbook

sys.path.insert(0, str(Path(__file__).resolve().parent))

import cb_comp_bottomup as bu


def _write_book(path: Path, sheets: dict, startrow: int = 0) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, content in sheets.items():
            content.to_excel(writer, sheet_name=name, index=False, startrow=startrow)


def _make_archive(root: Path) -> Path:
    """Синтетический архив: 2 компоненты (10 с BSU, 11 без) + агрегат 1 + ЖКУ."""
    archive = root / bu.ARCHIVE_ROOT_NAME if hasattr(bu, "ARCHIVE_ROOT_NAME") else root / "BusinessSurveyCBRCpiForecastingSourceCode"
    model = pd.DataFrame(
        [
            {
                "наименование_товара_услуги": "Все товары и услуги",
                "код_товара_услуги": "1",
                "вес_для_проверки": 1.0,
                "используется": True,
                "тип_группы": "товары и услуги",
                "мониторинг_код1": "<==>",
                "мониторинг_код2": "<==>",
                "мониторинг_код3": "<==>",
                "мониторинг_код4": "<==>",
            },
            {
                "наименование_товара_услуги": "Мясопродукты",
                "код_товара_услуги": "10",
                "вес_для_проверки": 0.6,
                "используется": True,
                "тип_группы": "товары",
                "мониторинг_код1": "10.1",
                "мониторинг_код2": "<==>",
                "мониторинг_код3": "<==>",
                "мониторинг_код4": "<==>",
            },
            {
                "наименование_товара_услуги": "Рыбопродукты",
                "код_товара_услуги": "11",
                "вес_для_проверки": 0.2,
                "используется": True,
                "тип_группы": "товары",
                "мониторинг_код1": "<==>",
                "мониторинг_код2": "<==>",
                "мониторинг_код3": "<==>",
                "мониторинг_код4": "<==>",
            },
        ]
    )
    _write_book(archive / "Models/CpiComponents/data/model/model.xlsx", {"1.0.4.2": model}, startrow=4)

    def dictionary_row(code: str, level: int, levels: dict) -> dict:
        row = {"код_товара_услуги": code, "уровень": level}
        for index in range(1, 8):
            row[f"уровень_{index}"] = levels.get(index, code)
        return row

    dictionary = pd.DataFrame(
        [
            dictionary_row("10", 3, {1: "1", 2: "6", 3: "10"}),
            dictionary_row("11", 3, {1: "1", 2: "6", 3: "11"}),
            dictionary_row("9470", 4, {1: "1", 2: "9000", 3: "940.АГ", 4: "9470"}),
            dictionary_row("101", 7, {1: "1", 2: "6", 3: "10", 4: "101"}),
            dictionary_row("111", 7, {1: "1", 2: "6", 3: "11", 4: "111"}),
            dictionary_row("94701", 7, {1: "1", 2: "9000", 3: "940.АГ", 4: "9470"}),
        ]
    )
    _write_book(
        archive / "Models/CpiComponents/StfCpi_External_data/dictionaries/dictionary_cpi.xlsx",
        {"2024": dictionary, "2023": dictionary},
    )
    weights = pd.DataFrame(
        [
            {"наименование_товара_услуги": "Мясо", "вес_товара": 0.6, "год": 2023, "регион": "Россия", "код_товара_услуги": "101", "окато_код": "643"},
            {"наименование_товара_услуги": "Рыба", "вес_товара": 0.2, "год": 2023, "регион": "Россия", "код_товара_услуги": "111", "окато_код": "643"},
            {"наименование_товара_услуги": "ЖКУ", "вес_товара": 0.1, "год": 2023, "регион": "Россия", "код_товара_услуги": "94701", "окато_код": "643"},
        ]
    )
    _write_book(
        archive / "Models/CpiComponents/StfCpi_External_data/databases/weights/cpi_weights_tovar.xlsx",
        {"2023": weights},
    )
    utilities = pd.DataFrame(
        [
            {"дата": "2023-01-01", "Россия": 0.5, "факт_прогноз": "факт"},
            {"дата": "2023-02-01", "Россия": 0.6, "факт_прогноз": "факт"},
        ]
    )
    _write_book(archive / "Models/CpiComponents/data/Коммунальные услуги прогноз MoM_sa.xlsx", {"Прогноз": utilities})
    return archive


class TestCodesAndCalendar(unittest.TestCase):
    def test_normalize_code(self):
        self.assertEqual(bu.normalize_code("803.0"), "803")
        self.assertEqual(bu.normalize_code("9300->930.АГ"), "930.АГ")
        self.assertEqual(bu.normalize_code("10.1"), "10.1")
        self.assertEqual(bu.normalize_code(None), "")

    def test_month_helpers(self):
        self.assertEqual(bu.shift_month(date(2024, 12, 1), 1), date(2025, 1, 1))
        self.assertEqual(bu.month_end(date(2024, 2, 1)), date(2024, 2, 29))
        self.assertEqual(bu.mp_available(date(2024, 1, 1), 1), date(2024, 2, 29))
        self.assertEqual(bu.mp_available(date(2024, 1, 1), 2), date(2024, 3, 31))


class TestSpecsAndWeights(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.archive = _make_archive(Path(self.tmp.name))

    def tearDown(self):
        self.tmp.cleanup()

    def test_load_specs(self):
        specs = bu.load_specs(self.archive)
        self.assertEqual([spec.code for spec in specs], ["10", "11"])
        by_code = {spec.code: spec for spec in specs}
        self.assertEqual(by_code["10"].bsu_codes, ("10.1",))
        self.assertEqual(by_code["11"].bsu_codes, ())
        self.assertEqual(by_code["10"].level, 3)

    def test_year_weights_and_utilities(self):
        specs = bu.load_specs(self.archive)
        weights = bu._year_weights(self.archive, 2023, specs)
        self.assertAlmostEqual(weights["10"], 0.6, places=12)
        self.assertAlmostEqual(weights["11"], 0.2, places=12)
        self.assertAlmostEqual(weights["9470"], 0.1, places=12)

    def test_weight_for_month_uses_previous_year(self):
        weights = {2022: {"10": 0.5}, 2023: {"10": 0.6}}
        self.assertEqual(bu.weight_for_month(weights, date(2024, 3, 1)), {"10": 0.6})
        self.assertEqual(bu.weight_for_month(weights, date(2019, 3, 1)), {"10": 0.5})


class TestComponentTargets(unittest.TestCase):
    def test_cumulative_target_formula(self):
        index = [date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)]
        diff = pd.DataFrame({"10": [0.01, 0.02, 0.03]}, index=index)
        targets = bu.component_targets(diff, horizons=(1, 2))
        self.assertAlmostEqual(targets[1].at[index[2], "10"], (math.exp(0.03) - 1) * 100, places=10)
        self.assertAlmostEqual(targets[2].at[index[2], "10"], (math.exp(0.05) - 1) * 100, places=10)
        self.assertTrue(math.isnan(targets[2].at[index[0], "10"]))

    def test_base_features_use_only_lags(self):
        index = [date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)]
        diff = pd.DataFrame({"10": [0.01, 0.02, 0.03]}, index=index)
        spec = bu.GroupSpec("10", (), 3)
        macro = {date(2024, 3, 1): {"usd_mm": 1.0, "key_rate": 7.5, "fao_mm": 0.5}}
        features, reason = bu.base_features(spec, date(2024, 3, 1), diff, macro)
        self.assertEqual(reason, "")
        self.assertAlmostEqual(features["y_lag1"], 2.0, places=10)
        self.assertAlmostEqual(features["y_lag2"], 1.0, places=10)
        self.assertEqual(features["key_rate"], 7.5)
        # Значение самого тестового месяца в признаки не попадает.
        diff.at[date(2024, 3, 1), "10"] = 9.0
        features_after, _ = bu.base_features(spec, date(2024, 3, 1), diff, macro)
        self.assertEqual(features["y_lag1"], features_after["y_lag1"])
        self.assertEqual(features["y_lag2"], features_after["y_lag2"])

    def test_model_rows_use_origin(self):
        features = {
            date(2024, 1, 1): {"y_lag1": 1.0},
            date(2024, 3, 1): {"y_lag1": 3.0},
        }
        target = pd.DataFrame({"10": [5.0]}, index=[date(2024, 3, 1)])
        rows = bu.model_rows("10", features, target, 3, [date(2024, 3, 1)])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], date(2024, 3, 1))
        self.assertEqual(rows[0][1]["y_lag1"], 1.0)
        self.assertAlmostEqual(rows[0][2][3], 5.0, places=12)

    def test_leakage_origin_check(self):
        bu.assert_no_leakage_bottomup([date(2024, 3, 1)], 3)

    def test_base_features_missing_lag(self):
        diff = pd.DataFrame({"10": [0.01]}, index=[date(2024, 3, 1)])
        spec = bu.GroupSpec("10", (), 3)
        features, reason = bu.base_features(spec, date(2024, 3, 1), diff, {})
        self.assertIsNone(features)
        self.assertIn("лаг", reason)


class TestMpFeatures(unittest.TestCase):
    def _mp_panel(self, lag: int) -> pd.DataFrame:
        rows = []
        for month, value in ((1, 10.0), (2, 12.0), (3, 14.0)):
            rows.append(
                {
                    "date": date(2024, month, 1),
                    "code": "10.1",
                    "kind": "Indikator biznes-klimata Banka Rossii (IBK)",
                    "value": value,
                    "lag": lag,
                }
            )
        frame = pd.DataFrame(rows)
        frame["indicator"] = frame["kind"].map(bu.MP_INDICATORS)
        return frame

    def test_asof_with_publication_lag(self):
        spec = bu.GroupSpec("10", ("10.1",), 3)
        panel = self._mp_panel(1)
        lookup = bu._mp_lookup(panel)
        features = bu.mp_features(spec, date(2024, 3, 1), lookup)
        # cutoff(2024-03) = 2024-04-08; точка марта опубликована 2024-04-30 —
        # не участвует; последняя доступная — февраль, предыдущая — январь.
        self.assertAlmostEqual(features["mp_ibk"], 12.0, places=10)
        self.assertAlmostEqual(features["mp_ibk_chg"], 2.0, places=10)
        self.assertTrue(math.isnan(features["mp_costs"]))

    def test_lag_two_takes_previous_month(self):
        spec = bu.GroupSpec("10", ("10.1",), 3)
        panel = self._mp_panel(2)
        lookup = bu._mp_lookup(panel)
        features = bu.mp_features(spec, date(2024, 3, 1), lookup)
        # cutoff 2024-04-08: январь доступен 2024-03-31, февраль — 2024-04-30.
        self.assertAlmostEqual(features["mp_ibk"], 10.0, places=10)
        self.assertTrue(math.isnan(features["mp_ibk_chg"]))

    def test_group_without_bsu_has_no_mp_features(self):
        spec = bu.GroupSpec("11", (), 3)
        self.assertEqual(bu.mp_features(spec, date(2024, 3, 1), {}), {})
        self.assertEqual(bu.feature_names(False)[-1], "month")
        self.assertEqual(bu.feature_names(True), list(bu.BASE_FEATURES)[:-1] + list(bu.MP_FEATURES) + ["month"])

    def test_per_bsu_names_and_values(self):
        rows = []
        for code, base in (("10.1", 10.0), ("10.2", 20.0)):
            for month, value in ((1, base), (2, base + 2), (3, base + 4)):
                rows.append(
                    {
                        "date": date(2024, month, 1),
                        "code": code,
                        "kind": "Indikator biznes-klimata Banka Rossii (IBK)",
                        "value": value,
                        "lag": 1,
                    }
                )
        frame = pd.DataFrame(rows)
        frame["indicator"] = frame["kind"].map(bu.MP_INDICATORS)
        spec = bu.GroupSpec("10", ("10.1", "10.2"), 3)
        lookup = bu._mp_lookup(frame)

        features = bu.mp_features(spec, date(2024, 3, 1), lookup, mode="per_bsu")
        self.assertEqual(len(features), 12)
        self.assertAlmostEqual(features["mp_10_1_ibk"], 12.0, places=10)
        self.assertAlmostEqual(features["mp_10_1_ibk_chg"], 2.0, places=10)
        self.assertAlmostEqual(features["mp_10_2_ibk"], 22.0, places=10)
        self.assertAlmostEqual(features["mp_10_2_ibk_chg"], 2.0, places=10)
        self.assertTrue(math.isnan(features["mp_10_1_costs"]))

        names = bu.feature_names(True, "per_bsu", spec)
        self.assertEqual(len(names), 18)
        self.assertEqual(names[-1], "month")
        self.assertTrue(set(names) <= set(features) | set(bu.BASE_FEATURES))

        mean = bu.mp_features(spec, date(2024, 3, 1), lookup, mode="mean")
        self.assertEqual(len(mean), 6)
        self.assertAlmostEqual(mean["mp_ibk"], 17.0, places=10)

    def test_load_sheet_map_and_public_panel(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            sheet_path = root / "sheet_map.csv"
            pd.DataFrame(
                [
                    {"code": "01.11", "industry_slug": "agriculture"},
                    {"code": "10.1", "industry_slug": "manufacturing_consumer"},
                    {"code": "47.2", "industry_slug": "trade_retail"},
                ]
            ).to_csv(sheet_path, index=False)
            self.assertEqual(
                bu.load_sheet_map(sheet_path),
                {
                    "01.11": "agriculture",
                    "10.1": "manufacturing_consumer",
                    "47.2": "trade_retail",
                },
            )

            panel_path = root / "public_panel.csv"
            pd.DataFrame(
                [
                    {"code": "01.11", "indicator": "ibk", "date": "2024-01-01", "value": 7.0},
                    {"code": "10.1", "indicator": "ibk", "date": "2024-01-01", "value": 10.0},
                    {"code": "10.1", "indicator": "ibk", "date": "2024-02-01", "value": 12.0},
                ]
            ).to_csv(panel_path, index=False)
            panel = bu.load_public_panel(panel_path)
            # `01.11` обязан остаться строкой с ведущим нулём, а не 1.11.
            self.assertEqual(set(panel["code"]), {"01.11", "10.1"})
            self.assertEqual(
                list(panel["date"]),
                [date(2024, 1, 1), date(2024, 1, 1), date(2024, 2, 1)],
            )
            self.assertEqual(panel["value"].tolist(), [7.0, 10.0, 12.0])

            pd.DataFrame(
                [{"code": "10.1", "indicator": "bogus", "date": "2024-01-01", "value": 1.0}]
            ).to_csv(panel_path, index=False)
            with self.assertRaises(ValueError):
                bu.load_public_panel(panel_path)

    def test_per_sheet_names_and_values(self):
        rows = []
        for code, sheet, base in (
            ("10.1", "manufacturing_consumer", 10.0),
            ("10.2", "manufacturing_consumer", 10.0),
            ("47.2", "trade_retail", 20.0),
        ):
            for month, offset in ((1, 0.0), (2, 2.0), (3, 4.0)):
                rows.append(
                    {
                        "code": code,
                        "indicator": "ibk",
                        "date": date(2024, month, 1),
                        "value": base + offset,
                    }
                )
        frame = pd.DataFrame(rows)
        sheet_map = {
            "10.1": "manufacturing_consumer",
            "10.2": "manufacturing_consumer",
            "47.2": "trade_retail",
        }
        lookup = bu._sheet_lookup(frame, sheet_map)
        spec = bu.GroupSpec(
            "10",
            ("10.1", "10.2"),
            3,
            ("manufacturing_consumer", "trade_retail"),
        )
        features = bu.mp_features(
            spec, date(2024, 3, 1), lookup, mode="per_sheet"
        )
        self.assertEqual(len(features), 12)
        self.assertAlmostEqual(
            features["mp_sheet_manufacturing_consumer_ibk"], 12.0, places=10
        )
        self.assertAlmostEqual(
            features["mp_sheet_manufacturing_consumer_ibk_chg"], 2.0, places=10
        )
        self.assertAlmostEqual(
            features["mp_sheet_trade_retail_ibk"], 22.0, places=10
        )
        self.assertTrue(
            math.isnan(features["mp_sheet_manufacturing_consumer_costs"])
        )

        names = bu.feature_names(True, "per_sheet", spec)
        self.assertEqual(len(names), 18)
        self.assertEqual(names[-1], "month")
        self.assertTrue(set(names) <= set(features) | set(bu.BASE_FEATURES))

    def test_sheet_lookup_rejects_inconsistent_sheet(self):
        frame = pd.DataFrame(
            [
                {"code": "10.1", "indicator": "ibk", "date": date(2024, 1, 1), "value": 1.0},
                {"code": "10.2", "indicator": "ibk", "date": date(2024, 1, 1), "value": 2.0},
            ]
        )
        with self.assertRaises(ValueError):
            bu._sheet_lookup(
                frame,
                {
                    "10.1": "manufacturing_consumer",
                    "10.2": "manufacturing_consumer",
                },
            )

    def test_per_sheet_requires_public_panel(self):
        result = bu.run_bottomup(
            archive=Path("/nonexistent"),
            target_full=pd.DataFrame(),
            macro={},
            normal_months=[],
            train_months=[],
            test_months=[],
            feature_mode="per_sheet",
        )
        self.assertEqual(result.status, "not_assembled")
        self.assertIn("public_panel", result.reason)

    def test_full_features_and_names(self):
        panel = pd.DataFrame(
            [
                {"date": date(2024, 2, 1), "group_stat": "Индекс потребительских цен", "code": "10", "kind": "trend", "value": 1.5, "lag": 0},
                {"date": date(2024, 2, 1), "group_stat": "Индекс потребительских цен", "code": "10", "kind": "trend_deviation", "value": 0.2, "lag": 0},
                {"date": date(2024, 2, 1), "group_stat": "Рыночные индексы", "code": "usd_rub", "kind": "base_index", "value": 90.0, "lag": 0},
                {"date": date(2024, 2, 1), "group_stat": "Рыночные индексы", "code": "usd_rub", "kind": "diff", "value": 0.5, "lag": 0},
                {"date": date(2024, 2, 1), "group_stat": "Кредитные ставки", "code": "miacr", "kind": "interest_rate", "value": 7.0, "lag": 0},
                {"date": date(2024, 2, 1), "group_stat": "Кредитные ставки", "code": "miacr", "kind": "diff", "value": -0.1, "lag": 0},
                {"date": date(2024, 1, 1), "group_stat": "Кредит населению", "code": "debt_households_wh_mortgage", "kind": "base_index", "value": 100.0, "lag": 2},
            ]
        )
        spec = bu.GroupSpec("10", ("10.1",), 3)
        lookup = bu._full_lookup(panel)
        features = bu.full_features(spec, date(2024, 3, 1), lookup)
        self.assertAlmostEqual(features["cpi_trend"], 1.5, places=10)
        self.assertAlmostEqual(features["cpi_trend_dev"], 0.2, places=10)
        self.assertAlmostEqual(features["usd_level"], 90.0, places=10)
        self.assertAlmostEqual(features["miacr_chg"], -0.1, places=10)
        self.assertAlmostEqual(features["debt_mortgage"], 100.0, places=10)
        self.assertEqual(set(features), set(bu.FULL_FEATURES))

        names = bu.feature_names(True, "full", spec)
        self.assertEqual(len(names), 19)
        self.assertEqual(names[-1], "month")
        self.assertIn("cpi_trend", names)
        self.assertIn("mp_10_1_ibk", names)

    def test_feature_table_full_mode(self):
        index = [date(2024, 1, 1), date(2024, 2, 1)]
        cpi_diff = pd.DataFrame({"10": [0.01, 0.02]}, index=index)
        macro = {date(2024, 3, 1): {"usd_mm": 1.0, "key_rate": 7.5, "fao_mm": 0.5}}
        spec = bu.GroupSpec("10", ("10.1",), 3)
        mp_panel = pd.DataFrame(
            [
                {
                    "date": date(2024, 2, 1),
                    "code": "10.1",
                    "kind": "Indikator biznes-klimata Banka Rossii (IBK)",
                    "value": 12.0,
                    "lag": 1,
                    "indicator": "ibk",
                }
            ]
        )
        full_panel = pd.DataFrame(
            [
                {"date": date(2024, 2, 1), "group_stat": "Индекс потребительских цен", "code": "10", "kind": "trend", "value": 1.5, "lag": 0},
            ]
        )
        table = bu.feature_table(
            [spec],
            [date(2024, 3, 1)],
            cpi_diff,
            macro,
            bu._mp_lookup(mp_panel),
            feature_mode="full",
            full_lookup=bu._full_lookup(full_panel),
        )
        features = table["10"][date(2024, 3, 1)]
        self.assertIn("cpi_trend", features)
        self.assertIn("mp_10_1_ibk", features)
        self.assertEqual(features["month"], 3)

    def test_unknown_feature_mode_raises(self):
        with self.assertRaises(ValueError):
            bu.validate_feature_mode("unknown")


class TestAggregateAndGate(unittest.TestCase):
    def test_aggregate_normalizes_weights(self):
        predictions = {"10": {date(2024, 3, 1): 1.0}, "11": {date(2024, 3, 1): 2.0}}
        weights = {2023: {"10": 0.6, "11": 0.2}}
        aggregated, coverage = bu.aggregate(predictions, weights, [date(2024, 3, 1)])
        self.assertAlmostEqual(aggregated[date(2024, 3, 1)], 1.25, places=10)
        self.assertAlmostEqual(coverage[date(2024, 3, 1)], 0.8, places=10)

    def test_aggregate_skips_missing_weight(self):
        predictions = {"10": {date(2024, 3, 1): 1.0}, "11": {date(2024, 3, 1): 2.0}}
        weights = {2023: {"10": 0.6}}
        aggregated, coverage = bu.aggregate(predictions, weights, [date(2024, 3, 1)])
        self.assertAlmostEqual(aggregated[date(2024, 3, 1)], 1.0, places=10)
        self.assertAlmostEqual(coverage[date(2024, 3, 1)], 0.6, places=10)

    def test_reconstruction_matches_aggregate(self):
        index = [date(2024, 3, 1)]
        targets = {1: pd.DataFrame({"10": [1.0], "11": [2.0]}, index=index)}
        target_full = pd.DataFrame({1: [1.25]}, index=index)
        weights = {2023: {"10": 0.6, "11": 0.2}}
        metrics = bu.reconstruction(targets, target_full, weights, index)
        self.assertAlmostEqual(metrics[1]["rmse"], 0.0, places=10)
        self.assertAlmostEqual(metrics[1]["coverage_mean"], 0.8, places=10)

    def test_assembly_gate(self):
        ok, reason = bu.assembly_gate({1: {"rmse": 0.05}}, 0.9)
        self.assertTrue(ok)
        self.assertEqual(reason, "")
        ok, reason = bu.assembly_gate({1: {"rmse": 0.2}}, 0.9)
        self.assertFalse(ok)
        self.assertIn("RMSE h1", reason)
        ok, reason = bu.assembly_gate({1: {"rmse": 0.05}}, 0.5)
        self.assertFalse(ok)
        self.assertIn("покрытие", reason)


class TestModelSmoke(unittest.TestCase):
    def test_fit_and_predict_component(self):
        rows = []
        for index in range(30):
            month = bu.shift_month(date(2017, 1, 1), index)
            features = {
                "y_lag1": 0.2 + 0.01 * index,
                "y_lag2": 0.1 + 0.01 * index,
                "usd_mm": 0.5,
                "key_rate": 7.5,
                "fao_mm": 0.3,
                "month": month.month,
            }
            rows.append((month, features, {1: 0.5 + 0.05 * math.sin(index)}))
        model = bu.fit_component(rows, bu.feature_names(False), 1, iterations=5, thread_count=1)
        predictions = bu.predict_component(model, rows, bu.feature_names(False))
        self.assertEqual(len(predictions), len(rows))
        self.assertTrue(all(math.isfinite(value) for value in predictions))


class TestPanelReaders(unittest.TestCase):
    def test_load_cpi_diff(self):
        cpi = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-01-01", "2024-02-01"]),
                "code": ["10", "10", "1", "1"],
                "kind": ["diff", "diff", "diff", "diff"],
                "value": [0.01, 0.02, 0.5, 0.6],
            }
        )
        diff = bu.load_cpi_diff(cpi)
        self.assertEqual(list(diff.columns), ["1", "10"])
        self.assertAlmostEqual(diff.at[date(2024, 2, 1), "10"], 0.02, places=12)

    def test_load_mp_panel(self):
        mp = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01", "2024-02-01"]),
                "code": ["10.1", "10.1"],
                "kind": ["Indikator biznes-klimata Banka Rossii (IBK)", "чужой"],
                "value": [10.0, 99.0],
                "lag": [1, 1],
            }
        )
        panel = bu.load_mp_panel(mp)
        self.assertEqual(len(panel), 1)
        self.assertEqual(panel.iloc[0]["indicator"], "ibk")


if __name__ == "__main__":
    unittest.main()
