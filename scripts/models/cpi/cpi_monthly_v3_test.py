#!/usr/bin/env python3
"""Офлайн-тесты `scripts/models/cpi/cpi_monthly_v3.py` (issue #76, Stage 1).

Запуск из корня репозитория:

    uv run --with "catboost>=1.2" --with scikit-learn --with pandas \
      --with openpyxl --with "statsmodels>=0.15" --with scipy \
      --python 3.12 python3 scripts/models/cpi/cpi_monthly_v3_test.py -v

Тесты не требуют ClickHouse и сети: недельная панель и строки выборки
собираются синтетически. Проверяются as-of недель, формула diffusion-признаков,
NaN-политика, порядок признаков и smoke CatBoost/ElasticNet.
"""

import math
import sys
import unittest
from datetime import date, timedelta
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from models.cpi import cpi_monthly as cm
from models.cpi import cpi_monthly_v3 as v3


def _panel(rows: list) -> tuple:
    """`[(date, series, value)]` → `(sorted_dates, by_date)`."""
    by_date = {}
    for day, series, value in rows:
        by_date.setdefault(day, {})[series] = value
    return sorted(by_date), by_date


def _weeks(start: date, n: int, values: list) -> list:
    return [
        (start + timedelta(days=7 * index), v3.WEEKLY_GASOLINE, value)
        for index, value in enumerate(values[:n])
    ]


def _shift_month(day: date, months: int) -> date:
    index = day.year * 12 + (day.month - 1) + months
    return date(index // 12, index % 12 + 1, 1)


class TestWeeklyFeatures(unittest.TestCase):
    def test_asof_excludes_unpublished_week(self):
        start = date(2024, 3, 11)
        rows = _weeks(start, 5, [100.0, 101.0, 102.0, 103.0, 110.0])
        panel = _panel(rows)
        # cutoff 2024-04-06: неделя 2024-04-08 (05-я) недоступна (date+2 > cutoff).
        features = v3.weekly_features(panel, date(2024, 4, 6))
        self.assertEqual(features["wk_median"], 3.0)
        # cutoff 2024-04-10: 05-я неделя доступна.
        features = v3.weekly_features(panel, date(2024, 4, 10))
        self.assertEqual(features["wk_median"], 10.0)

    def test_formula(self):
        rows = []
        values = {
            date(2024, 3, 4): {"a": 100.0, "b": 100.0, "c": 100.0},
            date(2024, 3, 11): {"a": 100.0, "b": 100.0, "c": 100.0},
            date(2024, 3, 18): {"a": 100.0, "b": 100.0, "c": 100.0},
            date(2024, 3, 25): {"a": 100.0, "b": 102.0, "c": 98.0},
        }
        for day, positions in values.items():
            for series, value in positions.items():
                rows.append((day, series, value))
        panel = _panel(rows)
        features = v3.weekly_features(panel, date(2024, 4, 8))
        self.assertAlmostEqual(features["wk_share_rising"], 1 / 3, places=12)
        self.assertAlmostEqual(features["wk_median"], 0.0, places=12)
        self.assertAlmostEqual(features["wk_median4"], 0.0, places=12)
        self.assertTrue(math.isnan(features["wk_gasoline4"]))

    def test_gasoline(self):
        panel = _panel(_weeks(date(2024, 3, 4), 4, [100.0, 101.0, 102.0, 103.0]))
        features = v3.weekly_features(panel, date(2024, 4, 8))
        # Бензин: 0 + 1 + 2 + 3 = 6.
        self.assertAlmostEqual(features["wk_gasoline4"], 6.0, places=12)

    def test_empty_and_missing_gasoline(self):
        empty = v3.weekly_features(_panel([]), date(2024, 4, 8))
        self.assertTrue(all(math.isnan(empty[name]) for name in v3.WEEKLY_FEATURES))

        panel = _panel([(date(2024, 3, 25), "a", 100.0)])
        features = v3.weekly_features(panel, date(2024, 4, 8))
        self.assertAlmostEqual(features["wk_share_rising"], 0.0, places=12)
        self.assertTrue(math.isnan(features["wk_gasoline4"]))

    def test_feature_names_order(self):
        names = v3.feature_names_v3()
        self.assertEqual(names[-1], "month")
        for name in v3.WEEKLY_FEATURES:
            self.assertIn(name, names)
            self.assertLess(names.index(name), names.index("month"))
        self.assertEqual(len(names), len(cm.feature_names(with_mp=True)) + 4)


class TestModels(unittest.TestCase):
    def _rows(self) -> list:
        rows = []
        for index in range(30):
            month = _shift_month(date(2020, 1, 1), index)
            features = {
                name: 0.1 + 0.01 * index
                for name in v3.feature_names_v3()
                if name != "month"
            }
            features["month"] = month.month
            targets = {
                code: 0.2 + 0.01 * index for code in cm.TARGET_SERIES
            }
            rows.append(cm.Row(month=month, y=targets, features=features))
        return rows

    def test_catboost_smoke(self):
        rows = self._rows()
        value = v3.fit_catboost(
            rows[:-1], rows[-1], cm.TARGET_ALL, True, iterations=5, thread_count=1
        )
        self.assertTrue(math.isfinite(value))

    def test_enet_smoke(self):
        rows = self._rows()
        value = v3.fit_enet_v3(rows[:-1], rows[-1], True)
        self.assertTrue(math.isfinite(value))


if __name__ == "__main__":
    unittest.main()
