#!/usr/bin/env python3
"""Офлайн-тесты `core_nnls` и моделей v4c/v4d (#396 Task 3).

Проверяются: полнота маппинга групп на корзины, взвешенное среднее корзины,
NNLS (восстановление весов, неотрицательность, отсутствие нормировки),
as-of признак `core_nnls` (утечка по месяцам, NaN при короткой истории,
правило колонок), прикрепление к строкам и два смоук-теста моделей на
синтетике. ClickHouse и сеть не используются.

Запуск — `make cpi-core-nnls-test`.
"""

import math
import sys
import unittest
from datetime import date
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from models.cpi import cpi_monthly as cm
from cpi_core_nnls import (
    BUCKETS,
    BUCKET_NAMES,
    MIN_BUCKETS,
    MIN_TRAIN_MONTHS,
    attach_core_nnls,
    bucket_of,
    bucket_series,
    core_nnls_asof,
    feature_names_v4c,
    feature_names_v4d,
    fit_nnls,
    make_v4c,
    make_v4d,
)
import weekly_core_components as wcc

# 29 групп панели #364 после предрегистрированных исключений (ровно как в CSV).
PANEL_GROUPS = (
    "Мясопродукты", "Молоко и молочная продукция", "Макаронные и крупяные изделия",
    "Медицинские товары", "Масло и жиры", "Моющие и чистящие средства",
    "Трикотажные изделия", "Бумажно-беловые товары", "Хлеб и хлебобулочные изделия",
    "Бытовые услуги", "Чулочно-носочные изделия", "Рыбопродукты", "Одежда и белье",
    "Парфюмерно-косметические товары", "Мука", "Кондитерские изделия",
    "Обувь кожаная, текстильная и комбинированная", "Строительные материалы",
    "Чай, кофе, какао", "Консервы овощные", "Консервы фруктово-ягодные",
    "Общественное питание", "Галантерея", "Электротовары и другие бытовые приборы",
    "Соль, соус, специи, концентраты", "Товары для животных", "Телерадиотовары",
    "Спички", "Сыр",
)


def months(start: date, count: int) -> list:
    out = []
    year, month = start.year, start.month
    for _ in range(count):
        out.append(date(year, month, 1))
        month += 1
        if month == 13:
            year, month = year + 1, 1
    return out


def synthetic(weights: dict, count: int = 40, seed: int = 7) -> tuple:
    """`(buckets_by_month, core_mm)`: core = w·x без шума, корзины — случайные."""
    rng = np.random.default_rng(seed)
    grid = months(date(2021, 1, 1), count)
    vectors = {}
    core = {}
    for month in grid:
        vector = {name: float(rng.normal(0.4, 0.5)) for name in BUCKET_NAMES}
        vectors[month] = vector
        core[month] = sum(weights.get(name, 0.0) * vector[name] for name in BUCKET_NAMES)
    return vectors, core


class BucketsTest(unittest.TestCase):
    def test_every_panel_group_maps(self):
        for group in PANEL_GROUPS:
            self.assertIn(bucket_of(group), BUCKET_NAMES, group)

    def test_mapping_is_exactly_the_panel_groups(self):
        self.assertEqual(set(BUCKETS), set(PANEL_GROUPS))
        self.assertEqual(len(BUCKET_NAMES), 14)
        self.assertEqual(set(BUCKETS.values()), set(BUCKET_NAMES))

    def test_excluded_groups_are_not_mapped(self):
        for group in wcc.EXCLUSIONS:
            self.assertNotIn(group, BUCKETS)

    def test_unknown_group_raises(self):
        with self.assertRaises(ValueError):
            bucket_of("Неизвестная группа")


class BucketSeriesTest(unittest.TestCase):
    def row(self, month, group, weight, mm):
        return wcc.PanelRow(month=month, series="s", component="c", group_name=group, weight=weight, mm_pp=mm)

    def test_weighted_mean_within_bucket(self):
        rows = [
            self.row("2024-03", "Молоко и молочная продукция", 1.0, 1.0),
            self.row("2024-03", "Сыр", 3.0, 2.0),
            self.row("2024-03", "Мясопродукты", 2.0, 0.5),
        ]
        out = bucket_series(rows)
        self.assertAlmostEqual(out[date(2024, 3, 1)]["dairy_fats"], (1.0 * 1.0 + 3.0 * 2.0) / 4.0)
        self.assertAlmostEqual(out[date(2024, 3, 1)]["meat"], 0.5)
        self.assertNotIn("fish", out[date(2024, 3, 1)])

    def test_unmapped_row_raises(self):
        with self.assertRaises(ValueError):
            bucket_series([self.row("2024-03", "Яйца", 1.0, 1.0)])

    def test_months_sorted_by_date(self):
        rows = [self.row("2024-04", "Сыр", 1.0, 1.0), self.row("2024-03", "Сыр", 1.0, 1.0)]
        self.assertEqual(list(bucket_series(rows)), [date(2024, 3, 1), date(2024, 4, 1)])


class FitNnlsTest(unittest.TestCase):
    def test_recovers_known_weights(self):
        rng = np.random.default_rng(1)
        x = rng.normal(size=(60, 4))
        truth = np.array([0.5, 0.0, 0.25, 1.5])
        weights = fit_nnls(x, x @ truth)
        np.testing.assert_allclose(weights, truth, atol=1e-8)

    def test_weights_non_negative_even_for_negative_truth(self):
        rng = np.random.default_rng(2)
        x = rng.normal(size=(60, 3))
        weights = fit_nnls(x, x @ np.array([1.0, -2.0, 0.5]))
        self.assertTrue((weights >= 0).all())

    def test_no_normalisation_to_unit_sum(self):
        rng = np.random.default_rng(3)
        x = rng.normal(size=(50, 3))
        weights = fit_nnls(x, x @ np.array([0.2, 0.2, 0.2]))
        self.assertAlmostEqual(float(weights.sum()), 0.6, places=8)


class CoreNnlsAsOfTest(unittest.TestCase):
    TRUTH = {"meat": 0.3, "services": 0.5, "apparel": 0.2}

    def setUp(self):
        self.vectors, self.core = synthetic(self.TRUTH, count=40)
        self.months = sorted(self.vectors)

    def test_prediction_matches_known_weights(self):
        origin = self.months[30]
        value = core_nnls_asof(self.vectors, self.core, origin)
        expected = sum(self.TRUTH[name] * self.vectors[origin][name] for name in self.TRUTH)
        self.assertAlmostEqual(value, expected, places=6)

    def test_nan_when_history_shorter_than_min_train(self):
        origin = self.months[MIN_TRAIN_MONTHS - 1]  # обучающих месяцев m ≤ O−1: 23
        self.assertTrue(math.isnan(core_nnls_asof(self.vectors, self.core, origin)))
        origin = self.months[MIN_TRAIN_MONTHS]  # ровно 24
        self.assertFalse(math.isnan(core_nnls_asof(self.vectors, self.core, origin)))

    def test_no_leak_from_future_months(self):
        origin = self.months[28]
        base = core_nnls_asof(self.vectors, self.core, origin)
        vectors = {m: dict(v) for m, v in self.vectors.items()}
        core = dict(self.core)
        for month in self.months:
            if month >= origin:
                core[month] += 100.0  # core за O и позже неизвестен на cutoff O
            if month > origin:
                vectors[month] = {name: 99.0 for name in BUCKET_NAMES}
        self.assertEqual(core_nnls_asof(vectors, core, origin), base)

    def test_uses_core_of_previous_month(self):
        origin = self.months[28]
        base = core_nnls_asof(self.vectors, self.core, origin)
        core = dict(self.core)
        core[self.months[27]] += 5.0  # O−1 уже известен — признак обязан измениться
        self.assertNotEqual(core_nnls_asof(self.vectors, core, origin), base)

    def test_bucket_missing_in_origin_month_is_dropped(self):
        origin = self.months[30]
        vectors = {m: dict(v) for m, v in self.vectors.items()}
        del vectors[origin]["fish"]
        value = core_nnls_asof(vectors, self.core, origin)
        expected = sum(self.TRUTH[name] * vectors[origin][name] for name in self.TRUTH)
        self.assertAlmostEqual(value, expected, places=6)

    def test_sparse_bucket_in_train_is_dropped(self):
        origin = self.months[34]
        vectors = {m: dict(v) for m, v in self.vectors.items()}
        for month in self.months[:20]:  # fish есть < 24 месяцев обучения
            del vectors[month]["fish"]
        value = core_nnls_asof(vectors, self.core, origin)
        self.assertFalse(math.isnan(value))

    def test_months_without_core_are_skipped(self):
        origin = self.months[34]
        core = {m: v for m, v in self.core.items() if m != self.months[10]}
        value = core_nnls_asof(self.vectors, core, origin)
        expected = sum(self.TRUTH[name] * self.vectors[origin][name] for name in self.TRUTH)
        self.assertAlmostEqual(value, expected, places=6)

    def test_nan_when_fewer_than_min_buckets(self):
        origin = self.months[30]
        keep = set(BUCKET_NAMES[: MIN_BUCKETS - 1])
        vectors = {m: {n: x for n, x in v.items() if n in keep} for m, v in self.vectors.items()}
        self.assertTrue(math.isnan(core_nnls_asof(vectors, self.core, origin)))

    def test_defined_with_exactly_min_buckets(self):
        origin = self.months[30]
        keep = set(BUCKET_NAMES[:MIN_BUCKETS])
        vectors = {m: {n: x for n, x in v.items() if n in keep} for m, v in self.vectors.items()}
        self.assertFalse(math.isnan(core_nnls_asof(vectors, self.core, origin)))

    def test_nan_when_origin_month_has_no_buckets(self):
        origin = self.months[30]
        vectors = {m: v for m, v in self.vectors.items() if m != origin}
        self.assertTrue(math.isnan(core_nnls_asof(vectors, self.core, origin)))


class AttachTest(unittest.TestCase):
    def test_adds_feature_without_mutating_rows(self):
        vectors, core = synthetic({"meat": 1.0}, count=40)
        grid = sorted(vectors)
        rows = [cm.Row(month=m, y={"cpi_all": 0.5}, features={"month": m.month}) for m in grid]
        out = attach_core_nnls(rows, vectors, core)
        self.assertEqual(len(out), len(rows))
        self.assertNotIn("core_nnls", rows[0].features)
        self.assertTrue(math.isnan(out[0].features["core_nnls"]))
        self.assertFalse(math.isnan(out[-1].features["core_nnls"]))
        self.assertEqual(out[-1].features["month"], grid[-1].month)


class FeatureNamesTest(unittest.TestCase):
    def test_v4c_is_v4a_plus_core_nnls_before_month(self):
        names = feature_names_v4c("S_L09")
        self.assertEqual(names[-1], "month")
        self.assertEqual(names[-2], "core_nnls")
        self.assertNotIn("usd_mm", names)

    def test_v4d_has_no_core_nnls(self):
        names = feature_names_v4d("S_L09")
        self.assertNotIn("core_nnls", names)
        self.assertIn("wkagg_mtd", names)
        self.assertEqual(names[:-1], feature_names_v4c("S_L09")[:-2])


class ModelSmokeTest(unittest.TestCase):
    """v4c/v4d на крошечной синтетике: конечный прогноз, лишние колонки не нужны."""

    NAMES = ["x1", "x2", "core_nnls", "month"]

    def rows(self, count=30):
        rng = np.random.default_rng(5)
        out = []
        for index, month in enumerate(months(date(2020, 1, 1), count)):
            x1, x2 = float(rng.normal()), float(rng.normal())
            nnls = float(rng.normal()) if index > 5 else float("nan")
            out.append(
                cm.Row(
                    month=month,
                    y={"cpi_all": 0.8 * x1 + 0.1 * x2 + float(rng.normal(0, 0.05))},
                    features={"x1": x1, "x2": x2, "core_nnls": nnls, "month": month.month},
                )
            )
        return out

    def test_v4c_returns_finite_with_nan_feature(self):
        rows = self.rows()
        value = make_v4c(self.NAMES)(rows[:-1], rows[-1])
        self.assertTrue(math.isfinite(value))

    def test_v4d_ignores_core_nnls_and_is_finite(self):
        rows = self.rows()
        names = ["x1", "x2", "month"]
        value = make_v4d(names)(rows[:-1], rows[-1])
        self.assertTrue(math.isfinite(value))
        # признак core_nnls не влияет на v4d
        changed = [
            cm.Row(month=r.month, y=r.y, features={**r.features, "core_nnls": 123.0}) for r in rows
        ]
        self.assertEqual(make_v4d(names)(changed[:-1], changed[-1]), value)


if __name__ == "__main__":
    unittest.main()
