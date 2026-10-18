#!/usr/bin/env python3
"""Офлайн-тесты `scripts/shock_classifier.py` (Tasks 2–3).

Запуск из корня репозитория:

    uv run --with scikit-learn --with pandas --with "statsmodels>=0.15" \
        --with scipy --python 3.12 python3 scripts/shock_classifier_test.py -v

Набор НЕ обращается к ClickHouse и сети: данные — синтетические фикстуры
`scripts/testdata/shock_classifier/`, которые пишет и читает сам модуль
(`export_fixtures`/`read_fixtures`). Проверяются:

* правило метки: явный шок = внеочередное И изменение ставки;
* as-of: точка, опубликованная после заседания, не участвует (месячная и
  дневная конвенции);
* пропуск канала — `None`, а не 0; `missing_count` его считает;
* дельта — к предыдущему ЗАСЕДАНИЮ панели, а не к предыдущему месяцу;
* в панель попадают только решения `key_rate_model`;
* экспорт фикстур детерминирован и читается обратно теми же строками;
* причинные веса: клип `[0.05, 1]`, монотонность, первые `MIN_TRAIN` = 1,
  тестовая строка не меняет статистики прошлых, явные шоки понижены;
* пропуски заполняются медианой ОБУЧЕНИЯ (в стандартизованном виде 0);
* классификатор: AUC > 0.9 на разделимой синтетике, вырожденные фолды —
  `None`, метрики без положительных — явные `None`.
"""

import contextlib
import csv
import dataclasses
import io
import random
import statistics
import sys
import tempfile
import unittest
import unittest.mock
from datetime import date, timedelta
from pathlib import Path

import numpy as np

# Модуль и тест лежат рядом, запуск прямой (`python3 scripts/...`), поэтому его
# каталог добавляется в sys.path до импорта.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import shock_classifier as sc


def fixture_panel() -> list:
    """Панель на синтетических точках в памяти (без CSV и ClickHouse)."""
    return sc.build_panel(
        sc.FIXTURE_CANONICAL_MEETINGS,
        sc.FIXTURE_KEY_RATE_POINTS,
        sc.fixture_channel_points(),
        [],
    )


def fixture_rows(name: str, directory: Path) -> list:
    """Строки CSV-фикстуры словарями — в форме ответа `ch_query`."""
    with (directory / name).open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return rows


def synthetic_panel(
    count: int,
    shock_indices=(),
    *,
    consistent_shocks: bool = False,
    seed: int = sc.SEED,
) -> list:
    """Детерминированная панель для тестов Task 3 (без ClickHouse и сети).

    Обычные строки — компактное облако вокруг `10..17`; явные шоки — выбросы.
    `consistent_shocks=True` сдвигает все шоки в ОДНУ сторону (классы линейно
    отделимы — проверка AUC), `False` — у каждого шока своя случайная
    комбинация знаков (ковариация обучения не «выучивает» направление —
    проверка весов). Генератор — собственный `random.Random(seed)`: тест не
    зависит от глобального ГПСЧ и повторяется побитово.
    """
    shock_indices = set(shock_indices)
    generator = random.Random(seed)
    start = date(2014, 1, 1)
    rows = []
    for index in range(count):
        shock = index in shock_indices
        features = {}
        for offset, name in enumerate(sc.FEATURES_CORE):
            value = 10.0 + offset + generator.gauss(0.0, 0.2)
            if shock:
                if consistent_shocks:
                    value += 100.0
                else:
                    value += generator.choice((-1.0, 1.0)) * (50.0 + 5.0 * offset)
            features[name] = value
        decision = sc.krm.Decision(
            meeting_date=start + timedelta(days=30 * index),
            rate_before=7.5,
            rate_after=7.5,
            step_bp=-100 if shock else 0,
            y3=-1 if shock else 0,
        )
        rows.append(
            sc.PanelRow(
                decision=decision,
                emergency=shock,
                explicit_shock=shock,
                features=features,
                missing_count=0,
            )
        )
    return rows


def with_feature(row, name: str, value):
    """Копия строки панели с заменённым значением одного признака."""
    features = dict(row.features)
    features[name] = value
    return dataclasses.replace(row, features=features)


class LabelTest(unittest.TestCase):
    def test_label_shock_requires_emergency_and_change(self):
        self.assertTrue(sc.label_shock(True, -300))
        self.assertFalse(sc.label_shock(True, 0))
        self.assertFalse(sc.label_shock(False, 100))

    def test_emergency_without_change_is_not_shock(self):
        # Реальная панель: внеочередное заседание с неизменной ставкой. Метка
        # emergency сохраняется, explicit_shock — нет.
        meetings = [(date(2022, 2, 28), True)]
        key_rate = [
            (date(2022, 2, 27), 7.5),
            (date(2022, 3, 7), 7.5),
            (date(2022, 6, 1), 7.5),
        ]
        rows = sc.build_panel(meetings, key_rate, {}, [])
        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0].emergency)
        self.assertFalse(rows[0].explicit_shock)
        self.assertEqual(rows[0].decision.step_bp, 0)


class MeetingsTest(unittest.TestCase):
    def test_meetings_filter_title_and_dedup(self):
        rows = [
            # Каноническая строка + её дубль с другим флагом: OR по дате.
            {
                "event_date": "2022-02-28",
                "category": "cbr",
                "title": sc.SAMPLE_TITLE,
                "emergency": 0,
            },
            {
                "event_date": "2022-02-28",
                "category": "cbr",
                "title": sc.SAMPLE_TITLE,
                "emergency": 1,
            },
            # Ручной триггер на ту же дату: отсекается фильтром по title.
            {
                "event_date": "2022-02-28",
                "category": "cbr",
                "title": "СД ЦБ РФ: ручной триггер",
                "emergency": 0,
            },
            # Уникальная дата ручного триггера: без фильтра дала бы заседание.
            {
                "event_date": "2022-03-01",
                "category": "cbr",
                "title": "СД ЦБ РФ: ручной триггер",
                "emergency": 1,
            },
            # Чужая категория: не заседание ЦБ.
            {
                "event_date": "2022-03-02",
                "category": "fomc",
                "title": sc.SAMPLE_TITLE,
                "emergency": 1,
            },
        ]
        self.assertEqual(
            sc.meetings_with_emergency_from_rows(rows),
            [(date(2022, 2, 28), True)],
        )

    def test_fixture_has_eight_meetings_and_two_emergency(self):
        meetings = sc.meetings_with_emergency_from_rows(
            sc._fixture_meeting_rows()
        )
        self.assertEqual(len(meetings), 8)
        self.assertEqual(
            [day for day, emergency in meetings if emergency],
            [date(2022, 2, 28), date(2022, 4, 8)],
        )
        # Ручная строка с уникальной датой не попала в список.
        self.assertNotIn(date(2021, 5, 14), [day for day, _ in meetings])


class AsOfTest(unittest.TestCase):
    def test_point_published_after_meeting_is_not_used(self):
        # Точка за март 2022 доступна только в конце апреля; заседание
        # 2022-04-08 обязано видеть февральскую.
        points = [
            (date(2022, 1, 31), 1.0),
            (date(2022, 2, 28), 2.0),
            (date(2022, 3, 31), 3.0),
        ]
        self.assertEqual(
            sc.feature_asof(points, date(2022, 4, 8), monthly=True), 2.0
        )

    def test_point_available_at_end_of_next_month_is_used(self):
        # Граница включительна: февральская точка доступна 2022-03-31.
        points = [(date(2022, 2, 28), 2.0)]
        self.assertEqual(
            sc.feature_asof(points, date(2022, 3, 31), monthly=True), 2.0
        )
        self.assertIsNone(
            sc.feature_asof(points, date(2022, 3, 30), monthly=True)
        )

    def test_daily_point_after_meeting_is_not_used(self):
        points = [
            (date(2022, 4, 7), 10.0),
            (date(2022, 4, 8), 11.0),
            (date(2022, 4, 9), 12.0),
        ]
        self.assertEqual(
            sc.feature_asof(points, date(2022, 4, 8), monthly=False), 11.0
        )

    def test_march_fixture_point_is_not_used_on_april_meeting(self):
        points = sc.fixture_cpd_points()["pi"]
        february = dict(points)[date(2022, 2, 28)]
        march = dict(points)[date(2022, 3, 31)]
        self.assertNotEqual(february, march)
        self.assertEqual(
            sc.feature_asof(points, date(2022, 4, 8), monthly=True),
            february,
        )


class PanelTest(unittest.TestCase):
    def test_panel_has_expected_decisions_and_labels(self):
        rows = fixture_panel()
        self.assertEqual(
            [row.decision.meeting_date for row in rows],
            [day for day, _ in sc.FIXTURE_CANONICAL_MEETINGS],
        )
        self.assertEqual(
            [row.emergency for row in rows],
            [emergency for _, emergency in sc.FIXTURE_CANONICAL_MEETINGS],
        )
        self.assertEqual(
            [row.explicit_shock for row in rows],
            [False, False, False, False, False, True, False, True],
        )
        self.assertEqual([row.decision.step_bp for row in rows],
                         [25, 25, 25, 50, 100, 1350, 0, -300])

    def test_missing_channel_is_none_not_zero(self):
        row = fixture_panel()[0]
        self.assertIsNone(row.features["infl_exp_expected"])
        self.assertIsNone(row.features["infl_exp_observed"])
        self.assertGreaterEqual(row.missing_count, 1)

    def test_missing_count_counts_base_features_only(self):
        row = fixture_panel()[0]
        expected = sum(
            1 for name in sc.BASE_FEATURES if row.features[name] is None
        )
        self.assertEqual(row.missing_count, expected)

    def test_delta_is_to_previous_meeting(self):
        rows = fixture_panel()
        self.assertIsNotNone(rows[0].features["pi_saar"])
        self.assertIsNotNone(rows[1].features["pi_saar"])
        self.assertAlmostEqual(
            rows[1].features["pi_saar_chg"],
            rows[1].features["pi_saar"] - rows[0].features["pi_saar"],
            places=9,
        )
        # И это именно сдвиг между заседаниями: дельта второго заседания не
        # равна разнице текущего и прошлого МЕСЯЧНЫХ значений.
        self.assertNotEqual(rows[1].features["pi_saar_chg"], 0.0)

    def test_first_row_deltas_are_none(self):
        row = fixture_panel()[0]
        for name in sc.DELTA_FEATURES:
            self.assertIsNone(row.features[f"{name}_chg"])

    def test_inflation_expectation_delta_appears_after_first_meeting(self):
        rows = fixture_panel()
        self.assertIsNone(rows[0].features["infl_exp_expected"])
        self.assertIsNotNone(rows[1].features["infl_exp_expected"])
        # У второй строки предыдущее значение отсутствовало — дельта None.
        self.assertIsNone(rows[1].features["infl_exp_expected_chg"])
        # А у третьей уже есть оба значения.
        self.assertIsNotNone(rows[2].features["infl_exp_expected_chg"])
        self.assertAlmostEqual(
            rows[2].features["infl_exp_expected_chg"],
            rows[2].features["infl_exp_expected"]
            - rows[1].features["infl_exp_expected"],
            places=9,
        )

    def test_yoy_uses_same_month_last_year(self):
        channels = sc.fixture_channel_points()
        row = fixture_panel()[0]
        wage = dict(channels["wage"])
        expected = (wage[date(2020, 12, 1)] / wage[date(2019, 12, 1)] - 1.0) * 100.0
        self.assertAlmostEqual(row.features["wage_yoy"], expected, places=9)

    def test_budget_balance_is_trailing_12m_ratio(self):
        channels = sc.fixture_channel_points()
        row = fixture_panel()[0]
        income = sum(
            value
            for day, value in channels["budget_income"]
            if date(2020, 1, 1) <= day <= date(2020, 12, 1)
        )
        expense = sum(
            value
            for day, value in channels["budget_expense"]
            if date(2020, 1, 1) <= day <= date(2020, 12, 1)
        )
        expected = (income - expense) / income * 100.0
        self.assertAlmostEqual(row.features["budget_balance"], expected, places=9)

    def test_build_panel_uses_decisions_only(self):
        # 2022-06-30 — будущее заседание: окно исхода (d+7) выходит за ряд
        # ставки, поэтому в панели его быть не должно.
        meetings = list(sc.FIXTURE_CANONICAL_MEETINGS) + [
            (date(2022, 6, 30), False)
        ]
        rows = sc.build_panel(
            meetings,
            sc.FIXTURE_KEY_RATE_POINTS,
            sc.fixture_channel_points(),
            [],
        )
        self.assertEqual(
            [row.decision.meeting_date for row in rows],
            [day for day, _ in sc.FIXTURE_CANONICAL_MEETINGS],
        )

    def test_excluded_future_meeting_does_not_change_previous_decision(self):
        # Предыдущее решение не зависит от того, передали ли вызывающий
        # будущее заседание: окно исхода ограничено d+7.
        without = fixture_panel()
        with_future = sc.build_panel(
            list(sc.FIXTURE_CANONICAL_MEETINGS) + [(date(2022, 6, 30), False)],
            sc.FIXTURE_KEY_RATE_POINTS,
            sc.fixture_channel_points(),
            [],
        )
        self.assertEqual(without, with_future)


class FixturesTest(unittest.TestCase):
    def test_export_fixtures_is_deterministic(self):
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(sc.export_fixtures(Path(first)), 0)
                self.assertEqual(sc.export_fixtures(Path(second)), 0)
            for name in sc.FIXTURE_COLUMNS:
                self.assertEqual(
                    (Path(first) / name).read_bytes(),
                    (Path(second) / name).read_bytes(),
                    f"{name}: повторный экспорт обязан давать те же байты",
                )

    def test_export_fixtures_roundtrip(self):
        with tempfile.TemporaryDirectory() as directory:
            with contextlib.redirect_stdout(io.StringIO()):
                sc.export_fixtures(Path(directory))
            meetings, key_rate, channels = sc.read_fixtures(Path(directory))
            exported = sc.build_panel(meetings, key_rate, channels, [])
            expected = fixture_panel()
            self.assertEqual(exported, expected)
            self.assertEqual(len(exported), 8)
            self.assertEqual(
                [row.explicit_shock for row in exported],
                [False, False, False, False, False, True, False, True],
            )

    def test_fixture_csv_shapes(self):
        with tempfile.TemporaryDirectory() as directory:
            with contextlib.redirect_stdout(io.StringIO()):
                sc.export_fixtures(Path(directory))
            meetings = fixture_rows(sc.FIXTURE_MEETINGS, Path(directory))
            self.assertEqual(
                set(meetings[0]), set(sc.FIXTURE_COLUMNS[sc.FIXTURE_MEETINGS])
            )
            # Ручная строка с уникальной датой присутствует в файле, но
            # отфильтровывается загрузчиком.
            self.assertIn(
                "2021-05-14", {row["event_date"] for row in meetings}
            )
            channels = fixture_rows(sc.FIXTURE_CHANNELS, Path(directory))
            self.assertEqual(
                set(channels[0]), set(sc.FIXTURE_COLUMNS[sc.FIXTURE_CHANNELS])
            )
            self.assertIn(
                "infl_exp_expected", {row["channel"] for row in channels}
            )


class LoaderTest(unittest.TestCase):
    def test_channel_queries_use_final(self):
        queries = []

        def fake_query(statement):
            queries.append(statement)
            return []

        with unittest.mock.patch.object(sc.krm, "ch_query", fake_query):
            channels = sc.load_channels()
        self.assertEqual(set(channels), set(sc.CHANNEL_KEYS))
        self.assertEqual(len(queries), len(sc.CHANNEL_SQL))
        for sql in queries:
            self.assertIn("FINAL", sql)
        self.assertIn("emergency", sc.MEETINGS_SQL)
        self.assertIn("FINAL", sc.MEETINGS_SQL)

    def test_salary_points_are_normalized_to_reporting_month(self):
        self.assertEqual(
            sc.normalize_salary_points([(date(2021, 3, 1), 100.0)]),
            [(date(2021, 2, 1), 100.0)],
        )
        # Декабрь отчётного года не должен «переехать» на январь.
        self.assertEqual(
            sc.normalize_salary_points([(date(2022, 1, 1), 200.0)]),
            [(date(2021, 12, 1), 200.0)],
        )

    def test_points_from_rows_rejects_duplicate_dates(self):
        rows = [
            {"date": "2022-01-31", "value": 1.0},
            {"date": "2022-01-31", "value": 2.0},
        ]
        with self.assertRaises(ValueError):
            sc.points_from_rows(rows, "тест")

    def test_points_from_rows_rejects_null_value(self):
        with self.assertRaises(ValueError):
            sc.points_from_rows([{"date": "2022-01-31", "value": ""}], "тест")


class AnomalyWeightsTest(unittest.TestCase):
    def test_weights_are_bounded_and_monotone(self):
        rows = synthetic_panel(48, {30, 34, 38, 42})
        weights = sc.walk_forward_weights(rows, 24, sc.FEATURES_CORE)
        self.assertEqual(len(weights), len(rows))
        for weight in weights:
            self.assertGreaterEqual(weight, sc.WEIGHT_MIN)
            self.assertLessEqual(weight, sc.WEIGHT_MAX)
        # Синтетика содержит выбросы: проверка не должна быть вакуумной.
        self.assertLess(min(weights), 1.0)
        self.assertLessEqual(
            sc.weight_from_score(10.0, 1.0),
            sc.weight_from_score(2.0, 1.0),
        )
        self.assertLessEqual(
            sc.weight_from_score(2.0, 1.0),
            sc.weight_from_score(1.0, 1.0),
        )
        self.assertEqual(sc.weight_from_score(1.0, 1.0), 1.0)
        self.assertEqual(sc.weight_from_score(10.0, 1.0), sc.WEIGHT_MIN)
        # Неизмеримый скор не понижает наблюдение.
        self.assertEqual(sc.weight_from_score(float("nan"), 1.0), 1.0)
        self.assertEqual(sc.weight_from_score(10.0, 0.0), 1.0)

    def test_first_min_train_weights_are_one(self):
        rows = synthetic_panel(30, {26, 27})
        weights = sc.walk_forward_weights(rows, 24, sc.FEATURES_CORE)
        self.assertEqual(weights[:24], [1.0] * 24)
        self.assertEqual(len(weights), len(rows))

    def test_test_row_does_not_change_training_statistics(self):
        rows = synthetic_panel(48, {30, 34, 38, 42})
        before = sc.walk_forward_weights(rows, 24, sc.FEATURES_CORE)
        mutated = with_feature(rows[-1], "pi_saar", 1e6)
        after = sc.walk_forward_weights(
            rows[:-1] + [mutated], 24, sc.FEATURES_CORE
        )
        # Своя строка влияет только на свой вес: статистики прошлых строк
        # считаются по тем же прошлым заседаниям.
        self.assertEqual(before[:-1], after[:-1])

    def test_explicit_shocks_are_downweighted(self):
        rows = synthetic_panel(48, {30, 34, 38, 42})
        weights = sc.walk_forward_weights(rows, 24, sc.FEATURES_CORE)
        shock = [w for row, w in zip(rows, weights) if row.explicit_shock]
        normal = [w for row, w in zip(rows, weights) if not row.explicit_shock]
        self.assertTrue(shock)
        self.assertLess(statistics.median(shock), statistics.median(normal))

    def test_missing_values_are_imputed_from_train_only(self):
        rows = synthetic_panel(32, {28, 29}, consistent_shocks=True)
        train = rows[:28]
        model = sc.fit_anomaly(train, sc.FEATURES_CORE)
        column = sc.FEATURES_CORE.index("pi_saar")
        missing = with_feature(rows[28], "pi_saar", None)
        matrix = sc._feature_matrix([missing], sc.FEATURES_CORE)
        standardized = sc._standardize(matrix, model.median, model.scale)
        # Пропуск заполнен медианой ОБУЧЕНИЯ → в стандартизованном виде ровно 0.
        self.assertEqual(float(standardized[0, column]), 0.0)
        observed = [
            row.features["pi_saar"]
            for row in train
            if row.features["pi_saar"] is not None
        ]
        self.assertAlmostEqual(
            float(model.median[column]), statistics.median(observed)
        )
        # Статистики зависят только от обучения: повторная подгонка совпадает,
        # а значение в тестовой строке на них не влияет.
        again = sc.fit_anomaly(train, sc.FEATURES_CORE)
        np.testing.assert_array_equal(model.median, again.median)
        np.testing.assert_array_equal(model.scale, again.scale)
        np.testing.assert_array_equal(model.precision, again.precision)
        self.assertEqual(model.median_distance, again.median_distance)
        filled = with_feature(rows[28], "pi_saar", 123456.0)
        filled_matrix = sc._feature_matrix([filled], sc.FEATURES_CORE)
        filled_standardized = sc._standardize(
            filled_matrix, model.median, model.scale
        )
        # Заполненное значение — это значение тестовой строки, а не статистика:
        # оно не совпадает с импутированным нулём.
        self.assertNotEqual(float(filled_standardized[0, column]), 0.0)


class ShockClassifierTest(unittest.TestCase):
    def test_classifier_separates_synthetic_shocks(self):
        rows = synthetic_panel(44, set(range(20, 32)), consistent_shocks=True)
        probabilities = sc.walk_forward_classifier(rows, 24, sc.FEATURES_CORE)
        labels = [int(row.explicit_shock) for row in rows]
        metrics = sc.classifier_metrics(labels, probabilities)
        self.assertIsNotNone(metrics["roc_auc"])
        self.assertGreater(metrics["roc_auc"], 0.9)
        self.assertGreater(metrics["pr_auc"], 0.9)
        self.assertGreater(metrics["recall"], 0.5)

    def test_degenerate_classifier_fold_is_skipped(self):
        no_positive = synthetic_panel(30, set())
        self.assertEqual(
            sc.walk_forward_classifier(no_positive, 24, sc.FEATURES_CORE),
            [None] * len(no_positive),
        )
        # Один класс — не только «нет шоков»: окно из одних шоков вырождено так
        # же и обязано пропускаться, а не отдавать вероятность 1.
        all_positive = synthetic_panel(
            30, set(range(30)), consistent_shocks=True
        )
        self.assertEqual(
            sc.walk_forward_classifier(all_positive, 24, sc.FEATURES_CORE),
            [None] * len(all_positive),
        )

    def test_metrics_without_positives_are_explicit_none(self):
        metrics = sc.classifier_metrics([0, 0, 0], [0.1, 0.2, 0.3])
        self.assertEqual(metrics["n"], 3)
        self.assertEqual(metrics["positives"], 0)
        self.assertIsNone(metrics["roc_auc"])
        self.assertIsNone(metrics["pr_auc"])
        self.assertIsNone(metrics["recall"])
        self.assertAlmostEqual(metrics["brier"], (0.01 + 0.04 + 0.09) / 3)
        self.assertEqual(metrics["confusion"], [[3, 0], [0, 0]])
        empty = sc.classifier_metrics([], [])
        self.assertEqual(empty["n"], 0)
        self.assertIsNone(empty["brier"])


if __name__ == "__main__":
    unittest.main()
