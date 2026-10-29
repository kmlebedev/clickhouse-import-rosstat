#!/usr/bin/env python3
"""Офлайн-тесты as-of конвенции `publication` (#401 W0).

Проверяются: конвенция по умолчанию `late` не изменилась; `publication` —
cutoff = публикация месячного ИПЦ `M−1` (9-е `M`), точная доступность месячной
метки; восстановление конвенции при выходе из контекста; согласованность с
`publication_calendar`; отсутствие утечки: порча любых данных, недоступных к
cutoff, не меняет признаки месяца (базовые, МП, `wkagg_*`), а порча доступных —
меняет (контроль чувствительности теста). ClickHouse и сеть не используются.

Запуск — `make cpi-asof-test`.
"""

import math
import sys
import unittest
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from lib.publication_calendar import PublicationCalendar
from models.cpi import cpi_monthly as cm
from models.cpi import cpi_weekly_aggregate as agg
import cpi_monthly_test as cmt

ORIGIN = date(2020, 6, 1)


def poison(value: float) -> float:
    return value * 1000.0 + 7.0


class ConventionTest(unittest.TestCase):
    def test_default_is_late_and_unchanged(self):
        self.assertEqual(cm.current_asof_convention(), "late")
        self.assertEqual(cm.cutoff_for(date(2026, 10, 1)), date(2026, 11, 8))
        self.assertEqual(cm.available_from_mes(date(2026, 9, 30)), date(2026, 10, 31))

    def test_publication_cutoff_and_label_availability(self):
        with cm.asof_convention("publication"):
            self.assertEqual(cm.current_asof_convention(), "publication")
            self.assertEqual(cm.cutoff_for(date(2026, 10, 1)), date(2026, 10, 9))
            self.assertEqual(cm.cutoff_for(date(2026, 10, 17)), date(2026, 10, 9))
            self.assertEqual(cm.available_from_mes(date(2026, 9, 30)), date(2026, 10, 9))
            self.assertEqual(cm.available_from_mes(date(2025, 12, 31)), date(2026, 1, 9))

    def test_publication_label_of_origin_is_not_available(self):
        with cm.asof_convention("publication"):
            for month in (date(2014, 1, 1), date(2020, 6, 1), date(2026, 8, 1)):
                own = cm.available_from_mes(cm.month_end(month))
                self.assertGreater(own, cm.cutoff_for(month))
                lag = cm.available_from_mes(cm.month_end(cm.previous_month(month)))
                self.assertLessEqual(lag, cm.cutoff_for(month))

    def test_restored_after_exit_and_after_error(self):
        with cm.asof_convention("publication"):
            pass
        self.assertEqual(cm.current_asof_convention(), "late")
        with self.assertRaises(RuntimeError):
            with cm.asof_convention("publication"):
                raise RuntimeError("boom")
        self.assertEqual(cm.current_asof_convention(), "late")
        self.assertEqual(cm.cutoff_for(date(2026, 10, 1)), date(2026, 11, 8))

    def test_unknown_convention_is_error(self):
        with self.assertRaises(ValueError):
            with cm.asof_convention("tomorrow"):
                pass
        self.assertEqual(cm.current_asof_convention(), "late")

    def test_nested_contexts_restore_outer(self):
        with cm.asof_convention("publication"):
            with cm.asof_convention("late"):
                self.assertEqual(cm.cutoff_for(date(2026, 10, 1)), date(2026, 11, 8))
            self.assertEqual(cm.cutoff_for(date(2026, 10, 1)), date(2026, 10, 9))

    def test_consistent_with_publication_calendar(self):
        calendar = PublicationCalendar()
        with cm.asof_convention("publication"):
            for month in (date(2014, 3, 1), date(2020, 6, 1), date(2026, 10, 1)):
                prev = cm.previous_month(month)
                published = calendar.lookup("rosstat", "cpi_all", cm.month_end(prev)).published_at
                self.assertEqual(cm.cutoff_for(month), published.date())
                self.assertEqual(cm.available_from_mes(cm.month_end(prev)), published.date())
                self.assertEqual(published, datetime(published.year, published.month, 9, 19, 0))


class BuildSampleTest(unittest.TestCase):
    def build(self, world, convention):
        with cm.asof_convention(convention):
            return cm.build_sample(
                world["targets"], world["survey"], world["usd"], world["key_rate"], world["fao"]
            )

    def test_late_default_unchanged_by_context_machinery(self):
        rows_default, skipped_default = cmt.build_world(cmt.synthetic_world())
        rows_late, skipped_late = self.build(cmt.synthetic_world(), "late")
        self.assertEqual(rows_default, rows_late)
        self.assertEqual(skipped_default, skipped_late)
        self.assertEqual(len(rows_default), 155)

    def test_publication_builds_rows_with_stale_mp(self):
        rows, skipped = self.build(cmt.synthetic_world(), "publication")
        # Первому месяцу выборки не хватает пары доступных точек МП (нужны M−2 и
        # M−3): строка пропускается с причиной, а не заполняется молча.
        self.assertEqual([month for month, _ in skipped], [date(2013, 10, 1)])
        self.assertIn("МП", skipped[0][1])
        row = next(item for item in rows if item.month == ORIGIN)
        late_rows, _ = self.build(cmt.synthetic_world(), "late")
        late = next(item for item in late_rows if item.month == ORIGIN)
        # Метка M−1 и M−2, USD, ставка, FAO не зависят от конвенции здесь:
        # публикация M−1 уже вышла к 9-му M.
        self.assertEqual(row.features["y_lag1"], late.features["y_lag1"])
        self.assertEqual(row.features["y_lag2"], late.features["y_lag2"])
        self.assertEqual(row.features["usd_mm"], late.features["usd_mm"])
        # МП за M−1 доступен только в конце M (консервативное правило): к 9-му
        # берётся последняя доступная точка — M−2, то есть признак «старше».
        self.assertNotEqual(row.features["price_expectations_sa"], late.features["price_expectations_sa"])

    def leak_world(self, cutoff: date, origin: date, poisoned: bool):
        world = cmt.synthetic_world()
        if not poisoned:
            return world
        with cm.asof_convention("publication"):
            targets = {
                code: [
                    (day, poison(value) if cm.available_from_mes(day) > cutoff else value)
                    for day, value in points
                ]
                for code, points in world["targets"].items()
            }
            survey = {
                name: [
                    (day, poison(value) if cm.available_from_survey(day) > cutoff else value)
                    for day, value in points
                ]
                for name, points in world["survey"].items()
            }
        usd = [
            (day, poison(value) if cm.month_of(day) >= origin else value) for day, value in world["usd"]
        ]
        rate = [
            (day, poison(value) if cm.available_from_rate(day) > cutoff else value)
            for day, value in world["key_rate"]
        ]
        fao = [
            (day, poison(value) if cm.available_from_fao(day) > cutoff else value)
            for day, value in world["fao"]
        ]
        return {"targets": targets, "survey": survey, "usd": usd, "key_rate": rate, "fao": fao}

    def row_for(self, rows, month):
        return next(item for item in rows if item.month == month)

    def test_no_leak_under_publication(self):
        with cm.asof_convention("publication"):
            cutoff = cm.cutoff_for(ORIGIN)
        clean, _ = self.build(self.leak_world(cutoff, ORIGIN, False), "publication")
        dirty, _ = self.build(self.leak_world(cutoff, ORIGIN, True), "publication")
        a, b = self.row_for(clean, ORIGIN), self.row_for(dirty, ORIGIN)
        self.assertEqual(a.features.keys(), b.features.keys())
        for name in a.features:
            same = a.features[name] == b.features[name] or (
                isinstance(a.features[name], float)
                and math.isnan(a.features[name])
                and math.isnan(b.features[name])
            )
            self.assertTrue(same, f"утечка в признаке {name}: {a.features[name]} != {b.features[name]}")

    def test_no_leak_by_observation_date_independent_of_availability_rules(self):
        # Порча по дате наблюдения, зашитой в тест (не через правила модуля):
        # метки и МП месяцев ≥ M, дневные/ставка/FAO — после 9 июня 2020.
        hard_cutoff = date(2020, 6, 9)
        world = cmt.synthetic_world()
        bad = {
            "targets": {
                code: [(d, poison(v) if d >= ORIGIN else v) for d, v in points]
                for code, points in world["targets"].items()
            },
            "survey": {
                name: [(d, poison(v) if d >= ORIGIN else v) for d, v in points]
                for name, points in world["survey"].items()
            },
            "usd": [(d, poison(v) if d > hard_cutoff else v) for d, v in world["usd"]],
            "key_rate": [(d, poison(v) if d > hard_cutoff else v) for d, v in world["key_rate"]],
            "fao": [(d, poison(v) if d > hard_cutoff else v) for d, v in world["fao"]],
        }
        clean, _ = self.build(world, "publication")
        dirty, _ = self.build(bad, "publication")
        a, b = self.row_for(clean, ORIGIN), self.row_for(dirty, ORIGIN)
        for name in a.features:
            same = a.features[name] == b.features[name] or (
                isinstance(a.features[name], float)
                and math.isnan(a.features[name])
                and math.isnan(b.features[name])
            )
            self.assertTrue(same, f"утечка по дате наблюдения в {name}")

    def test_sensitivity_control_available_data_does_change_features(self):
        world = cmt.synthetic_world()
        with cm.asof_convention("publication"):
            cutoff = cm.cutoff_for(ORIGIN)
        clean, _ = self.build(world, "publication")
        # Порча метки M−1 (доступна к cutoff) обязана менять y_lag1.
        bad = dict(world)
        lag = cm.month_end(cm.previous_month(ORIGIN))
        bad["targets"] = {
            code: [(d, poison(v) if d == lag else v) for d, v in points]
            for code, points in world["targets"].items()
        }
        dirty, _ = self.build(bad, "publication")
        self.assertNotEqual(self.row_for(clean, ORIGIN).features["y_lag1"], self.row_for(dirty, ORIGIN).features["y_lag1"])
        # Порча последней доступной ставки меняет key_rate.
        bad = dict(world)
        available = [d for d, _ in world["key_rate"] if cm.available_from_rate(d) <= cutoff]
        last = max(available)
        bad["key_rate"] = [(d, poison(v) if d == last else v) for d, v in world["key_rate"]]
        dirty, _ = self.build(bad, "publication")
        self.assertNotEqual(self.row_for(clean, ORIGIN).features["key_rate"], self.row_for(dirty, ORIGIN).features["key_rate"])

    def test_publication_needs_one_more_month_of_mp_history(self):
        late, _ = self.build(cmt.synthetic_world(), "late")
        pub, _ = self.build(cmt.synthetic_world(), "publication")
        self.assertEqual(len(pub), len(late) - 1)
        self.assertEqual([row.month for row in pub], [row.month for row in late][1:])


class ExactReleaseDatesTest(unittest.TestCase):
    """`publication` с точными датами релизов: cutoff = фактическая дата, иначе правило 9-го."""

    DATES = {date(2020, 5, 1): date(2020, 6, 5), date(2025, 3, 1): date(2025, 4, 11)}

    def test_cutoff_and_availability_use_exact_dates_with_fallback(self):
        with cm.asof_convention("publication", release_dates=self.DATES):
            self.assertEqual(cm.cutoff_for(date(2020, 6, 1)), date(2020, 6, 5))
            self.assertEqual(cm.available_from_mes(date(2020, 5, 31)), date(2020, 6, 5))
            self.assertEqual(cm.cutoff_for(date(2025, 4, 1)), date(2025, 4, 11))
            # Месяца нет в таблице — правило 9-го.
            self.assertEqual(cm.cutoff_for(date(2018, 6, 1)), date(2018, 6, 9))
            self.assertEqual(cm.available_from_mes(date(2018, 5, 31)), date(2018, 6, 9))

    def test_release_dates_only_for_publication_and_restored_after(self):
        with self.assertRaises(ValueError):
            with cm.asof_convention("late", release_dates=self.DATES):
                pass
        with cm.asof_convention("publication", release_dates=self.DATES):
            pass
        with cm.asof_convention("publication"):
            self.assertEqual(cm.cutoff_for(date(2020, 6, 1)), date(2020, 6, 9))

    def test_nested_inner_without_dates_uses_rule_and_outer_restored(self):
        with cm.asof_convention("publication", release_dates=self.DATES):
            with cm.asof_convention("publication"):
                self.assertEqual(cm.cutoff_for(date(2020, 6, 1)), date(2020, 6, 9))
            self.assertEqual(cm.cutoff_for(date(2020, 6, 1)), date(2020, 6, 5))

    def test_no_leak_by_observation_date_with_exact_early_release(self):
        # Релиз за май 2020 вышел 5 июня (раньше правила): данные после 5 июня — будущее.
        hard_cutoff = date(2020, 6, 5)
        world = cmt.synthetic_world()
        bad = {
            "targets": world["targets"],
            "survey": world["survey"],
            "usd": [(d, poison(v) if d > hard_cutoff else v) for d, v in world["usd"]],
            "key_rate": [(d, poison(v) if d > hard_cutoff else v) for d, v in world["key_rate"]],
            "fao": [(d, poison(v) if d > hard_cutoff else v) for d, v in world["fao"]],
        }

        def build(w):
            with cm.asof_convention("publication", release_dates=self.DATES):
                return cm.build_sample(w["targets"], w["survey"], w["usd"], w["key_rate"], w["fao"])

        clean, _ = build(world)
        dirty, _ = build(bad)
        a = next(r for r in clean if r.month == ORIGIN)
        b = next(r for r in dirty if r.month == ORIGIN)
        for name in a.features:
            same = a.features[name] == b.features[name] or (
                isinstance(a.features[name], float) and math.isnan(a.features[name]) and math.isnan(b.features[name])
            )
            self.assertTrue(same, f"утечка после фактической даты релиза в {name}")

    def test_weekly_visibility_follows_exact_date(self):
        points = [(date.fromordinal(date(2020, 3, 2).toordinal() + 7 * i), 100.0 + 0.1 * (i % 5)) for i in range(40)]
        with cm.asof_convention("publication", release_dates=self.DATES):
            early = agg.wkagg_features(points, ORIGIN, cm.cutoff_for(ORIGIN))
        with cm.asof_convention("publication"):
            rule = agg.wkagg_features(points, ORIGIN, cm.cutoff_for(ORIGIN))
        self.assertLessEqual(early["wkagg_cov"], rule["wkagg_cov"])


class WeeklyAggregateLeakTest(unittest.TestCase):
    def points(self):
        start = date(2020, 3, 2)
        out = []
        for i in range(0, 40):
            out.append((date.fromordinal(start.toordinal() + 7 * i), 100.0 + 0.1 * (i % 5)))
        return out

    def test_wkagg_ignores_weeks_unavailable_at_publication_cutoff(self):
        with cm.asof_convention("publication"):
            cutoff = cm.cutoff_for(ORIGIN)
        clean = agg.wkagg_features(self.points(), ORIGIN, cutoff)
        dirty_points = [
            (day, poison(value) if (day.toordinal() + agg.AVAILABLE_LAG_DAYS) > cutoff.toordinal() else value)
            for day, value in self.points()
        ]
        dirty = agg.wkagg_features(dirty_points, ORIGIN, cutoff)
        for name in clean:
            a, b = clean[name], dirty[name]
            self.assertTrue(a == b or (math.isnan(a) and math.isnan(b)), name)

    def test_publication_cutoff_sees_less_of_the_origin_month_than_late(self):
        with cm.asof_convention("publication"):
            early = agg.wkagg_features(self.points(), ORIGIN, cm.cutoff_for(ORIGIN))
        with cm.asof_convention("late"):
            late = agg.wkagg_features(self.points(), ORIGIN, cm.cutoff_for(ORIGIN))
        self.assertLess(early["wkagg_cov"], late["wkagg_cov"])
        self.assertLessEqual(early["wkagg_cov"], 0.35)
        self.assertGreaterEqual(late["wkagg_cov"], 0.95)


if __name__ == "__main__":
    unittest.main()
