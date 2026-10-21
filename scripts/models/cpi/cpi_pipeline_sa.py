#!/usr/bin/env python3
"""Сезонная корректировка CPI в реальном времени (STL, as-of).

Task 2 конвейера CPI → SA/ожидания → ставка, пункт
`key-rate/phase-4-cpi-key-rate-pipeline`; спека
`docs/superpowers/specs/2026-10-20-cpi-key-rate-pipeline-design.md` §6.2.

Модуль — библиотека: на вход подаётся месячный NSA-ряд `[(date, float)]`,
на выходе — аддитивные сезонные факторы, SA-ряд и применение факторов к
списку значений. Ни ClickHouse, ни сети, ни записи в БД здесь нет.

Оценка — `statsmodels.tsa.seasonal.STL(period=12, robust=True)` строго на
точках `date <= as_of`: будущие месяцы не участвуют ни в факторах, ни в
результате. Сезонная компонента — `observed - trend - resid`, SA-значение —
`observed - seasonal`. Фактор позиции цикла — среднее сезонной компоненты
по наблюдениям этой позиции (для месячного ряда позиция = номер месяца).

Факторы не «финальные»: это снимок на `as_of`, пересчитываемый каждым
прогоном. X-13 может заменить STL за тем же интерфейсом
(`seasonal_factors`, `sa_series`, `apply_sa`), не меняя вызывающий код.

Недостаточная история — детерминированное поведение, а не ошибка: если
после отсечения `date <= as_of` осталось меньше двух полных периодов
(`2 * period`, то есть 24 месяца), `seasonal_factors` возвращает пустой
словарь. `apply_sa` и `sa_series` тогда возвращают ряд без изменений:
stage 2/3 должны работать и на коротком окне, а молчаливая оценка
сезонности по одному периоду была бы неотличима от настоящей.

Ряд обязан быть непрерывным месячным: дубликаты дат и разрывы дают
`ValueError`, чтобы пропущенный месяц не сдвигал сезонный цикл молча.

`apply_sa(values, factors)` не получает дат и следует контракту: `values` —
последовательные месячные наблюдения, начиная с января (позиция 1 цикла);
`factors` — полный словарь `{позиция 1..period: float}`. `sa_series`
работает по календарным месяцам дат, поэтому корректно обрабатывает ряд,
начинающийся не с января.

Запуск тестов — `make cpi-pipeline-sa-test`.
"""

from __future__ import annotations

import math
from datetime import date, datetime

import numpy as np
from statsmodels.tsa.seasonal import STL

# Месячная сезонность ИПЦ: 12 позиций в году. Параметр `period` оставлен в
# интерфейсе `seasonal_factors` для будущей замены STL на X-13.
DEFAULT_PERIOD = 12

# Сколько полных сезонных периодов нужно, чтобы аддитивная сезонность
# считалась идентифицируемой.
MIN_PERIODS = 2


def _month_index(day: date) -> int:
    """Абсолютный номер месяца: соседние месяцы отличаются на единицу."""
    return day.year * 12 + (day.month - 1)


def _season_position(day: date, period: int) -> int:
    """Позиция наблюдения в сезонном цикле 1..period.

    Для `period=12` совпадает с номером календарного месяца.
    """
    return _month_index(day) % period + 1


def _check_date(value, name: str) -> date:
    """Дата — именно `date`: `datetime` сломал бы сравнение с `as_of`."""
    if isinstance(value, datetime) or not isinstance(value, date):
        raise ValueError(
            f"{name}: ожидается date, получено "
            f"{type(value).__name__} ({value!r})"
        )
    return value


def _monthly_points(
    series: list[tuple[date, float]], as_of: date
) -> list[tuple[date, float]]:
    """Отсечённый `date <= as_of` ряд, отсортированный и проверенный.

    Проверки: конечные значения, отсутствие дубликатов месяцев и разрывов.
    Разрыв опасен тем, что STL считает наблюдения равноотстоящими, и
    пропущенный месяц молча сдвинул бы сезонный цикл.
    """
    as_of = _check_date(as_of, "as_of")
    points = []
    for day, value in series:
        _check_date(day, "series")
        numeric = float(value)
        if not math.isfinite(numeric):
            raise ValueError(
                f"series: нечисловое значение {value!r} на {day.isoformat()}"
            )
        if day <= as_of:
            points.append((day, numeric))
    points.sort(key=lambda item: item[0])
    for (previous, _), (current, _) in zip(points, points[1:]):
        if current == previous:
            raise ValueError(f"series: дубликат месяца {current.isoformat()}")
        if _month_index(current) - _month_index(previous) != 1:
            raise ValueError(
                "series: ряд не непрерывен, пропущен месяц между "
                f"{previous.isoformat()} и {current.isoformat()}"
            )
    return points


def _factors_from_points(
    points: list[tuple[date, float]], period: int
) -> dict[int, float]:
    """Аддитивные факторы по позициям цикла из уже отсечённого ряда."""
    if period < 2:
        raise ValueError(f"period={period}: сезонный период должен быть ≥ 2")
    if len(points) < MIN_PERIODS * period:
        return {}
    observed = np.asarray([value for _, value in points], dtype=float)
    fitted = STL(observed, period=period, robust=True).fit()
    seasonal = observed - fitted.trend - fitted.resid
    if not np.all(np.isfinite(seasonal)):
        raise ValueError("STL вернул нечисловую сезонную компоненту")
    grouped: dict[int, list[float]] = {}
    for (day, _), value in zip(points, seasonal):
        grouped.setdefault(_season_position(day, period), []).append(
            float(value)
        )
    # Полный набор позиций: `apply_sa` определяет период по максимальному
    # ключу. Пропущенная позиция возможна только в рваном ряду, который
    # отсекается выше; на всякий случай она получает нейтральный ноль.
    return {
        position: (
            float(np.mean(grouped[position]))
            if position in grouped
            else 0.0
        )
        for position in range(1, period + 1)
    }


def seasonal_factors(
    series: list[tuple[date, float]],
    as_of: date,
    period: int = DEFAULT_PERIOD,
) -> dict[int, float]:
    """Аддитивные сезонные факторы ряда на срезе `as_of`.

    STL оценивается только на точках `date <= as_of`; будущее в факторы не
    попадает. Возврат — полный словарь `{позиция 1..period: float}`; для
    `period=12` позиция — номер месяца. Пустой словарь означает, что
    доступно меньше `2 * period` точек и сезонность не идентифицируется;
    `apply_sa`/`sa_series` в этом случае ряд не меняют.
    """
    return _factors_from_points(_monthly_points(series, as_of), period)


def apply_sa(
    values: list[float], factors: dict[int, float]
) -> list[float]:
    """Вычитает аддитивные факторы: `value − factors[позиция]`.

    `values` — последовательные месячные наблюдения, начиная с января
    (позиция 1 цикла); `factors` — полный словарь `{1..period: float}` из
    `seasonal_factors` (период определяется по максимальному ключу). Пустые
    факторы — ряд не скорректирован, возвращается копия значений.
    """
    if not factors:
        return [float(value) for value in values]
    period = max(factors)
    if sorted(factors) != list(range(1, period + 1)):
        raise ValueError(
            "factors: ожидается полный набор позиций 1..period, "
            f"получено {sorted(factors)} (period={period})"
        )
    return [
        float(value) - factors[(index % period) + 1]
        for index, value in enumerate(values)
    ]


def sa_series(
    series: list[tuple[date, float]], as_of: date
) -> list[tuple[date, float]]:
    """SA-ряд на срезе `as_of`: `observed − seasonal` по календарным месяцам.

    Ряд отсекается `date <= as_of`; будущие точки не участвуют ни в оценке,
    ни в результате. Факторы — те же, что у
    `seasonal_factors(series, as_of)`. При недостаточной истории (меньше
    двух полных периодов) возвращается исходный отсечённый ряд.
    """
    points = _monthly_points(series, as_of)
    factors = _factors_from_points(points, DEFAULT_PERIOD)
    if not factors:
        return points
    return [
        (
            day,
            value - factors[_season_position(day, DEFAULT_PERIOD)],
        )
        for day, value in points
    ]
