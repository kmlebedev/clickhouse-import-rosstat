#!/usr/bin/env python3
"""Общие оценочные функции бэктестов: RMSE/MAE, DM (HLN) и блочный бутстрап.

Дом для функций, которые нужны сразу трём скриптам: модели ключевой ставки
(`key_rate_model.py`), ML-треку nowcast'а ИПЦ v1 (`cpi_nowcast.py`, спека
`docs/superpowers/specs/2026-10-16-cpi-nowcast-design.md` §4) и его месячному
v2 с МП (`cpi_monthly.py`, спека
`docs/superpowers/specs/2026-10-18-cpi-monthly-mp-design.md` §4). Реализация
перенесена из `key_rate_model.py` один-в-один (включая формулировки
docstring'ов и конвенции вырожденных входов); там же остались тонкие обёртки
`rmse_bp`/`mae_bp`/`diebold_mariano`/`block_bootstrap_rmse_diff` с прежними
именами и значениями по умолчанию.

Термины docstring'ов — от первого потребителя: шкала ошибок — шаг в БАЗИСНЫХ
пунктах, единица наблюдения — заседание. Для вызывающего это лишь имена:
функции работают с любым рядом ошибок в его собственной шкале (у nowcast'а —
пункты процента месяца к месяцу).

Модуль ничего не знает про модели и не пишет в ClickHouse; `block`, `n` и
`seed` у бутстрапа обязательны явно — фиксированные значения (SEED, блок)
принадлежат спеке конкретного прогона, а не общему модулю.
"""

import math
import random
from dataclasses import dataclass

import statsmodels.api as sm


def rmse(errors: list) -> float:
    """RMSE прогноза шага в б.п.; `errors` — ошибки (прогноз − факт)."""
    if not errors:
        raise ValueError("RMSE: пустой список ошибок")
    return math.sqrt(sum(error * error for error in errors) / len(errors))


def mae(errors: list) -> float:
    """MAE прогноза шага в б.п.; `errors` — ошибки (прогноз − факт)."""
    if not errors:
        raise ValueError("MAE: пустой список ошибок")
    return sum(abs(error) for error in errors) / len(errors)


def diebold_mariano(err_a: list, err_b: list, h: int) -> tuple:
    """DM-тест (поправка HLN) на равенство точности двух прогнозов (спека §7).

    Путь — библиотечный: `statsmodels.tsa.stattools.diebold_mariano_test` с
    `harvey_adj=True` (пин `statsmodels>=0.15`). Поправка HLN масштабирует
    статистику и берёт p-value из распределения Стьюдента с `T − 1` степенями
    свободы; лаги HAC — правило библиотеки `max(h − 1, ⌈T^{1/3}⌉)` (для окон
    прогона это 5 лагов при T = 68…84). Путь назван и в отчёте.

    Потери — квадраты ошибок (как у RMSE, головной метрики §7). В API
    библиотеки подаются «факт» и два прогноза, а у нас на руках ошибки,
    поэтому «фактом» идёт нулевой ряд: `(0 − err)² = err²`, и разница потерь
    `d_t = err_a² − err_b²`. Положительная статистика значит, что ошибки
    `err_a` БОЛЬШЕ, то есть метод хуже.

    Соглашение о вырождении (нулевая дисперсия разницы потерь: t-статистике
    нечем делиться): среднее 0 — в частности, одинаковые ошибки —
    `(0.0, 1.0)`; среднее ≠ 0 — `(±inf, 0.0)`: преимущество бесспорно.
    """
    if len(err_a) != len(err_b):
        raise ValueError(f"DM: {len(err_a)} ошибок против {len(err_b)}")
    if not err_a:
        raise ValueError("DM: пустой список ошибок")
    differences = [a * a - b * b for a, b in zip(err_a, err_b)]
    if all(difference == differences[0] for difference in differences):
        mean = differences[0]
        if mean == 0.0:
            return (0.0, 1.0)
        return (math.copysign(math.inf, mean), 0.0)
    result = sm.tsa.stattools.diebold_mariano_test(
        [0.0] * len(err_a),
        list(err_a),
        list(err_b),
        criterion="mse",
        harvey_adj=True,
        horizon=h,
    )
    return (float(result.statistic), float(result.pvalue))


@dataclass(frozen=True)
class BootstrapDiff:
    """Блочный бутстрап разницы RMSE: точка, 95%-интервал и p-value."""

    rmse_diff: float
    low: float
    high: float
    p_value: float


def block_bootstrap_rmse_diff(
    err_a: list, err_b: list, block: int, n: int, seed: int
) -> BootstrapDiff:
    """95%-интервал и p-value разницы RMSE двух рядов ошибок (спека §7).

    Ресэмплируются пары ошибок ЦЕЛИКОМ: блок из `block` заседаний берётся
    одними и теми же индексами из обоих рядов — иначе разница RMSE размылась
    бы несвязанными точками, и интервал потерял бы парность сравнения. Блоки
    движутся по кругу, поэтому и последний блок полон.

    Квантили — по порядковым статистикам, без интерполяции: нижняя —
    `⌊0.025·n⌋`, верхняя — `⌈0.975·n⌉ − 1`.

    Конвенция p (зафиксирована до результатов): двусторонняя, по доле
    ресэмплов по разные стороны нуля,
    `p = min(1, 2·min(P(Δ* ≤ 0), P(Δ* ≥ 0)))`; нулевая разница во всех
    ресэмплах даёт p = 1.0, точное преимущество — p = 0.0.

    Генератор — собственный `random.Random(seed)`, а не глобальный `random`:
    прогон детерминирован при одном сиде и не зависит (и не влияет) на
    глобальное состояние ГПСЧ.
    """
    if len(err_a) != len(err_b):
        raise ValueError(f"бутстрап: {len(err_a)} ошибок против {len(err_b)}")
    if not err_a:
        raise ValueError("бутстрап: пустой список ошибок")
    if block < 1:
        raise ValueError(f"бутстрап: блок {block} — меньше одного заседания")
    if n < 1:
        raise ValueError(f"бутстрап: ресэмплов {n} — меньше одного")

    size = len(err_a)
    blocks_per_sample = -(-size // block)  # ⌈size/block⌉: блоков хватает на всю выборку
    generator = random.Random(seed)
    deltas = []
    for _ in range(n):
        indices = []
        for _ in range(blocks_per_sample):
            start = generator.randrange(size)
            indices.extend((start + offset) % size for offset in range(block))
        del indices[size:]
        squared_a = sum(err_a[index] * err_a[index] for index in indices)
        squared_b = sum(err_b[index] * err_b[index] for index in indices)
        deltas.append(math.sqrt(squared_a / size) - math.sqrt(squared_b / size))
    deltas.sort()
    low = deltas[int(0.025 * n)]
    high = deltas[min(n - 1, math.ceil(0.975 * n) - 1)]
    below = sum(1 for delta in deltas if delta <= 0.0) / n
    above = sum(1 for delta in deltas if delta >= 0.0) / n
    return BootstrapDiff(
        rmse_diff=rmse(err_a) - rmse(err_b),
        low=low,
        high=high,
        p_value=min(1.0, 2.0 * min(below, above)),
    )
