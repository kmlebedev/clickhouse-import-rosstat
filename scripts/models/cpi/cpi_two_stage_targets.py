#!/usr/bin/env python3
"""`T_SAX` и варианты коррекции КУ во входе Stage 1 (#401 W2).

Спека `docs/superpowers/specs/2026-10-29-cpi-two-stage-weekly-sa-design.md` §4 W2
(протокол W2 — до результатов).

* `T_SAX` — SA ИПЦ без КУ: `(π − w·u)/(1 − w)` на м/м, затем X-13 (R `seasonal`) на
  накопленном уровне. **Окончательный** ряд — фит по всей выборке; **винтажная
  история** origin'а `O` — тот же фит на месяцах `≤ O−1` (допущение A1: `u` месяца
  доступно с релизом ИПЦ этого месяца).
* Варианты входа Stage 1: `V0` — недельный SA как есть; `V1` — приросты в тарифных
  неделях заменяются медианой не тарифных недель того же года; `V2` — из полных
  месяцев 4-нед. индекса вычитается `w·u`, неполный месяц сырой.

Скрипт — библиотека (без записи в БД). Тесты — `make cpi-two-stage-targets-test`.
"""

import statistics
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Callable, Optional

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import cpi_sa_reconciliation as rec
import weekly_sa_asof as wa

VARIANTS = ("V0", "V1", "V2")
DEFAULT_W = 0.10
MIN_VINTAGE_MONTHS = 36  # нижняя граница R-скрипта X-13
FIRST_MONTH = date(2002, 1, 1)
NO_REAL_TIME = 10**6  # `first_rt` вне выборки: считается только фит по всей выборке
TARIFF_THRESHOLD = 1.5  # п.п.: месяц с таким `u` считается тарифным (реализованный)
STANDARD_TARIFF_DAYS = ((1, 1), (7, 1))  # 1 января и 1 июля
FALLBACK_POINTS = 52  # окно запасной медианы, если в году нет не тарифных точек


def _prev_month(first: date) -> date:
    return date(first.year - (first.month == 1), 12 if first.month == 1 else first.month - 1, 1)


# ---------------------------------------------------------------------------
# T_SAX
# ---------------------------------------------------------------------------


def sax_mm(nsa: dict, util: dict, weight: float = DEFAULT_W) -> dict:
    """ИПЦ без КУ м/м, п.п.: `(π − w·u)/(1 − w)`."""
    return rec.ex_utilities_mm(nsa, util, weight)


def _fit_sa(nsa: dict, util: dict, weight: float, last: Optional[date], runner: Callable) -> dict:
    months = sorted(m for m in nsa if m >= FIRST_MONTH and (last is None or m <= last))
    missing = [m for m in months if m not in util]
    if last is not None and missing:
        raise ValueError(f"нет КУ за {missing[0]:%Y-%m} (допущение A1): винтаж не строится")
    months = [m for m in months if m in util]
    if len(months) < MIN_VINTAGE_MONTHS:
        raise ValueError(f"месяцев {len(months)} меньше {MIN_VINTAGE_MONTHS}: X-13 не строится")
    mm = sax_mm({m: nsa[m] for m in months}, {m: util[m] for m in months}, weight)
    series = rec.x13_series(months, mm, NO_REAL_TIME, runner)["full"]
    return {m: v for m, v in series.items() if v == v}  # первый месяц — NaN (нет предшественника)


def sax_final(nsa: dict, util: dict, weight: float = DEFAULT_W, runner: Callable = None) -> dict:
    """Окончательный `T_SAX` (SA м/м, п.п.): фит X-13 по всей выборке."""
    return _fit_sa(nsa, util, weight, None, runner)


def sax_vintage(nsa: dict, util: dict, weight: float, origin: date, runner: Callable) -> dict:
    """Винтажная история `T_SAX` origin'а `O`: тот же фит на месяцах `≤ O−1`; ничего позже в фит не входит."""
    return _fit_sa(nsa, util, weight, _prev_month(origin.replace(day=1)), runner)


def sax_vintages(nsa: dict, util: dict, weight: float, origins: list, runner: Callable) -> tuple:
    """`({origin: история}, [(origin, причина)])`: недоступные origin'ы пропускаются с причиной, не заполняются."""
    done, skipped = {}, []
    for origin in origins:
        try:
            done[origin] = sax_vintage(nsa, util, weight, origin, runner)
        except ValueError as error:
            skipped.append((origin, str(error)))
    return done, skipped


# ---------------------------------------------------------------------------
# V1: тарифные недели
# ---------------------------------------------------------------------------


def is_tariff_window(previous: date, end: date, util: dict, known_until: date, threshold: float = TARIFF_THRESHOLD) -> bool:
    """Окно `(previous, end]` тарифное: содержит 1 января/1 июля либо 1-е число месяца `m ≤ known_until`
    с реализованным `u(m) ≥ threshold` (известно к cutoff)."""
    day = previous + timedelta(days=1)
    while day <= end:
        if (day.month, day.day) in STANDARD_TARIFF_DAYS:
            return True
        if day.day == 1:
            first = day.replace(day=1)
            if first <= known_until and util.get(first, 0.0) >= threshold:
                return True
        day += timedelta(days=1)
    return False


def v1_adjust_points(points: list, cutoff: date, util: dict, known_until: date) -> list:
    """Тарифные недели → медиана не тарифных недель того же календарного года (as-of: `points` уже доступны к `cutoff`).

    Окно точки — интервал от предыдущей даты до её даты (новогодний пропуск — целиком); если в году нет
    не тарифных точек, берётся медиана последних 52 не тарифных точек до неё; иначе точка не меняется.
    """
    flags = []
    for i, (day, _) in enumerate(points):
        previous = points[i - 1][0] if i else day - timedelta(days=7)
        flags.append(is_tariff_window(previous, day, util, known_until))
    out = []
    for i, (day, value) in enumerate(points):
        if not flags[i]:
            out.append((day, value))
            continue
        same_year = [v for (d, v), flag in zip(points, flags) if not flag and d.year == day.year]
        if not same_year:
            same_year = [v for (d, v), flag in zip(points[:i], flags[:i]) if not flag][-FALLBACK_POINTS:]
        out.append((day, statistics.median(same_year) if same_year else value))
    return out


# ---------------------------------------------------------------------------
# V2: вычитание вклада КУ из полных месяцев
# ---------------------------------------------------------------------------


def v2_adjust_months(months: list, util: dict, weight: float = DEFAULT_W) -> list:
    """`v' = 100 + ((v − 100) − w·u)/(1 − w)` для полных месяцев; неполный остаётся сырым."""
    out = []
    for month in months:
        if month.partial:
            out.append(month)
            continue
        key = month.date.replace(day=1)
        if key not in util:
            raise ValueError(f"нет КУ за {key:%Y-%m} для вычитания (V2)")
        adjusted = 100 + ((month.value - 100) - weight * util[key]) / (1 - weight)
        out.append(wa.MonthPoint(month.date, adjusted, month.as_of, month.partial))
    return out


# ---------------------------------------------------------------------------
# As-of срез по варианту
# ---------------------------------------------------------------------------


def asof_variant_snapshot(
    points: list,
    cutoff: date,
    runner: Callable,
    variant: str,
    util: dict,
    weight: float,
    known_until: date,
    **kwargs,
) -> wa.Snapshot:
    """As-of срез W1 с коррекцией КУ по варианту; `util` — только месяцы, известные к `cutoff`."""
    if variant not in VARIANTS:
        raise ValueError(f"неизвестный вариант {variant!r}: ожидается одно из {VARIANTS}")
    if variant == "V1":
        adjusted = v1_adjust_points(wa.available_points(points, cutoff), cutoff, util, known_until)
        return wa.asof_snapshot(adjusted, cutoff, runner, **kwargs)
    if variant == "V2":
        return wa.asof_snapshot(points, cutoff, runner, month_adjust=lambda months: v2_adjust_months(months, util, weight), **kwargs)
    return wa.asof_snapshot(points, cutoff, runner, **kwargs)
