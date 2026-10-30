#!/usr/bin/env python3
"""As-of перефит недельного X-13 `weekly_cpi_sa` (#401 W1, главная дыра).

Продовая таблица `weekly_cpi_sa` — один полный фит X-13 по всем месяцам
(окончательная редакция): использовать её в бэктесте нельзя, сезонные факторы и
выбросы оценены по будущим данным. Здесь — порт сетки `weeklysa/grid.go` на
Python (`merge_sources` → `build_grid` → `four_week_index` →
`monthly_last_in_month`/`partial_month` → `compute_metrics`) и тот же R-скрипт
`weeklysa/data/weekly_cpi_sa_x13.R` (пин SHA-256), но на данных, доступных к
дате `P_k` публикации месячного ИПЦ Росстата.

Недельная точка доступна к `date + 2 дня` (включительно; календарь
`publication_calendar`). Cutoff origin'а `O` — конвенция `publication` из
`cpi_monthly` (точная дата релиза из `cpi_release_dates`, иначе 9-е число `O`).

Скрипт только читает ClickHouse; в БД не пишет. Запуск — `make
cpi-weekly-sa-asof`, тесты — `make cpi-weekly-sa-asof-test`.
"""

import argparse
import csv
import hashlib
import json
import math
import re
import subprocess
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path
from typing import Callable, NamedTuple, Optional

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

R_SCRIPT = ROOT / "weeklysa" / "data" / "weekly_cpi_sa_x13.R"
# Пин продового скрипта (weeklysa/schema.go `weeklyX13ScriptSHA256`): другой скрипт —
# другая спецификация, parity с таблицей не имеет смысла.
SCRIPT_SHA256 = "29f87c53fe620332dd51c4a17402b2175e85db97ea1932273aab8d187d208b6a"

AGGREGATE_WEEKS = 4  # окно 4-нед. индекса (ровно 28 дней на 7-дневной сетке)
ANNUAL_PERIODS = 13  # окон по 4 недели в году — для SAAR
AVAILABLE_LAG_DAYS = 2  # точка недели доступна в `date + 2` (среда 19:00)
FIRST_DATE = date(2009, 4, 6)
FIRST_MONTH = date(2009, 5, 1)
MIN_MONTHS = 36  # нижняя граница R-скрипта
PUBLICATION_DAY = 9  # правило календаря, если даты релиза нет в таблице
PARITY_TOL = 1e-6
FIRST_ORIGIN = date(2013, 1, 1)

# Критерии gate W1 (спека §4 W1).
QUALITY_RMSE_MAX = 0.20
QUALITY_BIAS_MAX = 0.05
QUALITY_CORR_MIN = 0.95

AGGREGATE_SERIES = "Индекс потребительских цен (недельная оценка)"
BULLETIN_SQL = (
    f"SELECT date, percent FROM rosstat_ipc_weeks_bulletins FINAL WHERE name = '{AGGREGATE_SERIES}' "
    "ORDER BY date FORMAT JSON"
)
BI_SQL = f"SELECT date, value FROM rosstat_bi_cpi_weekly FINAL WHERE series = '{AGGREGATE_SERIES}' ORDER BY date FORMAT JSON"
FINAL_SQL = (
    "SELECT date, sa_index, sa_4w, saar, as_of, partial FROM weekly_cpi_sa FINAL "
    "WHERE series = 'weekly_cpi' ORDER BY date FORMAT JSON"
)


class MonthPoint(NamedTuple):
    date: date  # конец месяца (метка ряда)
    value: float  # 4-нед. индекс, 100 = без изменения
    as_of: date  # последняя неделя сетки внутри месяца
    partial: bool


class Metric(NamedTuple):
    date: date
    sa_index: float
    sa_4w: float
    saar: float
    as_of: date
    partial: bool


class Snapshot(NamedTuple):
    cutoff: date
    last_grid_date: date
    months: list  # полные месяцы `MonthPoint`
    sa_complete: list  # SA-уровни полных месяцев (фит на полных месяцах)
    partial: Optional[MonthPoint]
    sa_partial: Optional[float]  # SA неполного месяца (фит на полных + неполный)


# ---------------------------------------------------------------------------
# Порт weeklysa/grid.go
# ---------------------------------------------------------------------------


def _month_start(day: date) -> date:
    return day.replace(day=1)


def _month_end(day: date) -> date:
    first = _month_start(day)
    nxt = date(first.year + first.month // 12, first.month % 12 + 1, 1)
    return nxt - timedelta(days=1)


def _add_month(first: date) -> date:
    return date(first.year + first.month // 12, first.month % 12 + 1, 1)


def _check_value(value: float, day: date, source: str) -> None:
    if not (value > 0) or math.isinf(value):
        raise ValueError(f"{source}: неположительное или нечисловое значение {value} на {day}")


def merge_sources(bulletin: list, bi: list) -> list:
    """Слияние бюллетеней 2009–2021 и BI 2019+; BI перекрывает бюллетень на той же дате."""
    merged = {}
    for source, points in (("bulletins", bulletin), ("bi", bi)):
        seen = set()
        for day, value in points:
            if day in seen:
                raise ValueError(f"{source}: дубль даты {day}")
            seen.add(day)
            _check_value(value, day, source)
            merged[day] = value
    return sorted(merged.items())


def validate_series(points: list, first_date: date, min_points: int) -> None:
    if len(points) < min_points:
        raise ValueError(f"точек входа {len(points)}, ожидалось не меньше {min_points}")
    if points[0][0] != first_date:
        raise ValueError(f"первая точка {points[0][0]}, ожидалась {first_date}")
    for left, right in zip(points, points[1:]):
        if not right[0] > left[0]:
            raise ValueError(f"даты не возрастают: {right[0]} после {left[0]}")


def build_grid(points: list) -> list:
    """Регулярная 7-дневная сетка кумулятивного лог-уровня (research #348, `regular_grid`)."""
    if len(points) < 2:
        raise ValueError(f"для 7-дневной сетки нужно не меньше двух точек, получено {len(points)}")
    cumulative = [0.0]
    for index in range(1, len(points)):
        cumulative.append(cumulative[-1] + math.log(points[index][1] / 100))
    last = points[-1][0]
    grid, cursor, index = [], points[0][0], 0
    while cursor <= last:
        while index + 1 < len(points) and points[index + 1][0] <= cursor:
            index += 1
        if cursor == points[index][0]:
            level = cumulative[index]
        elif index + 1 < len(points):
            span = (points[index + 1][0] - points[index][0]).days
            offset = (cursor - points[index][0]).days
            level = cumulative[index] + (cumulative[index + 1] - cumulative[index]) * offset / span
        else:
            level = cumulative[index]
        grid.append((cursor, level))
        cursor += timedelta(days=7)
    return grid


def four_week_index(grid: list) -> list:
    """4-нед. индекс на сетке: `exp(L_t − L_{t−4}) × 100`, ровно 28 дней."""
    if len(grid) <= AGGREGATE_WEEKS:
        raise ValueError(f"сетка из {len(grid)} точек короче окна {AGGREGATE_WEEKS} недель")
    return [
        (grid[i][0], math.exp(grid[i][1] - grid[i - AGGREGATE_WEEKS][1]) * 100)
        for i in range(AGGREGATE_WEEKS, len(grid))
    ]


def monthly_last_in_month(index: list, last_grid_date: date, first_month: date, min_months: int) -> list:
    """Последняя точка каждого месяца на конец месяца; неполный последний месяц не включается."""
    if not index:
        raise ValueError("пустой ряд 4-нед. индекса")
    groups = []
    for day, value in index:
        month = _month_start(day)
        if not groups or groups[-1][0] != month:
            groups.append([month, value, day])
        else:
            groups[-1][1], groups[-1][2] = value, day
    for left, right in zip(groups, groups[1:]):
        if right[0] != _add_month(left[0]):
            raise ValueError(f"пропуск месяца после {left[0]:%Y-%m}: следующая точка {right[0]:%Y-%m}")
    if groups[0][0] != _month_start(first_month):
        raise ValueError(f"первый месяц {groups[0][0]:%Y-%m}, ожидался {first_month:%Y-%m}")
    result = []
    for month, value, as_of in groups:
        end = _month_end(month)
        if last_grid_date < end:
            continue
        result.append(MonthPoint(end, value, as_of, False))
    if len(result) < min_months:
        raise ValueError(f"полных месяцев {len(result)}, ожидалось не меньше {min_months} (последняя дата сетки {last_grid_date})")
    return result


def partial_month(index: list, last_grid_date: date) -> Optional[MonthPoint]:
    """Текущий неполный месяц (nowcast-строка) или None, если последний месяц полный."""
    if not index:
        raise ValueError("пустой ряд 4-нед. индекса")
    day, value = index[-1]
    end = _month_end(_month_start(day))
    if last_grid_date >= end:
        return None
    return MonthPoint(end, value, day, True)


def compute_metrics(months: list, sa: list) -> list:
    """`sa_index`, `sa_4w = (sa/100 − 1)·100`, `saar = ((sa/100)^13 − 1)·100`; `sa` — `[(дата, значение)]`."""
    if len(months) != len(sa):
        raise ValueError(f"{len(sa)} SA-точек против {len(months)} месяцев")
    out = []
    for month, (day, value) in zip(months, sa):
        if day != month.date:
            raise ValueError(f"дата SA {day:%Y-%m}, ожидалась {month.date:%Y-%m}")
        if not (value > 0) or math.isinf(value):
            raise ValueError(f"неположительный или нечисловой SA {value} на {day:%Y-%m}")
        out.append(Metric(month.date, value, (value / 100 - 1) * 100, ((value / 100) ** ANNUAL_PERIODS - 1) * 100, month.as_of, month.partial))
    return out


# ---------------------------------------------------------------------------
# As-of слой
# ---------------------------------------------------------------------------


def available_points(points: list, cutoff: date) -> list:
    """Недельные точки, доступные к `cutoff`: `date + 2 дня ≤ cutoff` (включительно)."""
    limit = cutoff - timedelta(days=AVAILABLE_LAG_DAYS)
    return [point for point in points if point[0] <= limit]


def publication_cutoff(origin: date, release_dates: Optional[dict]) -> date:
    """Cutoff origin'а `O` — конвенция `publication`: день релиза ИПЦ `O−1` (таблица) или 9-е число `O`.

    Совпадает с `cpi_monthly.cutoff_for` под `asof_convention("publication", release_dates=…)`
    (проверяется тестом); дублируется здесь, чтобы не тянуть зависимости моделей.
    """
    first = _month_start(origin)
    previous = date(first.year - (first.month == 1), 12 if first.month == 1 else first.month - 1, 1)
    if release_dates and previous in release_dates:
        return release_dates[previous]
    return first.replace(day=PUBLICATION_DAY)


def asof_snapshot(
    points: list,
    cutoff: date,
    runner: Callable,
    first_month: date = FIRST_MONTH,
    min_months: int = MIN_MONTHS,
    month_adjust: Optional[Callable] = None,
) -> Snapshot:
    """Перефит на данных, доступных к `cutoff`; `runner(months) → список SA-уровней`.

    `month_adjust(полные месяцы) → месяцы` — хук коррекции входа (W2, вариант `V2`):
    применяется только к полным месяцам, неполный месяц остаётся сырым; `Snapshot.months`
    хранит исходные (нескорректированные) точки.
    """
    avail = available_points(points, cutoff)
    if len(avail) < 2:
        raise ValueError(f"к {cutoff} доступно {len(avail)} недельных точек")
    for left, right in zip(avail, avail[1:]):
        if not right[0] > left[0]:
            raise ValueError(f"даты не возрастают: {right[0]} после {left[0]}")
    grid = build_grid(avail)
    index = four_week_index(grid)
    last = grid[-1][0]
    months = monthly_last_in_month(index, last, first_month, min_months)
    partial = partial_month(index, last)
    fit_months = list(month_adjust(months)) if month_adjust is not None else list(months)
    if len(fit_months) != len(months):
        raise ValueError(f"month_adjust вернул {len(fit_months)} месяцев вместо {len(months)}")
    sa_complete = list(runner(fit_months))
    if len(sa_complete) != len(months):
        raise ValueError(f"runner вернул {len(sa_complete)} значений вместо {len(months)}")
    sa_partial = None
    if partial is not None:
        with_partial = fit_months + [partial]
        out = list(runner(with_partial))
        if len(out) != len(with_partial):
            raise ValueError(f"runner вернул {len(out)} значений вместо {len(with_partial)}")
        sa_partial = out[-1]
    return Snapshot(cutoff, last, months, sa_complete, partial, sa_partial)


def try_snapshot(points: list, cutoff: date, runner: Callable, **kwargs) -> tuple:
    """`(срез, None)` или `(None, причина)`: сбой фита не прячется, а попадает в отчёт."""
    try:
        return asof_snapshot(points, cutoff, runner, **kwargs), None
    except (ValueError, RuntimeError) as error:
        return None, str(error)


def first_releases(ordered: list) -> dict:
    """`{конец месяца: sa_4w}` — значение в первом срезе, где месяц стал полным.

    `ordered` — `[(origin, Snapshot)]` по возрастанию. В первом срезе регистрируется
    только последний полный месяц (остальные — результат фита на «будущем» относительно
    них, а не первый выпуск); дальше — все месяцы, ставшие полными после предыдущего среза.
    """
    out, upto = {}, None
    for _, snap in ordered:
        for month, level in zip(snap.months, snap.sa_complete):
            fresh = month.date == snap.months[-1].date if upto is None else month.date > upto
            if fresh:
                out[month.date] = (level / 100 - 1) * 100
        upto = snap.months[-1].date
    return out


def snapshot_rows(snap: Snapshot) -> list:
    """Строки как в `weekly_cpi_sa`: полные месяцы и (если есть) `partial`."""
    rows = [m._asdict() for m in compute_metrics(snap.months, [(m.date, v) for m, v in zip(snap.months, snap.sa_complete)])]
    if snap.partial is not None:
        rows.extend(m._asdict() for m in compute_metrics([snap.partial], [(snap.partial.date, snap.sa_partial)]))
    return rows


def parity(final_rows: list, snap: Snapshot) -> dict:
    """Сверка с таблицей `weekly_cpi_sa`: даты обязаны совпасть; max |diff| по `sa_index`, `sa_4w`, `saar`."""
    ours = snapshot_rows(snap)
    if [r["date"] for r in ours] != [r["date"] for r in final_rows]:
        raise ValueError(
            f"даты не совпадают: порт {len(ours)} строк ({ours[0]['date']}…{ours[-1]['date']}), "
            f"таблица {len(final_rows)} строк"
        )
    worst = 0.0
    for mine, theirs in zip(ours, final_rows):
        for key in ("sa_index", "sa_4w", "saar"):
            worst = max(worst, abs(mine[key] - theirs[key]))
    return {"n": len(ours), "max_abs_diff": worst}


def decide_w1(parity_max_diff: float, quality: dict) -> dict:
    """Gate W1 (спека §4): parity ≤ 1e-6 и качество as-of ряда на 2016+."""
    if parity_max_diff > PARITY_TOL:
        return {"decision": "stop_parity", "reason": f"parity {parity_max_diff:.3g} > {PARITY_TOL:g}: ошибка порта"}
    ok = (
        quality["rmse"] <= QUALITY_RMSE_MAX
        and abs(quality["bias"]) <= QUALITY_BIAS_MAX
        and quality["corr"] >= QUALITY_CORR_MIN
    )
    if ok:
        return {"decision": "pass", "reason": "parity выполнен, качество as-of входа в пределах порогов"}
    return {"decision": "demote_input", "reason": "качество as-of входа вне порогов: недельный SA понижается до wkagg_* и месячных дамми"}


# ---------------------------------------------------------------------------
# Мост R и загрузка данных
# ---------------------------------------------------------------------------


def script_sha256() -> str:
    return hashlib.sha256(R_SCRIPT.read_bytes()).hexdigest()


def encode_x13_input(months: list) -> str:
    if not months:
        raise ValueError("CSV входа X-13: пустой месячный ряд")
    return "date,level\n" + "".join(f"{m.date.isoformat()},{m.value!r}\n" for m in months)


def make_r_runner() -> Callable:
    """Мост к продовому R-скрипту: CSV `date,level` → CSV `date,sa_level`."""
    if script_sha256() != SCRIPT_SHA256:
        raise RuntimeError(f"{R_SCRIPT}: SHA-256 {script_sha256()} не совпадает с пином {SCRIPT_SHA256}")

    def run(months: list) -> list:
        with tempfile.TemporaryDirectory() as tmp:
            source, target = Path(tmp) / "in.csv", Path(tmp) / "out.csv"
            source.write_text(encode_x13_input(months), encoding="utf-8")
            try:
                subprocess.run(["Rscript", str(R_SCRIPT), str(source), str(target)], check=True, capture_output=True, text=True)
            except subprocess.CalledProcessError as error:
                tail = (error.stderr or "").strip().splitlines()[-3:]
                text = re.sub(r"\S*iofile\.spc", "iofile.spc", " | ".join(tail))  # без случайных путей tmp: детерминизм отчёта
                raise RuntimeError(f"X-13 (R) завершился с кодом {error.returncode}: {text}") from error
            by_date = {}
            for row in csv.DictReader(target.open(encoding="utf-8")):
                by_date[date.fromisoformat(row["date"])] = float(row["sa_level"])
        missing = [m.date for m in months if m.date not in by_date]
        if missing or len(by_date) != len(months):
            raise ValueError(f"X-13: {len(by_date)} точек, ожидалось {len(months)}, нет {missing[:3]}")
        return [by_date[m.date] for m in months]

    return run


def _query(sql: str) -> list:
    from lib.clickhouse import ch_query

    return ch_query(sql)


def load_points() -> list:
    bulletin = [(date.fromisoformat(r["date"]), float(r["percent"])) for r in _query(BULLETIN_SQL)]
    bi = [(date.fromisoformat(r["date"]), float(r["value"])) for r in _query(BI_SQL)]
    if not bulletin or not bi:
        raise ValueError("нет точек агрегата в бюллетенях или BI")
    return merge_sources(bulletin, bi)


def load_final_table() -> list:
    rows = _query(FINAL_SQL)
    return [
        {
            "date": date.fromisoformat(r["date"]),
            "sa_index": float(r["sa_index"]),
            "sa_4w": float(r["sa_4w"]),
            "saar": float(r["saar"]),
            "as_of": date.fromisoformat(r["as_of"]),
            "partial": int(r["partial"]),
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Прогон
# ---------------------------------------------------------------------------


def month_key(day: date) -> date:
    return _month_start(day)


def _stats(values: list) -> dict:
    if not values:
        return {"n": 0}
    n = len(values)
    mean = sum(values) / n
    return {
        "n": n,
        "bias": mean,
        "rmse": math.sqrt(sum(v * v for v in values) / n),
        "mae": sum(abs(v) for v in values) / n,
        "max_abs": max(abs(v) for v in values),
    }


def run(runner: Optional[Callable] = None, release_dates: Optional[dict] = None) -> dict:
    import cpi_release_dates as rd
    import cpi_sa_reconciliation as rec

    runner = runner or make_r_runner()
    release_dates = release_dates if release_dates is not None else rd.load_release_dates()
    points = load_points()
    final_rows = load_final_table()
    final_complete = {r["date"]: r for r in final_rows if not r["partial"]}
    partial_rows = [r for r in final_rows if r["partial"]]

    # 1. Parity: тот же вход, тот же конвейер → таблица `weekly_cpi_sa`.
    last_week = max(r["as_of"] for r in final_rows)
    parity_cutoff = last_week + timedelta(days=AVAILABLE_LAG_DAYS)
    full = asof_snapshot(points, parity_cutoff, runner)
    parity_result = parity(final_rows, full)
    parity_result["cutoff"] = parity_cutoff.isoformat()
    parity_result["n_complete"] = len(full.months)
    parity_result["partial_in_table"] = len(partial_rows)

    # 2. As-of срезы по origin'ам 2013-01 …: cutoff = день релиза ИПЦ `O−1` (конвенция publication).
    last_data = points[-1][0] + timedelta(days=AVAILABLE_LAG_DAYS)
    snapshots, failures, origin = {}, [], FIRST_ORIGIN
    while True:
        cutoff = publication_cutoff(origin, release_dates)
        if cutoff > last_data:
            break
        snap, error = try_snapshot(points, cutoff, runner)
        if snap is not None:
            snapshots[origin] = snap
        else:
            failures.append({"origin": origin.isoformat(), "cutoff": cutoff.isoformat(), "error": error})
        origin = _add_month(origin)

    records = []
    for origin, snap in snapshots.items():
        last = snap.months[-1]
        metric_last = compute_metrics([last], [(last.date, snap.sa_complete[-1])])[0]
        record = {
            "origin": origin.isoformat(),
            "cutoff": snap.cutoff.isoformat(),
            "last_grid_date": snap.last_grid_date.isoformat(),
            "last_complete_month": last.date.isoformat(),
            "sa_4w_last_complete": metric_last.sa_4w,
            "n_complete": len(snap.months),
        }
        if snap.partial is not None:
            metric_partial = compute_metrics([snap.partial], [(snap.partial.date, snap.sa_partial)])[0]
            record.update(partial_month=snap.partial.date.isoformat(), partial_as_of=snap.partial.as_of.isoformat(), sa_4w_partial=metric_partial.sa_4w)
        final_value = final_complete.get(last.date)
        if final_value is not None:
            record["sa_4w_final_last_complete"] = final_value["sa_4w"]
            record["revision_last_complete"] = metric_last.sa_4w - final_value["sa_4w"]
        records.append(record)

    # 3. Ревизии: первое появление месяца (первый срез, где он последний полный) против окончательной редакции;
    #    дрейф значения месяца по последующим срезам (лаги 1…12 срезов).
    ordered = list(snapshots.items())
    first_release, by_lag = first_releases(ordered), {lag: [] for lag in range(1, 13)}
    for position, (origin, snap) in enumerate(ordered):
        sa4w = {m.date: (v / 100 - 1) * 100 for m, v in zip(snap.months, snap.sa_complete)}
        for lag in range(1, 13):
            if position - lag < 0:
                continue
            earlier_snap = ordered[position - lag][1]
            earlier_last = earlier_snap.months[-1].date
            final_value = final_complete.get(earlier_last)
            if final_value is not None and earlier_last in sa4w:
                by_lag[lag].append(sa4w[earlier_last] - final_value["sa_4w"])
    revisions = {}
    for lo, hi, label in [(date(2013, 1, 1), date(2015, 12, 31), "2013_2015"), (date(2016, 1, 1), None, "2016"), (date(2023, 1, 1), None, "2023")]:
        values = [
            first_release[m] - final_complete[m]["sa_4w"]
            for m in sorted(first_release)
            if m in final_complete and m >= lo and (hi is None or m <= hi)
        ]
        revisions[label] = _stats(values)
    revisions["by_year"] = {}
    for m in sorted(first_release):
        if m in final_complete and m.year >= 2013:
            revisions["by_year"].setdefault(str(m.year), []).append(first_release[m] - final_complete[m]["sa_4w"])
    revisions["by_year"] = {y: _stats(v) for y, v in revisions["by_year"].items()}
    revisions["drift_vs_final_by_lag"] = {str(lag): _stats(v) for lag, v in by_lag.items()}

    # 4. Качество as-of ряда: первое появление месяца против месячного X-13 (#399); окно 2016+.
    nsa = rec.load_nsa()
    grid = [m for m in sorted(nsa) if m >= date(2002, 1, 1)]
    monthly = rec.x13_series(grid, nsa, rec._first_rt(grid), rec.make_r_runner(grid[0]))
    asof_series = {month_key(m): v for m, v in first_release.items()}
    quality = {}
    for label, (lo, hi) in {"2016": (date(2016, 1, 1), None), "2023": (date(2023, 1, 1), None), "2013_2015": (date(2013, 1, 1), date(2015, 12, 1))}.items():
        for ref_name, ref in (("x13_rt", monthly["rt"]), ("x13_full", monthly["full"])):
            try:
                quality[f"asof_vs_{ref_name}_{label}"] = rec.metrics(asof_series, ref, lo, hi)
            except ValueError:
                continue
    final_series = {month_key(m): r["sa_4w"] for m, r in final_complete.items()}
    for label, (lo, hi) in {"2016": (date(2016, 1, 1), None), "2013_2015": (date(2013, 1, 1), date(2015, 12, 1))}.items():
        try:
            quality[f"final_vs_x13_rt_{label}"] = rec.metrics(final_series, monthly["rt"], lo, hi)
        except ValueError:
            continue

    # 5. Информативность nowcast-значения: sa_4w неполного месяца против окончательного значения его конца.
    partial_vs_final = []
    for origin, snap in snapshots.items():
        if snap.partial is None:
            continue
        end_value = final_complete.get(snap.partial.date)
        if end_value is not None:
            partial_vs_final.append((snap.partial.date, (snap.sa_partial / 100 - 1) * 100 - end_value["sa_4w"]))
    partial_stats = {
        label: _stats([v for m, v in partial_vs_final if m >= lo and (hi is None or m <= hi)])
        for label, (lo, hi) in {"2016": (date(2016, 1, 1), None), "2013_2015": (date(2013, 1, 1), date(2015, 12, 31))}.items()
    }

    decision = decide_w1(parity_result["max_abs_diff"], quality["asof_vs_x13_rt_2016"])
    return {
        "script_sha256": script_sha256(),
        "parity": parity_result,
        "n_origins": len(snapshots),
        "failed_origins": failures,
        "first_origin": min(snapshots).isoformat() if snapshots else None,
        "last_origin": max(snapshots).isoformat() if snapshots else None,
        "revisions": revisions,
        "quality": quality,
        "partial_vs_final_month": partial_stats,
        "decision": decision,
        "records": records,
    }


def _jsonable(value):
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    if isinstance(value, float):
        return None if not math.isfinite(value) else round(value, 6)
    return value


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out-dir", type=Path, default=ROOT / "build" / "cpi-two-stage")
    args = parser.parse_args(argv)
    result = run()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    records = result.pop("records")
    (args.out_dir / "weekly_sa_asof.json").write_text(
        json.dumps(_jsonable(result), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    columns = sorted({key for record in records for key in record}, key=lambda k: (k != "origin", k))
    with (args.out_dir / "weekly_sa_asof.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(_jsonable(records))
    print(json.dumps(_jsonable({"parity": result["parity"], "decision": result["decision"], "n_origins": result["n_origins"]}), ensure_ascii=False, indent=2))
    return 0 if result["decision"]["decision"] != "stop_parity" else 2


if __name__ == "__main__":
    sys.exit(main())
