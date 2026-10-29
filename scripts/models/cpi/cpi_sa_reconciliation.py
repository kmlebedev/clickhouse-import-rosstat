#!/usr/bin/env python3
"""Сверка SA и core из сырого ИПЦ и недельного SA с ЦБ (#399).

Пункт `key-rate/phase-4-cpi-sa-reconciliation`, роль research. Спека
`docs/superpowers/specs/2026-10-29-cpi-sa-reconciliation-design.md`.

Q1 — наш SA из `rosstat_ipc_mes` (STL и X-13; as-of и вся выборка) против SA и
«Базового ИПЦ» ЦБ (`cbr_indicators_cpd`); Q2 — ИПЦ без КУ из реальных тарифов
(`ipc31074/Коммунальные услуги`) против прокси `T_SA`; Q3 — недельный SA
`weekly_cpi_sa` против месячного X-13. Только чтение ClickHouse; в БД не пишет.
X-13 — R `seasonal` (мост `run_r_x13`, подменяется в тестах). Запуск —
`make cpi-sa-reconciliation`, тесты — `make cpi-sa-reconciliation-test`.
"""

import argparse
import csv
import json
import math
import subprocess
import sys
import tempfile
from datetime import date
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

R_SCRIPT = Path(__file__).resolve().parent / "cpi_sa_reconciliation_x13.R"
FIRST_RT_MONTH = date(2015, 1, 1)
UTILITIES_WEIGHTS = (0.08, 0.10, 0.12)
UTILITIES_SERIES = "ipc31074/Коммунальные услуги"
WINDOWS = {
    "2016": (date(2016, 1, 1), None),
    "2023": (date(2023, 1, 1), None),
    "2016_2019": (date(2016, 1, 1), date(2019, 12, 1)),
    "2010": (date(2010, 1, 1), date(2015, 12, 1)),
}

# Пороги решений — спека §5.
RMSE_SA_MAX = 0.15
CORR_SA_MIN = 0.95
PROXY_REPLACE_RMSE = 0.10
WEEKLY_RMSE_MAX = 0.20
WEEKLY_BIAS_MAX = 0.05
WEEKLY_CORR_MIN = 0.95


def level_from_mm(grid: list, mm: dict) -> list:
    """Накопленный уровень индекса из м/м, %; месяцы обязаны идти подряд."""
    for left, right in zip(grid, grid[1:]):
        if (right.year * 12 + right.month) - (left.year * 12 + left.month) != 1:
            raise ValueError(f"разрыв месяцев между {left} и {right}")
    level, out = 1.0, []
    for month in grid:
        if month not in mm:
            raise ValueError(f"нет значения за {month}")
        level *= 1 + mm[month] / 100
        out.append(level)
    return out


def ex_utilities_mm(nsa: dict, util: dict, weight: float) -> dict:
    """ИПЦ без КУ: `(π − w·u)/(1 − w)`, м/м, п.п."""
    if not 0 < weight < 1:
        raise ValueError(f"вес КУ {weight} вне (0, 1)")
    if set(nsa) != set(util):
        raise ValueError("месяцы ИПЦ и КУ не совпадают")
    return {month: (nsa[month] - weight * util[month]) / (1 - weight) for month in nsa}


def metrics(ours: dict, ref: dict, lo: date, hi: date | None = None) -> dict:
    """RMSE/MAE/bias (ours − ref), корреляция, наклон и RMSE после калибровки."""
    keys = [
        month
        for month in sorted(ours)
        if month in ref
        and month >= lo
        and (hi is None or month <= hi)
        and math.isfinite(ours[month])
        and math.isfinite(ref[month])
    ]
    if len(keys) < 3:
        raise ValueError(f"мало точек для метрик: {len(keys)}")
    x = np.array([ours[m] for m in keys], dtype=float)
    y = np.array([ref[m] for m in keys], dtype=float)
    diff = x - y
    design = np.vstack([x, np.ones(len(x))]).T
    slope, intercept = np.linalg.lstsq(design, y, rcond=None)[0]
    resid = y - (slope * x + intercept)
    corr = float(np.corrcoef(x, y)[0, 1]) if x.std() > 0 and y.std() > 0 else float("nan")
    return {
        "n": len(keys),
        "rmse": float(np.sqrt((diff**2).mean())),
        "mae": float(np.abs(diff).mean()),
        "bias": float(diff.mean()),
        "corr": corr,
        "slope": float(slope),
        "rmse_cal": float(np.sqrt((resid**2).mean())),
    }


def decisions(results: dict) -> dict:
    """Решения по таблице §5 спеки; вход — метрики сверки."""
    x13 = results["x13_rt_vs_cbr_2016"]
    stl = results["stl_rt_vs_cbr_2016"]
    nsa = results["nsa_vs_cbr_2016"]
    weekly = results["weekly_vs_x13_2016"]
    early = results["weekly_vs_x13_2010"]

    def weekly_pass(item: dict) -> bool:
        return (
            item["rmse"] <= WEEKLY_RMSE_MAX
            and abs(item["bias"]) <= WEEKLY_BIAS_MAX
            and item["corr"] >= WEEKLY_CORR_MIN
        )

    return {
        "x13_ok": x13["rmse"] <= RMSE_SA_MAX and x13["corr"] >= CORR_SA_MIN,
        "stl_ok": stl["rmse"] <= RMSE_SA_MAX and stl["rmse"] < nsa["rmse"],
        "core_not_from_headline": results["best_vs_core_2016"]["rmse"] > RMSE_SA_MAX,
        "replace_proxy": any(
            item["rmse"] >= PROXY_REPLACE_RMSE for item in results["exku_vs_proxy_2023"].values()
        ),
        "weekly_ok_2016": weekly_pass(weekly),
        "weekly_nan_before_2016": not weekly_pass(early),
    }


def x13_series(grid: list, mm: dict, first_rt: int, runner) -> dict:
    """SA м/м X-13: `full` (вся выборка) и `rt` (as-of, расширяющееся окно).

    `runner(levels, first_rt)` возвращает по месяцу пару `(full, rt)`;
    `first_rt` — 1-based номер первого месяца as-of.
    """
    levels = level_from_mm(grid, mm)
    rows = runner(levels, first_rt)
    if len(rows) != len(grid):
        raise ValueError(f"runner вернул {len(rows)} строк вместо {len(grid)}")
    return {
        "full": {month: float(row[0]) for month, row in zip(grid, rows)},
        "rt": {month: float(row[1]) for month, row in zip(grid, rows)},
    }


def make_r_runner(start: date):
    """Мост к R `seasonal`: CSV уровня → CSV (`full`, `rt`), NA → NaN."""

    def run(levels: list, first_rt: int) -> list:
        with tempfile.TemporaryDirectory() as tmp:
            source, target = Path(tmp) / "level.csv", Path(tmp) / "sa.csv"
            year, month, lines = start.year, start.month, ["month,level"]
            for value in levels:
                lines.append(f"{year}-{month:02d}-01,{value!r}")
                year, month = (year + 1, 1) if month == 12 else (year, month + 1)
            source.write_text("\n".join(lines) + "\n", encoding="utf-8")
            subprocess.run(
                ["Rscript", str(R_SCRIPT), str(source), str(first_rt), str(target)],
                check=True,
                capture_output=True,
                text=True,
            )
            out = []
            for row in csv.DictReader(target.open(encoding="utf-8")):
                out.append(tuple(float("nan") if row[k] == "NA" else float(row[k]) for k in ("full", "rt")))
            return out

    return run


def stl_series(grid: list, mm: dict) -> dict:
    """STL (`cpi_pipeline_sa`): `full` — вся выборка, `rt` — as-of по каждому месяцу с 2015-01."""
    import cpi_pipeline_sa as sa

    series = [(month, mm[month]) for month in grid]
    full = dict(sa.sa_series(series, grid[-1]))
    rt = {}
    for month in grid:
        if month < FIRST_RT_MONTH:
            continue
        factors = sa.seasonal_factors(series, month)
        rt[month] = mm[month] - factors[month.month] if factors else float("nan")
    return {"full": full, "rt": rt}


# ---------------------------------------------------------------------------
# Загрузка (только чтение ClickHouse)
# ---------------------------------------------------------------------------

NSA_NAME = "Индексы потребительских цен на товары и услуги по Российской Федерации"
CBR_SA_NAME = "Все товары и услуги"
CBR_CORE_NAME = "Базовый ИПЦ"


def _month(text: str) -> date:
    return date.fromisoformat(text).replace(day=1)


def _query(sql: str) -> list:
    from lib.clickhouse import ch_query

    return ch_query(sql)


def load_nsa() -> dict:
    rows = _query(
        f"SELECT toStartOfMonth(date) AS m, argMax(toFloat64(percent), date) AS v "
        f"FROM rosstat_ipc_mes FINAL WHERE name = '{NSA_NAME}' GROUP BY m ORDER BY m FORMAT JSON"
    )
    return {_month(r["m"]): float(r["v"]) - 100.0 for r in rows}


def _cbr(name: str) -> dict:
    rows = _query(
        f"SELECT toStartOfMonth(date) AS m, argMax(toFloat64(value), date) AS v "
        f"FROM cbr_indicators_cpd FINAL WHERE name = '{name}' GROUP BY m ORDER BY m FORMAT JSON"
    )
    return {_month(r["m"]): float(r["v"]) for r in rows}


def load_cbr_sa() -> dict:
    return _cbr(CBR_SA_NAME)


def load_cbr_core() -> dict:
    return _cbr(CBR_CORE_NAME)


def load_utilities() -> dict:
    rows = _query(
        f"SELECT date, value FROM v_fedstat_ipc WHERE series = '{UTILITIES_SERIES}' ORDER BY date FORMAT JSON"
    )
    return {_month(r["date"]): float(r["value"]) - 100.0 for r in rows}


def load_weekly_sa4w() -> dict:
    rows = _query(
        "SELECT toStartOfMonth(date) AS m, argMax(value, date) AS v FROM v_weekly_cpi_sa "
        "WHERE series = 'weekly_cpi:sa_4w' AND partial = 0 GROUP BY m ORDER BY m FORMAT JSON"
    )
    return {_month(r["m"]): float(r["v"]) for r in rows}


# ---------------------------------------------------------------------------
# Прогон
# ---------------------------------------------------------------------------


def _first_rt(grid: list) -> int:
    return next(i for i, month in enumerate(grid) if month >= FIRST_RT_MONTH) + 1


def run(runner=None) -> dict:
    """Все сверки; возвращает метрики и решения."""
    from models.cb_comp import cb_comp_proxy as proxy

    nsa, cbr, core = load_nsa(), load_cbr_sa(), load_cbr_core()
    util, weekly = load_utilities(), load_weekly_sa4w()
    grid = [month for month in sorted(nsa) if month >= date(2002, 1, 1)]
    out = {"sources": {"nsa": len(nsa), "cbr_sa": len(cbr), "cbr_core": len(core), "util": len(util), "weekly": len(weekly)}}

    x13 = x13_series(grid, nsa, _first_rt(grid), runner or make_r_runner(grid[0]))
    stl = stl_series(grid, nsa)
    variants = {"nsa": nsa, "stl_full": stl["full"], "stl_rt": stl["rt"], "x13_full": x13["full"], "x13_rt": x13["rt"]}

    results = {}
    for window, (lo, hi) in WINDOWS.items():
        for name, series in variants.items():
            for ref_name, ref in (("cbr", cbr), ("core", core)):
                try:
                    results[f"{name}_vs_{ref_name}_{window}"] = metrics(series, ref, lo, hi)
                except ValueError:
                    continue
    results["best_vs_core_2016"] = min(
        (results[f"{name}_vs_core_2016"] for name in variants if f"{name}_vs_core_2016" in results),
        key=lambda item: item["rmse"],
    )

    exku, util_grid = {}, [month for month in grid if month in util]
    for weight in UTILITIES_WEIGHTS:
        ex = ex_utilities_mm({m: nsa[m] for m in util_grid}, {m: util[m] for m in util_grid}, weight)
        series = x13_series(util_grid, ex, 10**6, runner or make_r_runner(util_grid[0]))
        prox = dict(proxy.proxy_target(cbr, weight, proxy.DEFAULT_UTILITIES_GROWTH))
        exku[weight] = {w: metrics(series["full"], prox, WINDOWS[w][0], WINDOWS[w][1]) for w in ("2016", "2023")}
    results["exku_all"] = {str(w): v for w, v in exku.items()}
    results["exku_vs_proxy_2023"] = {w: v["2023"] for w, v in exku.items()}

    for window in WINDOWS:
        lo, hi = WINDOWS[window]
        for name, ref in (("x13", x13["full"]), ("cbr", cbr), ("core", core), ("nsa", nsa)):
            try:
                results[f"weekly_vs_{name}_{window}"] = metrics(weekly, ref, lo, hi)
            except ValueError:
                continue
    results["weekly_bias_by_year"] = _bias_by_year(weekly, x13["full"])
    out["metrics"] = results
    out["decisions"] = decisions(results)
    return out


def _bias_by_year(weekly: dict, ref: dict) -> dict:
    years = {}
    for month in sorted(weekly):
        if month in ref and math.isfinite(ref[month]):
            years.setdefault(month.year, []).append(weekly[month] - ref[month])
    return {
        str(year): {"n": len(v), "bias": float(np.mean(v)), "rmse": float(np.sqrt(np.mean(np.square(v))))}
        for year, v in years.items()
    }


def _jsonable(value):
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, float):
        return round(value, 4)
    return value


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=ROOT / "build" / "cpi-sa" / "reconciliation.json")
    args = parser.parse_args(argv)
    report = _jsonable(run())
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"decisions": report["decisions"], "sources": report["sources"]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
