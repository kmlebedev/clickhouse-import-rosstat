#!/usr/bin/env python3
"""CPI monthly v3 public: компонентный nowcast ИПЦ на публичных данных.

Task 4 пункта `key-rate/phase-4-ml-components` (issue #237, родитель #76),
спека `docs/superpowers/specs/2026-10-19-cpi-v3-public-design.md`.

Идея: собрать production-v3 на публичных данных — 4 официальных компонента
`rosstat_ipc_mes`, недельные diffusion-признаки Stage 1, отраслевые МП из
16 листов публичной книги ЦБ и макро-контроли; без 87 внутренних кодов
ОКВЭД2. Отраслевые МП читаются СРАЗУ из ClickHouse: 48 SA-рядов
`v_cbr_survey` (`industry_<slug>_<indicator>_sa`), импортированных в #239
(Task 6). Официальный прогон XLSX не читает; `--verify-xlsx` — разовая
сверка источника.

Горизонты: h1 — месячный `y = percent − 100`; h2/h3/h6 — накопленный
прирост за h месяцев, origin `M−(h−1)`, cutoff(origin). Для h>1 train-origin
`j` допускается только при `j+h−1 < i` (purge), поэтому
`start_eff = start+h−1`, `embargo_eff = embargo+h−1`.

Выборки (предрегистрация спеки §4):
* `weekly` 2022-01…2026-08, start 24 — primary, h1/h2/h3/h6;
* `weekly` h1, start 24, embargo 1 — robustness;
* `long` 2013-10…2026-08, h1: start 48 (гейт Task 2) и start 24
  (robustness окна).

Скрипт только читает ClickHouse; в БД данные не пишет. `ML_LOG=1` пишет
паспорт и fold-предсказания в `ml_runs`/`ml_predictions` (и MLflow).
Запуск — `make cpi-monthly-v3-public`, тесты —
`scripts/models/cpi/cpi_monthly_v3_public_test.py`.
"""

import argparse
import dataclasses
import json
import math
import os
import sys
from datetime import date
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))

from models.cpi import cpi_monthly as cm
from models.cpi import cpi_monthly_industry as ci
from models.cpi import cpi_monthly_v3 as cmv3
from lib import eval_utils as eu
from lib import ml_experiment as mlx
from lib.clickhouse import ch_query

SEED = cm.SEED
SAMPLES = ("weekly", "long")
DEFAULT_START = {"weekly": 24, "long": 48}
DEFAULT_HORIZONS = {"weekly": (1, 2, 3, 6), "long": (1,)}
BOOTSTRAP_BLOCK = 3
BOOTSTRAP_N = 2000
NAN = float("nan")

# ---------------------------------------------------------------------------
# Источник #239: CH-slug → лист Task 2, CH-индикатор → ключ Task 2
# ---------------------------------------------------------------------------

CH_INDUSTRY_SLUGS = {
    "Промышленность": "total",
    "Добыча": "mining",
    "Обработка": "manufacturing",
    "Обработка_потребительские": "manufacturing_consumer",
    "Обработка_инвестиционные": "manufacturing_investment",
    "Обработка_промежуточные": "manufacturing_intermediate",
    "ЭЭГП": "electricity",
    "Водоснабжение": "water",
    "Сельское хозяйство": "agriculture",
    "Строительство": "construction",
    "Торговля (45+46+47)": "trade",
    "Торговля авто (45)": "trade_auto",
    "Торговля оптовая (46)": "trade_wholesale",
    "Торговля розничная (47)": "trade_retail",
    "Транспортировка и хранение": "transport",
    "Услуги": "services",
}

CH_INDICATORS = {
    "ibk": "ibk",
    "price_exp": "price_expectations",
    "costs": "cost_change",
}


def ch_series_name(sheet: str, key: str) -> str:
    """Имя ряда `v_cbr_survey` для листа и индикатора Task 2."""
    return f"industry_{CH_INDUSTRY_SLUGS[sheet]}_{CH_INDICATORS[key]}_sa"


CH_INDUSTRY_SERIES = {
    ch_series_name(sheet, key): (sheet, key)
    for sheet in ci.INDUSTRY_SHEETS
    for key in ci.INDICATOR_KEYS
}

INDUSTRY_SQL = """
SELECT series, date, value
FROM v_cbr_survey
WHERE source = 'cbr_survey' AND series IN (%s)
ORDER BY series, date
FORMAT JSON
""" % ", ".join(f"'{name}'" for name in sorted(CH_INDUSTRY_SERIES))

EXPECTED_INDUSTRY_SERIES = 48
EXPECTED_INDUSTRY_POINTS = 14_256
EXPECTED_INDUSTRY_FIRST = date(2002, 1, 1)
EXPECTED_INDUSTRY_LAST = date(2026, 9, 1)

STAGE1_REFERENCE = {
    ("weekly", 24, 0): {"v2_bottomup": 0.362300, "v3_bottomup": 0.311487},
    ("long", 48, 0): {"v2_bottomup": 0.691019, "v3_bottomup": 0.643146},
}
TASK2_REFERENCE = {
    ("weekly", 24, 0): {
        "v3_ind_bottomup": 0.354359,
        "ind_only_bottomup": 0.384958,
    },
    ("long", 48, 0): {"v3_ind_bottomup": 0.642664},
}
REGRESSION_TOLERANCE = 1e-6

FULL_MODELS = (
    "v2_baseline",
    "v2_bottomup",
    "v3_direct",
    "v3_bottomup",
    "ind_direct",
    "ind_bottomup",
    "ind_only_bottomup",
    "v3_ind_direct",
    "v3_ind_bottomup",
    "v3_ind_all_bottomup",
    "B1",
    "B2",
    "B3",
    "B3_weekly",
    "B3_ind",
)
SECONDARY_MODELS = (
    "v2_baseline",
    "v2_bottomup",
    "v3_direct",
    "v3_bottomup",
    "v3_ind_direct",
    "v3_ind_bottomup",
    "B1",
    "B3",
    "B3_weekly",
)
LONG_MODELS = (
    "v2_baseline",
    "v2_bottomup",
    "v3_bottomup",
    "v3_ind_bottomup",
    "v3_ind_direct",
    "B1",
)
LONG_WINDOW_MODELS = ("v2_bottomup", "v3_bottomup", "v3_ind_bottomup", "B1")

# `ci.MODELS` — предрегистрация Task 2; B3 в Stage 1 — `cm.fit_enet_mp`
# (база+МП), а не `fit_enet_base`. Остальные модели — как в Task 2.
MODELS = dict(ci.MODELS)
MODELS["B3"] = cm.fit_enet_mp

PRIMARY_BASELINES = ("v2_bottomup", "B1", "B2", "B3_weekly")
PRIMARY_CANDIDATES = ("v3_ind_bottomup", "v3_bottomup")
INDUSTRY_INCREMENT = ("v3_ind_bottomup", "v3_bottomup")


# ---------------------------------------------------------------------------
# Данные #239
# ---------------------------------------------------------------------------


def row_date(value) -> date:
    return cm.row_date(value)


def row_float(value) -> float:
    return float(value)


def validate_panel(panel: dict, expected_points: int = EXPECTED_INDUSTRY_POINTS) -> dict:
    """Проверить 16 листов × 3 индикатора и вернуть метаданные панели.

    Отсутствующий ряд, дубль даты или NaN — ошибка: молчаливый NaN сместил бы
    информационное множество. `expected_points` — предрегистрированный объём
    (14 256); новая волна источника требует обновления спеки, а не тихого
    расширения выборки.
    """
    missing = []
    duplicate = []
    non_finite = []
    date_sets = {}
    counts = {}
    for sheet in ci.INDUSTRY_SHEETS:
        for key in ci.INDICATOR_KEYS:
            points = sorted(panel.get(sheet, {}).get(key, []))
            if not points:
                missing.append(f"{sheet}/{key}")
                continue
            dates = [day for day, _ in points]
            values = [value for _, value in points]
            if len(dates) != len(set(dates)):
                duplicate.append(f"{sheet}/{key}")
            if any(not math.isfinite(float(value)) for value in values):
                non_finite.append(f"{sheet}/{key}")
            date_sets[f"{sheet}/{key}"] = set(dates)
            counts[f"{sheet}/{key}"] = len(points)
    if missing:
        raise SystemExit(f"industry: нет рядов {missing}")
    if duplicate:
        raise SystemExit(f"industry: дубли дат {duplicate}")
    if non_finite:
        raise SystemExit(f"industry: нечисловые значения {non_finite}")
    distinct_date_sets = {frozenset(dates) for dates in date_sets.values()}
    if len(distinct_date_sets) != 1:
        raise SystemExit("industry: у 48 рядов разные наборы дат")
    dates = sorted(next(iter(distinct_date_sets)))
    total = sum(counts.values())
    if total != expected_points:
        raise SystemExit(
            f"industry: точек {total}, ожидалось {expected_points}; "
            "источник изменился — обновите спеку/ожидаемый объём"
        )
    if dates[0] != EXPECTED_INDUSTRY_FIRST or dates[-1] != EXPECTED_INDUSTRY_LAST:
        raise SystemExit(
            f"industry: диапазон {dates[0]}…{dates[-1]}, ожидалось "
            f"{EXPECTED_INDUSTRY_FIRST}…{EXPECTED_INDUSTRY_LAST}"
        )
    return {
        "n_series": len(counts),
        "n_points": total,
        "n_dates": len(dates),
        "first": dates[0].isoformat(),
        "last": dates[-1].isoformat(),
        "series": sorted(CH_INDUSTRY_SERIES),
    }


def panel_from_rows(rows: list) -> dict:
    """`{sheet: {key: [(date, value)]}}` из строк `v_cbr_survey`."""
    panel = {
        sheet: {key: [] for key in ci.INDICATOR_KEYS}
        for sheet in ci.INDUSTRY_SHEETS
    }
    unknown = []
    for row in rows:
        series = str(row["series"])
        target = CH_INDUSTRY_SERIES.get(series)
        if target is None:
            unknown.append(series)
            continue
        sheet, key = target
        panel[sheet][key].append(
            (row_date(row["date"]), row_float(row["value"]))
        )
    if unknown:
        raise SystemExit(f"industry: неизвестные ряды {sorted(set(unknown))}")
    for sheet in panel:
        for key in panel[sheet]:
            panel[sheet][key].sort()
    return panel


def load_industry_panel_ch() -> tuple:
    """Панель industry из `v_cbr_survey` (#239) и её метаданные."""
    panel = panel_from_rows(ch_query(INDUSTRY_SQL))
    meta = validate_panel(panel)
    return panel, meta


def panel_from_xlsx(path) -> dict:
    """Панель из публичного XLSX — только для `--verify-xlsx`."""
    return ci.load_industry_panel(Path(path))


def verify_panels(panel_ch: dict, panel_xlsx: dict) -> dict:
    """Поточечная сверка CH ↔ XLSX; extra-даты CH — новая волна, не ошибка."""
    missing = []
    mismatched = []
    extra = []
    common_n = 0
    max_diff = 0.0
    for sheet in ci.INDUSTRY_SHEETS:
        for key in ci.INDICATOR_KEYS:
            ch_points = dict(panel_ch.get(sheet, {}).get(key, []))
            xlsx_points = dict(panel_xlsx.get(sheet, {}).get(key, []))
            for day, xlsx_value in xlsx_points.items():
                if day not in ch_points:
                    missing.append(f"{sheet}/{key}/{day}")
                    continue
                common_n += 1
                diff = abs(float(ch_points[day]) - float(xlsx_value))
                max_diff = max(max_diff, diff)
                if diff > REGRESSION_TOLERANCE:
                    mismatched.append(f"{sheet}/{key}/{day}")
            extra.extend(
                f"{sheet}/{key}/{day}"
                for day in ch_points
                if day not in xlsx_points
            )
    return {
        "common_points": common_n,
        "max_abs_diff": max_diff,
        "missing_in_ch": missing,
        "mismatched": mismatched,
        "extra_in_ch": extra,
        "ok": not missing and not mismatched,
    }


def panel_hash(panel: dict) -> str:
    """sha256 канонической панели (для паспорта и отчёта)."""
    payload = {
        sheet: {
            key: [[day.isoformat(), value] for day, value in sorted(points)]
            for key, points in data.items()
        }
        for sheet, data in panel.items()
    }
    return mlx.sha256_text(mlx.canonical_json(payload))


# ---------------------------------------------------------------------------
# Выборка и horizon-таргеты
# ---------------------------------------------------------------------------


def build_sample_public(sample: str, panel: dict) -> tuple:
    """Строки Stage 1 + 96 industry-признаков из панели #239."""
    if sample not in SAMPLES:
        raise ValueError(f"неизвестная выборка {sample!r}")
    rows, skipped = cmv3.build_sample_v3(sample)
    enriched = []
    for row in rows:
        extra = ci.industry_features(
            panel, row.month, cm.cutoff_for(row.month)
        )
        enriched.append(
            dataclasses.replace(row, features={**row.features, **extra})
        )
    return enriched, skipped


def assert_contiguous_months(rows: list) -> None:
    for left, right in zip(rows, rows[1:]):
        if cm.previous_month(right.month) != left.month:
            raise AssertionError(
                f"выборка не непрерывна: {left.month} → {right.month}"
            )


def cumulative_target(values: list, horizon: int) -> float:
    """`100·(Π(1+y/100)−1)`; NaN в окне — NaN."""
    if len(values) != horizon:
        raise ValueError(f"окно {len(values)} месяцев, ожидалось {horizon}")
    product = 1.0
    for value in values:
        if not math.isfinite(float(value)):
            return NAN
        product *= 1.0 + float(value) / 100.0
    return (product - 1.0) * 100.0


def horizon_rows(rows: list, horizon: int) -> list:
    """Origin-строки с накопленными таргетами h; h1 — исходные строки."""
    if horizon < 1:
        raise ValueError(f"горизонт {horizon} — меньше одного месяца")
    if horizon == 1:
        return list(rows)
    assert_contiguous_months(rows)
    if len(rows) < horizon:
        return []
    result = []
    for index in range(len(rows) - horizon + 1):
        y = {}
        for code in cm.TARGET_SERIES:
            window = [rows[index + offset].y[code] for offset in range(horizon)]
            y[code] = cumulative_target(window, horizon)
        result.append(dataclasses.replace(rows[index], y=y))
    return result


def horizon_window(start: int, embargo: int, horizon: int) -> tuple:
    """`(start_eff, embargo_eff)` для purge h>1."""
    return start + horizon - 1, embargo + horizon - 1


def assert_no_horizon_leakage(
    rows: list, horizon: int, start_eff: int, embargo_eff: int
) -> None:
    """Train-origin `j` не должен иметь target-окно, заходящее в test-origin."""
    for index in range(start_eff, len(rows)):
        max_train = index - embargo_eff - 1
        if max_train >= 0 and max_train + horizon - 1 >= index:
            raise AssertionError(
                f"утечка h{horizon}: train-origin {max_train} перекрывает "
                f"test-origin {index}"
            )


def sample_hash(rows: list, names: list) -> str:
    payload = [
        {
            "month": row.month.isoformat(),
            "y": mlx.json_safe(row.y),
            "features": mlx.json_safe(
                {name: row.features[name] for name in names}
            ),
        }
        for row in rows
    ]
    return mlx.sha256_text(mlx.canonical_json(payload))


def feature_names() -> list:
    """v2-база + weekly + industry; `month` последним."""
    return ci.feature_names(
        component=None, include_mp=True, include_weekly=True
    )


# ---------------------------------------------------------------------------
# Модели и оценка
# ---------------------------------------------------------------------------


def select_models(names: tuple) -> dict:
    unknown = set(names) - set(MODELS)
    if unknown:
        raise SystemExit(
            f"неизвестные модели: {sorted(unknown)}; доступны {list(MODELS)}"
        )
    return {name: MODELS[name] for name in names}


def _errors(predictions: list, model: str) -> tuple:
    group = [item for item in predictions if item.model == model]
    group.sort(key=lambda item: item.month)
    return (
        [item.month for item in group],
        [item.y_pred - item.y_true for item in group],
        group,
    )


def _mde_ratio(err_a: list, err_b: list) -> float:
    if len(err_a) < 2:
        return NAN
    differences = [a * a - b * b for a, b in zip(err_a, err_b)]
    sd = float(np.std(differences, ddof=1))
    rmse_a = eu.rmse(err_a)
    rmse_b = eu.rmse(err_b)
    mde_mse = 2.802 * sd / math.sqrt(len(differences))
    return mde_mse / (rmse_a + rmse_b) if (rmse_a + rmse_b) else NAN


def pair(predictions: list, left: str, right: str, horizon: int) -> dict:
    months_left, errors_left, _ = _errors(predictions, left)
    months_right, errors_right, _ = _errors(predictions, right)
    common = sorted(set(months_left) & set(months_right))
    if not common:
        return {"n": 0}
    by_month_left = dict(zip(months_left, errors_left))
    by_month_right = dict(zip(months_right, errors_right))
    err_left = [by_month_left[month] for month in common]
    err_right = [by_month_right[month] for month in common]
    dm_stat, dm_p = eu.diebold_mariano(err_left, err_right, horizon)
    boot = eu.block_bootstrap_rmse_diff(
        err_left, err_right, BOOTSTRAP_BLOCK, BOOTSTRAP_N, SEED
    )
    return {
        "n": len(common),
        "rmse_left": eu.rmse(err_left),
        "rmse_right": eu.rmse(err_right),
        "delta": boot.rmse_diff,
        "delta_ci": [boot.low, boot.high],
        "dm_stat": dm_stat,
        "dm_p": dm_p,
        "mde_ratio": _mde_ratio(err_left, err_right),
    }


def window_pairs(horizon: int, models: dict) -> list:
    """Предрегистрированные пары окна; отсутствующие модели пропускаются."""
    present = set(models)
    if horizon == 1 and set(FULL_MODELS) == present:
        primary = [
            (candidate, baseline)
            for candidate in PRIMARY_CANDIDATES
            for baseline in PRIMARY_BASELINES
        ]
    else:
        primary = [
            (candidate, baseline)
            for candidate in PRIMARY_CANDIDATES
            for baseline in ("v2_bottomup", "B3_weekly")
        ]
    secondary = [
        (INDUSTRY_INCREMENT[0], INDUSTRY_INCREMENT[1]),
        ("v3_ind_direct", "v2_baseline"),
        ("v3_ind_direct", "v3_ind_bottomup"),
        ("v3_bottomup", "v2_bottomup"),
        ("v3_ind_bottomup", "v2_bottomup"),
        ("v3_ind_bottomup", "v3_bottomup"),
    ]
    seen = set()
    result = []
    for left, right in primary + secondary:
        if left in present and right in present and (left, right) not in seen:
            seen.add((left, right))
            result.append((left, right))
    return result


def evaluate_window(
    rows: list,
    models: dict,
    horizon: int,
    start: int,
    embargo: int,
) -> tuple:
    start_eff, embargo_eff = horizon_window(start, embargo, horizon)
    assert_no_horizon_leakage(rows, horizon, start_eff, embargo_eff)
    cm._reset_fold_weights(models)
    skipped = {name: 0 for name in models}
    notes = {name: {} for name in models}
    predictions = cm.walk_forward(
        rows,
        models,
        start_eff,
        embargo_eff,
        skipped=skipped,
        notes=notes,
    )
    summary = {}
    for model in models:
        months, errors, group = _errors(predictions, model)
        if not group:
            summary[model] = {"n": 0}
            continue
        summary[model] = {
            "n": len(group),
            "rmse": cm.rmse_pp(group),
            "mae": cm.mae_pp(group),
            "months": [month.isoformat() for month in months],
        }
    pairs = {}
    primary_names = []
    for left, right in window_pairs(horizon, models):
        name = f"{left} vs {right}"
        item = pair(predictions, left, right, horizon)
        item["primary"] = (
            left in PRIMARY_CANDIDATES
            and right in PRIMARY_BASELINES
            and horizon == 1
            and set(FULL_MODELS) == set(models)
        )
        pairs[name] = item
        if item["primary"]:
            primary_names.append(name)
    if primary_names:
        adjusted = ci.holm_adjust([pairs[name].get("dm_p") for name in primary_names])
        for name, value in zip(primary_names, adjusted):
            pairs[name]["dm_p_holm"] = value
    diagnostics = {
        "last12": _subset_summary(predictions, models, lambda months: months[-12:]),
        "ex2022": _subset_summary(
            predictions, models, lambda months: [m for m in months if m.year != 2022]
        ),
    }
    weights = cm._window_weights(models)
    return {
        "summary": summary,
        "pairs": pairs,
        "diagnostics": diagnostics,
        "skipped": skipped,
        "notes": notes,
        "weights": {name: list(value) for name, value in weights.items()},
        "predictions": predictions,
        "start_eff": start_eff,
        "embargo_eff": embargo_eff,
        "folds": max(0, len(rows) - start_eff),
    }


def _subset_summary(predictions: list, models: dict, selector) -> dict:
    result = {}
    for model in models:
        months, errors, group = _errors(predictions, model)
        if not group:
            continue
        keep = set(selector(months))
        subset = [
            error for month, error in zip(months, errors) if month in keep
        ]
        if subset:
            result[model] = {
                "n": len(subset),
                "rmse": eu.rmse(subset),
                "mae": eu.mae(subset),
            }
    return result


# ---------------------------------------------------------------------------
# Гейты и решение
# ---------------------------------------------------------------------------


def _reference_gate(reference: dict, summary: dict) -> tuple:
    """Сверка summary с эталоном; `(gate, failed)` или `({}, [])` без эталона."""
    if reference is None:
        return {}, []
    gate = {}
    failed = []
    for model, expected in reference.items():
        item = summary.get(model)
        if not item or not item.get("n"):
            gate[model] = {"status": "missing"}
            continue
        actual = item["rmse"]
        delta = actual - expected
        ok = abs(delta) <= REGRESSION_TOLERANCE
        gate[model] = {
            "status": "ok" if ok else "differs",
            "expected": expected,
            "actual": actual,
            "delta": delta,
        }
        if not ok:
            failed.append(model)
    return gate, failed


def regression_gate(
    sample: str, start: int, embargo: int, summary: dict, horizon: int = 1
) -> tuple:
    """Stage 1 gate только для h1: эталоны — месячные п.п., не cumulative %."""
    if horizon != 1:
        return {}, []
    return _reference_gate(
        STAGE1_REFERENCE.get((sample, start, embargo)), summary
    )


def task2_gate(
    sample: str, start: int, embargo: int, summary: dict, horizon: int = 1
) -> tuple:
    """Task 2 gate только для h1: эталоны — месячные п.п."""
    if horizon != 1:
        return {}, []
    return _reference_gate(
        TASK2_REFERENCE.get((sample, start, embargo)), summary
    )


def build_gates(
    windows: dict, industry_meta: dict, xlsx_gate: dict
) -> tuple:
    """Гейты по сохранённым окнам: Stage 1/Task 2 (h1), данные, источник.

    Возвращает `(gates, gate_failed)`. `data_revision` документируется, если
    weekly h1 Stage 1 расходится, а `ind_only_bottomup` (без weekly и aggregate
    MP) воспроизводит Task 2 в пределах допуска.
    """
    gates = {"data": {"status": "ok", "detail": _data_detail(industry_meta)}}
    if xlsx_gate.get("status", "not run") != "not run":
        gates["xlsx"] = dict(xlsx_gate)
        if not gates["xlsx"].get("detail"):
            gates["xlsx"]["detail"] = (
                f"n={gates['xlsx'].get('n', '—')}, "
                f"max abs diff={gates['xlsx'].get('max_abs_diff', '—')}, "
                f"missing={gates['xlsx'].get('missing_in_ch', '—')}, "
                f"mismatched={gates['xlsx'].get('mismatched', '—')}, "
                f"extra={gates['xlsx'].get('extra_in_ch', '—')}"
            )
    gate_failed = [] if xlsx_gate.get("status") != "failed" else ["xlsx"]
    raw_gates = {}
    for key, window in windows.items():
        stage1, failed1 = regression_gate(
            window["sample"],
            window["start"],
            window["embargo"],
            window["summary"],
            window.get("horizon", 1),
        )
        task2, failed2 = task2_gate(
            window["sample"],
            window["start"],
            window["embargo"],
            window["summary"],
            window.get("horizon", 1),
        )
        raw_gates[key] = {"stage1": stage1, "task2": task2}
        if stage1:
            gates[f"stage1_{key}"] = {
                "status": "ok" if not failed1 else "differs",
                "detail": _gate_detail(stage1),
            }
            gate_failed.extend(failed1)
        if task2:
            gates[f"task2_{key}"] = {
                "status": "ok" if not failed2 else "differs",
                "detail": _gate_detail(task2),
            }
            gate_failed.extend(failed2)
    weekly_gate = raw_gates.get("weekly__h1__start24__embargo0", {})
    if weekly_gate:
        stage1_differs = any(
            item.get("status") == "differs"
            for item in weekly_gate.get("stage1", {}).values()
        )
        industry_ok = (
            weekly_gate.get("task2", {})
            .get("ind_only_bottomup", {})
            .get("status") == "ok"
        )
        if stage1_differs and industry_ok:
            gates["data_revision"] = {
                "status": "documented",
                "detail": (
                    "Stage 1 baseline сдвинулся из-за живой ревизии weekly/"
                    "aggregate MP; ind_only_bottomup (без weekly и aggregate MP) "
                    "воспроизводит Task 2 в пределах 1e-6 — источник #239 и "
                    "базовые признаки стабильны. Контрасты считаются от "
                    "текущих baseline на общих фолдах."
                ),
            }
    return gates, sorted(set(gate_failed))


def _significant_better(item: dict) -> bool:
    if not item.get("n") or item.get("delta") is None:
        return False
    holm = item.get("dm_p_holm", item.get("dm_p"))
    return bool(
        item["delta"] < 0.0
        and (
            (holm is not None and holm < 0.05)
            or item["delta_ci"][1] < 0.0
        )
    )


def _significant_worse(item: dict) -> bool:
    if not item.get("n") or item.get("delta") is None:
        return False
    return bool(
        item["delta"] > 0.0
        and item["delta_ci"][0] > 0.0
        and item["dm_p"] is not None
        and item["dm_p"] < 0.05
    )


def candidate_passes(name: str, pairs: dict) -> tuple:
    reasons = []
    for baseline in PRIMARY_BASELINES:
        item = pairs.get(f"{name} vs {baseline}")
        if not item or not item.get("n"):
            return False, [f"нет пары {name} vs {baseline}"]
        if not _significant_better(item):
            reasons.append(
                f"{name} vs {baseline}: Δ {item['delta']:+.6f}, "
                f"p={item['dm_p']:.4f}"
            )
    return not reasons, reasons


def not_worse(name: str, pairs: dict) -> tuple:
    item = pairs.get(f"{name} vs v2_bottomup")
    if not item or not item.get("n"):
        return False, [f"нет пары {name} vs v2_bottomup"]
    if _significant_worse(item):
        return False, [
            f"{name} значимо хуже v2_bottomup: Δ {item['delta']:+.6f}, "
            f"p={item['dm_p']:.4f}"
        ]
    return True, []


def decide(windows: dict) -> dict:
    """Финальный вердикт по decision tree спеки §7.1."""
    weekly_h1 = windows.get("weekly__h1__start24__embargo0")
    weekly_h2 = windows.get("weekly__h2__start24__embargo0")
    weekly_h3 = windows.get("weekly__h3__start24__embargo0")
    if not weekly_h1:
        return {"verdict": "iterate", "reasons": ["нет weekly h1"]}
    if not weekly_h2 or not weekly_h3:
        return {
            "verdict": "iterate",
            "reasons": ["нет weekly h2/h3 — promote-гейт не проверен"],
        }
    pairs_h1 = weekly_h1["pairs"]
    pairs_h2 = weekly_h2["pairs"]
    pairs_h3 = weekly_h3["pairs"]
    a, b = PRIMARY_CANDIDATES
    a_pass, a_reasons = candidate_passes(a, pairs_h1)
    b_pass, b_reasons = candidate_passes(b, pairs_h1)
    a_not_worse_h2, a_h2_reasons = not_worse(a, pairs_h2)
    a_not_worse_h3, a_h3_reasons = not_worse(a, pairs_h3)
    b_not_worse_h2, b_h2_reasons = not_worse(b, pairs_h2)
    b_not_worse_h3, b_h3_reasons = not_worse(b, pairs_h3)
    a_ok = a_pass and a_not_worse_h2 and a_not_worse_h3
    b_ok = b_pass and b_not_worse_h2 and b_not_worse_h3
    increment = pairs_h1.get(f"{a} vs {b}", {})
    increment_significant = _significant_better(increment)
    if a_ok:
        return {
            "verdict": "promote",
            "candidate": a,
            "reasons": [
                f"{a} значимо лучше всех бенчмарков на weekly h1",
                "h2/h3 не хуже v2_bottomup",
                (
                    "industry-инкремент значим"
                    if increment_significant
                    else "industry-инкремент незначим; кандидат — полный набор"
                ),
            ],
            "increment": increment,
        }
    if b_ok:
        return {
            "verdict": "promote",
            "candidate": b,
            "reasons": [
                f"{a} не прошёл h1/h2/h3-гейт",
                f"{b} значимо лучше всех бенчмарков на weekly h1",
                "h2/h3 не хуже v2_bottomup",
                (
                    "industry-инкремент незначим — выбран fallback без industry"
                    if not increment_significant
                    else "industry-инкремент значим, но A не прошёл robustness"
                ),
            ],
            "increment": increment,
        }
    a_worse_h1 = any(
        _significant_worse(pairs_h1.get(f"{a} vs {baseline}", {}))
        for baseline in ("v2_bottomup", "B3_weekly")
    )
    b_worse_h1 = any(
        _significant_worse(pairs_h1.get(f"{b} vs {baseline}", {}))
        for baseline in ("v2_bottomup", "B3_weekly")
    )
    a_worse_h23 = any(
        _significant_worse(pairs.get(f"{a} vs v2_bottomup", {}))
        for pairs in (pairs_h2, pairs_h3)
    )
    b_worse_h23 = any(
        _significant_worse(pairs.get(f"{b} vs v2_bottomup", {}))
        for pairs in (pairs_h2, pairs_h3)
    )
    reasons = []
    if a_worse_h1 or a_worse_h23:
        reasons.append(f"{a}: значимая деградация на h1/h2/h3")
    if b_worse_h1 or b_worse_h23:
        reasons.append(f"{b}: значимая деградация на h1/h2/h3")
    if (a_worse_h1 and b_worse_h1) or (a_worse_h23 and b_worse_h23):
        return {
            "verdict": "reject",
            "reasons": reasons + ["оба кандидата значимо хуже"],
            "increment": increment,
        }
    reasons.extend(a_reasons[:2])
    reasons.extend(b_reasons[:2])
    reasons.extend(a_h2_reasons[:1])
    reasons.extend(a_h3_reasons[:1])
    reasons.extend(b_h2_reasons[:1])
    reasons.extend(b_h3_reasons[:1])
    if not reasons:
        reasons.append("значимого улучшения над v2/B1/B2/B3_weekly нет")
    return {"verdict": "iterate", "reasons": reasons, "increment": increment}


# ---------------------------------------------------------------------------
# Артефакты
# ---------------------------------------------------------------------------


def _fmt(value, digits: int = 6) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "—"
    return f"{value:.{digits}f}"


def _pair_table(lines: list, pairs: dict, title: str) -> None:
    lines.append(f"## {title}")
    lines.append("")
    lines.append(
        "| Пара | n | RMSE left | RMSE right | Δ | 95 % CI Δ | DM p | Holm p | MDE |"
    )
    lines.append("|---|---:|---:|---:|---:|---|---:|---:|---:|")
    for name, item in pairs.items():
        if not item.get("n"):
            continue
        holm = item.get("dm_p_holm")
        lines.append(
            f"| `{name}` | {item['n']} | {_fmt(item['rmse_left'])} | "
            f"{_fmt(item['rmse_right'])} | {item['delta']:+.6f} | "
            f"[{item['delta_ci'][0]:+.6f}; {item['delta_ci'][1]:+.6f}] | "
            f"{_fmt(item['dm_p'], 4)} | "
            f"{'—' if holm is None else _fmt(holm, 4)} | "
            f"{_fmt(item['mde_ratio'], 4)} |"
        )
    lines.append("")


def write_window_report(path: Path, payload: dict) -> None:
    lines = [f"# CPI v3 public: {payload['window']}", ""]
    lines.append(
        f"- Выборка: `{payload['sample']}`; горизонт h{payload['horizon']}; "
        f"origin-строк: {payload['n_rows']}; start={payload['start']}; "
        f"start_eff={payload['start_eff']}; embargo={payload['embargo']}; "
        f"embargo_eff={payload['embargo_eff']}; фолдов: {payload['folds']}"
    )
    lines.append(f"- Пропущено: {payload['n_skipped']}")
    lines.append("")
    lines.append("## Модели")
    lines.append("")
    lines.append("| Модель | n | RMSE | MAE |")
    lines.append("|---|---:|---:|---:|")
    for model, item in payload["summary"].items():
        if not item.get("n"):
            lines.append(f"| `{model}` | 0 | — | — |")
            continue
        lines.append(
            f"| `{model}` | {item['n']} | {_fmt(item['rmse'])} | "
            f"{_fmt(item['mae'])} |"
        )
    lines.append("")
    _pair_table(lines, payload["pairs"], "Парные сравнения")
    path.write_text("\n".join(lines), encoding="utf-8")


def write_json(path: Path, payload: dict) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def write_final_report(path: Path, payload: dict) -> None:
    decision = payload["decision"]
    lines = ["# Модель v3 на публичных данных: отчёт прогона", ""]
    lines.append(
        "> `scripts/models/cpi/cpi_monthly_v3_public.py`; пункт "
        "`key-rate/phase-4-ml-components`, Task 4 (issue #237). "
        "Числа не редактируются руками."
    )
    lines.append("")
    lines.append(f"**Вердикт: `{decision['verdict']}`**")
    for reason in decision.get("reasons", []):
        lines.append(f"- {reason}")
    if decision.get("candidate"):
        lines.append(f"- Кандидат: `{decision['candidate']}`")
    lines.append("")
    lines.append("## Данные")
    lines.append("")
    industry = payload["data"]["industry"]
    lines.append(
        f"- Industry: `v_cbr_survey` (#239), {industry['n_series']} SA-рядов, "
        f"{industry['n_points']} точек, {industry['first']}…{industry['last']}; "
        f"sha256 `{industry['sha256']}`"
    )
    lines.append(
        "- Официальный прогон XLSX не читает; сверка источника — "
        f"`{payload['gates'].get('xlsx', {}).get('status', 'not run')}`"
    )
    xlsx = payload["gates"].get("xlsx", {})
    if xlsx.get("n"):
        lines.append(
            f"- CH↔XLSX: n={xlsx['n']}, max abs diff={xlsx['max_abs_diff']}, "
            f"extra-dates CH={xlsx.get('extra_in_ch', 0)}"
        )
    lines.append(
        f"- Таргеты: 4 ряда `rosstat_ipc_mes`, cutoff 8-е M+1; "
        f"weekly-ряды: {payload['data']['weekly']['n_series']} товарных "
        "(агрегат исключён)."
    )
    lines.append("")
    lines.append("## Окна")
    lines.append("")
    lines.append("| Окно | n | start | start_eff | embargo | embargo_eff | фолды |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for key, window in payload["windows"].items():
        lines.append(
            f"| `{key}` | {window['n_rows']} | {window['start']} | "
            f"{window['start_eff']} | {window['embargo']} | "
            f"{window['embargo_eff']} | {window['folds']} |"
        )
    lines.append("")
    lines.append("## Гейты")
    lines.append("")
    lines.append("| Гейт | Статус | Детали |")
    lines.append("|---|---|---|")
    for name, item in payload["gates"].items():
        lines.append(
            f"| `{name}` | {item.get('status', '—')} | "
            f"{item.get('detail', '')} |"
        )
    revision = payload["gates"].get("data_revision")
    if revision:
        lines.append("")
        lines.append(f"> **Ревизия данных:** {revision['detail']}")
    lines.append("")
    lines.append("## Модель × окно (RMSE)")
    lines.append("")
    model_names = sorted(
        {
            model
            for window in payload["windows"].values()
            for model, item in window["summary"].items()
            if item.get("n")
        }
    )
    window_keys = list(payload["windows"])
    lines.append("| Модель | " + " | ".join(f"`{key}`" for key in window_keys) + " |")
    lines.append("|---|" + "---:|" * len(window_keys))
    for model in model_names:
        values = []
        for key in window_keys:
            item = payload["windows"][key]["summary"].get(model, {})
            values.append(_fmt(item.get("rmse")) if item.get("n") else "—")
        lines.append(f"| `{model}` | " + " | ".join(values) + " |")
    lines.append("")
    h1 = payload["windows"].get("weekly__h1__start24__embargo0")
    if h1:
        _pair_table(lines, h1["pairs"], "Контрасты: weekly h1")
        lines.append("## Last 12 / ex-2022")
        lines.append("")
        lines.append("| Модель | last 12 RMSE | ex-2022 RMSE |")
        lines.append("|---|---:|---:|")
        for model in sorted(h1["diagnostics"]["last12"]):
            last = h1["diagnostics"]["last12"][model]
            ex = h1["diagnostics"]["ex2022"].get(model, {})
            lines.append(
                f"| `{model}` | {_fmt(last['rmse'])} | "
                f"{_fmt(ex.get('rmse'))} |"
            )
        lines.append("")
        lines.append(
            "> Weekly-тест начинается 2024-01, поэтому ex-2022 = полная "
            "выборка; режим 2022 покрыт `long`-окном."
        )
        lines.append("")
    for key, window in payload["windows"].items():
        if key == "weekly__h1__start24__embargo0":
            continue
        _pair_table(lines, window["pairs"], f"Контрасты: {key}")
    lines.append("## Что не проверено")
    lines.append("")
    lines.append(
        "- Task 3 component panel не используется: гейт покрытия весов "
        "39.299 % < 50 %; полный 88-групповой bottom-up не заявляется."
    )
    lines.append(
        "- CB_COMP напрямую не сравнивается — Task 5 (#238)."
    )
    lines.append(
        "- `long` h2/h3/h6 не считаются; `long` start 24 — только 4 модели."
    )
    lines.append(
        "- Публичные фиксированные веса не используются; NNLS — "
        "предрегистрированный вариант."
    )
    lines.append(
        "- Винтажей/PIT-слоя нет: as-of — правило cutoff, а не фактические "
        "даты публикаций."
    )
    lines.append(
        "- Используется SA-блок industry; raw-блок не проверяется."
    )
    lines.append("")
    lines.append("## Воспроизводимость")
    lines.append("")
    lines.append("```bash")
    lines.append("make cpi-monthly-v3-public-test")
    lines.append(
        'ML_LOG=1 make cpi-monthly-v3-public ARGS="--all --allow-gate-diff"'
    )
    lines.append("make all")
    lines.append("make docs-check")
    lines.append("```")
    lines.append("")
    lines.append(
        "Сверка источника (опционально): "
        "`ARGS=\"--verify-only --verify-xlsx PATH\"`; официальный прогон "
        "XLSX не читает."
    )
    lines.append("")
    lines.append(
        f"sample_hash: `{payload.get('sample_hash', '—')}`; "
        f"panel_hash: `{payload['data']['industry']['sha256']}`"
    )
    for key, window in payload["windows"].items():
        lines.append(f"- `{key}`: run_id `{window.get('run_id', '—')}`")
    path.write_text("\n".join(lines), encoding="utf-8")


# ---------------------------------------------------------------------------
# Прогон
# ---------------------------------------------------------------------------


def _window_key(sample: str, horizon: int, start: int, embargo: int) -> str:
    return f"{sample}__h{horizon}__start{start}__embargo{embargo}"


def _window_specs(
    sample: str = None, horizons: tuple = None, start: int = None,
    embargo: int = 0, models: tuple = None,
) -> list:
    if sample is None:
        return [
            {
                "sample": "weekly",
                "horizon": 1,
                "start": 24,
                "embargo": 0,
                "models": FULL_MODELS,
            },
            {
                "sample": "weekly",
                "horizon": 2,
                "start": 24,
                "embargo": 0,
                "models": SECONDARY_MODELS,
            },
            {
                "sample": "weekly",
                "horizon": 3,
                "start": 24,
                "embargo": 0,
                "models": SECONDARY_MODELS,
            },
            {
                "sample": "weekly",
                "horizon": 6,
                "start": 24,
                "embargo": 0,
                "models": SECONDARY_MODELS,
            },
            {
                "sample": "weekly",
                "horizon": 1,
                "start": 24,
                "embargo": 1,
                "models": SECONDARY_MODELS,
            },
            {
                "sample": "long",
                "horizon": 1,
                "start": 48,
                "embargo": 0,
                "models": LONG_MODELS,
            },
            {
                "sample": "long",
                "horizon": 1,
                "start": 24,
                "embargo": 0,
                "models": LONG_WINDOW_MODELS,
            },
        ]
    if sample not in SAMPLES:
        raise SystemExit(f"неизвестная выборка {sample!r}")
    selected_horizons = (
        tuple(horizons) if horizons is not None else DEFAULT_HORIZONS[sample]
    )
    if sample == "long" and any(horizon != 1 for horizon in selected_horizons):
        raise SystemExit("long: поддерживается только h1")
    selected_start = DEFAULT_START[sample] if start is None else start
    # None — набор выбирает `_default_models_for_spec` по sample/horizon/
    # embargo/start; CLI `--models` переопределяет его только для одного окна.
    selected_models = models
    return [
        {
            "sample": sample,
            "horizon": horizon,
            "start": selected_start,
            "embargo": embargo,
            "models": selected_models,
        }
        for horizon in selected_horizons
    ]


def _default_models_for_spec(spec: dict) -> tuple:
    if spec["models"] is not None:
        return spec["models"]
    if spec["sample"] == "long":
        return LONG_MODELS if spec["start"] == 48 else LONG_WINDOW_MODELS
    if spec["horizon"] == 1 and spec["embargo"] == 0:
        return FULL_MODELS
    return SECONDARY_MODELS


def run_window(
    spec: dict,
    rows: list,
    panel: dict,
    out_dir: Path,
    skipped: list,
    panel_sha: str = "",
) -> dict:
    sample = spec["sample"]
    horizon = spec["horizon"]
    start = spec["start"]
    embargo = spec["embargo"]
    model_names = _default_models_for_spec(spec)
    models = select_models(model_names)
    rows_h = horizon_rows(rows, horizon)
    if not rows_h:
        raise SystemExit(f"пустая horizon-выборка {sample}/h{horizon}")
    key = _window_key(sample, horizon, start, embargo)
    print(
        f"Окно {key}: {len(rows_h)} origin-строк, "
        f"модели {len(models)}, start={start} embargo={embargo}",
        flush=True,
    )
    result = evaluate_window(rows_h, models, horizon, start, embargo)
    window = {
        "window": key,
        "sample": sample,
        "horizon": horizon,
        "start": start,
        "embargo": embargo,
        "start_eff": result["start_eff"],
        "embargo_eff": result["embargo_eff"],
        "folds": result["folds"],
        "n_rows": len(rows_h),
        "n_skipped": len(skipped),
        "models": list(model_names),
        "summary": result["summary"],
        "pairs": result["pairs"],
        "diagnostics": result["diagnostics"],
        "skipped": result["skipped"],
        "notes": result["notes"],
        "weights": result["weights"],
    }
    window_dir = out_dir / key
    window_dir.mkdir(parents=True, exist_ok=True)
    report_path = window_dir / "report.md"
    json_path = window_dir / "result.json"
    write_window_report(report_path, window)
    write_json(json_path, window)
    if os.environ.get("ML_LOG") == "1":
        try:
            logger = mlx.start_run(
                experiment="cpi_monthly_v3_public",
                run_name=f"cpi_monthly_v3_public__{key}",
                config={
                    "sample": sample,
                    "horizon": horizon,
                    "start": start,
                    "start_eff": result["start_eff"],
                    "embargo": embargo,
                    "embargo_eff": result["embargo_eff"],
                    "seed": SEED,
                    "models": list(model_names),
                    "industry_source": "v_cbr_survey",
                    "panel_hash": panel_sha or panel_hash(panel),
                },
                data_asof=cm.month_end(rows[-1].month),
                sample_meta={
                    "rows": len(rows_h),
                    "first": rows_h[0].month.isoformat(),
                    "last_origin": rows_h[-1].month.isoformat(),
                    "last_target": cm._month_shift(
                        rows_h[-1].month, horizon - 1
                    ).isoformat(),
                    "skipped": len(skipped),
                    "horizon": horizon,
                },
                sample=[
                    {
                        "month": row.month.isoformat(),
                        "y": mlx.json_safe(row.y),
                        "features": mlx.json_safe(row.features),
                    }
                    for row in rows_h
                ],
                spec_path=(
                    "docs/superpowers/specs/"
                    "2026-10-19-cpi-v3-public-design.md"
                ),
                enabled=True,
            )
            logger.log_metrics(
                {
                    "sample": sample,
                    "horizon": horizon,
                    "summary": result["summary"],
                    "pairs": result["pairs"],
                    "diagnostics": result["diagnostics"],
                }
            )
            unit = "pp" if horizon == 1 else "cum_pct"
            logger.log_predictions(
                [
                    mlx.PredictionRow(
                        model=item.model,
                        window=f"h{horizon}",
                        target_date=cm._month_shift(
                            item.month, horizon - 1
                        ),
                        predicted=item.y_pred,
                        actual=item.y_true,
                        unit=unit,
                        meta={"sample": sample, "start": start, "embargo": embargo},
                    )
                    for item in result["predictions"]
                ]
            )
            logger.log_artifacts([report_path, json_path])
            logger.finish("finished", f"window={key}; rows={len(rows_h)}")
            window["run_id"] = logger.run_id
            write_json(json_path, window)
            write_window_report(report_path, window)
            print(f"MLflow run_id: {window['run_id']}", flush=True)
        except Exception as exc:  # noqa: BLE001 - журнал не должен ронять прогон
            print(
                f"Журнал эксперимента не записан: {type(exc).__name__}: {exc}",
                flush=True,
            )
    return window


def run(
    sample: str = None,
    horizons: tuple = None,
    start: int = None,
    embargo: int = 0,
    models: tuple = None,
    out_dir: Path = None,
    verify_xlsx=None,
    allow_gate_diff: bool = False,
    report_only: bool = False,
    verify_only: bool = False,
) -> dict:
    out_dir = Path(out_dir or ROOT / "build" / "cpi-monthly-v3-public")
    out_dir.mkdir(parents=True, exist_ok=True)
    specs = _window_specs(sample, horizons, start, embargo, models)
    if report_only:
        return _report_only(out_dir, specs, allow_gate_diff)
    panel, industry_meta = load_industry_panel_ch()
    panel_sha = panel_hash(panel)
    industry_meta["sha256"] = panel_sha
    xlsx_gate = {"status": "not run"}
    if verify_xlsx is not None:
        xlsx_path = (
            ci.resolve_mp_survey(None, out_dir)[0]
            if verify_xlsx == ""
            else Path(verify_xlsx)
        )
        verification = verify_panels(panel, panel_from_xlsx(xlsx_path))
        xlsx_gate = {
            "status": "ok" if verification["ok"] else "failed",
            "n": verification["common_points"],
            "max_abs_diff": verification["max_abs_diff"],
            "missing_in_ch": len(verification["missing_in_ch"]),
            "mismatched": len(verification["mismatched"]),
            "extra_in_ch": len(verification["extra_in_ch"]),
            "path": str(xlsx_path),
            "detail": (
                f"n={verification['common_points']}, "
                f"max abs diff={verification['max_abs_diff']}, "
                f"missing={len(verification['missing_in_ch'])}, "
                f"mismatched={len(verification['mismatched'])}, "
                f"extra={len(verification['extra_in_ch'])}"
            ),
        }
        print(
            f"CH↔XLSX: n={verification['common_points']}, "
            f"max abs diff={verification['max_abs_diff']}, "
            f"status={xlsx_gate['status']}",
            flush=True,
        )
        if verify_only:
            return {
                "gates": {"xlsx": xlsx_gate},
                "data": {"industry": industry_meta},
                "gate_failed": [] if verification["ok"] else ["xlsx"],
            }
        if not verification["ok"] and not allow_gate_diff:
            print("ВНИМАНИЕ: CH↔XLSX гейт не пройден", flush=True)
    elif verify_only:
        raise SystemExit("--verify-only требует --verify-xlsx")

    samples = {}
    for spec in specs:
        name = spec["sample"]
        if name not in samples:
            print(f"Сборка выборки {name}…", flush=True)
            samples[name] = build_sample_public(name, panel)
            ci.assert_no_industry_leakage(samples[name][0], panel)
            cmv3.assert_no_leakage_v3(samples[name][0])
    windows = {}
    for spec in specs:
        rows, skipped = samples[spec["sample"]]
        window = run_window(
            spec, rows, panel, out_dir, skipped, panel_sha=panel_sha
        )
        windows[window["window"]] = window

    gates, gate_failed = build_gates(windows, industry_meta, xlsx_gate)
    decision = decide(windows)
    first_rows, _ = next(iter(samples.values()))
    payload = {
        "schema": "cpi-monthly-v3-public/v1",
        "data": {
            "industry": industry_meta,
            "weekly": {
                "n_series": len(cmv3.load_weekly()),
            },
        },
        "gates": gates,
        "windows": windows,
        "decision": decision,
        "sample_hash": sample_hash(first_rows, feature_names()),
        "gate_failed": sorted(set(gate_failed)),
        "allow_gate_diff": bool(allow_gate_diff),
    }
    write_json(
        out_dir / "meta.json",
        {
            "data": payload["data"],
            "gates": payload["gates"],
            "sample_hash": payload["sample_hash"],
            "gate_failed": payload["gate_failed"],
        },
    )
    report_path = ROOT / "docs" / "research" / "2026-10-19-cpi-v3-public.md"
    json_path = ROOT / "docs" / "research" / "2026-10-19-cpi-v3-public.json"
    write_final_report(report_path, payload)
    write_json(json_path, payload)
    print(f"Вердикт: {decision['verdict']}", flush=True)
    print(f"Артефакты: {report_path}, {json_path}", flush=True)
    if gate_failed and not allow_gate_diff:
        print(
            "ВНИМАНИЕ: регрессионный гейт не пройден: "
            + ", ".join(sorted(set(gate_failed)))
            + "; артефакты записаны, для официального прогона разберите "
            "расхождение или передайте --allow-gate-diff",
            flush=True,
        )
    return payload


def _data_detail(meta: dict) -> str:
    return (
        f"{meta['n_series']} рядов, {meta['n_points']} точек, "
        f"{meta['first']}…{meta['last']}"
    )


def _gate_detail(gate: dict) -> str:
    parts = []
    for model, item in gate.items():
        if item.get("status") in ("missing", "skipped"):
            parts.append(f"{model}: {item['status']}")
        else:
            parts.append(
                f"{model}: {item['actual']:.6f} vs {item['expected']:.6f} "
                f"({item['delta']:+.9f}, {item['status']})"
            )
    return "; ".join(parts)


def _report_only(out_dir: Path, specs: list, allow_gate_diff: bool) -> dict:
    windows = {}
    for spec in specs:
        key = _window_key(
            spec["sample"], spec["horizon"], spec["start"], spec["embargo"]
        )
        path = out_dir / key / "result.json"
        if not path.exists():
            raise SystemExit(f"нет результата окна {key}: {path}")
        windows[key] = json.loads(path.read_text(encoding="utf-8"))
    meta_path = out_dir / "meta.json"
    meta = (
        json.loads(meta_path.read_text(encoding="utf-8"))
        if meta_path.exists()
        else {}
    )
    industry = meta.get("data", {}).get(
        "industry",
        {
            "n_series": 0,
            "n_points": 0,
            "first": "—",
            "last": "—",
            "sha256": "—",
        },
    )
    xlsx_gate = meta.get("gates", {}).get("xlsx", {"status": "not run"})
    gates, gate_failed = build_gates(windows, industry, xlsx_gate)
    payload = {
        "schema": "cpi-monthly-v3-public/v1",
        "data": meta.get(
            "data", {"industry": industry, "weekly": {"n_series": 0}}
        ),
        "gates": gates,
        "windows": windows,
        "decision": decide(windows),
        "sample_hash": meta.get("sample_hash", ""),
        "gate_failed": gate_failed,
        "allow_gate_diff": bool(allow_gate_diff),
        "note": "report-only: данные из meta.json, гейты пересчитаны по окнам",
    }
    report_path = ROOT / "docs" / "research" / "2026-10-19-cpi-v3-public.md"
    json_path = ROOT / "docs" / "research" / "2026-10-19-cpi-v3-public.json"
    write_final_report(report_path, payload)
    write_json(json_path, payload)
    print(f"Вердикт: {payload['decision']['verdict']}", flush=True)
    print(f"Артефакты: {report_path}, {json_path}", flush=True)
    return payload


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="CPI monthly v3 public: industry MP из v_cbr_survey (issue #237)"
    )
    parser.add_argument("--sample", choices=SAMPLES, default=None)
    parser.add_argument(
        "--horizons",
        default=None,
        help="через запятую; по умолчанию — набор выборки (weekly 1,2,3,6; long 1)",
    )
    parser.add_argument("--start", type=int, default=None)
    parser.add_argument("--embargo", type=int, default=0)
    parser.add_argument(
        "--models",
        default=None,
        help="через запятую; только для одного окна (smoke/диагностика)",
    )
    parser.add_argument(
        "--out-dir",
        default=str(ROOT / "build" / "cpi-monthly-v3-public"),
    )
    parser.add_argument(
        "--verify-xlsx",
        nargs="?",
        const="",
        default=None,
        help="путь к mp_survey_data.xlsx; без пути — скачать публичный URL",
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="только сверка CH↔XLSX и выход",
    )
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="собрать финальный отчёт из сохранённых result.json",
    )
    parser.add_argument(
        "--allow-gate-diff",
        action="store_true",
        help="не падать при расхождении регрессионного/источникового гейта",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="все предрегистрированные окна (по умолчанию при пустых --sample/--horizons)",
    )
    arguments = parser.parse_args(argv)
    horizons = (
        tuple(int(value) for value in arguments.horizons.split(","))
        if arguments.horizons
        else None
    )
    models = (
        tuple(arguments.models.split(",")) if arguments.models else None
    )
    if arguments.all or (arguments.sample is None and horizons is None):
        payload = run(
            sample=None,
            out_dir=Path(arguments.out_dir),
            verify_xlsx=arguments.verify_xlsx,
            allow_gate_diff=arguments.allow_gate_diff,
            report_only=arguments.report_only,
            verify_only=arguments.verify_only,
        )
    else:
        if arguments.sample is None:
            raise SystemExit("--horizons требует --sample")
        payload = run(
            sample=arguments.sample,
            horizons=horizons,
            start=arguments.start,
            embargo=arguments.embargo,
            models=models,
            out_dir=Path(arguments.out_dir),
            verify_xlsx=arguments.verify_xlsx,
            allow_gate_diff=arguments.allow_gate_diff,
            report_only=arguments.report_only,
            verify_only=arguments.verify_only,
        )
    if payload.get("gate_failed") and not arguments.allow_gate_diff:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
