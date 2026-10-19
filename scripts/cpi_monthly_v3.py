#!/usr/bin/env python3
"""CPI monthly v3: публичные недельные признаки в месячном nowcast ИПЦ.

Stage 1 пункта `key-rate/phase-4-ml-components` (issue #76), спека
`docs/superpowers/specs/2026-10-19-cpi-components-public-data-design.md`.

Идея: h1-разрыв с CB_COMP после #221 остаётся 5,5 %, а недельные индексы
Росстата месяца M доступны до релиза месячного ИПЦ (10–16-е M+1). Скрипт
добавляет к признакам v2 четыре diffusion-признака по 149 недельным рядам
`v_rosstat_macro` (`source='rosstat'`, `frequency='W'`) и сравнивает с
v2-baseline, B1–B3 и B3_weekly на walk-forward.

Выборки:
* `weekly` — 2022-01…2026-08, все недельные признаки доступны (primary);
* `long` — 2013-10…2026-08, до 2020 недельные ряды разрежены; NaN остаётся
  в признаках (CatBoost — нативно, ElasticNet — медиана train).

Недельные признаки (до 4 последних доступных недель, as-of `date + 2`):
`wk_share_rising`, `wk_median`, `wk_median4`, `wk_gasoline4`.

Скрипт только читает ClickHouse; в БД не пишет. Запуск — `make cpi-monthly-v3`
(env подключает Makefile), тесты — `scripts/cpi_monthly_v3_test.py`.
"""

import argparse
import bisect
import json
import math
import os
import statistics
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
from catboost import CatBoostRegressor
from sklearn.linear_model import ElasticNetCV

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import cpi_monthly as cm
import eval_utils as eu
import ml_experiment as mlx
from extrapolation_aisc import ch_query

WEEKLY_SQL = """
SELECT series, date, value
FROM v_rosstat_macro
WHERE series IN (
    SELECT series FROM v_series_catalog
    WHERE source = 'rosstat' AND frequency = 'W'
)
ORDER BY date, series
FORMAT JSON
"""

WEEKLY_AGGREGATE = "Индекс потребительских цен (недельная оценка)"
WEEKLY_GASOLINE = "Бензин автомобильный"
WEEKLY_WINDOW_WEEKS = 4
WEEKLY_FEATURES = (
    "wk_share_rising",
    "wk_median",
    "wk_median4",
    "wk_gasoline4",
)

# Границы выборок Stage 1: weekly — полная доступность недельных рядов;
# long — робастность, до 2020 недельные признаки разрежены.
SAMPLE_BOUNDS = {
    "weekly": (date(2022, 1, 1), date(2026, 8, 1)),
    "long": (date(2013, 10, 1), date(2026, 8, 1)),
}

SEED = cm.SEED
DEFAULT_START = 24
BOOTSTRAP_BLOCK = 3
BOOTSTRAP_N = 2000


# ---------------------------------------------------------------------------
# Недельная панель и признаки
# ---------------------------------------------------------------------------


def row_date(value) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def row_float(value) -> float:
    return float(value)


def load_weekly() -> dict:
    """`{series: [(date, value)]}` из `v_rosstat_macro`; агрегат исключён."""
    weekly = {}
    for row in ch_query(WEEKLY_SQL):
        series = str(row["series"])
        if series == WEEKLY_AGGREGATE:
            continue
        weekly.setdefault(series, []).append(
            (row_date(row["date"]), row_float(row["value"]))
        )
    return {series: sorted(points) for series, points in weekly.items()}


def weekly_panel(weekly: dict) -> tuple:
    """`(sorted_dates, {date: {series: value}})` для bisect-as-of."""
    by_date = {}
    for series, points in weekly.items():
        for day, value in points:
            by_date.setdefault(day, {})[series] = value
    return sorted(by_date), by_date


def available_from_weekly(day: date) -> date:
    """Публикация недельной точки: `date + 2` (по средам)."""
    return day + timedelta(days=2)


def eligible_weeks(panel: tuple, cutoff: date) -> list:
    """До 4 последних недель с `available_from_weekly(date) ≤ cutoff`."""
    dates, by_date = panel
    boundary = cutoff - timedelta(days=2)
    position = bisect.bisect_right(dates, boundary)
    return [(day, by_date[day]) for day in dates[max(0, position - WEEKLY_WINDOW_WEEKS) : position]]


def weekly_features(panel: tuple, cutoff: date) -> dict:
    """4 diffusion-признака; NaN, если недель/бензина нет.

    Отличие от v1 `weekly_features`: окно — до 4 доступных недель (не ровно 4),
    `wk_gasoline4` — NaN при отсутствии бензина, а не пропуск месяца. Это
    позволяет держать long-выборку с разреженными неделями; отличие названо в
    спеке §3.2.
    """
    result = {name: float("nan") for name in WEEKLY_FEATURES}
    weeks = eligible_weeks(panel, cutoff)
    if not weeks:
        return result
    last_changes = [float(value) - 100.0 for value in weeks[-1][1].values()]
    if last_changes:
        result["wk_share_rising"] = sum(
            1 for change in last_changes if change > 0
        ) / len(last_changes)
        result["wk_median"] = statistics.median(last_changes)
    medians = [
        statistics.median([float(value) - 100.0 for value in positions.values()])
        for _, positions in weeks
        if positions
    ]
    if medians:
        result["wk_median4"] = sum(medians) / len(medians)
    gasoline = [
        float(positions[WEEKLY_GASOLINE]) - 100.0
        for _, positions in weeks
        if WEEKLY_GASOLINE in positions
    ]
    if gasoline:
        result["wk_gasoline4"] = sum(gasoline)
    return result


# ---------------------------------------------------------------------------
# Выборка
# ---------------------------------------------------------------------------


def feature_names_v3() -> list:
    """v2-база + 4 недельных признака перед `month`."""
    names = cm.feature_names(with_mp=True)
    return names[:-1] + list(WEEKLY_FEATURES) + [names[-1]]


def build_sample_v3(sample: str) -> tuple:
    """Строки v3 и пропущенные месяцы с причинами."""
    if sample not in SAMPLE_BOUNDS:
        raise ValueError(f"неизвестная выборка {sample!r}")
    first, last = SAMPLE_BOUNDS[sample]
    targets = cm.load_targets()
    survey = cm.load_survey()
    usd = cm.load_usd()
    key_rate = cm.load_key_rate()
    fao = cm.load_fao()
    panel = weekly_panel(load_weekly())
    index = cm.target_index(targets)
    rows = []
    skipped = []
    for month in cm.sample_months(targets):
        if month < first or month > last:
            continue
        cutoff = cm.cutoff_for(month)
        base, reason = cm.base_features(month, index, usd, key_rate, fao, cutoff)
        if base is None:
            skipped.append((month, reason))
            continue
        mp_part, reason = cm.mp_features(month, survey, cutoff)
        if mp_part is None:
            skipped.append((month, reason))
            continue
        weekly_part = weekly_features(panel, cutoff)
        y = {
            code: values[cm.month_end(month)]
            for code, values in index.items()
        }
        rows.append(
            cm.Row(
                month=month,
                y=y,
                features={**base, **mp_part, **weekly_part},
            )
        )
    return rows, skipped


def assert_no_leakage_v3(rows: list) -> None:
    """Cutoff месяца строго раньше публикации его собственной метки."""
    for row in rows:
        if cm.cutoff_for(row.month) >= cm.available_from_mes(cm.month_end(row.month)):
            raise AssertionError(
                f"утечка: cutoff {cm.cutoff_for(row.month)} не раньше метки "
                f"{cm.month_end(row.month)}"
            )


# ---------------------------------------------------------------------------
# Модели
# ---------------------------------------------------------------------------


def model_names(with_weekly: bool) -> list:
    return feature_names_v3() if with_weekly else cm.feature_names(with_mp=True)


def feature_matrix(rows: list, names: list) -> np.ndarray:
    return np.array(
        [[row.features[name] for name in names] for row in rows], dtype=object
    )


def fit_catboost(
    train: list,
    row: cm.Row,
    target: str,
    with_weekly: bool,
    **overrides,
) -> float:
    """CatBoost на `target` с v3-признаками; дефолты, seed, `month` категориальным."""
    names = model_names(with_weekly)
    params = {
        "loss_function": "RMSE",
        "random_seed": SEED,
        "verbose": False,
        "allow_writing_files": False,
        "cat_features": [names.index("month")],
    }
    params.update(overrides)
    model = CatBoostRegressor(**params)
    model.fit(
        feature_matrix(train, names),
        np.asarray([float(item.y[target]) for item in train], dtype=float),
    )
    return float(model.predict(feature_matrix([row], names))[0])


def fit_v2_baseline(train: list, row: cm.Row) -> float:
    return cm.fit_catboost_direct_mp(train, row)


def fit_v3_direct(train: list, row: cm.Row) -> float:
    return fit_catboost(train, row, cm.TARGET_ALL, True)


def fit_v3_bottomup(train: list, row: cm.Row) -> float:
    weights = cm.nnls_weights(train)
    components = [
        fit_catboost(train, row, code, True) for code in cm.BOTTOMUP_COMPONENTS
    ]
    return float(sum(weight * value for weight, value in zip(weights, components)))


def _impute_matrix(train: list, row: cm.Row, names: list) -> tuple:
    """Медиана train по каждому признаку; NaN → медиана, затем стандартизация."""
    train_matrix = np.asarray(
        [[float(item.features[name]) for name in names] for item in train],
        dtype=float,
    )
    row_matrix = np.asarray(
        [[float(row.features[name]) for name in names]], dtype=float
    )
    medians = np.nanmedian(train_matrix, axis=0)
    medians = np.where(np.isnan(medians), 0.0, medians)
    train_matrix = np.where(np.isnan(train_matrix), medians, train_matrix)
    row_matrix = np.where(np.isnan(row_matrix), medians, row_matrix)
    mean = train_matrix.mean(axis=0)
    std = train_matrix.std(axis=0)
    std = np.where(std == 0.0, 1.0, std)
    return (train_matrix - mean) / std, (row_matrix - mean) / std


def fit_enet_v3(train: list, row: cm.Row, with_weekly: bool) -> float:
    names = [name for name in model_names(with_weekly) if name != "month"]
    train_matrix, row_matrix = _impute_matrix(train, row, names)
    target = np.asarray([float(item.y[cm.TARGET_ALL]) for item in train], dtype=float)
    model = ElasticNetCV(
        l1_ratio=[0.1, 0.5, 0.9, 1.0],
        cv=5,
        random_state=SEED,
        max_iter=10000,
    )
    model.fit(train_matrix, target)
    return float(model.predict(row_matrix)[0])


def fit_b3_weekly(train: list, row: cm.Row) -> float:
    return fit_enet_v3(train, row, True)


MODELS = {
    "v2_baseline": fit_v2_baseline,
    "v2_bottomup": cm.fit_catboost_bottomup_mp,
    "v3_direct": fit_v3_direct,
    "v3_bottomup": fit_v3_bottomup,
    "B1": cm.predict_last,
    "B2": cm.fit_ar1,
    "B3": cm.fit_enet_mp,
    "B3_weekly": fit_b3_weekly,
}


# ---------------------------------------------------------------------------
# Оценка
# ---------------------------------------------------------------------------


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
        return float("nan")
    differences = [a * a - b * b for a, b in zip(err_a, err_b)]
    sd = float(np.std(differences, ddof=1))
    rmse_a = eu.rmse(err_a)
    rmse_b = eu.rmse(err_b)
    mde_mse = 2.802 * sd / math.sqrt(len(differences))
    return mde_mse / (rmse_a + rmse_b) if (rmse_a + rmse_b) else float("nan")


def evaluate(rows: list, start: int, embargo: int, models: dict = None) -> tuple:
    models = dict(MODELS) if models is None else dict(models)
    predictions = cm.walk_forward(rows, models, start, embargo)
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
    for left, right in (
        ("v3_direct", "v2_baseline"),
        ("v3_bottomup", "v2_bottomup"),
        ("B3_weekly", "B3"),
        ("v3_direct", "B3"),
        ("v3_bottomup", "v3_direct"),
        ("v3_direct", "B3_weekly"),
    ):
        if left not in models or right not in models:
            continue
        months_left, errors_left, _ = _errors(predictions, left)
        months_right, errors_right, _ = _errors(predictions, right)
        common = sorted(set(months_left) & set(months_right))
        if not common:
            pairs[f"{left} vs {right}"] = {"n": 0}
            continue
        by_month_left = dict(zip(months_left, errors_left))
        by_month_right = dict(zip(months_right, errors_right))
        err_left = [by_month_left[month] for month in common]
        err_right = [by_month_right[month] for month in common]
        dm_stat, dm_p = eu.diebold_mariano(err_left, err_right, 1)
        boot = eu.block_bootstrap_rmse_diff(
            err_left, err_right, BOOTSTRAP_BLOCK, BOOTSTRAP_N, SEED
        )
        pairs[f"{left} vs {right}"] = {
            "n": len(common),
            "rmse_left": eu.rmse(err_left),
            "rmse_right": eu.rmse(err_right),
            "delta": boot.rmse_diff,
            "delta_ci": [boot.low, boot.high],
            "dm_stat": dm_stat,
            "dm_p": dm_p,
            "mde_ratio": _mde_ratio(err_left, err_right),
        }
    return summary, pairs, predictions


# ---------------------------------------------------------------------------
# Артефакты
# ---------------------------------------------------------------------------


def write_report(path: Path, payload: dict) -> None:
    lines = ["# CPI monthly v3: публичные недельные признаки — отчёт прогона", ""]
    lines.append(
        "> `scripts/cpi_monthly_v3.py`; пункт `key-rate/phase-4-ml-components` "
        "(issue #76), Stage 1. Числа не редактируются руками."
    )
    lines.append("")
    lines.append(f"- Выборка: `{payload['sample']}`; строк: {payload['n_rows']}; "
                 f"start={payload['start']}; embargo={payload['embargo']}")
    lines.append(f"- Пропущено: {payload['n_skipped']}")
    lines.append("")
    lines.append("## Модели")
    lines.append("")
    lines.append("| Модель | n | RMSE | MAE |")
    lines.append("|---|---:|---:|---:|")
    for model, item in payload["summary"].items():
        if not item.get("n"):
            lines.append(f"| {model} | 0 | — | — |")
            continue
        lines.append(
            f"| {model} | {item['n']} | {item['rmse']:.6f} | {item['mae']:.6f} |"
        )
    lines.append("")
    lines.append("## Парные сравнения")
    lines.append("")
    lines.append("| Пара | n | RMSE left | RMSE right | Δ | 95 % CI Δ | DM p | MDE (ratio) |")
    lines.append("|---|---:|---:|---:|---:|---|---:|---:|")
    for name, item in payload["pairs"].items():
        if not item.get("n"):
            lines.append(f"| {name} | 0 | — | — | — | — | — | — |")
            continue
        lines.append(
            f"| {name} | {item['n']} | {item['rmse_left']:.6f} | "
            f"{item['rmse_right']:.6f} | {item['delta']:+.6f} | "
            f"[{item['delta_ci'][0]:+.6f}; {item['delta_ci'][1]:+.6f}] | "
            f"{item['dm_p']:.4f} | {item['mde_ratio']:.4f} |"
        )
    lines.append("")
    lines.append("## Что не проверено")
    lines.append("")
    lines.append("- 149 недельных рядов, 118 нормализованных совпадений с весами, "
                 "покрытие 39,3 %; полный 88-групповой bottom-up не заявляется.")
    lines.append("- Недельные ряды до 2020 разрежены: `weekly` — primary, `long` — робастность.")
    lines.append("- CB_COMP не переобучался; сравнение с ним — отдельный контур #190/#221.")
    lines.append("- As-of недельной точки — `date + 2`; фактическая задержка публикации "
                 "может отличаться.")
    path.write_text("\n".join(lines), encoding="utf-8")


def write_json(path: Path, payload: dict) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Прогон
# ---------------------------------------------------------------------------


def run(
    sample: str,
    out_dir: Path,
    start: int = DEFAULT_START,
    embargo: int = 0,
    models: list = None,
) -> dict:
    selected = (
        dict(MODELS)
        if models is None
        else {name: MODELS[name] for name in models if name in MODELS}
    )
    unknown = set(models or ()) - set(MODELS)
    if unknown:
        raise SystemExit(f"неизвестные модели: {sorted(unknown)}; доступны {list(MODELS)}")
    rows, skipped = build_sample_v3(sample)
    if not rows:
        raise SystemExit(f"пустая выборка {sample!r}")
    assert_no_leakage_v3(rows)
    print(
        f"Выборка {sample}: {len(rows)} строк "
        f"({rows[0].month}…{rows[-1].month}), пропущено {len(skipped)}",
        flush=True,
    )
    summary, pairs, predictions = evaluate(rows, start, embargo, selected)
    payload = {
        "sample": sample,
        "n_rows": len(rows),
        "n_skipped": len(skipped),
        "start": start,
        "embargo": embargo,
        "models": list(selected),
        "summary": summary,
        "pairs": pairs,
        "features": feature_names_v3(),
        "seed": SEED,
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "cpi_monthly_v3_report.md"
    json_path = out_dir / "cpi_monthly_v3_result.json"
    write_report(report_path, payload)
    write_json(json_path, payload)

    if os.environ.get("ML_LOG") == "1":
        try:
            logger = mlx.start_run(
                experiment="cpi_monthly_v3",
                run_name=f"cpi_monthly_v3__{sample}__start{start}__embargo{embargo}",
                config={
                    "sample": sample,
                    "start": start,
                    "embargo": embargo,
                    "seed": SEED,
                    "features": feature_names_v3(),
                    "models": list(selected),
                },
                data_asof=cm.SAMPLE_BOUNDS[cm.DEFAULT_SAMPLE][1],
                sample_meta={
                    "rows": len(rows),
                    "first": rows[0].month.isoformat(),
                    "last": rows[-1].month.isoformat(),
                    "skipped": len(skipped),
                },
                sample=[
                    {
                        "month": row.month.isoformat(),
                        "y": mlx.json_safe(row.y),
                        "features": mlx.json_safe(row.features),
                    }
                    for row in rows
                ],
                spec_path=(
                    "docs/superpowers/specs/"
                    "2026-10-19-cpi-components-public-data-design.md"
                ),
                enabled=True,
            )
            logger.log_metrics({"sample": sample, "summary": summary, "pairs": pairs})
            logger.log_predictions(
                [
                    mlx.PredictionRow(
                        model=item.model,
                        window="h1",
                        target_date=item.month,
                        predicted=item.y_pred,
                        actual=item.y_true,
                        unit="pp",
                        meta={"sample": sample},
                    )
                    for item in predictions
                ]
            )
            logger.log_artifacts([report_path, json_path])
            logger.finish("finished", f"sample={sample}; rows={len(rows)}")
        except Exception as exc:
            print(f"Журнал эксперимента не записан: {type(exc).__name__}: {exc}", flush=True)
    else:
        logger = None

    print("Модели:", flush=True)
    for model, item in summary.items():
        if item.get("n"):
            print(f"  {model}: RMSE {item['rmse']:.6f}, MAE {item['mae']:.6f}", flush=True)
    print("Пары:", flush=True)
    for name, item in pairs.items():
        if item.get("n"):
            print(
                f"  {name}: Δ {item['delta']:+.6f} "
                f"[{item['delta_ci'][0]:+.6f}; {item['delta_ci'][1]:+.6f}], "
                f"DM p={item['dm_p']:.4f}, MDE {item['mde_ratio']:.4f}",
                flush=True,
            )
    print(f"Артефакты: {report_path}, {json_path}", flush=True)
    return payload


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="CPI monthly v3: публичные недельные признаки (issue #76)"
    )
    parser.add_argument("--sample", choices=tuple(SAMPLE_BOUNDS), default="weekly")
    parser.add_argument("--out-dir", default=str(ROOT / "build" / "cpi-monthly-v3"))
    parser.add_argument("--start", type=int, default=DEFAULT_START)
    parser.add_argument("--embargo", type=int, default=0)
    parser.add_argument(
        "--models",
        default=None,
        help="через запятую; по умолчанию все модели (см. MODELS)",
    )
    arguments = parser.parse_args(argv)
    run(
        sample=arguments.sample,
        out_dir=Path(arguments.out_dir),
        start=arguments.start,
        embargo=arguments.embargo,
        models=arguments.models.split(",") if arguments.models else None,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
