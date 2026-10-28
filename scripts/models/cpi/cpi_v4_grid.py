#!/usr/bin/env python3
"""Сетка 3 таргета × h1/h3/h6: оценка моделей (эксперимент 1, #396 Task 5).

Пункт `key-rate/phase-4-cpi-v4-weekly-aggregate-core`, роль ML. Спека
`docs/superpowers/specs/2026-10-28-cpi-v4-weekly-aggregate-core-design.md`
§4–§6 и поправка §5.1 (состав моделей), план Task 5.

Движок общий для всех ячеек: строки origin `O` (признаки на `cutoff_for(O)`)
получают накопленную цель `O+1…O+h`; обучение — только origin'ы `O' ≤ O−h−1`;
модели — прежний код `cpi_monthly`/`cpi_monthly_v3` без копирования плюс
CatBoost с явным списком признаков для `v4a_*`/`v4b_*`. На `S_L09` признаки
`usd_mm`/`key_rate` убираются у всех моделей подменой `cpi_monthly.feature_names`
на время прогона.

Скрипт только читает ClickHouse; в БД и `forecast_log` не пишет. Паспорт —
`ML_LOG=1` (эксперимент `cpi_v4`), MLflow — как у `cpi_monthly_v3`.

Запуск — `make cpi-v4-grid ARGS="--sample S_L09"`, тесты — `make cpi-v4-grid-test`.
"""

import argparse
import contextlib
import json
import math
import os
import sys
import time
from datetime import date
from pathlib import Path

import numpy as np
from catboost import CatBoostRegressor

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from lib import eval_utils as eu
from lib import ml_experiment as mlx
from models.cpi import cpi_monthly as cm
from models.cpi import cpi_monthly_v3 as v3
from models.cpi import cpi_weekly_aggregate as agg
import cpi_v4_leaderboard as lb

SPEC_PATH = "docs/superpowers/specs/2026-10-28-cpi-v4-weekly-aggregate-core-design.md"
SEED = lb.SEED
MIN_COMMON_FOLDS = 12
QUICK_ORIGINS = 18

MODEL_SETS = {
    "nsa": ("B1", "B2", "B3", "v2_bottomup", "v3_bottomup", "v4a_bottomup", "v4b_bottomup"),
    "direct": ("B1", "B2", "B3", "v2_baseline", "v3_direct", "v4a_direct", "v4b_direct"),
}
REFERENCE_MODEL = {"T_NSA": "v2_bottomup", "T_SA": "v2_baseline", "T_CORE": "v2_baseline"}

SAMPLE_CONFIG = {
    "S_L09": {"min_train": 60, "primary_from": date(2016, 1, 1)},
    "S_L13": {"min_train": 60, "primary_from": date(2018, 10, 1)},
    "S_W22": {"min_train": 24, "primary_from": None},
}


# ---------------------------------------------------------------------------
# Чистый слой: цели, purge, walk-forward, оценка ячейки, паспорт
# ---------------------------------------------------------------------------


def model_names_for(target: str) -> tuple:
    if target == "T_NSA":
        return MODEL_SETS["nsa"]
    if target in ("T_SA", "T_CORE"):
        return MODEL_SETS["direct"]
    raise ValueError(f"неизвестный таргет {target!r}")


def build_origin_rows(rows: list, target: str, horizon: int, series: dict, components=None) -> dict:
    """`{origin: Row}` с накопленной целью `O+1…O+h` в `y["cpi_all"]`.

    `T_NSA` дополнительно получает накопленные компоненты (для bottom-up) и
    сохраняет лаги строки; `T_SA`/`T_CORE` заменяют `y_lag1/2` значениями
    самого таргета за `O−1/O−2`. Origin без полного окна или без лагов
    отбрасывается.
    """
    out = {}
    for row in rows:
        origin = row.month
        cumulative = lb.cumulative_pp(series, lb.shift_month(origin, 1), horizon)
        if cumulative is None:
            continue
        y = {"cpi_all": cumulative}
        if target == "T_NSA":
            complete = True
            for code, component in (components or {}).items():
                value = lb.cumulative_pp(component, lb.shift_month(origin, 1), horizon)
                if value is None:
                    complete = False
                    break
                y[code] = value
            if not complete:
                continue
            features = row.features
        else:
            lag1 = series.get(lb.shift_month(origin, -1))
            lag2 = series.get(lb.shift_month(origin, -2))
            if lag1 is None or lag2 is None:
                continue
            features = {**row.features, "y_lag1": float(lag1), "y_lag2": float(lag2)}
        out[origin] = cm.Row(month=origin, y=y, features=features)
    return out


def make_b1(series: dict, horizon: int):
    """B1: накопленный рост за последние h опубликованных месяцев `O−h…O−1`."""

    def fit_predict(train, row):
        value = lb.cumulative_pp(series, lb.shift_month(row.month, -horizon), horizon)
        if value is None:
            raise cm.DegenerateWindowError(f"B1: нет окна {horizon} мес. перед {row.month}")
        return value

    return fit_predict


def assert_no_lookahead_grid(train_origins: list, origin: date, horizon: int) -> None:
    """Цель каждого train-origin `O'` (до `O'+h`) наблюдена до месяца `O`."""
    boundary = lb.shift_month(origin, -1)
    for train_origin in train_origins:
        if lb.shift_month(train_origin, horizon) > boundary:
            raise AssertionError(
                f"утечка h{horizon}: train-origin {train_origin} имеет цель до "
                f"{lb.shift_month(train_origin, horizon)} позже наблюдённого {boundary}"
            )


def run_walk_forward(origin_rows: dict, models: dict, horizon: int, min_train: int, eval_from=None) -> list:
    """Расширяющийся walk-forward; `[{model, origin, horizon, pred, actual}]`.

    `DegenerateWindowError` — пропуск фолда этой модели; прочие исключения
    пробрасываются (баг или порча данных). `eval_from` ограничивает только
    тестовые origin'ы (обучение — полное).
    """
    origins = sorted(origin_rows)
    predictions = []
    for origin in origins:
        if eval_from is not None and origin < eval_from:
            continue
        train_origins = lb.purged_train_origins(origins, origin, horizon)
        assert_no_lookahead_grid(train_origins, origin, horizon)
        if len(train_origins) < min_train:
            continue
        train = [origin_rows[item] for item in train_origins]
        row = origin_rows[origin]
        for name, fit_predict in models.items():
            try:
                value = float(fit_predict(train, row))
            except cm.DegenerateWindowError:
                continue
            if not math.isfinite(value):
                raise ValueError(f"{name}: нечисловой прогноз на {origin}")
            predictions.append(
                {
                    "model": name,
                    "origin": origin.isoformat(),
                    "horizon": horizon,
                    "pred": value,
                    "actual": float(row.y["cpi_all"]),
                }
            )
    return predictions


def evaluate_cell(predictions: list, horizon: int, reference: str, primary_from=None) -> dict:
    """Метрики ячейки на общих фолдах primary-окна: RMSE/MAE, пары, MCS, отпечаток."""
    errors = {}
    for item in predictions:
        errors.setdefault(item["model"], {})[item["origin"]] = item["pred"] - item["actual"]
    floor = primary_from.isoformat() if primary_from else None
    filtered = {
        model: {o: e for o, e in by_origin.items() if floor is None or o >= floor}
        for model, by_origin in errors.items()
    }
    common = lb.common_folds(filtered)
    result = {
        "status": "ok",
        "horizon": horizon,
        "reference": reference,
        "n_common": len(common),
        "predictions_sha256": lb.predictions_digest(predictions),
    }
    if len(common) < MIN_COMMON_FOLDS:
        result["status"] = "insufficient_folds"
        return result
    result["first_origin"] = common[0]
    result["last_origin"] = common[-1]
    aligned = {model: [filtered[model][o] for o in common] for model in sorted(filtered)}
    result["models"] = {
        model: {"n": len(values), "rmse": eu.rmse(values), "mae": eu.mae(values)}
        for model, values in aligned.items()
    }
    pairs = {}
    for model, values in aligned.items():
        if model != reference and reference in aligned:
            pairs[f"{model} vs {reference}"] = lb.compare_pair(values, aligned[reference], horizon)
        if model != "B3" and reference != "B3" and "B3" in aligned and model != reference:
            pairs[f"{model} vs B3"] = lb.compare_pair(values, aligned["B3"], horizon)
    result["pairs"] = pairs
    if len(aligned) >= 3:
        included, pvalues = lb.mcs_included({model: [e * e for e in values] for model, values in aligned.items()})
        result["mcs"] = {"included": sorted(included), "pvalues": pvalues}
    return result


def passport_config(sample: str, targets: list, horizons: list, quick: bool, min_train: int, primary_from) -> dict:
    """Конфиг паспорта run'а (детерминирован; `quick` входит в хеш)."""
    return {
        "sample": sample,
        "targets": list(targets),
        "horizons": list(horizons),
        "quick": bool(quick),
        "min_train": int(min_train),
        "primary_from": primary_from.isoformat() if primary_from else None,
        "seed": SEED,
        "models": {target: list(model_names_for(target)) for target in targets},
        "bootstrap": {"block": lb.BOOTSTRAP_BLOCK, "n": lb.BOOTSTRAP_N},
        "mcs": {"alpha": lb.MCS_ALPHA, "reps": lb.MCS_REPS},
        "spec": SPEC_PATH,
    }


def passport_prediction_rows(predictions: list, target: str, sample: str) -> list:
    """Строки `PredictionRow` для `ML_LOG`: окно `<таргет>:h<h>`, дата — месяц origin."""
    return [
        mlx.PredictionRow(
            model=item["model"],
            window=f"{target}:h{item['horizon']}",
            target_date=date.fromisoformat(item["origin"]),
            predicted=item["pred"],
            actual=item["actual"],
            unit="pp",
            meta={"sample": sample, "horizon": item["horizon"], "origin": item["origin"]},
        )
        for item in predictions
    ]


@contextlib.contextmanager
def feature_override(excluded):
    """Подмена `cpi_monthly.feature_names`: исключённые признаки пропадают у v2/v3-моделей."""
    if not excluded:
        yield
        return
    original = cm.feature_names
    dropped = set(excluded)

    def patched(with_mp: bool) -> list:
        return [name for name in original(with_mp) if name not in dropped]

    cm.feature_names = patched
    try:
        yield
    finally:
        cm.feature_names = original


# ---------------------------------------------------------------------------
# Модели
# ---------------------------------------------------------------------------


def fit_catboost_names(train: list, row, target: str, names: list, **overrides) -> float:
    """CatBoost на `target` с явным списком признаков; дефолты v3, `month` категориальный."""
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
        v3.feature_matrix(train, names),
        np.asarray([float(item.y[target]) for item in train], dtype=float),
    )
    return float(model.predict(v3.feature_matrix([row], names))[0])


def _direct(names):
    return lambda train, row: fit_catboost_names(train, row, cm.TARGET_ALL, names)


def _bottomup(names):
    def fit_predict(train, row):
        weights = cm.nnls_weights(train)
        parts = [fit_catboost_names(train, row, code, names) for code in cm.BOTTOMUP_COMPONENTS]
        return float(sum(weight * value for weight, value in zip(weights, parts)))

    return fit_predict


def make_models(target: str, sample: str, series: dict, horizon: int) -> dict:
    """`{имя: fit_predict}` для ячейки по составу §5.1."""
    v4a = agg.feature_names_v4(sample, with_diffusion=False)
    v4b = agg.feature_names_v4(sample, with_diffusion=True)
    common = {"B1": make_b1(series, horizon), "B2": cm.fit_ar1, "B3": cm.fit_enet_mp}
    if target == "T_NSA":
        extra = {
            "v2_bottomup": cm.fit_catboost_bottomup_mp,
            "v3_bottomup": v3.fit_v3_bottomup,
            "v4a_bottomup": _bottomup(v4a),
            "v4b_bottomup": _bottomup(v4b),
        }
    else:
        extra = {
            "v2_baseline": v3.fit_v2_baseline,
            "v3_direct": v3.fit_v3_direct,
            "v4a_direct": _direct(v4a),
            "v4b_direct": _direct(v4b),
        }
    models = {**common, **extra}
    return {name: models[name] for name in model_names_for(target)}


# ---------------------------------------------------------------------------
# Прогон сетки на данных (только чтение ClickHouse)
# ---------------------------------------------------------------------------


def _component_series() -> dict:
    targets = cm.load_targets()
    return {
        code: {cm.month_of(day): float(value) for day, value in targets[code]}
        for code in cm.BOTTOMUP_COMPONENTS
    }


def _write_cell(cell_dir: Path, target: str, horizon: int, result: dict, predictions: list) -> None:
    cell_dir.mkdir(parents=True, exist_ok=True)
    (cell_dir / f"cell_{target}_h{horizon}.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8"
    )
    with (cell_dir / f"cell_{target}_h{horizon}_predictions.csv").open("w", encoding="utf-8") as handle:
        handle.write("model,origin,horizon,pred,actual\n")
        for item in sorted(predictions, key=lambda r: (r["model"], r["origin"])):
            handle.write(f"{item['model']},{item['origin']},{item['horizon']},{item['pred']!r},{item['actual']!r}\n")


def run_cell(rows: list, series_by_target: dict, components: dict, sample: str, target: str, horizon: int, quick: bool) -> tuple:
    config = SAMPLE_CONFIG[sample]
    series = series_by_target[target]
    origin_rows = build_origin_rows(rows, target, horizon, series, components if target == "T_NSA" else None)
    origins = sorted(origin_rows)
    eval_from = None
    primary_from = config["primary_from"]
    if quick and origins:
        eval_from = lb.shift_month(origins[-1], -(QUICK_ORIGINS - 1))
        primary_from = max(primary_from, eval_from) if primary_from else eval_from
    models = make_models(target, sample, series, horizon)
    excluded = agg.EXCLUDED_BASE_FEATURES.get(sample, ())
    started = time.time()
    with feature_override(excluded):
        predictions = run_walk_forward(origin_rows, models, horizon, config["min_train"], eval_from)
    result = evaluate_cell(predictions, horizon, REFERENCE_MODEL[target], primary_from)
    result.update(
        {
            "target": target,
            "sample": sample,
            "quick": quick,
            "n_origins": len(origins),
            "min_train": config["min_train"],
            "primary_from": primary_from.isoformat() if primary_from else None,
            "seconds": round(time.time() - started, 1),
        }
    )
    return result, predictions


def finalize(rows: list, sample: str, quick: bool, cell_dir: Path, targets: list, horizons: list) -> dict:
    """Сводка по готовым ячейкам; паспорт `ML_LOG=1`, если готовы все."""
    results, predictions_all = {}, {}
    for target in targets:
        for horizon in horizons:
            path = cell_dir / f"cell_{target}_h{horizon}.json"
            if not path.exists():
                return {"complete": False, "missing": f"{target}:h{horizon}"}
            results[f"{target}:h{horizon}"] = json.loads(path.read_text(encoding="utf-8"))
            csv_path = cell_dir / f"cell_{target}_h{horizon}_predictions.csv"
            items = []
            for line in csv_path.read_text(encoding="utf-8").splitlines()[1:]:
                model, origin, h, pred, actual = line.split(",")
                items.append({"model": model, "origin": origin, "horizon": int(h), "pred": float(pred), "actual": float(actual)})
            predictions_all[(target, horizon)] = items
    config = SAMPLE_CONFIG[sample]
    summary = {
        "complete": True,
        "sample": sample,
        "quick": quick,
        "cells": results,
        "config": passport_config(sample, targets, horizons, quick, config["min_train"], config["primary_from"]),
    }
    json_path = cell_dir / "cpi_v4_grid_result.json"
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    md_path = cell_dir / "cpi_v4_grid_report.md"
    write_report(md_path, summary)
    if os.environ.get("ML_LOG") == "1":
        try:
            logger = mlx.start_run(
                experiment="cpi_v4",
                run_name=f"cpi_v4__{sample}__{'quick' if quick else 'full'}",
                config=summary["config"],
                data_asof=rows[-1].month,
                sample_meta={
                    "rows": len(rows),
                    "first": rows[0].month.isoformat(),
                    "last": rows[-1].month.isoformat(),
                    "quick": quick,
                },
                sample=[
                    {"month": r.month.isoformat(), "y": mlx.json_safe(r.y), "features": mlx.json_safe(r.features)}
                    for r in rows
                ],
                spec_path=SPEC_PATH,
                enabled=True,
            )
            logger.log_metrics({"sample": sample, "quick": quick, "cells": results})
            for (target, _horizon), items in predictions_all.items():
                logger.log_predictions(passport_prediction_rows(items, target, sample))
            logger.log_artifacts([json_path, md_path])
            logger.finish("finished", f"sample={sample}; quick={quick}; rows={len(rows)}")
        except Exception as exc:  # паспорт не должен ронять уже посчитанный результат
            print(f"Журнал эксперимента не записан: {type(exc).__name__}: {exc}", flush=True)
    return summary


def write_report(path: Path, summary: dict) -> None:
    lines = [
        f"# CPI v4: сетка 3 таргета × h1/h3/h6, выборка {summary['sample']} ({'quick' if summary['quick'] else 'full'})",
        "",
        "RMSE/MAE — п.п. накопленного роста `O+1…O+h`, общие фолды primary-окна; Δ — RMSE пары (отрицательная — левая лучше).",
    ]
    for key, cell in summary["cells"].items():
        lines += ["", f"## {key}", ""]
        if cell["status"] != "ok":
            lines.append(f"Статус `{cell['status']}`: общих фолдов {cell['n_common']} < {MIN_COMMON_FOLDS}.")
            continue
        lines.append(f"Фолдов {cell['n_common']} ({cell['first_origin']} … {cell['last_origin']}), эталон `{cell['reference']}`.")
        mcs = cell.get("mcs", {}).get("included", [])
        lines += ["", "| модель | RMSE | MAE | в MCS 90 % |", "|---|---:|---:|:---:|"]
        for model, item in sorted(cell["models"].items(), key=lambda kv: kv[1]["rmse"]):
            lines.append(f"| {model} | {item['rmse']:.4f} | {item['mae']:.4f} | {'да' if model in mcs else 'нет'} |")
        lines += ["", "| пара | Δ RMSE | 95 % CI | DM p | MDE |", "|---|---:|---|---:|---:|"]
        for name, item in cell["pairs"].items():
            lines.append(
                f"| {name} | {item['delta']:+.4f} | [{item['delta_ci'][0]:+.4f}; {item['delta_ci'][1]:+.4f}] "
                f"| {item['dm_p']:.4f} | {item['mde_ratio']:.3f} |"
            )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(sample: str, quick: bool, out_dir: Path, cells=None) -> dict:
    if sample not in SAMPLE_CONFIG:
        raise SystemExit(f"неизвестная выборка {sample!r}; доступны {list(SAMPLE_CONFIG)}")
    targets = list(lb.TARGETS)
    horizons = list(lb.HORIZONS)
    wanted = cells or [(t, h) for t in targets for h in horizons]
    rows, skipped = agg.build_sample_v4(sample, with_diffusion=True)
    if not rows:
        raise SystemExit(f"пустая выборка {sample!r}")
    print(f"Выборка {sample}: {len(rows)} строк ({rows[0].month}…{rows[-1].month}), пропущено {len(skipped)}", flush=True)
    series_by_target = lb.load_target_series()
    components = _component_series()
    cell_dir = out_dir / f"{sample}_{'quick' if quick else 'full'}"
    for target, horizon in wanted:
        result, predictions = run_cell(rows, series_by_target, components, sample, target, horizon, quick)
        _write_cell(cell_dir, target, horizon, result, predictions)
        best = min(result["models"].items(), key=lambda kv: kv[1]["rmse"]) if result["status"] == "ok" else None
        print(
            f"  {target} h{horizon}: {result['status']}, фолдов {result['n_common']}, "
            f"{'лучший ' + best[0] + f' {best[1]['rmse']:.4f}' if best else '—'}, {result['seconds']} с",
            flush=True,
        )
    return finalize(rows, sample, quick, cell_dir, targets, horizons)


def parse_cells(text: str) -> list:
    out = []
    for part in text.split(","):
        target, _, horizon = part.partition(":")
        if target not in lb.TARGETS or int(horizon) not in lb.HORIZONS:
            raise SystemExit(f"неверная ячейка {part!r}; формат T_NSA:1")
        out.append((target, int(horizon)))
    return out


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sample", default="S_L09", choices=tuple(SAMPLE_CONFIG))
    parser.add_argument("--quick", action="store_true", help="последние 18 origin'ов; только для итераций (quick=true в паспорте)")
    parser.add_argument("--cells", default=None, help="например T_NSA:1,T_CORE:3; по умолчанию все девять")
    parser.add_argument("--out-dir", default="build/cpi-v4", type=Path)
    args = parser.parse_args(argv)
    out_dir = args.out_dir if args.out_dir.is_absolute() else ROOT / args.out_dir
    summary = run(args.sample, args.quick, out_dir, parse_cells(args.cells) if args.cells else None)
    print("Сводка:", "готова" if summary.get("complete") else f"не все ячейки ({summary.get('missing')})", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
