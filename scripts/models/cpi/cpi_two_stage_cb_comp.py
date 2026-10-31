#!/usr/bin/env python3
"""W5b: сравнение точности two-stage-линейки с CB_COMP на их тесте 2019-01…2024-07 (#401, #76 Task 5).

Протокол — спека `docs/superpowers/specs/2026-10-29-cpi-two-stage-weekly-sa-design.md` §4 «Поправка
2026-10-30», «Протокол W5b», §6.3. Модели без Stage 1: `v3_direct@P_k`, `v5_direct` (v3 + недельный SA
as-of), `weekly_naive` (h1); варианты `wf` (walk-forward) и `frozen` (как у CB_COMP). Истина —
`TARGET_FULL[h][M]` (`M` — конец окна), строка `O = M−h+1`; цель обучения — `T_SAX` (w = 0,08, W3b).
Архив читается локально (`cb_comp_parity.resolve_archive`); скрипт только читает ClickHouse и архив.
Запуск — `make cpi-two-stage-cb-comp`, тесты — `make cpi-two-stage-cb-comp-test`.
"""

import argparse
import csv
import json
import math
import sys
from datetime import date
from pathlib import Path
from typing import Callable, Optional

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import cpi_two_stage_folds as tf
import cpi_two_stage_s1 as s1

HORIZONS = (1, 2, 3, 6)
PRIMARY_HORIZONS = (1, 3, 6)
TEST_FIRST, TEST_LAST = date(2019, 1, 1), date(2024, 7, 1)
TRAIN_FIRST_M, TRAIN_LAST_M = date(2010, 3, 1), date(2018, 12, 1)
TRAIN_OUTLIER_MONTHS = frozenset([date(2014, 12, 1), date(2015, 1, 1), date(2015, 2, 1), date(2015, 3, 1)])
OUTLIER_TEST_MONTHS = frozenset([date(2020, 4, 1)] + [date(2022, m, 1) for m in range(1, 13)])
FROZEN_VINTAGE_ORIGIN = date(2019, 1, 1)
SA4W_FIRST = date(2016, 1, 1)
MIN_TRAIN = 60
ROW_FIRST = date(2002, 5, 1)
WEIGHT = 0.08  # primary w после калибровки W3b
BONFERRONI_ALPHA = 0.05 / 3
PARITY_RATIO = (0.90, 1.10)
TARGET = "T_SAX"


def _shift(month: date, k: int) -> date:
    index = month.year * 12 + (month.month - 1) + k
    return date(index // 12, index % 12 + 1, 1)


def _months(first: date, last: date) -> list:
    out, current = [], first
    while current <= last:
        out.append(current)
        current = _shift(current, 1)
    return out


TEST_MONTHS = _months(TEST_FIRST, TEST_LAST)
NORMAL_MONTHS = [m for m in TEST_MONTHS if m not in OUTLIER_TEST_MONTHS]


def origin_for(month: date, horizon: int) -> date:
    """Строка `O` для месяца `M` (конец окна): `O = M−h+1` (`cb_comp_parity.forecast_origin`)."""
    return _shift(month, -(horizon - 1))


# ---------------------------------------------------------------------------
# Признаки и строки
# ---------------------------------------------------------------------------


def v3_names() -> list:
    import cpi_monthly_v3 as v3

    return list(v3.feature_names_v3())


def v5_names() -> list:
    names = v3_names()
    return names[:-1] + ["sa4w_last", "sa4w_partial", names[-1]]


def v5_row(row, snap_features: Optional[dict]):
    """Строка `v3` + недельный SA as-of; NaN до 2016-01 и при отсутствии среза (исходная строка не меняется)."""
    import dataclasses

    nan = float("nan")
    use = snap_features is not None and row.month >= SA4W_FIRST
    features = dict(row.features)
    features["sa4w_last"] = snap_features["sa4w_last"] if use else nan
    features["sa4w_partial"] = snap_features["sa4w_partial"] if use else nan
    return dataclasses.replace(row, features=features)


def frozen_train_origins(horizon: int, candidates: list) -> list:
    """Обучающие строки `frozen`: конец окна `M' = O'+h−1 ∈ [2010-03, 2018-12] без 2014-12…2015-03 (как у CB_COMP)."""
    out = []
    for origin in sorted(candidates):
        end = _shift(origin, horizon - 1)
        if TRAIN_FIRST_M <= end <= TRAIN_LAST_M and end not in TRAIN_OUTLIER_MONTHS:
            out.append(origin)
    return out


# ---------------------------------------------------------------------------
# Прогнозы: fit(train_rows, test_rows, target, names) -> [pred]
# ---------------------------------------------------------------------------


def catboost_fit(train: list, test_rows: list, target: str, names: list) -> list:
    """CatBoost с параметрами `cpi_v4_grid.fit_catboost_names` (дефолты v3, `month` категориальный); одна подгонка на пакет строк."""
    import numpy as np
    from catboost import CatBoostRegressor

    import cpi_monthly_v3 as v3
    import cpi_v4_grid as grid

    model = CatBoostRegressor(
        loss_function="RMSE", random_seed=grid.SEED, verbose=False, allow_writing_files=False, cat_features=[names.index("month")]
    )
    model.fit(v3.feature_matrix(train, names), np.asarray([float(r.y[target]) for r in train], dtype=float))
    return [float(v) for v in model.predict(v3.feature_matrix(test_rows, names))]


def window_touches(month: date, horizon: int, excluded) -> bool:
    """Окно накопленной цели `t−h+1…t` пересекает исключённый месяц (#425)."""
    return any(_shift(month, -offset) in excluded for offset in range(horizon))


def wf_predict(rows: dict, data: tf.TargetData, horizon: int, test_months: list, fit: Callable, names: list, release_dates,
               min_train: int = MIN_TRAIN, exclude_months=frozenset()) -> dict:
    """Walk-forward: для каждого `M` одна подгонка на `O = M−h+1`; цели обучения — винтаж тестового origin'а, purge по W3.

    `exclude_months` (по умолчанию пусто — поведение прежнее) убирает из train
    строки, чьё окно `t−h+1…t` пересекает исключённый месяц; используется
    диагностикой гипотезы W5b (#425).
    """
    out = {}
    candidates = sorted(rows)
    for month in test_months:
        origin = origin_for(month, horizon)
        if origin not in rows or origin not in data.sax_vintages:
            continue
        folds = tf.make_folds(TARGET, horizon, [origin], candidates, candidates[0], min_train, release_dates)
        if not folds:
            continue
        keep = [o for o in folds[0].train if not window_touches(o, horizon, exclude_months)]
        values = {o: data.train_value(TARGET, origin, o, horizon) for o in keep}
        train = s1.v3_fold_rows(rows, values, origin, TARGET)
        if len(train) < min_train:
            continue
        out[month] = float(fit(train, [rows[origin]], TARGET, names)[0])
    return out


def frozen_predict(rows: dict, data: tf.TargetData, horizon: int, test_months: list, fit: Callable, names: list) -> dict:
    """Одна подгонка на горизонт: цели — винтаж на origin 2019-01 (данные до 2018-12), как у CB_COMP."""
    if FROZEN_VINTAGE_ORIGIN not in data.sax_vintages:
        raise ValueError("нет винтажа T_SAX на origin 2019-01 для frozen")
    values = {o: data.train_value(TARGET, FROZEN_VINTAGE_ORIGIN, o, horizon) for o in frozen_train_origins(horizon, list(rows))}
    train = s1.v3_fold_rows(rows, values, FROZEN_VINTAGE_ORIGIN, TARGET)
    pairs = [(m, rows[origin_for(m, horizon)]) for m in test_months if origin_for(m, horizon) in rows]
    if not pairs:
        return {}
    preds = fit(train, [row for _, row in pairs], TARGET, names)
    return {m: float(p) for (m, _), p in zip(pairs, preds)}


# ---------------------------------------------------------------------------
# Диагностика гипотезы W5b (#425): шок 2022 в train
# ---------------------------------------------------------------------------

SHOCK_MONTHS = TRAIN_OUTLIER_MONTHS | OUTLIER_TEST_MONTHS
HYPOTHESIS_HORIZONS = (1, 3, 6)
REPRODUCE_RATIO = 1.20
MECHANISM_SHARE = 0.50
W5B_WF_PINNED = {
    "v3_direct": {1: 0.208974, 3: 1.018481, 6: 2.504270},
    "v5_direct": {1: 0.199228, 3: 0.875457, 6: 2.061249},
}
W5B_PIN_TOL = 1e-3


def hypothesis_verdict(rmse_shock: float, rmse_noshock: float, rmse_frozen: float) -> dict:
    """Правило H-W5b (спека #419 §17.5): `reproduce` и `mechanism` → support/partial/reject."""
    ratio = rmse_shock / rmse_frozen if rmse_frozen else float("nan")
    gap_shock = rmse_shock - rmse_frozen
    gap_noshock = rmse_noshock - rmse_frozen
    reduction = (gap_shock - gap_noshock) / gap_shock if gap_shock > 0 else None
    reproduce = ratio >= REPRODUCE_RATIO - 1e-12
    mechanism = None if reduction is None else reduction >= MECHANISM_SHARE - 1e-12
    verdict = "support" if reproduce and mechanism else ("partial" if reproduce or mechanism else "reject")
    return {
        "ratio": ratio, "gap_shock": gap_shock, "gap_noshock": gap_noshock, "reduction": reduction,
        "reproduce": reproduce, "mechanism": mechanism, "verdict": verdict,
    }


def hypothesis_check(archive: Optional[str] = None, runner: Optional[Callable] = None,
                     wa_runner: Optional[Callable] = None, cache_dir: Optional[Path] = None) -> dict:
    """W5b wf с шоками и без: воспроизведение провала и проверка механизма.

    Подготовка данных — те же шаги, что `run()` (винтажи `T_SAX` на origin'ах
    теста, строки v3/v5, срезы недельного SA). Поведение `run()` не меняется:
    это отдельная диагностика.
    """
    import cpi_release_dates as rd
    import cpi_sa_reconciliation as rec
    import cpi_two_stage_targets as tt
    import weekly_sa_asof as wa
    from models.cb_comp import cb_comp_parity as cbp

    root = cbp.resolve_archive(archive)
    base = cbp.check_archive(root)
    target_full = base["target"]
    runner = runner or rec.make_r_runner(tt.FIRST_MONTH)
    wa_runner = wa_runner or wa.make_r_runner()
    release_dates = rd.load_release_dates()
    nsa, util = rec.load_nsa(), rec.load_utilities()
    origins = sorted({origin_for(m, h) for m in TEST_MONTHS for h in HYPOTHESIS_HORIZONS} | {FROZEN_VINTAGE_ORIGIN})
    data = tf.build_target_data(nsa, util, {}, WEIGHT, origins, runner)
    rows_v3 = s1.build_v3_rows(release_dates, first=ROW_FIRST)
    points = wa.load_points()
    snaps = {}
    for origin in sorted(rows_v3):
        if SA4W_FIRST <= origin <= TEST_LAST:
            snap, _ = wa.try_snapshot(points, tf.cutoff(origin, release_dates), wa_runner)
            if snap is not None:
                snaps[origin] = s1.snapshot_features(snap)
    rows_v5 = {o: v5_row(r, snaps.get(o)) for o, r in rows_v3.items()}
    specs = {"v3_direct": (rows_v3, v3_names()), "v5_direct": (rows_v5, v5_names())}

    cache_dir = Path(cache_dir) if cache_dir else None
    predictions = {}
    for horizon in HYPOTHESIS_HORIZONS:
        for model, (rows, names) in specs.items():
            for variant, excluded in (("wf", frozenset()), ("wf_noshock", SHOCK_MONTHS)):
                key = f"{model}_{variant}/h{horizon}"
                path = cache_dir / (key.replace("/", "_") + ".json") if cache_dir else None
                if path is not None and path.exists():
                    predictions[key] = {date.fromisoformat(k): v for k, v in json.loads(path.read_text(encoding="utf-8")).items()}
                    continue
                preds = wf_predict(rows, data, horizon, TEST_MONTHS, catboost_fit, names, release_dates,
                                   exclude_months=excluded)
                predictions[key] = preds
                print(f"{key}: {len(preds)} прогнозов", flush=True)
                if path is not None:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(json.dumps({m.isoformat(): v for m, v in preds.items()}), encoding="utf-8")
            key = f"{model}_frozen/h{horizon}"
            predictions[key] = frozen_predict(rows, data, horizon, TEST_MONTHS, catboost_fit, names)

    truth = {
        h: {m: float(target_full.at[m, h]) for m in TEST_MONTHS
            if m in target_full.index and not math.isnan(float(target_full.at[m, h]))}
        for h in HYPOTHESIS_HORIZONS
    }
    models = {}
    for model in specs:
        per_h = {}
        for horizon in HYPOTHESIS_HORIZONS:
            errors_by_variant = {}
            for variant in ("wf", "wf_noshock", "frozen"):
                months, errors = aligned_errors(predictions[f"{model}_{variant}/h{horizon}"], truth[horizon], NORMAL_MONTHS)
                errors_by_variant[variant] = {month: error for month, error in zip(months, errors)}
            common = sorted(set.intersection(*(set(values) for values in errors_by_variant.values())))

            def rmse_of(variant: str) -> float:
                values = [errors_by_variant[variant][month] for month in common]
                return math.sqrt(sum(e * e for e in values) / len(values)) if values else float("nan")

            rmse_shock = rmse_of("wf")
            rmse_noshock = rmse_of("wf_noshock")
            rmse_frozen = rmse_of("frozen")
            verdict = hypothesis_verdict(rmse_shock, rmse_noshock, rmse_frozen)
            pinned = W5B_WF_PINNED[model][horizon]
            complete = len(common) == len(NORMAL_MONTHS)
            per_h[str(horizon)] = {
                "n": len(common), "normal_complete": complete,
                "n_by_variant": {variant: len(values) for variant, values in errors_by_variant.items()},
                "rmse_shock": rmse_shock, "rmse_noshock": rmse_noshock, "rmse_frozen": rmse_frozen,
                "pin": {"value": pinned, "diff": rmse_shock - pinned,
                        "ok": complete and abs(rmse_shock - pinned) <= W5B_PIN_TOL},
                **verdict,
            }
        h6 = per_h["6"]["verdict"]
        models[model] = {
            "horizons": per_h,
            "verdict_h6": h6,
            "pin_ok": all(per_h[str(h)]["pin"]["ok"] for h in HYPOTHESIS_HORIZONS),
        }
    verdicts = [models[model]["verdict_h6"] for model in models]
    overall = "support" if all(v == "support" for v in verdicts) else ("partial" if any(v != "reject" for v in verdicts) else "reject")
    return {
        "shocks": [m.isoformat() for m in sorted(SHOCK_MONTHS)],
        "horizons": list(HYPOTHESIS_HORIZONS),
        "models": models,
        "verdict": overall,
    }


def weekly_naive(snaps: dict, months: list) -> dict:
    """h1: `sa4w_partial` как прогноз (NaN → `sa4w_last`); строка `O = M` (h1)."""
    out = {}
    for month in months:
        snap = snaps.get(month)
        if snap is None:
            continue
        partial = snap["sa4w_partial"]
        out[month] = float(partial if math.isfinite(partial) else snap["sa4w_last"])
    return out


# ---------------------------------------------------------------------------
# Ошибки, статистика, классификация (спека §6.3)
# ---------------------------------------------------------------------------


def aligned_errors(preds: dict, truth: dict, months: list) -> tuple:
    common = [m for m in months if m in preds and m in truth]
    return common, [preds[m] - truth[m] for m in common]


def paired_errors(ours: dict, cb: dict, truth: dict, months: list) -> tuple:
    common = [m for m in months if m in ours and m in cb and m in truth]
    return common, [ours[m] - truth[m] for m in common], [cb[m] - truth[m] for m in common]


def classify(rmse_ours: float, rmse_cb: float, dm_p: float, delta_ci95: tuple, ratio_ci90: tuple, alpha: float = BONFERRONI_ALPHA) -> str:
    """«лучше»/«хуже» — RMSE, DM p < α (Бонферрони) и 95 % CI ΔRMSE по одну сторону 0; «паритет» — 90 % CI отношения ∈ [0,90; 1,10] и p ≥ 0,05."""
    if rmse_ours < rmse_cb and dm_p < alpha and delta_ci95[1] < 0:
        return "better"
    if rmse_ours > rmse_cb and dm_p < alpha and delta_ci95[0] > 0:
        return "worse"
    if ratio_ci90[0] >= PARITY_RATIO[0] and ratio_ci90[1] <= PARITY_RATIO[1] and dm_p >= 0.05:
        return "parity"
    return "undetermined"


def compare_to_cb(err_ours: list, err_cb: list, horizon: int) -> dict:
    """Парное сравнение с CB_COMP: RMSE/MAE, ΔRMSE (95 % CI), отношение (90 % CI), DM-HLN, класс."""
    import cpi_v4_leaderboard as lb
    from models.cb_comp import cb_comp_parity as cbp

    rmse_cb = math.sqrt(sum(e * e for e in err_cb) / len(err_cb))
    pair = lb.compare_pair(err_ours, err_cb, horizon)
    boot = cbp.block_bootstrap_delta_ratio(err_ours, err_cb, cbp.MARGIN_FRACTION * rmse_cb)
    rmse_ours = pair["rmse_left"]
    return {
        "n": len(err_ours),
        "rmse_ours": rmse_ours,
        "rmse_cb": pair["rmse_right"],
        "mae_ours": sum(abs(e) for e in err_ours) / len(err_ours),
        "mae_cb": sum(abs(e) for e in err_cb) / len(err_cb),
        "ratio": boot.ratio,
        "delta": pair["delta"],
        "delta_ci95": list(pair["delta_ci"]),
        "ratio_ci90": [boot.ratio_low, boot.ratio_high],
        "dm_p": pair["dm_p"],
        "class": classify(rmse_ours, pair["rmse_right"], pair["dm_p"], tuple(pair["delta_ci"]), (boot.ratio_low, boot.ratio_high)),
    }


# ---------------------------------------------------------------------------
# Прогон
# ---------------------------------------------------------------------------


def run(archive: Optional[str] = None, runner: Optional[Callable] = None, wa_runner: Optional[Callable] = None, cache_dir: Optional[Path] = None) -> dict:
    import cpi_release_dates as rd
    import cpi_sa_reconciliation as rec
    import cpi_two_stage_targets as tt
    import weekly_sa_asof as wa
    from models.cb_comp import cb_comp_parity as cbp

    root = cbp.resolve_archive(archive)
    base = cbp.check_archive(root)  # регрессионная база: исключение, если архив изменился
    target_full, cb_sheet = base["target"], base["sheet"]
    runner = runner or rec.make_r_runner(tt.FIRST_MONTH)
    wa_runner = wa_runner or wa.make_r_runner()
    release_dates = rd.load_release_dates()
    nsa, util = rec.load_nsa(), rec.load_utilities()

    origins = sorted({origin_for(m, h) for m in TEST_MONTHS for h in HORIZONS} | {FROZEN_VINTAGE_ORIGIN})
    data = tf.build_target_data(nsa, util, {}, WEIGHT, origins, runner)
    rows_v3 = s1.build_v3_rows(release_dates, first=ROW_FIRST)
    points = wa.load_points()
    snaps = {}
    for origin in sorted(rows_v3):
        if SA4W_FIRST <= origin <= TEST_LAST:
            snap, _ = wa.try_snapshot(points, tf.cutoff(origin, release_dates), wa_runner)
            if snap is not None:
                snaps[origin] = s1.snapshot_features(snap)
    rows_v5 = {o: v5_row(r, snaps.get(o)) for o, r in rows_v3.items()}
    specs = {"v3_direct": (rows_v3, v3_names()), "v5_direct": (rows_v5, v5_names())}

    cache_dir = Path(cache_dir) if cache_dir else None
    predictions = {}
    for horizon in HORIZONS:
        for model, (rows, names) in specs.items():
            for variant in ("wf", "frozen"):
                key = f"{model}_{variant}/h{horizon}"
                path = cache_dir / (key.replace("/", "_") + ".json") if cache_dir else None
                if path is not None and path.exists():
                    predictions[key] = {date.fromisoformat(k): v for k, v in json.loads(path.read_text(encoding="utf-8")).items()}
                    continue
                if variant == "wf":
                    preds = wf_predict(rows, data, horizon, TEST_MONTHS, catboost_fit, names, release_dates)
                else:
                    preds = frozen_predict(rows, data, horizon, TEST_MONTHS, catboost_fit, names)
                predictions[key] = preds
                print(f"{key}: {len(preds)} прогнозов", flush=True)
                if path is not None:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(json.dumps({m.isoformat(): v for m, v in preds.items()}), encoding="utf-8")
    predictions["weekly_naive/h1"] = weekly_naive(snaps, TEST_MONTHS)

    truth = {h: {m: float(target_full.at[m, h]) for m in TEST_MONTHS if m in target_full.index} for h in HORIZONS}
    cb = {h: {m: float(cb_sheet.at[m, h]) for m in TEST_MONTHS if m in cb_sheet.index} for h in HORIZONS}
    own_truth = {h: {m: tf.cumulative(data.sax_final, origin_for(m, h), h) for m in TEST_MONTHS} for h in HORIZONS}
    results = {"comparisons": {}, "regression_base": {"rmse_cb": {str(h): v for h, v in base["rmse_cb"].items()}}, "own_truth": {}}
    for key, preds in predictions.items():
        horizon = int(key.rsplit("/h", 1)[1])
        for window_name, window in (("normal", NORMAL_MONTHS), ("all", TEST_MONTHS)):
            months, e_ours, e_cb = paired_errors(preds, cb[horizon], truth[horizon], window)
            if len(months) >= 10:
                results["comparisons"].setdefault(key, {})[window_name] = compare_to_cb(e_ours, e_cb, horizon)
        own_months, own_errors = aligned_errors(preds, {m: v for m, v in own_truth[horizon].items() if v is not None}, NORMAL_MONTHS)
        if own_errors:
            results["own_truth"][key] = {"n": len(own_errors), "rmse": math.sqrt(sum(e * e for e in own_errors) / len(own_errors))}
    # Справочно: v5_direct против v3_direct (одинаковые месяцы), normal-54.
    results["v5_vs_v3"] = {}
    for variant in ("wf", "frozen"):
        for horizon in HORIZONS:
            a, b = predictions.get(f"v5_direct_{variant}/h{horizon}", {}), predictions.get(f"v3_direct_{variant}/h{horizon}", {})
            months = [m for m in NORMAL_MONTHS if m in a and m in b and m in truth[horizon]]
            if len(months) >= 10:
                results["v5_vs_v3"][f"{variant}/h{horizon}"] = compare_to_cb([a[m] - truth[horizon][m] for m in months], [b[m] - truth[horizon][m] for m in months], horizon)
    results["predictions_sha256"] = tf.predictions_digest(
        [{"key": k, "month": m.isoformat(), "pred": v} for k, preds in predictions.items() for m, v in preds.items()]
    )
    results["predictions"] = {k: {m.isoformat(): v for m, v in p.items()} for k, p in predictions.items()}
    return results


def _jsonable(value):
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float):
        return None if not math.isfinite(value) else round(value, 6)
    return value


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--archive", default=None)
    parser.add_argument("--out-dir", type=Path, default=ROOT / "build" / "cpi-two-stage")
    parser.add_argument("--cache-dir", type=Path, default=ROOT / "build" / "cpi-two-stage" / "w5b-cache")
    parser.add_argument("--hypothesis-check", action="store_true",
                        help="только диагностика гипотезы W5b (#425): wf с шоками и без; сравнение с run() не пишется")
    args = parser.parse_args(argv)
    if args.hypothesis_check:
        result = hypothesis_check(args.archive, cache_dir=args.cache_dir)
        args.out_dir.mkdir(parents=True, exist_ok=True)
        path = args.out_dir / "w5b_hypothesis.json"
        path.write_text(json.dumps(_jsonable(result), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(json.dumps(_jsonable({"verdict": result["verdict"],
                                    "models": {m: {h: v["verdict"] for h, v in body["horizons"].items()}
                                               for m, body in result["models"].items()}}),
                         ensure_ascii=False, indent=2))
        return 0
    result = run(args.archive, cache_dir=args.cache_dir)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    predictions = result.pop("predictions")
    (args.out_dir / "cb_comp_compare.json").write_text(json.dumps(_jsonable(result), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    with (args.out_dir / "cb_comp_compare_predictions.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["key", "month", "pred"])
        for key in sorted(predictions):
            for month in sorted(predictions[key]):
                writer.writerow([key, month, repr(predictions[key][month])])
    summary = {k: {w: (v["class"], round(v["ratio"], 3)) for w, v in windows.items() if w == "normal"} for k, windows in result["comparisons"].items()}
    print(json.dumps(summary, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
