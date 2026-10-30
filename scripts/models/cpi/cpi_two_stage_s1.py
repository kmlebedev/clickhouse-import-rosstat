#!/usr/bin/env python3
"""Stage 1 two-stage CPI: прогноз `T_SAX(M0)` и `T_CORE(M0)` по недельному SA as-of (#401 W4, gate S1).

Протокол — спека `docs/superpowers/specs/2026-10-29-cpi-two-stage-weekly-sa-design.md` §4 W4
(«Протокол W4», зафиксирован до результатов) и §6.1. Строка `O` (h1), фолды и purge —
`cpi_two_stage_folds`; недельный SA — as-of срез W1 (вариант КУ `V0`, W2).

Скрипт только читает ClickHouse; в БД не пишет. Запуск — `make cpi-two-stage-s1`,
тесты — `make cpi-two-stage-s1-test`.
"""

import argparse
import csv
import json
import math
import sys
from datetime import date
from pathlib import Path
from typing import Callable, Optional

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import cpi_two_stage_folds as tf
import cpi_two_stage_w2 as w2
import weekly_sa_asof as wa
from models.cpi import cpi_monthly as cm

FEATURES = (
    "sa4w_last", "saar_last", "sa4w_partial", "saar_partial",
    "wkagg_mtd", "wkagg_cov", "wkagg_mtd_scaled", "wkagg_last", "wkagg_4w",
    "tariff_expected",
    "sax_l1", "sax_l2", "core_l2", "core_l3", "nsa_l1", "nsa_l2",
    "wk_share_rising", "wk_median", "wk_median4", "wk_gasoline4",
    "month",
)
ENET_FEATURES = tuple(name for name in FEATURES if name != "month")
B3_FEATURES = ("sax_l1", "sax_l2", "core_l2", "core_l3", "nsa_l1", "nsa_l2")
WKAGG_NAMES = ("wkagg_mtd", "wkagg_cov", "wkagg_mtd_scaled", "wkagg_last", "wkagg_4w")
SAX_LAGS = ("sax_l1", "sax_l2")
B2_MIN_TRAIN = 14
LAST_KNOWN = {"T_SAX": "sax_l1", "T_CORE": "core_l2", "T_NSA": "nsa_l1"}
BASELINE_ORDER = ("B1", "B2", "B3", "weekly_naive", "v3_direct")
DEV_FIRST = date(2018, 1, 1)
DEV_LAST = date(2021, 12, 1)
HOLDOUT_FIRST = date(2022, 1, 1)
ALPHA = 0.05


class DegenerateWindow(Exception):
    """Недостаточно строк для модели на этом фолде: фолд модели пропускается."""


def _shift(month: date, k: int) -> date:
    index = month.year * 12 + (month.month - 1) + k
    return date(index // 12, index % 12 + 1, 1)


# ---------------------------------------------------------------------------
# Признаки
# ---------------------------------------------------------------------------


def saar(sa_4w: float) -> float:
    """SAAR из `sa_4w` (% за 4 недели): `((1 + x/100)^13 − 1)·100`."""
    if not math.isfinite(sa_4w):
        return float("nan")
    return ((1 + sa_4w / 100) ** wa.ANNUAL_PERIODS - 1) * 100


def snapshot_features(snap: wa.Snapshot) -> dict:
    """`sa4w_last`, `saar_last` (последний полный месяц), `sa4w_partial`, `saar_partial` (NaN без неполного месяца)."""
    last = (snap.sa_complete[-1] / 100 - 1) * 100
    partial = (snap.sa_partial / 100 - 1) * 100 if snap.sa_partial is not None else float("nan")
    return {"sa4w_last": last, "saar_last": saar(last), "sa4w_partial": partial, "saar_partial": saar(partial)}


def tariff_expected(origin: date) -> int:
    """Календарный признак: месяц `O` содержит стандартную дату индексации (январь или июль)."""
    return int(origin.month in (1, 7))


def wkagg_part(points: list, origin: date, cutoff: date) -> dict:
    """`wkagg_*` на cutoff без `tariff_week` (его календарь расходится с данными, W2)."""
    from models.cpi import cpi_weekly_aggregate as agg

    full = agg.wkagg_features(points, origin, cutoff)
    return {name: full[name] for name in WKAGG_NAMES}


def assert_core_available(core_month: date, origin: date, release_dates: Optional[dict]) -> None:
    """Месяц `T_CORE` в признаке должен быть опубликован ЦБ к cutoff строки."""
    if tf.cbr_available(core_month) > tf.cutoff(origin, release_dates):
        raise AssertionError(f"core за {core_month:%Y-%m} недоступен к cutoff {tf.cutoff(origin, release_dates)} строки {origin:%Y-%m}")


def lag_features(origin: date, nsa: dict, core: dict, vintage: Optional[dict], release_dates: Optional[dict]) -> dict:
    """Лаги: `nsa_l1/2` (`O−1`, `O−2`), `core_l2/3` (`O−2`, `O−3`), `sax_l1/2` из винтажа (`O−1`, `O−2`); пропуск — NaN."""
    nan = float("nan")
    for lag in (2, 3):
        assert_core_available(_shift(origin, -lag), origin, release_dates)
    vint = vintage or {}
    return {
        "sax_l1": vint.get(_shift(origin, -1), nan),
        "sax_l2": vint.get(_shift(origin, -2), nan),
        "core_l2": core.get(_shift(origin, -2), nan),
        "core_l3": core.get(_shift(origin, -3), nan),
        "nsa_l1": nsa.get(_shift(origin, -1), nan),
        "nsa_l2": nsa.get(_shift(origin, -2), nan),
    }


def static_features(
    origin: date,
    snap: wa.Snapshot,
    points: list,
    wk_diffusion: dict,
    nsa: dict,
    core: dict,
    release_dates: Optional[dict],
) -> dict:
    """Признаки строки `O`, не зависящие от тестового origin'а (без лагов `T_SAX`, они — из винтажа фолда)."""
    cutoff = tf.cutoff(origin, release_dates)
    lags = lag_features(origin, nsa, core, None, release_dates)
    out = {**snapshot_features(snap), **wkagg_part(points, origin, cutoff), "tariff_expected": float(tariff_expected(origin))}
    out.update({name: value for name, value in lags.items() if name not in SAX_LAGS})
    out.update({name: wk_diffusion.get(name, float("nan")) for name in ("wk_share_rising", "wk_median", "wk_median4", "wk_gasoline4")})
    out["month"] = float(origin.month)
    return out


# ---------------------------------------------------------------------------
# Строки фолда
# ---------------------------------------------------------------------------


def _row(month: date, static: dict, vintage: Optional[dict], y: dict) -> cm.Row:
    vint = vintage or {}
    nan = float("nan")
    features = {**static, "sax_l1": vint.get(_shift(month, -1), nan), "sax_l2": vint.get(_shift(month, -2), nan)}
    features["month"] = int(features["month"])  # CatBoost: категориальный признак — целое
    return cm.Row(month=month, y=y, features={name: features[name] for name in FEATURES})


def fold_rows(static: dict, data: tf.TargetData, target: str, fold: tf.Fold):
    """`(train_rows, test_row, truth)` или `None`: цели — из винтажа тестового origin'а (`T_SAX`) / последней редакции."""
    truth = data.truth(target, fold.origin, 1)
    if fold.origin not in static or truth is None:
        return None
    vintage = data.sax_vintages.get(fold.origin)
    train = []
    for origin in fold.train:
        value = data.train_value(target, fold.origin, origin, 1)
        if origin not in static or value is None:
            continue
        train.append(_row(origin, static[origin], vintage, {target: value}))
    return train, _row(fold.origin, static[fold.origin], vintage, {target: truth}), truth


# ---------------------------------------------------------------------------
# Модели и baselines: fn(train_rows, test_row, target) -> float
# ---------------------------------------------------------------------------


def _matrix(rows: list, names: tuple) -> np.ndarray:
    return np.asarray([[float(row.features[name]) for name in names] for row in rows], dtype=float)


def _target(rows: list, target: str) -> np.ndarray:
    return np.asarray([float(row.y[target]) for row in rows], dtype=float)


def predict_b1(train: list, row: cm.Row, target: str) -> float:
    return float(row.features[LAST_KNOWN[target]])


def predict_weekly_naive(train: list, row: cm.Row, target: str) -> float:
    partial = row.features["sa4w_partial"]
    return float(partial if math.isfinite(partial) else row.features["sa4w_last"])


def predict_b2(train: list, row: cm.Row, target: str) -> float:
    """AR(1) по последнему известному значению + сезонные дамми месяца (OLS, ≥ 14 строк)."""
    name = LAST_KNOWN[target]
    usable = [r for r in train if math.isfinite(r.features[name]) and math.isfinite(r.y[target])]
    if len(usable) < B2_MIN_TRAIN:
        raise DegenerateWindow(f"B2: {len(usable)} строк < {B2_MIN_TRAIN}")

    def design(r):
        dummies = [1.0 if int(r.features["month"]) == m else 0.0 for m in range(2, 13)]
        return [1.0, float(r.features[name])] + dummies

    x = np.asarray([design(r) for r in usable], dtype=float)
    coef = np.linalg.lstsq(x, np.asarray([float(r.y[target]) for r in usable]), rcond=None)[0]
    return float(np.asarray(design(row)) @ coef)


def _enet(train: list, row: cm.Row, target: str, names: tuple) -> float:
    if len(train) < cm.ENET_MIN_TRAIN:
        raise DegenerateWindow(f"ElasticNetCV: {len(train)} строк < {cm.ENET_MIN_TRAIN}")
    return w2._fit_predict(_matrix(train, names), _target(train, target), _matrix([row], names))


def predict_b3(train: list, row: cm.Row, target: str) -> float:
    return _enet(train, row, target, B3_FEATURES)


def predict_enet(train: list, row: cm.Row, target: str) -> float:
    return _enet(train, row, target, ENET_FEATURES)


def predict_catboost(train: list, row: cm.Row, target: str) -> float:
    import cpi_v4_grid as grid

    return grid.fit_catboost_names(train, row, target, list(FEATURES))


BASELINES = {"B1": predict_b1, "B2": predict_b2, "B3": predict_b3, "weekly_naive": predict_weekly_naive}
STAGE1_MODELS = {"s1_catboost": predict_catboost, "s1_enet": predict_enet}


def walk_forward(target: str, static: dict, data: tf.TargetData, folds: list, models: dict) -> list:
    """`[{model, origin, pred, truth}]`; фолд без истины/строки теста пропускается, вырожденное окно — пропуск модели."""
    out = []
    for fold in folds:
        prepared = fold_rows(static, data, target, fold)
        if prepared is None:
            continue
        train, test, truth = prepared
        for name, fn in models.items():
            try:
                value = float(fn(train, test, target))
            except DegenerateWindow:
                continue
            if not math.isfinite(value):
                raise ValueError(f"{name}: нечисловой прогноз на {fold.origin} ({target})")
            out.append({"model": name, "origin": fold.origin, "pred": value, "truth": truth})
    return out


def v3_fold_rows(rows: dict, values: dict, test_origin: date, target: str) -> list:
    """Строки `v3` (`cm.Row`) обучающих origin'ов с целью, заменённой на `target` (`None` — строка пропускается)."""
    import dataclasses

    out = []
    for origin in sorted(values):
        value = values[origin]
        if value is None or origin not in rows:
            continue
        out.append(dataclasses.replace(rows[origin], y={target: value}))
    return out


# ---------------------------------------------------------------------------
# Кандидат и gate S1 (спека §4 W4 «Протокол W4», §6.1)
# ---------------------------------------------------------------------------


def _by_origin(items: list) -> dict:
    return {p["origin"]: p for p in items}


def _rmse(errors: list) -> float:
    return math.sqrt(sum(e * e for e in errors) / len(errors))


def select_candidate(preds: dict, dev_first: date, dev_last: date) -> str:
    """Модель Stage 1 с минимальным RMSE на общих dev-фолдах (выбор до holdout)."""
    names = [name for name in STAGE1_MODELS if name in preds]
    maps = {name: _by_origin(preds[name]) for name in names}
    common = sorted(set.intersection(*(set(m) for m in maps.values())))
    common = [o for o in common if dev_first <= o <= dev_last]
    if not common:
        raise ValueError("нет общих dev-фолдов у моделей Stage 1")
    scores = {name: _rmse([maps[name][o]["pred"] - maps[name][o]["truth"] for o in common]) for name in names}
    return min(names, key=lambda name: scores[name])


def holm(pvalues: dict, alpha: float = ALPHA) -> dict:
    """Holm: меньшее p — с `α/m`, следующее — с `α/(m−1)` (только если предыдущее отвергнуто); NaN → не отвергается."""
    order = sorted(pvalues, key=lambda k: (math.inf if not math.isfinite(pvalues[k]) else pvalues[k]))
    out = {key: False for key in pvalues}
    stopped = False
    for position, key in enumerate(order):
        p = pvalues[key]
        if stopped or not math.isfinite(p) or p > alpha / (len(order) - position):
            stopped = True
            continue
        out[key] = True
    return out


def gate_target(preds: dict, candidate: str, holdout_first: date, dev_first: date, dev_last: date) -> dict:
    """Условия 1, 3, 4 и статистика для условия 2 по одной цели (спека §6.1); holdout — общие фолды 6 моделей."""
    import cpi_v4_leaderboard as lb

    names = [candidate] + list(BASELINE_ORDER)
    missing = [name for name in names if name not in preds]
    if missing:
        raise ValueError(f"нет предсказаний моделей: {missing}")
    maps = {name: _by_origin(preds[name]) for name in names}
    holdout = sorted(o for o in set.intersection(*(set(m) for m in maps.values())) if o >= holdout_first)
    if not holdout:
        raise ValueError("нет общих holdout-фолдов")
    err = lambda name, origins: [maps[name][o]["pred"] - maps[name][o]["truth"] for o in origins]
    rmse = {name: _rmse(err(name, holdout)) for name in names}
    best = min(BASELINE_ORDER, key=lambda name: rmse[name])
    pair = lb.compare_pair(err(candidate, holdout), err(best, holdout), 1)
    dev = sorted(
        o
        for o in set(maps[candidate]) & set(maps[best]) & set(maps["weekly_naive"])
        if dev_first <= o <= dev_last
    )
    dev_delta = {
        other: (_rmse(err(candidate, dev)) - _rmse(err(other, dev))) if dev else float("nan")
        for other in {best, "weekly_naive"}
    }
    return {
        "candidate": candidate,
        "best_baseline": best,
        "rmse": rmse,
        "n_holdout": len(holdout),
        "n_dev": len(dev),
        "cond1": rmse[candidate] < rmse[best],
        "cond3": rmse[candidate] < rmse["weekly_naive"],
        "cond4": bool(dev) and all(delta < 0 for delta in dev_delta.values()),
        "dev_delta": dev_delta,
        "dm_p": pair["dm_p"],
        "delta": pair["delta"],
        "delta_ci": pair["delta_ci"],
    }


def decide_s1(results: dict) -> dict:
    """`pass` — условия 1–4 по обеим целям; `partial` — по одной; `fail` — иначе (Holm по DM p двух целей)."""
    holm_ok = holm({target: r["dm_p"] for target, r in results.items()})
    passed = [
        target
        for target, r in results.items()
        if r["cond1"] and r["cond3"] and r["cond4"] and holm_ok[target] and r["delta_ci"][1] < 0 and r["delta_ci"][0] <= r["delta_ci"][1]
    ]
    decision = "pass" if len(passed) == len(results) else ("partial" if passed else "fail")
    return {"decision": decision, "passed": passed, "holm": holm_ok}


# ---------------------------------------------------------------------------
# Прогон на живых данных
# ---------------------------------------------------------------------------


def _origins(first: date, last: date) -> list:
    out, current = [], first
    while current <= last:
        out.append(current)
        current = _shift(current, 1)
    return out


def build_v3_rows(release_dates: dict) -> dict:
    """Строки `v3` (`feature_names_v3`) под конвенцией `publication`: МП — последняя доступная точка (режим `live`)."""
    import cpi_monthly_v3 as v3

    targets, survey = cm.load_targets(), cm.load_survey()
    usd, key_rate, fao = cm.load_usd(), cm.load_key_rate(), cm.load_fao()
    panel = v3.weekly_panel(v3.load_weekly())
    index = cm.target_index(targets)
    first = v3.SAMPLE_BOUNDS["long"][0]
    rows = {}
    with cm.asof_convention("publication", release_dates=release_dates):
        for month in cm.sample_months(targets):
            if month < first:
                continue
            cutoff = cm.cutoff_for(month)
            base, _ = cm.base_features(month, index, usd, key_rate, fao, cutoff)
            if base is None:
                continue
            mp_part, _ = cm.mp_features(month, survey, cutoff, live=True)
            if mp_part is None:
                continue
            features = {**base, **mp_part, **v3.weekly_features(panel, cutoff)}
            rows[month] = cm.Row(month=month, y={}, features=features)
    return rows


def walk_forward_v3(target: str, v3_rows: dict, data: tf.TargetData, folds: list) -> list:
    """`v3_direct@P_k`: CatBoost на `feature_names_v3`, цель — `target` (винтаж/последняя редакция по правилам W3)."""
    import cpi_monthly_v3 as v3
    import cpi_v4_grid as grid

    names = v3.feature_names_v3()
    out = []
    for fold in folds:
        truth = data.truth(target, fold.origin, 1)
        if fold.origin not in v3_rows or truth is None:
            continue
        values = {o: data.train_value(target, fold.origin, o, 1) for o in fold.train}
        train = v3_fold_rows(v3_rows, values, fold.origin, target)
        if len(train) < 12:
            continue
        test = v3_rows[fold.origin]
        value = float(grid.fit_catboost_names(train, test, target, names))
        if not math.isfinite(value):
            raise ValueError(f"v3_direct: нечисловой прогноз на {fold.origin} ({target})")
        out.append({"model": "v3_direct", "origin": fold.origin, "pred": value, "truth": truth})
    return out


def run(wa_runner=None, rec_runner=None) -> dict:
    import cpi_release_dates as rd
    import cpi_monthly_v3 as v3
    import cpi_sa_reconciliation as rec

    wa_runner = wa_runner or wa.make_r_runner()
    release_dates = rd.load_release_dates()
    points = wa.load_points()
    nsa, core = rec.load_nsa(), rec.load_cbr_core()
    panel = v3.weekly_panel(v3.load_weekly())

    last_origin = _shift(max(nsa), 1)
    origins = _origins(tf.STAGE1_FIRST_TRAIN, last_origin)
    test_origins = _origins(DEV_FIRST, last_origin)
    data = tf.load_target_data(test_origins, runner=rec_runner)

    static, failures = {}, []
    for origin in origins:
        cutoff = tf.cutoff(origin, release_dates)
        snap, error = wa.try_snapshot(points, cutoff, wa_runner)
        if snap is None:
            failures.append({"origin": origin.isoformat(), "error": error})
            continue
        static[origin] = static_features(origin, snap, points, v3.weekly_features(panel, cutoff), nsa, core, release_dates)

    v3_rows = build_v3_rows(release_dates)
    result = {"failures": failures, "targets": {}, "n_static": len(static)}
    all_preds, oof = {}, []
    for target in ("T_SAX", "T_CORE"):
        folds = tf.make_folds(target, 1, test_origins, sorted(static), tf.STAGE1_FIRST_TRAIN, tf.STAGE1_MIN_TRAIN, release_dates)
        for fold in folds:
            tf.assert_no_lookahead(target, 1, fold, release_dates)
        preds = {}
        for item in walk_forward(target, static, data, folds, {**BASELINES, **STAGE1_MODELS}):
            preds.setdefault(item["model"], []).append(item)
        preds["v3_direct"] = walk_forward_v3(target, v3_rows, data, folds)
        candidate = select_candidate(preds, DEV_FIRST, DEV_LAST)
        gate = gate_target(preds, candidate, HOLDOUT_FIRST, DEV_FIRST, DEV_LAST)
        dev_rmse = {}
        for name, items in preds.items():
            dev_items = [p for p in items if DEV_FIRST <= p["origin"] <= DEV_LAST]
            if dev_items:
                dev_rmse[name] = _rmse([p["pred"] - p["truth"] for p in dev_items])
        result["targets"][target] = {"n_folds": len(folds), "candidate": candidate, "dev_rmse": dev_rmse, "gate": gate}
        for name, items in preds.items():
            for p in items:
                oof.append({"target": target, **p})
        all_preds[target] = preds
    result["decision"] = decide_s1({t: v["gate"] for t, v in result["targets"].items()})
    result["predictions_sha256"] = tf.predictions_digest([{**p, "origin": p["origin"].isoformat()} for p in oof])
    result["oof"] = oof
    return result


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
    parser.add_argument("--out-dir", type=Path, default=ROOT / "build" / "cpi-two-stage")
    args = parser.parse_args(argv)
    result = run()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    oof = result.pop("oof")
    (args.out_dir / "s1_gate.json").write_text(json.dumps(_jsonable(result), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    with (args.out_dir / "s1_oof.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["target", "model", "origin", "pred", "truth"])
        for p in sorted(oof, key=lambda p: (p["target"], p["model"], p["origin"])):
            writer.writerow([p["target"], p["model"], p["origin"].isoformat(), repr(p["pred"]), repr(p["truth"])])
    summary = {t: {"candidate": v["candidate"], "n_holdout": v["gate"]["n_holdout"], "best_baseline": v["gate"]["best_baseline"], "rmse": v["gate"]["rmse"]} for t, v in result["targets"].items()}
    print(json.dumps(_jsonable({"decision": result["decision"], "targets": summary, "sha": result["predictions_sha256"]}), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
