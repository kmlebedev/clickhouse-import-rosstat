#!/usr/bin/env python3
"""Выбор варианта коррекции КУ во входе Stage 1 на dev-окне (#401 W2).

Протокол — спека `docs/superpowers/specs/2026-10-29-cpi-two-stage-weekly-sa-design.md`
§4 W2 («Протокол W2», до результатов): фиксированная `ElasticNetCV` (B3) на
признаках `sa4w_last`, `sa4w_partial`, `y_lag1`, `tariff_expected`; цель — `T_SAX(O)`
из винтажной истории тестового origin'а; истина — окончательный `T_SAX`; train —
origin'ы с 2016-01 до `T−1`; тестовые origin'ы dev — 2018-01…2021-12; holdout 2022+
не читается. Выбирается вариант с минимальным RMSE, но `V1`/`V2` — только если лучше
`V0` не менее чем на 2 %.

Скрипт только читает ClickHouse; в БД не пишет. Запуск — `make cpi-two-stage-w2`,
тесты — `make cpi-two-stage-w2-test`.
"""

import argparse
import csv
import json
import math
import sys
import warnings
from datetime import date
from pathlib import Path
from typing import Optional

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

FIRST_TRAIN = date(2016, 1, 1)
DEV_FIRST_TEST = date(2018, 1, 1)
DEV_LAST_TEST = date(2021, 12, 1)
MIN_TRAIN = 24
SELECTION_TOLERANCE = 0.02  # V1/V2 выбираются, только если RMSE ниже V0 не менее чем на 2 %
FEATURES = ("sa4w_last", "sa4w_partial", "y_lag1", "tariff_expected")
WEIGHT = 0.10


def _prev(month: date) -> date:
    return date(month.year - (month.month == 1), 12 if month.month == 1 else month.month - 1, 1)


def _months(first: date, last: date) -> list:
    out, current = [], first
    while current <= last:
        out.append(current)
        current = date(current.year + current.month // 12, current.month % 12 + 1, 1)
    return out


def select_variant(rmse: dict, tolerance: float = SELECTION_TOLERANCE) -> str:
    """Минимальный RMSE; `V1`/`V2` вместо `V0` — только при улучшении не менее `tolerance` (иначе простейший `V0`)."""
    if "V0" not in rmse:
        raise ValueError("нет RMSE варианта V0")
    best = min(rmse, key=lambda name: rmse[name])
    if best == "V0" or rmse[best] > rmse["V0"] * (1 - tolerance):
        return "V0"
    return best


def _row_vector(origin: date, features: dict, y_lag1: float) -> list:
    tariff_expected = 1.0 if origin.month in (1, 7) else 0.0
    return [features["sa4w_last"], features["sa4w_partial"], y_lag1, tariff_expected]


def _fit_predict(train_x: np.ndarray, train_y: np.ndarray, test_x: np.ndarray) -> float:
    """ElasticNetCV (конфигурация B3 из `cpi_monthly`); импутация медианой и масштаб — только по train."""
    from sklearn.exceptions import ConvergenceWarning
    from sklearn.linear_model import ElasticNetCV
    from sklearn.model_selection import TimeSeriesSplit
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    from models.cpi import cpi_monthly as cm

    medians = np.array([np.nanmedian(col) if np.isfinite(col).any() else 0.0 for col in train_x.T])
    train_filled = np.where(np.isnan(train_x), medians, train_x)
    test_filled = np.where(np.isnan(test_x), medians, test_x)
    model = Pipeline(
        [
            ("scaler", StandardScaler()),
            (
                "enet",
                ElasticNetCV(
                    alphas=cm.ENET_ALPHAS,
                    l1_ratio=list(cm.ENET_L1_RATIOS),
                    cv=TimeSeriesSplit(cm.ENET_CV_SPLITS),
                    random_state=cm.SEED,
                ),
            ),
        ]
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ConvergenceWarning)
        model.fit(train_filled, train_y)
    return float(model.predict(test_filled)[0])


def walk_forward(rows: dict, vintages: dict, final: dict, test_origins: list, first_train: date = FIRST_TRAIN, min_train: int = MIN_TRAIN) -> list:
    """Прогноз `T_SAX(T)` h1 для каждого тестового origin'а `T`.

    `rows[o]` — признаки среза as-of origin'а `o`; `vintages[T]` — винтажная история `T_SAX` на `T`
    (цели обучающих строк и `y_lag1`); `final` — окончательный `T_SAX` (только истина). Train — origin'ы
    `[first_train, T−1]` (purge: цель `T−1` опубликована к `P_{T−1}`); origin'ы без среза или винтажа
    пропускаются; меньше `min_train` строк — фолд пропущен.
    """
    out = []
    for test in test_origins:
        vint = vintages.get(test)
        if vint is None or test not in rows or test not in final:
            continue
        lag = vint.get(_prev(test))
        if lag is None:
            continue
        train_x, train_y = [], []
        for origin in _months(first_train, _prev(test)):
            features = rows.get(origin)
            target, y_prev = vint.get(origin), vint.get(_prev(origin))
            if features is None or target is None or y_prev is None:
                continue
            train_x.append(_row_vector(origin, features, y_prev))
            train_y.append(target)
        if len(train_x) < min_train:
            continue
        test_x = np.asarray([_row_vector(test, rows[test], lag)], dtype=float)
        pred = _fit_predict(np.asarray(train_x, dtype=float), np.asarray(train_y, dtype=float), test_x)
        partial = rows[test]["sa4w_partial"]
        out.append(
            {
                "origin": test,
                "truth": final[test],
                "pred": pred,
                "lag": lag,
                "weekly_naive": partial if math.isfinite(partial) else rows[test]["sa4w_last"],
                "n_train": len(train_x),
            }
        )
    return out


def rmse_by_method(forecasts: list) -> dict:
    return {
        method: math.sqrt(sum((f[method] - f["truth"]) ** 2 for f in forecasts) / len(forecasts))
        for method in ("pred", "lag", "weekly_naive")
    }


# ---------------------------------------------------------------------------
# Прогон на живых данных
# ---------------------------------------------------------------------------


def run(wa_runner=None, rec_runner=None) -> dict:
    import cpi_release_dates as rd
    import cpi_sa_reconciliation as rec
    import cpi_two_stage_targets as tt
    import cpi_v4_leaderboard as lb
    import weekly_sa_asof as wa

    wa_runner = wa_runner or wa.make_r_runner()
    nsa, util = rec.load_nsa(), rec.load_utilities()
    points = wa.load_points()
    release_dates = rd.load_release_dates()
    rec_runner = rec_runner or rec.make_r_runner(tt.FIRST_MONTH)

    origins = _months(FIRST_TRAIN, DEV_LAST_TEST)
    final = tt.sax_final(nsa, util, WEIGHT, rec_runner)
    vintages, skipped_vintages = tt.sax_vintages(nsa, util, WEIGHT, origins, rec_runner)

    rows = {variant: {} for variant in tt.VARIANTS}
    failures = []
    for origin in origins:
        cutoff = wa.publication_cutoff(origin, release_dates)
        known_until = _prev(origin)
        util_known = {m: v for m, v in util.items() if m <= known_until}
        for variant in tt.VARIANTS:
            try:
                snap = tt.asof_variant_snapshot(points, cutoff, wa_runner, variant, util_known, WEIGHT, known_until)
            except (ValueError, RuntimeError) as error:
                failures.append({"origin": origin.isoformat(), "variant": variant, "error": str(error)})
                continue
            last = (snap.sa_complete[-1] / 100 - 1) * 100
            partial = (snap.sa_partial / 100 - 1) * 100 if snap.sa_partial is not None else float("nan")
            rows[variant][origin] = {"sa4w_last": last, "sa4w_partial": partial}

    tests = _months(DEV_FIRST_TEST, DEV_LAST_TEST)
    per_variant = {variant: walk_forward(rows[variant], vintages, final, tests) for variant in tt.VARIANTS}
    common = sorted(set.intersection(*(set(f["origin"] for f in per_variant[v]) for v in tt.VARIANTS)))
    aligned = {v: [f for f in per_variant[v] if f["origin"] in common] for v in tt.VARIANTS}

    rmse = {v: rmse_by_method(aligned[v])["pred"] for v in tt.VARIANTS}
    baselines = rmse_by_method(aligned["V0"])
    errors = {v: [f["pred"] - f["truth"] for f in aligned[v]] for v in tt.VARIANTS}
    pairs = {}
    for variant in ("V1", "V2"):
        pairs[f"{variant}_vs_V0"] = lb.compare_pair(errors[variant], errors["V0"], 1)
    chosen = select_variant(rmse)
    return {
        "protocol": "спека §4 W2 «Протокол W2» (2026-10-30)",
        "weight": WEIGHT,
        "dev_test_origins": [common[0].isoformat(), common[-1].isoformat()] if common else None,
        "n_test": len(common),
        "rmse": rmse,
        "baselines": {"lag": baselines["lag"], "weekly_naive": baselines["weekly_naive"]},
        "pairs": pairs,
        "chosen": chosen,
        "tolerance": SELECTION_TOLERANCE,
        "vintages": {"built": len(vintages), "skipped": [{"origin": o.isoformat(), "reason": r} for o, r in skipped_vintages]},
        "snapshot_failures": failures,
        "forecasts": {v: aligned[v] for v in tt.VARIANTS},
    }


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
    forecasts = result.pop("forecasts")
    (args.out_dir / "w2_select.json").write_text(json.dumps(_jsonable(result), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    with (args.out_dir / "w2_select_forecasts.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["variant", "origin", "truth", "pred", "lag", "weekly_naive", "n_train"])
        for variant, items in forecasts.items():
            for f in items:
                writer.writerow([variant, f["origin"].isoformat(), f["truth"], f["pred"], f["lag"], f["weekly_naive"], f["n_train"]])
    print(json.dumps(_jsonable({k: result[k] for k in ("n_test", "rmse", "baselines", "chosen")}), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
