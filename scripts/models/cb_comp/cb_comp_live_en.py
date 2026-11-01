#!/usr/bin/env python3
"""EN_COMP и CB+EN на нашей панели 88 групп, истина `TARGET_FULL` — Task 3 #423.

Пункт `key-rate/phase-4-cb-comp-88-groups-live`, зонтик #419. Протокол —
спека `docs/superpowers/specs/2026-10-30-cb-comp-88-groups-live-design.md` §15.

* **данные, окна, цели групп, признаки** — как в Task 2 (`cb_comp_live_bottomup`):
  SA групп из `panel.csv`, МП по листам публичной книги, макро v2;
* **кандидаты** — `live_cb` (CatBoost = `live_per_sheet` Task 2), `live_en`
  (ElasticNetCV в конфигурации B3 `cpi_monthly._fit_enet`, без `month`,
  импутация медианой train), `live_cb_en` (среднее CB и EN с равными весами);
* **оценка на `TARGET_FULL`** (normal-54) — классификация W5b против CB_COMP,
  MCS 90 % по h1/h3/h6, попарно против `live_cb`; справочно — `T_LIVE` и 67
  месяцев; правило выбора и гейт G3.

Только чтение ClickHouse и локальных файлов; в БД не пишет. Запуск —
`make cb-comp-live-en`, тесты — `make cb-comp-live-en-test`.
"""

import argparse
import csv
import hashlib
import json
import math
import sys
import warnings
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.exceptions import ConvergenceWarning
from sklearn.linear_model import ElasticNetCV
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "scripts" / "models" / "cpi"))

from models.cb_comp import cb_comp_bottomup as bu
from models.cb_comp import cb_comp_live_bottomup as lb
from models.cb_comp import cb_comp_parity as cp
from models.cpi import cpi_monthly as cm

# ---------------------------------------------------------------------------
# Предзарегистрированные константы (спека §15)
# ---------------------------------------------------------------------------

HORIZONS = bu.HORIZONS
PRIMARY_HORIZONS = (1, 3, 6)
MODEL_CB = "live_cb"
MODEL_EN = "live_en"
MODEL_CB_EN = "live_cb_en"
CANDIDATES = (MODEL_CB, MODEL_EN, MODEL_CB_EN)
CB_COMP = "cb_comp"
G3_THRESHOLD = {1: 1.264, 3: 1.098, 6: 1.050}
PAIR_ALPHA = 0.05

OUT_DIR = ROOT / "build" / "cb-comp-live-en"


# ---------------------------------------------------------------------------
# ElasticNet группы (конфигурация B3)
# ---------------------------------------------------------------------------


def en_names(names: list) -> list:
    """Признаки EN — признаки CatBoost без `month` (как у B3)."""
    return [name for name in names if name != "month"]


def float_matrix(rows: list, names: list) -> np.ndarray:
    def value(row, name):
        item = row[1].get(name, float("nan"))
        return float("nan") if item is None else float(item)

    return np.asarray([[value(row, name) for name in names] for row in rows], dtype=float)


def make_enet() -> Pipeline:
    """`StandardScaler → ElasticNetCV` — дословно конфигурация B3 `cpi_monthly`."""
    return Pipeline([
        ("scaler", StandardScaler()),
        ("enet", ElasticNetCV(
            alphas=cm.ENET_ALPHAS,
            l1_ratio=list(cm.ENET_L1_RATIOS),
            cv=TimeSeriesSplit(cm.ENET_CV_SPLITS),
            random_state=bu.SEED,
        )),
    ])


def enet_fit_predict(train_rows: list, test_rows: list, names: list, horizon: int) -> tuple:
    """`(прогнозы теста, не сошёлся)`; NaN — медианой train, пустая колонка — 0."""
    train = float_matrix(train_rows, names)
    medians = cm.enet_medians(train)
    imputed = np.where(np.isnan(train), medians, train)
    targets = np.asarray([row[2][horizon] for row in train_rows], dtype=float)
    model = make_enet()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        model.fit(imputed, targets)
    non_converged = any(issubclass(item.category, ConvergenceWarning) for item in caught)
    if not test_rows:
        return [], non_converged
    test = float_matrix(test_rows, names)
    test_imputed = np.where(np.isnan(test), medians, test)
    return [float(v) for v in model.predict(test_imputed)], non_converged


def fit_predict_en(specs: list, features: dict, targets: dict, train_months: list, test_months: list) -> tuple:
    """`({код: {h: {M: прогноз}}}, число недосходов)` — EN по группам, МП как у `live_cb`."""
    out, non_converged = {}, 0
    for spec in specs:
        use_mp = bool(spec.bsu_codes)
        base = bu.feature_names(with_mp=use_mp, mode="per_sheet", spec=spec) if use_mp else bu.feature_names(with_mp=False)
        names = en_names(base)
        for horizon in HORIZONS:
            bu.assert_no_leakage_bottomup(test_months, horizon)
            train_rows = [
                row for row in bu.model_rows(spec.code, features.get(spec.code, {}), targets[horizon], horizon, train_months)
                if not pd.isna(row[2][horizon])
            ]
            if len(train_rows) < max(bu.MIN_TRAIN_ROWS, cm.ENET_MIN_TRAIN):
                continue
            test_rows = bu.model_rows(spec.code, features.get(spec.code, {}), targets[horizon], horizon, test_months)
            predictions, failed = enet_fit_predict(train_rows, test_rows, names, horizon)
            non_converged += int(failed)
            out.setdefault(spec.code, {})[horizon] = {row[0]: value for row, value in zip(test_rows, predictions)}
    return out, non_converged


def combine(first: dict, second: dict) -> dict:
    """Среднее двух прогнозов группы с равными весами; группа/месяц без пары — не входит."""
    out = {}
    for code in sorted(set(first) & set(second)):
        for horizon in HORIZONS:
            left, right = first[code].get(horizon, {}), second[code].get(horizon, {})
            common = sorted(set(left) & set(right))
            if common:
                out.setdefault(code, {})[horizon] = {m: (left[m] + right[m]) / 2 for m in common}
    return out


# ---------------------------------------------------------------------------
# Оценка и выбор
# ---------------------------------------------------------------------------


def better_than_cb(pair: dict) -> bool:
    """Кандидат «лучше `live_cb`»: DM p < 0,05 и 95 % CI ΔRMSE целиком ниже 0."""
    return pair["dm_p"] < PAIR_ALPHA and pair["delta_ci"][1] < 0


def choose_model(ratios: dict, in_mcs: dict, beats_cb: dict) -> tuple:
    """Правило §15: `(модель, причина)`.

    `ratios[m][h]` — отношение к CB_COMP; `in_mcs[h]` — множество MCS;
    `beats_cb[m][h]` — «лучше `live_cb`». EN/CB+EN без значимого выигрыша над
    `live_cb` хотя бы на одном primary-горизонте не выбираются.
    """
    eligible = [m for m in CANDIDATES if all(m in in_mcs[h] for h in PRIMARY_HORIZONS)]
    eligible = [m for m in eligible if m == MODEL_CB or any(beats_cb[m][h] for h in PRIMARY_HORIZONS)]
    if not eligible:
        return MODEL_CB, "ни один кандидат не прошёл фильтр MCS/выигрыша — по умолчанию live_cb"
    best = min(eligible, key=lambda m: (sum(ratios[m][h] for h in PRIMARY_HORIZONS) / len(PRIMARY_HORIZONS), CANDIDATES.index(m)))
    return best, f"кандидаты после фильтра: {eligible}"


def g3_check(ratios: dict) -> dict:
    per_h = {h: {"ratio": ratios[h], "threshold": G3_THRESHOLD[h], "ok": ratios[h] <= G3_THRESHOLD[h]} for h in PRIMARY_HORIZONS}
    return {"horizons": per_h, "ok": all(v["ok"] for v in per_h.values())}


# ---------------------------------------------------------------------------
# Прогон
# ---------------------------------------------------------------------------


def prepare(archive: Path, panel_csv: Path, public_dir: Path, weights_source: str = "archive") -> dict:
    """Данные, цели и признаки — те же шаги, что `cb_comp_live_bottomup.run` (без R0).

    `weights_source` пробрасывается в `bu.load_weights`: Task 9 передаёт `live`,
    замороженные P0.6/Task 4/4b оставляют архив (default).
    """
    archive_data = cp.check_archive(archive)
    target_full = archive_data["target"]
    feature_rows, _ = cp.load_feature_rows()
    parity_rows, _ = cp.build_rows(feature_rows, target_full)
    cp.assert_no_leakage(parity_rows)
    train_rows, test_rows = cp.split_rows(parity_rows)
    macro = {row.month: row.features for row in feature_rows}
    public_panel = bu.load_public_panel(public_dir / "public_panel.csv")
    sheet_map = bu.load_sheet_map(public_dir / "sheet_map.csv")
    specs = bu.load_specs(archive, sheet_map)
    codes = [spec.code for spec in specs]
    weights = bu.load_weights(archive, bu.LIVE_WEIGHT_YEARS, specs, source=weights_source)
    sa_full, _sa_asof = lb.load_group_panel(panel_csv)
    last = max(m for values in sa_full.values() for m in values)
    first = lb.first_sa_month(sa_full)
    months = cp.month_range(first, last)
    diff_full = lb.log_diff_frame(sa_full, codes, months)
    train_months = [row.month for row in train_rows]
    test_months = [row.month for row in test_rows]
    features = bu.feature_table(specs, sorted(set(train_months) | set(test_months)), diff_full, macro,
                                bu._sheet_lookup(public_panel, sheet_map), feature_mode="per_sheet")
    return {
        "archive_data": archive_data, "target_full": target_full, "normal": archive_data["normal"],
        "specs": specs, "weights": weights, "targets": bu.component_targets(diff_full),
        "t_live": lb.t_live({c: sa_full.get(c, {}) for c in codes}, weights, months),
        "features": features, "train_months": train_months, "test_months": test_months,
        "groups_without_sa": sorted(c for c in codes if not sa_full.get(c)),
    }


def evaluate(aggregated: dict, data: dict) -> dict:
    import cpi_two_stage_cb_comp as w5b
    import cpi_v4_leaderboard as v4

    normal = data["normal"]
    target_full = data["target_full"]
    truth = {h: target_full[h].dropna().to_dict() for h in HORIZONS}
    all_test = cp.month_range(cp.TEST_START, cp.TEST_END)
    cb_err = {h: cp._cb_errors(data["archive_data"]["sheet"], target_full, h, normal) for h in HORIZONS}
    errors, per_model = {}, {}
    for model in CANDIDATES:
        errors[model], per_model[model] = {}, {}
        for h in HORIZONS:
            months, err = lb.errors_on(aggregated[model][h], truth[h], normal)
            if months != normal:
                raise SystemExit(f"{model} h{h}: нет прогноза на части normal-месяцев")
            errors[model][h] = err
            _, err_live = lb.errors_on(aggregated[model][h], data["t_live"][h], normal)
            _, err_all = lb.errors_on(aggregated[model][h], truth[h], all_test)
            per_model[model][h] = {
                "vs_cb_target_full": w5b.compare_to_cb(err, cb_err[h], h),
                "rmse_t_live": lb.rmse(err_live),
                "rmse_target_full_all67": lb.rmse(err_all),
            }
    pairs = {m: {h: v4.compare_pair(errors[m][h], errors[MODEL_CB][h], h) for h in HORIZONS} for m in (MODEL_EN, MODEL_CB_EN)}
    mcs = {}
    for h in PRIMARY_HORIZONS:
        losses = {m: [e * e for e in errors[m][h]] for m in CANDIDATES}
        losses[CB_COMP] = [e * e for e in cb_err[h]]
        included, pvalues = v4.mcs_included(losses)
        mcs[h] = {"included": sorted(included), "pvalues": pvalues}
    ratios = {m: {h: per_model[m][h]["vs_cb_target_full"]["ratio"] for h in HORIZONS} for m in CANDIDATES}
    beats = {m: {h: (m != MODEL_CB and better_than_cb(pairs[m][h])) for h in HORIZONS} for m in CANDIDATES}
    chosen, reason = choose_model(ratios, {h: set(mcs[h]["included"]) for h in PRIMARY_HORIZONS}, beats)
    return {"per_model": per_model, "pairs_vs_live_cb": pairs, "mcs": mcs, "ratios": ratios,
            "beats_live_cb": beats, "chosen": chosen, "choice_reason": reason,
            "g3": g3_check({h: ratios[chosen][h] for h in PRIMARY_HORIZONS})}


def run(archive: Path, panel_csv: Path, public_dir: Path, out_dir: Path) -> dict:
    data = prepare(archive, panel_csv, public_dir)
    args = (data["specs"], data["features"], data["features"], data["targets"], data["train_months"], data["test_months"])
    cb = lb.fit_predict(*args, with_mp=True)
    en, non_converged = fit_predict_en(data["specs"], data["features"], data["targets"], data["train_months"], data["test_months"])
    groups = {MODEL_CB: cb, MODEL_EN: en, MODEL_CB_EN: combine(cb, en)}
    aggregated = {m: lb.aggregate_predictions(groups[m], data["weights"], data["test_months"]) for m in CANDIDATES}
    result = evaluate(aggregated, data)
    result.update({
        "spec": "docs/superpowers/specs/2026-10-30-cb-comp-88-groups-live-design.md §15",
        "git_sha": lb.git_sha(),
        "inputs": {"panel_csv": lb.sha256(panel_csv), "public_panel_csv": lb.sha256(public_dir / "public_panel.csv"),
                   "sheet_map_csv": lb.sha256(public_dir / "sheet_map.csv"), "target_xlsx": lb.sha256(archive / cp.TARGET_XLSX)},
        "enet": {"alphas": [float(a) for a in cm.ENET_ALPHAS], "l1_ratio": list(cm.ENET_L1_RATIOS),
                 "cv_splits": cm.ENET_CV_SPLITS, "non_converged_fits": non_converged},
        "groups_without_sa": data["groups_without_sa"],
        "groups_count": {m: len(groups[m]) for m in CANDIDATES},
    })
    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / "predictions.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["model", "horizon", "month", "y_pred"])
        for m in CANDIDATES:
            for h in HORIZONS:
                for month in sorted(aggregated[m][h]):
                    writer.writerow([m, h, month.isoformat(), repr(aggregated[m][h][month])])
    (out_dir / "result.json").write_text(json.dumps(lb._jsonable(result), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--archive", default=None)
    parser.add_argument("--panel", default=None, help="по умолчанию — панель версии")
    parser.add_argument("--public-dir", default=str(lb.PUBLIC_DIR))
    parser.add_argument("--out", default=None, help="по умолчанию build/cb-comp-live-en[-<версия>]")
    parser.add_argument("--version", choices=lb.VERSIONS, default=lb.DEFAULT_VERSION,
                        help="версия цели: 2010 — канон, 2009 — T_LIVE с 2009-01")
    args = parser.parse_args(argv)
    panel = Path(args.panel) if args.panel else lb.default_panel(args.version)
    lb.check_panel_version(lb.load_group_panel(panel)[0], args.version)
    out_dir = Path(args.out) if args.out else lb.versioned_dir(OUT_DIR, args.version)
    archive = cp.resolve_archive(args.archive)
    result = run(archive, panel, Path(args.public_dir), out_dir)
    summary = {
        "chosen": result["chosen"], "reason": result["choice_reason"], "g3": result["g3"],
        "classes": {m: {h: (round(v["vs_cb_target_full"]["ratio"], 3), v["vs_cb_target_full"]["class"]) for h, v in per.items()}
                    for m, per in result["per_model"].items()},
        "mcs": result["mcs"], "beats_live_cb": result["beats_live_cb"], "enet": result["enet"]["non_converged_fits"],
    }
    print(json.dumps(lb._jsonable(summary), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
