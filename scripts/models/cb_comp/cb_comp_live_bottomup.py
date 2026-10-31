#!/usr/bin/env python3
"""Live bottom-up по 88 группам на нашей панели, цель `T_LIVE` — Task 2 #422.

Пункт `key-rate/phase-4-cb-comp-88-groups-live`, зонтик #419. Протокол —
спека `docs/superpowers/specs/2026-10-30-cb-comp-88-groups-live-design.md` §12–§13 (вход — §13.1).

* **данные** — SA групп из Task 1 (`build/cb-comp-groups/panel.csv`, C0 на окне `live`:
  `sa_full_mm` и `sa_asof_mm`), публичная книга МП по листам
  (`build/cb-comp-parity/{public_panel,sheet_map}.csv`), макро v2
  (`cb_comp_parity.load_feature_rows`), архив CB_COMP;
* **R0** — регрессия: `cb_comp_bottomup.run_bottomup(per_sheet)` на архивных
  целях воспроизводит #252;
* **модели** — `live_per_sheet` (основная), `live_nomp`, `live_per_sheet_asof`
  (тестовые лаги из as-of SA); 88 CatBoost на дефолтах, агрегат СПР;
* **цель** — `T_LIVE[h]`: накопленный SA ИПЦ без КУ, собранный из групп;
* **сравнения** — G2 (отношение к CB_COMP против порогов §13), E2 «каждый на
  своей цели», E3 на `TARGET_FULL`, E4 — расхождение целей.

Только чтение ClickHouse и локальных файлов; в БД не пишет. Запуск —
`make cb-comp-live-bottomup`, тесты — `make cb-comp-live-bottomup-test`.
"""

import argparse
import csv
import hashlib
import json
import math
import subprocess
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "scripts" / "models" / "cpi"))

from models.cb_comp import cb_comp_bottomup as bu
from models.cb_comp import cb_comp_parity as cp

# ---------------------------------------------------------------------------
# Предзарегистрированные константы (спека §13)
# ---------------------------------------------------------------------------

HORIZONS = bu.HORIZONS
PRIMARY_HORIZONS = (1, 3, 6)
WEIGHT_YEARS = tuple(range(2016, 2025))
ASOF_START = date(2019, 1, 1)

# R0: `per_sheet` #252 на архивных целях, normal-54.
R0_PINNED = {1: 0.191215, 2: 0.308390, 3: 0.463865, 6: 0.971324}
R0_TOLERANCE = 1e-6

# G2: отношение к CB_COMP не хуже `per_sheet` #252 на 5 %.
PER_SHEET_RATIO = {1: 1.204, 2: 1.040, 3: 1.046, 6: 1.000}
G2_SLACK = 1.05

MODEL_LIVE = "live_per_sheet"
MODEL_NOMP = "live_nomp"
MODEL_ASOF = "live_per_sheet_asof"
MODELS = (MODEL_LIVE, MODEL_NOMP, MODEL_ASOF)

PANEL_CSV = ROOT / "build" / "cb-comp-groups" / "panel.csv"
PUBLIC_DIR = ROOT / "build" / "cb-comp-parity"
OUT_DIR = ROOT / "build" / "cb-comp-live-bottomup"


def g2_threshold(horizon: int) -> float:
    return round(G2_SLACK * PER_SHEET_RATIO[horizon], 3)


# ---------------------------------------------------------------------------
# Панель групп и цель T_LIVE
# ---------------------------------------------------------------------------


def load_group_panel(path: Path) -> tuple:
    """`(sa_full, sa_asof)`: `{код: {месяц: SA м/м, %}}` из артефакта Task 1."""
    sa_full, sa_asof = {}, {}
    with path.open(encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            month = date.fromisoformat(row["month"])
            for column, table in (("sa_full_mm", sa_full), ("sa_asof_mm", sa_asof)):
                if row[column]:
                    table.setdefault(row["code"], {})[month] = float(row[column])
    return sa_full, sa_asof


def asof_series(sa_full: dict, sa_asof: dict) -> dict:
    """Лаги для `live_per_sheet_asof`: до 2019-01 — вся выборка, с 2019-01 — as-of.

    Месяц с 2019-01 без as-of значения остаётся без значения (NaN), а не
    подменяется SA всей выборки.
    """
    out = {}
    for code, values in sa_full.items():
        merged = {m: v for m, v in values.items() if m < ASOF_START}
        merged.update({m: v for m, v in sa_asof.get(code, {}).items() if m >= ASOF_START})
        out[code] = merged
    return out


def log_diff_frame(series: dict, codes: list, months: list) -> pd.DataFrame:
    """SA м/м, % → `log(1 + x/100)`: формат `cpi_diff` для `cb_comp_bottomup`."""
    data = {
        code: [math.log1p(series.get(code, {})[m] / 100) if m in series.get(code, {}) else np.nan for m in months]
        for code in codes
    }
    return pd.DataFrame(data, index=pd.Index(months, name="month"))


def aggregate_mm(sa: dict, weights: dict, months: list) -> dict:
    """М/м SA ИПЦ без КУ: `Σ w·sa / Σ w` по группам с SA; веса года `M.year − 1`."""
    out = {}
    for month in months:
        weights_month = bu.weight_for_month(weights, month)
        numerator = denominator = 0.0
        for code, values in sa.items():
            weight = weights_month.get(code)
            if not weight or month not in values or code == bu.UTILITIES_CODE:
                continue
            numerator += weight * values[month]
            denominator += weight
        if denominator:
            out[month] = numerator / denominator
    return out


def cumulative(mm: dict, horizon: int) -> dict:
    """Накопленный прирост за h месяцев, заканчивающихся в M (формула `TARGET_FULL`)."""
    out = {}
    for month in mm:
        window = [bu.shift_month(month, -j) for j in range(horizon)]
        if all(m in mm for m in window):
            out[month] = (math.exp(sum(math.log1p(mm[m] / 100) for m in window)) - 1) * 100
    return out


def t_live(sa: dict, weights: dict, months: list) -> dict:
    """`{h: {M: T_LIVE[h](M)}}` — спека §12."""
    mm = aggregate_mm(sa, weights, months)
    return {horizon: cumulative(mm, horizon) for horizon in HORIZONS}


# ---------------------------------------------------------------------------
# Модели
# ---------------------------------------------------------------------------


def fit_predict(specs: list, features_train: dict, features_test: dict, targets: dict,
                train_months: list, test_months: list, with_mp: bool) -> dict:
    """`{код: {h: {M: прогноз}}}`; MP-модель — только у групп с BSU и `with_mp`.

    Строки обучения — признаки origin `M−(h−1)` из `features_train`, тестовые —
    из `features_test` (для as-of варианта они разные; модели одни и те же).
    """
    out = {}
    for spec in specs:
        use_mp = with_mp and bool(spec.bsu_codes)
        names = bu.feature_names(with_mp=use_mp, mode="per_sheet", spec=spec) if use_mp else bu.feature_names(with_mp=False)
        for horizon in HORIZONS:
            bu.assert_no_leakage_bottomup(test_months, horizon)
            train_rows = [
                row for row in bu.model_rows(spec.code, features_train.get(spec.code, {}), targets[horizon], horizon, train_months)
                if not pd.isna(row[2][horizon])
            ]
            if len(train_rows) < bu.MIN_TRAIN_ROWS:
                continue
            model = bu.fit_component(train_rows, names, horizon)
            test_rows = bu.model_rows(spec.code, features_test.get(spec.code, {}), targets[horizon], horizon, test_months)
            predictions = bu.predict_component(model, test_rows, names)
            out.setdefault(spec.code, {})[horizon] = {row[0]: value for row, value in zip(test_rows, predictions)}
    return out


def aggregate_predictions(predictions: dict, weights: dict, test_months: list) -> dict:
    """`{h: {M: агрегат}}` через `cb_comp_bottomup.aggregate` (нормировка на группы с прогнозом)."""
    out = {}
    for horizon in HORIZONS:
        by_group = {code: per_h[horizon] for code, per_h in predictions.items() if per_h.get(horizon)}
        values, _coverage = bu.aggregate(by_group, weights, test_months)
        out[horizon] = {m: v for m, v in values.items() if not math.isnan(v)}
    return out


# ---------------------------------------------------------------------------
# Оценка
# ---------------------------------------------------------------------------


def rmse(errors: list) -> float:
    return math.sqrt(sum(e * e for e in errors) / len(errors)) if errors else float("nan")


def errors_on(predictions: dict, truth: dict, months: list) -> tuple:
    common = [m for m in months if m in predictions and m in truth]
    return common, [predictions[m] - truth[m] for m in common]


def g2_check(rmse_live: float, rmse_cb: float, horizon: int) -> dict:
    ratio = rmse_live / rmse_cb
    return {"ratio": ratio, "threshold": g2_threshold(horizon), "ok": ratio <= g2_threshold(horizon)}


def r0_check(values: dict) -> dict:
    """R0: RMSE `per_sheet` на архивных целях против пинов #252."""
    diffs = {h: abs(values[h] - R0_PINNED[h]) for h in R0_PINNED}
    return {"rmse": values, "pinned": R0_PINNED, "max_abs_diff": max(diffs.values()),
            "ok": all(d <= R0_TOLERANCE for d in diffs.values())}


def target_gap(t_live_h: dict, target_full: pd.DataFrame, horizon: int, months: list) -> dict:
    common = [m for m in months if m in t_live_h and m in target_full.index and not pd.isna(target_full.at[m, horizon])]
    diffs = [t_live_h[m] - float(target_full.at[m, horizon]) for m in common]
    return {"n": len(common), "rmse": rmse(diffs), "bias": float(np.mean(diffs)) if diffs else float("nan")}


# ---------------------------------------------------------------------------
# Прогон
# ---------------------------------------------------------------------------


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def git_sha() -> str:
    try:
        return subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return ""


def run(archive: Path, panel_csv: Path, public_dir: Path, out_dir: Path, skip_r0: bool = False) -> dict:
    import cpi_two_stage_cb_comp as w5b

    archive_data = cp.check_archive(archive)
    target_full = archive_data["target"]
    normal = archive_data["normal"]
    all_test = cp.month_range(cp.TEST_START, cp.TEST_END)

    feature_rows, _skipped = cp.load_feature_rows()
    parity_rows, _missing = cp.build_rows(feature_rows, target_full)
    cp.assert_no_leakage(parity_rows)
    train_rows, test_rows = cp.split_rows(parity_rows)
    train_months = [row.month for row in train_rows]
    test_months = [row.month for row in test_rows]
    macro = {row.month: row.features for row in feature_rows}

    public_panel = bu.load_public_panel(public_dir / "public_panel.csv")
    sheet_map = bu.load_sheet_map(public_dir / "sheet_map.csv")

    # R0 — тем же кодом на архивных целях.
    r0 = None
    if not skip_r0:
        reference = bu.run_bottomup(
            archive=archive, target_full=target_full, macro=macro, normal_months=normal,
            train_months=train_months, test_months=test_months, train_outliers=cp.TRAIN_OUTLIER_MONTHS,
            feature_mode="per_sheet", public_panel=public_panel, sheet_map=sheet_map,
        )
        if reference.status != "ok":
            raise SystemExit(f"R0: bottom-up на архивных целях не собран: {reference.reason}")
        values = {}
        for horizon in HORIZONS:
            preds = reference.predictions[bu.MODEL_MP][horizon]["values"]
            _, errs = errors_on(preds, target_full[horizon].dropna().to_dict(), normal)
            values[horizon] = rmse(errs)
        r0 = r0_check(values)
        if not r0["ok"]:
            raise SystemExit(f"R0 не пройден: {r0}")

    # Наша панель и цели.
    specs = bu.load_specs(archive, sheet_map)
    codes = [spec.code for spec in specs]
    weights = bu.load_weights(archive, WEIGHT_YEARS, specs)
    sa_full, sa_asof = load_group_panel(panel_csv)
    # Группа без SA (9218 — X-13 не сходится, спека §11–§12) не стоп: у неё нет
    # цели и прогноза, агрегаты нормируются на группы с данными.
    groups_without_sa = sorted(c for c in codes if not sa_full.get(c))
    if len(groups_without_sa) > 2:
        raise SystemExit(f"в панели групп нет SA у {groups_without_sa}: панель не та, что в Task 1")
    last = max(m for values in sa_full.values() for m in values)
    months = cp.month_range(date(2010, 1, 1), last)
    diff_full = log_diff_frame(sa_full, codes, months)
    diff_asof = log_diff_frame(asof_series(sa_full, sa_asof), codes, months)
    targets = bu.component_targets(diff_full)
    live = t_live({c: sa_full.get(c, {}) for c in codes}, weights, months)

    mp_lookup = bu._sheet_lookup(public_panel, sheet_map)
    # Признаки — на месяцах train ∪ test, как в `run_bottomup` (#190/#252): origin,
    # попавший на выброс train или до 2010-03, строки не даёт.
    feature_months = sorted(set(train_months) | set(test_months))
    features_full = bu.feature_table(specs, feature_months, diff_full, macro, mp_lookup, feature_mode="per_sheet")
    features_asof = bu.feature_table(specs, feature_months, diff_asof, macro, mp_lookup, feature_mode="per_sheet")

    group_predictions = {
        MODEL_LIVE: fit_predict(specs, features_full, features_full, targets, train_months, test_months, with_mp=True),
        MODEL_NOMP: fit_predict(specs, features_full, features_full, targets, train_months, test_months, with_mp=False),
        MODEL_ASOF: fit_predict(specs, features_full, features_asof, targets, train_months, test_months, with_mp=True),
    }
    aggregated = {model: aggregate_predictions(group_predictions[model], weights, test_months) for model in MODELS}

    # Сравнения.
    truth_full = {h: target_full[h].dropna().to_dict() for h in HORIZONS}
    cb_errors = {h: cp._cb_errors(archive_data["sheet"], target_full, h, normal) for h in HORIZONS}
    comparisons = {}
    for model in MODELS:
        per_h = {}
        for horizon in HORIZONS:
            months_live, err_live = errors_on(aggregated[model][horizon], live[horizon], normal)
            months_tf, err_tf = errors_on(aggregated[model][horizon], truth_full[horizon], normal)
            if months_live != normal or months_tf != normal:
                raise SystemExit(f"{model} h{horizon}: нет прогноза или цели на части normal-месяцев")
            rmse_cb = rmse(cb_errors[horizon])
            _, err_live_all = errors_on(aggregated[model][horizon], live[horizon], all_test)
            per_h[horizon] = {
                "rmse_t_live": rmse(err_live),
                "rmse_target_full": rmse(err_tf),
                "rmse_cb": rmse_cb,
                "rmse_t_live_all67": rmse(err_live_all),
                "g2": g2_check(rmse(err_live), rmse_cb, horizon),
                "e2_own_target": w5b.compare_to_cb(err_live, cb_errors[horizon], horizon),
                "e3_target_full": w5b.compare_to_cb(err_tf, cb_errors[horizon], horizon),
            }
        comparisons[model] = per_h

    gaps = {h: {"normal_54": target_gap(live[h], target_full, h, normal), "test_67": target_gap(live[h], target_full, h, all_test)}
            for h in HORIZONS}
    g2_pass = all(comparisons[MODEL_LIVE][h]["g2"]["ok"] for h in PRIMARY_HORIZONS)

    inputs = {
        "panel_csv": sha256(panel_csv),
        "public_panel_csv": sha256(public_dir / "public_panel.csv"),
        "sheet_map_csv": sha256(public_dir / "sheet_map.csv"),
        "target_xlsx": sha256(archive / cp.TARGET_XLSX),
    }
    config = {"seed": bu.SEED, "horizons": list(HORIZONS), "train": [str(train_months[0]), str(train_months[-1]), len(train_months)],
              "test": [str(test_months[0]), str(test_months[-1]), len(test_months)], "normal": len(normal),
              "models": list(MODELS), "g2_thresholds": {h: g2_threshold(h) for h in HORIZONS}}
    result = {
        "spec": "docs/superpowers/specs/2026-10-30-cb-comp-88-groups-live-design.md §13",
        "git_sha": git_sha(),
        "config": config,
        "config_hash": hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest(),
        "inputs": inputs,
        "r0": r0,
        "groups_without_sa": groups_without_sa,
        "g2_pass": g2_pass,
        "comparisons": comparisons,
        "target_gap": gaps,
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    write_predictions(out_dir / "predictions.csv", aggregated, live, truth_full, test_months, set(normal))
    (out_dir / "result.json").write_text(json.dumps(_jsonable(result), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


def write_predictions(path: Path, aggregated: dict, live: dict, truth_full: dict, months: list, normal: set) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["model", "horizon", "month", "normal", "y_pred", "t_live", "target_full"])
        for model in MODELS:
            for horizon in HORIZONS:
                for month in months:
                    if month not in aggregated[model][horizon]:
                        continue
                    writer.writerow([model, horizon, month.isoformat(), int(month in normal),
                                     repr(aggregated[model][horizon][month]),
                                     repr(live[horizon].get(month, float("nan"))),
                                     repr(truth_full[horizon].get(month, float("nan")))])


def _jsonable(value):
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k.isoformat() if isinstance(k, date) else k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (np.floating,)):
        value = float(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--archive", default=None)
    parser.add_argument("--panel", default=str(PANEL_CSV), help="panel.csv (C0, спека §13.1) из make cb-comp-groups-panel ARGS=--asof")
    parser.add_argument("--public-dir", default=str(PUBLIC_DIR), help="каталог public_panel.csv и sheet_map.csv (make cb-comp-public-export)")
    parser.add_argument("--out", default=str(OUT_DIR))
    parser.add_argument("--skip-r0", action="store_true", help="только для отладки; итоговый прогон — с R0")
    args = parser.parse_args(argv)
    archive = cp.resolve_archive(args.archive)
    result = run(archive, Path(args.panel), Path(args.public_dir), Path(args.out), args.skip_r0)
    summary = {
        "r0": result["r0"], "g2_pass": result["g2_pass"],
        "models": {m: {h: {k: result["comparisons"][m][h][k] for k in ("rmse_t_live", "rmse_target_full", "rmse_cb")}
                       | {"g2": result["comparisons"][m][h]["g2"], "e2": result["comparisons"][m][h]["e2_own_target"]["class"],
                          "e3": result["comparisons"][m][h]["e3_target_full"]["class"]}
                       for h in HORIZONS} for m in MODELS},
        "target_gap": result["target_gap"],
    }
    print(json.dumps(_jsonable(summary), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
