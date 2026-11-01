#!/usr/bin/env python3
"""CatBoost vs LightGBM/XGBoost на live-конвейере 88 групп — Task 17 #501.

Пункт `key-rate/phase-4-cb-comp-88-groups-live`, зонтик #419. Протокол —
спека `docs/superpowers/specs/2026-11-01-cb-comp-backend-compare-design.md`.

* **арена** — как Task 9: frozen-обучение 2010-03…2018-12, тест 2019-01…2024-07,
  normal-54; истина — `TARGET_FULL`, справочно — `T_LIVE`;
* **модель** — `live_cb` (88 групп × 4 горизонта) и `combo3` = среднее с BS-OLS
  Task 9; подменяется только learner;
* **плечи** — `defaults` (дефолты библиотеки) и `matched` (`n_estimators=1000`,
  `learning_rate=0.03`); у CatBoost они совпадают, второй run не создаётся;
* **оценка** — RMSE/отношение к CB_COMP, класс W5b, DM-HLN/CI/MDE, MCS 90 %;
* **скорость** — время `fit` каждого компонента, `timings.csv` и агрегаты;
* **паспорт** — `cb_comp_mlog`/MLflow, experiment `cb_comp_88_groups`.

Только чтение ClickHouse и локальных файлов; в БД скрипт не пишет (паспорт
пишется стандартным `ml_experiment`). Запуск — `make cb-comp-backend-compare`,
тесты — `make cb-comp-backend-compare-test`.
"""

import argparse
import csv
import hashlib
import importlib.metadata
import json
import math
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "scripts" / "models" / "cpi"))

from models.cb_comp import cb_comp_bottomup as bu
from models.cb_comp import cb_comp_h1_bs_combo as hc
from models.cb_comp import cb_comp_live_bottomup as lb
from models.cb_comp import cb_comp_live_en as en
from models.cb_comp import cb_comp_mlog as mlog
from models.cb_comp import cb_comp_parity as cp

# ---------------------------------------------------------------------------
# Предзарегистрированные константы (спека §3–§7)
# ---------------------------------------------------------------------------

HORIZONS = bu.HORIZONS
PRIMARY_HORIZONS = (1, 3, 6)
SEED = bu.SEED
CB_COMP = "cb_comp"
CATBOOST = "catboost"
MODEL_CB = "live_cb"
MODEL_COMBO = "combo3"
BACKEND_ORDER = (CATBOOST, "lightgbm", "xgboost")
ARMS = ("defaults", "matched")
# CatBoost на дефолтах уже 1000 итераций / lr 0,03 — плечо matched для него не запускается.
RUNS = (
    (CATBOOST, "defaults"),
    ("lightgbm", "defaults"),
    ("lightgbm", "matched"),
    ("xgboost", "defaults"),
    ("xgboost", "matched"),
)
MATCHED = {"n_estimators": 1000, "learning_rate": 0.03}
MONTH_CATEGORIES = list(range(1, 13))
VERSIONS = lb.VERSIONS
DEFAULT_VERSION = lb.DEFAULT_VERSION
OUT_DIR = ROOT / "build" / "cb-comp-backend-compare"
SPEC = "docs/superpowers/specs/2026-11-01-cb-comp-backend-compare-design.md"
EVIDENCE_JSON = ROOT / "docs" / "research" / "2026-11-01-cb-comp-backend-compare.json"
TASK3_PINS = {1: 1.193, 2: 1.013, 3: 1.076, 6: 1.062}
TASK9_PINS = {1: 1.035, 2: 0.957, 3: 1.010, 6: 0.894}
PIN_TOL = 0.002
PIN_STOP = 0.01
NO_HARM_MARGIN = 0.02
NONINF_MARGIN = 0.05
SPEED_FAST_RATIO = 0.5


# ---------------------------------------------------------------------------
# Бэкенды
# ---------------------------------------------------------------------------


def package_version(name: str) -> str:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return "missing"


def feature_frame(rows: list, names: list) -> pd.DataFrame:
    """Матрица признаков для LightGBM/XGBoost: float-колонки + `month`-категория.

    `month` — категориальный признак во всех трёх библиотеках (у CatBoost —
    `cat_features`), поэтому здесь фиксированный `CategoricalDtype(1…12)`.
    NaN сохраняются: импутации нет, все три библиотеки обрабатывают пропуски.
    """
    if not rows:
        return pd.DataFrame({name: pd.Series(dtype=object) for name in names})
    frame = pd.DataFrame([{name: row[1][name] for name in names} for row in rows], columns=names)
    for name in names:
        if name == "month":
            frame[name] = pd.Categorical(frame[name].astype(int), categories=MONTH_CATEGORIES)
        else:
            frame[name] = frame[name].astype(float)
    return frame


class Backend:
    """Learner с интерфейсом `fit(rows, names, horizon)` / `predict(model, rows, names)`."""

    name = ""
    package = ""

    def __init__(self, arm: str):
        if arm not in ARMS:
            raise ValueError(f"неизвестное плечо: {arm!r}")
        self.arm = arm

    def version(self) -> str:
        return package_version(self.package)

    def hyperparameters(self) -> dict:
        raise NotImplementedError

    def fit(self, train_rows: list, names: list, horizon: int):
        raise NotImplementedError

    def predict(self, model, rows: list, names: list) -> list:
        raise NotImplementedError

    def trees(self, model):
        raise NotImplementedError

    @staticmethod
    def target(train_rows: list, horizon: int):
        return np.asarray([row[2][horizon] for row in train_rows], dtype=float)


class CatBoostBackend(Backend):
    name = CATBOOST
    package = "catboost"

    def hyperparameters(self) -> dict:
        return {**bu.CATBOOST_DEFAULTS, "cat_features": ["month"]}

    def fit(self, train_rows: list, names: list, horizon: int):
        return bu.fit_component(train_rows, names, horizon)

    def predict(self, model, rows: list, names: list) -> list:
        return bu.predict_component(model, rows, names)

    def trees(self, model):
        return int(model.tree_count_)


class LightGBMBackend(Backend):
    name = "lightgbm"
    package = "lightgbm"

    def hyperparameters(self) -> dict:
        params = {
            "random_state": SEED,
            "verbose": -1,
            "deterministic": True,
            "force_row_wise": True,
            "categorical_feature": ["month"],
        }
        if self.arm == "matched":
            params.update(MATCHED)
        return params

    def fit(self, train_rows: list, names: list, horizon: int):
        from lightgbm import LGBMRegressor

        params = dict(self.hyperparameters())
        categorical = params.pop("categorical_feature")
        model = LGBMRegressor(**params)
        return model.fit(feature_frame(train_rows, names), self.target(train_rows, horizon),
                         categorical_feature=categorical)

    def predict(self, model, rows: list, names: list) -> list:
        if not rows:
            return []
        return [float(value) for value in model.predict(feature_frame(rows, names))]

    def trees(self, model):
        return int(model.booster_.num_trees())


class XGBoostBackend(Backend):
    name = "xgboost"
    package = "xgboost"

    def hyperparameters(self) -> dict:
        params = {
            "random_state": SEED,
            "tree_method": "hist",
            "enable_categorical": True,
            "verbosity": 0,
        }
        if self.arm == "matched":
            params.update(MATCHED)
        return params

    def fit(self, train_rows: list, names: list, horizon: int):
        from xgboost import XGBRegressor

        model = XGBRegressor(**self.hyperparameters())
        return model.fit(feature_frame(train_rows, names), self.target(train_rows, horizon))

    def predict(self, model, rows: list, names: list) -> list:
        if not rows:
            return []
        return [float(value) for value in model.predict(feature_frame(rows, names))]

    def trees(self, model):
        return int(model.get_booster().num_boosted_rounds())


BACKENDS = {cls.name: cls for cls in (CatBoostBackend, LightGBMBackend, XGBoostBackend)}


def make_backend(name: str, arm: str) -> Backend:
    return BACKENDS[name](arm)


def model_names(backend: str) -> tuple:
    return f"{MODEL_CB}_{backend}", f"{MODEL_COMBO}_{backend}"


def run_key(backend: str, arm: str) -> str:
    return f"{backend}/{arm}"


def run_name(backend: str, arm: str, version: str) -> str:
    return f"{MODEL_CB}_{backend}__h1-h6__normal54__target_full__{version}__{arm}"


# ---------------------------------------------------------------------------
# Прогон компонентных моделей с таймингом
# ---------------------------------------------------------------------------


def fit_predict_timed(backend: Backend, data: dict) -> tuple:
    """`(predictions, timings, n_skipped)`; повторяет цикл `lb.fit_predict`."""
    timings = []
    skipped = 0
    predictions = {}
    for spec in data["specs"]:
        use_mp = bool(spec.bsu_codes)
        names = bu.feature_names(with_mp=use_mp, mode="per_sheet", spec=spec) if use_mp else bu.feature_names(with_mp=False)
        for horizon in HORIZONS:
            bu.assert_no_leakage_bottomup(data["test_months"], horizon)
            train_rows = [
                row for row in bu.model_rows(spec.code, data["features"].get(spec.code, {}), data["targets"][horizon], horizon, data["train_months"])
                if not pd.isna(row[2][horizon])
            ]
            if len(train_rows) < bu.MIN_TRAIN_ROWS:
                skipped += 1
                continue
            started = time.perf_counter()
            model = backend.fit(train_rows, names, horizon)
            fit_seconds = time.perf_counter() - started
            test_rows = bu.model_rows(spec.code, data["features"].get(spec.code, {}), data["targets"][horizon], horizon, data["test_months"])
            started = time.perf_counter()
            predicted = backend.predict(model, test_rows, names)
            predict_seconds = time.perf_counter() - started
            timings.append({
                "backend": backend.name,
                "arm": backend.arm,
                "code": spec.code,
                "horizon": horizon,
                "rows": len(train_rows),
                "trees": backend.trees(model),
                "fit_seconds": fit_seconds,
                "predict_seconds": predict_seconds,
            })
            predictions.setdefault(spec.code, {})[horizon] = {row[0]: value for row, value in zip(test_rows, predicted)}
    return predictions, timings, skipped


def warm_up(backend: Backend, data: dict) -> float:
    """Один отброшенный fit: прогревает импорт/потоки и не попадает в статистику."""
    spec = data["specs"][0]
    use_mp = bool(spec.bsu_codes)
    names = bu.feature_names(with_mp=use_mp, mode="per_sheet", spec=spec) if use_mp else bu.feature_names(with_mp=False)
    train_rows = [
        row for row in bu.model_rows(spec.code, data["features"].get(spec.code, {}), data["targets"][1], 1, data["train_months"])
        if not pd.isna(row[2][1])
    ]
    if len(train_rows) < bu.MIN_TRAIN_ROWS:
        return 0.0
    started = time.perf_counter()
    backend.fit(train_rows, names, 1)
    return time.perf_counter() - started


def timing_summary(records: list, skipped: int, warmup: float) -> dict:
    fits = [row["fit_seconds"] for row in records]
    predicts = [row["predict_seconds"] for row in records]
    trees = [row["trees"] for row in records if row["trees"] is not None]
    return {
        "n_fits": len(fits),
        "n_fits_skipped": skipped,
        "warmup_seconds": warmup,
        "fit_seconds_total": float(sum(fits)) if fits else float("nan"),
        "fit_seconds_mean": float(statistics.fmean(fits)) if fits else float("nan"),
        "fit_seconds_median": float(statistics.median(fits)) if fits else float("nan"),
        "fit_seconds_p95": float(np.percentile(fits, 95)) if fits else float("nan"),
        "fit_seconds_max": float(max(fits)) if fits else float("nan"),
        "predict_seconds_total": float(sum(predicts)) if predicts else float("nan"),
        "predict_seconds_median": float(statistics.median(predicts)) if predicts else float("nan"),
        "trees_mean": float(statistics.fmean(trees)) if trees else None,
    }


# ---------------------------------------------------------------------------
# Оценка
# ---------------------------------------------------------------------------


def truth_map(target_full: pd.DataFrame) -> dict:
    return {h: {m: float(v) for m, v in target_full[h].dropna().items()} for h in HORIZONS}


def cb_comp_errors(archive_data: dict, target_full: pd.DataFrame, normal: list) -> dict:
    return {h: cp._cb_errors(archive_data["sheet"], target_full, h, normal) for h in HORIZONS}


def evaluate_model(predictions: dict, truth: dict, t_live: dict, cb_err: dict,
                   normal: list, all_test: list) -> dict:
    import cpi_two_stage_cb_comp as w5b

    out = {}
    for horizon in HORIZONS:
        months, errors = lb.errors_on(predictions[horizon], truth[horizon], normal)
        if months != list(normal):
            raise SystemExit(f"h{horizon}: нет прогноза на части normal-месяцев ({len(months)}/{len(normal)})")
        _, t_live_errors = lb.errors_on(predictions[horizon], t_live[horizon], normal)
        _, all_errors = lb.errors_on(predictions[horizon], truth[horizon], all_test)
        out[horizon] = {
            "n": len(errors),
            "rmse": lb.rmse(errors),
            "rmse_t_live": lb.rmse(t_live_errors),
            "rmse_all67": lb.rmse(all_errors),
            "vs_cb_target_full": w5b.compare_to_cb(errors, cb_err[horizon], horizon),
        }
    return out


def pairwise(predictions: dict, reference: dict, truth: dict, normal: list) -> dict:
    import cpi_v4_leaderboard as v4

    out = {}
    for horizon in HORIZONS:
        _, errors = lb.errors_on(predictions[horizon], truth[horizon], normal)
        _, ref_errors = lb.errors_on(reference[horizon], truth[horizon], normal)
        out[horizon] = v4.compare_pair(errors, ref_errors, horizon)
    return out


def mcs_by_horizon(runs: dict, arm: str, cb_err: dict, truth: dict, normal: list) -> dict:
    import cpi_v4_leaderboard as v4

    members = [record for record in runs.values()
               if record["arm"] == arm or (record["backend"] == CATBOOST and record["arm"] == "defaults")]
    if len(members) + 1 < 3:
        return {}
    out = {}
    for horizon in PRIMARY_HORIZONS:
        losses = {CB_COMP: [value * value for value in cb_err[horizon]]}
        for record in members:
            _, errors = lb.errors_on(record["live"][horizon], truth[horizon], normal)
            losses[f"{MODEL_CB}_{record['backend']}"] = [value * value for value in errors]
        included, pvalues = v4.mcs_included(losses)
        out[horizon] = {"included": sorted(included), "pvalues": pvalues}
    return out


def combo_rmse_ratio(combo: dict, reference: dict, truth: dict, normal: list) -> dict:
    out = {}
    for horizon in HORIZONS:
        _, errors = lb.errors_on(combo[horizon], truth[horizon], normal)
        _, ref_errors = lb.errors_on(reference[horizon], truth[horizon], normal)
        out[horizon] = lb.rmse(errors) / lb.rmse(ref_errors)
    return out


def decide(pairs: dict, combo_ratio: dict, timing: dict, reference_timing: dict) -> dict:
    """Предрегистрированное правило спеки §6; чистая функция, без I/O."""
    checks = {}
    reasons = []

    h1 = pairs[1]
    h1_better = h1["delta"] < 0 and h1["dm_p"] < 0.05 and h1["delta_ci"][1] < 0
    not_worse = True
    for horizon in (3, 6):
        pair = pairs[horizon]
        if pair["delta"] > 0 and pair["dm_p"] < 0.05 and pair["delta_ci"][0] > 0:
            not_worse = False
            reasons.append(f"h{horizon}: значимо хуже CatBoost (p={pair['dm_p']:.4f})")
    combo_ok = combo_ratio[1] <= 1.0 + NO_HARM_MARGIN
    if not combo_ok:
        reasons.append(f"combo3 h1 хуже CatBoost на {(combo_ratio[1] - 1) * 100:.1f} % (margin {NO_HARM_MARGIN * 100:.0f} %)")
    checks["swap"] = bool(h1_better and not_worse and combo_ok)

    within = {}
    for horizon in PRIMARY_HORIZONS:
        pair = pairs[horizon]
        limit = NONINF_MARGIN * pair["rmse_right"]
        within[horizon] = abs(pair["delta_ci"][0]) <= limit and abs(pair["delta_ci"][1]) <= limit
    speed_ratio = timing["fit_seconds_median"] / reference_timing["fit_seconds_median"]
    fast = speed_ratio <= SPEED_FAST_RATIO
    checks["speed"] = bool(all(within.values()) and fast)
    if not fast:
        reasons.append(f"медиана fit ×{speed_ratio:.2f} — не быстрее {SPEED_FAST_RATIO:.1f}×")

    if checks["swap"]:
        decision = "swap_candidate"
        reasons.insert(0, f"h1 лучше CatBoost: Δ={h1['delta']:.4f}, DM p={h1['dm_p']:.4f}, CI [{h1['delta_ci'][0]:+.4f}; {h1['delta_ci'][1]:+.4f}]")
    elif checks["speed"]:
        decision = "speed_candidate"
        reasons.insert(0, f"точность не различима (CI в ±{NONINF_MARGIN * 100:.0f} %), медиана fit ×{speed_ratio:.2f}")
    else:
        decision = "keep_catboost"
    return {"decision": decision, "checks": checks, "reasons": reasons}


# ---------------------------------------------------------------------------
# Файлы, отчёт, паспорт
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


def git_dirty() -> bool:
    try:
        result = subprocess.run(["git", "status", "--porcelain"], cwd=ROOT, capture_output=True, text=True, check=True)
    except (OSError, subprocess.CalledProcessError):
        return False
    return bool(result.stdout.strip())


def write_predictions(path: Path, runs: dict) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["backend", "arm", "model", "horizon", "month", "y_pred"])
        for record in runs.values():
            for model, field in ((record["model_live"], "live"), (record["model_combo"], "combo")):
                for horizon in HORIZONS:
                    for month, value in sorted(record[field][horizon].items()):
                        writer.writerow([record["backend"], record["arm"], model, horizon, month.isoformat(), repr(value)])


def write_timings(path: Path, records: list) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["backend", "arm", "code", "horizon", "rows", "trees", "fit_seconds", "predict_seconds"])
        for row in records:
            writer.writerow([row["backend"], row["arm"], row["code"], row["horizon"], row["rows"], row["trees"],
                             repr(row["fit_seconds"]), repr(row["predict_seconds"])])


def write_run_predictions(path: Path, record: dict) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["model", "horizon", "month", "y_pred"])
        for model, field in ((record["model_live"], "live"), (record["model_combo"], "combo")):
            for horizon in HORIZONS:
                for month, value in sorted(record[field][horizon].items()):
                    writer.writerow([model, horizon, month.isoformat(), repr(value)])


def render_report(result: dict) -> str:
    lines = [
        "# CatBoost vs LightGBM/XGBoost — сводка Task 17",
        "",
        f"Версия цели: `{result['version']}`; normal-54; истина `TARGET_FULL`; git `{result['git_sha'][:7]}`.",
        "",
        "## Точность: live_cb",
        "",
        "| run | h | RMSE | ratio CB_COMP | class | DM p vs catboost | ΔRMSE (95 % CI) |",
        "|---|---|---|---|---|---|---|",
    ]
    for key, record in result["runs"].items():
        for horizon in HORIZONS:
            metrics = record["per_model"][record["model_live"]][str(horizon)]
            pairs = record.get("pairs_vs_catboost") or {}
            pair = pairs.get(str(horizon))
            dm = "—" if pair is None else f"{pair['dm_p']:.4f}"
            delta = "—" if pair is None else f"{pair['delta']:+.4f} [{pair['delta_ci'][0]:+.4f}; {pair['delta_ci'][1]:+.4f}]"
            lines.append(f"| {key} | h{horizon} | {metrics['rmse']:.4f} | {metrics['vs_cb_target_full']['ratio']:.3f} | "
                         f"{metrics['vs_cb_target_full']['class']} | {dm} | {delta} |")
    lines += ["", "## Точность: combo3", "", "| run | h | RMSE | ratio CB_COMP | class |", "|---|---|---|---|---|"]
    for key, record in result["runs"].items():
        for horizon in HORIZONS:
            metrics = record["per_model"][record["model_combo"]][str(horizon)]
            lines.append(f"| {key} | h{horizon} | {metrics['rmse']:.4f} | {metrics['vs_cb_target_full']['ratio']:.3f} | {metrics['vs_cb_target_full']['class']} |")
    lines += ["", "## Скорость", "", "| run | fits | total, s | mean, ms | median, ms | p95, ms | trees |", "|---|---|---|---|---|---|---|"]
    for key, record in result["runs"].items():
        timing = record["timing"]
        trees = "—" if timing["trees_mean"] is None else f"{timing['trees_mean']:.0f}"
        lines.append(f"| {key} | {timing['n_fits']} | {timing['fit_seconds_total']:.1f} | {timing['fit_seconds_mean'] * 1000:.1f} | "
                     f"{timing['fit_seconds_median'] * 1000:.1f} | {timing['fit_seconds_p95'] * 1000:.1f} | {trees} |")
    lines += ["", "## MCS 90 %", "", "| arm | h | включены |", "|---|---|---|"]
    for arm, by_horizon in result["mcs"].items():
        for horizon, value in by_horizon.items():
            lines.append(f"| {arm} | h{horizon} | {', '.join(value['included'])} |")
    lines += ["", "## Вердикт", ""]
    for key, record in result["runs"].items():
        decision = record["decision"]
        if decision is None:
            lines.append(f"- `{key}` — эталон.")
            continue
        lines.append(f"- `{key}` — **{decision['decision']}**: {'; '.join(decision['reasons']) or 'нет замечаний'}.")
    lines += ["", f"MLflow: experiment `{mlog.EXPERIMENT}`; URI — в отчёте `docs/research/2026-11-01-cb-comp-backend-compare.md`."]
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Прогон
# ---------------------------------------------------------------------------


def run(archive: Path, panel: Path, public_dir: Path, out_dir: Path,
        version: str = DEFAULT_VERSION, backends=None, arms=None) -> dict:
    backends = tuple(backends or BACKEND_ORDER)
    arms = tuple(arms or ARMS)
    selected = [(b, a) for b, a in RUNS if b in backends and a in arms]
    if (CATBOOST, "defaults") not in selected:
        selected.insert(0, (CATBOOST, "defaults"))

    data = en.prepare(archive, panel, public_dir)
    truth = truth_map(data["target_full"])
    normal = data["normal"]
    all_test = cp.month_range(cp.TEST_START, cp.TEST_END)
    cb_err = cb_comp_errors(data["archive_data"], data["target_full"], normal)
    regressors = hc.load_regressors(public_dir)
    bs = {}
    for suffix, target in (("", data["t_live"]), ("_tf", truth)):
        for model in (hc.MODEL_ECON, hc.MODEL_ROZN):
            bs[model + suffix] = {h: hc.bs_predict(target[h], regressors[model], h, data["test_months"]) for h in HORIZONS}
    inputs = {
        "panel_csv": sha256(panel),
        "public_panel_csv": sha256(public_dir / "public_panel.csv"),
        "sheet_map_csv": sha256(public_dir / "sheet_map.csv"),
        "target_xlsx": sha256(archive / cp.TARGET_XLSX),
        "series2016_xlsx": sha256(archive / hc.SERIES2016),
        "series2009_xlsx": sha256(archive / hc.SERIES2009),
    }
    threads = os.cpu_count()
    train_window = [str(data["train_months"][0]), str(data["train_months"][-1]), len(data["train_months"])]
    test_window = [str(data["test_months"][0]), str(data["test_months"][-1]), len(data["test_months"])]
    sample = [
        {"horizon": h, "month": month.isoformat(), "target_full": float(truth[h][month]),
         "t_live": float(data["t_live"][h][month]) if month in data["t_live"][h] else None}
        for h in HORIZONS for month in normal
    ]
    sample_meta = {"rows": len(sample), "normal": len(normal), "first": normal[0].isoformat(),
                   "last": normal[-1].isoformat(), "horizons": list(HORIZONS)}
    spec_path = SPEC

    loggers = {}
    runs = {}
    all_timings = []
    try:
        for backend_name, arm in selected:
            record = {"backend": backend_name, "arm": arm, "model_live": f"{MODEL_CB}_{backend_name}",
                      "model_combo": f"{MODEL_COMBO}_{backend_name}"}
            backend = make_backend(backend_name, arm)
            config = {
                "version": version,
                "seed": SEED,
                "horizons": list(HORIZONS),
                "primary_horizons": list(PRIMARY_HORIZONS),
                "train": train_window,
                "test": test_window,
                "normal": len(normal),
                "models": [record["model_live"], record["model_combo"]],
                "truth": mlog.TRUTH,
                "unit": mlog.UNIT,
                "spec_path": spec_path,
                "inputs": inputs,
                "backend": backend_name,
                "backend_version": backend.version(),
                "arm": arm,
                "hyperparameters": backend.hyperparameters(),
                "threads": threads,
                "library_versions": {name: package_version(name) for name in ("catboost", "lightgbm", "xgboost", "pandas", "numpy")},
            }
            tags = {"stage": "backtest", "champion": CATBOOST, "backend": backend_name, "arm": arm}
            if backend_name != CATBOOST:
                tags["challenger"] = backend_name
            record["run_name"] = run_name(backend_name, arm, version)
            record["config"] = config
            record["backend_version"] = backend.version()
            record["hyperparameters"] = backend.hyperparameters()
            warm = warm_up(backend, data)
            print(f"[{run_key(backend_name, arm)}] warm-up {warm:.2f} s", flush=True)
            groups, timings, skipped = fit_predict_timed(backend, data)
            record["timing"] = timing_summary(timings, skipped, warm)
            all_timings.extend(timings)
            record["live"] = lb.aggregate_predictions(groups, data["weights"], data["test_months"])
            record["combo"] = {h: hc.combine_mean([record["live"][h], bs[hc.MODEL_ECON][h], bs[hc.MODEL_ROZN][h]]) for h in HORIZONS}
            record["_groups"] = groups
            runs[run_key(backend_name, arm)] = record
            print(f"[{run_key(backend_name, arm)}] fits={record['timing']['n_fits']} "
                  f"total={record['timing']['fit_seconds_total']:.1f} s", flush=True)

        reference = runs[run_key(CATBOOST, "defaults")]
        r_b1 = {"ok": None, "max_abs_diff": None}
        if not os.environ.get("CB_BACKEND_SKIP_RB1") == "1":
            groups_default = lb.fit_predict(data["specs"], data["features"], data["features"], data["targets"],
                                            data["train_months"], data["test_months"], with_mp=True)
            max_diff = 0.0
            for code, per_h in groups_default.items():
                for horizon, values in per_h.items():
                    for month, value in values.items():
                        other = reference["_groups"].get(code, {}).get(horizon, {}).get(month)
                        if other is None:
                            max_diff = float("inf")
                        else:
                            max_diff = max(max_diff, abs(value - other))
            r_b1 = {"ok": max_diff == 0.0, "max_abs_diff": max_diff}

        for key, record in runs.items():
            record["per_model"] = {
                record["model_live"]: evaluate_model(record["live"], truth, data["t_live"], cb_err, normal, all_test),
                record["model_combo"]: evaluate_model(record["combo"], truth, data["t_live"], cb_err, normal, all_test),
            }
            if record["backend"] == CATBOOST and record["arm"] == "defaults":
                record["pairs_vs_catboost"] = None
                record["combo_ratio"] = None
                record["decision"] = None
            else:
                record["pairs_vs_catboost"] = pairwise(record["live"], reference["live"], truth, normal)
                record["combo_ratio"] = combo_rmse_ratio(record["combo"], reference["combo"], truth, normal)
                record["decision"] = decide(record["pairs_vs_catboost"], record["combo_ratio"],
                                            record["timing"], reference["timing"])

        mcs = {arm: mcs_by_horizon(runs, arm, cb_err, truth, normal) for arm in arms if arm in {a for _b, a in selected}}

        ratios_live = {key: {h: record["per_model"][record["model_live"]][h]["vs_cb_target_full"]["ratio"] for h in HORIZONS}
                       for key, record in runs.items()}
        ratios_combo = {key: {h: record["per_model"][record["model_combo"]][h]["vs_cb_target_full"]["ratio"] for h in HORIZONS}
                        for key, record in runs.items()}
        reference_key = run_key(CATBOOST, "defaults")
        pins = {
            "r_b2": {
                "live_cb": {str(h): {"value": ratios_live[reference_key][h], "pin": TASK3_PINS[h],
                                     "diff": abs(ratios_live[reference_key][h] - TASK3_PINS[h])} for h in HORIZONS},
                "combo3": {str(h): {"value": ratios_combo[reference_key][h], "pin": TASK9_PINS[h],
                                    "diff": abs(ratios_combo[reference_key][h] - TASK9_PINS[h])} for h in HORIZONS},
            }
        }
        max_diff = max(item["diff"] for group in pins["r_b2"].values() for item in group.values())
        pins["r_b2"]["max_abs_diff"] = max_diff
        pins["r_b2"]["ok"] = max_diff <= PIN_TOL
        if max_diff > PIN_STOP:
            raise SystemExit(f"R-B2: пины Task 3/9 разошлись на {max_diff:.4f} > {PIN_STOP}: проверьте версию CatBoost и данные")

        for record in runs.values():
            record.pop("_groups", None)
        result = {
            "spec": spec_path,
            "version": version,
            "git_sha": git_sha(),
            "git_dirty": git_dirty(),
            "seed": SEED,
            "threads": threads,
            "library_versions": {name: package_version(name) for name in ("catboost", "lightgbm", "xgboost", "pandas", "numpy", "scipy", "scikit-learn")},
            "inputs": inputs,
            "train": train_window,
            "test": test_window,
            "normal": len(normal),
            "runs": runs,
            "ratios_live": ratios_live,
            "ratios_combo": ratios_combo,
            "mcs": mcs,
            "invariants": {"r_b1": r_b1, "r_b2": pins["r_b2"]},
            "decision_summary": {key: record["decision"]["decision"] for key, record in runs.items() if record["decision"]},
        }
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "result.json").write_text(json.dumps(lb._jsonable(result), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        write_predictions(out_dir / "predictions.csv", runs)
        write_timings(out_dir / "timings.csv", all_timings)
        (out_dir / "report.md").write_text(render_report(lb._jsonable(result)), encoding="utf-8")
        EVIDENCE_JSON.parent.mkdir(parents=True, exist_ok=True)
        EVIDENCE_JSON.write_text(json.dumps(lb._jsonable(result), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        for (backend_name, arm), record in runs.items():
            predictions_path = out_dir / f"predictions_{backend_name}__{arm}.csv"
            timings_path = out_dir / f"timings_{backend_name}__{arm}.csv"
            write_run_predictions(predictions_path, record)
            write_timings(timings_path, [row for row in all_timings
                                         if row["backend"] == backend_name and row["arm"] == arm])
            metrics = build_metrics(record, mcs.get(arm, {}), r_b1, pins["r_b2"], reference)
            artifacts = [out_dir / "result.json", out_dir / "report.md", predictions_path, timings_path]
            decision = record["decision"]["decision"] if record["decision"] else "reference"
            tags = {"stage": "backtest", "champion": CATBOOST, "backend": backend_name, "arm": arm}
            if backend_name != CATBOOST:
                tags["challenger"] = backend_name
            logger = mlog.start(record["run_name"], record["config"], cp.TEST_END, sample_meta,
                                sample, spec_path, tags=tags)
            loggers[(backend_name, arm)] = logger
            if logger is None:
                record["mlog_status"] = "disabled"
                continue
            folds = mlog.build_fold_rows({record["model_live"]: record["live"], record["model_combo"]: record["combo"]},
                                         truth, normal, [record["model_live"], record["model_combo"]],
                                         HORIZONS, t_live=data["t_live"], version=version)
            record["mlog_status"] = mlog.record(logger, folds, metrics, artifacts,
                                                f"version={version}; backend={backend_name}; arm={arm}",
                                                tags={"decision": decision})
        result["mlog"] = {key: record.get("mlog_status") for key, record in runs.items()}
        (out_dir / "result.json").write_text(json.dumps(lb._jsonable(result), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        EVIDENCE_JSON.write_text(json.dumps(lb._jsonable(result), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return result
    finally:
        for logger in loggers.values():
            if logger is not None:
                try:
                    logger.finish("failed", "aborted")
                except Exception:
                    pass


def build_metrics(record: dict, mcs: dict, r_b1: dict, r_b2: dict, reference: dict) -> dict:
    metrics = {"primary_metric": "rmse_pp"}
    for horizon in HORIZONS:
        live = record["per_model"][record["model_live"]][horizon]
        combo = record["per_model"][record["model_combo"]][horizon]
        metrics[f"rmse_target_full_normal_h{horizon}"] = live["rmse"]
        metrics[f"ratio_cb_comp_h{horizon}"] = live["vs_cb_target_full"]["ratio"]
        metrics[f"rmse_t_live_normal_h{horizon}"] = live["rmse_t_live"]
        metrics[f"rmse_target_full_all67_h{horizon}"] = live["rmse_all67"]
        metrics[f"combo3_rmse_h{horizon}"] = combo["rmse"]
        metrics[f"combo3_ratio_cb_comp_h{horizon}"] = combo["vs_cb_target_full"]["ratio"]
    if record["pairs_vs_catboost"]:
        for horizon in PRIMARY_HORIZONS:
            pair = record["pairs_vs_catboost"][horizon]
            metrics[f"dm_p_vs_catboost_h{horizon}"] = pair["dm_p"]
            metrics[f"delta_rmse_vs_catboost_h{horizon}"] = pair["delta"]
            metrics[f"ci_low_vs_catboost_h{horizon}"] = pair["delta_ci"][0]
            metrics[f"ci_high_vs_catboost_h{horizon}"] = pair["delta_ci"][1]
            metrics[f"mde_ratio_h{horizon}"] = pair["mde_ratio"]
            metrics[f"combo3_ratio_h{horizon}"] = record["combo_ratio"][horizon]
    timing = record["timing"]
    for name in ("n_fits", "n_fits_skipped", "fit_seconds_total", "fit_seconds_mean",
                 "fit_seconds_median", "fit_seconds_p95", "fit_seconds_max",
                 "predict_seconds_total", "predict_seconds_median"):
        metrics[name] = timing[name]
    if timing["trees_mean"] is not None:
        metrics["trees_mean"] = timing["trees_mean"]
    metrics["r_b1_ok"] = 1.0 if r_b1["ok"] else 0.0
    metrics["r_b2_max_abs_diff"] = r_b2["max_abs_diff"]
    metrics["r_b2_ok"] = 1.0 if r_b2["ok"] else 0.0
    if record["decision"]:
        metrics["decision_swap"] = 1.0 if record["decision"]["checks"]["swap"] else 0.0
        metrics["decision_speed"] = 1.0 if record["decision"]["checks"]["speed"] else 0.0
        metrics["speed_ratio_median"] = timing["fit_seconds_median"] / reference["timing"]["fit_seconds_median"]
    for horizon in PRIMARY_HORIZONS:
        if horizon in mcs:
            metrics[f"mcs_included_h{horizon}"] = 1.0 if f"{MODEL_CB}_{record['backend']}" in mcs[horizon]["included"] else 0.0
    numeric = {name: float(value) for name, value in metrics.items()
               if isinstance(value, (int, float)) and math.isfinite(value)}
    numeric["primary_metric"] = "rmse_pp"
    return numeric


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--archive", default=None)
    parser.add_argument("--panel", default=None, help="по умолчанию — панель версии")
    parser.add_argument("--public-dir", default=str(lb.PUBLIC_DIR))
    parser.add_argument("--version", choices=VERSIONS, default=DEFAULT_VERSION,
                        help="версия цели: 2010 — канон, 2009 — T_LIVE с 2009-01")
    parser.add_argument("--backend", choices=("all",) + BACKEND_ORDER, default="all")
    parser.add_argument("--arm", choices=("all",) + ARMS, default="all")
    parser.add_argument("--out", default=None, help="по умолчанию build/cb-comp-backend-compare[-<версия>]")
    args = parser.parse_args(argv)
    panel = Path(args.panel) if args.panel else lb.default_panel(args.version)
    lb.check_panel_version(lb.load_group_panel(panel)[0], args.version)
    out_dir = Path(args.out) if args.out else lb.versioned_dir(OUT_DIR, args.version)
    archive = cp.resolve_archive(args.archive)
    backends = BACKEND_ORDER if args.backend == "all" else (args.backend,)
    arms = ARMS if args.arm == "all" else (args.arm,)
    result = run(archive, panel, Path(args.public_dir), out_dir, args.version, backends, arms)
    print(json.dumps({
        "decision": result["decision_summary"],
        "ratios_live": {key: {str(h): round(v, 3) for h, v in per.items()} for key, per in result["ratios_live"].items()},
        "invariants": {"r_b1": result["invariants"]["r_b1"]["ok"], "r_b2": result["invariants"]["r_b2"]["ok"]},
        "mlog": result.get("mlog"),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
