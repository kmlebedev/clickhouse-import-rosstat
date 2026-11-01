#!/usr/bin/env python3
"""H2O AutoML как генератор challenger'а для CPI v4 (#502).

Пункт `key-rate/phase-4-cpi-h2o-automl`, роль ML. Спека
`docs/superpowers/specs/2026-11-01-cpi-h2o-automl-design.md`.

Этапы:

1. `--stage select` — один прогон `H2OAutoML` на до-тестовых origin'ах
   (`O + h <= 2015-12`, h1) с `fold_column` из временных блоков; семейство и
   гиперпараметры замораживаются в `select.json` до прогонов на 2016+.
2. `--stage refit` — замороженный оценщик переобучается в walk-forward тем же
   движком, что сетка v4 (`cpi_v4_grid`): `h2o_bottomup` (3 компоненты + NNLS)
   и `h2o_direct`; сравнение с `v3_bottomup` (primary) и `v2_bottomup`.
3. `--stage gate` — решение по предрегистрированным условиям спеки §5.

Скрипт только читает ClickHouse; в БД и `forecast_log` не пишет. Паспорт —
`ML_LOG=1` (эксперимент `cpi_v4_h2o`), MLflow — по умолчанию.
"""

import argparse
import contextlib
import json
import os
import subprocess
import sys
import time
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from lib import ml_experiment as mlx  # noqa: E402
from models.cpi import cpi_monthly as cm  # noqa: E402
from models.cpi import cpi_monthly_v3 as v3  # noqa: E402
from models.cpi import cpi_v4_grid as grid  # noqa: E402
from models.cpi import cpi_v4_leaderboard as lb  # noqa: E402
from models.cpi import cpi_weekly_aggregate as agg  # noqa: E402

SPEC_PATH = "docs/superpowers/specs/2026-11-01-cpi-h2o-automl-design.md"
SELECT_CUTOFF = date(2015, 12, 1)
SELECT_FOLDS = 3
MIN_BLOCK = 10
MIN_SELECTOR_ROWS = 60
MAX_MODELS = 20
H2O_VERSION = "3.46.0.12"
REFIT_REFERENCE = "v3_bottomup"
REFIT_REFERENCE_SECONDARY = "v2_bottomup"
REFIT_MODELS = ("B1", "B2", "B3", REFIT_REFERENCE_SECONDARY, REFIT_REFERENCE, "h2o_bottomup", "h2o_direct")
ALL_TARGETS = (cm.TARGET_ALL,) + tuple(cm.BOTTOMUP_COMPONENTS)

# Гиперпараметры, переносимые в refit по семейству. Остальные (служебные:
# training_frame, model_id, fold_column, nfolds, …) отбрасываются и
# перечисляются в select.json.
PARAM_WHITELIST = {
    "GBM": (
        "ntrees", "max_depth", "min_rows", "learn_rate", "learn_rate_annealing",
        "sample_rate", "col_sample_rate", "col_sample_rate_per_tree",
        "col_sample_rate_change_per_level", "histogram_type", "nbins", "nbins_cats",
        "reg_alpha", "reg_lambda", "max_abs_leafnode_pred", "categorical_encoding",
        "score_tree_interval", "stopping_rounds",
    ),
    "XGBoost": (
        "ntrees", "max_depth", "min_rows", "learn_rate", "sample_rate",
        "col_sample_rate", "colsample_bylevel", "colsample_bynode", "subsample",
        "reg_alpha", "reg_lambda", "tree_method", "booster", "categorical_encoding",
        "score_tree_interval", "stopping_rounds",
    ),
    "DRF": (
        "ntrees", "max_depth", "min_rows", "mtries", "sample_rate",
        "col_sample_rate_per_tree", "histogram_type", "nbins", "nbins_cats",
        "categorical_encoding", "score_tree_interval",
    ),
    "GLM": (
        "lambda_search", "alpha", "lambda_min_ratio", "standardize", "family",
        "link", "solver", "nlambdas", "max_iterations",
    ),
}
FAMILY_CLASSES = {
    "GBM": "H2OGradientBoostingEstimator",
    "XGBoost": "H2OXGBoostEstimator",
    "DRF": "H2ORandomForestEstimator",
    "GLM": "H2OGeneralizedLinearEstimator",
}


class SelectorFrameError(RuntimeError):
    """До-тестовый кадр селектора меньше минимума или блок пуст."""


class H2OUnavailable(RuntimeError):
    """H2O или JVM недоступны; это не повод «обходить» окружение."""


# ---------------------------------------------------------------------------
# Чистый слой: селектор, кадры, заморозка конфига
# ---------------------------------------------------------------------------


def selector_origins(origins: list, horizon: int) -> list:
    """Origin'ы, чья цель `O+1…O+h` наблюдена до primary-окна (≤ 2015-12)."""
    return [origin for origin in sorted(origins) if lb.shift_month(origin, horizon) <= SELECT_CUTOFF]


def fold_assignment(origins: list, folds: int = SELECT_FOLDS, min_block: int = MIN_BLOCK) -> dict:
    """`{origin: fold}`: `folds` последовательных блоков по времени, без перемешивания."""
    ordered = sorted(origins)
    total = len(ordered)
    if total < folds * min_block:
        raise SelectorFrameError(f"кадр селектора {total} строк < {folds} × {min_block}")
    bounds = [(index * total) // folds for index in range(folds + 1)]
    assignment = {}
    for index in range(folds):
        block = ordered[bounds[index]:bounds[index + 1]]
        if len(block) < min_block:
            raise SelectorFrameError(f"блок {index}: {len(block)} < {min_block}")
        for origin in block:
            assignment[origin] = index
    return assignment


def frame_records(rows: list, names: list, targets=(cm.TARGET_ALL,)) -> list:
    """Строки кадра: признаки + конечные таргеты; NaN остаётся NaN (не 0)."""
    records = []
    for row in rows:
        record = {name: row.features.get(name) for name in names}
        for target in targets:
            record[target] = float(row.y[target])
        records.append(record)
    return records


def to_dataframe(records: list, names: list, targets, categorical=("month",)):
    """pandas-кадр с явными типами: числовые — float, `month` — строка (enum в H2O)."""
    import pandas as pd

    frame = pd.DataFrame(records)
    for name in names:
        if name in categorical:
            frame[name] = frame[name].astype("Int64").astype(str)
        else:
            frame[name] = pd.to_numeric(frame[name], errors="coerce").astype("float64")
    for target in targets:
        frame[target] = pd.to_numeric(frame[target], errors="coerce").astype("float64")
    return frame


def frame_digest(records: list, names: list, targets) -> str:
    """sha256 кадра селектора: значения признаков и таргета в порядке строк."""
    payload = {
        "names": list(names),
        "targets": list(targets),
        "rows": [[record.get(name) for name in names] + [record.get(target) for target in targets] for record in records],
    }
    return mlx.sha256_text(mlx.canonical_json(mlx.json_safe(payload)))


def family_of(model_id: str) -> str:
    """Семейство по `model_id`: `XRT_…` — это DRF с `histogram_type=Random`."""
    head = model_id.split("_")[0]
    return "DRF" if head == "XRT" else head


def frozen_params(family: str, actual: dict) -> tuple:
    """`(params, dropped)`: whitelist гиперпараметров и отброшенные имена."""
    allowed = PARAM_WHITELIST[family]
    params = {key: value for key, value in (actual or {}).items() if key in allowed and value is not None}
    dropped = sorted(key for key in (actual or {}) if key not in allowed)
    return params, dropped


def freeze_config(model_id: str, actual: dict, leaderboard: list, seed: int,
                  digest: str, origins: list, frame_rows: int, h2o_version: str, java_version: str) -> dict:
    """Замороженный конфиг challenger'а; пишется до прогонов на 2016+."""
    family = family_of(model_id)
    if family not in FAMILY_CLASSES:
        raise SystemExit(f"семейство {family!r} не поддерживается refit (модель {model_id})")
    params, dropped = frozen_params(family, actual)
    return {
        "family": family,
        "model_id": model_id,
        "params": params,
        "dropped_params": dropped,
        "leaderboard": leaderboard,
        "cv_rmse": next((item["rmse"] for item in leaderboard if item["model_id"] == model_id), None),
        "seed": int(seed),
        "folds": SELECT_FOLDS,
        "max_models": MAX_MODELS,
        "selector_cutoff": SELECT_CUTOFF.isoformat(),
        "selector_first_origin": origins[0].isoformat() if origins else None,
        "selector_last_origin": origins[-1].isoformat() if origins else None,
        "selector_rows": int(frame_rows),
        "frame_sha256": digest,
        "h2o_version": h2o_version,
        "java_version": java_version,
    }


def validate_frozen(config: dict) -> dict:
    """Проверка замороженного конфига перед refit (fail fast, без «догадок»)."""
    family = config.get("family")
    if family not in FAMILY_CLASSES:
        raise SystemExit(f"в select.json семейство {family!r} не поддерживается")
    if not config.get("params") and family != "GLM":
        raise SystemExit(f"в select.json пустой набор гиперпараметров для {family}")
    if config.get("folds") != SELECT_FOLDS:
        raise SystemExit(f"select.json: folds={config.get('folds')}, ожидалось {SELECT_FOLDS}")
    return config


# ---------------------------------------------------------------------------
# H2O: кластер, оценщик, селектор
# ---------------------------------------------------------------------------


def import_h2o():
    """Ленивый импорт H2O с понятной ошибкой, если окружение не готово."""
    try:
        import h2o  # noqa: PLC0415
    except ImportError as exc:  # pragma: no cover - зависит от окружения
        raise H2OUnavailable(
            "H2O не установлен: запускайте через make cpi-v4-h2o (uv run --with h2o)"
        ) from exc
    return h2o


def java_version() -> str:
    """Версия JVM для паспорта; пустая строка, если java недоступна."""
    try:
        proc = subprocess.run(["java", "-version"], capture_output=True, text=True, check=False, timeout=20)
    except (OSError, subprocess.SubprocessError):
        return ""
    text = (proc.stderr or proc.stdout or "").strip().splitlines()
    return text[0] if text else ""


def java_major(text: str):
    """Мажорная версия из вывода `java -version`; `1.8.0_x` читается как 8."""
    if not text:
        return None
    start = text.find('"')
    end = text.find('"', start + 1) if start >= 0 else -1
    token = text[start + 1:end] if start >= 0 and end > start else text
    token = token.replace("_", ".").split("-")[0]
    parts = []
    for piece in token.split("."):
        digits = "".join(ch for ch in piece if ch.isdigit())
        if not digits:
            break
        parts.append(int(digits))
    if not parts:
        return None
    if parts[0] == 1 and len(parts) > 1:
        return parts[1]
    return parts[0]


def ensure_supported_java(text: str) -> None:
    """H2O 3.46 работает только на Java 8–17; версия вне диапазона — стоп."""
    if not text:
        raise H2OUnavailable("java не найдена в PATH: H2O 3.46 требует JDK 8–17 (make cpi-v4-h2o)")
    major = java_major(text)
    if major is None or not 8 <= major <= 17:
        raise H2OUnavailable(f"Java {major}: H2O 3.46 поддерживает только 8–17 ({text})")


@contextlib.contextmanager
def h2o_cluster(tmp_dir: Path):
    """Локальный кластер H2O: один поток, изолированные логи, shutdown в finally."""
    h2o = import_h2o()
    tmp_dir.mkdir(parents=True, exist_ok=True)
    last_error = None
    for port in (54321, 54322, 54323, 54324, 54325):
        try:
            h2o.init(
                nthreads=1,
                max_mem_size="4G",
                port=port,
                name="cpi_v4_h2o",
                ice_root=str(tmp_dir),
                log_dir=str(tmp_dir),
                strict_version_check=True,
            )
            break
        except Exception as exc:  # noqa: BLE001 - H2O бросает разные типы при занятом порте
            last_error = exc
    else:
        raise H2OUnavailable(f"кластер H2O не поднялся ни на одном порту: {last_error}")
    h2o.no_progress()
    try:
        yield h2o
    finally:
        with contextlib.suppress(Exception):
            h2o.cluster().shutdown(prompt=False)


def _leaderboard_rows(aml) -> list:
    frame = aml.leaderboard.as_data_frame()
    columns = [name for name in ("model_id", "rmse", "mae", "mse") if name in frame.columns]
    rows = []
    for _, item in frame.head(10).iterrows():
        row = {"model_id": str(item["model_id"]), "family": family_of(str(item["model_id"]))}
        for column in columns:
            if column != "model_id":
                row[column] = float(item[column])
        rows.append(row)
    return rows


def run_select(sample: str, out_dir: Path, seed: int = lb.SEED) -> dict:
    """Селектор на до-тестовых origin'ах; пишет select.json (+ второй seed)."""
    if sample != "S_L09":
        raise SystemExit("селектор определён только для S_L09 (спека §3)")
    rows, _skipped = agg.build_sample_v4(sample, with_diffusion=True)
    series = lb.load_target_series()[ "T_NSA"]
    targets = cm.load_targets()
    components = {
        code: {cm.month_of(day): float(value) for day, value in targets[code]}
        for code in cm.BOTTOMUP_COMPONENTS
    }
    excluded = agg.EXCLUDED_BASE_FEATURES.get(sample, ())
    java = java_version()
    ensure_supported_java(java)
    with grid.feature_override(excluded):
        names = v3.feature_names_v3()
        origin_rows = grid.build_origin_rows(rows, "T_NSA", 1, series, components)
        origins = selector_origins(list(origin_rows), 1)
        if len(origins) < MIN_SELECTOR_ROWS:
            raise SystemExit(f"селектор: {len(origins)} origin'ов < {MIN_SELECTOR_ROWS}")
        assignment = fold_assignment(origins)
        records = frame_records([origin_rows[origin] for origin in origins], names)
    dataframe = to_dataframe(records, names, (cm.TARGET_ALL,))
    dataframe["sel_fold"] = [assignment[origin] for origin in origins]
    digest = frame_digest(records, names, (cm.TARGET_ALL,))
    print(
        f"Селектор: {len(origins)} origin'ов ({origins[0]}…{origins[-1]}), "
        f"{SELECT_FOLDS} блока, кадр sha256 {digest[:12]}",
        flush=True,
    )
    runs = []
    with h2o_cluster(out_dir / "tmp") as h2o:
        from h2o.automl import H2OAutoML  # noqa: PLC0415

        frame = h2o.H2OFrame(dataframe)
        frame["month"] = frame["month"].asfactor()
        for run_seed in (seed, seed + 1):
            aml = H2OAutoML(
                max_models=MAX_MODELS,
                seed=run_seed,
                sort_metric="RMSE",
                stopping_metric="RMSE",
                exclude_algos=["DeepLearning", "StackedEnsemble"],
                fold_column="sel_fold",
                keep_cross_validation_predictions=False,
                project_name=f"cpi_v4_h2o_select_{sample}_h1_{run_seed}",
                verbosity="warn",
            )
            aml.train(x=names, y=cm.TARGET_ALL, training_frame=frame)
            leader = aml.leader
            leaderboard = _leaderboard_rows(aml)
            runs.append(
                {
                    "seed": run_seed,
                    "leader": leader.model_id,
                    "family": family_of(leader.model_id),
                    "leaderboard": leaderboard,
                    "actual_params": mlx.json_safe(dict(leader.actual_params or {})),
                    "h2o_version": h2o.__version__,
                }
            )
            print(f"  seed {run_seed}: лидер {leader.model_id} ({runs[-1]['family']}), CV RMSE {leaderboard[0].get('rmse')}", flush=True)
            with contextlib.suppress(Exception):
                h2o.remove(leader)
        with contextlib.suppress(Exception):
            h2o.remove(frame)
    stable = len({run["family"] for run in runs}) == 1
    if stable:
        chosen = runs[0]
    else:
        # Замораживаем семейство с меньшим средним CV-RMSE по двум seed'ам.
        by_family = {}
        for run in runs:
            by_family.setdefault(run["family"], []).append(run["leaderboard"][0].get("rmse", float("inf")))
        family = min(by_family, key=lambda name: sum(by_family[name]) / len(by_family[name]))
        chosen = next(run for run in runs if run["family"] == family)
    frozen = freeze_config(
        chosen["leader"],
        chosen["actual_params"],
        chosen["leaderboard"],
        chosen["seed"],
        digest,
        origins,
        len(records),
        chosen["h2o_version"],
        java,
    )
    frozen["selection_stable"] = stable
    frozen["runs"] = [{key: run[key] for key in ("seed", "leader", "family", "leaderboard")} for run in runs]
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "select.json"
    path.write_text(json.dumps(frozen, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    print(f"Заморожено: {frozen['family']} / {frozen['model_id']} → {path}", flush=True)
    return frozen


# ---------------------------------------------------------------------------
# Refit: замороженный оценщик в walk-forward
# ---------------------------------------------------------------------------


def build_estimator(h2o, frozen: dict, seed: int):
    """Оценщик замороженного семейства с фиксированными параметрами."""
    module = __import__("h2o.estimators", fromlist=[FAMILY_CLASSES[frozen["family"]]])
    cls = getattr(module, FAMILY_CLASSES[frozen["family"]])
    params = dict(frozen.get("params") or {})
    params.setdefault("seed", seed)
    return cls(**params)


class FrameCache:
    """Один H2O-кадр на фолд: 4 таргета и тестовая строка, модели — по требованию."""

    def __init__(self, h2o, names: list, frozen: dict, seed: int):
        self.h2o = h2o
        self.names = names
        self.frozen = frozen
        self.seed = seed
        self._key = None
        self._train = None
        self._test = None
        self._models = {}

    def _drop(self) -> None:
        for model in self._models.values():
            with contextlib.suppress(Exception):
                self.h2o.remove(model)
        self._models = {}
        for frame in (self._train, self._test):
            if frame is not None:
                with contextlib.suppress(Exception):
                    self.h2o.remove(frame)
        self._train = self._test = None
        self._key = None

    def _ensure(self, train: list, row) -> None:
        if self._key == row.month:
            return
        self._drop()
        records = frame_records(train, self.names, ALL_TARGETS)
        dataframe = to_dataframe(records, self.names, ALL_TARGETS)
        self._train = self.h2o.H2OFrame(dataframe)
        self._train["month"] = self._train["month"].asfactor()
        test_records = frame_records([row], self.names, ())
        test_frame = to_dataframe(test_records, self.names, ())
        self._test = self.h2o.H2OFrame(test_frame)
        self._test["month"] = self._test["month"].asfactor()
        self._key = row.month

    def predict(self, train: list, row, target: str) -> float:
        self._ensure(train, row)
        if target not in self._models:
            model = build_estimator(self.h2o, self.frozen, self.seed)
            model.train(x=self.names, y=target, training_frame=self._train)
            self._models[target] = model
        prediction = self._models[target].predict(self._test)
        value = float(prediction[0, 0])
        with contextlib.suppress(Exception):
            self.h2o.remove(prediction)
        return value

    def clear(self) -> None:
        self._drop()


def make_models(target: str, sample: str, series: dict, horizon: int, cache: FrameCache) -> dict:
    """`{имя: fit_predict}`: бенчмарки + v2/v3 + замороженный H2O (спека §4)."""

    def h2o_direct(train, row):
        return cache.predict(train, row, cm.TARGET_ALL)

    def h2o_bottomup(train, row):
        weights = cm.nnls_weights(train)
        parts = [cache.predict(train, row, code) for code in cm.BOTTOMUP_COMPONENTS]
        return float(sum(weight * part for weight, part in zip(weights, parts)))

    models = {
        "B1": grid.make_b1(series, horizon),
        "B2": cm.fit_ar1,
        "B3": cm.fit_enet_mp,
        "v2_bottomup": cm.fit_catboost_bottomup_mp,
        "v3_bottomup": v3.fit_v3_bottomup,
        "h2o_bottomup": h2o_bottomup,
        "h2o_direct": h2o_direct,
    }
    return {name: models[name] for name in REFIT_MODELS}


def merge_secondary(result: dict, predictions: list, horizon: int, primary_from) -> dict:
    """Добавляет пары против `v2_bottomup`, не меняя primary-эталон `v3_bottomup`."""
    secondary = grid.evaluate_cell(predictions, horizon, REFIT_REFERENCE_SECONDARY, primary_from)
    for name, item in secondary.get("pairs", {}).items():
        result.setdefault("pairs", {}).setdefault(name, item)
    return result


def run_cell(rows: list, series: dict, components: dict, sample: str, horizon: int,
             quick: bool, frozen: dict, cache: FrameCache) -> tuple:
    config = grid.SAMPLE_CONFIG[sample]
    origin_rows = grid.build_origin_rows(rows, "T_NSA", horizon, series, components)
    origins = sorted(origin_rows)
    eval_from = None
    primary_from = config["primary_from"]
    if quick and origins:
        eval_from = lb.shift_month(origins[-1], -(grid.QUICK_ORIGINS - 1))
        primary_from = max(primary_from, eval_from) if primary_from else eval_from
    excluded = agg.EXCLUDED_BASE_FEATURES.get(sample, ())
    started = time.time()
    with grid.feature_override(excluded):
        models = make_models("T_NSA", sample, series, horizon, cache)
        predictions = grid.run_walk_forward(origin_rows, models, horizon, config["min_train"], eval_from)
    cache.clear()
    result = grid.evaluate_cell(predictions, horizon, REFIT_REFERENCE, primary_from)
    result = merge_secondary(result, predictions, horizon, primary_from)
    result.update(
        {
            "target": "T_NSA",
            "sample": sample,
            "quick": quick,
            "n_origins": len(origins),
            "min_train": config["min_train"],
            "primary_from": primary_from.isoformat() if primary_from else None,
            "seconds": round(time.time() - started, 1),
            "h2o": {
                "family": frozen["family"],
                "model_id": frozen["model_id"],
                "frame_sha256": frozen.get("frame_sha256"),
                "h2o_version": frozen.get("h2o_version"),
            },
        }
    )
    return result, predictions


def passport_config(sample: str, horizons: list, quick: bool, frozen: dict) -> dict:
    config = grid.SAMPLE_CONFIG[sample]
    return {
        "sample": sample,
        "targets": ["T_NSA"],
        "horizons": list(horizons),
        "quick": bool(quick),
        "min_train": int(config["min_train"]),
        "primary_from": config["primary_from"].isoformat() if config["primary_from"] else None,
        "seed": lb.SEED,
        "models": {"T_NSA": list(REFIT_MODELS)},
        "bootstrap": {"block": lb.BOOTSTRAP_BLOCK, "n": lb.BOOTSTRAP_N},
        "mcs": {"alpha": lb.MCS_ALPHA, "reps": lb.MCS_REPS},
        "h2o": {
            "family": frozen["family"],
            "model_id": frozen["model_id"],
            "params": frozen.get("params"),
            "frame_sha256": frozen.get("frame_sha256"),
            "h2o_version": frozen.get("h2o_version"),
            "java_version": frozen.get("java_version"),
            "max_models": frozen.get("max_models"),
            "folds": frozen.get("folds"),
            "selector_cutoff": frozen.get("selector_cutoff"),
            "selection_stable": frozen.get("selection_stable"),
        },
        "spec": SPEC_PATH,
    }


def run_refit(sample: str, horizons: list, quick: bool, out_dir: Path, select_path: Path) -> dict:
    frozen = validate_frozen(json.loads(select_path.read_text(encoding="utf-8")))
    rows, _skipped = agg.build_sample_v4(sample, with_diffusion=True)
    if not rows:
        raise SystemExit(f"пустая выборка {sample!r}")
    series = lb.load_target_series()["T_NSA"]
    targets = cm.load_targets()
    components = {
        code: {cm.month_of(day): float(value) for day, value in targets[code]}
        for code in cm.BOTTOMUP_COMPONENTS
    }
    cell_dir = out_dir / f"{sample}_{'quick' if quick else 'full'}"
    results, predictions_all = {}, {}
    java = java_version()
    ensure_supported_java(java)
    with h2o_cluster(out_dir / "tmp") as h2o, grid.feature_override(agg.EXCLUDED_BASE_FEATURES.get(sample, ())):
        names = v3.feature_names_v3()
        cache = FrameCache(h2o, names, frozen, int(frozen.get("seed", lb.SEED)))
        for horizon in horizons:
            result, predictions = run_cell(rows, series, components, sample, horizon, quick, frozen, cache)
            grid._write_cell(cell_dir, "T_NSA", horizon, result, predictions)
            results[f"T_NSA:h{horizon}"] = result
            predictions_all[horizon] = predictions
            best = min(result["models"].items(), key=lambda kv: kv[1]["rmse"]) if result["status"] == "ok" else None
            print(
                f"  T_NSA h{horizon}: {result['status']}, фолдов {result['n_common']}, "
                f"{'лучший ' + best[0] + f' {best[1]['rmse']:.4f}' if best else '—'}, {result['seconds']} с",
                flush=True,
            )
        cache.clear()
    summary = {
        "complete": all(result["status"] == "ok" for result in results.values()),
        "sample": sample,
        "quick": quick,
        "cells": results,
        "frozen": {key: frozen.get(key) for key in ("family", "model_id", "frame_sha256", "selection_stable", "h2o_version", "java_version")},
        "config": passport_config(sample, horizons, quick, frozen),
    }
    cell_dir.mkdir(parents=True, exist_ok=True)
    json_path = cell_dir / "cpi_v4_h2o_result.json"
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    md_path = cell_dir / "cpi_v4_h2o_report.md"
    write_report(md_path, summary)
    if os.environ.get("ML_LOG") == "1":
        try:
            logger = mlx.start_run(
                experiment="cpi_v4_h2o",
                run_name=f"cpi_v4_h2o__{sample}__{'quick' if quick else 'full'}",
                config=summary["config"],
                data_asof=rows[-1].month,
                sample_meta={
                    "rows": len(rows),
                    "first": rows[0].month.isoformat(),
                    "last": rows[-1].month.isoformat(),
                    "quick": quick,
                },
                sample=[
                    {"month": row.month.isoformat(), "y": mlx.json_safe(row.y), "features": mlx.json_safe(row.features)}
                    for row in rows
                ],
                spec_path=SPEC_PATH,
                enabled=True,
            )
            logger.log_metrics({"sample": sample, "quick": quick, "cells": results})
            for horizon, items in predictions_all.items():
                logger.log_predictions(grid.passport_prediction_rows(items, "T_NSA", sample))
            logger.log_artifacts([json_path, md_path, select_path])
            logger.finish("finished", f"sample={sample}; quick={quick}; family={frozen['family']}")
        except Exception as exc:  # паспорт не должен ронять уже посчитанный результат
            print(f"Журнал эксперимента не записан: {type(exc).__name__}: {exc}", flush=True)
    return summary


# ---------------------------------------------------------------------------
# Gate
# ---------------------------------------------------------------------------


def _read_cell(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _pair(cell: dict, left: str, right: str):
    return (cell.get("pairs") or {}).get(f"{left} vs {right}")


def _rmse(cell: dict, model: str):
    item = cell.get("models", {}).get(model)
    return None if item is None else float(item["rmse"])


def run_gate(out_dir: Path, gate_dir: Path = None) -> dict:
    """Решение по условиям спеки §5 на прочитанных ячейках (без новых прогонов)."""
    gate_dir = gate_dir or (ROOT / "build/ml-gate")
    cells = {}
    for sample, prefix in (("S_L09", "S_L09_full"), ("S_L13", "S_L13_full"), ("S_W22", "S_W22_full")):
        for horizon in (1, 3, 6):
            path = out_dir / prefix / f"cell_T_NSA_h{horizon}.json"
            if path.exists():
                cells[f"{sample}:h{horizon}"] = _read_cell(path)
    checks = {}

    def check(name: str, condition, evidence):
        checks[name] = {"passed": bool(condition), "evidence": evidence}

    h1 = cells.get("S_L09:h1")
    h3 = cells.get("S_L09:h3")
    h6 = cells.get("S_L09:h6")
    primary = _pair(h1, "h2o_bottomup", REFIT_REFERENCE) if h1 else None
    h1_delta = primary["delta"] if primary else None
    h1_sig = bool(primary and primary["dm_p"] < 0.05 and (primary["delta_ci"][0] > 0 or primary["delta_ci"][1] < 0))
    check("1_h1_point_better", h1_delta is not None and h1_delta < 0, {"delta": h1_delta})
    check("2_h1_significant", h1_sig, primary or {})
    h3_pair = _pair(h3, "h2o_bottomup", REFIT_REFERENCE) if h3 else None
    check("3_h3_sign", bool(h3_pair and h3_pair["delta"] < 0), {"delta": None if not h3_pair else h3_pair["delta"]})
    robustness = {}
    for sample in ("S_L13", "S_W22"):
        cell = cells.get(f"{sample}:h1")
        pair = _pair(cell, "h2o_bottomup", REFIT_REFERENCE) if cell else None
        robustness[sample] = None if not pair else pair["delta"]
    check("4_robustness_sign", all(value is not None and value < 0 for value in robustness.values()), robustness)
    h6_pair = _pair(h6, "h2o_bottomup", REFIT_REFERENCE) if h6 else None
    degraded = bool(h6_pair and h6_pair["dm_p"] < 0.05 and h6_pair["delta"] > 0)
    check("5_h6_no_degradation", h6_pair is not None and not degraded, h6_pair or {})
    mcs = (h1 or {}).get("mcs", {}).get("included", [])
    check("6_mcs_included", "h2o_bottomup" in mcs, {"included": mcs})

    required = ("S_L09:h1", "S_L09:h3", "S_L09:h6", "S_L13:h1", "S_W22:h1")
    if not all(cells.get(key) and cells[key]["status"] == "ok" for key in required):
        decision = "incomplete"
        reason = "не все ячейки посчитаны: нужно " + ", ".join(required)
    elif not checks["1_h1_point_better"]["passed"]:
        decision = "reject"
        reason = "h1: точечное улучшение над v3_bottomup отсутствует"
    elif all(checks[key]["passed"] for key in ("1_h1_point_better", "2_h1_significant", "3_h3_sign", "4_robustness_sign", "5_h6_no_degradation")):
        decision = "promote"
        reason = "все условия §5 выполнены (MCS — справочно)"
    else:
        decision = "iterate"
        failed = [key for key in ("1_h1_point_better", "2_h1_significant", "3_h3_sign", "4_robustness_sign", "5_h6_no_degradation") if not checks[key]["passed"]]
        reason = "не выполнены: " + ", ".join(failed)
    result = {
        "decision": decision,
        "reason": reason,
        "reference": REFIT_REFERENCE,
        "secondary_reference": REFIT_REFERENCE_SECONDARY,
        "checks": checks,
        "cells": {
            key: {
                "n_common": cell.get("n_common"),
                "rmse": {model: _rmse(cell, model) for model in ("h2o_bottomup", "h2o_direct", REFIT_REFERENCE, REFIT_REFERENCE_SECONDARY, "B3")},
                "pair": _pair(cell, "h2o_bottomup", REFIT_REFERENCE),
            }
            for key, cell in cells.items()
        },
        "mcs_note": "MCS 90 % справочно: в #396 он включил все 7 моделей и не различает",
    }
    gate_dir = ROOT / "build/ml-gate"
    gate_dir.mkdir(parents=True, exist_ok=True)
    (gate_dir / "cpi_v4_h2o.json").write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    write_gate_report(gate_dir / "cpi_v4_h2o.md", result)
    return result


def write_gate_report(path: Path, result: dict) -> None:
    lines = [
        "# CPI v4 × H2O AutoML: gate (#502)",
        "",
        f"**Решение:** `{result['decision']}` — {result['reason']}",
        "",
        f"Эталон gate: `{result['reference']}`; справочно `{result['secondary_reference']}`.",
        "",
        "| условие | выполнено | свидетельство |",
        "|---|:---:|---|",
    ]
    for name, item in result["checks"].items():
        evidence = json.dumps(item["evidence"], ensure_ascii=False, default=str)
        lines.append(f"| {name} | {'да' if item['passed'] else 'нет'} | `{evidence[:180]}` |")
    lines += ["", "## Ячейки", "", "| ячейка | фолдов | h2o_bottomup | v3_bottomup | Δ | DM p |", "|---|---:|---:|---:|---:|---:|"]
    for key, cell in sorted(result["cells"].items()):
        pair = cell.get("pair") or {}
        rmse = cell.get("rmse", {})
        lines.append(
            f"| {key} | {cell.get('n_common')} | {rmse.get('h2o_bottomup')} | {rmse.get('v3_bottomup')} "
            f"| {pair.get('delta')} | {pair.get('dm_p')} |"
        )
    lines += ["", result["mcs_note"]]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_report(path: Path, summary: dict) -> None:
    lines = [
        f"# CPI v4 × H2O AutoML: {summary['sample']} ({'quick' if summary['quick'] else 'full'})",
        "",
        f"Замороженный конфиг: `{summary['frozen']['family']}` / `{summary['frozen']['model_id']}` "
        f"(`selection_stable={summary['frozen']['selection_stable']}`, h2o {summary['frozen']['h2o_version']}).",
        "",
        "RMSE/MAE — п.п. накопленного роста `O+1…O+h`, общие фолды primary-окна.",
    ]
    for key, cell in summary["cells"].items():
        lines += ["", f"## {key}", ""]
        if cell["status"] != "ok":
            lines.append(f"Статус `{cell['status']}`: общих фолдов {cell['n_common']} < {grid.MIN_COMMON_FOLDS}.")
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


def parse_horizons(text: str) -> list:
    out = []
    for part in text.split(","):
        value = int(part.strip())
        if value not in lb.HORIZONS:
            raise SystemExit(f"неверный горизонт {part!r}; допустимы {lb.HORIZONS}")
        out.append(value)
    return sorted(set(out))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--stage", choices=("select", "refit", "gate", "all"), default="refit")
    parser.add_argument("--sample", default="S_L09", choices=tuple(grid.SAMPLE_CONFIG))
    parser.add_argument("--horizons", default="1,3", help="например 1,3 (или 1,3,6)")
    parser.add_argument("--quick", action="store_true", help="последние 18 origin'ов; только для итераций")
    parser.add_argument("--select-json", default=None, type=Path)
    parser.add_argument("--out-dir", default="build/cpi-v4-h2o", type=Path)
    args = parser.parse_args(argv)
    out_dir = args.out_dir if args.out_dir.is_absolute() else ROOT / args.out_dir
    select_path = args.select_json or (out_dir / "select.json")
    if args.stage in ("select", "all"):
        run_select(args.sample, out_dir)
    if args.stage in ("refit", "all"):
        run_refit(args.sample, parse_horizons(args.horizons), args.quick, out_dir, select_path)
    if args.stage in ("gate", "all"):
        result = run_gate(out_dir)
        print(f"Gate: {result['decision']} — {result['reason']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
