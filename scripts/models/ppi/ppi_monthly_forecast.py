#!/usr/bin/env python3
"""Предрегистрированный research-эксперимент ценовых ожиданий для CPI/PPI."""

import argparse
import calendar
import csv
import hashlib
import json
import math
import sys
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lib import clickhouse as ch
from lib import eval_utils as eu
from lib import ml_experiment as mlx

ch_query = ch.ch_query

EXPERIMENT = "ppi_expectations_forecast"
RUN_NAME = "ppi_expectations_forecast__direct_h136"
ARTIFACT_STEM = "2026-10-21-ppi-expectations-forecast"
SPEC_PATH = "docs/superpowers/specs/2026-10-21-ppi-note-followups-design.md"
PLAN_PATH = "docs/superpowers/plans/2026-10-21-ppi-note-followups.md"
PRIMARY_METRIC = "paired_rmse_delta"
PREDICTION_SCHEMA = (
    "target", "horizon", "origin", "model", "y_true", "y_pred",
    "train_start", "train_end", "status", "diagnostic",
)

CPI_ALL_NAME = "Индексы потребительских цен на товары и услуги по Российской Федерации"
PPI_SERIES = ("total", "food", "nonfood", "core")
AGGREGATE_SURVEY_SERIES = (
    "price_expectations",
    "price_expectations_sa",
    "price_growth_expected",
)
PPI_SPEC_VERSION = "ppi-sa-2026-v1"

CPI_SQL = """
SELECT name, date, percent
FROM rosstat_ipc_mes FINAL
WHERE name = '%s'
ORDER BY date
FORMAT JSON
""" % CPI_ALL_NAME

PPI_SQL = """
SELECT series, date, mm_sa
FROM ppi_sa FINAL
WHERE spec_version = 'ppi-sa-2026-v1'
  AND series IN ('total', 'food', 'nonfood', 'core')
  AND mm_sa IS NOT NULL
ORDER BY series, date
FORMAT JSON
"""

SURVEY_SQL = """
SELECT series, date, value
FROM macro_series FINAL
WHERE source = 'cbr_survey'
ORDER BY series, date
FORMAT JSON
"""

PUBLICATIONS_SQL = """
SELECT series, period_date, published_at, quality, source_url
FROM v_series_publication
WHERE source = 'cbr_survey'
ORDER BY series, period_date
FORMAT JSON
"""


def month_start(value) -> date:
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, str):
        value = date.fromisoformat(value[:10])
    if not isinstance(value, date):
        raise TypeError(f"ожидалась дата, получено {type(value).__name__}")
    return value.replace(day=1)


def shift_month(month: date, months: int) -> date:
    year = month.year + (month.month - 1 + months) // 12
    month_number = (month.month - 1 + months) % 12 + 1
    day = min(month.day, calendar.monthrange(year, month_number)[1])
    return date(year, month_number, day)


def cutoff_for(origin: date) -> datetime:
    next_month = shift_month(month_start(origin), 1)
    return datetime.combine(next_month.replace(day=8), time(23, 59, 59))


def _finite(value, label: str) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{label}: значение должно быть finite")
    return result


def _published_at(row: dict) -> datetime:
    period = month_start(row["period_date"])
    quality = str(row["quality"])
    value = row["published_at"]
    if quality == "fallback":
        following = shift_month(period, 1)
        last_day = calendar.monthrange(following.year, following.month)[1]
        return datetime.combine(following.replace(day=last_day), time(23, 59, 59))
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if isinstance(value, date) and not isinstance(value, datetime):
        value = datetime.combine(value, time(23, 59, 59))
    if not isinstance(value, datetime):
        raise TypeError("published_at должен быть датой или datetime")
    if quality == "date":
        return datetime.combine(value.date(), time(23, 59, 59))
    if quality == "exact_time":
        return value
    raise ValueError(f"неизвестный quality публикации: {quality!r}")


def _publication_map(publications: list) -> dict:
    result = {}
    for row in publications:
        key = (str(row["series"]), month_start(row["period_date"]))
        if key in result:
            raise ValueError(f"дубль publication row: {key}")
        result[key] = {
            **row,
            "series": key[0],
            "period_date": key[1],
            "published_at": _published_at(row),
        }
    return result


def survey_publication_audit(origin: date, publications: list, series: tuple[str, ...]) -> list:
    cutoff = cutoff_for(origin)
    selected = _publication_map(publications)
    audit = []
    for name in series:
        rows = [row for (series_name, _), row in selected.items() if series_name == name]
        for row in sorted(rows, key=lambda item: item["period_date"], reverse=True):
            audit.append({
                "series": name,
                "period_date": row["period_date"],
                "published_at": row["published_at"],
                "quality": row["quality"],
                "available": row["published_at"] < cutoff,
                "source_url": row.get("source_url", ""),
            })
    return audit


def latest_survey_features(
    origin: date,
    values: dict,
    publications: list,
    series: tuple[str, ...],
    lags: tuple[int, ...] = (0, 1, 2, 3),
) -> dict[str, float]:
    cutoff = cutoff_for(origin)
    published = _publication_map(publications)
    normalized_values = {
        (str(name), month_start(period)): _finite(value, f"{name}/{period}")
        for (name, period), value in values.items()
    }
    features = {}
    for name in series:
        available = [
            period for (series_name, period), value in normalized_values.items()
            if series_name == name
            and (name, period) in published
            and published[(name, period)]["published_at"] < cutoff
        ]
        if not available:
            continue
        latest = max(available)
        for lag in lags:
            period = shift_month(latest, -lag)
            publication = published.get((name, period))
            value = normalized_values.get((name, period))
            features[f"{name}_lag{lag}"] = (
                value
                if value is not None and publication is not None
                and publication["published_at"] < cutoff
                else float("nan")
            )
    return features


def parse_targets(cpi_rows: list, ppi_rows: list) -> dict[str, dict[date, float]]:
    targets = {name: {} for name in ("cpi_all",) + tuple(f"ppi_{s}" for s in PPI_SERIES)}
    for row in cpi_rows:
        if row.get("name") != CPI_ALL_NAME:
            continue
        period = month_start(row["date"])
        if period in targets["cpi_all"]:
            raise ValueError(f"дубль target cpi_all/{period}")
        targets["cpi_all"][period] = _finite(row["percent"], "cpi_all") - 100.0
    for row in ppi_rows:
        series = str(row.get("series", ""))
        if series not in PPI_SERIES:
            continue
        target = f"ppi_{series}"
        period = month_start(row["date"])
        if period in targets[target]:
            raise ValueError(f"дубль target {target}/{period}")
        targets[target][period] = _finite(row["mm_sa"], target)
    missing = [name for name, points in targets.items() if not points]
    if missing:
        raise ValueError(f"обязательные ряды таргета пусты: {', '.join(missing)}")
    return targets


def parse_survey_values(rows: list) -> dict[tuple[str, date], float]:
    values = {}
    for row in rows:
        key = (str(row["series"]), month_start(row["date"]))
        if key in values:
            raise ValueError(f"дубль survey point: {key}")
        values[key] = _finite(row["value"], f"survey {key}")
    return values


def parse_publications(rows: list) -> list:
    return list(_publication_map(rows).values())


def load_inputs() -> tuple[dict, dict, list]:
    targets = parse_targets(ch_query(CPI_SQL), ch_query(PPI_SQL))
    values = parse_survey_values(ch_query(SURVEY_SQL))
    publications = parse_publications(ch_query(PUBLICATIONS_SQL))
    return targets, values, publications


@dataclass(frozen=True)
class Row:
    target: str
    origin: date
    actual: float
    base_features: dict
    expectation_features: dict


@dataclass(frozen=True)
class Fold:
    train: tuple[Row, ...]
    test: Row
    horizon: int

    @property
    def target_date(self) -> date:
        return shift_month(self.test.origin, self.horizon)


def direct_folds(rows: list[Row], horizon: int, min_train: int = 36) -> list[Fold]:
    if horizon < 1:
        raise ValueError("horizon должен быть положительным")
    if min_train < 1:
        raise ValueError("min_train должен быть положительным")
    ordered = sorted(rows, key=lambda row: row.origin)
    origins = [row.origin for row in ordered]
    if len(set(origins)) != len(origins):
        raise ValueError("дубли origins в monthly sample")
    for previous, current in zip(origins, origins[1:]):
        if shift_month(previous, 1) != current:
            raise ValueError(f"разрыв monthly sample: {previous} → {current}")
    return [
        Fold(
            train=tuple(ordered[:index - horizon]),
            test=ordered[index],
            horizon=horizon,
        )
        for index in range(min_train + horizon, len(ordered))
    ]


@dataclass(frozen=True)
class Prediction:
    target: str
    horizon: int
    origin: date
    model: str
    y_true: float
    y_pred: float
    train_start: date
    train_end: date
    status: str = "ok"
    diagnostic: str = ""


def feature_values(row: Row, horizon: int, expectations: bool = False) -> dict:
    features = {str(key): float(value) for key, value in row.base_features.items()}
    target_month = shift_month(row.origin, horizon).month
    for month_number in range(2, 13):
        features[f"month_{month_number:02d}"] = 1.0 if target_month == month_number else 0.0
    if expectations:
        features.update(
            {str(key): float(value) for key, value in row.expectation_features.items()}
        )
    return features


def _feature_names(rows: tuple, horizon: int, expectations: bool) -> list[str]:
    names = []
    seen = set()
    for row in rows:
        for name in feature_values(row, horizon, expectations):
            if name not in seen:
                seen.add(name)
                names.append(name)
    return names


def _matrix(rows: tuple, horizon: int, expectations: bool, names: list[str]):
    import numpy as np

    return np.asarray(
        [[feature_values(row, horizon, expectations).get(name, float("nan")) for name in names]
         for row in rows],
        dtype=float,
    )


def _fit_b3(train: tuple, horizon: int, expectations: bool):
    from sklearn.impute import SimpleImputer
    from sklearn.linear_model import ElasticNetCV
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    names = _feature_names(train, horizon, expectations)
    matrix = _matrix(train, horizon, expectations, names)
    if matrix.shape[0] < 2:
        raise ValueError("вырожденный фолд: меньше двух train origins")
    target = [row.actual for row in train]
    steps = [
        SimpleImputer(strategy="median", keep_empty_features=True),
        StandardScaler(),
        ElasticNetCV(
            cv=min(5, len(train)),
            random_state=20261027,
            max_iter=5000,
            tol=1e-3,
        ),
    ]
    model = make_pipeline(*steps)
    model.fit(matrix, target)
    model.feature_names_ = names
    return model[0], model[1], model


def _predict_b3(fitted, test: Row, horizon: int, expectations: bool) -> float:
    model = fitted[2]
    names = list(model.feature_names_)
    row = (test,)
    matrix = _matrix(row, horizon, expectations, names)
    return float(model.predict(matrix)[0])


def _fit_b2(train: tuple, horizon: int, expectations: bool):
    import numpy as np
    from sklearn.impute import SimpleImputer

    names = _feature_names(train, horizon, expectations)
    if "y_lag1" not in names:
        raise ValueError("вырожденный фолд: y_lag1 отсутствует")
    matrix = _matrix(train, horizon, expectations, names)
    lag_index = names.index("y_lag1")
    if not np.isfinite(matrix[:, lag_index]).any():
        raise ValueError("вырожденный фолд: y_lag1 полностью отсутствует в train")
    imputer = SimpleImputer(strategy="median", keep_empty_features=True)
    prepared = imputer.fit_transform(matrix)
    design = np.column_stack([np.ones(len(train)), prepared])
    target = np.asarray([row.actual for row in train], dtype=float)
    coefficients, _, _, _ = np.linalg.lstsq(design, target, rcond=None)
    return imputer, coefficients, names


def _predict_b2(fitted, test: Row, horizon: int, expectations: bool) -> float:
    import numpy as np

    imputer, coefficients, names = fitted
    matrix = _matrix((test,), horizon, expectations, names)
    prepared = imputer.transform(matrix)
    design = np.column_stack([np.ones(1), prepared])
    return float((design @ coefficients)[0])


def _make_prediction(fold: Fold, model_name: str, value: float, status="ok", diagnostic="") -> Prediction:
    return Prediction(
        target=fold.test.target,
        horizon=fold.horizon,
        origin=fold.test.origin,
        model=model_name,
        y_true=fold.test.actual,
        y_pred=value,
        train_start=fold.train[0].origin,
        train_end=fold.train[-1].origin,
        status=status,
        diagnostic=diagnostic,
    )


def walk_forward(rows: list[Row], horizons: tuple[int, ...] = (1, 3, 6)) -> list[Prediction]:
    import warnings

    import numpy as np

    predictions = []
    for horizon in horizons:
        for fold in direct_folds(rows, horizon):
            predictions.append(_make_prediction(fold, "b1_last", float(fold.train[-1].actual)))
            estimators = (
                ("b2_ar1", _fit_b2, _predict_b2, False),
                ("b2_ar1_expectations", _fit_b2, _predict_b2, True),
                ("b3_enet", _fit_b3, _predict_b3, False),
                ("b3_enet_expectations", _fit_b3, _predict_b3, True),
            )
            for name, fit, predict, expectations in estimators:
                try:
                    with warnings.catch_warnings(record=True) as caught:
                        warnings.simplefilter("always")
                        fitted = fit(fold.train, horizon, expectations)
                        value = predict(fitted, fold.test, horizon, expectations)
                    unique = []
                    for warning in caught:
                        text = f"{warning.category.__name__}: {warning.message}"
                        if text not in unique:
                            unique.append(text)
                    status = "warning" if unique else "ok"
                    predictions.append(
                        _make_prediction(fold, name, value, status, "; ".join(unique[:3]))
                    )
                except (ValueError, FloatingPointError, np.linalg.LinAlgError) as exc:
                    predictions.append(
                        _make_prediction(fold, name, float("nan"), "skipped", str(exc))
                    )
    return predictions


PRIMARY_PAIRS = (
    ("b2_ar1", "b2_ar1_expectations"),
    ("b3_enet", "b3_enet_expectations"),
)
SECONDARY_PAIRS = (
    ("b3_enet", "b3_enet_industry"),
    ("b3_enet", "b3_enet_aggregate_industry"),
)


def mde_ratio(err_a: list, err_b: list):
    import numpy as np

    if len(err_a) != len(err_b):
        raise ValueError(f"MDE: {len(err_a)} ошибок против {len(err_b)}")
    if len(err_a) < 2:
        return None
    differences = np.asarray(
        [a * a - b * b for a, b in zip(err_a, err_b)], dtype=float
    )
    denominator = eu.rmse(err_a) + eu.rmse(err_b)
    if denominator == 0.0:
        return None
    return float(2.802 * float(np.std(differences, ddof=1)) / math.sqrt(len(differences)) / denominator)


def holm_adjust(pvalues: list) -> list:
    adjusted = [float("nan")] * len(pvalues)
    finite = sorted(
        (value, index) for index, value in enumerate(pvalues) if math.isfinite(value)
    )
    running = 0.0
    total = len(finite)
    for rank, (value, index) in enumerate(finite):
        running = max(running, min(1.0, (total - rank) * value))
        adjusted[index] = running
    return adjusted


def _model_metrics(predictions: list) -> list:
    groups = {}
    for row in predictions:
        if not math.isfinite(row.y_pred) or not math.isfinite(row.y_true):
            continue
        groups.setdefault((row.target, row.horizon, row.model), []).append(row)
    result = []
    for (target, horizon, model), rows in sorted(groups.items()):
        errors = [row.y_pred - row.y_true for row in rows]
        result.append({
            "target": target,
            "horizon": horizon,
            "model": model,
            "n": len(errors),
            "rmse": eu.rmse(errors),
            "mae": eu.mae(errors),
        })
    return result


def evaluate(
    predictions: list[Prediction],
    block: int = 3,
    n: int = 2000,
    seed: int = 20261027,
    primary: bool = True,
) -> dict:
    grouped = {}
    for row in predictions:
        key = (row.target, row.horizon, row.model, row.origin)
        if key in grouped:
            raise ValueError(f"дубль prediction: {key}")
        grouped[key] = row
    pairs = []
    definitions = [
        (base, expected, True) for base, expected in PRIMARY_PAIRS
    ] + [(base, expected, False) for base, expected in SECONDARY_PAIRS]
    for base_model, expectations_model, primary_family in definitions:
        pair_groups = {}
        for (target, horizon, model, origin), row in grouped.items():
            if model in (base_model, expectations_model):
                pair_groups.setdefault((target, horizon), {}).setdefault(model, {})[origin] = row
        for (target, horizon), models in sorted(pair_groups.items()):
            base_rows = models.get(base_model, {})
            expectation_rows = models.get(expectations_model, {})
            common = sorted(
                origin for origin in base_rows.keys() & expectation_rows.keys()
                if math.isfinite(base_rows[origin].y_pred)
                and math.isfinite(expectation_rows[origin].y_pred)
            )
            if not common:
                continue
            base_errors = [
                base_rows[origin].y_pred - base_rows[origin].y_true for origin in common
            ]
            expectation_errors = [
                expectation_rows[origin].y_pred - expectation_rows[origin].y_true
                for origin in common
            ]
            if len(common) >= 2:
                dm_stat, dm_pvalue = eu.diebold_mariano(base_errors, expectation_errors, horizon)
                bootstrap = eu.block_bootstrap_rmse_diff(
                    base_errors, expectation_errors, block, n, seed
                )
                ci_low, ci_high = bootstrap.low, bootstrap.high
            else:
                dm_stat, dm_pvalue = float("nan"), float("nan")
                ci_low = ci_high = float("nan")
            base_rmse = eu.rmse(base_errors)
            expectation_rmse = eu.rmse(expectation_errors)
            pairs.append({
                "target": target,
                "horizon": horizon,
                "base_model": base_model,
                "expectations_model": expectations_model,
                "primary": bool(primary and primary_family),
                "n": len(common),
                "origins": common,
                "base_rmse": base_rmse,
                "expectations_rmse": expectation_rmse,
                "base_mae": eu.mae(base_errors),
                "expectations_mae": eu.mae(expectation_errors),
                "delta": base_rmse - expectation_rmse,
                "dm_stat": dm_stat,
                "dm_pvalue": dm_pvalue,
                "ci_low": ci_low,
                "ci_high": ci_high,
                "mde": mde_ratio(base_errors, expectation_errors),
                "holm_pvalue": None,
            })
    family = [row for row in pairs if row["primary"]]
    adjusted = holm_adjust([row["dm_pvalue"] for row in family])
    for row, value in zip(family, adjusted):
        row["holm_pvalue"] = value
    return {
        "models": _model_metrics(predictions),
        "pairs": pairs,
        "primary_family": family,
        "verdict": research_verdict(pairs),
    }


def research_verdict(pairs: list, min_primary: int = 12) -> str:
    primary = [row for row in pairs if row.get("primary")]
    if not primary or any(row.get("n", 0) < min_primary for row in primary):
        return "INSUFFICIENT"
    for row in primary:
        if (
            row.get("target") == "ppi_food"
            and row.get("horizon") in (1, 3)
            and row.get("delta", 0.0) > 0.0
            and math.isfinite(row.get("holm_pvalue") or float("nan"))
            and row["holm_pvalue"] < 0.05
            and row.get("ci_low", 0.0) > 0.0
        ):
            return "promote"
    return "iterate" if any(row.get("delta", 0.0) > 0.0 for row in primary) else "reject"


class DataError(Exception):
    """Недостаточное или несогласованное покрытие research-данных."""


def _date_range(points) -> dict:
    if not points:
        return {"first": None, "last": None, "n": 0}
    ordered = sorted(points)
    return {"first": ordered[0], "last": ordered[-1], "n": len(ordered)}


def secondary_consumer_status(publications: list) -> dict:
    industry = [
        row for row in publications
        if str(row.get("series", "")).startswith("industry_")
        and "price_expectations" in str(row.get("series", ""))
    ]
    if not industry:
        return {
            "status": "FAIL_CLOSED",
            "reason": "industry publication calendar is missing; secondary consumer is not evaluated",
            "publication_rows": 0,
        }
    return {
        "status": "CALENDAR_PRESENT",
        "reason": "industry calendar present; secondary consumer target still requires pinned reconstruction",
        "publication_rows": len(industry),
    }


def build_data_passport(targets: dict, values: dict, publications: list) -> dict:
    quality = {}
    for row in publications:
        name = str(row.get("quality", "missing"))
        quality[name] = quality.get(name, 0) + 1
    sources = {
        "cpi_all": "rosstat_ipc_mes.percent - 100 (NSA m/m, pp)",
        "ppi_total": "ppi_sa.mm_sa (ppi-sa-2026-v1, pp)",
        "ppi_food": "ppi_sa.mm_sa (ppi-sa-2026-v1, pp)",
        "ppi_nonfood": "ppi_sa.mm_sa (ppi-sa-2026-v1, pp)",
        "ppi_core": "ppi_sa.mm_sa (ppi-sa-2026-v1, pp)",
    }
    survey_ranges = {}
    for series in sorted({name for name, _ in values}):
        survey_ranges[series] = _date_range(
            period for name, period in values if name == series
        )
    return {
        "targets": {
            name: {"source": sources.get(name, name), **_date_range(points)}
            for name, points in sorted(targets.items())
        },
        "survey_values": {
            "series": survey_ranges,
            "points": len(values),
        },
        "publication_quality": quality,
        "publication_periods": _date_range(
            row["period_date"] for row in publications
        ),
        "secondary_consumer": secondary_consumer_status(publications),
    }


def build_horizon_rows(
    target: str,
    points: dict,
    values: dict,
    publications: list,
    horizon: int,
) -> list[Row]:
    if not points:
        raise DataError(f"{target}: обязательный ряд таргета пуст")
    ordered = sorted(points)
    rows = []
    for origin in ordered:
        prior = shift_month(origin, -1)
        target_date = shift_month(origin, horizon)
        if prior not in points or target_date not in points:
            continue
        expectations = latest_survey_features(
            origin, values, publications, AGGREGATE_SURVEY_SERIES
        )
        if not expectations:
            continue
        rows.append(
            Row(
                target=target,
                origin=origin,
                actual=points[target_date],
                base_features={"y_lag1": points[prior]},
                expectation_features=expectations,
            )
        )
    if len(rows) <= 36 + horizon:
        raise DataError(
            f"{target}/h{horizon}: доступно {len(rows)} origins — недостаточно для walk-forward"
        )
    return rows


def _prediction_record(row: Prediction) -> dict:
    record = {
        field: getattr(row, field) for field in PREDICTION_SCHEMA
    }
    for name in ("origin", "train_start", "train_end"):
        record[name] = record[name].isoformat()
    return record


def _csv_float(value) -> float:
    return float("nan") if value in (None, "") else float(value)


def _read_prediction_record(record: dict) -> Prediction:
    return Prediction(
        target=record["target"],
        horizon=int(record["horizon"]),
        origin=date.fromisoformat(record["origin"]),
        model=record["model"],
        y_true=_csv_float(record["y_true"]),
        y_pred=_csv_float(record["y_pred"]),
        train_start=date.fromisoformat(record["train_start"]),
        train_end=date.fromisoformat(record["train_end"]),
        status=record.get("status", "ok"),
        diagnostic=record.get("diagnostic", ""),
    )


def write_predictions_csv(path: Path, predictions: list[Prediction]) -> None:
    with Path(path).open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=PREDICTION_SCHEMA)
        writer.writeheader()
        for row in predictions:
            writer.writerow(mlx.json_safe(_prediction_record(row)))


def read_predictions_csv(path: Path) -> list[Prediction]:
    with Path(path).open(newline="", encoding="utf-8") as handle:
        records = list(csv.DictReader(handle))
    if not records:
        raise DataError(f"{path}: CSV predictions пуст")
    return [_read_prediction_record(record) for record in records]


def _fmt(value) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return "n/a" if not math.isfinite(value) else f"{value:.6g}"
    return str(value)


def _table(rows: list[dict], columns: tuple[str, ...]) -> list[str]:
    if not rows:
        return ["_Нет строк._"]
    lines = [
        "| " + " | ".join(columns) + " |",
        "|" + "|".join("---" for _ in columns) + "|",
    ]
    for row in rows:
        lines.append("| " + " | ".join(_fmt(row.get(column)) for column in columns) + " |")
    return lines


def render_report(data: dict) -> str:
    evaluation = data["evaluation"]
    primary = evaluation["primary_family"]
    secondary = [row for row in evaluation["pairs"] if not row["primary"]]
    robustness = data["robustness"]
    lines = [
        "# Ценовые ожидания как признаки прогноза ИПЦ/ИЦП",
        "",
        "Research-эксперимент Task 2 issue #318. Протокол предрегистрирован в "
        f"[{SPEC_PATH}](../../{SPEC_PATH}) и [{PLAN_PATH}](../../{PLAN_PATH}); "
        "production-модели, `forecast_log`, DDL/витрины и `ppi_sa` не меняются.",
        "",
        "## Паспорт прогона",
        "",
        f"- run id: `{data['run_id']}`",
        f"- experiment / run: `{data['experiment']}` / `{data['run_name']}`",
        f"- branch / SHA / dirty: `{data['git'].get('git_branch')}` / "
        f"`{data['git'].get('git_sha')}` / `{data['git'].get('git_dirty')}`",
        f"- config hash: `{data['config_hash']}`; sample hash: `{data['sample_hash']}`",
        f"- data as-of: `{_fmt(data['data_asof'])}`",
        "",
        "## Данные",
        "",
    ]
    target_rows = [
        {"target": name, **passport}
        for name, passport in data["data_passport"]["targets"].items()
    ]
    lines += _table(target_rows, ("target", "source", "first", "last", "n"))
    lines += ["", f"Survey points: {data['data_passport']['survey_values']['points']}; "
              f"publication quality: {data['data_passport']['publication_quality']}.", ""]
    lines += ["## Покрытие фолдов", ""]
    lines += _table(data["fold_counts"], ("target", "horizon", "folds", "skipped", "train_end"))
    lines += ["", "## Модели: RMSE / MAE", ""]
    lines += _table(
        evaluation["models"],
        ("target", "horizon", "model", "n", "rmse", "mae"),
    )
    lines += ["", "## Primary-контрасты", ""]
    lines += _table(
        primary,
        ("target", "horizon", "base_model", "expectations_model", "n", "delta",
         "dm_pvalue", "holm_pvalue", "ci_low", "ci_high", "mde"),
    )
    lines += ["", "## Secondary consumer", ""]
    lines += _table(
        secondary,
        ("target", "horizon", "base_model", "expectations_model", "n", "delta",
         "dm_pvalue", "ci_low", "ci_high", "mde"),
    )
    lines += ["", "## Robustness 2022–2023", ""]
    lines += _table(
        robustness.get("pairs", []),
        ("target", "horizon", "base_model", "expectations_model", "n", "delta",
         "dm_pvalue", "ci_low", "ci_high", "mde"),
    )
    lines += [
        "",
        f"Robustness status: `{robustness.get('label')}`; минимум 6 folds на пару.",
        "",
        "## Ограничения",
        "",
        "- Survey as-of соблюдён строго: `published_at < cutoff`, boundary 8-е M+1 23:59:59 MSK.",
        "- CPI/PPI targets — **latest-vintage**: полный real-time/PIT по ревизиям таргета не проверен.",
        "- `quality='fallback'` сохраняется в аудите и не выдаётся за точную дату публикации.",
        "- Secondary consumer: `"
        + data["data_passport"]["secondary_consumer"]["status"]
        + "` — "
        + data["data_passport"]["secondary_consumer"]["reason"]
        + ".",
        "- Robustness 2022–2023 описательная и не меняет primary-вывод.",
        "",
        "## Вердикт",
        "",
        f"`{data['verdict']}`. Это research-рекомендация по предрегистрированному правилу, "
        "не production promotion; registry/alias и `forecast_log` не затрагивались.",
        "",
    ]
    return "\n".join(lines)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fold_counts(predictions: list[Prediction]) -> list[dict]:
    result = []
    keys = sorted({(row.target, row.horizon) for row in predictions})
    for target, horizon in keys:
        rows = [row for row in predictions if row.target == target and row.horizon == horizon]
        origins = sorted({row.origin for row in rows})
        skipped = sum(1 for row in rows if row.status == "skipped")
        result.append({
            "target": target,
            "horizon": horizon,
            "folds": len(origins),
            "skipped": skipped,
            "train_end": max((row.train_end for row in rows), default=None),
        })
    return result


def _sample_payload(targets: dict, values: dict, publications: list) -> dict:
    return {
        "targets": targets,
        "survey_values": values,
        "publications": publications,
    }


def run_research(
    targets: dict,
    values: dict,
    publications: list,
    out_dir: Path = Path("build/ppi-expectations-forecast"),
    horizons: tuple[int, ...] = (1, 3, 6),
    min_train: int = 36,
    allow_data_revision: bool = False,
    logger_enabled=None,
) -> dict:
    required = ("cpi_all",) + tuple(f"ppi_{series}" for series in PPI_SERIES)
    missing = [name for name in required if not targets.get(name)]
    if missing:
        raise DataError(f"обязательные ряды таргета пусты: {', '.join(missing)}")
    rows_by_target = {}
    for target in required:
        for horizon in horizons:
            rows_by_target[(target, horizon)] = build_horizon_rows(
                target, targets[target], values, publications, horizon
            )
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    config = {
        "experiment": EXPERIMENT,
        "targets": list(required),
        "horizons": list(horizons),
        "min_train": min_train,
        "primary_pairs": [list(pair) for pair in PRIMARY_PAIRS],
        "secondary_pairs": [list(pair) for pair in SECONDARY_PAIRS],
        "survey_series": list(AGGREGATE_SURVEY_SERIES),
        "survey_lags": [0, 1, 2, 3],
        "cutoff_day": 8,
        "cutoff_time": "23:59:59 MSK",
        "bootstrap_block": 3,
        "bootstrap_runs": 2000,
        "seed": 20261027,
        "allow_data_revision": bool(allow_data_revision),
        "spec_path": SPEC_PATH,
        "plan_path": PLAN_PATH,
    }
    config_hash = mlx.sha256_text(mlx.canonical_json(config))
    sample = _sample_payload(targets, values, publications)
    sample_meta = {
        "targets": {name: _date_range(points) for name, points in sorted(targets.items())},
        "survey_points": len(values),
        "publications": len(publications),
        "folds": {
            f"{target}__h{horizon}": len(rows)
            for (target, horizon), rows in sorted(rows_by_target.items())
        },
    }
    sample_hash = mlx.sha256_text(mlx.canonical_json(sample))
    data_asof = max(
        point for name in required for point in targets[name]
    )
    passport = build_data_passport(targets, values, publications)
    logger = mlx.start_run(
        experiment=EXPERIMENT,
        run_name=RUN_NAME,
        config=config,
        data_asof=data_asof,
        sample_meta=sample_meta,
        sample=sample,
        spec_path=SPEC_PATH,
        enabled=logger_enabled,
    )
    predictions = []
    for (target, horizon), rows in rows_by_target.items():
        predictions.extend(walk_forward(rows, horizons=(horizon,)))
    if not any(row.status != "skipped" for row in predictions):
        raise DataError("все fold-предсказания пропущены — нет пригодной выборки")
    evaluation = evaluate(predictions)
    robustness_predictions = [
        row for row in predictions if 2022 <= row.origin.year <= 2023
    ]
    if robustness_predictions:
        robustness = evaluate(robustness_predictions, primary=False)
        robustness["label"] = (
            "INSUFFICIENT"
            if any(pair["n"] < 6 for pair in robustness["pairs"])
            else "reported (descriptive only)"
        )
    else:
        robustness = {"pairs": [], "label": "INSUFFICIENT", "models": []}
    fold_counts = _fold_counts(predictions)
    predictions_path = out_dir / f"{ARTIFACT_STEM}-predictions.csv"
    report_path = out_dir / f"{ARTIFACT_STEM}.md"
    json_path = out_dir / f"{ARTIFACT_STEM}.json"
    write_predictions_csv(predictions_path, predictions)
    data = {
        "schema_version": 1,
        "run_id": logger.run_id,
        "experiment": EXPERIMENT,
        "run_name": logger.run_name,
        "generated_at": datetime.now(),
        "git": logger.git,
        "spec_path": SPEC_PATH,
        "plan_path": PLAN_PATH,
        "config": config,
        "config_hash": config_hash,
        "data_asof": data_asof,
        "sample_meta": sample_meta,
        "sample_hash": sample_hash,
        "data_passport": passport,
        "fold_counts": fold_counts,
        "predictions_schema": list(PREDICTION_SCHEMA),
        "predictions": [_prediction_record(row) for row in predictions],
        "evaluation": evaluation,
        "robustness": robustness,
        "limitations": [
            "CPI/PPI targets are latest-vintage; full real-time/PIT revision history is not tested",
            "survey as-of is strict published_at < cutoff",
            "secondary consumer: "
            + passport["secondary_consumer"]["status"]
            + " — "
            + passport["secondary_consumer"]["reason"],
        ],
        "verdict": evaluation["verdict"],
        "primary_metric": PRIMARY_METRIC,
        "artifacts": [],
    }
    report_path.write_text(render_report(data), encoding="utf-8")
    data["artifacts"] = [
        {
            "name": predictions_path.name,
            "sha256": _sha256_file(predictions_path),
            "bytes": predictions_path.stat().st_size,
        },
        {
            "name": report_path.name,
            "sha256": _sha256_file(report_path),
            "bytes": report_path.stat().st_size,
        },
    ]
    json_path.write_text(
        json.dumps(mlx.json_safe(data), ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False)
        + "\n",
        encoding="utf-8",
    )
    logger.log_metrics({
        "primary_metric": PRIMARY_METRIC,
        "decision": evaluation["verdict"],
        "folds": sum(item["folds"] for item in fold_counts),
        "skipped_predictions": sum(item["skipped"] for item in fold_counts),
    })
    logger.log_predictions([
        mlx.PredictionRow(
            model=f"{row.target}::{row.model}",
            window=f"h{row.horizon}",
            target_date=shift_month(row.origin, row.horizon),
            predicted=row.y_pred,
            actual=row.y_true,
            unit="pp",
            meta={
                "target": row.target,
                "origin": row.origin.isoformat(),
                "train_start": row.train_start.isoformat(),
                "train_end": row.train_end.isoformat(),
                "status": row.status,
            },
        )
        for row in predictions
        if row.status != "skipped"
        and math.isfinite(row.y_pred)
        and math.isfinite(row.y_true)
    ])
    logger.log_artifacts([predictions_path, report_path, json_path])
    log_result = logger.finish("ok", notes=f"verdict={evaluation['verdict']}")
    return {
        "run_id": logger.run_id,
        "log_result": log_result,
        "artifact_json": json_path,
        "predictions_path": predictions_path,
        "report_path": report_path,
        "predictions": predictions,
        "evaluation": evaluation,
        "data": data,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Research-прогноз CPI/PPI по ценовым ожиданиям (Task 2 #318)."
    )
    parser.add_argument("--out-dir", default="build/ppi-expectations-forecast")
    parser.add_argument("--horizons", default="1,3,6")
    parser.add_argument("--min-train", type=int, default=36)
    parser.add_argument("--allow-data-revision", action="store_true")
    args = parser.parse_args(argv)
    try:
        horizons = tuple(int(value) for value in args.horizons.split(",") if value.strip())
    except ValueError:
        print(f"ошибка --horizons: {args.horizons!r}", file=sys.stderr)
        return 2
    if not horizons or any(horizon < 1 for horizon in horizons):
        print("ошибка --horizons: нужны положительные горизонты", file=sys.stderr)
        return 2
    try:
        targets, values, publications = load_inputs()
        result = run_research(
            targets,
            values,
            publications,
            out_dir=Path(args.out_dir),
            horizons=horizons,
            min_train=args.min_train,
            allow_data_revision=args.allow_data_revision,
        )
    except (DataError, ch.ClickHouseError) as error:
        print(f"ошибка данных: {error}", file=sys.stderr)
        return 1
    folds = len(result["predictions"])
    print(
        f"run_id={result['run_id']} horizons={','.join(map(str, horizons))} "
        f"predictions={folds} verdict={result['evaluation']['verdict']} "
        f"artifacts={result['artifact_json'].parent}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
