#!/usr/bin/env python3
"""Предрегистрированный research-эксперимент ценовых ожиданий для CPI/PPI."""

import calendar
import math
import sys
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lib import clickhouse as ch

ch_query = ch.ch_query

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
