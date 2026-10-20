#!/usr/bin/env python3
"""Live-адаптация production-пайплайна CB_COMP (схема авторов, 12 стадий).

Схема авторов (КСП ИПЦ по компонентам) в live-контуре воспроизводится в той
гранулярности, которую дают публичные витрины:

1. словари/веса — архив CB_COMP, лист 2024 (live-весов 2025/2026 нет);
2. сбор данных — `v_rosstat_macro` (4 официальных компонента ИПЦ + 148
   недельных товаров), `v_cbr_survey`/`v_cbr_mp_public_panel`, FAO, USD,
   ключевая ставка;
3. базисные индексы/SA/производные — месячный m/m товаров, SA FAO,
   live-таргет; 88-группового SA-ряда NMFI в live-витринах нет;
4. CatBoost — 3 компонента (food/nonfood/services) + агрегат, а не 88 групп;
5. прогноз/выгрузка — текущий nowcast и walk-forward;
6. ЖКУ — экспертный файл авторов (последний прогноз 2025-05, stale) или
   `--utilities-mom`;
7. история таргета — официальный ИПЦ до последнего опубликованного месяца;
8. агрегат и ошибка — взвешенный агрегат + ошибки по компонентам;
9. исторические компоненты — официальные 4 компонента + недельная панель,
   не NMFI;
10. дашборд — long/wide CSV + XLSX;
11. экспертные корректировки — CSV `month,component,delta_pp`;
12. финальный прогноз — Россия; ДФО не воспроизводится (нет live CPI/весов
    по ДФО, в архиве есть только прогноз ЖКУ по ДФО).

Скрипт только читает ClickHouse и локальный архив; в БД не пишет. Запуск —
`make cb-comp-live` (env подключает Makefile), тесты — офлайн.
"""

import argparse
import hashlib
import json
import math
import os
import subprocess
import sys
import uuid
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import cpi_monthly as cm
import cpi_monthly_v3 as v3
import cpi_public_panel_audit as audit
import eval_utils as eu

COMPONENTS = cm.BOTTOMUP_COMPONENTS
COMPONENT_LABELS = {
    "cpi_food": "food",
    "cpi_nonfood": "nonfood",
    "cpi_services": "services",
    "cpi_all": "all",
}
DEFAULT_HISTORY_MONTHS = 24
DEFAULT_SAMPLE_START = date(2022, 1, 1)
UTILITIES_RELATIVE = "Models/CpiComponents/data/Коммунальные услуги прогноз MoM_sa.xlsx"
WEIGHTS_YEAR = 2024

PIPELINE_STAGES = (
    ("1. Актуализация словарей и весов ИПЦ", "архив CB_COMP, 2024", "partial: live-весов 2025/2026 нет"),
    ("2. Сбор входящего набора данных", "v_rosstat_macro, v_cbr_survey, v_fao_food_price", "yes: 4 компонента + недельные товары + МП + макро"),
    ("3. Базисные индексы, SA, производные", "weekly panel + FAO SA + live target", "partial: 88-группового SA/NMFI нет"),
    ("4. CatBoost по компонентам", "3 компонента + агрегат", "partial: 88 групп → 3"),
    ("5. Прогноз и выгрузка", "walk-forward + current nowcast", "yes"),
    ("6. Экспертный прогноз ЖКУ", "архив, последний прогноз 2025-05", "stale; есть --utilities-mom"),
    ("7. Обновление истории таргета", "официальный ИПЦ до 2026-09", "yes"),
    ("8. Агрегат ИПЦ и ошибка", "NNLS-веса + ошибки компонент", "yes"),
    ("9. История компонент из NMFI", "4 официальных компонента + weekly", "partial: NMFI live нет"),
    ("10. Дашборд «КСП ИПЦ по компонентам»", "CSV long/wide + XLSX", "yes"),
    ("11. Экспертные корректировки", "--adjustments CSV", "yes"),
    ("12. Финальный прогноз Россия + ДФО", "Россия", "partial: ДФО не воспроизводится"),
)


# ---------------------------------------------------------------------------
# Календарь, архив, веса
# ---------------------------------------------------------------------------


def month_start(value) -> date:
    if isinstance(value, datetime):
        return date(value.year, value.month, 1)
    if isinstance(value, date):
        return date(value.year, value.month, 1)
    text = str(value).strip()
    if len(text) == 7:
        text += "-01"
    return date.fromisoformat(text[:10]).replace(day=1)


def parse_month(value: str) -> date:
    return month_start(value)


def resolve_archive(value: str = None) -> Path:
    """Архив CB_COMP: явный путь, env или worktree-сосед."""
    candidates = []
    if value:
        candidates.append(Path(value))
    env = os.environ.get("CB_COMP_ARCHIVE")
    if env:
        candidates.append(Path(env))
    candidates.append(
        ROOT.parent
        / "cb-comp-archive"
        / "build/cb-comp/extracted/BusinessSurveyCBRCpiForecastingSourceCode"
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise SystemExit(
        "не найден архив CB_COMP: передать --archive или CB_COMP_ARCHIVE"
    )


def load_reference_weights(archive: Path) -> dict:
    """Суммы листовых весов 2024 по компонентам food/nonfood/services."""
    if archive is None:
        return {}
    weights = audit.load_weights(archive, WEIGHTS_YEAR)
    dictionary = audit.load_dictionary(archive, WEIGHTS_YEAR)
    totals = {"food": 0.0, "nonfood": 0.0, "services": 0.0}
    for item in weights:
        meta = dictionary.get(item["code"])
        component = meta.get("component_key", "other") if meta else "other"
        if component in totals:
            totals[component] += float(item["weight"])
    return totals


def load_utilities(archive: Path, override: float = None) -> dict:
    """Последний экспертный прогноз ЖКУ из файла авторов + статус свежести."""
    if archive is None:
        return {"status": "no_archive"}
    path = archive / UTILITIES_RELATIVE
    if not path.exists():
        return {"status": "missing", "path": str(path)}
    frame = pd.read_excel(path, sheet_name="Прогноз")
    frame["дата"] = pd.to_datetime(frame["дата"])
    frame = frame.sort_values("дата")
    last = frame.iloc[-1]
    forecasts = frame[
        frame["факт_прогноз"].astype(str).str.strip() == "прогноз"
    ]
    last_forecast = forecasts.iloc[-1] if len(forecasts) else last
    forecast_date = month_start(last_forecast["дата"])
    value = float(last_forecast["Россия"])
    source = "archive_forecast"
    if override is not None:
        value = float(override)
        source = "override"
    return {
        "status": "ok",
        "path": str(path),
        "last_row_date": month_start(last["дата"]).isoformat(),
        "last_forecast_date": forecast_date.isoformat(),
        "last_forecast_russia": value,
        "last_forecast_far_eastern": float(last_forecast["Дальневосточное ГУ"]),
        "value_used": value,
        "source": source,
        "stale_months": max(
            0,
            (date.today().year * 12 + date.today().month)
            - (forecast_date.year * 12 + forecast_date.month),
        ),
    }


# ---------------------------------------------------------------------------
# Признаки и компонентные прогнозы
# ---------------------------------------------------------------------------


def load_live_inputs() -> dict:
    """Читает ровно те live-ряды, которые нужны пайплайну."""
    weekly = v3.weekly_panel(v3.load_weekly())
    return {
        "targets": cm.load_targets(),
        "survey": cm.load_survey(),
        "usd": cm.load_usd(),
        "key_rate": cm.load_key_rate(),
        "fao": cm.load_fao(),
        "panel": weekly,
    }


def build_historical_rows(inputs: dict, first: date = DEFAULT_SAMPLE_START) -> tuple:
    """Месячные строки v3-признаков от `first` до последнего опубликованного ИПЦ."""
    targets = inputs["targets"]
    index = cm.target_index(targets)
    rows = []
    skipped = []
    for month in cm.sample_months(targets):
        if month < first:
            continue
        cutoff = cm.cutoff_for(month)
        base, reason = cm.base_features(
            month, index, inputs["usd"], inputs["key_rate"], inputs["fao"], cutoff
        )
        if base is None:
            skipped.append((month, reason))
            continue
        mp_part, reason = cm.mp_features(month, inputs["survey"], cutoff)
        if mp_part is None:
            skipped.append((month, reason))
            continue
        weekly_part = v3.weekly_features(inputs["panel"], cutoff)
        y = {
            code: values[cm.month_end(month)]
            for code, values in index.items()
            if cm.month_end(month) in values
        }
        if len(y) != len(cm.TARGET_SERIES):
            skipped.append((month, "неполная метка компонент"))
            continue
        rows.append(
            cm.Row(
                month=month,
                y=y,
                features={**base, **mp_part, **weekly_part},
            )
        )
    return rows, skipped


def build_current_row(inputs: dict, target_month: date, as_of: date) -> tuple:
    """Строка признаков целевого месяца as-of даты прогона (live-режим)."""
    index = cm.target_index(inputs["targets"])
    base, reason = cm.base_features(
        target_month,
        index,
        inputs["usd"],
        inputs["key_rate"],
        inputs["fao"],
        as_of,
        live=True,
    )
    if base is None:
        return None, reason
    mp_part, reason = cm.mp_features(
        target_month, inputs["survey"], as_of, live=True
    )
    if mp_part is None:
        return None, reason
    weekly_part = v3.weekly_features(inputs["panel"], as_of)
    features = {**base, **mp_part, **weekly_part}
    missing = [name for name in v3.feature_names_v3() if name not in features]
    if missing:
        return None, f"нет признаков {missing}"
    return cm.Row(month=target_month, y={}, features=features), ""


def component_forecast(train: list, row: cm.Row) -> tuple:
    """`({component: forecast}, weights, aggregate)` для одной строки."""
    weights = cm.nnls_weights(train)
    components = {
        code: float(v3.fit_catboost(train, row, code, True))
        for code in COMPONENTS
    }
    aggregate = float(
        sum(
            weight * components[code]
            for weight, code in zip(weights, COMPONENTS)
        )
    )
    return components, weights, aggregate


def walk_forward_components(rows: list, history_months: int) -> tuple:
    """Компонентный walk-forward на последних `history_months` месяцах.

    Обучение — только прошлые строки (`rows[:index]`), без embargo: это
    диагностика качества, а не финальный прогноз. Возврат — длинные строки
    дашборда и агрегаты ошибок по компонентам/агрегату.
    """
    start = max(24, len(rows) - int(history_months))
    dashboard = []
    errors = {code: [] for code in COMPONENTS}
    aggregate_errors = []
    skipped = []
    for index in range(start, len(rows)):
        train = rows[:index]
        row = rows[index]
        try:
            components, weights, aggregate = component_forecast(train, row)
        except cm.DegenerateWindowError as error:
            skipped.append((row.month, str(error)))
            continue
        for component, weight in zip(COMPONENTS, weights):
            actual = float(row.y[component])
            forecast = components[component]
            dashboard.append(
                {
                    "month": row.month,
                    "component": component,
                    "label": COMPONENT_LABELS[component],
                    "actual": actual,
                    "forecast": forecast,
                    "error": forecast - actual,
                    "weight": float(weight),
                    "source": "walk_forward",
                    "adjustment": 0.0,
                }
            )
            errors[component].append(forecast - actual)
        actual_all = float(row.y[cm.TARGET_ALL])
        dashboard.append(
            {
                "month": row.month,
                "component": cm.TARGET_ALL,
                "label": COMPONENT_LABELS[cm.TARGET_ALL],
                "actual": actual_all,
                "forecast": aggregate,
                "error": aggregate - actual_all,
                "weight": 1.0,
                "source": "walk_forward",
                "adjustment": 0.0,
            }
        )
        aggregate_errors.append(aggregate - actual_all)
    summary = {
        code: {
            "n": len(errors[code]),
            "rmse": eu.rmse(errors[code]) if errors[code] else None,
            "mae": eu.mae(errors[code]) if errors[code] else None,
        }
        for code in COMPONENTS
    }
    summary[cm.TARGET_ALL] = {
        "n": len(aggregate_errors),
        "rmse": eu.rmse(aggregate_errors) if aggregate_errors else None,
        "mae": eu.mae(aggregate_errors) if aggregate_errors else None,
    }
    return dashboard, summary, skipped


# ---------------------------------------------------------------------------
# Экспертные корректировки и дашборд
# ---------------------------------------------------------------------------


def load_adjustments(path: Path) -> list:
    """CSV `month,component,delta_pp`; компонент — код или food/nonfood/... ."""
    if path is None:
        return []
    frame = pd.read_csv(path, dtype={"component": str})
    required = {"month", "component", "delta_pp"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"{path}: нет колонок {sorted(missing)}")
    aliases = {
        "food": "cpi_food",
        "nonfood": "cpi_nonfood",
        "services": "cpi_services",
        "all": "cpi_all",
    }
    result = []
    for _, row in frame.iterrows():
        component = aliases.get(str(row["component"]).strip(), str(row["component"]).strip())
        if component not in COMPONENT_LABELS:
            raise ValueError(f"{path}: неизвестный component {row['component']!r}")
        result.append(
            {
                "month": parse_month(row["month"]),
                "component": component,
                "delta": float(row["delta_pp"]),
            }
        )
    return result


def apply_adjustments(rows: list, adjustments: list) -> list:
    """Применяет корректировки к компонентам и пересчитывает агрегат месяца."""
    if not adjustments:
        return rows
    by_key = {
        (item["month"], item["component"]): item["delta"]
        for item in adjustments
    }
    for row in rows:
        if row["component"] == cm.TARGET_ALL:
            # Агрегат пересчитывается ниже из компонент; его собственная
            # корректировка добавляется один раз — после пересчёта.
            continue
        delta = by_key.get((row["month"], row["component"]))
        if delta is None:
            continue
        row["forecast"] = float(row["forecast"]) + delta
        row["adjustment"] = float(row.get("adjustment", 0.0)) + delta
        row["source"] = f"{row['source']}+expert"
        if row["actual"] is not None:
            row["error"] = row["forecast"] - float(row["actual"])
    # Пересчёт агрегата по скорректированным компонентам и их весам.
    by_month = {}
    for row in rows:
        by_month.setdefault(row["month"], []).append(row)
    for month, month_rows in by_month.items():
        components = [
            row for row in month_rows if row["component"] in COMPONENTS
        ]
        aggregate = next(
            (row for row in month_rows if row["component"] == cm.TARGET_ALL),
            None,
        )
        if aggregate is None or len(components) != len(COMPONENTS):
            continue
        forecast = sum(
            float(row["weight"]) * float(row["forecast"])
            for row in components
        )
        if (month, cm.TARGET_ALL) in by_key:
            forecast += by_key[(month, cm.TARGET_ALL)]
        aggregate["forecast"] = forecast
        if aggregate["actual"] is not None:
            aggregate["error"] = forecast - float(aggregate["actual"])
        if any(row["adjustment"] for row in components) or (
            month,
            cm.TARGET_ALL,
        ) in by_key:
            aggregate["source"] = f"{aggregate['source']}+expert"
    return rows


def build_current_rows(
    target_month: date,
    components: dict,
    weights: tuple,
    aggregate: float,
    adjustments: list,
) -> list:
    rows = []
    for component, weight in zip(COMPONENTS, weights):
        rows.append(
            {
                "month": target_month,
                "component": component,
                "label": COMPONENT_LABELS[component],
                "actual": None,
                "forecast": float(components[component]),
                "error": None,
                "weight": float(weight),
                "source": "nowcast",
                "adjustment": 0.0,
            }
        )
    rows.append(
        {
            "month": target_month,
            "component": cm.TARGET_ALL,
            "label": COMPONENT_LABELS[cm.TARGET_ALL],
            "actual": None,
            "forecast": float(aggregate),
            "error": None,
            "weight": 1.0,
            "source": "nowcast",
            "adjustment": 0.0,
        }
    )
    return apply_adjustments(rows, adjustments)


def dashboard_frame(rows: list) -> pd.DataFrame:
    order = {code: index for index, code in enumerate((*COMPONENTS, cm.TARGET_ALL))}
    frame = pd.DataFrame(rows)
    frame["month"] = pd.to_datetime(frame["month"])
    frame["order"] = frame["component"].map(order)
    frame = frame.sort_values(["month", "order"]).drop(columns=["order"])
    return frame


def wide_dashboard(frame: pd.DataFrame) -> pd.DataFrame:
    wide = frame.pivot(
        index="month", columns="component", values=["actual", "forecast", "error"]
    )
    wide.columns = [f"{component}_{kind}" for kind, component in wide.columns]
    columns = []
    for component in (*COMPONENTS, cm.TARGET_ALL):
        label = COMPONENT_LABELS[component]
        for kind in ("actual", "forecast", "error"):
            name = f"{component}_{kind}"
            if name in wide.columns:
                columns.append(name)
    return wide[columns].reset_index()


# ---------------------------------------------------------------------------
# Артефакты
# ---------------------------------------------------------------------------


def write_json(path: Path, payload: dict) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def git_state() -> dict:
    try:
        sha = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip()
        branch = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=ROOT, text=True
        ).strip()
        dirty = bool(
            subprocess.check_output(
                ["git", "status", "--porcelain"], cwd=ROOT, text=True
            ).strip()
        )
        return {"sha": sha, "branch": branch, "dirty": dirty}
    except (OSError, subprocess.SubprocessError):
        return {"sha": "", "branch": "", "dirty": None}


def render_report(payload: dict) -> str:
    lines = ["# Live-пайплайн CB_COMP: воспроизведение на витринах", ""]
    lines.append(
        f"> `scripts/cb_comp_live_pipeline.py`; as-of `{payload['as_of']}`; "
        f"целевой месяц `{payload['target_month']}`. Числа не редактируются руками."
    )
    lines.append("")
    lines.append(f"- Train: {payload['sample']['n_rows']} строк "
                 f"({payload['sample']['first']}…{payload['sample']['last']}); "
                 f"пропущено {payload['sample']['n_skipped']}.")
    lines.append(f"- История дашборда: {payload['history']['n_months']} мес.; "
                 f"walk-forward пропущено {payload['history']['n_skipped']}.")
    lines.append("")
    lines.append("## Карта стадий авторов → live")
    lines.append("")
    lines.append("| Стадия авторов | Live-источник/реализация | Статус |")
    lines.append("|---|---|---|")
    for stage, source, status in PIPELINE_STAGES:
        lines.append(f"| {stage} | {source} | {status} |")
    lines.append("")
    lines.append("## Текущий прогноз, Россия")
    lines.append("")
    lines.append("| Компонент | Прогноз, п.п. m/m | Вес NNLS |")
    lines.append("|---|---:|---:|")
    for component in (*COMPONENTS, cm.TARGET_ALL):
        value = payload["forecast"][component]
        weight = payload["weights"].get(component)
        weight_text = "—" if weight is None else f"{weight:.4f}"
        lines.append(f"| {COMPONENT_LABELS[component]} | {value:+.4f} | {weight_text} |")
    lines.append("")
    lines.append("## Ошибки walk-forward")
    lines.append("")
    lines.append("| Компонент | n | RMSE | MAE |")
    lines.append("|---|---:|---:|---:|")
    for component in (*COMPONENTS, cm.TARGET_ALL):
        item = payload["errors"][component]
        lines.append(
            f"| {COMPONENT_LABELS[component]} | {item['n']} | "
            f"{item['rmse']:.4f} | {item['mae']:.4f} |"
        )
    lines.append("")
    lines.append("## Веса и ЖКУ")
    lines.append("")
    lines.append(f"- NNLS-веса: `{payload['weights']}`")
    lines.append(f"- Веса ИПЦ 2024 (справочно): `{payload['cpi_weights']}`")
    utilities = payload["utilities"]
    lines.append(
        f"- ЖКУ: статус `{utilities.get('status')}`; "
        f"последний прогноз `{utilities.get('last_forecast_date')}` = "
        f"`{utilities.get('last_forecast_russia')}`; источник "
        f"`{utilities.get('source')}`; stale {utilities.get('stale_months')} мес."
    )
    lines.append("")
    lines.append("## Экспертные корректировки")
    lines.append("")
    if payload["adjustments"]:
        lines.append("| Месяц | Компонент | Δ, п.п. |")
        lines.append("|---|---|---:|")
        for item in payload["adjustments"]:
            lines.append(
                f"| {item['month']} | {item['component']} | {item['delta']:+.4f} |"
            )
    else:
        lines.append("Не заданы (`--adjustments` не передан).")
    lines.append("")
    lines.append("## ДФО и ограничения")
    lines.append("")
    lines.append(
        "- ДФО: `not_reproduced` — в live-витринах нет CPI и весов по ДФО; "
        "в архиве авторов есть только прогноз ЖКУ по ДФО."
    )
    lines.append(
        "- 88 компонентных групп и SA-ряд NMFI в live-витринах отсутствуют; "
        "воспроизведение — на 3 компонентах + агрегате."
    )
    lines.append(
        "- Официальный ИПЦ — NSA m/m; SA-таргет и ЖКУ-оверлей не подменяются."
    )
    lines.append("- В ClickHouse скрипт не пишет; `ML_LOG`/MLflow не используются.")
    lines.append("")
    lines.append("## Воспроизводимость")
    lines.append("")
    lines.append("```bash")
    lines.append("make cb-comp-live")
    lines.append("make cb-comp-live ARGS=\"--adjustments build/cb-comp-live/adjustments.csv\"")
    lines.append("```")
    return "\n".join(lines) + "\n"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Live-адаптация production-пайплайна CB_COMP"
    )
    parser.add_argument(
        "--out-dir",
        default=str(ROOT / "build" / "cb-comp-live"),
        help="каталог артефактов",
    )
    parser.add_argument("--archive", default=None, help="распакованный архив CB_COMP")
    parser.add_argument(
        "--as-of",
        default=None,
        help="дата среза признаков (по умолчанию сегодня)",
    )
    parser.add_argument(
        "--target-month",
        default=None,
        help="целевой месяц YYYY-MM (по умолчанию следующий за опубликованным ИПЦ)",
    )
    parser.add_argument(
        "--history-months",
        type=int,
        default=DEFAULT_HISTORY_MONTHS,
        help="глубина walk-forward дашборда",
    )
    parser.add_argument(
        "--adjustments",
        default=None,
        help="CSV month,component,delta_pp",
    )
    parser.add_argument(
        "--utilities-mom",
        type=float,
        default=None,
        help="переопределить экспертный прогноз ЖКУ, п.п. m/m",
    )
    arguments = parser.parse_args(argv)

    archive = resolve_archive(arguments.archive)
    out_dir = Path(arguments.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    as_of = (
        date.fromisoformat(arguments.as_of)
        if arguments.as_of
        else date.today()
    )
    print(f"Архив: {archive}", flush=True)
    print(f"As-of: {as_of}", flush=True)

    inputs = load_live_inputs()
    rows, skipped = build_historical_rows(inputs)
    if not rows:
        raise SystemExit("пустая историческая выборка")
    print(
        f"Историческая выборка: {len(rows)} строк "
        f"({rows[0].month}…{rows[-1].month}), пропущено {len(skipped)}",
        flush=True,
    )

    if arguments.target_month:
        target_month = parse_month(arguments.target_month)
    else:
        target_month, reason = cm.nowcast_target_month(inputs["targets"], as_of)
        if target_month is None:
            raise SystemExit(f"не определён целевой месяц: {reason}")

    current_row, reason = build_current_row(inputs, target_month, as_of)
    if current_row is None:
        raise SystemExit(f"не собраны признаки {target_month}: {reason}")
    print(f"Целевой месяц: {target_month}", flush=True)

    components, weights, aggregate = component_forecast(rows, current_row)
    weights_by_component = dict(zip(COMPONENTS, weights))
    weights_by_component[cm.TARGET_ALL] = 1.0

    history_rows, errors, history_skipped = walk_forward_components(
        rows, arguments.history_months
    )
    adjustments = load_adjustments(
        Path(arguments.adjustments) if arguments.adjustments else None
    )
    history_rows = apply_adjustments(history_rows, adjustments)
    current_rows = build_current_rows(
        target_month, components, weights, aggregate, adjustments
    )
    all_rows = history_rows + current_rows
    frame = dashboard_frame(all_rows)
    wide = wide_dashboard(frame)

    dashboard_csv = out_dir / "cb_comp_live_dashboard.csv"
    wide_csv = out_dir / "cb_comp_live_dashboard_wide.csv"
    xlsx_path = out_dir / "cb_comp_live_dashboard.xlsx"
    frame.to_csv(dashboard_csv, index=False)
    wide.to_csv(wide_csv, index=False)
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        frame.to_excel(writer, sheet_name="components_long", index=False)
        wide.to_excel(writer, sheet_name="components_wide", index=False)
        pd.DataFrame(
            [
                {"component": component, "weight": weight}
                for component, weight in weights_by_component.items()
            ]
        ).to_excel(writer, sheet_name="weights", index=False)

    current_adjusted = {
        row["component"]: row["forecast"]
        for row in current_rows
        if row["component"] in (*COMPONENTS, cm.TARGET_ALL)
    }
    utilities = load_utilities(archive, arguments.utilities_mom)
    cpi_weights = load_reference_weights(archive)
    gu_utilities = (
        {
            "last_forecast_date": utilities.get("last_forecast_date"),
            "last_forecast_value": utilities.get("last_forecast_far_eastern"),
        }
        if utilities.get("status") == "ok"
        else None
    )
    payload = {
        "run_id": str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"cb-comp-live|{target_month}|{as_of}|{len(rows)}|{len(frame)}",
            )
        ),
        "as_of": as_of.isoformat(),
        "target_month": target_month.isoformat(),
        "sample": {
            "n_rows": len(rows),
            "first": rows[0].month.isoformat(),
            "last": rows[-1].month.isoformat(),
            "n_skipped": len(skipped),
        },
        "history": {
            "n_months": len({row["month"] for row in history_rows}),
            "n_skipped": len(history_skipped),
        },
        "forecast": current_adjusted,
        "weights": {
            component: float(weight)
            for component, weight in weights_by_component.items()
        },
        "errors": errors,
        "cpi_weights": cpi_weights,
        "utilities": utilities,
        "far_eastern_gu": {
            "cpi_status": "not_reproduced",
            "reason": "нет live CPI и весов по ДФО; в архиве только ЖКУ по ДФО",
            "utilities": gu_utilities,
        },
        "adjustments": [
            {
                "month": item["month"].isoformat(),
                "component": item["component"],
                "delta": item["delta"],
            }
            for item in adjustments
        ],
        "pipeline_stages": [
            {"stage": stage, "source": source, "status": status}
            for stage, source, status in PIPELINE_STAGES
        ],
        "git": git_state(),
        "artifacts": {},
    }
    json_path = out_dir / "cb_comp_live_forecast.json"
    report_path = out_dir / "cb_comp_live_report.md"
    write_json(json_path, payload)
    report_path.write_text(render_report(payload), encoding="utf-8")
    for path in (dashboard_csv, wide_csv, xlsx_path, json_path, report_path):
        payload["artifacts"][path.name] = sha256(path)
    write_json(json_path, payload)

    print("Прогноз, Россия:", flush=True)
    for component in (*COMPONENTS, cm.TARGET_ALL):
        print(
            f"  {COMPONENT_LABELS[component]}: "
            f"{current_adjusted[component]:+.4f}",
            flush=True,
        )
    print(f"Ошибки walk-forward: {errors}", flush=True)
    print(f"ЖКУ: {utilities.get('status')} / {utilities.get('source')}", flush=True)
    print(
        f"Артефакты: {dashboard_csv}, {wide_csv}, {xlsx_path}, "
        f"{json_path}, {report_path}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
