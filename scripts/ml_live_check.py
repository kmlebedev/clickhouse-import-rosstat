#!/usr/bin/env python3
"""Проверка «live-прогноз записан до публикации» (infra/4.3, #184).

Читает `v_events_calendar` (ближайшее заседание ЦБ) и `v_ml_live_accuracy`
(production-строки `forecast_log`). Для каждого ожидаемого трека требует
строку на `target_date = next_meeting`, `forecast_date < target_date`,
непустой `data_version` и `stage='production'`.

Staged-строки (`:shadow`/`:canary`) публикационным прогнозом не считаются:
именно поэтому проверка идёт по витрине со стадией, а не по `LIKE 'cbr_rate%'`.

Запуск — только через `make ml-live-check` (env подключает Makefile).
Код 0 — все треки на месте; 1 — пропуск/нет заседания/нет витрины.
`--warn-only` превращает пропуски треков в предупреждения (код 0), но не
скрывает отсутствие витрины или календаря.
"""

import argparse
import json
import sys
from pathlib import Path

from extrapolation_aisc import ch_query

ROOT = Path(__file__).resolve().parent.parent

# Треки, которые публикация обязана иметь в журнале. LLM — опционально:
# статья может выйти без LLM-трека, но если он заявлен, нужен --require-llm.
DEFAULT_TRACKS = ("cbr_rate:taylor", "cbr_rate:ordered_logit")
LLM_TRACK = "cbr_rate:llm"


class LiveCheckError(Exception):
    """Ошибка входа/данных; текст — без секретов."""


def _sql_literal(value: str) -> str:
    """SQL-литерал для проверенного значения."""
    return "'" + value.replace("'", "''") + "'"


def next_meeting() -> str:
    """Ближайшее будущее заседание ЦБ (`YYYY-MM-DD`)."""
    sql = (
        "SELECT toString(min(event_date)) AS next_meeting "
        "FROM v_events_calendar "
        "WHERE category = 'cbr' AND event_date >= today()\nFORMAT JSON"
    )
    rows = ch_query(sql)
    if not rows or not rows[0].get("next_meeting"):
        raise LiveCheckError(
            "в v_events_calendar нет будущего заседания category='cbr' — "
            "прогнать make import STAT=events_calendar"
        )
    return str(rows[0]["next_meeting"])[:10]


def live_rows(meeting: str) -> list:
    """Production-строки витрины на дату заседания."""
    sql = (
        "SELECT metric, toString(forecast_date) AS forecast_date, "
        "toString(target_date) AS target_date, stage, data_version, is_resolved "
        "FROM v_ml_live_accuracy "
        f"WHERE target_date = {_sql_literal(meeting)} AND stage = 'production' "
        "ORDER BY metric\nFORMAT JSON"
    )
    rows = ch_query(sql)
    if not isinstance(rows, list):
        raise LiveCheckError("v_ml_live_accuracy: ответ не список строк")
    return rows


def check(rows: list, meeting: str, tracks: tuple) -> list:
    """Список проблем: трек без корректной production-строки."""
    by_metric = {}
    for row in rows:
        metric = str(row.get("metric", ""))
        if metric not in tracks:
            continue
        if str(row.get("stage", "")) != "production":
            continue
        by_metric.setdefault(metric, []).append(row)

    problems = []
    for track in tracks:
        candidates = by_metric.get(track, [])
        if not candidates:
            problems.append(f"{track}: нет production-строки на {meeting}")
            continue
        # Последняя по forecast_date строка — та, что публикуется.
        row = max(candidates, key=lambda item: str(item.get("forecast_date", "")))
        forecast_date = str(row.get("forecast_date", ""))[:10]
        target_date = str(row.get("target_date", ""))[:10]
        data_version = str(row.get("data_version", "")).strip()
        if target_date != meeting:
            problems.append(f"{track}: target_date {target_date} ≠ {meeting}")
        elif not forecast_date or forecast_date >= target_date:
            problems.append(
                f"{track}: forecast_date {forecast_date or '—'} не раньше target_date {target_date}"
            )
        elif not data_version:
            problems.append(f"{track}: пустой data_version — публикация запрещена")
    return problems


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Проверка live-прогноза ключевой ставки перед публикацией "
                    "(infra/4.3)."
    )
    parser.add_argument(
        "--track", action="append", default=[],
        help="ожидаемая production-метрика (повторяемый); по умолчанию "
             "cbr_rate:taylor и cbr_rate:ordered_logit",
    )
    parser.add_argument(
        "--require-llm", action="store_true",
        help=f"дополнительно требовать {LLM_TRACK}",
    )
    parser.add_argument(
        "--warn-only", action="store_true",
        help="пропуски треков — предупреждения, код 0",
    )
    args = parser.parse_args(argv)

    tracks = tuple(args.track or DEFAULT_TRACKS)
    if args.require_llm and LLM_TRACK not in tracks:
        tracks = tracks + (LLM_TRACK,)

    try:
        meeting = next_meeting()
        rows = live_rows(meeting)
    except LiveCheckError as error:
        print(f"ошибка: {error}", file=sys.stderr)
        return 1
    except Exception as error:
        # Витрина может отсутствовать до первого `make import STAT=key_rate_views`.
        text = str(error)
        hint = ""
        if "v_ml_live_accuracy" in text or "UNKNOWN_TABLE" in text:
            hint = " — создайте витрину: make import STAT=key_rate_views"
        print(f"ошибка: {text}{hint}", file=sys.stderr)
        return 1

    problems = check(rows, meeting, tracks)
    if not problems:
        print(f"OK: {len(tracks)} трек(ов) на {meeting} записаны до публикации")
        return 0

    for problem in problems:
        print(("WARN " if args.warn_only else "FAIL ") + problem, file=sys.stderr)
    if args.warn_only:
        print("warn-only: публикация не блокируется, проверьте треки выше")
        return 0
    print(
        "перед публикацией запишите прогноз: make key-rate-forecast-log "
        "(и make key-rate-llm-forecast, если нужен LLM-трек)",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
