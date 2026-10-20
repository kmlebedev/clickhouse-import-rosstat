#!/usr/bin/env python3
"""CPI live3 h1/h3/h6: walk-forward адаптация модели `cb_comp_live_compare`.

Эксперимент Task 4.1 (issue #296): проверить, добавляет ли model-based прогноз
ИПЦ информацию каналу «Инфляционные и ценовые ожидания» модели ставки.

Модель взята из `cb_comp_live_compare.py` (live3): 3 официальных компонента
ИПЦ (`cpi_food`, `cpi_nonfood`, `cpi_services`) + признаки
`cpi_monthly_v3.feature_names_v3()` + NNLS-агрегат. Сам `cb_comp_live_compare`
обучается на фиксированном train 2010-03…2018-12 и тестируется 2019-01…2024-07;
для модели ставки это look-ahead и обрезанное покрытие, поэтому здесь та же
модель обёрнута в walk-forward с as-of по дате заседания.

Конвенция горизонта — issue #296: для origin `O` и горизонта h таргет —
накопленный m/m за `O+1 … O+h` (h=1 — следующий месяц, h=3/6 — путь вперёд).
Обучение — только origin'ы `O' <= O−h−1`, чей целевой конец `O'+h` уже
наблюдён к cutoff O (месяц O публикуется в конце O+1).

Feature-вселенная шире target-выборки: для живого заседания нужен origin,
чья метка ещё не опубликована, но признаки уже доступны. Строка с `y={}`
участвует в прогнозе и не участвует в обучении.

Скрипт только читает ClickHouse; в БД не пишет.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import cb_comp_live_compare as cbc  # noqa: E402
import cpi_monthly as cm  # noqa: E402
import cpi_monthly_v3 as cmv3  # noqa: E402

HORIZONS = (1, 3, 6)
MIN_TRAIN = 24
SAMPLE_FIRST = cmv3.SAMPLE_BOUNDS["long"][0]


def shift_month(day: date, months: int) -> date:
    return cbc.shift_month(day, months)


def latest_origin(as_of: date) -> date:
    """Последний origin `O` с `cutoff_for(O) <= as_of`.

    `cutoff_for(O)` — 8-е `O+1`; если `as_of` до 8-го, последний origin —
    месяц до предыдущего. Возвращает первое число месяца.
    """
    month = cm.month_of(as_of)
    candidate = shift_month(month, -1) if as_of.day >= 8 else shift_month(month, -2)
    while cm.cutoff_for(candidate) > as_of:
        candidate = shift_month(candidate, -1)
    return candidate


def load_rows(as_of: Optional[date] = None) -> tuple:
    """`({month: Row}, skipped)` для feature-вселенной `SAMPLE_FIRST…latest_origin`.

    `y` заполняется только при полном наборе четырёх меток; иначе `{}`.
    Для месяца без метки признаки всё равно строятся: это нужно для live-origin.
    """
    as_of = as_of or date.today()
    targets = cm.load_targets()
    survey = cm.load_survey()
    usd = cm.load_usd()
    key_rate = cm.load_key_rate()
    fao = cm.load_fao()
    panel = cmv3.weekly_panel(cmv3.load_weekly())
    index = cm.target_index(targets)
    last = latest_origin(as_of)
    rows = {}
    skipped = []
    month = SAMPLE_FIRST
    while month <= last:
        cutoff = cm.cutoff_for(month)
        base, reason = cm.base_features(month, index, usd, key_rate, fao, cutoff)
        if base is None:
            skipped.append((month, reason))
            month = shift_month(month, 1)
            continue
        mp_part, reason = cm.mp_features(month, survey, cutoff)
        if mp_part is None:
            skipped.append((month, reason))
            month = shift_month(month, 1)
            continue
        weekly_part = cmv3.weekly_features(panel, cutoff)
        y = {
            code: values[cm.month_end(month)]
            for code, values in index.items()
            if cm.month_end(month) in values
        }
        if len(y) != len(cm.TARGET_SERIES):
            y = {}
        rows[month] = cm.Row(
            month=month,
            y=y,
            features={**base, **mp_part, **weekly_part},
        )
        month = shift_month(month, 1)
    return rows, skipped


def cumulative_targets(rows_by_month: dict) -> dict:
    """`{h: {M: {code: накопленный h-рост, п.п.}}}` за `M−h+1 … M`.

    Для origin `O` целевой месяц `M = O+h`, поэтому окно — `O+1 … O+h`.
    Строки без полного набора компонент в таргет не попадают.
    """
    result = {horizon: {} for horizon in HORIZONS}
    for month, row in rows_by_month.items():
        if not row.y:
            continue
        for horizon in HORIZONS:
            window = [shift_month(month, -offset) for offset in range(horizon)]
            if any(
                item not in rows_by_month or not rows_by_month[item].y
                for item in window
            ):
                continue
            result[horizon][month] = {}
            for code in (*cm.BOTTOMUP_COMPONENTS, cm.TARGET_ALL):
                product = 1.0
                for item in window:
                    product *= 1.0 + float(rows_by_month[item].y[code]) / 100.0
                result[horizon][month][code] = (product - 1.0) * 100.0
    return result


def training_origins(months: list, origin: date, horizon: int) -> list:
    """Origin'ы `O' <= O−h−1`: целевой конец `O'+h` наблюдён к cutoff O."""
    boundary = shift_month(origin, -horizon - 1)
    return [month for month in months if month <= boundary]


def assert_no_lookahead(
    rows_by_month: dict,
    origin: date,
    horizon: int,
) -> None:
    """Train-таргеты не выходят за наблюдённые месяцы, цель — впереди cutoff."""
    target_month = shift_month(origin, horizon)
    if cm.cutoff_for(origin) >= cm.available_from_mes(cm.month_end(target_month)):
        raise AssertionError(
            f"утечка h{horizon}: cutoff origin {origin} не раньше публикации "
            f"цели {target_month}"
        )
    for month in training_origins(sorted(rows_by_month), origin, horizon):
        end = shift_month(month, horizon)
        if end > shift_month(origin, -1):
            raise AssertionError(
                f"утечка h{horizon}: train-origin {month} имеет конец {end} "
                f"позже наблюдённого {shift_month(origin, -1)}"
            )
        if cm.available_from_mes(cm.month_end(end)) > cm.cutoff_for(origin):
            raise AssertionError(
                f"утечка h{horizon}: таргет {end} не наблюдён к cutoff {origin}"
            )


def forecast_origin(
    rows_by_month: dict,
    targets: dict,
    months: list,
    origin: date,
    horizon: int,
) -> Optional[float]:
    """live3-агрегат h-горизонта в origin; None — меньше MIN_TRAIN."""
    assert_no_lookahead(rows_by_month, origin, horizon)
    train = []
    for month in training_origins(months, origin, horizon):
        target = targets[horizon].get(shift_month(month, horizon))
        if target is None:
            continue
        train.append(
            cm.Row(
                month=month,
                y=target,
                features=rows_by_month[month].features,
            )
        )
    if len(train) < MIN_TRAIN:
        return None
    try:
        weights = cm.nnls_weights(train)
    except cm.DegenerateWindowError:
        return None
    components = []
    for code in cm.BOTTOMUP_COMPONENTS:
        model, names = cbc.fit_component(train, code)
        components.append(cbc.predict(model, names, rows_by_month[origin].features))
    return float(sum(weight * value for weight, value in zip(weights, components)))


def forecast_all(rows_by_month: dict, origins: Optional[list] = None) -> dict:
    months = sorted(rows_by_month)
    selected = sorted(set(origins)) if origins is not None else months
    targets = cumulative_targets(rows_by_month)
    return {
        horizon: {
            origin: forecast_origin(
                rows_by_month, targets, months, origin, horizon
            )
            for origin in selected
        }
        for horizon in HORIZONS
    }


def asof_origin(meeting: date, origins: list) -> Optional[date]:
    eligible = [origin for origin in origins if cm.cutoff_for(origin) <= meeting]
    return max(eligible) if eligible else None


def build_cpi_features(meetings: list, rows_by_month: dict) -> dict:
    months = sorted(rows_by_month)
    needed = sorted(
        {
            origin
            for meeting in set(meetings)
            for origin in [asof_origin(meeting, months)]
            if origin is not None
        }
    )
    forecasts = forecast_all(rows_by_month, needed)
    result = {}
    for meeting in sorted(set(meetings)):
        origin = asof_origin(meeting, months)
        if origin is None:
            result[meeting] = {
                "cpi_v3_h1": None,
                "cpi_v3_h3": None,
                "cpi_v3_h6": None,
                "cpi_v3_available": 0,
                "cpi_v3_origin": None,
            }
            continue
        item = {
            f"cpi_v3_h{horizon}": forecasts[horizon].get(origin)
            for horizon in HORIZONS
        }
        item["cpi_v3_available"] = int(
            all(item[f"cpi_v3_h{horizon}"] is not None for horizon in HORIZONS)
        )
        item["cpi_v3_origin"] = origin
        result[meeting] = item
    return result


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(
        description="CPI live3 h1/h3/h6: as-of прогнозы для эксперимента #296."
    )
    parser.add_argument("--as-of", default=None, help="дата среза, YYYY-MM-DD")
    parser.add_argument("--meeting", default=None, help="дата заседания, YYYY-MM-DD")
    parser.add_argument("--out", default="", help="JSON-файл (необязательно)")
    args = parser.parse_args(argv)
    try:
        as_of = date.fromisoformat(args.as_of) if args.as_of else date.today()
    except ValueError:
        print(f"ошибка --as-of: {args.as_of!r}", file=sys.stderr)
        return 2
    try:
        meetings = (
            [date.fromisoformat(args.meeting)] if args.meeting else None
        )
    except ValueError:
        print(f"ошибка --meeting: {args.meeting!r}", file=sys.stderr)
        return 2
    rows_by_month, skipped = load_rows(as_of)
    if not rows_by_month:
        print("feature-вселенная пуста", file=sys.stderr)
        return 1
    selected = meetings or sorted(rows_by_month)[-6:]
    features = build_cpi_features(selected, rows_by_month)
    print(
        f"месяцев {len(rows_by_month)}, пропущено {len(skipped)}; "
        f"as-of {as_of}; origin до {latest_origin(as_of)}"
    )
    for meeting in selected:
        item = features[meeting]
        print(
            f"{meeting}: h1={item['cpi_v3_h1']}, h3={item['cpi_v3_h3']}, "
            f"h6={item['cpi_v3_h6']} (origin {item['cpi_v3_origin']})"
        )
    if args.out:
        payload = {
            key.isoformat(): {
                name: (value.isoformat() if isinstance(value, date) else value)
                for name, value in item.items()
            }
            for key, item in features.items()
        }
        Path(args.out).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"записано: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
