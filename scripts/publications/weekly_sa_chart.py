#!/usr/bin/env python3
"""График недельного SA м/м ИПЦ против SA м/м ЦБ для публикации.

Данные берутся из витрин, а не переносятся литералами: `v_weekly_cpi_sa`
(ряд `weekly_cpi:sa_4w`, строка `partial=1` — nowcast неполного месяца) и
`v_cbr_macro` («Все товары и услуги»). Поэтому обновление публикации — это
прогон импортёра `weekly_cpi_sa` и повторный запуск скрипта, а не ручной
перенос десятков чисел.

Запуск из корня репозитория::

    python3 scripts/publications/weekly_sa_chart.py \
        --publication 2026-10-22-weekly-sa-cpi --as-of 2026-10-31

`--as-of` ограничивает ряд датой включительно: это нужно, чтобы
воспроизвести уже опубликованный график на данных, которые успели уйти
вперёд. Пароль и DSN — только из окружения (`CLICKHOUSE_URL`), как во всём
проекте; скрипт ничего не пишет в ClickHouse.
"""

from __future__ import annotations

import argparse
import html
import json
import sys
from datetime import date
from pathlib import Path

# Скрипт лежит в `scripts/publications/`, запуск прямой, поэтому каталог
# `scripts` добавляется в sys.path до импорта общего транспорта.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lib import clickhouse as ch

# Каталог публикаций по умолчанию: имя каталога — дата и тема статьи.
DEFAULT_PUBLICATION = "2026-10-22-weekly-sa-cpi"

# Ряды витрин. `weekly_cpi:sa_4w` — SA-темп за 4 недели в процентах; CBR —
# SA м/м «Все товары и услуги» из indicators_cpd.xlsx.
WEEKLY_SERIES = "weekly_cpi:sa_4w"
CBR_SERIES = "Все товары и услуги"

# Разрядность: недельный ряд публикуется до 0,0001 п.п., ЦБ — до 0,01 п.п.
# Округление — часть контракта графика: без него в JSON уходил бы Float32
# ЦБ (0,38999998569488525) и файл менялся бы от версии драйвера.
WEEKLY_DIGITS = 4
CBR_DIGITS = 2

TITLE = "Недельный SA м/м ИПЦ против SA м/м ЦБ"
UNIT = "% м/м, SA"
# Подзаголовок публикует единицу без «SA»: сокращение расшифровано в легенде.
SUBTITLE_UNIT = "% м/м"
SOURCE = (
    "Росстат, недельный агрегат «Индекс потребительских цен (недельная оценка)»; "
    "ЦБ, indicators_cpd.xlsx, «Все товары и услуги»; "
    "расчёт — 4-нед. индекс + X-13ARIMA-SEATS"
)
# Подпись источника на самом графике короче поля `source`: в JSON она
# развёрнута для читателя файла, на холсте — сжата до строки.
SVG_SOURCE = (
    "Источник: Росстат, недельный агрегат; ЦБ, indicators_cpd.xlsx; "
    "расчёт — 4-нед. индекс + X-13ARIMA-SEATS."
)

# Даты, отмеченные точками на графике: начало окна, тарифный март 2022 и
# последний полный месяц. Список — часть оформления, а не данных.
CBR_MARKER_DATES = ("2021-01-31", "2022-03-31", "2026-09-30")
WEEKLY_MARKER_DATES = ("2021-01-31", "2026-09-30")

MONTHS_NOMINATIVE = (
    "январь", "февраль", "март", "апрель", "май", "июнь",
    "июль", "август", "сентябрь", "октябрь", "ноябрь", "декабрь",
)
MONTHS_SHORT = (
    "янв.", "фев.", "мар.", "апр.", "мая", "июн.",
    "июл.", "авг.", "сен.", "окт.", "ноя.", "дек.",
)


def _points(rows):
    """Приводит строки витрины к точкам `(дата, значение, partial, as_of)`.

    `value` — `Nullable(Float64)`: NULL-точка в график не попадает, иначе
    `None` уехал бы в JSON и сломал рендер. Дата — строка `YYYY-MM-DD` в том
    виде, в каком её отдаёт ClickHouse.
    """
    points = []
    for row in rows:
        value = row.get("value")
        if value is None:
            continue
        points.append(
            {
                "date": str(row["date"]),
                "value": float(value),
                "partial": int(row.get("partial") or 0),
                "as_of": str(row.get("as_of") or ""),
            }
        )
    points.sort(key=lambda point: point["date"])
    return points


def month_index(day: str, base: date) -> int:
    """Номер месяца точки относительно первого месяца окна."""
    parsed = date.fromisoformat(day)
    return (parsed.year - base.year) * 12 + (parsed.month - base.month)


def month_label(day: str, *, short: bool = False) -> str:
    """Подпись месяца точки: «октябрь 2026» или «окт.»."""
    parsed = date.fromisoformat(day)
    names = MONTHS_SHORT if short else MONTHS_NOMINATIVE
    name = names[parsed.month - 1]
    if short:
        return name
    return f"{name} {parsed.year}"


def decimal(value: float, digits: int) -> str:
    """Число с запятой и фиксированной разрядностью: «7,04»."""
    return f"{value:.{digits}f}".replace(".", ",")


def build_chart_data(weekly_rows, cbr_rows):
    """Собирает данные графика: полные месяцы, строка nowcast и подписи.

    Полные месяцы и nowcast разделены: октябрь 2026 — неполный месяц, его
    точка рисуется отдельно и не участвует в линии. Если nowcast-строки нет
    (ряд пересчитан без текущего месяца), график строится без неё, а не
    падает.
    """
    weekly_points = _points(weekly_rows)
    cbr_points = _points(cbr_rows)
    if len(weekly_points) < 2:
        raise ValueError(
            f"ряд {WEEKLY_SERIES}: {len(weekly_points)} точек — нужно минимум 2"
        )
    if not cbr_points:
        raise ValueError(f"ряд {CBR_SERIES}: нет точек")

    complete = [point for point in weekly_points if point["partial"] == 0]
    nowcast_point = None
    for point in weekly_points:
        if point["partial"] == 1:
            nowcast_point = point
    if not complete:
        raise ValueError(f"ряд {WEEKLY_SERIES}: нет полных месяцев")

    # Недельный ряд в файле непрерывен: nowcast-точка входит и в `weekly`
    # (чтобы линия и ось доходили до последнего месяца), и отдельным блоком
    # `nowcast` (чтобы её можно было нарисовать маркером, не трогая линию).
    # Так устроен опубликованный chart-data.json; переставь — и диффы графика
    # перестанут быть сравнимыми.
    weekly = [[point["date"], round(point["value"], WEEKLY_DIGITS)] for point in weekly_points]
    cbr = [[point["date"], round(point["value"], CBR_DIGITS)] for point in cbr_points]

    subtitle = f"{weekly[0][0][:7]} … {complete[-1]['date'][:7]}"
    if nowcast_point:
        subtitle += f"; {month_label(nowcast_point['date'])} — nowcast"
    subtitle += f"; {SUBTITLE_UNIT}"

    # Порядок ключей — как в опубликованном файле: он читается глазами и диффится.
    return {
        "title": TITLE,
        "subtitle": subtitle,
        "unit": UNIT,
        "as_of": nowcast_point["as_of"] if nowcast_point else "",
        "weekly": weekly,
        "cbr": cbr,
        "nowcast": (
            {
                "date": nowcast_point["date"],
                "value": round(nowcast_point["value"], WEEKLY_DIGITS),
                "partial": True,
            }
            if nowcast_point
            else None
        ),
        "source": SOURCE,
    }


def render_svg(data) -> str:
    """SVG графика: оформление перенесено из опубликованной версии без правок.

    Подписи, зависящие от данных (месяц nowcast, `as_of`, значения марта 2022),
    вычисляются: иначе следующее обновление оставило бы в подписи «as_of 12.10»
    рядом с июнем.
    """
    weekly = [(day, value) for day, value in data["weekly"]]
    cbr = [(day, value) for day, value in data["cbr"]]
    nowcast = data["nowcast"]

    width, height = 1100, 560
    left, right, top, bottom = 78, 1040, 86, 470
    ymin, ymax = -0.5, 2.0
    font = "DejaVu Sans, Arial, Helvetica, sans-serif"

    base = date.fromisoformat(weekly[0][0])
    # Окно графика заканчивается самой поздней точкой любого ряда: иначе точка
    # ЦБ за пределами недельного ряда уезжала бы за правую границу холста.
    last_index = max(
        month_index(weekly[-1][0], base),
        month_index(cbr[-1][0], base),
    )
    if nowcast:
        last_index = max(last_index, month_index(nowcast["date"], base))
    n = last_index + 1

    def x_of(day):
        return left + month_index(day, base) * (right - left) / (n - 1)

    def y_of(value):
        value = max(ymin, min(ymax, value))
        return top + (ymax - value) * (bottom - top) / (ymax - ymin)

    def esc(text):
        return html.escape(text, quote=True)

    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img" aria-labelledby="title desc">',
        f'<title id="title">{esc(data["title"])}</title>',
        f'<desc id="desc">{esc(data["subtitle"])}.</desc>',
        f'<rect x="0" y="0" width="{width}" height="{height}" fill="#ffffff"/>',
        f'<text x="{left}" y="30" font-family="{font}" font-size="22" font-weight="bold" fill="#222222">'
        f"{esc(data['title'])}</text>",
        f'<text x="{left}" y="52" font-family="{font}" font-size="14" fill="#555555">'
        f"{esc(data['subtitle'])}</text>",
    ]

    y = -0.5
    while y <= 2.0001:
        yy = y_of(y)
        stroke = "#b8b8b8" if abs(y) < 1e-9 else "#e3e3e3"
        svg.append(
            f'<line x1="{left}" y1="{yy:.1f}" x2="{right}" y2="{yy:.1f}" '
            f'stroke="{stroke}" stroke-width="1"/>'
        )
        label = f"{y:.1f}".replace(".", ",")
        svg.append(
            f'<text x="{left - 10}" y="{yy + 4:.1f}" font-family="{font}" font-size="12" '
            f'text-anchor="end" fill="#666666">{label}</text>'
        )
        y += 0.5

    ticks = list(range(0, n, 6))
    if ticks and n - 1 - ticks[-1] < 4:
        ticks = ticks[:-1]
    ticks.append(n - 1)
    for index in ticks:
        label = None
        for day, _value in weekly:
            if month_index(day, base) == index:
                label = day[:7]
                break
        if label is None and nowcast and month_index(nowcast["date"], base) == index:
            label = nowcast["date"][:7]
        if label is None:
            continue
        xx = left + index * (right - left) / (n - 1)
        svg.append(
            f'<line x1="{xx:.1f}" y1="{top}" x2="{xx:.1f}" y2="{bottom}" '
            f'stroke="#f0f0f0" stroke-width="1"/>'
        )
        anchor = "end" if index == n - 1 else "middle"
        svg.append(
            f'<text x="{xx:.1f}" y="{bottom + 22}" font-family="{font}" font-size="11" '
            f'text-anchor="{anchor}" fill="#666666">{label}</text>'
        )

    svg.append(
        f'<text x="{left}" y="{top - 12}" font-family="{font}" font-size="12" fill="#666666">'
        f"{esc(data['unit'])}</text>"
    )

    # Тарифный март 2022 выходит за верхнюю границу шкалы; значения подписи
    # берутся из рядов, а не из текста статьи.
    march_day = "2022-03-31"
    march_weekly = next((value for day, value in weekly if day == march_day), None)
    march_cbr = next((value for day, value in cbr if day == march_day), None)
    march_x = x_of(march_day)
    svg.append(
        f'<line x1="{march_x:.1f}" y1="{top}" x2="{march_x:.1f}" y2="{bottom}" '
        f'stroke="#c9c9c9" stroke-width="1" stroke-dasharray="3,3"/>'
    )
    if march_weekly is not None and march_cbr is not None:
        march_note = (
            f"март 2022: {decimal(march_weekly, 2)} / {decimal(march_cbr, 2)} — "
            f"выше шкалы"
        )
        svg.append(
            f'<text x="{march_x + 6:.1f}" y="{top + 14}" font-family="{font}" font-size="11" fill="#777777">'
            f"{esc(march_note)}</text>"
        )

    def polyline(points, color, upto=None, dash=None):
        coords = []
        for day, value in points:
            if upto is not None and month_index(day, base) > upto:
                break
            coords.append(f"{x_of(day):.1f},{y_of(value):.1f}")
        if len(coords) < 2:
            return ""
        dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
        return (
            f'<polyline points="{" ".join(coords)}" fill="none" stroke="{color}" '
            f'stroke-width="2.4" stroke-linejoin="round" stroke-linecap="round"{dash_attr}/>'
        )

    # Сплошная линия — полные месяцы, пунктир — участок до nowcast.
    complete_days = [day for day, _value in weekly]
    if nowcast:
        complete_days = [day for day in complete_days if day != nowcast["date"]]
    last_complete_index = month_index(complete_days[-1], base)
    svg.append(polyline(cbr, "#c0392b"))
    svg.append(polyline(weekly, "#1f5fae", upto=last_complete_index))
    if nowcast:
        svg.append(polyline(weekly, "#1f5fae", upto=last_index, dash="5,4"))

    for day, value in cbr:
        if day in CBR_MARKER_DATES:
            svg.append(
                f'<circle cx="{x_of(day):.1f}" cy="{y_of(value):.1f}" r="3.2" fill="#c0392b"/>'
            )
    for day, value in weekly:
        if day in WEEKLY_MARKER_DATES:
            svg.append(
                f'<circle cx="{x_of(day):.1f}" cy="{y_of(value):.1f}" r="3.2" fill="#1f5fae"/>'
            )

    if nowcast:
        oct_x, oct_y = x_of(nowcast["date"]), y_of(nowcast["value"])
        svg.append(
            f'<circle cx="{oct_x:.1f}" cy="{oct_y:.1f}" r="4.5" fill="#ffffff" '
            f'stroke="#1f5fae" stroke-width="2.4"/>'
        )
        as_of = date.fromisoformat(data["as_of"])
        label = (
            f"{month_label(nowcast['date'], short=True)} — nowcast, "
            f"as_of {as_of.strftime('%d.%m')}"
        )
        svg.append(
            f'<text x="{oct_x - 8:.1f}" y="{top + 36}" font-family="{font}" font-size="11" '
            f'text-anchor="end" fill="#1f5fae">{esc(label)}</text>'
        )

    legend_x, legend_y = 665, 86
    svg.append(
        f'<line x1="{legend_x}" y1="{legend_y}" x2="{legend_x + 28}" y2="{legend_y}" '
        f'stroke="#1f5fae" stroke-width="2.4"/>'
    )
    svg.append(
        f'<text x="{legend_x + 36}" y="{legend_y + 4}" font-family="{font}" font-size="12" '
        f'fill="#333333">{esc("недельный SA м/м")}</text>'
    )
    svg.append(
        f'<line x1="{legend_x}" y1="{legend_y + 22}" x2="{legend_x + 28}" y2="{legend_y + 22}" '
        f'stroke="#c0392b" stroke-width="2.4"/>'
    )
    svg.append(
        f'<text x="{legend_x + 36}" y="{legend_y + 26}" font-family="{font}" font-size="12" '
        f'fill="#333333">{esc("SA м/м ЦБ")}</text>'
    )

    note = ""
    if nowcast:
        month_name = month_label(nowcast["date"]).capitalize()
        note = f"{month_name} — неполный месяц (nowcast)."
        if march_weekly is not None and march_cbr is not None:
            values = f"{decimal(march_weekly, 2)} и {decimal(march_cbr, 2)}"
            note = (
                f"{month_name} — неполный месяц (nowcast); март 2022 ({values}) "
                f"выходит за верхнюю границу шкалы."
            )
    elif march_weekly is not None and march_cbr is not None:
        values = f"{decimal(march_weekly, 2)} и {decimal(march_cbr, 2)}"
        note = f"Март 2022 ({values}) выходит за верхнюю границу шкалы."
    if note:
        svg.append(
            f'<text x="{left}" y="{bottom + 42}" font-family="{font}" font-size="11" fill="#777777">'
            f"{esc(note)}</text>"
        )
    svg.append(
        f'<text x="{left}" y="{height - 16}" font-family="{font}" font-size="11" fill="#777777">'
        f"{esc(SVG_SOURCE)}</text>"
    )
    svg.append("</svg>")
    return "\n".join(svg) + "\n"


def fetch_weekly(start: str, as_of: str | None) -> list[dict]:
    where = [
        f"series = {ch.sql_literal(WEEKLY_SERIES)}",
        f"date >= {ch.sql_literal(start)}",
    ]
    if as_of:
        where.append(f"date <= {ch.sql_literal(as_of)}")
    sql = (
        "SELECT date, value, partial, as_of FROM v_weekly_cpi_sa WHERE "
        + " AND ".join(where)
        + " ORDER BY date\nFORMAT JSON"
    )
    return ch.ch_query(sql)


def fetch_cbr(start: str, as_of: str | None) -> list[dict]:
    where = [
        f"series = {ch.sql_literal(CBR_SERIES)}",
        f"date >= {ch.sql_literal(start)}",
    ]
    if as_of:
        where.append(f"date <= {ch.sql_literal(as_of)}")
    sql = (
        "SELECT date, value FROM v_cbr_macro WHERE "
        + " AND ".join(where)
        + " ORDER BY date\nFORMAT JSON"
    )
    return ch.ch_query(sql)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--publication",
        default=DEFAULT_PUBLICATION,
        help=f"каталог публикации в docs/publications (по умолчанию {DEFAULT_PUBLICATION})",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="каталог артефактов; по умолчанию docs/publications/<publication>",
    )
    parser.add_argument(
        "--start",
        default="2021-01-01",
        help="начало окна графика (по умолчанию 2021-01-01)",
    )
    parser.add_argument(
        "--as-of",
        default=None,
        help="брать точки не позже этой даты (YYYY-MM-DD); нужно для воспроизведения",
    )
    args = parser.parse_args(argv)

    out_dir = Path(args.out) if args.out else Path("docs/publications") / args.publication
    data = build_chart_data(
        fetch_weekly(args.start, args.as_of),
        fetch_cbr(args.start, args.as_of),
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    chart_path = out_dir / "chart-data.json"
    chart_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    svg_path = out_dir / "weekly-sa-mm-vs-cbr.svg"
    svg_path.write_text(render_svg(data), encoding="utf-8")
    print("written", chart_path, svg_path)


if __name__ == "__main__":
    main()
