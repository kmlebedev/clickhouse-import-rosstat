"""Календарь публикаций: point-in-time доступность рядов Росстата и ЦБ.

Модуль отвечает на один вопрос: какая точка ряда была опубликована к моменту
`as_of` (обычно — время заседания ЦБ). Источник точных дат — версионированный
CSV (в эксперименте `scripts/testdata/publication_calendar.csv`, в production —
таблица/витрина `publication_calendar`, отдельный Go/DDL-пункт). Если точной
даты нет, для рядов с установленным правилом применяется консервативное
правило, и точка явно помечается `precision="fallback"`; молчаливых «доступна
по умолчанию» точек нет. Для рядов мониторинга предприятий ЦБ фиксированный
лаг запрещён (спека §3.2): им нужна точная дата, иначе `ValueError`.

Все даты и времена — наивные, московское время: сравнение
`published_at <= as_of` прямое и не зависит от часового пояса машины.
Tz-aware `datetime` отклоняется с `ValueError`, чтобы не смешивать пояса.

Модуль ничего не читает из ClickHouse, не пишет в него и не делает сетевых
вызовов. Экспериментальная граница: CSV — fallback на время эксперимента,
production-путь — таблица.
"""

import csv
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Callable, Iterable, Literal, cast

# Сколько месяцев назад `latest_published_month` ищет опубликованную точку.
# Двадцать лет с запасом покрывают окна бэктестов репозитория (с 2013 г.);
# ограничение нужно только для вырожденного `as_of`, чтобы скан завершался.
_MAX_MONTH_LOOKBACK = 240

# Точность даты публикации: `exact` — из календаря источника, `fallback` —
# вычислена по консервативному правилу.
Precision = Literal["exact", "fallback"]
_PRECISIONS: tuple[Precision, ...] = ("exact", "fallback")

# Недельные ряды ИПЦ: точка недели — понедельник, в БД доступна на `+2` дня.
_WEEKLY_SERIES_PREFIX = "cpi_weekly"

# SA/тренд ЦБ: публикуются в одном окне через 5–6 рабочих дней после ИПЦ.
_CBR_SA_SERIES = frozenset({"cpi_sa", "cpi_trend", "cpi_trend_dev"})


def _require_naive(value: datetime, name: str) -> None:
    """Календарь работает только с наивным московским временем."""
    if value.utcoffset() is not None:
        raise ValueError(
            f"{name}: ожидается наивное московское время без tzinfo, "
            f"получено {value!r}"
        )


@dataclass(frozen=True)
class CalendarPoint:
    """Точка календаря: когда опубликовано значение ряда за период.

    `precision` — `exact` для даты из календаря источника, `fallback` для
    вычисленной по правилу. `period` — период ряда в терминах вызывающего кода
    (для месячных рядов допускаются и первое, и последнее число месяца).
    `published_at` — наивное московское время; tz-aware значение отклоняется.
    """

    source: str
    series: str
    period: date
    published_at: datetime
    precision: Precision

    def __post_init__(self) -> None:
        _require_naive(self.published_at, "published_at")


def _is_weekly_series(series: str) -> bool:
    """Недельный ли ряд: только у него период — конкретная дата, а не месяц."""
    return series == _WEEKLY_SERIES_PREFIX or series.startswith(
        _WEEKLY_SERIES_PREFIX + "_"
    )


def _is_cbr_sa_series(series: str) -> bool:
    return (
        series in _CBR_SA_SERIES
        or series.startswith("cpi_sa_")
        or series.startswith("cpi_trend_")
    )


def _first_of_month(day: date) -> date:
    return date(day.year, day.month, 1)


def _shift_month(month_start: date, delta: int) -> date:
    """Сдвигает первое число месяца на `delta` месяцев."""
    index = month_start.year * 12 + (month_start.month - 1) + delta
    year, month_number = divmod(index, 12)
    return date(year, month_number + 1, 1)


def _month_end(month_start: date) -> date:
    return _shift_month(month_start, 1) - timedelta(days=1)


def _nth_of_next_month(period: date, day: int, hour: int, minute: int = 0) -> datetime:
    next_month = _shift_month(_first_of_month(period), 1)
    return datetime(next_month.year, next_month.month, day, hour, minute)


def _monthly_cpi_rule(period: date) -> datetime:
    """Месячный ИПЦ Росстата: 9-е число M+1, 19:00 МСК."""
    return _nth_of_next_month(period, day=9, hour=19)


def _weekly_cpi_rule(period: date) -> datetime:
    """Недельный ИПЦ: точка понедельника + 2 дня (среда), 19:00 МСК."""
    return datetime.combine(period + timedelta(days=2), time(19, 0))


def _cbr_sa_rule(period: date) -> datetime:
    """SA/тренд ЦБ: 18-е число M+1, 15:00 МСК."""
    return _nth_of_next_month(period, day=18, hour=15)


def _cbr_infl_exp_rule(period: date) -> datetime:
    """Инфляционные ожидания населения: 21-е число того же месяца, 15:00 МСК."""
    return datetime(period.year, period.month, 21, 15, 0)


# Fallback-правило — callable: дата публикации вычисляется от периода точки.
_ReleaseRule = Callable[[date], datetime]


def _fallback_rule(source: str, series: str) -> _ReleaseRule | None:
    """Fallback-правило пары source/series или `None`, если его нет.

    Ряды мониторинга предприятий ЦБ правила не имеют: спека §3.2 запрещает
    для них фиксированный лаг, поэтому им нужна точная дата в календаре.
    """
    if source == "rosstat":
        if _is_weekly_series(series):
            return _weekly_cpi_rule
        if series.startswith("cpi_"):
            return _monthly_cpi_rule
    elif source == "cbr":
        if series.startswith("infl_exp"):
            return _cbr_infl_exp_rule
        if _is_cbr_sa_series(series):
            return _cbr_sa_rule
    return None


def _release_rule(source: str, series: str) -> _ReleaseRule:
    """Fallback-правило пары source/series; без правила — `ValueError`.

    Неизвестная пара — ошибка, а не догадка: неверная дата публикации даёт
    look-ahead, который невозможно заметить по результату модели.
    """
    rule = _fallback_rule(source, series)
    if rule is None:
        raise ValueError(
            f"нет правила публикации для source={source!r}, series={series!r}: "
            "добавьте точную дату в календарь или правило в publication_calendar.py"
        )
    return rule


def _parse_row(row: dict, path: Path, line_number: int) -> CalendarPoint:
    fields = ("source", "series", "period", "published_at", "precision")
    values = {name: (row.get(name) or "").strip() for name in fields}
    empty = [name for name, value in values.items() if not value]
    if empty:
        raise ValueError(f"{path}:{line_number}: пустые поля: {', '.join(empty)}")
    precision = values["precision"]
    if precision not in _PRECISIONS:
        raise ValueError(
            f"{path}:{line_number}: precision={precision!r}, "
            f"ожидалось одно из {_PRECISIONS}"
        )
    try:
        period = date.fromisoformat(values["period"])
        published_at = datetime.fromisoformat(values["published_at"])
    except ValueError as error:
        raise ValueError(f"{path}:{line_number}: {error}") from error
    if published_at.utcoffset() is not None:
        raise ValueError(
            f"{path}:{line_number}: published_at должен быть наивным "
            f"московским временем, получено {values['published_at']!r}"
        )
    return CalendarPoint(
        source=values["source"],
        series=values["series"],
        period=period,
        published_at=published_at,
        precision=cast(Precision, precision),
    )


class PublicationCalendar:
    """Календарь публикаций: точные точки из CSV плюс fallback-правила.

    Ключ точной точки — `(source, series, period)`. Для месячных рядов период
    дополнительно нормализуется до месяца, поэтому `2026-09-01` и `2026-09-30`
    находят одну и ту же точку: в разных источниках дата периода записана
    первым или последним числом месяца.

    Ряды без fallback-правила (например, мониторинг предприятий ЦБ) ищутся
    только среди точных точек; иначе `ValueError`.
    """

    def __init__(self, points: Iterable[CalendarPoint] = ()):
        self._exact: dict[tuple[str, str, date], CalendarPoint] = {}
        self._by_month: dict[tuple[str, str, int, int], CalendarPoint] = {}
        for point in points:
            self._add(point)

    @classmethod
    def from_csv(cls, path: str | Path) -> "PublicationCalendar":
        """Загружает календарь из CSV `source,series,period,published_at,precision`."""
        path = Path(path)
        calendar = cls()
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            required = {"source", "series", "period", "published_at", "precision"}
            header = set(reader.fieldnames or ())
            missing = required - header
            if missing:
                raise ValueError(
                    f"{path}: нет колонок: {', '.join(sorted(missing))}"
                )
            for line_number, row in enumerate(reader, start=2):
                calendar._add(_parse_row(row, path, line_number))
        return calendar

    def _add(self, point: CalendarPoint) -> None:
        key = (point.source, point.series, point.period)
        if key in self._exact:
            raise ValueError(f"дубль точки календаря: {key}")
        self._exact[key] = point
        if _is_weekly_series(point.series):
            return
        month_key = (point.source, point.series, point.period.year, point.period.month)
        if month_key in self._by_month:
            raise ValueError(
                "дубль месячной точки календаря: "
                f"source={point.source!r}, series={point.series!r}, "
                f"{point.period.year}-{point.period.month:02d}"
            )
        self._by_month[month_key] = point

    def lookup(self, source: str, series: str, period: date) -> CalendarPoint:
        """Точка ряда за период: точная из календаря или fallback-правило.

        `precision` результата показывает, была ли дата публикации известна
        (`exact`) или вычислена (`fallback`).
        """
        point = self._exact.get((source, series, period))
        if point is not None:
            return point
        if not _is_weekly_series(series):
            point = self._by_month.get((source, series, period.year, period.month))
            if point is not None:
                return point
        rule = _release_rule(source, series)
        return CalendarPoint(
            source=source,
            series=series,
            period=period,
            published_at=rule(period),
            precision="fallback",
        )

    def published_at(self, source: str, series: str, period: date) -> datetime:
        """Момент публикации точки (точный или fallback)."""
        return self.lookup(source, series, period).published_at

    def is_available(
        self, source: str, series: str, period: date, as_of: datetime
    ) -> bool:
        """Опубликована ли точка к `as_of`; граница включительная.

        `as_of` — наивное московское время, как и `published_at`.
        """
        _require_naive(as_of, "as_of")
        return self.published_at(source, series, period) <= as_of

    def _latest_exact_month(
        self, source: str, series: str, as_of: datetime
    ) -> date | None:
        """Последний опубликованный месяц среди точных точек ряда."""
        latest: date | None = None
        for (point_source, point_series, year, month), point in self._by_month.items():
            if (point_source, point_series) != (source, series):
                continue
            if point.published_at > as_of:
                continue
            month_start = date(year, month, 1)
            if latest is None or month_start > latest:
                latest = month_start
        return latest

    def latest_published_month(
        self, source: str, series: str, as_of: datetime
    ) -> date | None:
        """Последний опубликованный месяц ряда (первое число) или `None`.

        Скан идёт от месяца `as_of` назад: точка текущего месяца проверяется
        первой, потому что ожидания ЦБ публикуются в том же месяце. Ряды без
        fallback-правила (мониторинг предприятий ЦБ) сканируются только по
        точным точкам; если их нет — `None`. `as_of` — наивное московское
        время.
        """
        _require_naive(as_of, "as_of")
        if _is_weekly_series(series):
            raise ValueError(
                f"latest_published_month: ряд {series!r} недельный, месяц не определён"
            )
        if _fallback_rule(source, series) is None:
            return self._latest_exact_month(source, series, as_of)
        month = _first_of_month(as_of.date())
        for _ in range(_MAX_MONTH_LOOKBACK):
            if self.is_available(source, series, _month_end(month), as_of):
                return month
            if month.year == 1 and month.month == 1:
                break
            month = _shift_month(month, -1)
        return None

    def next_unpublished_month(
        self, source: str, series: str, as_of: datetime
    ) -> date:
        """Первый неопубликованный месяц ряда (первое число).

        Если опубликованных месяцев не нашлось, это месяц `as_of`. `as_of` —
        наивное московское время.
        """
        _require_naive(as_of, "as_of")
        latest = self.latest_published_month(source, series, as_of)
        if latest is None:
            return _first_of_month(as_of.date())
        return _shift_month(latest, 1)
