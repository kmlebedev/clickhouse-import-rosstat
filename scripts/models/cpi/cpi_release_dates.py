#!/usr/bin/env python3
"""Фактические даты публикации месячного ИПЦ Росстата против правила «9-е M+1».

#401 W0. Источник — график срочных информаций Росстата
(https://rosstat.gov.ru/compendium/document/50798, «Об индексе потребительских
цен в <месяц> <год> года»); таблица — `scripts/testdata/rosstat_cpi_release_dates.csv`
(период, дата размещения, источник: имя файла релиза `N_DD-MM-YYYY.html`
на rosstat.gov.ru, где дата проверяется по имени, либо `wayback:…` — таблица
графика срочных информаций из архивных снимков страницы; время суток не
указано и не записывается). Охват: декабрь 2019 … сентябрь 2026. Скрипт только читает CSV.
"""

import csv
import re
import statistics
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PATH = ROOT / "scripts" / "testdata" / "rosstat_cpi_release_dates.csv"
RULE_DAY = 9  # `publication_calendar`/`cpi_monthly.PUBLICATION_DAY`
_FILE_DATE = re.compile(r"_(\d{2})-(\d{2})-(\d{4})\.")
WAYBACK_PREFIX = "wayback:"  # дата из таблицы графика (архивные снимки страницы), без файла релиза


def rule_date(period: date) -> date:
    """Дата публикации по правилу: 9-е число месяца, следующего за `period`."""
    index = period.year * 12 + (period.month - 1) + 1
    return date(index // 12, index % 12 + 1, RULE_DAY)


def load_release_dates(path: Path = DEFAULT_PATH) -> dict:
    """`{первое число отчётного месяца: дата публикации}` с проверкой целостности."""
    out = {}
    with Path(path).open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if set(reader.fieldnames or ()) != {"period", "published_on", "source_file"}:
            raise ValueError(f"{path}: ожидаются колонки period,published_on,source_file")
        for line, row in enumerate(reader, start=2):
            year, month = (int(part) for part in row["period"].split("-"))
            period = date(year, month, 1)
            published = date.fromisoformat(row["published_on"])
            if period in out:
                raise ValueError(f"{path}:{line}: дубль периода {row['period']}")
            source = row["source_file"]
            match = _FILE_DATE.search(source)
            if match is not None:
                if date(int(match[3]), int(match[2]), int(match[1])) != published:
                    raise ValueError(f"{path}:{line}: дата в имени файла не совпадает с published_on")
            elif not source.startswith(WAYBACK_PREFIX):
                raise ValueError(f"{path}:{line}: источник {source!r} — ни файл релиза, ни {WAYBACK_PREFIX}")
            if published <= date(year + month // 12, month % 12 + 1, 1):
                raise ValueError(f"{path}:{line}: публикация {published} не позже конца периода {period}")
            out[period] = published
    return out


def compare_with_rule(dates: dict) -> list:
    """По каждому месяцу: факт, правило и разность в днях (факт − правило)."""
    return [
        {
            "period": period,
            "actual": dates[period],
            "rule": rule_date(period),
            "diff_days": (dates[period] - rule_date(period)).days,
        }
        for period in sorted(dates)
    ]


def summarize(rows: list) -> dict:
    diffs = [row["diff_days"] for row in rows]
    return {
        "n": len(diffs),
        "later_than_rule": sum(1 for d in diffs if d > 0),
        "earlier_than_rule": sum(1 for d in diffs if d < 0),
        "max_diff": max(diffs),
        "min_diff": min(diffs),
        "mean_diff": statistics.mean(diffs),
        "median_diff": statistics.median(diffs),
    }


def main() -> int:
    rows = compare_with_rule(load_release_dates())
    for row in rows:
        print(f"{row['period']:%Y-%m}  факт {row['actual']}  правило {row['rule']}  {row['diff_days']:+d}")
    print(summarize(rows))
    return 0


if __name__ == "__main__":
    sys.exit(main())
