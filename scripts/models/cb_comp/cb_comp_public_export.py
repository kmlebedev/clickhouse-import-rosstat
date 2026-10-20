#!/usr/bin/env python3
"""Экспорт live-панели публичной книги ЦБ для режима `per_sheet`.

Пишет два CSV в `build/cb-comp-parity/` (gitignored):

* `public_panel.csv` — `v_cbr_mp_public_panel`: 87 кодов × 3 индикатора ×
  297 месяцев; значения внутри листа совпадают, режим `per_sheet` сворачивает
  их в 11 листов × 3 индикатора;
* `sheet_map.csv` — `v_cbr_mp_bsu_map`: `код ОКВЭД2 → industry_slug`.

Скрипт только читает ClickHouse через общий транспорт `ch_query`; секреты
берутся из env, как у остальных ML-скриптов. Запуск — `make
cb-comp-public-export` (Makefile подключает `~/.config/rosstat/env`).
"""

import argparse
import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))

from lib import clickhouse as ch

PUBLIC_SQL = """
SELECT code, indicator, toString(date) AS date, value
FROM v_cbr_mp_public_panel
ORDER BY code, indicator, date
FORMAT JSON
"""

SHEET_MAP_SQL = """
SELECT code, industry_slug
FROM v_cbr_mp_bsu_map
ORDER BY code
FORMAT JSON
"""

PUBLIC_FIELDS = ("code", "indicator", "date", "value")
SHEET_MAP_FIELDS = ("code", "industry_slug")


def write_csv(path: Path, rows: list, fields: tuple) -> None:
    """Пишет строки `ch_query` в CSV с фиксированным порядком колонок."""
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fields))
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row[field] for field in fields})


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Экспорт v_cbr_mp_public_panel и v_cbr_mp_bsu_map в CSV"
    )
    parser.add_argument(
        "--out-dir",
        default=str(ROOT / "build" / "cb-comp-parity"),
        help="каталог CSV (по умолчанию build/cb-comp-parity)",
    )
    arguments = parser.parse_args(argv)
    out_dir = Path(arguments.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    public = ch.ch_query(PUBLIC_SQL)
    sheet_map = ch.ch_query(SHEET_MAP_SQL)
    if not public:
        raise SystemExit("v_cbr_mp_public_panel: пустой ответ ClickHouse")
    if not sheet_map:
        raise SystemExit("v_cbr_mp_bsu_map: пустой ответ ClickHouse")

    public_path = out_dir / "public_panel.csv"
    sheet_path = out_dir / "sheet_map.csv"
    write_csv(public_path, public, PUBLIC_FIELDS)
    write_csv(sheet_path, sheet_map, SHEET_MAP_FIELDS)
    print(
        f"{public_path}: {len(public)} rows; "
        f"{sheet_path}: {len(sheet_map)} rows",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
