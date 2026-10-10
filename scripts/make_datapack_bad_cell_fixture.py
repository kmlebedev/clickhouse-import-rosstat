#!/usr/bin/env python3
"""Пересобирает polyus/testdata/datapack_bad_cell.xlsx из эталонного датапака.

Фикстура нужна тесту TestParseDatapackToleratesBadCell: в эталонном файле
ячейка F13 (Total rock moved / ANNUAL 2007, значение 49474) заменяется на
текстовую "n/a". Разбор такой файл не должен падать — битая ячейка
считается в stats.Skipped, остальные точки сохраняются.

Запуск из корня репозитория:
    python3 scripts/make_datapack_bad_cell_fixture.py
"""

import re
import shutil
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "polyus/testdata/polyus_datapack_fy2025_new.xlsx"
DST = ROOT / "polyus/testdata/datapack_bad_cell.xlsx"
SHEET = "xl/worksheets/sheet1.xml"
CELL = "F13"
BAD_VALUE = "n/a"


def main() -> None:
    shutil.copyfile(SRC, DST)
    with zipfile.ZipFile(SRC) as zin:
        sheet = zin.read(SHEET).decode()

        pattern = r'(<c r="%s"[^>]*>)<v>[^<]*</v>' % CELL
        sheet, replaced = re.subn(pattern, r"\1<v>%s</v>" % BAD_VALUE, sheet, count=1)
        if replaced != 1:
            raise SystemExit(f"cell {CELL} not found in {SHEET}")

        with zipfile.ZipFile(DST, "w", zipfile.ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                data = sheet.encode() if item.filename == SHEET else zin.read(item.filename)
                zout.writestr(item, data)

    print(f"wrote {DST.relative_to(ROOT)} ({CELL} = {BAD_VALUE!r})")


if __name__ == "__main__":
    main()
