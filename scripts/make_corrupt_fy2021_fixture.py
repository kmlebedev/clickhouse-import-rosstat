#!/usr/bin/env python3
"""Создаёт намеренно испорченную фикстуру `polyus_pr_4q_fy2021_corrupt.pdf`.

Зачем она нужна
---------------
`build_series` в `extrapolation_aisc.py` сверяет год, который в одном отчёте
назван отчётным, со тем же годом, процитированным как сравнительный в СЛЕДУЮЩЕМ
отчёте. Единственное такое пересечение в фикстурах 2018–2021 — цитата
прошлогоднего годового TCC Наталки внутри годового абзаца TCC:

* FY2019: «… TCC at Natalka declined to $396 per ounce, compared to $747 per
  ounce in 2018» — 747 совпадает с годовым TCC Наталки в FY2018;
* FY2020: «… TCC at Natalka declined to $355 per ounce, down 10% compared to $396
  per ounce in 2019» — 396 совпадает с годовым TCC Наталки в FY2019;
* FY2021: «… TCC at Natalka increased to $368 per ounce, up 4% compared to $355
  per ounce in 2020» — 355 совпадает с годовым TCC Наталки в FY2020.

Испорченная фикстура портит ИМЕННО цитату в FY2021: `$355 … in 2020` →
`$365 … in 2020`. Тогда цитируемый TCC Наталки за 2020 (365) расходится с
собственным годовым значением FY2020 (355), и `cross_check_overlap` обязан
поднять `CrossCheckError`. Ничего другого в документе не меняется.

**Фикстура НИКОГДА не должна использоваться для калибровки.** Именно поэтому
`build_series` молча пропускает любую фикстуру с суффиксом `_corrupt.pdf`,
если она не передана явно: испорченное число не может просочиться в наблюдения
по недосмотру.

Как это сделано
---------------
Правка идёт на уровне сжатого потока содержимого страницы (FlateDecode):

1. находится объект потока (`14 0 obj`) и границы его данных по `/Length`;
2. поток распаковывается через `zlib` — получается исходный PDF-контент;
3. в контенте заменяется ровно одна текстовая строка-оператор TJ, несущая
   фразу про цитату (`…$355 per ounce in 2020…`) — цифра `5` в позиции
   сотен заменяется на `6` прямо в глифовой строке; длина оператора не
   меняется, поэтому раскладка и метрики текста не сдвигаются;
4. контент сжимается тем же `zlib` и подставляется на место старого; `/Length`
   пересчитывается. Правка меняет смещения байтов ВСЕХ объектов после
   правленого, поэтому xref-таблица стала бы невалидной при смене длины файла:
   скрипт правит только байты внутри объекта и НЕ меняет длину файла — новый
   сжатый поток дополняется нулевыми байтами до исходной длины.

Проверка воспроизводимости обязательна: скрипт сам запускает `pdftotext -layout`
на результате и печатает строку с цитатой, чтобы факт порчи был виден глазами.

Запуск (из корня репозитория):

    python3 scripts/make_corrupt_fy2021_fixture.py

Скрипт идемпотентен: перезапуск перезаписывает фикстуру из чистого исходника.
"""

import re
import subprocess
import sys
import zlib
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
FIXTURE_DIR = SCRIPT_DIR / "testdata"
SOURCE_PDF = FIXTURE_DIR / "polyus_pr_4q_fy2021.pdf"
CORRUPT_PDF = FIXTURE_DIR / "polyus_pr_4q_fy2021_corrupt.pdf"

# Текстовая строка-оператор, несущая цитату прошлого года. Берётся целиком,
# чтобы правка не задела другие вхождения '$355' (например, число в диаграмме).
TARGET_OPERATOR = (
    b"(Nat)-3(a)-3(lka )-53(incre)-2(a)-3(s)10(e)-3(d)-3( )-51(t)8(o)-3( )-51($)6"
    b"(3)-3(6)-3(8)-3( )-51(p)6(e)-3(r )-48(o)-3(u)-3(n)-3(c)10(e)-3(, )-53(u)6"
    b"(p)-3( )-51(4)-3(%)9( )-51(co)-3(m)-6(p)6(a)-3(red)-4( )-51(t)8(o)-3( )-51($)"
    b"-3(3)6(5)-3(5)-3( )-51(p)6(e)6(r )-48(o)-3(u)-3(n)-3(ce)-3( )-51(in )-53(2)6"
    b"(0)-3(2)6(0)-3(,)8( )"
)

# Та же строка с испорченной цитатой: '$365' вместо '$355' (глиф '5' → '6').
CORRUPT_OPERATOR = TARGET_OPERATOR.replace(b"($)-3(3)6(5)-3(5)", b"($)-3(3)6(6)-3(5)")

STREAM_OBJ = b"14 0 obj"
STREAM_END = b"endstream"


def _stream_bounds(data: bytes) -> tuple[int, int]:
    """Возвращает (начало, конец) данных потока объекта `STREAM_OBJ`.

    Границы берутся по объявленному `/Length`, а не по ближайшему `endstream`:
    иначе в тело попали бы завершающие переводы строк, и распаковка сломалась бы.
    """
    obj = data.find(STREAM_OBJ)
    if obj < 0:
        raise SystemExit(f"не найден объект {STREAM_OBJ!r} в {SOURCE_PDF}")
    header = data[obj:data.find(b"stream", obj)]
    match = re.search(rb"/Length\s+(\d+)", header)
    if not match:
        raise SystemExit(f"у объекта {STREAM_OBJ!r} нет /Length")
    length = int(match.group(1))
    start = data.find(b"stream", obj) + len(b"stream")
    if data[start:start + 2] == b"\r\n":
        start += 2
    elif data[start:start + 1] in (b"\n", b"\r"):
        start += 1
    return start, start + length


def build_corrupt_pdf() -> None:
    data = SOURCE_PDF.read_bytes()
    start, end = _stream_bounds(data)
    packed = data[start:end]
    content = zlib.decompress(packed)

    if content.count(TARGET_OPERATOR) != 1:
        raise SystemExit(
            "целевой текстовый оператор не найден однозначно "
            f"(вхождений: {content.count(TARGET_OPERATOR)}) — PDF изменился?"
        )
    patched = content.replace(TARGET_OPERATOR, CORRUPT_OPERATOR)
    if len(patched) != len(content):
        raise SystemExit("правка изменила длину контента — так нельзя")

    repacked = zlib.compress(patched, 9)
    if len(repacked) > len(packed):
        raise SystemExit(
            f"пересжатый поток длиннее исходного ({len(repacked)} > {len(packed)}); "
            "выравнивание нулями невозможно"
        )
    # Общий размер объекта сохраняется: поток дополняется нулями до исходной
    # длины, поэтому все байтовые смещения в xref остаются валидными.
    padded = repacked + b"\x00" * (len(packed) - len(repacked))
    result = data[:start] + padded + data[end:]
    assert len(result) == len(data)

    CORRUPT_PDF.write_bytes(result)


def _extracted_text(path: Path) -> str:
    raw = subprocess.run(
        ["pdftotext", "-layout", str(path), "-"],
        capture_output=True, text=True, check=True,
    ).stdout
    # Пресс-релиз переносит предложения по строкам, поэтому перед поиском
    # фразы строки склеиваются в одну.
    return re.sub(r"\s+", " ", raw)


def verify() -> None:
    citation = re.search(
        r"In 2021, TCC at Natalka increased to \$\d+ per ounce[^.]*",
        _extracted_text(CORRUPT_PDF),
    )
    if not citation:
        raise SystemExit("в испорченной фикстуре не найдена цитата TCC Наталки")
    print("цитата TCC Наталки в испорченной фикстуре:")
    print("  " + citation.group(0))


def main() -> int:
    if not SOURCE_PDF.exists():
        print(f"нет исходной фикстуры {SOURCE_PDF}", file=sys.stderr)
        return 1
    build_corrupt_pdf()
    print(f"создано: {CORRUPT_PDF.relative_to(SCRIPT_DIR.parent)} "
          f"({CORRUPT_PDF.stat().st_size} байт)")
    verify()
    return 0


if __name__ == "__main__":
    sys.exit(main())
