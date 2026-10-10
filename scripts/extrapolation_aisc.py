#!/usr/bin/env python3
"""Калибрует поактивную структуру затрат Полюса и экстраполирует её на LOM.

Сегодня поактивный AISC выводится единым клином `TCC + 698 USD/oz`, одинаковым
для всех рудников и плоским на весь срок жизни. Скрипт заменяет этот клин
структурой, откалиброванной по фактически опубликованным поактивным данным:

* клин считается долей от TCC: `r_i,t = (AISC_i,t − TCC_i,t) / TCC_i,t`;
* доля усредняется по калибровочным годам и переносится на 2025–LOM;
* AISC увязывается с групповым целевым значением одним скалярным
  множителем, который правит клин, но никогда не трогает наблюдённый TCC.

**Скрипт ничего не пишет в ClickHouse** — только читает (факт добычи из
`databook_polyus`). Вставка результата выполняется отдельно, через `make`.

Запуск — только через `make` (переменные окружения подключаются Makefile,
а не читаются скриптом из `~/.config/rosstat/env`).

**Калибровочные годы — 2019–2021.** 2018 год исключён намеренно: в FY2018
Полюс не раскрывает годовой поактивный AISC по Вернинскому, и включение
этого года дало бы неполный вектор наблюдений.
"""

import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Канонические имена активов: совпадают с `databook_polyus.table` и
# `dcf/mine_plans_seed.go` дословно.
ASSETS = ["OLIMPIADA", "BLAGODATNOYE", "NATALKA", "VERNINSKOYE2", "KURANAKH"]

# Годы наблюдённого поактивного AISC/TCC. 2018 исключён: в FY2018 нет
# годового поактивного AISC по Вернинскому.
CALIBRATION_YEARS = (2019, 2020, 2021)

# Первый год планового горизонта.
PLAN_BASE_YEAR = 2025

# Сроки жизни рудников, как в `dcf/mine_plans_seed.go`. Сумма = 77 строк плана.
MINE_LIFE_YEARS = {
    "OLIMPIADA": 10,
    "BLAGODATNOYE": 13,
    "NATALKA": 24,
    "VERNINSKOYE2": 15,
    "KURANAKH": 15,
}

# Групповые ориентиры 2025 года, USD/oz: опубликованный AISC группы и
# политика TCC. Служат якорем увязки для плановых лет.
GROUP_AISC_2025_USD_PER_OZ = 1437.0
GROUP_TCC_2025_USD_PER_OZ = 739.0

# PDF-фикстуры пресс-релизов 4Q/FY 2018–2021 и артефакты вывода.
FIXTURE_DIR = ROOT / "scripts/testdata"
OUTPUT_SQL = ROOT / "sql/dcf_mine_plans_seed.sql"
OUTPUT_CSV = ROOT / "polyus_lom_reconciled_long.csv"


# Строка, состоящая только из пробелов и цифр, — это номер страницы,
# попавший в поток текста (иногда прямо в середину предложения).
PAGE_NUMBER_LINE = re.compile(r"^[\s\d]+$")

# Любая последовательность пробельных символов, включая неразрывный пробел.
WHITESPACE_RUN = re.compile(r"\s+", re.UNICODE)

# Разделители разрядов в суммах: запятая и пробел (в т.ч. неразрывный).
AMOUNT_SEPARATORS = re.compile(r"[,\s\u00a0\u202f]")


def pdf_to_text(path: Path) -> str:
    """Извлекает текст PDF через `pdftotext -layout`.

    `-layout` сохраняет табличную раскладку пресс-релиза, по которой дальше
    восстанавливаются колонки по активу и году. Если `pdftotext` не
    установлен, это отсутствующая системная предпосылка, а не повод
    подставлять пустой текст: падаем с внятным сообщением.
    """
    try:
        result = subprocess.run(
            ["pdftotext", "-layout", str(path), "-"],
            capture_output=True,
            text=True,
            check=True,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            "не найден pdftotext: установите poppler (предпосылка для разбора PDF)"
        ) from exc
    return result.stdout


def normalize(text: str) -> str:
    """Приводит сырой вывод `pdftotext` к однострочному виду.

    Полюс переносит предложения по строкам и вставляет номера страниц внутрь
    фразы ('AISC at\\n\\n6\\n\\nBlagodatnoye increased'), поэтому строки-номера
    страниц удаляются, оставшиеся строки склеиваются одним пробелом, а затем
    схлопываются любые пробельные серии — включая неразрывный пробел в суммах
    вида '$1 277'.
    """
    kept = [line for line in text.splitlines() if not PAGE_NUMBER_LINE.match(line)]
    return WHITESPACE_RUN.sub(" ", " ".join(kept)).strip()


def parse_amount(raw: str) -> float:
    """Преобразует сумму из текста пресс-релиза в число.

    Разделитель разрядов встречается и запятой ('$1,277'), и пробелом
    ('$1 277', в том числе неразрывным), поэтому удаляются оба.
    """
    return float(AMOUNT_SEPARATORS.sub("", raw))


def ch_config() -> dict:
    """Параметры подключения к ClickHouse из переменных окружения.

    Секреты приходят только из env: скрипт не читает `~/.config/rosstat/env`
    сам, это делает Makefile. `CH_HTTP_PORT` — локальный default для
    HTTP-интерфейса.
    """
    return {
        "host": os.environ.get("CLICKHOUSE_HOST", "localhost"),
        "port": os.environ.get("CLICKHOUSE_PORT", "9000"),
        "user": os.environ.get("CLICKHOUSE_USER", ""),
        "password": os.environ.get("CLICKHOUSE_PASSWORD", ""),
        "database": os.environ.get("CLICKHOUSE_DATABASE", "default"),
        "http_port": os.environ.get("CH_HTTP_PORT", "8123"),
    }


def mask(secret: str, text: str) -> str:
    """Убирает секрет из текста сообщения.

    Слепой replace портит слова, содержащие короткий пароль ('x' ломает
    'Exception'), поэтому заменяются только вхождения, не окружённые
    буквами и цифрами.
    """
    if not secret:
        return text
    return re.sub(rf"(?<![0-9A-Za-z]){re.escape(secret)}(?![0-9A-Za-z])", "***", text)


def main() -> int:
    """Печатает краткую сводку по конфигурации и границам расчёта."""
    config = ch_config()
    host = mask(config["password"], config["host"])
    assets = ", ".join(ASSETS)
    rows = sum(MINE_LIFE_YEARS.values())
    print("Экстраполяция поактивного AISC Полюса")
    print(f"  целевая база: ClickHouse {host}:{config['http_port']}, БД {config['database']}")
    print(f"  активы ({len(ASSETS)}): {assets}")
    print(f"  калибровочные годы: {', '.join(str(y) for y in CALIBRATION_YEARS)}")
    print(f"  плановый горизонт: {PLAN_BASE_YEAR}+, строк плана: {rows}")
    print(f"  групповой ориентир 2025: AISC {GROUP_AISC_2025_USD_PER_OZ} USD/oz, "
          f"TCC {GROUP_TCC_2025_USD_PER_OZ} USD/oz")
    print(f"  фикстуры: {FIXTURE_DIR.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
