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

# --- Разбор годовой прозы поактивного AISC/TCC -------------------------------

# Канонизация имён рудников. В пресс-релизах Вернинское встречается с опечаткой
# «Verniskoye» (подтверждено в FY2018 и FY2020–2021), поэтому обе формы ведут к
# `VERNINSKOYE2`, как он назван в `databook_polyus.table`.
ASSET_CANON = {
    "Olimpiada": "OLIMPIADA",
    "Blagodatnoye": "BLAGODATNOYE",
    "Natalka": "NATALKA",
    "Verninskoye": "VERNINSKOYE2",
    "Verniskoye": "VERNINSKOYE2",
    "Kuranakh": "KURANAKH",
}

# Непроизводственные объекты, встречающиеся в той же прозе. Это не рудники
# пятёрки, поэтому предложения с ними пропускаются молча, а не считаются ошибкой.
SKIP_NAMES = {"Alluvial", "Alluvials", "Irkutsk business unit"}

# Имя актива в прозе — одно слово с заглавной буквы. Многословный
# «Irkutsk business unit» из `SKIP_NAMES` называется буквально: шаблон имени не
# должен жадно тянуть идущие следом строчные слова («Olimpiada decreased to» —
# это не имя, а имя плюс начало предиката).
_ENTITY_NAME = r"(?:Irkutsk business unit|[A-Z][A-Za-z]+)"

# Упоминание затраты при активе. Две реальные формы предложения:
#   «AISC at Olimpiada …»  (имя ПОСЛЕ затраты)
#   «At Kuranakh, AISC …»  (имя ПЕРЕД затратой)
COST_MENTION = re.compile(
    rf"\b(AISC|TCC)\s+at\s+({_ENTITY_NAME})"
    rf"|\bAt\s+({_ENTITY_NAME}),?\s+(AISC|TCC)\b"
)

# Величина затраты: «$455 per ounce» и эквивалент «$836/oz».
COST_AMOUNT = re.compile(r"\$\s*([\d,]+)\s*(?:per ounce|/oz)")

# Начало годового абзаца: «In 2019, AISC at» / «In 2019, TCC at».
ANNUAL_ANCHOR = "In {year}, {cost} at"

# Годовой префикс предложения: «In 2019, …». Квартальные предложения начинаются
# иначе («In the fourth quarter, …», «At Kuranakh, …»), поэтому явный год
# встречается только у предложений с настоящим годовым наблюдением.
SENTENCE_YEAR = re.compile(r"^\s*In\s+(\d{4})\s*,")


class AssetNameError(Exception):
    """Неизвестное имя актива в годовой прозе AISC/TCC.

    Сигнализирует о новом объекте, которого нет ни в `ASSET_CANON`, ни в
    `SKIP_NAMES`: молча его пропустить нельзя, иначе поактивная структура
    затрат потеряет строку незаметно.
    """


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


def _annual_block(text: str, year: int, cost: str) -> str:
    """Вырезает годовой абзац затраты `cost` из нормализованного текста.

    Годовой абзац начинается буквально с «In {year}, {cost} at» и тянется до
    следующего раздела. Для AISC это заголовок «Capex», для TCC — заголовок
    раздела AISC («All-in sustaining costs (AISC)»). Если якоря нет, возвращается
    пустая строка: отсутствие абзаца — не ошибка (в FY2018 нет годового AISC по
    Вернинскому, а сам год может быть неполным).

    Возврат именно ОТ якоря — не «всё, что найдётся» — главная защита от ловушки
    квартального абзаца: предложение «In the fourth quarter, AISC at Olimpiada
    decreased to $352 per ounce» лежит ДО якоря и в вырезанный блок не попадает.

    Якорь ищется как первое вхождение. Годовой абзац всегда открывается
    Олимпиадой, поэтому её же фраза «In {year}, {cost} at Olimpiada» и есть
    начало: остальные активы (Blagodatnoye, Natalka, …) идут ПОСЛЕ неё уже без
    префикса года либо со своим «In {year},» внутри блока. Проверено на всех
    четырёх фикстурах.
    """
    anchor = ANNUAL_ANCHOR.format(year=year, cost=cost)
    start = text.find(anchor)
    if start < 0:
        return ""
    end = text.find("Capex", start) if cost == "AISC" else text.find(
        "All-in sustaining costs (AISC)", start
    )
    return text[start:end] if end > start else text[start:]


def _sentence_year(sentence: str):
    """Год, названный в самом предложении, или None.

    «In 2019, TCC at …» → 2019. Предложения без префикса («AISC at Blagodatnoye
    …», «At Verninskoye AISC …») возвращают None и наследуют год абзаца.
    """
    match = SENTENCE_YEAR.match(sentence)
    return int(match.group(1)) if match else None


def _mention_amount(sentence: str, mention, amounts: list, limit: int):
    """Выбирает сумму, относящуюся к данному упоминанию.

    Кандидаты — суммы после упоминания и до следующего упоминания в предложении
    (это отсекает суммы, принадлежащие соседнему активу в многоактивном
    предложении: «AISC at Natalka … $529, while AISC at Verninskoye … $560»).

    Из кандидатов берётся ПЕРВАЯ — в норме наблюдение названо раньше сравнения:
    «In 2019, TCC at Natalka declined to $396 per ounce, compared to $747 per
    ounce in 2018» → 396 (747 — прошлый год).

    ОГРАНИЧЕНИЕ (известное, см. отчёт): если сравнение стоит ПЕРЕД наблюдением
    («AISC at Olimpiada, compared to $111 per ounce in 2018, was $222 per
    ounce»), первая сумма — это сравнение, и вернётся 111, а не 222. В реальных
    фикстурах такой конструкции нет: во всех шести годовых абзацах наблюдение
    идёт первым. Разбирать клаузу «was $N per ounce» как наблюдение — сшивать
    предложение заново, и это добавочная хрупкость без подтверждённого случая.
    """
    candidates = [a for a in amounts if mention.end() <= a.start() < limit]
    return candidates[0] if candidates else None


def _block_values(block: str, cost: str, year: int) -> dict[str, float]:
    """Извлекает значения `cost` по активам из вырезанного годового абзаца.

    Разбор идёт по предложениям, и КАЖДОЕ предложение сначала проверяется на
    год:

    * предложение с явным префиксом «In <YYYY>,» даёт значение только если
      `<YYYY> == year`; иначе оно пропускается целиком (чужой год в блоке —
      например, сравнительное «In 2020, …», попавшее в абзац 2019);
    * предложение без префикса наследует год абзаца (это и есть форма
      «AISC at Blagodatnoye increased to $566 per ounce»).

    Внутри предложения допускается несколько упоминаний («AISC at Natalka …
    $529, while AISC at Verninskoye … $560»), поэтому у каждого упоминания
    берётся своя первая сумма — см. `_mention_amount`.

    ПРИОРИТЕТ ГОДА УБИРАЕТ ЗАВИСИМОСТЬ ОТ ПОРЯДКА. Для актива может найтись и
    годовая, и безымянно-наследованная (квартальная) величина: в FY2019 TCC
    квартальные предложения «At Blagodatnoye, TCC amounted to $373» и
    «At Natalka, TCC amounted to $376» соседствуют с годовыми «In 2019, TCC at
    Blagodatnoye were $398» и «In 2019, TCC at Natalka declined to $396». Какая
    из пар побеждает, НЕ должно зависеть от порядка предложений: величина из
    предложения с собственным годовым префиксом приоритетнее величины из
    предложения без префикса. Без этого правила переписанный (но не изменённый
    по содержанию) абзац возвращал бы квартальные 373/376 вместо годовых
    398/396.
    """
    # (значение, приоритет): приоритет 1 — предложение с явным годом, 0 — без.
    selected: dict[str, tuple] = {}
    # Точка — граница предложения. «per ounce»/«$/oz» точку не содержат, так что
    # дробление по «точка + пробел» безопасно и не рвёт суммы.
    for sentence in re.split(r"(?<=\.)\s+", block):
        sentence_year = _sentence_year(sentence)
        if sentence_year is not None and sentence_year != year:
            # Чужой год: предложение не относится к разбираемому году.
            continue
        priority = 1 if sentence_year == year else 0
        mentions = list(COST_MENTION.finditer(sentence))
        amounts = list(COST_AMOUNT.finditer(sentence))
        for index, mention in enumerate(mentions):
            mention_cost = mention.group(1) or mention.group(4)
            name = mention.group(2) or mention.group(3)
            if mention_cost != cost or name in SKIP_NAMES:
                continue
            if name not in ASSET_CANON:
                raise AssetNameError(
                    f"неизвестный актив {name!r} в годовом абзаце {cost} за {year}"
                )
            limit = mentions[index + 1].start() if index + 1 < len(mentions) else len(sentence)
            amount = _mention_amount(sentence, mention, amounts, limit)
            if amount is None:
                # Упоминание без величины — это прочая проза (например,
                # «The Company anticipates AISC at Natalka to normalize…»).
                continue
            asset = ASSET_CANON[name]
            current = selected.get(asset)
            if current is None or priority >= current[0]:
                selected[asset] = (priority, parse_amount(amount.group(1)))
    return {asset: value for asset, (_, value) in selected.items()}


def extract_year(text: str, year: int) -> dict[str, dict[str, float]]:
    """Возвращает поактивный AISC и TCC за `year` из годовой прозы пресс-релиза.

    Формат результата: `{ASSET_NAME: {"aisc": float, "tcc": float}}` по
    каноническим именам (см. `ASSET_CANON`).

    Ключ присутствует, только если в годовой прозе найдены ОБА значения —
    AISC и TCC. Так устроен год, где Полюс раскрывает TCC, но умалчивает
    годовой AISC: в FY2018 нет годового AISC по Вернинскому, поэтому
    `VERNINSKOYE2` в результате не появится (хотя TCC по нему в абзаце есть).
    Смешанный словарь с одним только `tcc` был бы непригоден для вызывающей
    стороны: перекрёстная сверка годов сравнивает пары (AISC, TCC), и клин
    `(AISC−TCC)/TCC` из неполной пары не считается.

    Полнота набора (все пять активов) — забота вызывающей стороны: отсутствие
    актива здесь не ошибка.

    Разбор строго привязан к годовому абзацу (`In {year}, …`), поэтому
    квартальные значения того же года в результат не попадают.
    """
    by_asset: dict[str, dict[str, float]] = {}
    for cost, field in (("AISC", "aisc"), ("TCC", "tcc")):
        block = _annual_block(text, year, cost)
        if not block:
            continue
        for asset, value in _block_values(block, cost, year).items():
            by_asset.setdefault(asset, {})[field] = value
    return {asset: fields for asset, fields in by_asset.items() if len(fields) == 2}


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
