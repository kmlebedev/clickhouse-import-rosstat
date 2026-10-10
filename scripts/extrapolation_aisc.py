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
from typing import Optional

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

# Допуск перекрёстной сверки AISC между двумя документами, USD/oz. TCC
# сверяется точно; AISC — с малым допуском, потому что сравнительная колонка
# иногда пересчитана при реклассификации затрат. Расширять его молча нельзя:
# реальная реклассификация должна быть записана в отчёте, а не заглушена.
AISC_CROSSCHECK_TOLERANCE_USD_PER_OZ = 0.5


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

# --- Цитата прошлогоднего годового TCC внутри годового абзаца ----------------

# В годовом абзаце TCC Полюс называет прошлогодний TCC актива как сравнение:
#   «In 2021, TCC at Natalka increased to $368 per ounce, up 4% compared to $355
#    per ounce in 2020»
# Прошлый год здесь — это НЕ отдельный периодический блок (FY2021 не
# пересчитывает таблицу 2020 года целиком), а ровно одна цифра, которую можно
# сверить с собственным годовым значением предыдущего отчёта.
#
# Синтаксис строго узкий и потому надёжный: сумма «$N per ounce», сразу за
# которой идёт «in <YYYY>». Имя актива берётся из УПОМИНАНИЯ той же формы, что
# и в наблюдении («TCC at Natalka», «At Natalka, TCC»), — оно должно стоять в
# предложении ДО суммы. Имя не вытаскивается шаблоном «любое слово с заглавной
# буквы»: так в имя попало бы «In» из оборота «per ounce in 2020».
PRIOR_YEAR_CITATION_AMOUNT = re.compile(
    r"\$\s*([\d,]+)\s*(?:per ounce|/oz)\s+in\s+(\d{4})"
)


class AssetNameError(Exception):
    """Неизвестное имя актива в годовой прозе AISC/TCC.

    Сигнализирует о новом объекте, которого нет ни в `ASSET_CANON`, ни в
    `SKIP_NAMES`: молча его пропустить нельзя, иначе поактивная структура
    затрат потеряет строку незаметно.
    """


class CrossCheckError(Exception):
    """Расхождение значения между двумя документами об одном и том же годе.

    Один год приходит от двух источников: как ОТЧЁТНЫЙ в своём релизе и как
    ЦИТИРУЕМЫЙ сравнительный в релизе следующего года. Если числа не совпадают,
    испорчен как минимум один из документов, и молча продолжать нельзя: AISC
    серии калибруются именно по этим числам.

    Обратное НЕ верно: отсутствие цитаты за какой-то год — норма, а не ошибка
    (FY2019–FY2021 цитируют TCC только по Наталке, полной сравнительной таблицы
    прошлого года в релизах нет).
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


def extract_prior_year_citations(text: str, year: int) -> dict[str, dict[str, float]]:
    """Прошлогодние годовые TCC, ПРОЦИТИРОВАННЫЕ в отчёте за `year`.

    Формат: `{ASSET_NAME: {cited_year: tcc}}`. Для всех четырёх фикстур
    2018–2021 это ровно одна запись — TCC Наталки за год, предшествующий
    отчётному:

    * FY2019 → `{"NATALKA": {2018: 747.0}}`
    * FY2020 → `{"NATALKA": {2019: 396.0}}`
    * FY2021 → `{"NATALKA": {2020: 355.0}}`

    Почему только TCC и только Наталка. Полюс НЕ публикует в отчёте полную
    сравнительную таблицу прошлого года: поактивный AISC/TCC прошлого года по
    остальным рудникам в тексте отсутствует (`extract_year(text, year - 1)`
    для каждого из четырёх релизов возвращает пустой словарь). Единственная
    цифра прошлого года, повторённая словами, — это TCC Наталки, названный как
    сравнение в её собственном годовом предложении. Молчаливое расширение
    шаблона «на всякий случай» дало бы ложные совпадения в посторонней прозе
    («$21 per ounce in 2018»), поэтому шаблон узкий: `$N per ounce in YYYY`
    внутри предложения с явным годовым префиксом `In {year},`.

    Возвращаются только пары «цитируемый год < отчётного»: цитата того же года
    или будущего года — не проверяемое пересечение, а шум.

    Значения сопоставляются с собственным годовым наблюдением предыдущего
    отчёта в `build_series` — расхождение поднимает `CrossCheckError`.
    """
    cited: dict[str, dict[str, float]] = {}
    block = _annual_block(text, year, "TCC")
    if not block:
        return cited
    for sentence in re.split(r"(?<=\.)\s+", block):
        if _sentence_year(sentence) != year:
            # Квартальные и прочие предложения без годового префикса не могут
            # нести проверяемую цитату: год сравнения в них не привязан к году
            # отчёта, и «per ounce in 2018» в них — про что угодно.
            continue
        mentions = list(COST_MENTION.finditer(sentence))
        for index, mention in enumerate(mentions):
            if (mention.group(1) or mention.group(4)) != "TCC":
                continue
            name = mention.group(2) or mention.group(3)
            if name in SKIP_NAMES:
                continue
            if name not in ASSET_CANON:
                raise AssetNameError(
                    f"неизвестный актив {name!r} в цитате TCC за {year}"
                )
            limit = (
                mentions[index + 1].start()
                if index + 1 < len(mentions)
                else len(sentence)
            )
            for amount in PRIOR_YEAR_CITATION_AMOUNT.finditer(sentence):
                if not (mention.end() <= amount.start() < limit):
                    continue
                cited_year = int(amount.group(2))
                if cited_year >= year:
                    continue
                asset = ASSET_CANON[name]
                cited.setdefault(asset, {})[cited_year] = parse_amount(amount.group(1))
    return cited


def cross_check_overlap(
    year: int,
    a: dict[str, dict[str, float]],
    b: dict[str, dict[str, float]],
    src_a: Path,
    src_b: Path,
) -> None:
    """Сверяет значения одного года из двух источников `a` и `b`.

    Каждый источник — результат `extract_year(text, year)`, то есть
    `{ASSET: {"aisc": float, "tcc": float}}`. Сравниваются ТОЛЬКО те пары
    (актив, поле), которые присутствуют в ОБОИХ источниках; всё остальное —
    норма (у FY2018 нет Вернинского, и это не конфликт).

    Допуски. TCC сверяется точно: TCC не пересчитывается при реклассификации
    затрат. AISC — с абсолютным допуском `AISC_CROSSCHECK_TOLERANCE_USD_PER_OZ`
    (0.5 USD/oz): сравнительная колонка иногда пересчитана, и это допустимо.
    Допуск сознательно НЕ расширяется «на месте» под конкретный случай —
    настоящая реклассификация должна быть записана в отчёте, а не заглушена
    правкой константы.

    При расхождении поднимается `CrossCheckError` с активом, годом, обоими
    значениями и обоими путями к файлам.
    """
    for asset in ASSETS:
        for field in ("aisc", "tcc"):
            left = a.get(asset, {}).get(field)
            right = b.get(asset, {}).get(field)
            if left is None or right is None:
                continue
            if field == "tcc":
                agree = left == right
                tolerance = 0.0
            else:
                tolerance = AISC_CROSSCHECK_TOLERANCE_USD_PER_OZ
                agree = abs(left - right) <= tolerance
            if agree:
                continue
            label = field.upper()
            raise CrossCheckError(
                f"расхождение {label} по активу {asset} за {year}: "
                f"{left} USD/oz (источник {src_a.name}) против "
                f"{right} USD/oz (источник {src_b.name})"
                + ("" if tolerance == 0.0 else f", допуск {tolerance} USD/oz")
            )


def _is_corrupt_fixture(path: Path) -> bool:
    """True для намеренно испорченных фикстур (`*_corrupt.pdf`).

    Такие файлы существуют только для регрессионного теста перекрёстной сверки
    и НИКОГДА не участвуют в калибровке: если их подхватить по маске каталога,
    испорченное число незаметно попадёт в наблюдения.
    """
    return path.stem.endswith("_corrupt")


def build_series(
    fixtures: Optional[dict] = None,
) -> dict[str, dict[int, dict[str, float]]]:
    """Собирает поактивную серию AISC/TCC по нескольким релизам.

    Формат: `{ASSET: {year: {"aisc": float | None, "tcc": float | None}}}`.
    Каждый релиз даёт свой ОТЧЁТНЫЙ год (`extract_year`) плюс процитированные
    в нём прошлогодние TCC (`extract_prior_year_citations`). Значения разных
    документов об одном годе сводятся воедино и сверяются: пересечение есть
    там, где год присутствует и как отчётный, и как цитируемый.

    Отсутствие цитируемого года — ШТАТНЫЙ случай, а не ошибка: у FY2018 нет
    предшественника среди фикстур, и ни один релиз не пересчитывает полную
    таблицу прошлого года. Проверка срабатывает только на НАЛИЧИИ двух чисел.

    Неполный набор активов (FY2018 без Вернинского) не мешает: актив просто
    присутствует в серии за те годы, за которые он раскрыт.

    Порядок обхода словаря фиксирован (по возрастанию года), чтобы сообщение
    об ошибке было детерминированным: сначала идёт более ранний документ.

    `*_corrupt.pdf` (см. `_is_corrupt_fixture`) отбрасываются молча при
    АВТООБНАРУЖЕНИИ фикстур (`fixtures is None`). Если файл передан явно в
    словаре, он участвует: именно так строится регрессионный тест на
    испорченную пару. Явный словарь — всегда намерение вызывающей стороны,
    и подменять его догадкой «наверное, это случайная порча» нельзя.
    """
    if fixtures is None:
        fixtures = {
            int(match.group(1)): path
            for path in sorted(FIXTURE_DIR.glob("polyus_pr_4q_fy*.pdf"))
            if not _is_corrupt_fixture(path)
            and (match := re.search(r"fy(\d{4})", path.stem))
        }

    series: dict[str, dict[int, dict[str, float]]] = {}
    # Год -> {актив: {"aisc": ..., "tcc": ...}}, накопленное по всем документам.
    observed: dict[int, dict[str, dict[str, float]]] = {}
    # Год -> (путь, значения): первый документ, назвавший год отчётным.
    reporting_source: dict[int, tuple[Path, dict[str, dict[str, float]]]] = {}

    for year in sorted(fixtures):
        path = fixtures[year]
        text = normalize(pdf_to_text(path))
        annual = extract_year(text, year)
        citations = extract_prior_year_citations(text, year)

        # Прошлогодние цитаты могут пересечься с уже накопленным годовым
        # наблюдением (из более раннего документа) либо добавить новый год.
        for asset, per_year in citations.items():
            for cited_year, value in per_year.items():
                prior = observed.get(cited_year, {}).get(asset)
                if prior is not None and prior.get("tcc") is not None:
                    source = reporting_source[cited_year]
                    cross_check_overlap(
                        cited_year,
                        {asset: {"tcc": prior["tcc"]}},
                        {asset: {"tcc": value}},
                        source[0],
                        path,
                    )
                else:
                    observed.setdefault(cited_year, {}).setdefault(
                        asset, {"aisc": None, "tcc": None}
                    )["tcc"] = value

        observed.setdefault(year, {})
        for asset, fields in annual.items():
            observed[year][asset] = dict(fields)
        reporting_source[year] = (path, annual)

    for year, per_asset in observed.items():
        for asset, fields in per_asset.items():
            series.setdefault(asset, {})[year] = {
                "aisc": fields.get("aisc"),
                "tcc": fields.get("tcc"),
            }
    return series


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
