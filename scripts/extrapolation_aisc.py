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

import base64
import json
import os
import re
import statistics
import subprocess
import sys
import urllib.error
import urllib.request
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

# Последний год, на который ЧИТАЕТСЯ факт добычи из `databook_polyus`.
#
# Добыча берётся ГОДОМ, а не плато на весь LOM: в датапаке есть 2025-й
# (OLIMPIADA 1441.3, BLAGODATNOYE 499.8, NATALKA 454.0, VERNINSKOYE2 230.8,
# KURANAKH 309.9 koz), и он же первый год плана. Строить веса увязки на
# плоском плато значило бы выбросить последний известный факт. Из этого
# 2025-го и растёт точка перелома эскалации — см. `ESCALATION`.
ACTUAL_PRODUCTION_LAST_YEAR = 2025

# Эскалация затрат по годам — ДОЛИ, а не проценты: 0.06 = +6 % за год.
# ЕДИНСТВЕННЫЙ источник эскалации в скрипте (на него же опирается
# `escalation_factor`).
#
# ИСТОЧНИК КОНСТАНТ — ЯВНО НАЗВАННОЕ ДОПУЩЕНИЕ, потому что готовая
# константа `ESCALATION` из Задачи 1 В РЕПОЗИТОРИИ ОТСУТСТВУЕТ: Задача 1 её
# не завела (проверено `grep -rn ESCALATION` по `scripts/`, `dcf/`, `docs/`:
# только упоминания в плане и спеке), а заводить ВТОРОЙ источник эскалации
# бриф запрещает. Спекулятивного ряда ИПЦ в репозитории тоже нет:
# `ipc_mes` лежит в БД, но движок его не читает (`Params.Ipc` пуст), и
# `read_actual_production` читает из `databook_polyus`, а не оттуда.
#
# ЧИСЛА. Темп 6 % — верхняя часть целевого коридора ЦБ РФ (4 %); узловая
# точка 2028 — первый год ПОСЛЕ последнего, по которому есть факт добычи
# (`ACTUAL_PRODUCTION_LAST_YEAR`), после чего темп снижается до 4 %. Это
# НЕ измеренный ИПЦ, а допущение политики затрат, названное здесь, чтобы его
# можно было заменить чтением `ipc_mes` правкой ОДНОГО словаря: формула
# `escalation_factor` при этом не меняется (спека §3.4).
ESCALATION = {
    2025: 0.06,
    2026: 0.06,
    2027: 0.06,
    2028: 0.04,
    2029: 0.04,
    2030: 0.04,
    2031: 0.04,
    2032: 0.04,
    2033: 0.04,
    2034: 0.04,
    2035: 0.04,
    2036: 0.04,
    2037: 0.04,
    2038: 0.04,
    2039: 0.04,
    2040: 0.04,
    2041: 0.04,
    2042: 0.04,
    2043: 0.04,
    2044: 0.04,
    2045: 0.04,
    2046: 0.04,
    2047: 0.04,
    2048: 0.04,
}

# Эталонное значение множителя эскалации 2025 → 2034, закреплённое тестом.
# 1.06³ · 1.04⁶ = 1.507015… (2034-й берёт множитель за 2033-й). Названо
# отдельной константой, чтобы тест показывал, ЧТО именно он пинует, а не
# прятал магическое число в себе.
ESCALATION_2025_TO_2034 = 1.06**3 * 1.04**6

# PDF-фикстуры пресс-релизов 4Q/FY 2018–2021 и артефакты вывода.
FIXTURE_DIR = ROOT / "scripts/testdata"
OUTPUT_SQL = ROOT / "sql/dcf_mine_plans_seed.sql"
OUTPUT_CSV = ROOT / "polyus_lom_reconciled_long.csv"

# Допуск перекрёстной сверки AISC между двумя документами, USD/oz. TCC
# сверяется точно; AISC — с малым допуском, потому что сравнительная колонка
# иногда пересчитана при реклассификации затрат. Расширять его молча нельзя:
# реальная реклассификация должна быть записана в отчёте, а не заглушена.
AISC_CROSSCHECK_TOLERANCE_USD_PER_OZ = 0.5

# Множитель, приводящий MAD к оценке σ для нормального распределения:
# 1 / Φ⁻¹(0.75) ≈ 1.4826. Нужен, чтобы порог читался в привычных «сигмах»,
# хотя сама мера устойчива к выбросам.
MAD_TO_SIGMA = 1.4826

# Порог отбраковки в устойчивых сигмах.
#
# РАСХОЖДЕНИЕ С БРИФОМ, ЗАФИКСИРОВАНО СОЗНАТЕЛЬНО. Бриф предписывает порог
# `3 · 1.4826 · mad`, но на проверенных калибровочных данных он противоречит
# собственным ожидаемым значениям брифа: при пороге 3 отбрасывается 2021 год по
# Благодатному (отклонение 0.371 против полосы 0.198) и по Наталке (0.173 против
# 0.167), после чего r_bar выходят 0.400 и 0.509 — тогда как бриф требует 0.531
# и 0.573, а это В ТОЧНОСТИ средние по всем трём годам (0.5308 и 0.5730). Бриф
# также прямо утверждает: «No outliers in this clean data — all three years
# survive for every asset». Оба требования одновременно выполнимы только при
# более широкой полосе. В буквальном виде «3» и ожидаемые r_bar несовместимы.
#
# Приоритет отдан ВЫХОДНЫМ ЗНАЧЕНИЯМ брифа (тесты и таблица названы
# авторитетными), а правило MAD сохранено — оно и есть содержательное
# требование задачи. Нижняя граница полосы, при которой чистые данные проходят
# целиком, — 5.7; взято 6.0 с запасом. Испорченный год Куранача (клин 8.653)
# отбрасывается при любом пороге вплоть до 10, так что защита от грубого
# выброса не ослаблена. Пересмотр порога без пересмотра ожидаемых r_bar
# невозможен — см. отчёт Task 5.
OUTLIER_SIGMA_LIMIT = 6.0

# Порог, ниже которого знаменатель увязки `den = Σ tcc·r̄·w` считается
# вырожденным. Клинья — доли от TCC, поэтому положительные значения дают `den`
# порядка десятков-сотен; 1e-12 отделяет настоящую вырожденность от просто
# малого знаменателя и не даёт делить на ноль.
#
# Сюда попадает НЕ ТОЛЬКО «все клинья нулевые»: при отрицательных клиньях,
# перевешивающих положительные, `den < 0` и ветвь тоже срабатывает — см.
# разбор в докстроке `reconcile`. В реальных данных (все r̄ положительны,
# 0.488–0.755) `den > 0` всегда, поэтому ветвь достижима только через нулевые
# или отрицательные клинья.
RECONCILE_WEDGE_EPSILON = 1e-12

# Проверенные поактивные пары (AISC, TCC) за калибровочные годы, USD/oz.
# Вычитаны из годовой прозы релизов и перекрёстно сверены (Task 3–4); тесты
# `TestBuildSeries` подтверждают, что та же выборка получается из фикстур.
_REAL_AISC_TCC = {
    "OLIMPIADA": {2019: (455.0, 293.0), 2020: (515.0, 312.0), 2021: (618.0, 369.0)},
    "BLAGODATNOYE": {2019: (566.0, 398.0), 2020: (478.0, 347.0), 2021: (658.0, 367.0)},
    "NATALKA": {2019: (605.0, 396.0), 2020: (529.0, 355.0), 2021: (626.0, 368.0)},
    "VERNINSKOYE2": {2019: (636.0, 363.0), 2020: (560.0, 327.0), 2021: (644.0, 358.0)},
    "KURANAKH": {2019: (777.0, 523.0), 2020: (756.0, 518.0), 2021: (865.0, 569.0)},
}


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


class CalibrationError(Exception):
    """Калибровочная серия непригодна для оценки устойчивого клина.

    Поднимается, когда после отбраковки выбросов остаётся меньше двух
    наблюдений: по одной точке нельзя ни проверить её на выброс, ни отличить
    устойчивый уровень от случайности. Молча вернуть среднее из одной точки
    значило бы выдать шум за калибровку.
    """


def wedge_share(aisc: float, tcc: float) -> float:
    """Клин затрат поддержания как ДОЛЯ от TCC: `(AISC − TCC) / TCC`.

    Доля, а не абсолютная разница: абсолютный клин растёт вместе с TCC, а
    переносится на плановые годы именно структура — доля, на которую потом
    умножается плановый TCC.

    Нулевой или отрицательный TCC — не вычислительная мелочь, а испорченное
    наблюдение: делить на него нельзя.
    """
    if tcc <= 0:
        raise ValueError(f"TCC должен быть положительным, получено {tcc}")
    return (aisc - tcc) / tcc


def _mad_survivors(values: list[float]) -> list[bool]:
    """Маска «значение не выброс» по правилу MAD.

    `med = median(values)`, `mad = median(|x − med|)`,
    `scaled = MAD_TO_SIGMA · mad`; значение отбрасывается, если
    `|x − med| > OUTLIER_SIGMA_LIMIT · scaled`. При `scaled == 0` не
    отбрасывается ничего.

    Почему порог равен 6, а не предписанным брифом 3 — разобрано у константы
    `OUTLIER_SIGMA_LIMIT`: при 3 чистые данные теряют 2021 год по Благодатному и
    Наталке, что противоречит ожидаемым r_bar самого брифа.

    ПОЧЕМУ MAD, А НЕ mean ± 2σ. На калибровке всего три точки, и грубый выброс
    сам раздувает σ настолько, что проходит собственную полосу. Проверено на
    Куранаха: клинья (0.486, 8.653, 0.520) правило mean ± 2σ сохраняет ЦЕЛИКОМ и
    даёт среднее 3.22, а правило MAD сохраняет (0.486, 0.520) и даёт 0.503.
    Медиана и MAD выбросом не сдвигаются — в этом весь смысл устойчивой меры.

    Случай `scaled == 0` (медиана отклонений нулевая) — не ошибка: это либо
    совпадающие наблюдения, либо больше половины точек на одной и той же
    величине. Тогда отбраковывать нечего, и порог не применяется.
    """
    med = statistics.median(values)
    deviations = [abs(value - med) for value in values]
    scaled = MAD_TO_SIGMA * statistics.median(deviations)
    if scaled == 0.0:
        return [True] * len(values)
    limit = OUTLIER_SIGMA_LIMIT * scaled
    return [deviation <= limit for deviation in deviations]


def calibrate(series: dict) -> dict[str, dict]:
    """Калибрует поактивный устойчивый клин по активам.

    Вход — серия вида `{ASSET: {year: {"aisc": float|None, "tcc": float|None}}}`
    (форма `build_series`). Берутся только годы из `CALIBRATION_YEARS`, по
    которым есть ОБА значения. 2018 год не участвует: в FY2018 Полюс не
    раскрывает годовой поактивный AISC по Вернинскому, и вектор наблюдений
    был бы неполным.

    Выход — `{ASSET: {"r_bar": float, "years": list[int],
    "discarded": list[tuple[int, float]]}}`, где `years` — годы, по которым
    усреднено (то есть ОСТАВШИЕСЯ после отбраковки), а `discarded` — пары
    (год, значение клина) для отчёта. `discarded` — обычный список кортежей, а
    не dataclass: итерация вида `[y for y, _ in cal[asset]["discarded"]]` должна
    работать без разворачивания дополнительного типа.

    Отбраковка — по MAD (`_mad_survivors`), а не по mean/σ: см. объяснение там
    же. Здесь важно, что при n = 3 только устойчивая мера ловит грубый выброс.

    Активы, по которым не набралось двух выживших наблюдений, дают
    `CalibrationError` с именем актива: калибровать клин по одной точке нельзя.
    """
    calibrated: dict[str, dict] = {}
    for asset in ASSETS:
        per_year = series.get(asset, {})
        observations: list[tuple[int, float]] = []
        for year in CALIBRATION_YEARS:
            fields = per_year.get(year)
            if not fields:
                continue
            aisc = fields.get("aisc")
            tcc = fields.get("tcc")
            if aisc is None or tcc is None:
                # Неполная пара — не клин: (AISC − TCC) из неё не считается.
                continue
            observations.append((year, wedge_share(aisc, tcc)))

        survivors = _mad_survivors([value for _, value in observations]) if observations else []
        kept = [obs for obs, keep in zip(observations, survivors) if keep]
        discarded = [obs for obs, keep in zip(observations, survivors) if not keep]
        if len(kept) < 2:
            raise CalibrationError(
                f"актив {asset}: после отбраковки выбросов осталось "
                f"{len(kept)} наблюдений из {len(observations)} — "
                f"для калибровки клина нужно минимум два"
            )
        calibrated[asset] = {
            "r_bar": statistics.mean([value for _, value in kept]),
            "years": [year for year, _ in kept],
            "discarded": discarded,
        }
    return calibrated


def reconcile(
    tcc: dict[str, float],
    r_bar: dict[str, float],
    production: dict[str, float],
    group_aisc: float,
) -> tuple[dict[str, float], float, float]:
    """Увязывает поактивные AISC с опубликованным групповым значением.

    Задача — одна скалярная величина, а не подгонка: множитель `m` масштабирует
    калиброванные клинья так, чтобы средневзвешенный по добыче групповой AISC
    лёг РОВНО на `group_aisc`. Замкнутая форма, без итераций и оптимизатора.

    Веса — доли добычи: `w_i = production_i / Σ_j production_j`. Множитель
    выводится из требования `Σ_j tcc_j·(1 + r̄_j·m)·w_j = group_aisc`:

        den = Σ_j tcc_j·r̄_j·w_j
        m   = 1 + (group_aisc − Σ_j tcc_j·(1+r̄_j)·w_j) / den
        aisc_i = tcc_i·(1 + r̄_i·m)

    ДВА ПРАВИЛА, ОПРЕДЕЛЯЮЩИХ КОРРЕКТНОСТЬ.

    1. `m` правит КЛИН, а не AISC. `tcc_i` — наблюдённая величина, её
       масштабировать нельзя; оцениваемым является только `r̄_i`. Поэтому
       `aisc_i = tcc_i·(1 + r̄_i·m)`, а НЕ `tcc_i·(1 + r̄_i)·m`: последнее
       исказило бы наблюдённые затраты и разошлось бы с формулой множителя.

    2. `m` прижат снизу нулём. Отрицательный `m` увёл бы AISC ниже TCC —
       отрицательные затраты поддержания, что экономически бессмысленно.
       Прижим срабатывает ровно тогда, когда `group_aisc` ниже взвешенного пола
       TCC, то есть цель ДОСТИЖИМО недостижима. В этом случае остаток ненулевой:
       это честный отчёт о недостижимой цели, а не отказ от подгонки.

    Вырожденный случай — `den <= RECONCILE_WEDGE_EPSILON`, и понимать его надо
    буквально по знаку `den`, а не как «все клинья нулевые». Порог ловит
    ВЫРОЖДЕННЫЙ знаменатель, то есть устойчиво НЕПОЛОЖИТЕЛЬНЫЙ взвешенный клин
    `Σ_j tcc_j·r̄_j·w_j <= 0`: все клинья нулевые (законный мир Go-сида, где
    AISC == TCC) либо отрицательные клинья перевешивают положительные. В обоих
    случаях формула множителя неприменима, и делить нельзя. Тогда `m = 1.0`
    (нейтральный множитель, клин переносится как откалиброван), и
    `aisc_i = tcc_i·(1 + r̄_i)`.

    Отрицательный `den` по смыслу ближе к пути прижима, чем к вырожденности:
    при `den < 0` точная увязка достижима при `m = 0` (весь клин снимается, AISC
    садится на TCC), и формула `m = max(0, 1 + (target − base)/den)` дала бы
    ровно это. Условие записано как `den <= eps` НАМЕРЕННО, вопреки более
    точному разбору по знаку: такова предписанная спецификация, и в реальных
    данных (все r̄ положительны, 0.488–0.755) ветви неотличимы. Смена условия
    меняет поведение на отрицательных клиньях — это отдельное решение, а не
    молчаливая правка.

    Остаток считается POST-HOC по фактически выданным строкам, а не по тому,
    что обещала формула: `Σ_i aisc_i·w_i − group_aisc`. Знак однозначен по
    определению «факт минус цель»: при достижимой цели он ~0, а при
    недостижимой НИЗКОЙ цели (включая прижим к `m = 0`) он ПОЛОЖИТЕЛЬНЫЙ —
    выданные строки стоят выше цели. Отрицательным он становится на
    недостижимой ВЫСОКОЙ цели, то есть когда положительного `m` не хватает.
    Нулевая Σ production — не «неопределённый остаток»: это отсутствие весов,
    при котором `w_i` не существуют, поэтому поднимается `ValueError`.
    """
    total_production = sum(production[asset] for asset in tcc)
    if total_production == 0.0:
        raise ValueError(
            "суммарная добыча равна нулю: веса w_i = production_i / Σ production_j "
            "не определены, увязка AISC невозможна"
        )

    aisc: dict[str, float] = {}
    target = float(group_aisc)
    den = 0.0
    base = 0.0
    for asset in tcc:
        weight = production[asset] / total_production
        den += tcc[asset] * r_bar[asset] * weight
        base += tcc[asset] * (1.0 + r_bar[asset]) * weight

    if den <= RECONCILE_WEDGE_EPSILON:
        # Ветвь ВЫРОЖДЕННОГО знаменателя, а не только «все клинья нулевые»: по
        # условию сюда попадает и den < 0 (отрицательные клинья перевесили
        # положительные). См. разбор в докстроке — условие намеренно оставлено
        # как `den <= eps`, чтобы не менять поведение молча.
        m = 1.0
        for asset in tcc:
            aisc[asset] = tcc[asset] * (1.0 + r_bar[asset])
    else:
        # Прижим к нулю: см. правило 2 выше.
        m = max(0.0, 1.0 + (target - base) / den)
        for asset in tcc:
            aisc[asset] = tcc[asset] * (1.0 + r_bar[asset] * m)

    residual = 0.0
    for asset in tcc:
        residual += aisc[asset] * (production[asset] / total_production)
    residual -= target
    return aisc, m, residual


def real_series() -> dict:
    """Проверенная серия AISC/TCC за 2019–2021 в памяти, USD/oz.

    Значения вычитаны из годовой прозы четырёх релизов и перекрёстно сверены
    (Task 3–4). Хранятся здесь, чтобы калибровка была воспроизводима без
    разбора PDF: клин считается из тех же чисел, что подтверждены тестами
    `TestBuildSeries`.

    Форма совпадает с `build_series`: `{ASSET: {year: {"aisc": ..., "tcc": ...}}}`.
    Возвращается КОПИЯ: тест с порчей года правит свой экземпляр и не влияет на
    соседние тесты.

    Не путать с `build_series`: тот собирает серию из настоящих фикстур и
    участвует в перекрёстной сверке, эта функция — проверенный снимок тех же
    чисел для калибровочных тестов.
    """
    return {
        asset: {year: {"aisc": aisc, "tcc": tcc} for year, (aisc, tcc) in per_year.items()}
        for asset, per_year in _REAL_AISC_TCC.items()
    }


# --- План 2025–LOM: эскалация, увязка, аллокация capex -----------------------


def escalation_factor(base_year: int, target_year: int) -> float:
    """Множитель эскалации затрат с `base_year` по `target_year` включительно.

    Произведение `(1 + ESCALATION[год])` по годам ПЕРЕХОДА: год `base_year`
    уже назван в ценах базы и множителя не получает, год `target_year` —
    получает. Отсюда два крайних случая, закреплённые тестами:
    `escalation_factor(2025, 2025) == 1.0` (тот же год — тождество) и
    `escalation_factor(2025, 2026) == 1 + ESCALATION[2025]`.

    Тот же принцип, что у `escalate` в `dcf/model.go` («множители за годы
    строго до yearIndex»), но с двумя отличиями, и оба намеренны:

    * ряд задан словарём по КАЛЕНДАРНОМУ году, а не позицией в массиве:
      строка эскалации начинается с 2025-го, и индексация «с нуля» сделала бы
      границу ряда невидимой;
    * за пределами словаря эскалация НЕ продолжается. У `escalate` кончился
      ряд ИПЦ — значит, прогноза больше нет; здесь словарь покрывает весь
      плановый горизонт (2025–2048, Наталка 24 года), и пропущенный год —
      это забытая константа, а не отсутствие прогноза. Молча вернуть 1.0
      значило бы занизить затраты во всех последующих годах разом. Поэтому —
      `ValueError` с именем года.

    Годы меньше базового (обратный ход, `target_year < base_year`) дают
    множитель меньше единицы механически: произведение пустое. Отдельной
    ветки для этого нет — вызывающая сторона плана так не делает.
    """
    if target_year < base_year:
        raise ValueError(
            f"обратная эскалация {base_year} → {target_year}: "
            f"множитель определён только для target_year >= base_year"
        )
    factor = 1.0
    for year in range(base_year, target_year):
        if year not in ESCALATION:
            raise ValueError(
                f"нет константы ESCALATION за {year}: ряд покрывает "
                f"{min(ESCALATION)}..{max(ESCALATION)}"
            )
        factor *= 1.0 + ESCALATION[year]
    return factor


def group_aisc_for_year(year: int) -> float:
    """Групповой AISC планового года `year`, USD/oz.

    **Решение контроллера, реализованное здесь.** План не говорит, откуда
    брать групповой AISC для каждого года LOM (2025…2048). Берётся
    ЕДИНСТВЕННЫЙ опубликованный групповой якорь —
    `GROUP_AISC_2025_USD_PER_OZ` (1437.0) — и эскалируется ТЕМ ЖЕ
    множителем, что и TCC (`escalation_factor(PLAN_BASE_YEAR, year)`).

    Почему именно так:

    * это единственный опубликованный групповой ориентир; других нет;
    * это повторяет поведение Go-сида (`dcf/mine_plans_seed.go`), где
      постоянный клин `1437 − 739 = 698` USD/oz переносится на весь LOM;
    * метод не вводит ни одного нового некалиброванного входа: и якорь, и
      ряд эскалации уже есть в расчёте.

    Следствие, которое надо называть вслух: и цель увязки, и TCC растут
    одним и тем же множителем, поэтому относительный клин `(AISC − TCC)/TCC`
    от года к году НЕ меняется — меняются только абсолютные USD/oz. Доля
    клина остаётся калиброванной величиной, и увязка каждый год попадает в
    ту же точку `m`. Это свойство допущения, а не свойство рынка.

    Добавка к якорю намеренно НЕ применяется: она бы сломала тождество
    относительного клина и потребовала бы второй необоснованной константы.
    """
    if year < PLAN_BASE_YEAR:
        raise ValueError(
            f"групповой AISC не определён за {year}: "
            f"якорь опубликован только за {PLAN_BASE_YEAR}"
        )
    return GROUP_AISC_2025_USD_PER_OZ * escalation_factor(PLAN_BASE_YEAR, year)


def sustaining_capex_mn(aisc: float, tcc: float, production_koz: float) -> float:
    """Поддержание производства в МЛН USD: `(aisc − tcc) · production_koz / 1000`.

    Единицы — здесь и только здесь, потому что ошибка в них даёт NPV, неверный
    в тысячи раз:

        koz × USD/oz = тыс. USD;  / 1000 → МЛН USD.

    Проверка из брифа: `(950 − 450) · 1400 / 1000 = 700.0` млн USD.

    ВСЁ поддержание идёт в capex с этой одной формулой: публичного разбиения
    «поддержание / SG&A» по активам не существует, поэтому доля `r̄_i`
    покрывает всю дельту `AISC − TCC` (спека §3.3, план §3.6 п. 2).
    """
    return (aisc - tcc) * production_koz / 1000.0


# Список активов, пропущенных последним вызовом `build_plan_rows`, с причиной.
# См. докстроку `build_plan_rows` — это наблюдаемый контракт, а не отладка.
LAST_SKIPPED_ASSETS: list[tuple[str, str]] = []


def _plateau_production(observed, asset: str, year: int) -> float:
    """Добыча года с плато: последний известный уровень вместо пропуска.

    `observed(asset, year)` — то, что реально пришло во входе (в том числе
    `0.0` как «строки нет»). Если год известен, возвращается он; если нет —
    ближайший ПРЕДЫДУЩИЙ известный год актива; если известных годов раньше
    нет вовсе — `0.0`.

    Почему не «просто 0.0 за неизвестный год»: `read_actual_production`
    отдаёт `0.0` и для отсутствующей строки, и для честного нуля, а
    публикации не покрывают все годы LOM. Ноль вместо плато обнулил бы вес
    актива в увязке и его `capex_sustaining` на всех годах без раскрытия.

    Ищем только НАЗАД, а не «ближайший в любую сторону»: план не должен
    знать будущую добычу раньше её года. Для первых известных лет это даёт
    `0.0` — что честно, там добыча действительно неизвестна.
    """
    value = observed(asset, year)
    if value != 0.0:
        return value
    for earlier in range(year - 1, PLAN_BASE_YEAR - 1, -1):
        earlier_value = observed(asset, earlier)
        if earlier_value != 0.0:
            return earlier_value
    return 0.0


def build_plan_rows(
    tcc_2025: dict[str, float],
    r_bar: dict[str, float],
    production: dict[str, dict[int, float]],
    group_aisc: float,
) -> list[dict]:
    """Собирает плановые строки 2025–LOM по всем активам.

    Строка — словарь с ключами `asset`, `year`, `production_koz`, `tcc`,
    `aisc`, `capex_sustaining`. Единицы: `production_koz` — тыс. тройских
    унций, `tcc`/`aisc` — USD/oz, `capex_sustaining` — МЛН USD (см.
    `sustaining_capex_mn`).

    Горизонт актива — `PLAN_BASE_YEAR … PLAN_BASE_YEAR + MINE_LIFE_YEARS[asset] − 1`:
    Олимпиада 10 лет → 2025–2034, Благодатное 13, Наталка 24, Вернинское 15,
    Куранах 15; всего 77 строк.

    ПОЧЕМУ ГОРИЗОНТ ПЕРЕСЕКАЕТСЯ С ФАКТОМ. У Олимпиады в `databook_polyus`
    есть 2025-й (1441.3 koz), и это ПЕРВЫЙ год плана: план стартует с
    последнего известного факта, а не с прогноза (тот же выбор, что
    `reportingYearBase` в `dcf/mine_plans_seed.go`). Лишние годы во входной
    добыче на горизонт НЕ влияют — он задан `MINE_LIFE_YEARS`, и это
    единственный авторитет по длине плана.

    Увязка — ОДИН вызов `reconcile` НА ГОД, по всем активам этого года
    сразу: веса `w_i` считаются внутри одного года, иначе один актив
    «перевешивал» бы другой из чужого года. Множитель `m` при этом общий для
    года, а не для актива: он перераспределяет клин между активами так,
    чтобы взвешенный по добыче AISC лёг на групповую цель.

    Добыча берётся из `production[asset][year]`; отсутствующий год — это
    ПЛАТО, а не ноль (см. `_plateau_production` и пояснение ниже).

    ГОДЫ БЕЗ ДОБЫЧИ — ЭТО ПОСЛЕДНИЙ ИЗВЕСТНЫЙ ЕЁ УРОВЕНЬ, А НЕ НОЛЬ.
    Публикации дают поактивную добычу не на все годы LOM, и `production`
    приходит из `read_actual_production`, который отдаёт `0.0` там, где строки
    нет (отсутствие строки в `databook_polyus` — норма, а не ноль, см. её
    докстроку). Взять такие нули за добычу значило бы поставить вес 0 активу,
    который на самом деле работает, и уронить его `capex_sustaining` в 0 на
    всём горизонте. Поэтому актив ведёт годовое плато: добыча года — это
    значение года, а если его нет, то ближайшее известное ПРЕДЫДУЩЕЕ; иными
    словами, при отсутствии годового профиля рудник держит последний факт
    (то же поведение, что у Go-сида, где плато задаётся последним фактом,
    развёрнутым на весь LOM).

    Осознанная цена этого решения: консервация карьера (нулевая добыча в
    середине срока) в годовом профиле неотличима от отсутствия данных, и такая
    строка получит добычу предыдущего года. Различение потребовало бы
    отдельного признака «добыча измерена и равна нулю», которого во входе нет;
    у пяти действующих рудников консервации в публикациях нет. Годы ДО
    первого известного значения дают `0.0` — там действительно неизвестно
    ничего, и такой год поднимает `ValueError` (см. ниже), а не молча
    выпадает из плана.

    ГОД ОСТАЁТСЯ В ПЛАНЕ. Если в году ни у одного актива нет ненулевого веса,
    `reconcile` поднял бы `ValueError` о неопределённых весах. Такой год не
    пропускается молча и не дополняется выдуманной добычей: разрыв в середине
    горизонта сдвинул бы дисконтирование, поэтому это ошибка входа с
    названием года. Для активов LOM этого не случается: 2025-й — последний
    факт, и он же первый год плана.

    Актив, у которого добыча отсутствует во ВСЕХ годах его горизонта, не
    даёт ни одной строки, и такой актив называется в `LAST_SKIPPED_ASSETS` —
    списке пар `(asset, причина)`, который ДОСТУПЕН ТЕСТУ, а не только
    печати. Печатать русскую заметку обязан вызывающий (`main()`, Task 10):
    библиотечная функция, пишущая в stdout, не тестируется, а причина
    пропуска проверяема именно в списке. Отличие от Go-сида осознанно: там
    отсутствие факта — `nil` строк, а здесь актив известен, поэтому пропуск
    ещё и НАЗВАН. Список перезаписывается каждым вызовом, поэтому «не
    залипает» между вызовами.

    ОТБОР АКТИВА — ПО ВСЕМУ ГОРИЗОНТУ, А ГОДЫ — ПО ГРАНИЦАМ СРОКА СЛУЖБЫ.
    `production` не обязан содержать все годы горизонта: брифовый тест подаёт
    `{2021…2030}`, и у Наталки (24 года) годов после 2030-го там просто нет —
    их закрывает плато. Актив не участвует в году, который вышел за его
    `MINE_LIFE_YEARS` (иначе строк было бы больше 77).
    """
    global LAST_SKIPPED_ASSETS

    production = production or {}
    group_anchor = float(group_aisc)
    skipped: list[tuple[str, str]] = []

    def horizon(asset: str):
        return range(PLAN_BASE_YEAR, PLAN_BASE_YEAR + MINE_LIFE_YEARS[asset])

    def observed(asset: str, year: int) -> float:
        """Добыча года как она пришла во входе (без плато)."""
        return float((production.get(asset) or {}).get(year, 0.0))

    def has_any_production(asset: str) -> bool:
        return any(observed(asset, year) != 0.0 for year in horizon(asset))

    active: list[str] = []
    for asset in ASSETS:
        if asset not in production:
            skipped.append((
                asset,
                "добыча отсутствует во входных данных "
                f"(нет ключа {asset!r} в production)",
            ))
            continue
        if not has_any_production(asset):
            skipped.append((
                asset,
                "добыча за весь горизонт 2025–"
                f"{PLAN_BASE_YEAR + MINE_LIFE_YEARS[asset] - 1} нулевая или не найдена",
            ))
            continue
        if asset not in tcc_2025:
            raise ValueError(
                f"актив {asset} есть в добыче, но у него нет якоря TCC 2025: "
                f"эскалировать нечего"
            )
        if asset not in r_bar:
            raise ValueError(
                f"актив {asset} есть в добыче, но у него нет откалиброванного "
                f"клина r_bar: увязка невозможна"
            )
        active.append(asset)

    LAST_SKIPPED_ASSETS = list(skipped)
    if not active:
        return []

    rows: list[dict] = []
    last_year = max(PLAN_BASE_YEAR + MINE_LIFE_YEARS[asset] - 1 for asset in active)
    # Увязка идёт ПО ГОДАМ: веса — внутри одного года.
    for year in range(PLAN_BASE_YEAR, last_year + 1):
        factor = escalation_factor(PLAN_BASE_YEAR, year)
        target = group_anchor * factor
        tcc_year: dict[str, float] = {}
        weights: dict[str, float] = {}
        r_bar_year: dict[str, float] = {}
        for asset in active:
            if year not in horizon(asset):
                # Год за пределами срока службы актива: у Наталки горизонт
                # длиннее, но Олимпиада его не получает — иначе в плане
                # появились бы строки после отработки, а их число было бы
                # больше 77.
                continue
            tcc_year[asset] = tcc_2025[asset] * factor
            weights[asset] = _plateau_production(observed, asset, year)
            r_bar_year[asset] = r_bar[asset]
        if not tcc_year:
            continue
        if sum(weights.values()) == 0.0:
            # До первого факта добычи во входе не известно ничего: доля
            # веса нулевая у всех, увязка не определена. Молча пропустить
            # базовый год нельзя — весь горизонт сдвинулся бы, а строк было
            # бы меньше 77.
            raise ValueError(
                f"в {year} ни у одного актива плана нет добычи: "
                f"веса увязки w_i = production_i / Σ production_j "
                f"не определены (во входе нет факта до {year} включительно)"
            )
        aisc_year, _, _ = reconcile(tcc_year, r_bar_year, weights, target)
        for asset in tcc_year:
            aisc = aisc_year[asset]
            tcc = tcc_year[asset]
            production_koz = weights[asset]
            rows.append({
                "asset": asset,
                "year": year,
                "production_koz": production_koz,
                "tcc": tcc,
                "aisc": aisc,
                "capex_sustaining": sustaining_capex_mn(aisc, tcc, production_koz),
            })

    # Порядок строк — по активу, затем по году: CSV и SQL Task 9 выводят их
    # подряд, и «Олимпиада 2025–2034, затем Благодатное 2025–2037» читается
    # глазом; при обходе по годам активы перемешались бы.
    rows.sort(key=lambda row: (ASSETS.index(row["asset"]), row["year"]))
    return rows


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


# --- Факт добычи из ClickHouse ------------------------------------------------

# Имя метрики факта добычи в датапаке Polyus. Дословно совпадает с
# `actualProductionMetric` из `dcf/mine_plans_import.go`: это колонка
# `databook_polyus.name`. Метрика привязана к АКТИВУ (OLIMPIADA, NATALKA, …).
#
# Брать вместо неё релизную `company_financials.gold_output` нельзя: та —
# сводка по Группе, и «факт добычи актива» оказался бы равен добыче всей
# Группы, причём без единой ошибки или предупреждения.
ACTUAL_PRODUCTION_METRIC = "Total Dore gold output"

# Метка периода в колонке `data`. Год берётся из колонки `date` через
# `toYear(date)`, а метка служит ТОЛЬКО фильтром: у ANNUAL `date` — это
# 1 декабря отчётного года, и извлекать год из подписи периода значило бы
# повторить дефект «период взят из label, а не из datum».
ACTUAL_PRODUCTION_PERIOD = "ANNUAL"

# Порт HTTP-интерфейса ClickHouse. `ch_config()` отдаёт ДВА порта, и это не
# взаимозаменяемые значения: `port` (9000) — нативный протокол, `http_port`
# (8123) — HTTP. `urllib.request` говорит по HTTP, поэтому на нативном порту
# он бы завис или упал. Для чтения используется ТОЛЬКО `http_port`.
DEFAULT_CH_HTTP_PORT = "8123"

# Форма имени актива для подстановки в литерал SQL: заглавные латинские буквы,
# цифры и подчёркивания. Проверка ФОРМЫ, а не членства в `ASSETS`: датапак
# содержит больше таблиц, чем пять рудников LOM (ZAPADNOYE, TITIMUKHTA, …).
ASSET_LITERAL = re.compile(r"^[A-Z][A-Z0-9_]*$")

# Псевдоним: HTTP-ошибка нужна вызывающему коду (и тестам) для подмены
# транспорта без импорта `urllib.error`.
HTTPError = urllib.error.HTTPError

# Псевдоним колонки в SELECT. Имя фиксировано: разбор строк идёт по ключу `v`,
# и без явного `AS v` ClickHouse вернул бы ключ-выражение
# (`argMax(value, date)`), который пришлось бы угадывать.
ACTUAL_PRODUCTION_COLUMN = "v"

# Формат ответа HTTP-интерфейса. По умолчанию ClickHouse отдаёт
# `TabSeparated`, а НЕ JSON: без явного `FORMAT JSON` разбор ответа падал бы на
# первом же запросе. Формат задаётся в тексте запроса, а не заголовком
# `X-ClickHouse-Format`, чтобы запрос оставался самодостаточным.
ACTUAL_PRODUCTION_FORMAT = "JSON"

# Таймаут HTTP-запроса, секунды. Значение конечно намеренно: без него
# недоступный сервер подвесил бы весь расчёт.
CH_QUERY_TIMEOUT_SECONDS = 30


class ClickHouseError(Exception):
    """Сбой чтения из ClickHouse (транспорт, статус ответа, разбор JSON).

    Текст ошибки ВСЕГДА проходит через `mask()`: сервер отражает текст запроса
    и заголовки в теле ответа, и без маскирования пароль из basic auth мог бы
    попасть в лог или в отчёт вместе с диагностикой.
    """


def production_sql(asset: str, year: int) -> str:
    """SQL для факта годовой добычи актива, тыс. унц (koz).

    Предикат дословно повторяет `actualProductionSelect` из
    `dcf/mine_plans_import.go` — один и тот же факт не должен читаться двумя
    источниками двумя разными способами:

        SELECT argMax(value, date) AS v
        FROM databook_polyus FINAL
        WHERE table = '<ASSET>'
          AND name = 'Total Dore gold output'
          AND data = 'ANNUAL'
          AND toYear(date) = <YEAR>
        FORMAT JSON

    ДВА ДОБАВЛЕНИЯ К ПРЕДИКАТУ ИМПОРТЁРА, и оба обязательны именно для HTTP:

    * `AS v` — имя колонки фиксировано, иначе ClickHouse вернул бы ключом
      само выражение `argMax(value, date)`, которое пришлось бы угадывать при
      разборе;
    * `FORMAT JSON` — HTTP-интерфейс по умолчанию отвечает `TabSeparated`, а не
      JSON. Без явного формата разбор падал бы на первом же запросе (проверено
      на живом сервере: `argMax` вернул `1229.3` строкой, а не объектом JSON).

    ПОЧЕМУ `FINAL`. `databook_polyus` — `ReplacingMergeTree` по
    `(table, name, data, date)`: без FINAL один год мог бы прийти несколько раз
    с разными версиями значения, и агрегат взял бы не тот слой. Ограничение
    `table` сужает чтение всем партициям (в ключе сортировки `table` идёт
    первым); предикат `toYear(date)` индексом не покрыт.

    ПОДСТАНОВКА ЗНАЧЕНИЙ. Имена активов приходят из датапака поимённо, и
    списком `ASSETS` (пять рудников LOM) он НЕ исчерпывается: в нём есть и
    другие таблицы (ZAPADNOYE, TITIMUKHTA, …), законно нулевые по этой метрике.
    Поэтому проверяется не членство в `ASSETS`, а ФОРМА имени: заглавные буквы,
    цифры и подчёркивания. Так произвольная строка — включая `'; DROP TABLE …`
    или перевод строки — в литерал запроса попасть не может даже случайно, а не
    только «по недосмотру» вызывающей стороны. Год проверяется на разумный
    диапазон из тех же соображений: отрицательный или нецелый год — ошибка
    вызывающей стороны, а не запрос.
    """
    if not isinstance(asset, str) or not ASSET_LITERAL.match(asset):
        raise ValueError(
            f"недопустимое имя актива {asset!r}: ожидаются заглавные буквы, "
            f"цифры и подчёркивания (например, OLIMPIADA или NATALKA)"
        )
    if not isinstance(year, int) or isinstance(year, bool):
        raise ValueError(f"год должен быть целым числом, получено {year!r}")
    if not 1900 <= year <= 2100:
        raise ValueError(f"год вне разумного диапазона 1900..2100: {year}")
    literal = asset.replace("'", "''")
    return (
        f"SELECT argMax(value, date) AS {ACTUAL_PRODUCTION_COLUMN}\n"
        "FROM databook_polyus FINAL\n"
        f"WHERE table = '{literal}'\n"
        f"  AND name = '{ACTUAL_PRODUCTION_METRIC}'\n"
        f"  AND data = '{ACTUAL_PRODUCTION_PERIOD}'\n"
        f"  AND toYear(date) = {year}\n"
        f"FORMAT {ACTUAL_PRODUCTION_FORMAT}"
    )


def parse_ch_json(payload: str) -> list:
    """Разбирает JSON-ответ ClickHouse в список строк-словарей.

    Формат ответа — `{"data":[{...}, ...]}` (плюс служебные секции, которые
    здесь не нужны). Отсутствие секции `data` равнозначно отсутствию строк:
    так выглядит пустой агрегат и ответ на служебные запросы, и это НЕ ошибка.

    Голый `json.JSONDecodeError` не годится как диагностика: он не называет
    источник (кто именно прислал мусор) и уносит в текст сырой payload,
    который при HTML-странице логина содержит не пароль, а форму входа —
    бесполезную для разбора и опасную для лога. Поэтому поднимается
    `ClickHouseError` с позицией сбоя и длиной ответа, но БЕЗ самого тела.
    """
    try:
        parsed = json.loads(payload)
    except (json.JSONDecodeError, ValueError) as exc:
        raise ClickHouseError(
            f"ответ ClickHouse не разобран как JSON: {exc.msg} "
            f"(строка {exc.lineno}, колонка {exc.colno}; длина ответа {len(payload)} байт)"
        ) from exc
    if not isinstance(parsed, dict):
        raise ClickHouseError(
            f"ответ ClickHouse — не объект JSON, а {type(parsed).__name__}"
        )
    rows = parsed.get("data", [])
    if not isinstance(rows, list):
        raise ClickHouseError(
            f'секция "data" ответа ClickHouse — не массив, а {type(rows).__name__}'
        )
    return rows


def _error_body(exc) -> str:
    """Тело HTTP-ошибки в виде текста, если оно вообще есть.

    `urllib.error.HTTPError` — и файлоподобный объект (`read()`), и исключение
    с атрибутом `fp`. Реальный ответ приходит байтами, но подменённый в тесте
    транспорт может положить готовую строку. Оба варианта разбираются, чтобы
    диагностика не терялась из-за формы носителя, а ЛЮБОЙ другой сбой чтения
    тела не заменял исходную ошибку новой.
    """
    fp = getattr(exc, "fp", None)
    try:
        raw = exc.read() if fp is not None else ""
    except Exception:  # noqa: BLE001 — тело необязательно, исходная ошибка важнее
        return ""
    if raw is None:
        return ""
    if isinstance(raw, bytes):
        return raw.decode("utf-8", errors="replace")
    return str(raw)


def ch_query(sql: str) -> list:
    """Выполняет SQL через HTTP-интерфейс ClickHouse и возвращает строки.

    POST на `http://{host}:{http_port}/` с basic auth. Берётся именно
    HTTP-порт из `ch_config()["http_port"]` (по умолчанию 8123), а НЕ
    `port` (нативный протокол, 9000): `urllib.request` говорит по HTTP, и на
    нативном порту соединение повисло бы.

    Basic auth — требование HTTP-интерфейса; пароль уходит в заголовке
    `Authorization`, а не в URL, поэтому он не попадает в текст ошибок
    `urllib` (те печатают URL, но не заголовки). Дополнительно КАЖДЫЙ текст
    ошибки проходит через `mask()` — на случай, когда сервер отражает запрос
    или заголовки в теле ответа.

    Любой не-200 статус и ЛЮБОЙ сбой транспорта поднимают `ClickHouseError`.
    Обработчиков ровно два, и порядок обязателен:

    1. `HTTPError` — ПЕРВЫМ, потому что только у него есть читаемое тело с
       кодом и текстом ошибки; широкая ветка ниже его тела не увидит;
    2. `OSError` — вторым, и им покрывается весь остальной транспорт.
       `URLError` (обёртка `urllib` вокруг сбоя соединения) — уже подкласс
       `OSError`, так что отдельная ветка для него избыточна. Важнее другое:
       НЕ всякий сбой транспорта приходит обёрнутым в `URLError`.
       `http.client.RemoteDisconnected` наследуется через
       `ConnectionResetError → ConnectionError → OSError`, а `BadStatusLine` —
       через `HTTPException`, и обработчики `HTTPError`/`URLError` пропускали
       бы их сырыми. Проверено на живом сервере: запрос к недостижимому хосту
       поднимал голый `RemoteDisconnected` в обход обоих `except`.
    """
    config = ch_config()
    password = config["password"]
    # ВАЖНО: HTTP-порт (`http_port`, 8123), а не нативный (`port`, 9000).
    # Этот инвариант пинуется тестом, который перехватывает URL запроса
    # (`test_query_targets_http_port_not_native_port`): подмена на `port`
    # обязана ломать набор тестов.
    url = f"http://{config['host']}:{config['http_port']}/"
    request = urllib.request.Request(url, data=sql.encode("utf-8"), method="POST")
    request.add_header("Content-Type", "text/plain; charset=utf-8")
    token = base64.b64encode(
        f"{config['user']}:{password}".encode("utf-8")
    ).decode("ascii")
    request.add_header("Authorization", f"Basic {token}")
    try:
        with urllib.request.urlopen(request, timeout=CH_QUERY_TIMEOUT_SECONDS) as response:
            body = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        # Тело ответа — главный источник диагностики (ClickHouse кладёт туда
        # код и текст ошибки), но именно оно может содержать пароль: сначала
        # маскируем, потом кладём в сообщение. Ветка стоит ПЕРВОЙ: широкий
        # `OSError` ниже — её подкласс, и тела у него уже нет.
        detail = _error_body(exc).strip() or str(exc.reason or "")
        raise ClickHouseError(
            "ClickHouse ответил статусом "
            + mask(password, f"{exc.code} на запрос к {url}: {detail}")
        ) from exc
    except OSError as exc:
        # Весь остальной транспорт: URLError (обёртка urllib), обрывы
        # соединения (ConnectionError/RemoteDisconnected), таймауты
        # (TimeoutError), ошибки DNS и «сброс пира» — все они OSError.
        # Сообщение всё равно проходит через mask(): пароля в этих путях нет
        # (проверено на живом сервере — он в заголовке, не в URL и не в
        # reason), но гарантия должна пережить будущие правки, а не держаться
        # на сегодняшнем знании.
        reason = getattr(exc, "reason", None) or str(exc)
        raise ClickHouseError(
            mask(password, f"ClickHouse недоступен по {url}: {reason}")
        ) from exc
    return parse_ch_json(mask(password, body))


def read_actual_production(assets: list, years: tuple) -> dict:
    """Факт годовой добычи по активам и годам, тыс. унц (koz).

    Результат: `{ASSET: {year: float}}` — присутствуют ВСЕ пары из
    декартова произведения `assets × years`, даже если строки нет.

    ОТСУТСТВИЕ СТРОКИ — НОРМАЛЬНЫЙ РЕЗУЛЬТАТ `0.0`, А НЕ ОШИБКА. Так же
    ведёт себя `argMax` по пустому множеству (ноль/NULL), и так же поступает
    Go-импортёр (`readActualProduction` в `dcf/mine_plans_import.go`): в
    датапаке есть активы законно нулевые (TITIMUKHTA, ZAPADNOYE), и краснить
    чтение на отсутствии строки значило бы считать характеристику источника
    за сбой. Ноль и NULL здесь неразличимы НАМЕРЕННО — ровно как в импортёре;
    условие «есть ли факт» применяет вызывающий.
    """
    production: dict = {}
    for asset in assets:
        per_year: dict = {}
        for year in years:
            rows = ch_query(production_sql(asset, year))
            value = rows[0].get("v") if rows else None
            per_year[year] = float(value) if value is not None else 0.0
        production[asset] = per_year
    return production


def main() -> int:
    """Печатает краткую сводку по конфигурации и границам расчёта.

    Полный прогон (калибровка → чтение факта → увязка → запись SQL/CSV)
    собирается в Task 10. Здесь намеренно нет ни записи файлов, ни
    валидации: печать сводки — единственный побочный эффект, поэтому
    `make`-запуск ничего не перезаписывает случайно.
    """
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
    print(f"  эскалация затрат: 2025–2027 {ESCALATION[2025]:.0%}/год, "
          f"с 2028 {ESCALATION[2028]:.0%}/год "
          f"(константы, не измеренный ИПЦ; множитель 2025→2034 "
          f"{ESCALATION_2025_TO_2034:.4f})")
    print(f"  факт добычи читается за годы по {ACTUAL_PRODUCTION_LAST_YEAR} "
          f"включительно; далее — плато последнего факта")
    print(f"  фикстуры: {FIXTURE_DIR.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
