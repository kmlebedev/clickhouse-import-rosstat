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
import csv
import json
import os
import re
import statistics
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import date
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

# Окна консервации карьера: актив → `(первый год, последний год)`, ВКЛЮЧИТЕЛЬНО.
# Данные, а не ветвь логики: новый рудник объявляет консервацию здесь, не трогая
# `build_plan_rows` (та же дисциплина, что у `ESCALATION` и `MINE_LIFE_YEARS`).
#
# В годы окна `production_koz` равно НУЛЮ, но строка года ОСТАЁТСЯ в плане.
# Это не «нет строки», а «добычи нет»: пропуск годов сдвинул бы нумерацию лет
# плана и дисконтирование относительно реального календаря — ровно тот дефект,
# от которого предостерегает комментарий к `HaltFrom/HaltTo` в
# `dcf/mine_plans_seed.go`. Остальные колонки строки (эскалированный `tcc`,
# увязанный `aisc`) заполняются как в обычный год: строка настоящая, нулевая
# только добыча.
#
# Источник чисел — Go-сид, дословно (`dcf/mine_plans_seed.go`,
# `polyusAssetPlans`): у ВЕРНИНСКОГО `HaltFrom: 2024, HaltTo: 2028`. Начало
# окна (2024) лежит ДО первого года плана (2025) и потому на план не влияет —
# оно сохранено здесь дословно, чтобы окно читалось против источника без
# арифметики. Пересечение же окна с горизонтом даёт 2025–2028 (см.
# `halted_years`).
#
# ЗАЧЕМ ЭТО ВООБЩЕ НУЖНО. Карьер Вернинского с сентября 2024 законсервирован,
# а Вернинская ЗИФ перерабатывает руду Сухого Лога в рамках ОПР (Годовой обзор
# 2025, стр. 5, сноска 1; стр. 34–35). Факт добычи из `databook_polyus` не
# отличает «добыча измерена и равна нулю» от «строки нет», поэтому годовое
# плато без явного окна растянуло бы последний факт ПРОИЗВОДЯЩЕГО года на годы
# консервации и выдумало бы добычу, которой не было (см. `_plateau_production`).
HALT_WINDOWS = {
    "VERNINSKOYE2": (2024, 2028),
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
# `rosstat_ipc_mes` лежит в БД, но движок его не читает (`Params.Ipc` пуст), и
# `read_actual_production` читает из `databook_polyus`, а не оттуда.
#
# ЧИСЛА. Темп 6 % — верхняя часть целевого коридора ЦБ РФ (4 %); узловая
# точка 2028 — первый год ПОСЛЕ последнего, по которому есть факт добычи
# (`ACTUAL_PRODUCTION_LAST_YEAR`), после чего темп снижается до 4 %. Это
# НЕ измеренный ИПЦ, а допущение политики затрат, названное здесь, чтобы его
# можно было заменить чтением `rosstat_ipc_mes` правкой ОДНОГО словаря: формула
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
    # дробление по «точка + пробел» безопасно и не рвёт суммы. ЭТОТ ЖЕ шаблон
    # использует `extract_prior_year_citations`: деление на предложения общее,
    # но правила года у них разные (здесь — приоритет своего года без запрета
    # чужого, там — требование совпадения с годом отчёта).
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
    # Тот же шаблон дробления, что в `_block_values`, — общая граница предложения;
    # правила года, напротив, РАЗНЫЕ: здесь год предложения обязан совпадать с
    # годом отчёта (иначе цитата не привязана), там чужой год лишь понижает
    # приоритет, но предложение не отбрасывается.
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


class CorruptFixtureError(Exception):
    """В набор фикстур просочился намеренно испорченный `*_corrupt.pdf`.

    Поднимается только там, где испорченный файл был передан ЯВНО (в
    автообнаружении он отфильтрован по построению). Участие испорченного
    документа в конвейере означало бы, что его подменённое число попало в
    калибровку: перекрёстная сверка такое ловит, но лишь когда рядом есть
    второй, ЦЕЛЫЙ документ с тем же годом, — а убери его, и порча пройдёт
    молча. Поэтому отказ здесь, а не надежда на сверку.
    """


def discover_fixtures() -> dict[int, Path]:
    """Фикстуры пресс-релизов по отчётному году, БЕЗ испорченных файлов.

    ЕДИНСТВЕННЫЙ путь автообнаружения в скрипте: его использует и `build_series`
    по умолчанию, и `main()`. Второй фильтр «на месте» (`_is_corrupt_fixture` в
    вызывающем) означал бы два места, где записано правило, и правка одного из
    них молча вернула бы испорченный файл в конвейер.

    Ключ — год из имени (`fy(\\d{4})`). Файл без года в имени пропускается:
    безымянный PDF не даёт отчётного года, и подставлять его годом по порядку
    значит гадать. Сортировка по пути фиксирует порядок обхода, а словарь — по
    возрастанию года, чтобы первым в сверке был более ранний документ.
    """
    fixtures: dict[int, Path] = {}
    for path in sorted(FIXTURE_DIR.glob("polyus_pr_4q_fy*.pdf")):
        if _is_corrupt_fixture(path):
            continue
        match = re.search(r"fy(\d{4})", path.stem)
        if match:
            fixtures[int(match.group(1))] = path
    return fixtures


def assert_no_corrupt_fixtures(fixtures: dict) -> None:
    """Отказывает, если в явно переданном наборе есть `*_corrupt.pdf`.

    Защита второго слоя. Первый — сам `discover_fixtures`, который испорченные
    файлы не возвращает; но `main()` обязан явно проверить набор, чтобы будущая
    правка (например, явный словарь фикстур для отладки) не скормила порчу
    калибровке незаметно. Проверка идёт по ЗНАЧЕНИЯМ словаря, а не по его ключам:
    год можно назвать любым, а испорченность живёт в имени файла.
    """
    corrupt = [path.name for path in fixtures.values() if _is_corrupt_fixture(path)]
    if corrupt:
        raise CorruptFixtureError(
            "испорченные фикстуры недопустимы в конвейере калибровки: "
            + ", ".join(sorted(corrupt))
            + " — они существуют только для регрессионного теста перекрёстной "
            "сверки, и подменённое в них число исказило бы наблюдения"
        )


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
    АВТООБНАРУЖЕНИИ фикстур (`fixtures is None`, набор собирает
    `discover_fixtures`). Если файл передан явно в словаре, он участвует: именно
    так строится регрессионный тест на испорченную пару. Явный словарь — всегда
    намерение вызывающей стороны, и подменять его догадкой «наверное, это
    случайная порча» нельзя. Конвейер же (`main()`) идёт ТОЛЬКО через
    автообнаружение и дополнительно отказывается явным
    `assert_no_corrupt_fixtures`.
    """
    if fixtures is None:
        fixtures = discover_fixtures()

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
    наблюдение: делить на него нельзя. Поднимается `CalibrationError`, а не
    голый `ValueError`: по контракту модуля непригодность калибровочной серии —
    именно этот класс, и `calibrate` пропускает исключение наружу без обёртки,
    так что голый `ValueError` дошёл бы до вызывающей стороны анонимно.
    """
    if tcc <= 0:
        raise CalibrationError(
            f"TCC должен быть положительным, получено {tcc} — "
            f"наблюдение непригодно для калибровки клина"
        )
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
    """Клин поддержания в МЛН USD: `(aisc − tcc) · production_koz / 1000`.

    Единицы — здесь и только здесь, потому что ошибка в них даёт NPV, неверный
    в тысячи раз:

        koz × USD/oz = тыс. USD;  / 1000 → МЛН USD.

    Проверка из брифа: `(950 − 450) · 1400 / 1000 = 700.0` млн USD.

    ⚠ ЭТУ ВЕЛИЧИНУ НЕЛЬЗЯ КЛАСТЬ В `capex_sustaining` СТРОКИ ПЛАНА. Полюс
    определяет AISC как «TCC plus … sustaining capital expenditures …», то есть
    клин УЖЕ ВНУТРИ `aisc`, а движок (`dcf/model.go`) вычитает и `aisc` (в
    выручке), и `capex_sustaining` (в `opCash`) — положить сюда клин значит
    списать поддержание дважды. Подробный разбор и числа — в докстроке
    `build_plan_rows` (раздел «CAPEX_SUSTAINING = 0 ПО КОНТРАКТУ»).

    Функция оставлена — арифметика и единицы верны, и величина нужна: она
    попадает в строку плана ключом `wedge_mn_usd` и выводится в CSV как справка
    (`wedge_usd_per_oz`, `wedge_mn_usd`). Единая доля `r̄_i` покрывает всю
    дельту `AISC − TCC` — публичного разбиения «поддержание / SG&A» по активам
    не существует (спека §3.3, план §3.6 п. 2), поэтому это весь клин целиком,
    а не его capex-часть.
    """
    return (aisc - tcc) * production_koz / 1000.0


# Наблюдаемые выходы последнего вызова `build_plan_rows`, кроме самих строк.
# Оба перезаписываются ЦЕЛИКОМ в начале каждого вызова (см. докстроку
# `build_plan_rows`) — это контракт для вызывающего и для тестов, а не отладка.
#
# `LAST_SKIPPED_ASSETS` — пары `(asset, причина)` по активам, не давшим строк.
LAST_SKIPPED_ASSETS: list[tuple[str, str]] = []
#
# `LAST_YEAR_RECONCILIATION` — по годам плана: `(групповая цель USD/oz,
# остаток USD/oz, множитель клина m)`. Невязка — ЕДИНСТВЕННЫЙ сигнал, что
# цель недостижима и `m` прижат к нулю: строки при этом структурно корректны
# (`aisc >= tcc` выполняется на прижатом полу), и молча принять это за
# успешную увязку нельзя.
LAST_YEAR_RECONCILIATION: dict[int, tuple[float, float, float]] = {}


def halted_years(asset: str) -> frozenset:
    """Годы консервации актива внутри планового горизонта, включительно.

    `HALT_WINDOWS[asset] = (первый, последний)` — данные, объявленные рядом с
    `MINE_LIFE_YEARS`. Возвращаются только годы, ПОПАДАЮЩИЕ в горизонт
    `PLAN_BASE_YEAR … PLAN_BASE_YEAR + MINE_LIFE_YEARS[asset] − 1`:
    у Вернинского окно начинается в 2024-м, а план — с 2025-го, и 2024 в план
    не попадает (его там и не должно быть — это факт, а не план).

    Возврат — `frozenset`, а не `range`: членство проверяется в горячем цикле
    `build_plan_rows` по годам, и хеш-множество читается как «является ли год
    консервации», без арифметики границ на каждом шаге.
    """
    if asset not in HALT_WINDOWS:
        return frozenset()
    first, last = HALT_WINDOWS[asset]
    last_plan_year = PLAN_BASE_YEAR + MINE_LIFE_YEARS[asset] - 1
    return frozenset(
        year for year in range(first, last + 1)
        if PLAN_BASE_YEAR <= year <= last_plan_year
    )


def _plateau_production(observed, asset: str, year: int) -> float:
    """Добыча года с плато: последний известный уровень вместо пропуска.

    `observed(asset, year)` — то, что реально пришло во входе (в том числе
    `0.0` как «строки нет»). Если год известен, возвращается он; если нет —
    ближайший ПРЕДЫДУЩИЙ известный год актива; если известных годов раньше
    нет вовсе — `0.0`.

    Почему не «просто 0.0 за неизвестный год»: `read_actual_production`
    отдаёт `0.0` и для отсутствующей строки, и для честного нуля, а
    публикации не покрывают все годы LOM. Ноль вместо плато обнулил бы вес
    актива в увязке (и, вместе с ним, весь отток года по этому активу).

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
    `aisc`, `capex_sustaining`, `wedge_mn_usd`. Единицы: `production_koz` —
    тыс. тройских унций, `tcc`/`aisc` — USD/oz, `capex_sustaining` и
    `wedge_mn_usd` — МЛН USD.

    CAPEX_SUSTAINING = 0 ПО КОНТРАКТУ — ДВОЙНОГО СЧЁТА БЫТЬ НЕ ДОЛЖНО.
    Полюс определяет AISC прямо, дословно (пресс-релиз 4Q/FY2021,
    `scripts/testdata/polyus_pr_4q_fy2021.pdf`): «AISC is defined by the group
    as TCC plus selling, general and administrative expenses, stripping activity
    asset additions, **sustaining capital expenditures**, unwinding of discounts
    on decommissioning liabilities…». То есть затраты поддержания УЖЕ ВНУТРИ
    `aisc`. Движок же (`dcf/model.go`) читает обе величины РАЗДЕЛЬНО:

        aisc := escalate(y.AISC, ipc, yearIndex)
        revenue := y.ProductionKoz * (gold - aisc)
        opCash := revenue - ndpi - (y.CapexSustaining+y.CapexProject)*thousand

    Клин `(aisc − tcc)` вычитается в `revenue`, и он же вычитался бы второй раз
    в `opCash`, если положить его в `capex_sustaining`. Проверено на живом сиде:
    для Олимпиады 2025 это 1053.3 млн USD против опубликованных Go-сидом 450 —
    завышение оттока примерно на 603 млн USD в год по одному руднику, и все NAV
    из этого сида систематически занижены. Поэтому `capex_sustaining` — НОЛЬ, а
    клин живёт отдельным ключом `wedge_mn_usd` и выводится в CSV для глазной
    сверки (колонки `wedge_usd_per_oz`/`wedge_mn_usd`), но НЕ в отток модели.
    `capex_project` тоже ноль: ни у одного из пяти рудников LOM нет проекта
    развития. Тест `TestPlanRows.test_every_row_has_zero_sustaining_capex`
    пинует инвариант, чтобы правка не вернула двойной счёт.

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
    который на самом деле работает, и уронить вес актива в увязке, а вместе с
    ним и выручку года. Поэтому актив ведёт годовое плато: добыча года — это
    значение года, а если его нет, то ближайшее известное ПРЕДЫДУЩЕЕ; иными
    словами, при отсутствии годового профиля рудник держит последний факт
    (то же поведение, что у Go-сида, где плато задаётся последним фактом,
    развёрнутым на весь LOM).

    Осознанная цена этого решения: консервация карьера (нулевая добыча в
    середине срока) в годовом профиле неотличима от отсутствия данных, и такая
    строка получила бы добычу предыдущего года. Различение потребовало бы
    отдельного признака «добыча измерена и равна нулю», которого во входе нет,
    у пяти действующих рудников консервации в публикациях нет — и такого
    признака здесь НЕТ.

    ПОЭТОМУ КОНСЕРВАЦИЯ ЗАДАЁТСЯ ЯВНО, ЧЕРЕЗ `HALT_WINDOWS`. Отсутствие
    признака во входе — не повод выдумывать добычу: известные окна простоя
    объявлены ДАННЫМИ рядом с `MINE_LIFE_YEARS`, и год окна получает
    `production_koz = 0` при СОХРАНЁННОЙ строке (см. `halted_years`).
    У Вернинского это 2025–2028 (окно Go-сида 2024–2028 пересечено с планом):
    без него годовое плато растянуло бы последний производящий факт на годы
    консервации и выдумало бы добычу, которой не было. Годы ДО
    первого известного значения дают `0.0` — там действительно неизвестно
    ничего, и такой год поднимает `ValueError` (см. ниже), а не молча
    выпадает из плана.

    ГОД ОСТАЁТСЯ В ПЛАНЕ. Если в году ни у одного актива нет ненулевого веса,
    `reconcile` поднял бы `ValueError` о неопределённых весах. Такой год не
    пропускается молча и не дополняется выдуманной добычей: разрыв в середине
    горизонта сдвинул бы дисконтирование, поэтому это ошибка входа с
    названием года. Для активов LOM этого не случается: 2025-й — последний
    факт, и он же первый год плана, а консервация Вернинского (2025–2028)
    никого, кроме Вернинского, не обнуляет, поэтому веса года не вырождаются.
    Случай «в году консервируют ВСЕ активы» не выдумывает добычу и не делит на
    ноль: он поднимает то же `ValueError` с номером года, что и год без факта
    вообще — вырожденные веса ловятся ДО деления.

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

    НАБЛЮДАЕМЫЕ ВЫХОДЫ, КРОМЕ СТРОК. У функции два модульных «выхода по
    ссылке», и оба перезаписываются ЦЕЛИКОМ ПЕРВЫМ ДЕЙСТВИЕМ вызова — до
    любой валидации:

    * `LAST_SKIPPED_ASSETS` — `[(asset, причина), …]`;
    * `LAST_YEAR_RECONCILIATION` — `{год: (цель, остаток, m)}`.

    Сброс стоит ПЕРЕД валидацией НАМЕРЕННО. Если вызов падает (например,
    актив есть в добыче, а якоря TCC у него нет), `try/except` у вызывающего
    прочитал бы `LAST_SKIPPED_ASSETS` уже сброшенным, а не застрявшим от
    ПРЕДЫДУЩЕГО успешного вызова: молча отнести чужой пропуск к упавшему
    вызову — это ровно тот класс дефектов, от которого список и защищает.
    После исключения списки пусты, и это закреплено тестом
    `test_raising_call_does_not_leak_skip_list`.

    ОСТАТОК УВЯЗКИ ОБЯЗАН БЫТЬ НАБЛЮДАЕМ. `reconcile` возвращает
    `(aisc, m, residual)`, и остаток — единственный признак того, что
    групповая цель НЕДОСТИЖИМА и `m` прижат к нулю (см. правило 2 в её
    докстроке). Строки при этом выглядят безупречно: `aisc` садится на TCC,
    то есть `aisc >= tcc` выполняется и все структурные проверки проходят.
    Принять такой год за успешно увязанный нельзя, поэтому по каждому году
    сохраняются и невязка, и `m`. Для штатных якорей остаток ~0 (цель и TCC
    эскалируются ОДНИМ множителем, поэтому отношение год от года не меняется
    и режим прижима недостижим), но подменённый `group_aisc` ниже пола
    взвешенного TCC даёт ненулевой остаток — и это видно, а не тонет.
    """
    global LAST_SKIPPED_ASSETS, LAST_YEAR_RECONCILIATION

    # Сброс — ПЕРВЫМ действием, до валидации: см. «НАБЛЮДАЕМЫЕ ВЫХОДЫ» выше.
    LAST_SKIPPED_ASSETS = []
    LAST_YEAR_RECONCILIATION = {}

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
            # Год консервации: добычи нет, но строка года ОСТАЁТСЯ. Вес 0
            # честно снимает актив с увязки этого года (его доля
            # w_i = production_i / Σ production_j равна нулю), а `tcc`/`aisc`
            # ниже заполняются как в обычный год — строка настоящая, нулевая
            # только добыча. См. `HALT_WINDOWS`.
            weights[asset] = (
                0.0 if year in halted_years(asset)
                else _plateau_production(observed, asset, year)
            )
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
        aisc_year, m_year, residual_year = reconcile(
            tcc_year, r_bar_year, weights, target
        )
        # Невязка и m сохраняются по каждому году: прижатый к нулю m — это
        # недостижимая цель, и молчать об этом нельзя (см. докстроку).
        LAST_YEAR_RECONCILIATION[year] = (target, residual_year, m_year)
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
                # НОЛЬ ПО КОНТРАКТУ, А НЕ ПО РАСЧЁТУ — см. «CAPEX_SUSTAINING = 0»
                # ниже. Клин поддержания уже сидит внутри `aisc`; класть его же
                # ещё раз в `capex_sustaining` означало бы списать его дважды.
                "capex_sustaining": 0.0,
                # Клин ДЛЯ ГЛАЗ: та самая величина, которую нельзя класть в
                # `capex_sustaining`. Держится в строке, чтобы CSV мог её
                # показать, и чтобы правка `capex_sustaining` обратно на неё
                # ломала тест, а не уезжала молча.
                "wedge_mn_usd": sustaining_capex_mn(aisc, tcc, production_koz),
            })

    # Порядок строк — по активу, затем по году: CSV и SQL Task 9 выводят их
    # подряд, и «Олимпиада 2025–2034, затем Благодатное 2025–2037» читается
    # глазом; при обходе по годам активы перемешались бы.
    rows.sort(key=lambda row: (ASSETS.index(row["asset"]), row["year"]))
    return rows


# --- Сериализация плана: SQL-сид `mine_plans` и CSV для глазной сверки -------

# Компания-эмитент в каждой строке. Дословно совпадает с `assetPlanSeed.Company`
# и `seed.Company` в `dcf/mine_plans_seed.go`: тот же ключ сортировки
# `ReplacingMergeTree (company, asset, year)`, поэтому расхождение в написании
# дало бы ДВЕ строки на один год вместо перезаписи.
COMPANY = "PLZL"

# ПОЛНЫЙ список колонок INSERT — дословно порядок DDL из `dcf/schema.go`
# (`minePlansCreateTable`). Десять колонок, включая ключевые.
#
# ПОРЯДОК ЗДЕСЬ — КОНТРАКТ, А НЕ ОФОРМЛЕНИЕ. ClickHouse принимает перестановку
# однотипных Float64 молча и записал бы `tcc` в `aisc`; именно об этом
# предупреждает комментарий в `dcf/mine_plans_import.go` («Сверять глазами
# обязательно»). Здесь перестановку ловит тест, а не глаз.
MINE_PLANS_TABLE = "mine_plans"
MINE_PLANS_COLUMNS = (
    "company", "asset", "year", "production_koz", "grade_gpt",
    "tcc", "aisc", "capex_sustaining", "capex_project", "closure_costs",
)

# Размер батча INSERT по умолчанию. 500 — из плана §3.2 (`emit_sql`); весь план
# (77 строк) укладывается в ОДИН оператор, поэтому значение проверено тестом.
DEFAULT_BATCH_SIZE = 500

# Значения колонок, которые НЕ берутся из строки, а заданы политикой. Каждое
# — семантическое, а не заглушка:
#
# * `grade_gpt = None` → `NULL`. Содержания в `MinePlanYear` нет
#   (`dcf/model.go`), `npvLOM` его не читает, и Go-сид пишет `nil` безусловно
#   (`dcf/mine_plans_import.go`). Ноль здесь означал бы ИЗМЕРЕННОЕ «0 г/т».
# * `capex_project = 0`. Ни у одного из пяти рудников LOM нет проекта
#   развития: весь их capex — поддержание (`dcf/mine_plans_build.go`,
#   `ProjectCapex: 0`). Ноль — измеренное отсутствие проекта, а не «не задано».
#   Записан ЦЕЛЫМ (`int`), чтобы литерал был `0`, а не `0.0`: ClickHouse
#   приводит к Float64 сам, а в CSV и в глазах читателя «0» и «0.0» — одно и
#   то же отсутствие величины. Полная точность (`repr`) для ЭТИХ колонок
#   смысла не имеет по построению.
# * `closure_costs = 0` → «НЕ ОПУБЛИКОВАНО ПО АКТИВУ / НЕ ЗАДАНО», и НИКОГДА
#   «закрытие бесплатно». Дословно контракт `assetPlanSeed.ClosureCosts`
#   (`dcf/mine_plans_seed.go`): per-asset провижен Полюс не публикует, а
#   распределение групповой суммы не утверждено. Go-сид кладёт хвост в
#   последний год LOM ПУСТЫМ (0), и здесь колонка остаётся нулевой ВО ВСЕХ
#   годах, а не в одном: иначе форма «хвост в последнем году» была бы заявлена
#   там, где величина неизвестна.
GRADE_GPT = None
CAPEX_PROJECT = 0
CLOSURE_COSTS = 0


def sql_literal(value) -> str:
    """Литерал ClickHouse для одного значения строки плана.

    * `None` → `NULL` (отличает «не раскрыто» от измеренного нуля);
    * `str` → в одинарных кавычках с удвоением тика (`O'HARA` → `'O''HARA'`):
      литерал не должен разрывать текст запроса;
    * `bool` → `1`/`0`: в Python `True` — это `int`, и без явной ветви он
      попал бы в `repr(True)` = `'True'`, что ClickHouse не разберёт;
    * `int` → `repr`;
    * `float` → `repr`, то есть ПОЛНАЯ точность двоичного представления
      (`0.1 + 0.2` → `'0.30000000000000004'`). Формат вроде `%.6f` внёс бы
      ошибку округления, неотличимую потом от ошибки модели; а `str()` для
      float в Python 3 — это и есть `repr`, но явный `repr` фиксирует
      намерение, а не совпадение поведения.

    Не-конечные float НЕ допускаются: `NaN` и `±inf` поднимают `ValueError`.
    ClickHouse взял бы их как Float64 и записал бы строку, а `NaN`/`inf`
    разъехались бы по всему NPV (сравнения с `NaN` всегда ложны, поэтому
    `aisc < tcc` их даже не пометил бы). Молчаливая запись — недопустимый
    исход, поэтому отказ здесь, а не в валидации после.

    Важно про `assert`: проверка `NaN` в тесте (`assertNotEqual(nan, nan)`)
    ложна по определению, поэтому конечность проверяется явно
    (`math.isfinite`-эквивалент через сравнения), а не отрицанием равенства.
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        # Ветвь ОБЯЗАНА стоять до `isinstance(value, int)`: bool — подкласс int.
        return "1" if value else "0"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, int):
        return repr(value)
    if isinstance(value, float):
        if value != value:  # NaN: единственное значение, не равное себе
            raise ValueError("NaN недопустим в литерале SQL: значение не записано")
        if value == float("inf") or value == float("-inf"):
            raise ValueError(f"бесконечность недопустима в литерале SQL: {value!r}")
        return repr(value)
    raise ValueError(
        f"тип {type(value).__name__} не поддержан в литерале SQL: {value!r}"
    )


def _value_tuple(row: dict) -> str:
    """Кортеж VALUES одной строки плана — ровно 10 позиций в порядке колонок.

    Постоянные колонки подставляются здесь, а не приходят из строки: эмиттер не
    доверяет вызывающему значения, которые заданы политикой (`company`,
    `grade_gpt`, `capex_project`, `closure_costs`).
    """
    return (
        "("
        + ",".join((
            sql_literal(COMPANY),
            sql_literal(row["asset"]),
            sql_literal(row["year"]),
            sql_literal(row["production_koz"]),
            sql_literal(GRADE_GPT),
            sql_literal(row["tcc"]),
            sql_literal(row["aisc"]),
            sql_literal(row["capex_sustaining"]),
            sql_literal(CAPEX_PROJECT),
            sql_literal(CLOSURE_COSTS),
        ))
        + ")"
    )


def sql_batches(rows: list, batch_size: int = DEFAULT_BATCH_SIZE) -> list:
    """Операторы `INSERT INTO mine_plans (…) VALUES …;` батчами по `batch_size`.

    Возвращается СПИСОК готовых операторов: по одному на батч, каждый — с
    полным списком колонок и завершающей точкой с запятой, поэтому батчи можно
    склеивать и подавать `clickhouse client --multiquery` без правок.

    Пустой список строк даёт ПУСТОЙ список операторов, а не `INSERT … VALUES;`
    (это синтаксически неверный SQL). Шапка файла при этом пишется — план
    нулевой длины остаётся видимым, а не превращается в невалидный запрос.

    Порядок строк сохраняется как есть: сортировка — ответственность
    `build_plan_rows` (по активу, затем по году), и повторять её здесь значило
    бы иметь два места, где порядок задаётся.
    """
    if batch_size <= 0:
        raise ValueError(f"размер батча должен быть положительным, получено {batch_size}")
    header = (
        f"INSERT INTO {MINE_PLANS_TABLE} ({', '.join(MINE_PLANS_COLUMNS)}) VALUES"
    )
    statements: list = []
    for start in range(0, len(rows), batch_size):
        chunk = rows[start:start + batch_size]
        body = ",\n".join(_value_tuple(row) for row in chunk)
        statements.append(f"{header}\n{body};")
    return statements


# Название команды генерации сида для шапки. Пишется в файл как есть, чтобы
# строку из шапки можно было скопировать в терминал.
GENERATE_COMMAND = "make dcf-aisc"

# Шаблон шапки SQL-сида. Текст русский, как и вся документация репозитория.
# Две ПУСТЫЕ строки после `--` оставлены намеренно: шапка отделена от данных,
# и `grep -c '^INSERT'` по файлу считает только операторы.
SQL_HEADER_TEMPLATE = """\
-- Сид таблицы mine_plans: поактивный AISC Полюса, экстраполированный на LOM.
--
-- СГЕНЕРИРОВАН АВТОМАТИЧЕСКИ, ПРАВИТЬ РУКАМИ НЕЛЬЗЯ: правка будет затёрта
-- следующим прогоном. Источник чисел — калибровка по пресс-релизам Полюса
-- и факт добычи из databook_polyus; всё это делает scripts/extrapolation_aisc.py.
--
-- Команда генерации: {command}
-- Дата генерации: {generated}
-- Калибровочные годы клина: {calibration_years}
-- Множитель клина m (увязка к групповому AISC, последний год плана): {m}
-- Остаток увязки, USD/oz (факт минус цель, последний год плана): {residual}
-- Строк в сиде: {row_count}
--
-- Замечания к семантике строк (повторяют контракт dcf/mine_plans_seed.go):
--   * company = 'PLZL' у всех строк;
--   * grade_gpt = NULL — содержания в модели нет (dcf/model.go), ноль означал
--     бы измеренное «0 г/т»;
--   * capex_project = 0 — ни у одного из пяти рудников LOM нет проекта
--     развития;
--   * capex_sustaining = 0 — «ПОДДЕРЖАНИЕ УЖЕ ВНУТРИ AISC, ВТОРОЙ РАЗ НЕ
--     СПИСЫВАЕТСЯ». Полюс определяет AISC как «TCC plus … sustaining capital
--     expenditures …» (пресс-релиз 4Q/FY2021), а dcf/model.go вычитает и
--     `aisc` (в выручке), и `capex_sustaining` (в opCash) — ненулевое
--     значение здесь удвоило бы отток по руднику. Клин для глазной сверки
--     лежит в CSV (wedge_usd_per_oz, wedge_mn_usd), НЕ здесь;
--   * closure_costs = 0 означает «НЕ ОПУБЛИКОВАНО ПО АКТИВУ / НЕ ЗАДАНО»,
--     и НИКОГДА — «закрытие бесплатно»: per-asset провижен Полюс не
--     публикует, распределение групповой суммы не утверждено.
--
-- Строки писались под тем же ключом (company, asset, year), что и сид
-- dcf_mine_plans, поэтому ReplacingMergeTree перезапишет его плато
-- уточнённой структурой затрат.
--
-- ПРИМЕНЯЕТСЯ ВРУЧНУЮ, ОТДЕЛЬНЫМ ШАГОМ — и это намеренно. В `dagu/dcf.yaml`
-- этого шага НЕТ: цепочка там остаётся dcf_mine_plans → dcf_engine. Причина в
-- том, что запланированный шаг сида на staging всё равно не заработал бы
-- (там нет ни make, ни Makefile, ни этого файла — релиз кладёт только
-- README.md и dagu/*.yaml), и, что хуже, провал был бы БЕСШУМНЫМ:
-- dcf_mine_plans в той же цепочке пересеял бы все ключи из Go-снимка, расчёт
-- прошёл бы на старом клине, а лог прогона остался бы зелёным.
--
-- Поэтому ручной порядок — сначала сид, потом расчёт:
--   make ch-sql FILE=sql/dcf_mine_plans_seed.sql
--   DCF_RUN_ID=<uuid> make import STAT=dcf_engine
-- Если запустить один dcf_engine, он посчитает на плато Go-сида, а не на
-- этой структуре затрат.
{statements}"""


def write_sql(path: Path, rows: list, meta: dict) -> None:
    """Пишет SQL-сид `mine_plans`: шапка-комментарий плюс батчи INSERT.

    `meta` — словарь генерации. Обязательные ключи: `command` (команда),
    `calibration_years` (годы калибровки, iterable), `m` (множитель клина),
    `residual` (остаток увязки, USD/oz). Если ключа нет или значение `None`,
    в шапке печатается «не задано»: шапка обязана оставаться честной, а не
    подставлять ноль вместо неизвестного — ноль рядом с «остатком» читался бы
    как безупречная увязка.

    Дата берётся системная на момент записи (`date.today()`), а НЕ из `meta`:
    шапка описывает ИМЕННО этот файл, и подсунутая вызывающим дата — это ровно
    тот «ручной» вход, от которого файл и защищает.

    Каталог пути создаётся, если его нет: `sql/` может отсутствовать в свежем
    клоне, а падение на середине записи оставило бы усечённый файл, который
    `make ch-sql` потом применил бы как настоящий сид.
    """
    calibration_years = meta.get("calibration_years")
    if calibration_years is None:
        years_text = "не заданы"
    else:
        years_text = ", ".join(str(year) for year in calibration_years)

    def _shown(key):
        value = meta.get(key)
        return "не задано" if value is None else repr(value)

    header = SQL_HEADER_TEMPLATE.format(
        command=meta.get("command") or "не задана",
        generated=date.today().isoformat(),
        calibration_years=years_text,
        m=_shown("m"),
        residual=_shown("residual"),
        row_count=len(rows),
        # Операторы разделяются переводом строки: склейка без разделителя дала
        # бы `…,0,0);INSERT INTO …`, что клиент отверг бы как синтаксис.
        statements="\n".join(sql_batches(rows)),
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(header, encoding="utf-8")


# Колонки CSV. Первые десять — дословно колонки сида, чтобы файл читался рядом
# с SQL без перевода глазами; дальше — СПРАВОЧНЫЕ величины для глазной сверки.
# Они НЕ идут в модель (как и рублёвая колонка, спека §3.6 п. 3) и помечены
# именами с суффиксом `_check`, чтобы не выглядеть входом.
#
# `wedge_usd_per_oz` / `wedge_mn_usd` — клин поддержания `(aisc − tcc)` в
# USD/oz и млн USD. Это ИМЕННО ТА величина, которую модель НЕ списывает
# отдельно: она уже внутри `aisc` (см. докстроку `build_plan_rows`). Колонка
# нужна человеку для глазной сверки, поэтому и названа `wedge_*`, а не
# `capex_sustaining`: одинаковые имена заставляли бы читателя думать, что CSV и
# SQL расходятся, тогда как расходятся они ровно так, как и должны.
CSV_COLUMNS = MINE_PLANS_COLUMNS + (
    "r_bar",
    "escalation_factor",
    "aisc_per_oz_rub",
    "wedge_usd_per_oz",
    "wedge_mn_usd",
)

# Курс для СПРАВОЧНОЙ рублёвой колонки CSV, RUB/USD. В модель не идёт и на NPV
# не влияет (спека §3.6 п. 3) — нужен только человеку, чтобы «1300 USD/oz»
# читалось привычнее. Это допущение, названное здесь одним числом, а не ряд:
# ряда `usdrub_path` в расчёте нет, и второй источник курса заводить запрещено.
USD_RUB_REFERENCE = 78.0


def write_csv(path: Path, rows: list) -> None:
    """Пишет long-формат CSV: те же 10 колонок плюс справка для сверки.

    Числа пишутся `repr`-формой (`float → str`), БЕЗ разделителей разрядов и
    без запятой как десятичного знака: запятая внутри ячейки сломала бы
    `csv.DictReader`, а локализованное «1 441,3» — ещё и `float()`. Тест
    проверяет не только наличие заголовка и число строк, но и то, что каждое
    число читается обратно.

    `grade_gpt` — пустая ячейка: `NULL` выражается ОТСУТСТВИЕМ значения, а не
    строкой `NULL` (это справка для человека, а не SQL) и не измеренным нулём.

    `capex_sustaining` пишется ИЗ СТРОКИ, то есть НУЛЁМ — ровно тем, что
    списывает модель (см. докстроку `build_plan_rows`). Клин поддержания
    НЕ кладётся сюда даже как «справка»: колонка обязана совпадать с SQL, а
    иначе CSV и сид разойдутся в том, что модель реально вычитает. Клин выведен
    отдельными колонками `wedge_usd_per_oz` / `wedge_mn_usd` (см. ниже).

    Справочные колонки:

    * `r_bar` — калиброванная доля клина актива. Берётся из строки, если
      вызывающий её положил (Task 10 кладёт из `calibrate`); иначе — пустая
      ячейка, потому что выдуманное среднее выглядело бы калибровкой;
    * `escalation_factor` — множитель затрат 2025 → год строки. Считается
      `escalation_factor`, то есть тем же единственным источником эскалации
      (спека §3.4), а не второй копией ряда;
    * `aisc_per_oz_rub` — AISC в рублях по справке `USD_RUB_REFERENCE`;
    * `wedge_usd_per_oz` — `aisc − tcc`, клин поддержания в USD/oz. Он УЖЕ
      внутри `aisc`, и отдельно моделью НЕ списывается — колонка существует,
      чтобы человек видел состав затрат, а не чтобы задавать отток;
    * `wedge_mn_usd` — тот же клин в млн USD (`sustaining_capex_mn`). Берётся
      из строки, если вызывающий положил ключ `wedge_mn_usd`; иначе считается
      ВЫЗОВОМ `sustaining_capex_mn` — того же единственного источника формулы,
      а не второй её копией; инлайнового пересчёта нет. Строка `rows` из
      `build_plan_rows` уже несёт `wedge_mn_usd`, поэтому ветка «иначе» —
      страховка для вызывающего, который ключ не положил. Пустая ячейка здесь
      означала бы «клина нет», а не «не посчитали».
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(CSV_COLUMNS)
        for row in rows:
            factor = escalation_factor(PLAN_BASE_YEAR, row["year"])
            wedge_mn = row.get("wedge_mn_usd")
            if wedge_mn is None:
                wedge_mn = sustaining_capex_mn(
                    row["aisc"], row["tcc"], row["production_koz"]
                )
            writer.writerow((
                COMPANY,
                row["asset"],
                row["year"],
                repr(row["production_koz"]),
                "" if GRADE_GPT is None else repr(GRADE_GPT),
                repr(row["tcc"]),
                repr(row["aisc"]),
                repr(row["capex_sustaining"]),
                repr(CAPEX_PROJECT),
                repr(CLOSURE_COSTS),
                "" if row.get("r_bar") is None else repr(row["r_bar"]),
                repr(factor),
                repr(row["aisc"] * USD_RUB_REFERENCE),
                repr(row["aisc"] - row["tcc"]),
                repr(wedge_mn),
            ))


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

def parse_clickhouse_url(url: str) -> dict:
    """Разбирает DSN `clickhouse://[user[:password]@]host[:port]/database`.

    Это ТОТ ЖЕ формат, что разбирает `clickhouse.ParseDSN` в Go-коде проекта
    (`cmd/ingest/main.go`, `financial/db.go`), потому что `CLICKHOUSE_URL` —
    основной способ задать подключение в этом репозитории, и env-файл
    `~/.config/rosstat/env` содержит именно его, а не дискретные
    `CLICKHOUSE_HOST`/`CLICKHOUSE_USER`/`CLICKHOUSE_DATABASE`.

    Незаданные части не подставляются: возвращаются только реально
    присутствующие в DSN ключи, чтобы вызывающий мог отличить «в DSN указано»
    от «не указано вовсе» и применить свой default. Порт здесь — НАТИВНЫЙ
    (обычно 9000); HTTP-порт к нему отношения не имеет и задаётся отдельно.
    """
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in ("clickhouse", "http", "https"):
        raise ValueError(
            f"CLICKHOUSE_URL со схемой {parsed.scheme!r}: ожидается "
            f"clickhouse://[user[:password]@]host[:port]/database — {url!r}"
        )
    out: dict = {"scheme": parsed.scheme}
    if parsed.hostname:
        out["host"] = parsed.hostname
    if parsed.port:
        out["port"] = str(parsed.port)
    if parsed.username:
        out["user"] = urllib.parse.unquote(parsed.username)
    if parsed.password:
        out["password"] = urllib.parse.unquote(parsed.password)
    if parsed.path and parsed.path != "/":
        out["database"] = parsed.path.lstrip("/")
    return out


def ch_config() -> dict:
    """Параметры подключения к ClickHouse из переменных окружения.

    Секреты приходят только из env: скрипт не читает `~/.config/rosstat/env`
    сам, это делает Makefile.

    Источник адреса — `CLICKHOUSE_URL` (DSN), как во всём остальном проекте.
    Дискретные `CLICKHOUSE_HOST`/`CLICKHOUSE_PORT`/`CLICKHOUSE_USER`/
    `CLICKHOUSE_DATABASE` работают как ПЕРЕОПРЕДЕЛЕНИЕ поверх DSN: так можно
    прогнать скрипт против другого сервера, не собирая DSN.

    Пароль берётся из `CH_IMPORT_PASSWORD` либо из DSN — и НИКОГДА из
    `CLICKHOUSE_PASSWORD`. Это не придирка: `CLICKHOUSE_PASSWORD` в этом
    репозитории — пароль `kimi_reader`, read-only пользователя ВИТРИН,
    которого Makefile задаёт для `mcp-check` (`MCP_USER ?= kimi_reader`).
    Скрипту же нужно читать СЫРУЮ таблицу `databook_polyus`, закрытую для
    `kimi_reader` (`ACCESS_DENIED`), то есть ходить под другим пользователем.
    Прогон 2026-10-11 показал, чем обходится путаница: `CLICKHOUSE_URL`
    объявляет пользователя `default` БЕЗ пароля (у локального dev-сервера его
    нет), подстановка `CLICKHOUSE_PASSWORD` «на всякий случай» давала 401 —
    чужой пароль к чужому пользователю.
    """
    cfg = {"host": "localhost", "port": "9000", "user": "", "password": "",
           "database": "default", "http_port": "8123"}

    url = os.environ.get("CLICKHOUSE_URL", "")
    if url:
        from_url = parse_clickhouse_url(url)
        for key in ("host", "port", "user", "password", "database"):
            if key in from_url:
                cfg[key] = from_url[key]

    for key, env in (("host", "CLICKHOUSE_HOST"), ("port", "CLICKHOUSE_PORT"),
                     ("user", "CLICKHOUSE_USER"), ("database", "CLICKHOUSE_DATABASE")):
        value = os.environ.get(env)
        if value:
            cfg[key] = value

    cfg["password"] = os.environ.get("CH_IMPORT_PASSWORD") or cfg["password"]
    cfg["http_port"] = os.environ.get("CH_HTTP_PORT", "8123")
    return cfg


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


def ch_exec(sql: str) -> None:
    """Выполняет SQL через HTTP-интерфейс ClickHouse, не разбирая ответ.

    Пишущий двойник `ch_query`: тот же POST на `http_port` (8123, а не
    нативный 9000), тот же basic auth и тот же таймаут, но тело УСПЕШНОГО
    ответа игнорируется. Это не небрежность: на INSERT ClickHouse отвечает
    пустой строкой или служебным `Ok.`, и разбор ответа как JSON падал бы на
    каждой успешной вставке.

    Контракт ошибок совпадает с `ch_query`: не-200 и любой сбой транспорта
    (`OSError`, включая `URLError` и `RemoteDisconnected`) поднимают
    `ClickHouseError`, а каждый текст проходит через `mask()` — сервер может
    отразить запрос или заголовки в теле ошибки.
    """
    config = ch_config()
    password = config["password"]
    url = f"http://{config['host']}:{config['http_port']}/"
    request = urllib.request.Request(url, data=sql.encode("utf-8"), method="POST")
    request.add_header("Content-Type", "text/plain; charset=utf-8")
    token = base64.b64encode(
        f"{config['user']}:{password}".encode("utf-8")
    ).decode("ascii")
    request.add_header("Authorization", f"Basic {token}")
    try:
        with urllib.request.urlopen(request, timeout=CH_QUERY_TIMEOUT_SECONDS) as response:
            # Тело читается и выбрасывается: оно не нужно, но чтение
            # завершает запрос так же, как в `ch_query`.
            response.read()
    except urllib.error.HTTPError as exc:
        detail = _error_body(exc).strip() or str(exc.reason or "")
        raise ClickHouseError(
            "ClickHouse ответил статусом "
            + mask(password, f"{exc.code} на запрос к {url}: {detail}")
        ) from exc
    except OSError as exc:
        reason = getattr(exc, "reason", None) or str(exc)
        raise ClickHouseError(
            mask(password, f"ClickHouse недоступен по {url}: {reason}")
        ) from exc


# --- Поактивный якорь TCC 2025: источник плановых затрат ----------------------

# Поактивный TCC 2025, USD/oz, по каноническим именам. Источник — Годовой обзор
# ПАО «Полюс» за 2025 год, стр. 5 и 31; ГОД отчёта назван здесь, а не у каждого
# числа, потому что именно он делает снимок проверяемым (читатель откроет тот же
# PDF и сверит).
#
# ДОСЛОВНОЕ СООТВЕТСТВИЕ `dcf/mine_plans_seed.go` — НЕ СОВПАДЕНИЕ, А ТРЕБОВАНИЕ.
# Сид модели (`polyusAssetPlans`, поле `TCC`) несёт те же пять чисел, и SQL-сид
# этого скрипта применяется ПОВЕРХ Go-сида тем же ключом
# `(company, asset, year)` в `ReplacingMergeTree`. Расхождение здесь означало бы,
# что TCC плана переписывается другим TCC при том же AISC-клоне — то есть
# относительный клин `(AISC − TCC)/TCC` разъехался бы с откалиброванным, и НИ
# ОДНА проверка этого не заметила бы. Пинуется тестом.
#
# У Вернинского 735 — это TCC ИРКУТСКОЙ БЕ (бизнес-единицы), в которую входит и
# ОПР руды Сухого Лога (сноска 1 к таблице, стр. 31): собственного TCC у актива
# в обзоре нет. Пометка «прокси БУ» — в карте покрытия
# `docs/DCF_DATA_COVERAGE.md` §2, и пересказывать её здесь не нужно.
TCC_2025_USD_PER_OZ = {
    "OLIMPIADA": 773.0,
    "BLAGODATNOYE": 691.0,
    "NATALKA": 671.0,
    "VERNINSKOYE2": 735.0,
    "KURANAKH": 925.0,
}

# Порог невязки увязки: ДОЛЯ, а не USD/oz. 0.01 = 1 % группового AISC (спека
# §5, «Guard'ы: невязка < 1 %»). Порог именно относительный: абсолютный
# «±14 USD/oz» привязан к сегодняшнему якорю и молча ослаб бы, если якорь
# заменить (эрозия порога вместе с эскалацией), а доля остаётся той же.
RESIDUAL_TOLERANCE_SHARE = 0.01

# Окно допустимых плановых годов, ПОЛУИНТЕРВАЛ `[2000, 2100)`. Нижняя граница
# совпадает с окном `dcf/coverage.go`/`MinePlanYear`; верхняя — ИСКЛЮЧАЮЩАЯ
# намеренно: год 2100 отбрасывается моделью молча (с warn), и план по нему дал
# бы NPV = 0. Поэтому `2100` — ошибка, а не граница.
PLAN_YEAR_MIN = 2000
PLAN_YEAR_MAX_EXCLUSIVE = 2100

# Диапазон допустимых клиньев `r̄`, ЗАКРЫТЫЙ с обеих сторон (спека §5). Оба
# конца — не произвол: калиброванный ряд лежит в 0.488…0.755, и клин ниже 0.3
# означал бы поддержание меньше трети TCC (несовместимо с публикациями Полюса),
# выше 1.0 — удвоение TCC, тоже не наблюдённое. Порог шире данных НАМЕРЕННО:
# он ловит грубую порчу калибровки, а не сужает коридор до сегодняшнего ряда.
R_BAR_MIN = 0.3
R_BAR_MAX = 1.0


# --- Итоговая валидация таблицы плана ----------------------------------------

def validate(rows: list, cal: dict, meta: dict) -> list:
    """Guard'ы итоговой таблицы плана: СПИСОК русских проблем, `[]` — успех.

    Проверки, каждая — отдельный пункт отчёта, а не исключение: валидатор
    обязан назвать ВСЁ, что не так, одним прогоном. Таблица, падающая на первой
    ошибке, чинится итеративно по одной строке, а отчёт — это документ о
    состоянии прогона, и он должен быть полным.

        `aisc < tcc`            отрицательный клин поддержания — экономически
                                бессмыслен (это же условие прижима `m >= 0` в
                                `reconcile`, так что нарушение означает дефект
                                НЕ в увязке);
        `production_koz < 0`    отрицательная добыча не существует;
        годы вне `[2000, 2100)` модель отбрасывает молча (см. `PLAN_YEAR_MIN`),
                                и план по такому году дал бы NPV = 0;
        capex ≠ 0               ЗАЩИТА ОТ ДВОЙНОГО СЧЁТА (см. ниже);
        пропуск актива          каждый ОТКАЛИБРОВАННЫЙ актив (ключ `cal`) обязан
                                дать хотя бы одну строку: пропуск — это молча
                                выпавший из NAV рудник, а не «нет данных»;
        `r̄` вне `0.3–1.0`       калибровка испорчена (см. `R_BAR_MIN`);
        `|остаток| >= 1 %`      увязка не сошлась (см. `RESIDUAL_TOLERANCE_SHARE`).

    CAPEX ≠ 0 — ЗАЩИТА ОТ ДВОЙНОГО СЧЁТА, и она обязана жить ЗДЕСЬ, а не только
    в тестах. Полюс определяет AISC как «TCC plus … sustaining capital
    expenditures …», то есть клин поддержания УЖЕ ВНУТРИ `aisc`, а движок
    (`dcf/model.go`) вычитает и `aisc` (в выручке), и `capex_sustaining` (в
    `opCash`). Ненулевое значение здесь списывает поддержание дважды и на
    десятки процентов занижает каждый NAV из сида — ровно тот дефект, который
    уже был допущен и исправлен (см. докстроку `build_plan_rows`, раздел
    «CAPEX_SUSTAINING = 0 ПО КОНТРАКТУ»). Юнит-тесты ловят регрессию в CI, но
    `main()` обязан поймать её и на прогоне: сид со удвоенным оттоком не должен
    уезжать в репозиторий ни при каком состоянии тестов.

    Проверяются ОБЕ capex-колонки:
    `capex_sustaining != 0` — двойной счёт (см. выше);
    `capex_project != 0` — у пяти рудников LOM нет проекта развития, и
    незаявленный проектный capex так же исказил бы отток, как и поддержание.
    Обе — контрактные нули сида, поэтому проверка строгая (любое `!= 0`).

    `closure_costs` НЕ проверяется НАМЕРЕННО, хотя здесь он тоже ноль. Его ноль
    означает «НЕ ОПУБЛИКОВАНО ПО АКТИВУ / НЕ ЗАДАНО», а не «закрытие бесплатно»
    (см. контракт `assetPlanSeed.ClosureCosts`): у этого актива провижен
    неизвестен, и требование нуля утверждало бы факт о публикации, а не о
    двойном счёте. Ненулевой хвост рекультивации — законное состояние сида
    (Go-сид кладёт его в последний год), поэтому «guard на ноль» здесь отклонил
    бы корректные строки, когда распределение провижена появится.

    ПРОПУСК СЧИТАЕТСЯ ПО `cal`, А НЕ ПО ПЯТИ `ASSETS` — и это не ослабление.
    Калибровка (`cal`) — это ровно тот набор активов, который конвейер взялся
    планировать: `calibrate` обходит `ASSETS` и падает `CalibrationError`, если
    по активу не набралось наблюдений, поэтому «откалиброван» и «должен дать
    строки» — одно и то же условие. Сверка же с константой `ASSETS` ломала бы
    саму возможность вызвать `validate` на частичной таблице: и брифовый
    `test_accepts_consistent_rows` (одна строка, один актив в `cal`), и
    `test_flags_missing_asset` (пустые строки, пять активов в `cal`) обязаны
    вести себя по-разному, что возможно только при счёте по `cal`.

    ОСТАТОК СРАВНИВАЕТСЯ ПО ВЕЛИЧИНЕ, А НЕ ПО ЗНАКУ — правило контроллера из
    Задачи 6. `reconcile` считает `residual = взвешенный(факт) − цель`, поэтому
    ЦЕЛЬ СЛИШКОМ НИЗКАЯ (недостижимая, `m` прижат к нулю) даёт ПОЛОЖИТЕЛЬНЫЙ
    остаток: выданные строки стоят выше цели. Проверка вида «остаток < 0 —
    плохо» пропустила бы эту ветвь целиком, а она — единственный видимый признак
    прижатого `m` (строки при этом структурно безупречны, `aisc == tcc`).
    Поэтому только `abs`.

    ПОРОГ — ДОЛЯ ГРУППОВОГО AISC 2025, И БЕРЁТСЯ ОНА ИЗ `meta`, а не из
    константы: вызывающий может увязать план на другой якорь (`group_aisc`
    параметр `build_plan_rows`), и сверять его остаток с ЧУЖИМ якорем значило бы
    мерить не тот масштаб. Ключ `meta["group_aisc"]` необязателен; при
    отсутствии берётся `GROUP_AISC_2025_USD_PER_OZ` — опубликованный якорь
    конвейера. Остаток при этом тоже необязателен: если ключа `residual` нет
    вовсе (план не строился), проверять нечего и это НЕ ошибка валидатора — он
    проверяет то, что ему дали.

    `r̄` берётся из `cal[asset]["r_bar"]` ПО КАЖДОМУ активу из `cal`, а не
    только из встреченных в строках: иначе испорченная калибровка пропущенного
    актива осталась бы незамеченной, а «актив пропущен» и «актив пропущен с
    испорченным клином» — разные сообщения, и оба нужны.
    """
    problems: list = []

    present: set = set()
    for row in rows:
        asset = row.get("asset")
        present.add(asset)
        year = row.get("year")

        if row.get("aisc", 0.0) < row.get("tcc", 0.0):
            problems.append(
                f"{asset} {year}: aisc {row.get('aisc')} ниже tcc {row.get('tcc')} — "
                f"отрицательный клин поддержания, строка экономически бессмысленна"
            )
        if row.get("production_koz", 0.0) < 0.0:
            problems.append(
                f"{asset} {year}: отрицательная добыча "
                f"{row.get('production_koz')} koz — такой величины не существует"
            )
        if not isinstance(year, int) or not PLAN_YEAR_MIN <= year < PLAN_YEAR_MAX_EXCLUSIVE:
            problems.append(
                f"{asset}: год {year} вне окна [{PLAN_YEAR_MIN}, "
                f"{PLAN_YEAR_MAX_EXCLUSIVE}) — модель отбросила бы его молча, "
                f"и NPV по году был бы нулевым"
            )
        # Двойной счёт поддержания и незаявленный проектный capex — см. докстроку.
        if row.get("capex_sustaining", 0.0) != 0.0:
            problems.append(
                f"{asset} {year}: capex_sustaining {row.get('capex_sustaining')} "
                f"не равен нулю — ДВОЙНОЙ СЧЁТ: затраты поддержания уже внутри "
                f"aisc (Полюс определяет AISC как «TCC plus … sustaining capital "
                f"expenditures …»), а dcf/model.go вычитает и aisc (в выручке), и "
                f"capex_sustaining (в opCash) — ненулевое значение занизило бы NPV"
            )
        if row.get("capex_project", 0.0) != 0.0:
            problems.append(
                f"{asset} {year}: capex_project {row.get('capex_project')} не равен "
                f"нулю — у пяти рудников LOM нет проекта развития, весь capex идёт "
                f"в aisc/AISC, и отдельный отток здесь занизил бы NPV"
            )

    # Пропуск считается по `cal`: актив обязан дать строку ровно тогда, когда
    # конвейер его откалибровал (см. докстроку — почему не по константе ASSETS).
    missing = [asset for asset in cal if asset not in present]
    for asset in missing:
        problems.append(
            f"актив {asset} отсутствует в плане: ни одной строки — рудник молча "
            f"выпал бы из NAV"
        )
    # Обратный случай: строка ЕСТЬ у актива, которого калибровка не знает. Это
    # не «пропуск», а расхождение двух входов: клин для строки откуда-то взялся,
    # и молча принять это за норму значило бы планировать неклиброванным клином.
    unknown = [asset for asset in present if asset is not None and asset not in cal]
    for asset in unknown:
        problems.append(
            f"актив {asset} есть в строках плана, но отсутствует в калибровке: "
            f"его клин r_bar не откалиброван, план по нему не обоснован"
        )

    for asset, entry in cal.items():
        r_bar = (entry or {}).get("r_bar")
        if r_bar is None:
            problems.append(
                f"{asset}: калиброванный клин r_bar отсутствует — увязка по "
                f"этому активу не определена"
            )
            continue
        if not R_BAR_MIN <= r_bar <= R_BAR_MAX:
            problems.append(
                f"{asset}: клин r_bar {r_bar} вне допустимого диапазона "
                f"{R_BAR_MIN}–{R_BAR_MAX} — калибровка испорчена"
            )

    residual = meta.get("residual")
    if residual is not None:
        group_aisc = meta.get("group_aisc", GROUP_AISC_2025_USD_PER_OZ)
        limit = RESIDUAL_TOLERANCE_SHARE * abs(group_aisc)
        if abs(residual) >= limit:
            problems.append(
                f"остаток увязки {residual} USD/oz при цели {group_aisc} USD/oz "
                f"по модулю не ниже порога {limit} USD/oz "
                f"({RESIDUAL_TOLERANCE_SHARE:.0%} группового AISC): увязка не сошлась "
                f"(при цели ниже пола TCC множитель m прижат к нулю, и остаток "
                f"положителен)"
            )

    return problems


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


def _display_path(path: Path) -> str:
    """Путь к артефакту для печати: относительный от корня репозитория, если он
    внутри него, иначе — как есть.

    `Path.relative_to` ПОДНИМАЕТ `ValueError`, если путь вне корня, поэтому
    прямой вызов в печати ронял бы прогон на последней строке сводки — после
    того, как файлы уже записаны, то есть с уже готовым результатом и без
    внятной причины. Каталоги вывода — модульные константы (`OUTPUT_SQL`,
    `OUTPUT_CSV`) и лежат внутри корня, но вызывающий вправе направить их в
    `tmp` (как делает тест), и это не повод падать.
    """
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def main() -> int:
    """Полный прогон: калибровка → факт добычи → увязка → запись SQL/CSV.

    Возвращает НОЛЬ только при пустом списке проблем `validate`. Ненулевой код
    возврата — единственный сигнал `make`-запуску, что таблица непригодна:
    печатать проблемы и всё равно выходить с нулём значило бы, что CI и `make`
    считают дефектный сид успехом, и `sql/dcf_mine_plans_seed.sql` уехал бы в
    репозиторий как артефакт.

    ФАЙЛЫ ПИШУТСЯ ДАЖЕ ПРИ НЕПУСТЫХ ПРОБЛЕМАХ. Это намеренно: отчёт о валидации
    ссылается на конкретные строки, и оператору нужен файл, чтобы увидеть, что
    именно не так. Опасности нет ровно потому, что код возврата ненулевой —
    `make ch-sql` в цепочке не запустится, а файл остаётся уликой, а не входом.

    ПОРЯДОК ВЫЗОВОВ СООТВЕТСТВУЕТ КОНВЕЙЕРУ, объявленному в спеке §3:

    1. набор фикстур — ЧЕРЕЗ `discover_fixtures()` (автообнаружение, без
       `*_corrupt.pdf`), плюс явный `assert_no_corrupt_fixtures` как второй
       слой: явный словарь фикстур в будущей правке не должен протащить порчу
       в калибровку незаметно;
    2. `build_series()` по этому набору → `calibrate()` → `{asset: {"r_bar": …}}`;
    3. факт добычи 2021…`ACTUAL_PRODUCTION_LAST_YEAR` из `databook_polyus`;
    4. якоря TCC 2025 (`TCC_2025_USD_PER_OZ`, снимок Годового обзора, дословно
       как в Go-сиде) и сверка их взвешенного среднего с групповым
       `GROUP_TCC_2025_USD_PER_OZ` — расхождение ПЕЧАТАЕТСЯ, но не роняет
       прогон (это диагностика согласованности источников, а не вход модели);
    5. `build_plan_rows` — по годам, с увязкой к `GROUP_AISC_2025_USD_PER_OZ`;
    6. `validate` по строкам, калибровке и ОСТАТКУ ПОСЛЕДНЕГО ГОДА плана;
    7. запись `OUTPUT_SQL` и `OUTPUT_CSV`;
    8. русская сводка: по активу `r̄`, отброшенные годы, `m`, остаток, число строк.

    ОСТАТОК БЕРЁТСЯ ИЗ `LAST_YEAR_RECONCILIATION`, А НЕ ПЕРЕСЧИТЫВАЕТСЯ. Это
    единственный пост-фактум отчёт `reconcile` о том, что строки реально
    содержат (см. её докстроку), и повторный расчёт здесь завёл бы вторую
    формулу в одном месте. Последний год плана — КОНТРОЛЬНАЯ точка, а не
    «остаток вообще»: для штатных якорей остаток ~0 во всех годах (цель и TCC
    эскалируются одним множителем, поэтому отношение год от года не меняется),
    но при подменённом якоре видно именно последний год, где искажение
    накопилось. Полный перечень по годам доступен в `LAST_YEAR_RECONCILIATION`.

    СЕКРЕТЫ. `ch_config()["password"]` нигде не печатается; хост и порт проходят
    через `mask`, как и в прежней версии сводки. Скрипт не пишет в ClickHouse —
    только читает (`read_actual_production`).
    """
    config = ch_config()
    host = mask(config["password"], config["host"])

    # 1. Набор фикстур — ТОЛЬКО автообнаружение, плюс явный отказ от порчи.
    fixtures = discover_fixtures()
    assert_no_corrupt_fixtures(fixtures)

    # 2. Серия → калибровка.
    series = build_series(fixtures)
    cal = calibrate(series)
    r_bar = {asset: cal[asset]["r_bar"] for asset in ASSETS}

    # 3. Факт добычи.
    years = tuple(range(2021, ACTUAL_PRODUCTION_LAST_YEAR + 1))
    production = read_actual_production(ASSETS, years)

    # 4. Якоря TCC 2025: снимок Годового обзора, дословно как в Go-сиде.
    tcc_2025 = dict(TCC_2025_USD_PER_OZ)

    print("Экстраполяция поактивного AISC Полюса")
    print(f"  целевая база: ClickHouse {host}:{config['http_port']}, БД {config['database']}")
    print(f"  плановый горизонт: {PLAN_BASE_YEAR}+, активы ({len(ASSETS)}): "
          f"{', '.join(ASSETS)}")
    print(f"  фикстуры: {FIXTURE_DIR.relative_to(ROOT)}, "
          f"годы {', '.join(str(y) for y in sorted(fixtures))}")
    print(f"  калибровочные годы клина: {', '.join(str(y) for y in CALIBRATION_YEARS)}")

    # Взвешенное среднее якорей против опубликованного группового TCC — ДИАГНОСТИКА
    # согласованности двух публикаций, не вход модели. Печатается всегда, чтобы
    # расхождение было видно, но прогон не роняется: TCC плана приходит из якорей,
    # и «не сошлось с групповым» — это свойство публикаций (групповой TCC взвешен
    # по всей Группе, включая россыпь и прочие активы), а не дефект расчёта.
    weight_total = sum(production[asset].get(ACTUAL_PRODUCTION_LAST_YEAR, 0.0)
                       for asset in ASSETS)
    if weight_total > 0.0:
        weighted_tcc = sum(
            tcc_2025[asset]
            * production[asset].get(ACTUAL_PRODUCTION_LAST_YEAR, 0.0) / weight_total
            for asset in ASSETS
        )
        print(f"  TCC 2025 по активам: взвешенное среднее {weighted_tcc:.1f} USD/oz "
              f"против группового {GROUP_TCC_2025_USD_PER_OZ} USD/oz "
              f"(расхождение {weighted_tcc - GROUP_TCC_2025_USD_PER_OZ:+.1f})")

    print("  калибровка клина r̄ (доля от TCC), по активу:")
    for asset in ASSETS:
        entry = cal[asset]
        discarded = ", ".join(
            f"{year} ({value:.3f})" for year, value in entry["discarded"]
        ) or "нет"
        print(f"    {asset}: r̄ = {entry['r_bar']:.4f} "
              f"по годам {', '.join(str(y) for y in entry['years'])}; "
              f"отброшено выбросов: {discarded}")

    # 5. План.
    plan_rows = build_plan_rows(
        tcc_2025, r_bar, production, GROUP_AISC_2025_USD_PER_OZ
    )
    for asset, reason in LAST_SKIPPED_ASSETS:
        print(f"    пропущен актив {asset}: {reason}")

    # `r̄` кладётся В СТРОКИ вызывающим — так устроен контракт `write_csv`
    # (Task 9): она читает `row.get("r_bar")` и оставляет ячейку ПУСТОЙ, если
    # ключа нет, потому что выдуманное среднее выглядело бы калибровкой.
    # `build_plan_rows` про r̄ не знает (её вход — увязка, а не отчётность),
    # поэтому связывает одно с другим именно `main()`.
    for row in plan_rows:
        row["r_bar"] = r_bar.get(row["asset"])

    # 6. Валидация. Остаток — последнего года плана (см. докстроку).
    if LAST_YEAR_RECONCILIATION:
        last_year = max(LAST_YEAR_RECONCILIATION)
        _target, residual, m = LAST_YEAR_RECONCILIATION[last_year]
    else:
        last_year, residual, m = None, None, None

    meta = {
        "residual": residual if residual is not None else 0.0,
        "group_aisc": GROUP_AISC_2025_USD_PER_OZ,
    }
    problems = validate(plan_rows, cal, meta)

    # 7. Запись артефактов — ДО печати вердикта, см. докстроку: отчёт ссылается
    # на строки, и оператору нужен файл, чтобы увидеть, что именно не так.
    write_sql(OUTPUT_SQL, plan_rows, {
        "command": GENERATE_COMMAND,
        "calibration_years": CALIBRATION_YEARS,
        "m": m,
        "residual": residual,
    })
    write_csv(OUTPUT_CSV, plan_rows)

    # 8. Сводка увязки и вердикт.
    print(f"  увязка к групповому AISC 2025 {GROUP_AISC_2025_USD_PER_OZ} USD/oz "
          f"(порог остатка {RESIDUAL_TOLERANCE_SHARE:.0%}):")
    print(f"    множитель клина m ({last_year}): "
          f"{'не определено' if m is None else f'{m:.6f}'}")
    print(f"    остаток {last_year}: "
          f"{'не определено' if residual is None else f'{residual:.6f}'} USD/oz")
    print(f"    строк плана: {len(plan_rows)}")
    print(f"  записано: {_display_path(OUTPUT_SQL)}, {_display_path(OUTPUT_CSV)}")

    if problems:
        print(f"ВАЛИДАЦИЯ НЕ ПРОЙДЕНА: проблем {len(problems)}")
        for problem in problems:
            print(f"  - {problem}")
        return 1

    print("валидация пройдена: проблем нет")
    return 0


if __name__ == "__main__":
    sys.exit(main())
