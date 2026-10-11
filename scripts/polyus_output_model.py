#!/usr/bin/env python3
"""Сид `mine_plans`: атрибуция ОПР-выпуска Сухого Лога (2025–2027).

Опытно-промышленная разработка (ОПР) Сухого Лога идёт с 2024 года: руда
перерабатывается на Вернинской ЗИФ, и результаты этой переработки Полюс
показывает В ПРОИЗВОДСТВЕННЫХ РЕЗУЛЬТАТАХ ВЕРНИНСКОГО (сноска 13 пресс-релиза
за 1 полугодие 2026). Экономически выпуск принадлежит источнику руды: затраты
`TCC` привязаны к карьеру Сухого Лога, а не к ЗИФ Вернинского.

В модели эти три года были потеряны целиком: AISC-сид обнуляет `production_koz`
Вернинского в окне консервации 2025–2028, а план Сухого Лога в Go-сиде
начинается только с 2028 (`ProfileStartYear`). Скрипт закрывает ровно этот
разрыв: печатает три строки плана, которых в таблице нет.

ГРАНИЦЫ. Скрипт НЕ трогает строки пяти действующих рудников и НЕ трогает
строки Сухого Лога 2028–2037 (владелец — Go-сид `dcf_mine_plans`, проект
развития). Множества ключей `(company, asset, year)` не пересекаются, и это
единственная защита от перезаписи: `ReplacingMergeTree` сворачивает строку
ЦЕЛИКОМ, поэтому сид, уточняющий одну колонку в существующей строке, затёр бы
все остальные вместе с калиброванным AISC.

Скрипт НИЧЕГО НЕ ПИШЕТ В CLICKHOUSE — только читает факт и печатает файлы
(SQL-сид и CSV для глазной сверки). Вставка — отдельный ручной шаг
`make ch-sql FILE=sql/polyus_output_seed.sql`. Так Python-скрипту не нужен
пароль, и read-only контур `kimi_reader` не нарушается (AGENTS.md, правило 11в).

Запуск — только через `make` (переменные окружения подключает Makefile, а не
скрипт из `~/.config/rosstat/env`).
"""

import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Скрипт-сосед: переиспользуем транспорт к ClickHouse, а не копируем его.
# `ch_query` уже несёт маскирование секретов в тексте ошибок, правильный
# HTTP-порт (8123, а не нативный 9000) и обработку `RemoteDisconnected`.
sys.path.insert(0, str(ROOT / "scripts"))

import extrapolation_aisc as aisc

ch_query = aisc.ch_query

# Каноническое имя актива, КОТОРОМУ ПРИПИСЫВАЕТСЯ выпуск: дословно совпадает с
# `databook_polyus.table` и `dcf/mine_plans_seed.go` (`Asset: "Sukhoi Log"`).
# Имя содержит пробел и смешанный регистр — это канон Полюса, а не опечатка.
ASSET = "Sukhoi Log"

# Актив, У КОТОРОГО читается факт. Это не тот же актив, что `ASSET`, и в этом
# суть задачи: у Сухого Лога в датапаке НЕТ ни одной метрики выпуска (только
# `Ore mined`, `Stripping`, `Average grade in ore mined` — горные работы), а
# выплавка ОПР числится за Вернинским, потому что руду перерабатывает его ЗИФ.
# Полюс закрепляет это публично: сноска 13 пресс-релиза за 1 полугодие 2026 —
# «Результаты переработки руды включены в производственные результаты
# Вернинского». Модель переносит выпуск источнику руды, потому что затраты
# (TCC) привязаны к карьеру, а не к ЗИФ.
SOURCE_ASSET = "VERNINSKOYE2"

# Окно ОПР в этом срезе — 2025–2027. Год 2028+ НЕ ВХОДИТ по построению: там
# начинается горизонт Go-сида (`ProfileStartYear: 2028`, `MineLifeYears: 10`),
# и пересечение ключей означало бы перезапись проектного профиля. См.
# `assert_no_overlap`.
OPR_YEARS = (2025, 2026, 2027)

# Метрика выпуска. Для ОПР-переработки берётся АФФИНИРОВАННОЕ золото (`refined`),
# а не `Total Dore gold output`: слиток — это то, что Полюс показывает в
# производственных результатах Вернинского, и на групповом уровне сумма
# сходится именно по `refined` (2025: 2790.6 против consolidated 2790.6,
# тогда как `dore` даёт 2779.2). Разница 11.4 koz в 2025 — это ровно ОПР-выпуск.
REFINED_METRIC = "Total refined gold output"

# TCC-прокси для ОПР-выпуска, USD/унц. Дословно как у Сухого Лога в Go-сиде
# (`TCC: 735`): это уровень Иркутской бизнес-единицы, в которую входят и
# Вернинское, и ОПР его руды. Новых констант не заводим — иначе одна и та же
# себестоимость считалась бы двумя разными числами в двух источниках.
TCC_PROXY_USD_PER_OZ = 735.0


class OutputDataError(Exception):
    """Факт выпуска за запрошенный год отсутствует или непригоден.

    Отличие от `extrapolation_aisc.read_actual_production` принципиальное: там
    ноль — НОРМАЛЬНЫЙ результат (в датапаке есть законно нулевые активы, и
    вызывающий сам решает, писать ли строку). Здесь ноль означает «факта нет»,
    а строка плана с нулевым выпуском занизила бы NPV Сухого Лога, не будучи
    отличимой от измеренного нуля. Поэтому отсутствие факта — отказ, а не
    значение: молчаливая подстановка `0.0` недопустима.
    """


class OverlapError(Exception):
    """Строка сида пересекается по ключу со строкой, которой владеет Go-сид.

    Почему это отказ, а не предупреждение. `mine_plans` — `ReplacingMergeTree`
    по `(company, asset, year)`, и строка сворачивается ЦЕЛИКОМ: выживает
    последняя вставленная, а не слияние колонок. Сид, уточняющий одну колонку в
    существующей строке, затрёт все остальные из той же строки. Для Сухого Лога
    2028–2037 это означает потерю проектного профиля (2 300–2 800 koz) и
    `capex_project` 600 млн/год; для пяти рудников — потерю калиброванного AISC.

    Поэтому множества ключей обязаны не пересекаться, и проверка стоит в коде,
    а не в комментарии: цена молчаливой перезаписи — неверный NAV, который
    выглядит посчитанным.
    """


# Годы, которыми владеет Go-сид `dcf_mine_plans` по активу Сухой Лог.
# Значение ВЫВОДИТСЯ из исходника Go-сида (`parse_go_seed_*`), а не
# транскрибируется числом: транскрипция разошлась бы с кодом молча — сдвиг
# `ProfileStartYear` оставил бы константу прежней, и сид перезаписал бы годы,
# которые Go-сид теперь считает своими. Проверка стоит в тесте
# (`test_horizon_is_derived_from_the_go_seed_source`), потому что чтение файла
# на импорте сделало бы модуль зависящим от рабочего каталога.
GO_SEED_FILE = ROOT / "dcf" / "mine_plans_seed.go"

# Запасное значение на случай, если исходник недоступен (модуль поставлен без
# репозитория). НЕ основной путь: при доступном файле значение сверяется с ним.
GO_SEED_SUKHOI_LOG_YEARS_FALLBACK = frozenset(range(2028, 2038))


def parse_go_seed_profile_start_year(source: str) -> int:
    """`ProfileStartYear` актива Сухой Лог из исходника Go-сида.

    Парсится БЛИЖАЙШЕЕ вхождение после объявления актива: `ProfileStartYear`
    есть только у Сухого Лога (у пяти рудников план строится от факта
    `databook_polyus`), поэтому первое совпадение в файле и есть его.
    """
    import re

    match = re.search(r'Asset:\s*"' + re.escape(ASSET) + r'"[^}]*?'
                      r'ProfileStartYear:\s*(\d{4})', source, re.DOTALL)
    if not match:
        raise OutputDataError(
            f"в {GO_SEED_FILE.name} не найден ProfileStartYear актива {ASSET!r}: "
            f"горизонт Go-сида не выводится, сид применять нельзя"
        )
    return int(match.group(1))


def parse_go_seed_mine_life_years(source: str, asset: str) -> int:
    """`MineLifeYears` актива из исходника Go-сида."""
    import re

    match = re.search(r'Asset:\s*"' + re.escape(asset) + r'"[^}]*?'
                      r'MineLifeYears:\s*(\d+)', source, re.DOTALL)
    if not match:
        raise OutputDataError(
            f"в {GO_SEED_FILE.name} не найден MineLifeYears актива {asset!r}"
        )
    return int(match.group(1))


def go_seed_years() -> frozenset:
    """Годы горизонта Go-сида для Сухого Лога: `[start, start + life)`.

    Полуоткрытый интервал повторяет конвенцию `dcf/seed.go`
    (`[minPlanYear, maxPlanYear)`): `MineLifeYears` — это ЧИСЛО лет, и
    последний год плана равен `start + life − 1`.
    """
    try:
        source = GO_SEED_FILE.read_text(encoding="utf-8")
        start = parse_go_seed_profile_start_year(source)
        life = parse_go_seed_mine_life_years(source, ASSET)
    except (OSError, OutputDataError):
        return GO_SEED_SUKHOI_LOG_YEARS_FALLBACK
    return frozenset(range(start, start + life))


GO_SEED_SUKHOI_LOG_YEARS = go_seed_years()


def output_sql(asset: str, year: int) -> str:
    """SQL для годового выпуска актива по метрике `REFINED_METRIC`, тыс. унц.

    Предикат — тот же, что у `extrapolation_aisc.production_sql` (и, через
    него, у `dcf/mine_plans_import.go`), с одной заменой метрики: один и тот
    же факт не должен читаться двумя источниками двумя разными способами.

        SELECT argMax(value, date) AS v
        FROM databook_polyus FINAL
        WHERE table = '<ASSET>' AND name = '<REFINED_METRIC>'
          AND data = 'ANNUAL' AND toYear(date) = <YEAR>
        FORMAT JSON

    `FINAL` обязателен: `databook_polyus` — `ReplacingMergeTree` по
    `(table, name, data, date)`, и без FINAL агрегат мог бы взять не тот слой
    значения. `AS v` и `FORMAT JSON` обязательны для HTTP-интерфейса: без
    первого ключом ответа стало бы само выражение `argMax(value, date)`, без
    второго ответ пришёл бы `TabSeparated`.

    Имя актива подставляется в литерал, а не параметром (HTTP-интерфейс требует
    текст запроса целиком), поэтому одинарная кавычка в имени ЭКРАНИРУЕТСЯ
    удвоением — так же, как в `extrapolation_aisc.sql_literal`. ПРОВЕРКА ФОРМЫ
    ИМЕНИ ЗДЕСЬ НЕУМЕСТНА: каноническое имя актива у Полюса — «Sukhoi Log», с
    пробелом и смешанным регистром (`dcf/mine_plans_seed.go` документирует это
    прямо), и шаблон вида `^[A-Z][A-Z0-9_]*$` отверг бы законные данные. Go-сид
    той же задачи подставляет имя параметром `{asset:String}`, где экранирование
    делает сервер; здесь его делает `_quote`, и это единственное отличие.
    """
    year = int(year)
    if not 1900 <= year <= 2200:
        raise OutputDataError(f"год {year} вне правдоподобного диапазона")
    return (
        "SELECT argMax(value, date) AS v\n"
        "FROM databook_polyus FINAL\n"
        f"WHERE table = {_quote(asset)}\n"
        f"  AND name = {_quote(REFINED_METRIC)}\n"
        "  AND data = 'ANNUAL'\n"
        f"  AND toYear(date) = {year}\n"
        "FORMAT JSON"
    )


def _quote(value: str) -> str:
    """Литерал ClickHouse для строки: одинарные кавычки с удвоением тика.

    `O'HARA` → `'O''HARA'`. Тот же приём, что в `extrapolation_aisc.sql_literal`,
    и по той же причине: литерал не должен разрывать текст запроса. Перевода
    строки в имени актива не бывает, а кавычка — единственный символ, способный
    выйти за пределы литерала, поэтому удвоения достаточно.
    """
    return "'" + str(value).replace("'", "''") + "'"


def read_refined_output(asset: str, year: int) -> float:
    """Аффинированный выпуск актива за год, тыс. унц (koz).

    Отсутствие строки или `NULL` — `OutputDataError`, а НЕ ноль: см. докстроку
    класса. Ноль был бы неотличим от измеренного нуля, и строка плана с ним
    молча занизила бы NPV.
    """
    rows = ch_query(output_sql(asset, year))
    value = rows[0].get("v") if rows else None
    if value is None:
        raise OutputDataError(
            f"нет факта {REFINED_METRIC!r} по активу {asset} за {year}: "
            f"строка плана не может быть построена на отсутствующем факте"
        )
    result = float(value)
    # КОНЕЧНОСТЬ проверяется ЯВНО, а не через `<= 0`: сравнение с NaN всегда
    # ложно, поэтому NaN прошёл бы любую знаковую проверку и разъехался бы по
    # NPV, где сравнения с ним тоже ложны (тот же довод, что в
    # `extrapolation_aisc.sql_literal`, отвергающем NaN/inf).
    if result != result or result in (float("inf"), float("-inf")):
        raise OutputDataError(
            f"выпуск {asset} за {year} не конечен ({value!r}): "
            f"NaN/inf прошли бы знаковые проверки и отравили NPV"
        )
    return result


# Год, для которого факта не публикуется и значение выводится средним по
# наблюдённым годам окна. Последний год окна — единственный: 2025 и 2026
# измерены, и подстановка среднего на их месте означала бы выдуманный факт
# вместо отказа (см. `build_rows`).
ESTIMATED_YEARS = (2027,)

# Среднее наблюдённых лет округляется до ЦЕЛОГО перед записью в план. Причина:
# это оценка, а не измерение, и выдавать её с точностью 0.01 koz значило бы
# заявлять точность, которой у среднего двух чисел нет. Измеренные годы
# округлению НЕ подвергаются — они пишутся как пришли из источника.
ESTIMATE_ROUND_DIGITS = 0


def average_output(values: list) -> float:
    """Среднее наблюдённых значений выпуска, тыс. унц, округлённое до целого.

    Пустой список — `OutputDataError`: по одной точке среднее не считается, а
    молчаливый `ZeroDivisionError` не объяснил бы, какой строке плана не хватило
    данных.

    Среднее, а НЕ плато последним фактом: ряд ОПР не имеет тренда, который
    стоило бы продлевать (2025 — 242.2, 2026 — 271.7), поэтому среднее двух
    наблюдённых лет честнее последнего значения. Это ДОПУЩЕНИЕ, и оно названо
    здесь, а не спрятано в формуле.

    Округление до целого — тоже часть допущения: `(242.2 + 271.7) / 2 = 256.95`,
    и записанное «257.0» заявляет, что третий знак у оценки не значим. Для
    NPV это разница порядка 0.02 % выпуска одного года.
    """
    if not values:
        raise OutputDataError("среднее выпуска не определено: нет наблюдений")
    return round(sum(values) / len(values), ESTIMATE_ROUND_DIGITS)


def build_rows(measured: dict) -> list:
    """Строки плана ОПР-выпуска: по одной на каждый год из `OPR_YEARS`.

    `measured` — `{year: output_koz}` ИЗМЕРЕННЫЕ годы. Год без наблюдения
    получает среднее по наблюдённым — но только если он объявлен в
    `ESTIMATED_YEARS`; отсутствие факта в измеряемом году — отказ.

    ПОЧЕМУ ТАК, А НЕ «СРЕДНЕЕ ДЛЯ ЛЮБОГО ПРОПУСКА». Подстановка среднего в
    измеряемый год превращает пропажу факта в правдоподобное число: сбой
    чтения 2026 тихо дал бы строку 2026 со средним, и отличить её от настоящего
    наблюдения было бы нечем. Такой год обязан падать (`OutputDataError`), а
    оценка допускается только там, где публикации нет по построению — в
    последнем году окна.

    Ключи строки ДОСЛОВНО совпадают с `extrapolation_aisc.MINE_PLANS_COLUMNS`,
    чтобы эмиттер мог печатать их тем же `sql_batches`, что и AISC-сид: строка
    плана — один контракт на два скрипта, а не два похожих словаря.

    ИНВАРИАНТЫ, проверяемые здесь, а не глазами:

    * каждый год `> 0` — нулевой выпуск неотличим от отсутствия факта и
      занизил бы NPV (см. `OutputDataError`);
    * `aisc >= tcc` — при равенстве клин поддержания нулевой, и это правильно:
      AISC-клин для ОПР-переработки не калибровался, а отрицательный клин
      означал бы отрицательные затраты поддержания, то есть бессмыслицу.

    `capex_project = 0`, `capex_sustaining = 0.0`, `closure_costs = 0` —
    политика, а не расчёт. Проектный capex Сухого Лога привязан Go-сидом к
    годам профиля 2028–2037 (по 600 млн/год); задать его здесь значило бы
    удвоить отток, когда сценарии начнут считать 2028+.
    """
    missing = [year for year in OPR_YEARS
               if year not in measured and year not in ESTIMATED_YEARS]
    if missing:
        raise OutputDataError(
            f"нет факта выпуска {ASSET} за {missing}: эти годы обязаны быть "
            f"измерены, оценка средним допускается только для {ESTIMATED_YEARS}"
        )

    observed = [value for _, value in sorted(measured.items())]
    rows = []
    for year in OPR_YEARS:
        value = measured.get(year)
        production_koz = float(value) if value is not None else average_output(observed)
        aisc_value = TCC_PROXY_USD_PER_OZ
        if production_koz <= 0:
            raise OutputDataError(
                f"выпуск {ASSET} за {year} неположителен ({production_koz!r}): "
                f"строка плана с нулевым выпуском занизила бы NPV, "
                f"оставаясь неотличимой от отсутствия факта"
            )
        if aisc_value < TCC_PROXY_USD_PER_OZ:
            raise OutputDataError(
                f"клин поддержания отрицателен в {year}: {aisc_value} < {TCC_PROXY_USD_PER_OZ}"
            )
        rows.append({
            "asset": ASSET,
            "year": year,
            "production_koz": production_koz,
            "tcc": TCC_PROXY_USD_PER_OZ,
            "aisc": aisc_value,
            "capex_sustaining": 0.0,
            "capex_project": 0,
            "closure_costs": 0,
            "grade_gpt": None,
        })
    return rows


def assert_no_overlap(rows: list, go_seed_years=frozenset(GO_SEED_SUKHOI_LOG_YEARS)) -> None:
    """Отказывает, если год строки принадлежит горизонту Go-сида.

    См. докстроку `OverlapError`: пересечение ключей означает не «уточнение», а
    перезапись чужой строки целиком. Проверка идёт по ГОДАМ и активу, а не по
    всему ключу, потому что сид пишет один актив (`ASSET`) — имя актива в
    проверке фиксировано и не может разойтись.

    Параметр `go_seed_years` существует для теста: он подаёт горизонт, в
    котором пересечение есть, и проверяет, что отказ срабатывает. Значение по
    умолчанию — константа рядом с `OPR_YEARS`, то есть проверка не может быть
    «забыта» вызывающим.
    """
    collisions = sorted(
        row["year"] for row in rows
        if row["asset"] == ASSET and row["year"] in go_seed_years
    )
    if collisions:
        raise OverlapError(
            f"годы {collisions} по активу {ASSET} принадлежат горизонту Go-сида "
            f"({min(go_seed_years)}…{max(go_seed_years)}): сид затёр бы его строки "
            f"целиком, вместе с проектным профилем и capex"
        )


# Артефакты прогона. SQL — то, что применяется вручную; CSV — то, что человек
# читает глазами, сверяя строки с публикацией. Ровно так же устроен AISC-сид
# (`sql/dcf_mine_plans_seed.sql` + `polyus_lom_reconciled_long.csv`).
OUTPUT_SQL = ROOT / "sql" / "polyus_output_seed.sql"
OUTPUT_CSV = ROOT / "polyus_output_attribution_long.csv"

# Команда применения сида — печатается в шапке, чтобы её можно было скопировать
# из файла в терминал, не заглядывая в документацию.
APPLY_COMMAND = "make ch-sql FILE=sql/polyus_output_seed.sql"

SQL_HEADER_TEMPLATE = """\
-- Сид mine_plans: ОПР-выпуск Сухого Лога 2025–2027 (атрибуция руды Вернинской ЗИФ).
--
-- СГЕНЕРИРОВАН АВТОМАТИЧЕСКИ, ПРАВИТЬ РУКАМИ НЕЛЬЗЯ: правка будет затёрта
-- следующим прогоном (scripts/polyus_output_model.py, цель `make polyus-output`).
--
-- Команда генерации: {command}
-- Дата генерации: {generated}
-- Строк в сиде: {row_count}
--
-- ЧТО ЭТО. Полюс с 2024 года ведёт опытно-промышленную разработку (ОПР) Сухого
-- Лога с переработкой руды на Вернинской ЗИФ, и результаты этой переработки
-- показывает В ПРОИЗВОДСТВЕННЫХ РЕЗУЛЬТАТАХ ВЕРНИНСКОГО (сноска 13 пресс-релиза
-- за 1 полугодие 2026). Экономически выпуск принадлежит источнику руды: затраты
-- привязаны к карьеру.
--
-- ПОЧЕМУ ЭТИ ГОДЫ БЫЛИ ПОТЕРЯНЫ. AISC-сид обнуляет production_koz Вернинского в
-- окне консервации (2025–2028), а план Сухого Лога в Go-сиде начинается с 2028
-- (ProfileStartYear). Поэтому выпуск 2025–2027 не входил в NAV НИ ОДНОГО актива.
--
-- ПОЧЕМУ ТОЛЬКО 2025–2027. Год 2028+ принадлежит Go-сиду (горизонт 2028–2037,
-- профиль 2 300–2 800 koz, capex 600 млн/год). `ReplacingMergeTree` сворачивает
-- строку по (company, asset, year) ЦЕЛИКОМ, поэтому пересечение ключей затёрло бы
-- проектный профиль. Множества годов не пересекаются по построению, и это
-- проверяет `assert_no_overlap` (OverlapError), а не комментарий.
--
-- ЗНАЧЕНИЯ. 2025 (242.2) и 2026 (271.7) — измеренный `Total refined gold output`
-- Вернинской ЗИФ, koz. 2027 (257.0) — ОЦЕНКА: среднее двух наблюдённых лет,
-- округлённое до целого. TCC = AISC = 735 USD/унц — прокси Иркутской БЕ,
-- дословно как у Сухого Лога в Go-сиде; при равенстве клин поддержания нулевой,
-- потому что AISC-клин для ОПР не калибровался.
--
-- ПРИМЕНЯЕТСЯ ВРУЧНУЮ, ОТДЕЛЬНЫМ ШАГОМ — и это намеренно, как у AISC-сида:
--   {apply}
-- В `dagu/dcf.yaml` шага НЕТ: на staging не заработали бы ни make, ни сам
-- SQL-файл, а провал был бы бесшумным (цепочка пересеяла бы строки из Go-снимка).
{statements}"""


def write_sql(path: Path, rows: list) -> None:
    """Пишет SQL-сид: шапка-комментарий плюс один `INSERT` со всеми строками.

    Эмиссия делегирована `aisc.sql_batches` — тому же эмиттеру, что печатает
    AISC-сид. Второй эмиттер разошёлся бы с первым в порядке колонок, а
    ClickHouse принимает перестановку однотипных `Float64` МОЛЧА и записал бы
    `tcc` в `aisc`: порядок колонок здесь — контракт, а не оформление.

    Шапка печатается даже при нулевом числе строк: пустой план должен быть
    виден как пустой, а не превращаться в файл без объяснения.
    """
    from datetime import date

    statements = "\n".join(aisc.sql_batches(rows))
    header = SQL_HEADER_TEMPLATE.format(
        command="make polyus-output",
        generated=date.today().isoformat(),
        row_count=len(rows),
        apply=APPLY_COMMAND,
        statements=statements,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(header, encoding="utf-8")


# Колонки CSV: десять колонок сида плюс справка для глаз. `source_metric`
# отвечает на вопрос, который иначе пришлось бы держать в голове: 242.2 — это
# измерение, а 257.0 — оценка средним. Без неё вторая строка выглядела бы как
# факт.
CSV_COLUMNS = tuple(aisc.MINE_PLANS_COLUMNS) + ("source_metric",)

# Метки источника выпуска. Строки, а не enum: CSV читает человек.
SOURCE_MEASURED = "refined_measured"
SOURCE_ESTIMATED = "mean_2025_2026"


def write_csv(path: Path, rows: list) -> None:
    """Пишет long-формат CSV для глазной сверки.

    `grade_gpt = None` выражается ПУСТОЙ ячейкой, а не строкой `NULL`: это файл
    для человека, и «NULL» в нём читался бы как значение, тогда как пустота
    означает «не раскрыто». Ноль здесь означал бы измеренное «0 г/т».

    Оценка помечается в `source_metric`: у 2027 выпуск выведен средним, и без
    пометки строка неотличима от наблюдения — ровно тот дефект, от которого
    защищает `ESTIMATED_YEARS`.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(CSV_COLUMNS)
        for row in rows:
            source = SOURCE_ESTIMATED if row["year"] in ESTIMATED_YEARS else SOURCE_MEASURED
            writer.writerow((
                "PLZL",
                row["asset"],
                row["year"],
                repr(row["production_koz"]),
                "" if row["grade_gpt"] is None else repr(row["grade_gpt"]),
                repr(row["tcc"]),
                repr(row["aisc"]),
                repr(row["capex_sustaining"]),
                repr(row["capex_project"]),
                repr(row["closure_costs"]),
                source,
            ))


def read_measured() -> dict:
    """Измеренный выпуск ОПР по годам, которые публикация даёт фактом.

    Читается актив `SOURCE_ASSET` (Вернинское), а не `ASSET` (Сухой Лог): у
    Сухого Лога метрики выпуска в датапаке нет вовсе, выплавка числится за
    Вернинским. Записывается результат при этом Сухому Логу — в этом и состоит
    атрибуция.

    Читаются `OPR_YEARS` без `ESTIMATED_YEARS`: у оценённого года факта нет по
    построению, и запрос к нему был бы попыткой прочитать то, чего не публикуют.
    """
    years = [year for year in OPR_YEARS if year not in ESTIMATED_YEARS]
    return {year: read_refined_output(SOURCE_ASSET, year) for year in years}


def main() -> int:
    """Полный прогон: чтение факта → сборка строк → запись SQL и CSV.

    Возвращает НОЛЬ только при успехе. `OutputDataError` и `OverlapError`
    печатаются как читаемое сообщение, а не как трейсбек: причина отказа
    (какой год не прочитан, какой ключ пересекается) — это и есть результат
    прогона, и она не должна тонуть в стеке вызовов.

    В ClickHouse скрипт НЕ ПИШЕТ: печатает файлы, вставка — `make ch-sql`.
    Поэтому прогон без БД на запись безопасен и пароль не нужен.
    """
    try:
        rows = build_rows(read_measured())
        assert_no_overlap(rows)
    except (OutputDataError, OverlapError) as exc:
        print(f"ОПР-выпуск не засеян: {exc}", file=sys.stderr)
        return 1

    write_sql(OUTPUT_SQL, rows)
    write_csv(OUTPUT_CSV, rows)

    for row in rows:
        source = "оценка (среднее 2025–2026)" if row["year"] in ESTIMATED_YEARS else "факт refined"
        print(f"{row['asset']} {row['year']}: {row['production_koz']} koz — {source}")
    # Путь печатается через `aisc._display_path`: `relative_to(ROOT)` поднимает
    # ValueError для каталога ВНЕ корня (тесты направляют вывод в tmp), а падать
    # на последней строке сводки — после записи файлов — значит ронять успешный
    # прогон без причины. Та же защита и по той же причине, что в AISC-скрипте.
    print(f"Сид: {aisc._display_path(OUTPUT_SQL)} ({len(rows)} строк)")
    print(f"Сверка: {aisc._display_path(OUTPUT_CSV)}")
    print(f"Применить: {APPLY_COMMAND}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
