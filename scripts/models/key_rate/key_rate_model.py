#!/usr/bin/env python3
"""Первый срез модели ключевой ставки: выборка решений, as-of слой и признаки.

Измерительная основа контура ключевой ставки (спека
`docs/superpowers/specs/2026-10-15-key-rate-model-v0-design.md` §3–4): единица
наблюдения — заседание Совета директоров ЦБ по ключевой ставке.

Что делает этот срез:

* **выборка решений** — канонические строки `events_calendar`
  (`category='cbr'∧title=SAMPLE_TITLE`; ручные строки-триггеры сида отсекаются
  фильтром по `title`) плюс дневной ряд `cbr_key_rate`: `rate_before` на `d−1`,
  `rate_after` на `min(d+7, next−1)`, шаг в б.п. и класс `y3`. Заседание даёт
  решение, только если его исход наблюдаем (окно уложилось в ряд ставки); иначе
  оно «вне выборки», а при отсутствии ставки на `d−1` — «пропущено» (§3);
* **as-of слой** — консервативные правила доступности объясняющих рядов (§4):
  точка участвует, только если её `available_from(period) ≤ даты заседания`;
* **признаки** — `pi_saar` (базовый ИПЦ SA, приведённый к годовому) и `gap`
  (ИБК ЦБ SA); рядом — `rate_before`. Ряд без точки раньше заседания
  исключается из выборки и перечисляется в «пропущенных».

Что добавляет этот срез (Task 3):

* **walk-forward** — расширяющееся окно (минимум 24 заседания в обучении) и
  скользящее 40 заседаний; каждый фолд переобучается только на прошлом,
  горизонт прогноза — одно заседание (спека §7); вырожденное обучающее окно
  (`DegenerateWindowError`) пропускается со счётчиком, а не роняет прогон;
* **бенчмарки B1–B3** (спека §5): «ставка не изменится», «повтор последнего
  решения», AR(1) на уровне ставки (OLS с HAC); все прогнозы и факты — в
  БАЗИСНЫХ пунктах (одна шкала у бенчмарков и метрик);
* **метрики §7** — RMSE и MAE по шагу в б.п., hit rate, матрица 3×3, QWK,
  Brier, log loss, надёжностная таблица; тесты DM (поправка HLN через
  `statsmodels.tsa.stattools.diebold_mariano_test`), Mincer–Zarnowitz с HAC,
  Pesaran–Timmermann (через `statsmodels.stats.diagnostic`) и блочный
  бутстрап разницы RMSE;
* **артефакты §8** — `key_rate_model_report.md`,
  `key_rate_model_predictions.csv` и `key_rate_model_forecast.json` в каталоге
  прогона (корень репозитория; в git не коммитятся).

Что добавляет этот срез (Task 4):

* **метод №2 — инерционное правило Тейлора** (спека §6): OLS
  `i_t = c + ρ·i_{t−1} + a·π + b·gap` на `[1, rate_before, pi_saar, gap]` с
  ковариацией HAC (`maxlags=4`, small-sample correction — та же конфигурация,
  что у B3 и Mincer–Zarnowitz); шаг прогноза — в БАЗИСНЫХ пунктах
  (`(pred_level − rate_before) × 100`), как у факта и остальных прогнозов;
  вырожденное окно — `DegenerateWindowError`, и walk-forward пропускает фолд
  со счётчиком;
* **долгосрочные импликации** — отдельным блоком отчёта: коэффициенты
  правила и его устойчивое состояние (`r*`, `α`, `β`); формулы выведены в
  docstring `taylor_long_run` и НЕ ограничивают свободную оценку.

Что добавляет этот срез (Task 5):

* **метод №3 — упорядоченная логит-модель решения** (спека §6):
  `OrderedModel(distr='logit')` (statsmodels) на регрессорах `[rate_before,
  pi_saar, gap]`, классы {−1, 0, +1}; выход — 3-кортеж вероятностей в этом же
  порядке и точечный шаг `E[step] = Σ_c P(c)·μ_c` в БАЗИСНЫХ пунктах (μ_c —
  средний шаг класса на обучающем окне, без утечек);
* **базлайн `climatology`** (спека §7) — та же постановка на безусловных
  частотах классов обучающего окна: отсчётная точка Brier и log loss метода №3,
  точечный шаг — тем же правилом `E[step]`, и та же конвенция вырожденности
  (окно из одного класса пропускается — см. `fit_climatology`);
* **вероятностные метрики** (Brier, log loss, надёжностная таблица) считаются
  для обеих вероятностных строк, а вырожденные окна метода №3 (один класс,
  неидентифицируемый дизайн, несошедшийся MLE, сингулярная информация) —
  `DegenerateWindowError`, и walk-forward пропускает фолд со счётчиком.

**Веса наблюдений (флаг `--shock-weights PATH`).** CSV `meeting_date,weight`
(лишние колонки игнорируются; `0 < w ≤ 1`) задаёт вес заседания в обучении:
правило Тейлора оценивается `WLS` на тех же регрессорах и с той же HAC-
ковариацией, упорядоченный логит — подклассом `WeightedOrderedModel`, который
умножает вклад наблюдения в `loglikeobs`. Бенчмарки B1–B3 (эталоны) и
`climatology` не взвешиваются; отсутствующая в CSV дата получает вес 1.0 со
счётчиком покрытия. Без флага путь прежний байт-в-байт (OLS/`OrderedModel` и
те же артефакты); с флагом в отчёт добавляется раздел «Веса шоков»
(покрытие, распределение, weighted vs unweighted RMSE/MAE, DM-HLN, топ-10
пониженных заседаний), а в CSV прогнозов — колонка `weight` (пустая у
невзвешенных методов). Спека §7:
`docs/superpowers/specs/2026-10-19-key-rate-shock-classifier-design.md`.

**Текущий прогноз (Task 6).** Ближайшее заседание — первая дата строго позже
даты прогона (`current_forecast`); признаки прогнозной строки снимаются as-of
даты прогона по правилам §4 (`forecast_row`), а факты будущего заседания в неё
кладёт плейсхолдер (`Decision` с неизвестными полями): методов они не касаются —
методы читают только `rate_before` и `x`, и это проверяется подменой фактов на
мусор. Методы §6 (`taylor`, `ordered_logit`) обучаются на ПОЛНОЙ выборке;
отсутствие входа на дату среза или вырожденная выборка — метод ВЫПАДАЕТ из
прогноза с пометкой в `note`, прогон не падает. Нет будущих заседаний — прогноз
не делается, JSON не пишется, прогон не падает. Артефакт —
`key_rate_model_forecast.json` (`FORECAST_NAME`), раздел «Текущий прогноз» —
в отчёте. В `forecast_log` сам скрипт прогноз НЕ пишет: запись выполняет
отдельный writer `scripts/key_rate_forecast_log.py` (`make key-rate-forecast-log`)
через ingest-контур (`POST /v1/key_rate_forecast`).

**Консенсус-бенчмарк №4 (спека
`docs/superpowers/specs/2026-10-19-key-rate-consensus-benchmark-design.md`
§2–§4).**
Медиана макроэкономического опроса ЦБ (`v_cbr_mo_br`, волна доступна с
`wave_date + 6 дней`) сравнивается с фактической среднегодовой ставкой
(бакеты по горизонту, наивный hold, DM), текущая волна печатается отдельно,
а B4 механически проецирует годовую медиану на шаг ближайшего заседания.
Сценарий удержания (`ordered_logit`) — мост к годовой средней. B4 не входит в
`METHODS`/`FORECAST_METHODS`, JSON и журнал: его дом — раздел отчёта.

Что остаётся за пределами среза — спека §11: метод №1 (рыночные вероятности из
KEYRATE) и ансамбль, байесовские priors и «inflated»-версия, ML-трек,
point-in-time слой, витрина и графики.

**По умолчанию скрипт в ClickHouse не пишет** — только читает (прецедент
`polyus_output_model.py`); с `ML_LOG=1` пишет паспорт и fold-предсказания
через `scripts/lib/ml_experiment.py` (журнал эксперимента). Транспорт (`ch_query`)
импортируется из `scripts/models/polyus/aisc.py`, а не копируется: там уже есть
маскирование секретов в текстах ошибок, HTTP-порт 8123 и обработка обрывов
соединения.
Общие оценочные функции (RMSE/MAE, DM с поправкой HLN, блочный бутстрап
разницы RMSE) вынесены в `scripts/lib/eval_utils.py` (`§4` спеки
`docs/superpowers/specs/2026-10-16-cpi-nowcast-design.md`): ниже от них
остались обёртки с прежними именами и значениями по умолчанию.
Секреты приходят только из окружения (их подставляет Makefile).

Запуск — только через `make` (переменные окружения подключает Makefile):

* `make key-rate-model` — полный прогон: сводка выборки, walk-forward
  бенчмарков по двум окнам, текущий прогноз, отчёт, CSV и JSON прогноза;
* `make key-rate-model ARGS="--rolling"` — то же, но консольная таблица
  метрик печатается по скользящему окну 40 (в отчёт и CSV оба окна идут
  всегда);
* `make key-rate-model ARGS="--shock-weights shock_classifier_weights.csv"` —
  взвешенные `taylor`/`ordered_logit`, раздел «Веса шоков» и колонка `weight`
  в CSV (бенчмарки B1–B3 не взвешиваются);
* `make key-rate-model ARGS="--export-fixtures scripts/testdata/key_rate"` —
  пересборка CSV-фикстур офлайн-тестов `scripts/models/key_rate/key_rate_model_test.py`.

**As-of — приближение, а не point-in-time слой** (спека §4): ревизии
объясняющих рядов и точные даты публикаций не учтены, полноценный PIT-слой —
следующий пункт фазы 4.
"""

import argparse
import bisect
import csv
import json
import logging
import math
import os
import re
import sys
import warnings
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable, Optional

import numpy as np
import statsmodels.api as sm
from statsmodels.miscmodels.ordinal_model import OrderedModel
from statsmodels.regression.linear_model import RegressionResultsWrapper

ROOT = Path(__file__).resolve().parents[3]

log = logging.getLogger(__name__)

# Скрипт-сосед: переиспользуем транспорт к ClickHouse, а не копируем его
# (прецедент `polyus_output_model.py`).
sys.path.insert(0, str(ROOT / "scripts"))

# Скрипты-соседи: транспорт к ClickHouse, общие оценочные функции (RMSE/MAE,
# DM с HLN, блочный бутстрап; вынесены в `eval_utils.py`, их же переиспользует
# ML-трек nowcast'а ИПЦ) и журнал экспериментов (`ml_experiment.py`).
from lib import eval_utils as eu
from lib import clickhouse as ch
from lib import ml_experiment as mlx

ch_query = ch.ch_query

# Суффикс `FINAL` обязателен во всех запросах ниже: таблицы — ReplacingMergeTree,
# и без него вытесненные копии строк дали бы дубли точек (и дубли заседаний).
# `FORMAT JSON` — требование транспорта: `ch_query` разбирает JSON-ответ, а
# HTTP-интерфейс по умолчанию отвечает TabSeparated.

# Канонический title заседания в `events_calendar`. Ручные строки-триггеры
# (сид `events_calendar_q4_2026.sql`) лежат в той же категории `cbr` с другими
# title и отсекаются ЭТИМ фильтром. Фильтр живёт в Python (запрос берёт всю
# категорию): так правило — одно, и оно проверяется на фикстуре, в которой
# ручные строки присутствуют.
SAMPLE_TITLE = "СД ЦБ РФ: заседание по ключевой ставке"

# Ряд базового ИПЦ ЦБ: сезонно очищенный месячный темп, % к предыдущему месяцу
# (`cbr_indicators_cpd`, дата точки — последний день месяца).
CPD_CORE_SERIES = "Базовый ИПЦ"

# Ряд ИБК ЦБ SA (пункты) — прокси разрыва выпуска (`macro_series`,
# `source='cbr_survey'`; дата точки — 1-е число отчётного месяца).
SURVEY_SOURCE = "cbr_survey"
IBK_SERIES = "business_climate_sa"

# Консенсус-бенчмарк №4 — медиана макроэкономического опроса ЦБ (спека
# `2026-10-19-key-rate-consensus-benchmark-design.md` §2). `CONSENSUS_SOURCE` —
# источник рядов в `macro_series`: витрина `v_cbr_mo_br`, которую читает
# `CONSENSUS_SQL`, построена по `macro_series FINAL WHERE source =
# CONSENSUS_SOURCE`; константа фиксирует контракт источника (проверяется
# тестом). Публикация волны наступает через 2–6 дней после полевого периода,
# поэтому точка волны считается доступной только на `wave_date + 6 дней` —
# консервативно и без заглядывания вперёд.
CONSENSUS_SOURCE = "cbr_mo_br"
CONSENSUS_PUBLICATION_LAG_DAYS = 6

# Порядок бакетов годового бенчмарка в отчёте (спека §3.1): сначала экс-анте
# (волна опубликована до начала года), затем внутригодовые — от дальнего
# горизонта к ближнему. Имена — контракт таблицы отчёта, поэтому порядок задан
# здесь, а не в рендерере.
CONSENSUS_BUCKETS = ("ex-ante", "10-12", "7-9", "4-6", "1-3")

# Guard B4 (спека §3.2): на остаток года должно оставаться не меньше
# 30 дней, иначе `r_implied` делится на короткий хвост и становится шумом.
CONSENSUS_MIN_DAYS_REMAINING = 30

# Короткие и стабильные метки причин пропуска B4 (спека §5.6). Подробные
# сообщения с датами/годами даёт `_consensus_path_with_reason`; в отчёте
# пропуски группируются по этим категориям, а диапазон дат печатается
# отдельно. Порядок кортежа — порядок строк в разделе отчёта.
CONSENSUS_SKIP_NO_WAVE = "нет волны на год заседания"
CONSENSUS_SKIP_RATE_AT_WAVE = "ряд ставки не покрывает дату доступности волны"
CONSENSUS_SKIP_RATE_YEAR_START = "ряд ставки не покрывает 1 января года заседания"
CONSENSUS_SKIP_SHORT_REMAINDER = (
    f"на остаток года меньше {CONSENSUS_MIN_DAYS_REMAINING} дней"
)
CONSENSUS_SKIP_IMPLIED_RANGE = "`r_implied` вне (−100, 100) %"
CONSENSUS_SKIP_UNKNOWN = "причина не определена"
CONSENSUS_SKIP_REASONS = (
    CONSENSUS_SKIP_NO_WAVE,
    CONSENSUS_SKIP_RATE_AT_WAVE,
    CONSENSUS_SKIP_RATE_YEAR_START,
    CONSENSUS_SKIP_SHORT_REMAINDER,
    CONSENSUS_SKIP_IMPLIED_RANGE,
    CONSENSUS_SKIP_UNKNOWN,
)

# Режимный срез годового бенчмарка (Task 5, спека §3.1) — завершённые годы
# текущего режима. Срез описательный: гиперпараметры на нём не подбираются.
CONSENSUS_REGIME_FROM = 2024
CONSENSUS_REGIME_TO = 2025

# Естественный цикл волн — 8 волн/год ≈ 6–7 недель (спека §4). Волна старше
# порога называется устаревшей: последняя публикация могла быть пропущена,
# и текущий консенсус не подменяется более свежими данными.
CONSENSUS_STALE_AFTER_DAYS = 49

# Метка окна B4 в `Prediction` (Task 4): механическая проекция не обучается и
# от окна walk-forward не зависит — у неё одно «окно», и оно не входит в
# общий перебор `METHODS × (expanding, rolling40)`.
CONSENSUS_WINDOW = "consensus"

# Сид для бутстрапа (Task 3+) и «версии набора данных»: воспроизводимость
# прогона считается от одного числа, названного здесь и больше нигде.
SEED = 20261015

# Горизонт окна исхода `d+7` (спека §3): верхняя граница в календарных днях.
OUTCOME_WINDOW_DAYS = 7

# Окно срезов-фикстур офлайн-тестов. Верхняя граница — 2016-12-31: у последнего
# заседания среза (2016-12-16) окно исхода (2016-12-23) ещё наблюдаемо целиком,
# а у заседаний 2017+ оно уже выходит за ряд ставки — так на фикстуре
# проверяется правило «окно исхода вне данных ставки → заседание вне выборки»
# (на живом прогоне оно отсеивает будущие заседания). Нижняя граница 2013-01-01
# взята с запасом, чтобы ставка «на d−1» первого заседания (2014-02-14) была
# покрыта. Имена файлов фикстур (`*_2013_2016`) равны фактическому окну содержимого.
FIXTURE_FROM = date(2013, 1, 1)
FIXTURE_TO = date(2016, 12, 31)

# --- Константы бэктеста (спека §5/§7) -----------------------------------------

# Walk-forward: минимум заседаний в обучении (расширяющееся окно) и размер
# скользящего окна. Обе схемы докладываются как проверка робастности; выбор
# окна по ошибке не делается (спека §7) — окна считаются всегда оба.
MIN_TRAIN = 24
ROLLING_WINDOW = 40

# Метка расширяющегося окна в отчёте и CSV; скользящее — `rolling<N>`.
WINDOW_EXPANDING = "expanding"

# Лаги HAC (Ньюи–Уэст) — 4 заседания ≈ полугодие между решениями (спека §6).
# Одна конфигурация на AR(1), Mincer–Zarnowitz и методы №2–3: p-value разных
# строк отчёта обязаны считаться на одинаковой ковариации.
HAC_MAXLAGS = 4

# Режимный блок отчёта — 2024–2026 (спека §7). В тексте спеки заседаний 24,
# но октябрь и декабрь 2026 на дату прогона ещё будущие: в блок входят
# прошедшие заседания, и отчёт печатает их число сам.
REGIME_FROM = date(2024, 1, 1)
REGIME_TO = date(2026, 12, 31)

# OLS с HAC не оценивается на слишком коротком окне: две точки — нулевые
# остаточные степени свободы, и поправка ковариации делит на ноль.
MIN_OLS_OBS = 3

# Метод №2 (правило Тейлора, спека §6) — четыре параметра: свободный член и три
# регрессора `[rate_before, pi_saar, gap]`. Пять наблюдений — не запас, а
# минимум: при четырёх остаётся ровно одна остаточная степень свободы, без неё
# HAC с small-sample correction делит на ноль (проверено на statsmodels).
MIN_TAYLOR_OBS = 5

# Метод №3 (упорядоченный логит, спека §6) — три регрессора и до двух порогов:
# шесть наблюдений оставляют MLE ровно одну остаточную степень свободы. Меньше —
# не запас, а вырожденное окно (как MIN_TAYLOR_OBS у метода №2).
MIN_ORDERED_OBS = 6

# Предел итераций MLE метода №3 (умолчание `OrderedModel` — 500). Замер
# 2026-10-16, uv-набор приёмки, `OrderedModel(...).fit(method="newton", …)`:
# окно из 60 заседаний с РАЗДЕЛИМЫМИ классами (конечного максимума
# правдоподобия нет) при `maxiter=500` исчерпывает предел — 500 итераций,
# 1.550 с, `converged=False`; при `maxiter=50` та же попытка — 50 итераций,
# 0.156 с, так же `converged=False` (вырожденный фолд распознаётся тем же
# сигналом). Нормальные окна сходятся за 6–8 итераций и 0.02–0.03 с: 24/40/84
# заседания синтетики `ordinal_rows` и 24/26 заседаний фикстуры — все
# `converged=True` без предупреждений. То есть предел бьёт только по тем окнам,
# которые и не сходятся, и ограничивает цену вырожденного фолда в ~10 раз.
ORDERED_MAXITER = 50

# ρ = 1 в пределах машинной погрешности: у правила Тейлора нет устойчивого
# состояния, и `c/(1−ρ)` — деление на ноль. ρ, близкие к 1, но отличные от неё,
# НЕ отсекаются: свободная оценка докладывается как есть (спека §6).
TAYLOR_STEADY_EPS = 1e-12

# Точная подгонка MZ (остатков нет) в пределах машинной погрешности: F-тест
# тогда 0/0, и шум округления дал бы ложное p. Допуск по остаточной сумме
# квадратов относительный (масштаб — сумма квадратов факта), по коэффициентам —
# абсолютный (уровни в % и б.п.).
MZ_EXACT_RTOL = 1e-12
MZ_EXACT_ATOL = 1e-9

# Нижний порог вероятности в log loss: нулевая вероятность факта иначе дала бы
# бесконечность, и среднее по фолдам «уехало» бы в inf.
LOG_LOSS_EPS = 1e-15

# Артефакты прогона в каталоге прогона (в git не коммитятся, см. .gitignore).
REPORT_NAME = "key_rate_model_report.md"
PREDICTIONS_NAME = "key_rate_model_predictions.csv"
FORECAST_NAME = "key_rate_model_forecast.json"

# Спека прогона — путь от корня репозитория в паспорте журнала экспериментов
# (`ml_experiment.start_run(spec_path=...)`).
SPEC_PATH = "docs/superpowers/specs/2026-10-15-key-rate-model-v0-design.md"

# Все артефакты §8 одним списком — для ранних выходов прогона (пустой календарь,
# пустая выборка), где не пишется НИ ОДИН из них: даты в именах файлов нет,
# поэтому оставшийся файл прошлого прогона читается как результат текущего, и
# убирать надо каждый (`clear_stale_artifacts`).
ARTIFACT_NAMES = (REPORT_NAME, PREDICTIONS_NAME, FORECAST_NAME)

# Колонки CSV прогнозов. `probs` — вероятности классов в порядке (−1, 0, +1)
# через «|»; пустая ячейка — метод распределения не даёт (бенчмарки B1–B3).
PREDICTION_COLUMNS = (
    "meeting_date",
    "method",
    "window",
    "actual_step_bp",
    "actual_y3",
    "step_bp",
    "probs",
)

# Колонка веса в CSV при `--shock-weights`: добавляется В КОНЕЦ, прежний
# порядок колонок не меняется. Пустая ячейка — метод веса не использует
# (бенчмарки B1–B3 и базлайн `climatology`).
SHOCK_WEIGHT_COLUMN = "weight"

# --- Константы запросов -------------------------------------------------------

MEETINGS_SQL = """
SELECT event_date, category, title
FROM events_calendar FINAL
WHERE category = 'cbr'
ORDER BY event_date
FORMAT JSON
"""

KEY_RATE_SQL = """
SELECT date, rate
FROM cbr_key_rate FINAL
ORDER BY date
FORMAT JSON
"""

CPD_CORE_SQL = f"""
SELECT date, value
FROM cbr_indicators_cpd FINAL
WHERE name = '{CPD_CORE_SERIES}'
ORDER BY date
FORMAT JSON
"""

IBK_SQL = f"""
SELECT date, value
FROM macro_series FINAL
WHERE source = '{SURVEY_SOURCE}' AND series = '{IBK_SERIES}'
ORDER BY date
FORMAT JSON
"""

MP_SUMMARY_SQL = """
SELECT meeting_date, decision_rate, decision_delta_bp
FROM cbr_mp_summary FINAL
ORDER BY meeting_date
FORMAT JSON
"""

# `FINAL` здесь не ставится: `v_cbr_mo_br` — витрина, и `FINAL` уже стоит в её
# определении (`macro_series FINAL WHERE source = 'cbr_mo_br'`).
CONSENSUS_SQL = """
SELECT date, series, value
FROM v_cbr_mo_br
WHERE series LIKE 'key_rate_avg_%'
ORDER BY date, series
FORMAT JSON
"""


def _fixture_sql(sql: str) -> str:
    """Тот же запрос, но в окне фикстур `[FIXTURE_FROM, FIXTURE_TO]`.

    Диапазон вставляется в `WHERE` готового запроса, а не дублирует его текст:
    колонки, `FINAL` и имена рядов обязаны совпадать у живого и фикстурного
    запроса, иначе фикстура проверяла бы не то, что читает прогон.
    """
    where = f"date >= '{FIXTURE_FROM.isoformat()}' AND date <= '{FIXTURE_TO.isoformat()}'"
    if "\nWHERE " in sql:
        return sql.replace("\nWHERE ", f"\nWHERE {where} AND ", 1)
    return sql.replace("\nORDER BY ", f"\nWHERE {where}\nORDER BY ", 1)


FIXTURE_KEY_RATE_SQL = _fixture_sql(KEY_RATE_SQL)
FIXTURE_CPD_CORE_SQL = _fixture_sql(CPD_CORE_SQL)
FIXTURE_IBK_SQL = _fixture_sql(IBK_SQL)

# Колонки CSV-фикстур — контракт с тестами: те же имена, что у колонок SELECT
# (строки фикстур читаются теми же `*_from_rows`, что и ответы ch_query).
FIXTURE_COLUMNS = {
    "meetings.csv": ("event_date", "category", "title"),
    "key_rate_2013_2016.csv": ("date", "rate"),
    "cpd_core_2013_2016.csv": ("date", "value"),
    "ibk_2013_2016.csv": ("date", "value"),
    "mp_summary.csv": ("meeting_date", "decision_rate", "decision_delta_bp"),
}

# Ручная строка-триггер с УНИКАЛЬНОЙ датой — только для фикстуры, добавляется
# ЭКСПОРТОМ (регенерация обязана давать тот же файл). Зачем: обе живые ручные
# строки сида делят дату с каноническими (2026-10-23, 2026-12-18 помечены и
# сидом, и календарём ЦБ), поэтому по датам фильтр по `title` на фикстуре
# ненаблюдаем — дата осталась бы в списке и без него. У даты 2016-12-20
# канонической строки нет: без фильтра она попала бы в выборку и дала бы лишнее
# «решение», а тест это ловит.
FIXTURE_MEETINGS_EXTRA = (
    (date(2016, 12, 20), "cbr", "СД ЦБ РФ: тест-фикстура — ручной триггер с уникальной датой"),
)


class SampleError(Exception):
    """Выборка решений внутренне противоречива (fail-fast, спека §3).

    Поднимается, когда шаг, посчитанный по `cbr_key_rate`, расходится с
    `decision_delta_bp` резюме ЦБ: один из двух источников (или сборка окна
    исхода) неверен, и продолжать прогон нельзя — ярлык модели оказался бы
    не тем, что решил ЦБ. Это ошибка прогона, а не строка отчёта.
    """


# --- Разбор строк: чистые функции, тестируемые на фикстурах -------------------

ONE_DAY = timedelta(days=1)


def row_date(value) -> date:
    """Дата из значения строки: `date`/`datetime` или ISO-строка ClickHouse.

    JSON-ответ SQL-транспорта отдаёт `Date`/`Date32` строками `YYYY-MM-DD`,
    CSV-фикстуры — тем же форматом; уже разобранное значение возвращается как
    есть, чтобы функцию можно было звать повторно.
    """
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def row_float(value) -> float:
    """Число из значения строки. NULL/пустая ячейка — ошибка, а не ноль.

    Молчаливый ноль на месте значения исказил бы и ставку, и признак: ноль
    ставки в 2014-м выглядел бы измеренным.
    """
    if value is None or value == "":
        raise ValueError("пустое числовое значение в ряду: NULL или пустая ячейка")
    return float(value)


def row_optional_int(value) -> Optional[int]:
    """Целое или None: NULL (`None`) и пустая ячейка CSV (`''`) — одно и то же."""
    if value is None or value == "":
        return None
    return int(value)


def meetings_from_rows(rows: list) -> list:
    """Даты канонических заседаний из строк `events_calendar`.

    Отбор: `category='cbr'` ∧ `title=SAMPLE_TITLE` (фильтр по title отсекает
    ручные строки-триггеры); дедуп и сортировка — свойство выборки, а не
    порядка строк ответа. Дедуп здесь — НОРМА, а не защита от дублей: у одного
    заседания законно лежат несколько строк (каноническая + ручной триггер), и
    без сведения по дате одна дата дала бы два «заседания».
    """
    meetings = []
    for row in rows:
        if str(row.get("category", "")) != "cbr":
            continue
        if str(row.get("title", "")) != SAMPLE_TITLE:
            continue
        meetings.append(row_date(row["event_date"]))
    return sorted(set(meetings))


def _unique_dates(points: list, series: str) -> list:
    """Отказывает на повторяющейся дате точки ряда (возвращает точки как есть).

    Дубль даты в таблице без FINAL (или с неверным ключом) означал бы ДВЕ точки
    одного дня: as-of выбрал бы одну из них по порядку сортировки, и решение
    молча зависело бы от порядка строк в ответе ClickHouse. Поэтому отказ с
    датой, а не «последний победил»: этот класс дефектов иначе не виден.
    """
    seen = set()
    for point_date, _ in points:
        if point_date in seen:
            raise ValueError(
                f"{series}: дата {point_date.isoformat()} встречается дважды — "
                f"повторная точка исказила бы as-of"
            )
        seen.add(point_date)
    return points


def key_rate_from_rows(rows: list) -> list:
    """Точки `cbr_key_rate`: `(дата действия, ставка %)`, по возрастанию даты."""
    points = [(row_date(row["date"]), row_float(row["rate"])) for row in rows]
    return sorted(_unique_dates(points, "cbr_key_rate"))


def cpd_from_rows(rows: list) -> list:
    """Точки базового ИПЦ: `(конец месяца M, SA м/м %)`, по возрастанию даты."""
    points = [(row_date(row["date"]), row_float(row["value"])) for row in rows]
    return sorted(_unique_dates(points, CPD_CORE_SERIES))


def ibk_from_rows(rows: list) -> list:
    """Точки ИБК SA: `(1-е число отчётного месяца, пункты)`, по возрастанию."""
    points = [(row_date(row["date"]), row_float(row["value"])) for row in rows]
    return sorted(_unique_dates(points, IBK_SERIES))


def mp_deltas_from_rows(rows: list) -> list:
    """Шаги решений из `cbr_mp_summary`: `(дата заседания, б.п. или None)`.

    `None` у самого раннего резюме — «предыдущего решения в ряду нет», а не
    «шаг равен нулю»: с NULL сверка не выполняется (сверять не с чем).
    """
    points = [
        (row_date(row["meeting_date"]), row_optional_int(row["decision_delta_bp"]))
        for row in rows
    ]
    return sorted(_unique_dates(points, "cbr_mp_summary"))


# Имена рядов консенсуса: `key_rate_avg_<год>_<статистика>`; прочие ряды
# витрины (ИПЦ, ВВП и т.п.) парсер пропускает.
CONSENSUS_SERIES_RE = re.compile(r"^key_rate_avg_(\d{4})_(median|p10|p90|answers)$")


@dataclass(frozen=True)
class ConsensusForecast:
    """Волна консенсуса по одному году: медиана, интервал и число ответов.

    `wave_date` — дата волны опроса (1-е число месяца), `available_from` —
    консервативная дата публикации (`wave_date + 6 дней`): раньше неё волна в
    выборку не попадает. `answers` — число ответов участников, а не прогноз.
    """

    wave_date: date
    available_from: date
    year: int
    median: float
    p10: Optional[float]
    p90: Optional[float]
    answers: Optional[int]


def _consensus_stat(value, wave_date: date, year: int, stat: str) -> float:
    """Число статистики консенсуса: пустое, NaN и inf — ошибка данных.

    `row_float` отсекает только NULL и пустую ячейку, но NaN/inf прошли бы
    `float()` и попали в прогноз как «измеренное» значение: NaN заразил бы
    медиану и метрики, inf — деление на остаток года. Конечность проверяется
    здесь, с именем волны, года и статистики — в том числе для `answers`,
    чтобы `int(inf)` не дал `OverflowError`, а `int(nan)` — текст «не целое».
    """
    number = row_float(value)
    if not math.isfinite(number):
        raise ValueError(
            f"консенсус: {stat}={number} у волны {wave_date.isoformat()} "
            f"за {year} — не конечное число"
        )
    return number


def consensus_from_rows(rows: list) -> list:
    """Волны консенсуса из строк `v_cbr_mo_br`: `(волна, год) → прогноз`.

    Берутся только ряды `key_rate_avg_<год>_<статистика>`; прочие ряды витрины
    (ИПЦ, ВВП и др.) пропускаются — запрос уже сужен `LIKE`, но парсер не
    обязан верить фильтру вызывающего. `median` обязательна: волна без неё —
    не консенсус, а огрызок. Дубль статистики в паре — потерянный `FINAL` или
    неверный ключ витрины, поэтому отказ, а не «последний победил»: иначе
    значение молча зависело бы от порядка строк ответа ClickHouse. `answers`
    приводится к целому; нецелое значение — дефект источника, а NaN/inf —
    отказ ещё до `int()`.
    """
    grouped = {}
    for row in rows:
        match = CONSENSUS_SERIES_RE.match(str(row.get("series", "")))
        if match is None:
            continue
        wave_date = row_date(row["date"])
        year = int(match.group(1))
        stat = match.group(2)
        stats = grouped.setdefault((wave_date, year), {})
        if stat in stats:
            raise ValueError(
                f"консенсус: дубль {stat} у волны {wave_date.isoformat()} "
                f"за {year} — повторная строка исказила бы прогноз"
            )
        stats[stat] = _consensus_stat(row["value"], wave_date, year, stat)

    waves = []
    for (wave_date, year), stats in sorted(grouped.items()):
        if "median" not in stats:
            raise ValueError(
                f"консенсус: у волны {wave_date.isoformat()} за {year} нет медианы"
            )
        answers = stats.get("answers")
        if answers is not None:
            if answers != int(answers):
                raise ValueError(
                    f"консенсус: answers={answers} у волны {wave_date.isoformat()} "
                    f"за {year} — не целое число ответов"
                )
            answers = int(answers)
        waves.append(
            ConsensusForecast(
                wave_date=wave_date,
                available_from=wave_date
                + timedelta(days=CONSENSUS_PUBLICATION_LAG_DAYS),
                year=year,
                median=stats["median"],
                p10=stats.get("p10"),
                p90=stats.get("p90"),
                answers=answers,
            )
        )
    return waves


# --- Годовой бенчмарк: средняя, наивный hold и наблюдения (спека §3.1) --------


def _rate_asof(points: list, day: date) -> Optional[float]:
    """Последняя точка ряда ставки с датой `<= day` (fill-forward), или None.

    Контракт входа: `points` отсортированы по дате (так их отдают
    `key_rate_from_rows`/`load_key_rate`). Ряд ставки — ступенчатая функция:
    до первой точки значения нет (`None`), после точки действует её значение
    до следующего изменения. `bisect` по ключу-дате — O(log n) без среза
    ряда на каждый календарный день.
    """
    index = bisect.bisect_right(points, day, key=lambda point: point[0])
    if index == 0:
        return None
    return points[index - 1][1]


def sum_rates_between(points: list, start: date, end: date) -> float:
    """Сумма ставок по КАЛЕНДАРНЫМ дням `[start, end]` включительно.

    Fill-forward: каждый день берёт последнюю точку с датой `<= дня` (спека
    §3.1: «с учётом выходных дней»). Отсутствие ставки на `start` — это
    `ValueError`, а не ноль: ноль молча занизил бы `R_known` и среднегодовую
    и выглядел бы как измеренное значение.

    Сумма считается отрезками между точками, а не 365–366 вызовами as-of:
    результат тот же, но на длинном ряду это O(точек), а не O(дней·log n).
    """
    if end < start:
        raise ValueError(
            f"ставка: конец интервала {end.isoformat()} раньше начала {start.isoformat()}"
        )
    index = bisect.bisect_right(points, start, key=lambda point: point[0])
    if index == 0:
        raise ValueError(
            f"ставка: нет точки на {start.isoformat()} — сумму не из чего собрать"
        )
    day = start
    rate = points[index - 1][1]
    total = 0.0
    while index < len(points) and points[index][0] <= end:
        next_day = points[index][0]
        total += rate * (next_day - day).days
        day = next_day
        rate = points[index][1]
        index += 1
    total += rate * ((end - day).days + 1)
    return total


def annual_average_rate(points: list, year: int) -> Optional[float]:
    """Фактическая среднегодовая ставка года `year` по календарным дням.

    `None`, если ряд не покрывает год: первая точка позже 1 января или
    последняя раньше 31 декабря (спека §3.1). Так неполный текущий год не
    выдаёт «среднюю с начала года» за факт. Число дней — календарное (366 в
    високосном году): формулировка опроса — «с учётом выходных дней».
    """
    first_day = date(year, 1, 1)
    last_day = date(year, 12, 31)
    if not points or points[0][0] > first_day or points[-1][0] < last_day:
        return None
    days = (last_day - first_day).days + 1
    return sum_rates_between(points, first_day, last_day) / days


def _consensus_bucket(available_from: date, year: int) -> str:
    """Бакет волны по горизонту до конца года `year` (спека §3.1).

    Горизонт — число календарных месяцев от месяца публикации до декабря
    включительно: январь–март — 12–10 месяцев (`10-12`), апрель–июнь — 9–7,
    июль–сентябрь — 6–4, октябрь–декабрь — 3–1. Волна, опубликованная до
    1 января, — `ex-ante`. Границы проверяются по `available_from`, а не по
    `wave_date`: в выборку волна входит датой публикации.
    """
    if available_from < date(year, 1, 1):
        return "ex-ante"
    months = 12 - available_from.month + 1
    if months >= 10:
        return "10-12"
    if months >= 7:
        return "7-9"
    if months >= 4:
        return "4-6"
    return "1-3"


def hold_forecast(points: list, year: int, available_from: date) -> Optional[float]:
    """Наивный прогноз «ставка не меняется» для среднегодовой года `year`.

    Формула спеки §3.1. Если волна доступна до 1 января, известного пути в
    году нет: hold равен ставке на `available_from` на весь год. Иначе
    известная часть `[1 января, A]` суммируется фактическим путём, а остаток
    года `A+1 … 31 декабря` — ставкой на `A`. `None` — на `available_from`
    нет ставки (ряд начинается позже): вызывающий решает, что это значит.
    """
    first_day = date(year, 1, 1)
    last_day = date(year, 12, 31)
    rate = _rate_asof(points, available_from)
    if rate is None:
        return None
    if available_from < first_day:
        return rate
    if available_from > last_day:
        raise ValueError(
            f"hold: available_from {available_from.isoformat()} позже конца {year} года"
        )
    known = sum_rates_between(points, first_day, available_from)
    days = (last_day - first_day).days + 1
    remaining = (last_day - available_from).days
    return (known + rate * remaining) / days


@dataclass(frozen=True)
class ConsensusObservation:
    """Годовая пара «волна консенсуса — факт» (спека §3.1).

    `actual` — фактическая среднегодовая ставка года `year`; ошибки — в
    БАЗИСНЫХ пунктах, `(прогноз − факт) × 100`, положительная — прогноз
    завысил. `bucket` — горизонт волны: `ex-ante` (публикация до 1 января
    года) или число месяцев до конца года (`10-12` … `1-3`). `p10`/`p90` и
    `answers` переносятся из волны для аудита, в метрики не входят.
    """

    year: int
    wave_date: date
    available_from: date
    bucket: str
    median: float
    p10: Optional[float]
    p90: Optional[float]
    answers: Optional[int]
    actual: float
    consensus_error_bp: float
    hold_error_bp: float


def consensus_observations(consensus: list, key_rate: list) -> list:
    """Пары «волна — факт» завершённых лет (спека §3.1), по `(год, доступность)`.

    Год считается завершённым, если `annual_average_rate` вернула факт (ряд
    покрывает 1 января и 31 декабря). Для каждого такого года берутся:

    * **экс-анте** — последняя волна с медианой на год, опубликованная строго
      до 1 января (`available_from < 1 января Y`); для 2021 её нет — опрос
      начинается с 2021-05;
    * **внутригодовые** — все волны с `1 января Y <= available_from <
      1 января Y+1`.

    Медиана вне этого окна (волна на будущий год, опубликованная посреди
    текущего) в выборку не попадает. Hold считается по §3.1; отсутствие
    ставки на дату доступности в завершённом году — `ValueError`: это разрыв
    ряда, а не норма, и молча выбросить пару значило бы сузить выборку.
    """
    observations = []
    for year in sorted({wave.year for wave in consensus}):
        actual = annual_average_rate(key_rate, year)
        if actual is None:
            continue
        first_day = date(year, 1, 1)
        next_first_day = date(year + 1, 1, 1)
        waves = [wave for wave in consensus if wave.year == year]
        ex_ante = [wave for wave in waves if wave.available_from < first_day]
        selected = []
        if ex_ante:
            selected.append(max(ex_ante, key=lambda wave: wave.available_from))
        selected.extend(
            wave for wave in waves if first_day <= wave.available_from < next_first_day
        )
        for wave in selected:
            hold = hold_forecast(key_rate, year, wave.available_from)
            if hold is None:
                raise ValueError(
                    f"консенсус: нет ставки на {wave.available_from.isoformat()} "
                    f"(волна {wave.wave_date.isoformat()} за {year})"
                )
            observations.append(
                ConsensusObservation(
                    year=year,
                    wave_date=wave.wave_date,
                    available_from=wave.available_from,
                    bucket=_consensus_bucket(wave.available_from, year),
                    median=wave.median,
                    p10=wave.p10,
                    p90=wave.p90,
                    answers=wave.answers,
                    actual=actual,
                    consensus_error_bp=(wave.median - actual) * 100.0,
                    hold_error_bp=(hold - actual) * 100.0,
                )
            )
    return sorted(
        observations,
        key=lambda observation: (observation.year, observation.available_from),
    )


@dataclass(frozen=True)
class ConsensusAggregate:
    """Метрики бакета: консенсус против наивного hold (спека §3.1).

    `n` — число пар; RMSE/MAE/bias — в базисных пунктах. Bias — средняя
    знаковая ошибка: положительная значит систематическое завышение.
    `dm_stat`/`dm_p` — Diebold–Mariano (HLN, `h=1`) на парных ошибках:
    положительная статистика значит, что консенсус ХУЖЕ hold (его ошибки
    больше). При `n < 2` тест не определён — `None`.
    """

    bucket: str
    n: int
    consensus_rmse_bp: float
    consensus_mae_bp: float
    consensus_bias_bp: float
    hold_rmse_bp: float
    hold_mae_bp: float
    hold_bias_bp: float
    dm_stat: Optional[float]
    dm_p: Optional[float]


def _dm_or_none(err_a: list, err_b: list) -> tuple:
    """DM (HLN, `h=1`) на парных ошибках; `(None, None)`, если тест не определён.

    Общее соглашение годового бенчмарка §3.1 и B4 §3.2: при `n < 2` (включая
    пустой список) тест не определён, а при нулевой дисперсии разницы потерь
    (`err_a² − err_b²` постоянна — одинаковые ошибки или постоянный сдвиг)
    t-статистике нечем делиться, и `p = 0.0` при `±inf` читался бы как
    доказанное превосходство, которого малая выборка не подтверждает. Длины
    рядов должны совпадать — это контракт вызывающего.
    """
    if len(err_a) < 2:
        return (None, None)
    differences = [a * a - b * b for a, b in zip(err_a, err_b)]
    if all(difference == differences[0] for difference in differences):
        return (None, None)
    return diebold_mariano(err_a, err_b, h=1)


def aggregate_consensus(observations: list, bucket: str) -> Optional[ConsensusAggregate]:
    """Метрики одного бакета годового бенчмарка (спека §3.1), или None.

    `None` — в бакете нет наблюдений. DM считается при `n >= 2`; при `n = 1`
    тест не определён и остаётся `None`, а RMSE/MAE/bias всё ещё считаются
    (в отчётности это одна пара). Вырожденная дисперсия разницы потерь (все
    `err_cons² − err_hold²` равны — одинаковые ошибки или постоянная
    ненулевая разница) тоже даёт `None`: t-статистике нечем делиться, а
    `p = 0.0` при `±inf` читался бы как доказанное превосходство, которого
    малая выборка не подтверждает. Guard вынесен в `_dm_or_none` — то же
    правило применяет B4 (§3.2); исключения DM не перехватываются.
    """
    selected = [
        observation for observation in observations if observation.bucket == bucket
    ]
    if not selected:
        return None
    consensus_errors = [observation.consensus_error_bp for observation in selected]
    hold_errors = [observation.hold_error_bp for observation in selected]
    dm_stat, dm_p = _dm_or_none(consensus_errors, hold_errors)
    return ConsensusAggregate(
        bucket=bucket,
        n=len(selected),
        consensus_rmse_bp=rmse_bp(consensus_errors),
        consensus_mae_bp=mae_bp(consensus_errors),
        consensus_bias_bp=sum(consensus_errors) / len(consensus_errors),
        hold_rmse_bp=rmse_bp(hold_errors),
        hold_mae_bp=mae_bp(hold_errors),
        hold_bias_bp=sum(hold_errors) / len(hold_errors),
        dm_stat=dm_stat,
        dm_p=dm_p,
    )


# --- B4: проекция годовой медианы на шаг заседания (спека §3.2) ---------------


@dataclass(frozen=True)
class ConsensusPath:
    """B4-проекция годовой медианы на шаг одного заседания (спека §3.2).

    Это МЕХАНИЧЕСКАЯ проекция, а не прогноз заседания: остаток года
    заполняется одной постоянной ставкой `implied_level`, при которой годовая
    средняя совпала бы с медианой консенсуса. `step_bp` — разница между этой
    проекцией и фактическим уровнем `rate_before` на `M−1`, в базисных
    пунктах. `days_remaining` — `N_rem`: число дней года с `A+1` до 31 декабря
    (для январского заседания — весь год `N_Y`).
    """

    meeting_date: date
    wave_date: date
    available_from: date
    year: int
    annual_median: float
    rate_before: float
    implied_level: float
    step_bp: float
    days_remaining: int


def _consensus_path_with_reason(
    consensus: list,
    key_rate: list,
    meeting: date,
    rate_before: float,
    asof: Optional[date] = None,
) -> tuple:
    """B4-проекция и причина её отсутствия (для пользовательского текста).

    Возврат — `(ConsensusPath | None, причина | None)`: причина заполнена
    только когда проекция не определена. Порядок guards — тот же, что у
    `consensus_path`, чтобы отчёт не расходился с расчётом.

    `asof` — дополнительный фильтр ТЕКУЩЕЙ проекции: волна обязана быть уже
    опубликована на дату среза (`available_from <= asof`), иначе это
    заглядывание вперёд. Бэктест B4 (`asof=None`) живёт по правилу §3.2 —
    волна строго до заседания.
    """
    year = meeting.year
    candidates = [
        wave
        for wave in consensus
        if wave.year == year and wave.available_from < meeting
    ]
    if asof is not None:
        available_candidates = [
            wave for wave in candidates if wave.available_from <= asof
        ]
        if not available_candidates and candidates:
            return (
                None,
                f"волна на {year} год ещё не опубликована на дату среза "
                f"{asof.isoformat()}",
            )
        candidates = available_candidates
    if not candidates:
        return (
            None,
            f"нет волны на {year} год с публикацией до заседания "
            f"{meeting.isoformat()}",
        )
    wave = max(candidates, key=lambda wave: (wave.available_from, wave.wave_date))
    available = wave.available_from
    if _rate_asof(key_rate, available) is None:
        return (
            None,
            f"ряд ставки не покрывает дату доступности волны "
            f"{available.isoformat()}",
        )
    first_day = date(year, 1, 1)
    last_day = date(year, 12, 31)
    days_in_year = (last_day - first_day).days + 1
    if available < first_day:
        implied = wave.median
        days_remaining = days_in_year
    else:
        days_remaining = (last_day - available).days
        if days_remaining < CONSENSUS_MIN_DAYS_REMAINING:
            return (
                None,
                f"на остаток {year} года меньше "
                f"{CONSENSUS_MIN_DAYS_REMAINING} дней",
            )
        # R_known начинается 1 января: без точки на эту дату сумму не собрать,
        # и `sum_rates_between` поднял бы ValueError. Для B4 это guard, а не
        # ошибка прогона — как и отсутствие ставки на `A`.
        if _rate_asof(key_rate, first_day) is None:
            return (None, f"ряд ставки не покрывает 1 января {year} года")
        known = sum_rates_between(key_rate, first_day, available)
        implied = (wave.median * days_in_year - known) / days_remaining
    if not (-100.0 < implied < 100.0):
        return (None, f"r_implied={implied:.2f} % вне (−100, 100) %")
    return (
        ConsensusPath(
            meeting_date=meeting,
            wave_date=wave.wave_date,
            available_from=available,
            year=year,
            annual_median=wave.median,
            rate_before=float(rate_before),
            implied_level=implied,
            step_bp=(implied - rate_before) * 100.0,
            days_remaining=days_remaining,
        ),
        None,
    )


def _consensus_skip_reason(reason: Optional[str]) -> str:
    """Короткая стабильная метка причины пропуска B4 (спека §5.6).

    Guards здесь НЕ повторяются: подробное сообщение уже названо
    `_consensus_path_with_reason`, функция только переводит его в категорию
    отчёта (даты и годы в метку не входят — диапазон печатает рендерер).
    `None` — путь не определён без названной причины; такая строка не
    теряется и попадает в «причина не определена».
    """
    if reason is None:
        return CONSENSUS_SKIP_UNKNOWN
    if reason.startswith("нет волны на "):
        return CONSENSUS_SKIP_NO_WAVE
    if reason.startswith("ряд ставки не покрывает дату доступности волны"):
        return CONSENSUS_SKIP_RATE_AT_WAVE
    if reason.startswith("ряд ставки не покрывает 1 января"):
        return CONSENSUS_SKIP_RATE_YEAR_START
    if reason.startswith("на остаток "):
        return CONSENSUS_SKIP_SHORT_REMAINDER
    if reason.startswith("r_implied="):
        return CONSENSUS_SKIP_IMPLIED_RANGE
    return CONSENSUS_SKIP_UNKNOWN


def consensus_path_skips(rows: list, consensus: list, key_rate: list) -> list:
    """Пропуски B4 по строкам выборки: `(meeting_date, короткая причина)`.

    Для каждого `Row`, у которого guards §3.2 не дали проекции, возвращается
    дата заседания и метка из `CONSENSUS_SKIP_REASONS` (спека §5.6).
    Причина берётся из `_consensus_path_with_reason` — того же расчёта, что у
    `consensus_path_predictions`, поэтому сумма `len(skips) + len(b4)` равна
    `len(rows)`: одна строка не может попасть и в фолды, и в пропуски.
    """
    skips = []
    for row in rows:
        decision = row.decision
        path, reason = _consensus_path_with_reason(
            consensus, key_rate, decision.meeting_date, decision.rate_before
        )
        if path is None:
            skips.append(
                (decision.meeting_date, _consensus_skip_reason(reason))
            )
    return skips


def consensus_path(
    consensus: list, key_rate: list, meeting: date, rate_before: float
) -> Optional[ConsensusPath]:
    """B4-проекция остатка года `Y = year(meeting)` на шаг заседания (§3.2).

    Волна — последняя с медианой на `Y`, доступная строго до заседания
    (`available_from < meeting`; при равенстве `available_from` порядок
    задаёт `wave_date`). Доступность `A = available_from`.

    Если `A < 1 января Y` (январское заседание, последняя волна — декабрьская),
    известного пути в году нет: `R_known = 0`, `N_rem = N_Y`, проекция равна
    медиане. Иначе `R_known` — сумма фактических ставок с 1 января по `A`
    включительно (fill-forward по календарным дням), а остаток года даёт
    `r_implied = (median·N_Y − R_known)/N_rem`.

    `None` — проекция не определена: нет подходящей волны; ряд ставки не
    покрывает `A` (а в формуле — ещё и 1 января); на остаток года меньше
    `CONSENSUS_MIN_DAYS_REMAINING` дней; `r_implied` вне `(−100, 100)`
    процента (деление на короткий хвост или порча данных). Это guard
    бенчмарка, а не ошибка: заседание просто выпадает из B4-выборки.
    Причины отсутствия называет `_consensus_path_with_reason`.
    """
    path, _ = _consensus_path_with_reason(consensus, key_rate, meeting, rate_before)
    return path


def consensus_path_predictions(rows: list, consensus: list, key_rate: list) -> list:
    """Фолды B4 по строкам выборки: только заседания с определённой проекцией.

    `rate_before` берётся из `Row` (ставка на `M−1`), а не из ряда на дату
    волны: шаг B4 — разница между проекцией остатка года и фактическим
    уровнем перед заседанием. Заседание, для которого `consensus_path`
    вернула `None`, пропускается, а не получает фолд с догадкой.

    B4 не обучается и не зависит от окна walk-forward: `probs=None`, окно —
    `CONSENSUS_WINDOW`. В `METHODS`/`FORECAST_METHODS` он не входит (§3.2):
    потребитель — отдельный раздел отчёта и отдельная таблица метрик.
    """
    predictions = []
    for row in rows:
        decision = row.decision
        path = consensus_path(
            consensus, key_rate, decision.meeting_date, decision.rate_before
        )
        if path is None:
            continue
        predictions.append(
            Prediction(
                meeting_date=decision.meeting_date,
                actual_step_bp=float(decision.step_bp),
                actual_y3=decision.y3,
                method="consensus",
                window=CONSENSUS_WINDOW,
                step_bp=path.step_bp,
                probs=None,
            )
        )
    return predictions


@dataclass(frozen=True)
class B4Metrics:
    """Метрики B4 по фолдам: RMSE/MAE в б.п., hit rate и QWK (§3.2)."""

    n: int
    rmse_bp: float
    mae_bp: float
    hit_rate: float
    qwk: float


def b4_metrics(predictions: list) -> Optional[B4Metrics]:
    """Метрики B4 по фолдам (спека §3.2), или `None` на пустом списке.

    Формулы — существующие функции §7: RMSE/MAE по ошибкам шага в б.п.,
    hit rate и QWK по направлениям. Пустой список — «B4 не определено ни на
    одном заседании», а не нулевые метрики: ноль RMSE читался бы как
    идеальная точность.
    """
    if not predictions:
        return None
    errors = _group_errors(predictions)
    predicted, actual = _group_steps(predictions)
    return B4Metrics(
        n=len(predictions),
        rmse_bp=rmse_bp(errors),
        mae_bp=mae_bp(errors),
        hit_rate=hit_rate(predicted, actual),
        qwk=qwk(predicted, actual),
    )


@dataclass(frozen=True)
class B4Comparison:
    """Сравнение B4 с одной строкой «метод × окно» на общих заседаниях (§3.2).

    `against` — имя метода, `window` — его окно walk-forward. `n` — число
    общих заседаний. `delta_rmse_bp = RMSE(B4) − RMSE(other)` на этих же
    датах: положительная значит, что B4 ХУЖЕ (его ошибки больше). DM (HLN,
    `h=1`) подаётся как `err_a = B4`, `err_b = other`, поэтому положительная
    статистика тоже значит «B4 хуже». `dm_stat`/`dm_p` — `None`, когда тест
    не определён (`n < 2` или нулевая дисперсия разницы потерь, см.
    `_dm_or_none`).
    """

    against: str
    window: str
    n: int
    delta_rmse_bp: float
    dm_stat: Optional[float]
    dm_p: Optional[float]


def b4_significance(b4: list, predictions: list) -> list:
    """DM B4 против каждой строки «метод × окно» на общих заседаниях (§3.2).

    Сравниваются только общие даты: у B4 guard пропускает заседания без
    волны/покрытия/запаса дней, у методов walk-forward — вырожденные окна,
    и DM по разным наборам смешал бы точность с составом выборки. Группа без
    общих дат пропускается (в отличие от `_significance_rows`, где пустое
    пересечение — ошибка: там строки методов посчитаны на одной выборке, а
    B4 живёт по своему guard).

    Порядок строк — как у `method_predictions`: окно (`expanding`, затем
    `rolling40`), внутри — `METHODS`; группы с окном `CONSENSUS_WINDOW` в
    перебор не входят.
    """
    b4_errors = {
        prediction.meeting_date: prediction.step_bp - prediction.actual_step_bp
        for prediction in b4
    }
    groups = _predictions_by_group(predictions)
    comparisons = []
    for window in (WINDOW_EXPANDING, window_label(ROLLING_WINDOW)):
        for method, _ in METHODS:
            group = groups.get((method, window))
            if group is None:
                continue
            other_errors = {
                prediction.meeting_date: prediction.step_bp
                - prediction.actual_step_bp
                for prediction in group
            }
            # Общие заседания — в календарном порядке, чтобы DM не зависел от
            # порядка строк ответа/выборки.
            shared = sorted(day for day in b4_errors if day in other_errors)
            if not shared:
                continue
            errors_a = [b4_errors[day] for day in shared]
            errors_b = [other_errors[day] for day in shared]
            dm_stat, dm_p = _dm_or_none(errors_a, errors_b)
            comparisons.append(
                B4Comparison(
                    against=method,
                    window=window,
                    n=len(shared),
                    delta_rmse_bp=rmse_bp(errors_a) - rmse_bp(errors_b),
                    dm_stat=dm_stat,
                    dm_p=dm_p,
                )
            )
    return comparisons


# --- Текущий консенсус и сценарий удержания (спека §3.3/§4) -------------------


@dataclass(frozen=True)
class CurrentConsensus:
    """Последняя доступная волна консенсуса (спека §4).

    `rows` — все годы одной волны опроса (медианы на разные годы публикуются
    вместе), отсортированные по году. `stale_days` — сколько дней прошло от
    ДАТЫ ВОЛНЫ `wave_date` до даты прогона (спека §4: «волна старше»; дата
    доступности `available_from` сдвинута на консервативный лаг и для
    возраста волны не используется): волна не подменяется, а устаревание
    показывается числом.
    """

    wave_date: date
    available_from: date
    rows: list
    stale_days: int


def current_consensus(consensus: list, today: date) -> Optional[CurrentConsensus]:
    """Последняя волна с `available_from <= today` (спека §4), или None.

    Граница включительна: волна, ставшая доступной ровно в дату прогона, — это
    текущий консенсус. Выбор не полагается на порядок входа: берётся `max` по
    `(available_from, wave_date)`. Ряды результата — ВСЕ годы выбранной волны
    (`wave_date` совпадает), сортировка по году: так таблица отчёта читается
    сверху вниз по горизонту.
    """
    eligible = [wave for wave in consensus if wave.available_from <= today]
    if not eligible:
        return None
    latest = max(eligible, key=lambda wave: (wave.available_from, wave.wave_date))
    rows = sorted(
        (wave for wave in consensus if wave.wave_date == latest.wave_date),
        key=lambda wave: wave.year,
    )
    return CurrentConsensus(
        wave_date=latest.wave_date,
        available_from=latest.available_from,
        rows=rows,
        stale_days=(today - latest.wave_date).days,
    )


def hold_scenario_average(
    key_rate: list,
    today: date,
    meeting: date,
    rate_before: Optional[float],
    model_level: Optional[float],
) -> Optional[float]:
    """Вменённая среднегодовая при удержании уровня после ближайшего заседания.

    Формула спеки §3.3:

    ```
    (Σ_{1 янв..today} rate(d) + rate_before·дни(today+1..M−1)
     + model_level·дни(M..31 дек)) / N_Y
    ```

    Известный путь года суммируется фактически (`sum_rates_between`), интервал
    до заседания `M` — ставкой `rate_before`, остаток `[M, 31 декабря]` —
    уровнем `model_level` (формальный прогноз `ordered_logit`). Это сценарий
    удержания, а не траектория: между заседаниями уровень не меняется.

    `None` — сценарий не определён: `rate_before`/`model_level` не конечны,
    `M` не в году `today` или не позже неё (формула смешала бы годы/дала
    отрицательное число дней), либо ряд ставки не покрывает `today` или
    1 января года — сумму не из чего собрать. Отчёт называет причину, а не
    падает: отсутствие `ordered_logit` — штатная ветка.
    """
    if rate_before is None or model_level is None:
        return None
    if not math.isfinite(rate_before) or not math.isfinite(model_level):
        return None
    year = today.year
    first_day = date(year, 1, 1)
    last_day = date(year, 12, 31)
    if not first_day <= meeting <= last_day or meeting <= today:
        return None
    if _rate_asof(key_rate, today) is None or _rate_asof(key_rate, first_day) is None:
        return None
    days = (last_day - first_day).days + 1
    known = sum_rates_between(key_rate, first_day, today)
    before = (meeting - today).days - 1  # дни today+1 … M−1
    after = (last_day - meeting).days + 1  # дни M … 31 декабря
    return (known + rate_before * before + model_level * after) / days


# --- Загрузчики: SQL + транспорт + разбор ------------------------------------


def load_meetings() -> list:
    """Даты канонических заседаний за весь доступный ряд (с 2014-02-14)."""
    return meetings_from_rows(ch_query(MEETINGS_SQL))


def load_key_rate() -> list:
    """Дневной ряд действующих ставок `cbr_key_rate` (2013-09-17+)."""
    return key_rate_from_rows(ch_query(KEY_RATE_SQL))


def load_cpd_core() -> list:
    """Базовый ИПЦ SA, м/м, % (серия `CPD_CORE_SERIES`)."""
    return cpd_from_rows(ch_query(CPD_CORE_SQL))


def load_ibk() -> list:
    """ИБК ЦБ SA, пункты — прокси разрыва выпуска (`macro_series`)."""
    return ibk_from_rows(ch_query(IBK_SQL))


def load_mp_summary_deltas() -> list:
    """Шаги решений из резюме ЦБ (`cbr_mp_summary`, 22 резюме с 2024-02)."""
    return mp_deltas_from_rows(ch_query(MP_SUMMARY_SQL))


def load_consensus() -> list:
    """Волны консенсуса по годам из витрины макроопроса ЦБ (`v_cbr_mo_br`)."""
    return consensus_from_rows(ch_query(CONSENSUS_SQL))


# --- As-of слой (спека §4) ----------------------------------------------------


def _end_of_next_month(period: date) -> date:
    """Конец месяца, следующего за месяцем даты `period`.

    Общее правило доступности обоих месячных рядов v0: точка отчётного месяца M
    становится доступной в конце M+1 (фактическая публикация — во второй декаде
    M+1; правило консервативно и намеренно НЕ point-in-time, спека §4).
    """
    year, month = period.year, period.month + 1
    if month > 12:
        year, month = year + 1, 1
    if month == 12:
        return date(year, 12, 31)
    return date(year, month + 1, 1) - ONE_DAY


def available_from_cpd(period_end: date) -> date:
    """Доступность точки базового ИПЦ: конец месяца M+1 (M — отчётный)."""
    return _end_of_next_month(period_end)


def available_from_survey(period_first: date) -> date:
    """Доступность точки ИБК: конец месяца M+1 (M — отчётный)."""
    return _end_of_next_month(period_first)


def _available_on_the_day(period: date) -> date:
    """Доступность ставки: в день действия лага нет."""
    return period


def asof(points: list, available: Callable[[date], date], asof_date: date) -> Optional[float]:
    """Последняя доступная на `asof_date` точка ряда, или None.

    Выбирается точка с НАИБОЛЬШЕЙ датой периода среди тех, у которых
    `available(period) ≤ asof_date` (граница включительна: точка, ставшая
    доступной ровно в дату заседания, участвует). Порядок входа не важен.
    Пустой результат — это `None`, а вызывающий решает, что это значит
    (для признаков — заседание в «пропущенных», спека §4).
    """
    candidates = [point for point in points if available(point[0]) <= asof_date]
    if not candidates:
        return None
    return max(candidates, key=lambda point: point[0])[1]


def annualize_saar(monthly_percent: float) -> float:
    """Годовой темп из месячного SA-темпа: `((1 + x/100)^12 − 1) × 100`.

    Сложное начисление, а не `×12`: месяц к месяцу — темп прироста, и годовой
    из него получается произведением двенадцати множителей (спека §4).
    """
    return ((1.0 + monthly_percent / 100.0) ** 12 - 1.0) * 100.0


# --- Выборка решений и строки признаков ---------------------------------------


@dataclass(frozen=True)
class Decision:
    """Решение заседания: уровни до/после, шаг в б.п. и класс `y3`.

    `y3` ∈ {−1 снижение, 0 сохранение, +1 повышение} — целевая переменная
    классификационных методов (спека §3/§6).
    """

    meeting_date: date
    rate_before: float
    rate_after: float
    step_bp: int
    y3: int


@dataclass(frozen=True)
class Row:
    """Строка выборки: решение и два признака, снятые as-of (спека §4)."""

    decision: Decision
    pi_saar: float
    gap: float

    @property
    def x(self) -> list:
        """Вектор регрессоров `x = [rate_before, pi_saar, gap]` (спека §4)."""
        return [self.decision.rate_before, self.pi_saar, self.gap]


def build_decisions(meetings: list, key_rate: list, mp_deltas: list) -> tuple:
    """Собирает решения по заседаниям из ряда действующих ставок (спека §3).

    Правила исхода для даты `d`:

    * `rate_before` — ставка, действующая на `d − 1` (решение публикуется в
      13:30 и вступает в силу со следующего дня; у внеочередного 2014-03-03
      ставка действует с того же дня, у внеочередного 2014-12-15 пресс-релиз
      опубликован в ночь на 16-е — `d − 1` защищает оба случая);
    * `rate_after` — ставка на `min(d + 7, next − 1)`, где `next` — следующее
      заседание СПИСКА (защита от двух решений в одном окне: 2014-12-11/15,
      2014-12-15/2015-01-30 и т.п.);
    * сверка ярлыков: для заседаний, присутствующих в `cbr_mp_summary` с
      непустым `decision_delta_bp`, шаг обязан совпасть — расхождение
      поднимает `SampleError` (fail-fast).

    Ряд ставки здесь — ЧАСЫ ВЫБОРКИ. У заседания ТРИ исхода, и они различимы:

    * решение — `rate_before` известен и окно исхода наблюдаемо целиком
      (`window_end ≤ max(date)` ряда);
    * «вне выборки» (`excluded`) — окно исхода ещё не наблюдаемо: заседание
      прошло (или назначено), но `min(d+7, next−1)` выходит за данные ряда, и
      «ставка ещё не изменилась» — не наблюдённый исход. Так же выглядит
      будущее заседание, если вызывающий передал его в списке;
    * «пропущено» (`missing`) — на `d − 1` нет точки ряда вовсе (ряд короче
      выборки или начинается позже заседания): ставку «до» взять неоткуда, и
      это дефект данных, а не незрелое окно.

    Обе причины названы разными списками намеренно: спека §3/§4 требует
    видимого счётчика «пропущено», а слитый список из двух разных причин его
    прятал. Кто именно попадает в `meetings` — решение вызывающего: живой
    прогон подаёт прошедшие заседания (решение ЦБ уже объявлено) и оставляет
    будущие в календаре.

    Возврат: `(decisions, excluded, missing)`.

    Формат входов: `meetings` — даты, `key_rate` — пары `(дата, ставка)`,
    `mp_deltas` — пары `(дата заседания, шаг в б.п. или None)`, как их отдают
    `load_key_rate` и `load_mp_summary_deltas`.
    """
    ordered = sorted(set(meetings))
    deltas = {day: delta for day, delta in mp_deltas}
    rate_points = sorted(key_rate)
    if not rate_points:
        # Пустой ряд ставки: решений нет, а каждое заседание — «пропущено»
        # (ставки «на d−1» нет ни у кого).
        return [], [], ordered
    series_max = rate_points[-1][0]

    decisions = []
    excluded = []
    missing = []
    for index, meeting in enumerate(ordered):
        rate_before = asof(rate_points, _available_on_the_day, meeting - ONE_DAY)
        if rate_before is None:
            missing.append(meeting)
            continue
        window_end = meeting + timedelta(days=OUTCOME_WINDOW_DAYS)
        if index + 1 < len(ordered):
            window_end = min(window_end, ordered[index + 1] - ONE_DAY)
        if window_end > series_max:
            excluded.append(meeting)
            continue
        rate_after = asof(rate_points, _available_on_the_day, window_end)
        step_bp = round((rate_after - rate_before) * 100.0)
        y3 = (step_bp > 0) - (step_bp < 0)

        delta = deltas.get(meeting)
        if delta is not None and int(delta) != step_bp:
            raise SampleError(
                f"заседание {meeting.isoformat()}: шаг по cbr_key_rate "
                f"{step_bp:+d} б.п. не совпал с decision_delta_bp "
                f"{int(delta):+d} б.п. из cbr_mp_summary — один из источников "
                f"неверен, прогон остановлен"
            )
        decisions.append(Decision(meeting, rate_before, rate_after, step_bp, y3))
    return decisions, excluded, missing


def build_rows(decisions: list, cpd: list, ibk: list) -> tuple:
    """Строки выборки: к каждому решению — признаки as-of (спека §4).

    `pi_saar` — годовой базовый ИПЦ из месячного SA-темпа; `gap` — ИБК SA.
    Оба снимаются на дату заседания (`asof`). Если хоть одного ряда нет —
    заседание исключается из выборки и попадает во второй список
    («пропущено»): подставлять ноль вместо неизвестного признака нельзя,
    такая строка выглядела бы измеренной.

    Возврат: `(rows, missing)`.
    """
    rows = []
    missing = []
    for decision in decisions:
        monthly = asof(cpd, available_from_cpd, decision.meeting_date)
        gap = asof(ibk, available_from_survey, decision.meeting_date)
        if monthly is None or gap is None:
            missing.append(decision.meeting_date)
            continue
        rows.append(Row(decision, annualize_saar(monthly), gap))
    return rows, missing


# --- Веса наблюдений: CSV и покрытие (спека §7) -------------------------------

# Колонки CSV весов. Лишние колонки (`emergency`, `explicit_shock`, признаки и
# т.п. из `shock_classifier_weights.csv`) игнорируются: модель читает только
# дату и вес, а формат файла принадлежит классификатору.
SHOCK_WEIGHTS_DATE_COLUMN = "meeting_date"
SHOCK_WEIGHTS_VALUE_COLUMN = "weight"


def load_shock_weights(path: Path) -> dict:
    """Веса заседаний из CSV `meeting_date,weight` (спека §7).

    Файл — артефакт `shock_classifier.py` (`shock_classifier_weights.csv`), но
    формат намеренно минимальный: модели нужны только дата и вес, остальные
    колонки игнорируются. Правила валидации — спека §7:

    * дата — ISO `YYYY-MM-DD` (`date.fromisoformat`);
    * дата уникальна: дубль сделал бы вес неоднозначным;
    * `0 < w ≤ 1`: вес мягко ПОНИЖАЕТ вклад наблюдения, а не исключает его
      (ноль был бы выбрасыванием строки, вес > 1 — усилением шока).

    Любое нарушение — `ValueError` с номером строки и значением: файл весов
    задаёт вход модели, и молчаливая правка здесь исказила бы бэктест.
    Отсутствие даты в файле ошибкой НЕ является — `weights_for_rows` даёт
    такому заседанию вес 1.0 и считает его в покрытии.
    """
    path = Path(path)
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        columns = reader.fieldnames or []
        missing_columns = [
            name
            for name in (SHOCK_WEIGHTS_DATE_COLUMN, SHOCK_WEIGHTS_VALUE_COLUMN)
            if name not in columns
        ]
        if missing_columns:
            raise ValueError(
                f"{path}: в CSV нет обязательных колонок {', '.join(missing_columns)} "
                f"(ожидаются {SHOCK_WEIGHTS_DATE_COLUMN},{SHOCK_WEIGHTS_VALUE_COLUMN}; "
                f"лишние колонки игнорируются)"
            )
        weights = {}
        for line_number, row in enumerate(reader, start=2):
            raw_date = str(row.get(SHOCK_WEIGHTS_DATE_COLUMN) or "").strip()
            raw_weight = str(row.get(SHOCK_WEIGHTS_VALUE_COLUMN) or "").strip()
            if not raw_date:
                raise ValueError(
                    f"{path}: строка {line_number}: пустая дата заседания "
                    f"({SHOCK_WEIGHTS_DATE_COLUMN})"
                )
            try:
                meeting = date.fromisoformat(raw_date)
            except ValueError as error:
                raise ValueError(
                    f"{path}: строка {line_number}: дата {raw_date!r} не ISO "
                    f"YYYY-MM-DD"
                ) from error
            if meeting in weights:
                raise ValueError(
                    f"{path}: дата {meeting.isoformat()} встречается дважды — "
                    f"вес заседания неоднозначен"
                )
            try:
                weight = float(raw_weight)
            except ValueError as error:
                raise ValueError(
                    f"{path}: строка {line_number}: вес {raw_weight!r} не число"
                ) from error
            if not math.isfinite(weight) or not 0.0 < weight <= 1.0:
                raise ValueError(
                    f"{path}: строка {line_number}: вес {weight!r} вне (0, 1] — "
                    f"наблюдение можно мягко понизить, но не исключить"
                )
            weights[meeting] = weight
    if not weights:
        raise ValueError(f"{path}: нет ни одной строки с весами")
    return weights


# Сколько дат пропущенных весов печатать в предупреждении. Полный список на
# живом прогоне — сотня дат в каждом из четырёх walk-forward-вызовов: счётчик
# остаётся в сообщении, а хвост списка не нужен.
WEIGHT_WARNING_DATES = 10


def _missing_weight_note(missing: list) -> str:
    """Первые `WEIGHT_WARNING_DATES` отсутствующих дат плюс остаток счётчиком."""
    shown = ", ".join(day.isoformat() for day in missing[:WEIGHT_WARNING_DATES])
    if len(missing) > WEIGHT_WARNING_DATES:
        shown += f", … (ещё {len(missing) - WEIGHT_WARNING_DATES})"
    return shown


def weights_for_rows(
    rows: list, weights: Optional[dict], warn: bool = True
) -> tuple:
    """Веса строк выборки по дате заседания; отсутствующая дата — 1.0.

    Возврат `(значения, отсутствующие даты)`: значения выровнены по `rows` и
    пригодны для WLS/`freq_weights`, список отсутствующих — счётчик покрытия
    для отчёта. Дата, которой нет в CSV, НЕ ошибка: ранние заседания и
    заседания вне среза классификатора не измерены, и вес 1.0 — нейтральное
    значение («не понижаем»), а не выдуманная аномальность.

    Предупреждение (один раз на вызов) называет число и даты: молчаливое
    заполнение скрыло бы, что часть весов не пришла. `warn=False` — для
    walk-forward, где один и тот же пропуск повторяется в каждом фолде: там
    предупреждение агрегируется вызывающим.
    """
    if weights is None:
        return [1.0] * len(rows), []
    values = []
    missing = []
    for row in rows:
        meeting = row.decision.meeting_date
        if meeting in weights:
            values.append(float(weights[meeting]))
        else:
            values.append(1.0)
            missing.append(meeting)
    if missing and warn:
        log.warning(
            "веса шоков: %d заседаний без веса — принят вес 1.0: %s",
            len(missing),
            _missing_weight_note(missing),
        )
    return values, missing


# --- Walk-forward и бенчмарки B1–B3 (спека §5, §7) ----------------------------


class DegenerateWindowError(ValueError):
    """Обучающее окно непригодно для оценки метода (спека §6).

    Поднимается методом walk-forward-шага, когда на прошлом нельзя оценить
    ничего осмысленного: точек меньше, чем нужно регрессии, у ordered-модели в
    окне один класс и т.п. `walk_forward` пропускает такой фолд и считает его,
    а прогон не падает: вырожденность — свойство окна, а не ошибка данных.

    Подкласс `ValueError` — чтобы вызывающий, ждущий `ValueError`, не ломался;
    но walk-forward пропускает ИМЕННО этот тип: прочие `ValueError` — ошибка
    прогона (баг кода или порча данных) и останавливают его.
    """


@dataclass(frozen=True)
class Forecast:
    """Прогноз одного фолда: шаг в БАЗИСНЫХ пунктах и вероятности классов.

    Единицы — б.п., как у факта (`Decision.step_bp`): у бенчмарка и метода
    одна шкала, иначе метрики, DM и бутстрап сравнивали бы разные величины.

    `probs` — вероятности классов в порядке (−1, 0, +1); `None` — метод
    распределения не даёт (вырожденные бенчмарки B1–B3, спека §5). Форма одна
    для бенчмарков и методов №2–3 (Tasks 4–5): walk-forward и отчёт не знают,
    вероятностный метод или нет, — это свойство самого прогноза.
    """

    step_bp: float
    probs: Optional[tuple]


# Контракт метода walk-forward: обучение на окне строк, прогноз следующей.
# Взвешиваемые методы (WEIGHTED_METHODS) получают третьим аргументом вектор
# весов наблюдений; остальные вызываются с двумя, как раньше.
FitPredict = Callable[..., Forecast]


@dataclass(frozen=True)
class Prediction:
    """Фолд walk-forward: факт заседания, метод, окно и прогноз.

    `actual_step_bp`/`actual_y3` — факт заседания; он хранится рядом с
    прогнозом, чтобы метрики, тесты значимости и CSV не восстанавливали его
    по выборке повторно (и не разошлись в трактовке, если что-то сдвинется).
    """

    meeting_date: date
    actual_step_bp: float
    actual_y3: int
    method: str
    window: str
    step_bp: float
    probs: Optional[tuple]


def window_label(rolling: Optional[int]) -> str:
    """Метка окна walk-forward для отчёта и CSV: `expanding` или `rolling<N>`."""
    return WINDOW_EXPANDING if rolling is None else f"rolling{rolling}"


def predict_hold(train: list, row: Row) -> Forecast:
    """B1: «ставка не изменится» — нулевой шаг (спека §5)."""
    return Forecast(0.0, None)


def predict_repeat(train: list, row: Row) -> Forecast:
    """B2: «повтор последнего решения» — шаг предыдущего заседания окна (§5)."""
    if not train:
        raise ValueError("B2 («повтор решения»): в обучающем окне нет предыдущего решения")
    return Forecast(float(train[-1].decision.step_bp), None)


def _hac_fit(y: list, design) -> RegressionResultsWrapper:
    """OLS на ГОТОВОМ дизайне с ковариацией HAC (одна конфигурация, спека §6).

    Конфигурация одна на срез (`HAC_MAXLAGS`, `use_correction=True`) и пинована
    тестом: AR(1), Mincer–Zarnowitz и методы №2–3 обязаны считать p-value на
    одной ковариации, иначе строки отчёта несопоставимы. Дизайн подаётся
    вызывающим — так проверка ранга у метода №2 идёт по тем же столбцам, что и
    подгонка, и «дизайн без константы» невозможен по построению.

    Возврат — результаты statsmodels (`params`, `ssr`, `f_test`): вызывающий
    берёт из них коэффициенты и совместные гипотезы.
    """
    model = sm.OLS(np.asarray(y, dtype=float), np.asarray(design, dtype=float))
    return model.fit(cov_type="HAC", cov_kwds={"maxlags": HAC_MAXLAGS, "use_correction": True})


def _ols_hac(y: list, x: list) -> RegressionResultsWrapper:
    """OLS `y = a + b·x` с ковариацией HAC (Ньюи–Уэст, спека §6).

    Константа добавляется ВСЕГДА (`has_constant="add"`): умолчание statsmodels
    (`has_constant="skip"`) пропускает её, если в `x` есть постоянный столбец, —
    тогда регрессия идёт на столбце меньше, и вызывающий, ждущий `k + 1`
    параметров, разваливается. Вырожденность регрессора ловят вызывающие, а не
    молчаливый пропуск константы.
    """
    design = sm.add_constant(np.asarray(x, dtype=float), has_constant="add")
    return _hac_fit(y, design)


def fit_ar1(train: list, row: Row) -> Forecast:
    """B3: AR(1) на уровне ставки `i_t = c + φ·i_{t−1}`, OLS с HAC (спека §5).

    В обучении берутся пары «(уровень до, уровень после)» = `(i_{t−1}, i_t)`:
    между соседними заседаниями ставка не меняется, поэтому `rate_before`
    тестового заседания — это `i_{t−1}`. Уровни — в процентных пунктах, а
    прогноз шага обязан быть в БАЗИСНЫХ, как у факта и остальных прогнозов
    harness'а: `(pred_level − rate_before) × 100`.
    """
    if len(train) < MIN_OLS_OBS:
        raise DegenerateWindowError(
            f"B3 (AR(1)): обучающее окно короче {MIN_OLS_OBS} заседаний — "
            f"OLS с HAC не оценивается"
        )
    levels_prev = [sample.decision.rate_before for sample in train]
    levels_next = [sample.decision.rate_after for sample in train]
    if len(set(levels_prev)) < 2:
        # Уровень в окне не менялся: наклон φ не идентифицирован (регрессор
        # коллинеарен свободному члену), и MLE вернул бы мусор или упал.
        raise DegenerateWindowError(
            "B3 (AR(1)): уровень ставки в обучающем окне не меняется — φ не идентифицирован"
        )
    result = _ols_hac(levels_next, levels_prev)
    pred_level = float(result.params[0]) + float(result.params[1]) * row.decision.rate_before
    step_percent = pred_level - row.decision.rate_before
    return Forecast(step_percent * 100.0, None)


def _as_weights(weights, expected: int, what: str) -> np.ndarray:
    """Валидирует вектор весов наблюдений обучающего окна.

    Веса приходят из `weights_for_rows`/`load_shock_weights` (там `0 < w ≤ 1`),
    но `_taylor_fit`/`_ordered_fit` — публичные точки входа, и молча
    несогласованный вектор (не та длина, NaN, ноль) дал бы либо исключение
    statsmodels в середине фолда, либо выброшенное наблюдение. Поэтому длина,
    конечность и положительность проверяются ЗДЕСЬ; `ValueError` — ошибка
    вызывающего, а не вырожденное окно.
    """
    array = np.asarray(weights, dtype=float)
    if array.ndim != 1 or array.shape[0] != expected:
        size = array.shape[0] if array.ndim == 1 else str(array.shape)
        raise ValueError(
            f"{what}: весов {size} на {expected} наблюдений обучающего окна"
        )
    if not np.all(np.isfinite(array)) or np.any(array <= 0.0):
        raise ValueError(f"{what}: веса должны быть конечными и положительными")
    return array


def _taylor_fit(train: list, weights=None) -> RegressionResultsWrapper:
    """Оценка правила Тейлора на обучающем окне: OLS/WLS с HAC (спека §6).

    Дизайн — `[1, rate_before, pi_saar, gap]`: константа добавляется явно
    (`has_constant="add"`), и проверка ранга идёт по ТЕМ ЖЕ столбцам, что и
    подгонка. Молчаливый пропуск константы (умолчание statsmodels) — не
    теория: при постоянном `pi_saar` регрессия шла бы на трёх столбцах,
    свободного члена в ней не было бы вовсе, а `fit_taylor` распаковывал бы
    четыре параметра и падал `ValueError` в середине фолда. Вырожденность
    обязана быть видна ЗДЕСЬ и поднимать `DegenerateWindowError`, который
    walk-forward пропускает со счётчиком.

    Отклик — уровень после решения `i_t`; ковариация — HAC с конфигурацией
    `_hac_fit` (та же, что у B3 и Mincer–Zarnowitz): p-value строк отчёта
    обязаны считаться на одной ковариации.

    `weights` — опциональные веса наблюдений (спека §7). При них оценка —
    `sm.WLS` на том же дизайне с ТОЙ ЖЕ конфигурацией HAC; без них — прежний
    `OLS` (`_hac_fit`) байт-в-байт, поэтому официальный путь без
    `--shock-weights` не меняется. Единичные веса дают те же коэффициенты,
    что OLS (пиновано тестом).

    Вырожденное окно — `DegenerateWindowError` с причиной, а не исключение
    statsmodels: окно короче `MIN_TAYLOR_OBS` (у четырёхпараметрической
    регрессии не остаётся остаточных степеней свободы, и HAC-поправка делит на
    ноль) или вырожденный дизайн (ранг полного дизайна с константой меньше
    числа столбцов: регрессор коллинеарен свободному члену — например,
    `pi_saar` или `gap` в окне не меняется).

    Возврат — результаты statsmodels; коэффициенты берут из них `fit_taylor`
    (прогноз) и `taylor_long_run` (форма свободной оценки).
    """
    if len(train) < MIN_TAYLOR_OBS:
        raise DegenerateWindowError(
            f"Тейлор (метод №2): обучающее окно короче {MIN_TAYLOR_OBS} заседаний — "
            f"OLS на [1, rate_before, pi_saar, gap] не оценивается"
        )
    design = sm.add_constant(
        np.asarray([sample.x for sample in train], dtype=float), has_constant="add"
    )
    if np.linalg.matrix_rank(design) < design.shape[1]:
        raise DegenerateWindowError(
            "Тейлор (метод №2): вырожденный дизайн обучающего окна — регрессор "
            "коллинеарен свободному члену (в окне не меняется), коэффициенты "
            "не идентифицированы"
        )
    response = np.asarray(
        [sample.decision.rate_after for sample in train], dtype=float
    )
    if weights is None:
        return _hac_fit(response, design)
    array = _as_weights(weights, len(train), "Тейлор (метод №2)")
    model = sm.WLS(response, design, weights=array)
    return model.fit(
        cov_type="HAC", cov_kwds={"maxlags": HAC_MAXLAGS, "use_correction": True}
    )


def fit_taylor(train: list, row: Row, weights=None) -> Forecast:
    """Метод №2: инерционное правило Тейлора (спека §6).

    `i_t = c + ρ·i_{t−1} + a·π + b·gap` — OLS на `[1, rate_before, pi_saar,
    gap]` с ковариацией HAC (`_taylor_fit`). Прогноз уровня подставляется в ту
    же формулу, а шаг переводится в БАЗИСНЫЕ пункты, как факт и остальные
    прогнозы harness'а: `(pred_level − rate_before) × 100`. Без ×100 метрики,
    DM и бутстрап сравнивали бы метод в п.п. с фактом в б.п. (ошибка, уже
    пойманная у B3 в Task 3).

    Коэффициенты — свободная оценка (спека §6): ни ρ, ни a, ни b не
    ограничиваются, Тейлоровы значения не навязываются; форма долгосрочных
    импликаций — в `taylor_long_run` и отдельном блоке отчёта.

    `probs` — `None`: правило даёт точечный шаг, а не распределение классов
    (как бенчмарки B1–B3); вероятностные метрики — у метода №3 (Task 5).

    `weights` — веса обучающих наблюдений по датам заседаний (спека §7,
    `--shock-weights`); без них вызов и результат прежние.
    """
    result = _taylor_fit(train, weights)
    predicted_level = float(np.dot(result.params, [1.0] + list(row.x)))
    return Forecast((predicted_level - row.decision.rate_before) * 100.0, None)


@dataclass(frozen=True)
class TaylorLongRun:
    """Свободная форма долгосрочных импликаций правила Тейлора (спека §6).

    `rho`, `c`, `a_pi`, `b_gap` — оценённые коэффициенты короткого правила;
    `r_star`, `alpha`, `beta` — его устойчивое состояние (см.
    `taylor_long_run`); `n` — заседаний в оценке.
    """

    n: int
    rho: float
    c: float
    a_pi: float
    b_gap: float
    r_star: float
    alpha: float
    beta: float


def taylor_long_run(rows: list) -> TaylorLongRun:
    """Долгосрочные импликации правила Тейлора: форма свободной оценки (§6).

    Из `i_t = c + ρ·i_{t−1} + a·π + b·gap` в устойчивом состоянии
    (`i_t = i_{t−1} = i*`) получается правило в уровнях:

        `i* = c/(1−ρ) + a/(1−ρ)·π + b/(1−ρ)·gap = r* + α·π + β·gap`,

    то есть `r* = c/(1−ρ)` — долгосрочный уровень при нулевых π и gap, а
    `α = a/(1−ρ)` и `β = b/(1−ρ)` — неявные долгосрочные отклики на инфляцию
    и разрыв. Экономическая интерпретация здесь не навязывается: величины —
    СЛЕДСТВИЕ свободной оценки (Тейлоровы значения, например α = 1.5, не
    ограничиваются), поэтому отчёт приводит их как есть.

    Оценка — на переданных строках; отчёт подаёт полную выборку: блок
    описывает ФОРМУ правила, а не фолды walk-forward. Вырожденное окно
    (`_taylor_fit`) и ρ = 1 (устойчивого состояния нет: `1 − ρ = 0`) —
    `DegenerateWindowError` с причиной: отчёт печатает её строкой «не
    оценены», а не падает.
    """
    result = _taylor_fit(rows)
    c, rho, a_pi, b_gap = (float(parameter) for parameter in result.params)
    if abs(1.0 - rho) <= TAYLOR_STEADY_EPS:
        raise DegenerateWindowError(
            f"Тейлор (метод №2): ρ = {rho!r} — устойчивого состояния нет "
            f"(ρ = 1), долгосрочные импликации не определены"
        )
    multiplier = 1.0 / (1.0 - rho)
    return TaylorLongRun(
        n=len(rows),
        rho=rho,
        c=c,
        a_pi=a_pi,
        b_gap=b_gap,
        r_star=c * multiplier,
        alpha=a_pi * multiplier,
        beta=b_gap * multiplier,
    )


# --- Метод №3: упорядоченный логит и базлайн `climatology` (спека §6/§7) ------


# Порядок классов решения: {−1 снижение, 0 сохранение, +1 повышение}. Он же
# порядок вероятностей в `Forecast.probs` (3-кортеж метода №3), строк и столбцов
# матрицы 3×3 и разметки вероятностных метрик §7.
_CLASS_ORDER = (-1, 0, 1)


def _class_index(outcome, what: str) -> int:
    """Индекс класса в `_CLASS_ORDER`; класс вне {−1, 0, +1} — ошибка."""
    if outcome not in _CLASS_ORDER:
        raise ValueError(f"{what}: класс {outcome!r} вне {{−1, 0, +1}}")
    return _CLASS_ORDER.index(outcome)


def _require_two_classes(train: list, what: str) -> list:
    """Классы решения в обучающем окне; меньше двух — окно вырождено (спека §6).

    Пороги ordered-модели и частоты базлайна описывают РАЗЛИЧИЕ классов: на окне
    из одного класса оценивать нечего (у модели не остаётся второго уровня, у
    частот — нулевая энтропия). Это свойство окна, а не ошибка данных:
    walk-forward пропускает фолд со счётчиком.
    """
    classes = sorted({sample.decision.y3 for sample in train})
    if len(classes) < 2:
        raise DegenerateWindowError(
            f"{what}: классов решения в обучающем окне меньше двух "
            f"({classes or 'окно пусто'}) — распределение классов не оценивается"
        )
    return classes


def _class_counts(train: list) -> list:
    """Числа заседаний каждого класса решения в окне (порядок `_CLASS_ORDER`)."""
    counts = [0] * len(_CLASS_ORDER)
    for sample in train:
        counts[_class_index(sample.decision.y3, "частоты классов")] += 1
    return counts


def _class_means(train: list) -> tuple:
    """μ_c — средний фактический шаг класса c на обучающем окне, БАЗИСНЫЕ пункты.

    По построению класса (`Decision.y3 = sign(step_bp)`) μ_0 = 0: сохранение —
    это ровно нулевой шаг. Класс, которого в окне нет, получает μ = 0: его
    вероятность у обоих методов нулевая (частота нулевая, а ordered-модель
    различает только уровни окна), поэтому слагаемое P(c)·μ_c всё равно нулевое.

    Считается ТОЛЬКО по обучающему окну: средний шаг тестового заседания был бы
    утечкой (спека §7 — переобучение только на прошлом).
    """
    totals = [0.0] * len(_CLASS_ORDER)
    counts = [0] * len(_CLASS_ORDER)
    for sample in train:
        index = _class_index(sample.decision.y3, "средний шаг класса")
        totals[index] += float(sample.decision.step_bp)
        counts[index] += 1
    return tuple(total / count if count else 0.0 for total, count in zip(totals, counts))


def _expected_step(probs: tuple, means: tuple) -> float:
    """E[step] = Σ_c P(c)·μ_c — точечный шаг вероятностного метода, б.п.

    Единицы — БАЗИСНЫЕ пункты, как у факта и остальных прогнозов harness'а:
    μ_c — средний шаг класса в б.п. (`_class_means`), значит и E[step] в б.п.
    Перевод уровня в п.п. был бы ошибкой масштаба, уже пойманной у B3 в Task 3:
    метрики, DM и бутстрап сравнивали бы метод с фактом в разных шкалах.
    """
    if len(probs) != len(_CLASS_ORDER) or len(means) != len(_CLASS_ORDER):
        raise ValueError(
            f"E[step]: {len(probs)} вероятностей и {len(means)} средних — "
            f"ожидается по {len(_CLASS_ORDER)} на класс решения {_CLASS_ORDER}"
        )
    return sum(probability * mean for probability, mean in zip(probs, means))


class WeightedOrderedModel(OrderedModel):
    """Упорядоченный логит с весами наблюдений (`freq_weights`, спека §7).

    Наследник `OrderedModel`, у которого вклад наблюдения в логарифм
    правдоподобия умножается на вес: `loglike(θ) = Σ_i w_i · log p(y_i | x_i)`.
    Это ровно взвешенная MLE: вес 1 — обычная модель, вес < 1 мягко понижает
    вклад заседания-шока, не выбрасывая его из выборки.

    Почему подкласс, а не параметр `fit`: `OrderedModel` не знает `freq_weights`
    (в statsmodels 0.15 его нет ни у модели, ни у `GenericLikelihoodModel`).
    Численные `score`/`hessian` базового класса дифференцируют `self.loglike`,
    а `GenericLikelihoodModel.loglike` суммирует `loglikeobs` — поэтому
    переопределения одного `loglikeobs` достаточно, чтобы оптимизатор
    `method="newton"` видел взвешенную цель, а не только взвешенное значение.

    `freq_weights=None` — поведение базовой модели один-в-один; вектор весов
    выровнен по `endog` и проверяется по длине сразу: несогласованный вектор
    иначе дал бы broadcast-ошибку или неверное произведение в недрах
    оптимизатора.
    """

    def __init__(self, endog, exog, freq_weights=None, **kwds):
        self.freq_weights = (
            None if freq_weights is None else np.asarray(freq_weights, dtype=float)
        )
        if self.freq_weights is not None:
            expected = np.asarray(endog).shape[0]
            if self.freq_weights.ndim != 1 or self.freq_weights.shape[0] != expected:
                size = (
                    self.freq_weights.shape[0]
                    if self.freq_weights.ndim == 1
                    else str(self.freq_weights.shape)
                )
                raise ValueError(
                    f"WeightedOrderedModel: весов {size} на {expected} наблюдений"
                )
        super().__init__(endog, exog, **kwds)

    def loglikeobs(self, params):
        """Логарифм правдоподобия наблюдений, умноженный на их веса."""
        loglike = super().loglikeobs(params)
        if self.freq_weights is None:
            return loglike
        return loglike * self.freq_weights


def _ordered_fit(train: list, weights=None):
    """Оценка ordered-модели на обучающем окне: (модель, результат) — спека §6.

    statsmodels `OrderedModel(distr='logit')` на регрессорах `[rate_before,
    pi_saar, gap]` — БЕЗ отдельной константы: её роль играют пороги (statsmodels
    и сам отвергает дизайн с константным столбцом). Уровни модели берутся из
    обучающего окна и отсортированы по возрастанию (`model.labels`), поэтому
    столбец `j` выдачи отвечает классу `_CLASS_ORDER[j]` — см. `fit_ordered_logit`.

    Вырожденные окна — `DegenerateWindowError` с причиной, а не исключение
    statsmodels и не молчаливый мусор:

    * классов решения меньше двух — пороги не оцениваются;
    * окно короче `MIN_ORDERED_OBS` — у MLE нет остаточных степеней свободы;
    * линейный индекс не идентифицирован: `rank([1, X]) < K + 1`. Это значит,
      что найдётся направление `v` с `Xv = c·1`: оно сдвигает все пороги на ту же
      константу и не меняет правдоподобие (`Xv = 0` — тот же случай при `c = 0`).
      Живой пример — постоянный регрессор в узком окне;
    * MLE не сошёлся (`mle_retvals['converged']` — False) либо оптимизатор
      предупредил о нездоровой оценке (неинвертируемый гессиан при разделимых
      классах, переполнение правдоподобия). Предупреждения ГЛУШАТСЯ здесь же:
      вывод прогона и тестов — только числа, а сигнал превращается в причину
      пропуска фолда.

    Подгонка — явно `method="newton"`: умолчание statsmodels — Nelder–Mead
    (`nm`), который на окнах с тонким классом объявляет несходимость там, где
    ньютон сходится к тому же оптимуму (лишние пропуски фолдов), а на
    разделимых классах, наоборот, выдаёт формально «сошедшуюся» точку с
    расходящимися коэффициентами. Ньютон в обоих случаях ведёт себя, как ждёт
    спека: обычные окна оцениваются, вырожденные (в том числе разделимые) —
    `DegenerateWindowError`.

    `weights` — опциональные веса наблюдений (спека §7). При них модель —
    `WeightedOrderedModel` с `freq_weights` (взвешенная MLE); без них — прежний
    `OrderedModel` байт-в-байт, поэтому официальный путь без `--shock-weights`
    не меняется. Все проверки вырожденности, `ORDERED_MAXITER`,
    `method="newton"` и превращение предупреждений в `DegenerateWindowError`
    общие для обеих веток.

    Возврат — модель и результат подгонки: вызывающему нужны и вероятности
    (`result.predict`), и `model.labels` (отображение столбцов на классы).
    """
    _require_two_classes(train, "метод №3 (ordered logit)")
    if len(train) < MIN_ORDERED_OBS:
        raise DegenerateWindowError(
            f"метод №3 (ordered logit): обучающее окно короче {MIN_ORDERED_OBS} "
            f"заседаний — MLE на [rate_before, pi_saar, gap] не оценивается"
        )
    exog = np.asarray([sample.x for sample in train], dtype=float)
    endog = np.asarray([sample.decision.y3 for sample in train], dtype=int)
    augmented = np.hstack([np.ones((len(train), 1)), exog])
    if np.linalg.matrix_rank(augmented) < exog.shape[1] + 1:
        raise DegenerateWindowError(
            "метод №3 (ordered logit): вырожденный дизайн обучающего окна — "
            "регрессор постоянен в окне или коллинеарен остальным: линейный "
            "индекс xβ не идентифицирован (β сдвигает пороги)"
        )
    if weights is None:
        model = OrderedModel(endog, exog, distr="logit")
    else:
        array = _as_weights(weights, len(train), "метод №3 (ordered logit)")
        model = WeightedOrderedModel(
            endog, exog, freq_weights=array, distr="logit"
        )
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = model.fit(method="newton", maxiter=ORDERED_MAXITER, disp=False)
    except np.linalg.LinAlgError as error:
        # Сингулярная информационная матрица (разделимые классы — обычный
        # случай узкого окна): обратить гессиан нечем, оценка не определена.
        # Это вырожденный фолд, а не ошибка данных и не падение прогона.
        raise DegenerateWindowError(
            f"метод №3 (ordered logit): сингулярная информационная матрица "
            f"({error}) — классы, вероятно, линейно разделимы, распределение "
            f"классов не оценено"
        ) from error
    if not result.mle_retvals.get("converged", False):
        raise DegenerateWindowError(
            f"метод №3 (ordered logit): MLE не сошёлся "
            f"(warnflag = {result.mle_retvals.get('warnflag')}) — распределение "
            f"классов не оценено"
        )
    if caught:
        raise DegenerateWindowError(
            "метод №3 (ordered logit): оптимизатор предупредил о нездоровой "
            "оценке — "
            + "; ".join(
                f"{warning.category.__name__}: {warning.message}" for warning in caught
            )
        )
    return model, result


def fit_ordered_logit(train: list, row: Row, weights=None) -> Forecast:
    """Метод №3: упорядоченная логит-модель решения — вероятности и точка (§6).

    Модель — `OrderedModel(distr='logit')` на регрессорах `[rate_before,
    pi_saar, gap]`; классы упорядочены {−1 снижение, 0 сохранение, +1 повышение},
    и выход — 3-кортеж вероятностей в этом же порядке (сумма 1).

    **Отображение выдачи statsmodels.** `OrderedModel` берёт уровни из обучающего
    окна: `model.labels` — отсортированные по возрастанию уникальные классы окна,
    и столбцы `predict()` идут в том же порядке. Наши классы — это и есть числа
    {−1, 0, +1}, поэтому отображение — `_class_index(label)`: столбец `j`
    отвечает `_CLASS_ORDER[j]`, перестановки столбцов нет. Это пиновано тестом
    («низкий gap → максимум в ячейке снижения, высокий → в ячейке повышения»).
    Окно без одного из трёх классов (например, без повышений) — НЕ вырожденное:
    у модели два уровня, а отсутствующий класс получает в 3-кортеже ровно 0.
    Вырождены окно из ОДНОГО класса и прочие случаи `_ordered_fit` — там
    `DegenerateWindowError`, и walk-forward пропускает фолд со счётчиком.

    **Точка.** Точечный шаг — `E[step] = Σ_c P(c)·μ_c` в БАЗИСНЫХ пунктах
    (`_expected_step`), где μ_c — средний фактический шаг класса c на ТОМ ЖЕ
    обучающем окне (`_class_means`); μ_0 = 0 по построению класса. Так
    вероятностный прогноз сводится к общей шкале RMSE/MAE и DM-таблицы (спека
    §6), и без утечек: средние берутся только с прошлого.

    Тройка проверяется `_require_distribution` ДО расчёта точки: не-конечная
    вероятность (NaN/inf) — `ValueError` на выходе метода, а не nan в `E[step]`,
    Brier и log loss отчёта (и не пропуск фолда: это дефект кода, а не свойство
    окна).

    `weights` — веса обучающих наблюдений по датам заседаний (спека §7,
    `--shock-weights`); без них вызов и результат прежние, с ними — взвешенная
    MLE (`WeightedOrderedModel`), а средние шагов классов остаются
    невзвешенными: это описание фактов окна, а не правдоподобие.
    """
    model, result = _ordered_fit(train, weights)
    row_probs = result.predict(exog=np.asarray([row.x], dtype=float))[0]
    probs = [0.0] * len(_CLASS_ORDER)
    for label, probability in zip(model.labels, row_probs):
        probs[_class_index(int(label), "метод №3 (ordered logit)")] = float(probability)
    probs = tuple(probs)
    _require_distribution(probs, "метод №3 (ordered logit)")
    return Forecast(_expected_step(probs, _class_means(train)), probs)


def fit_climatology(train: list, row: Row) -> Forecast:
    """Базлайн `climatology`: безусловные частоты классов обучающего окна (§7).

    Спека §7 называет базлайном вероятностных метрик метода №3 «безусловные
    частоты классов на обучающем окне»: постановка та же — распределение классов
    против факта, — но признаки не используются вовсе, и прогноз одинаков для
    всех заседаний теста, пока окно то же. Это не метод прогноза ставки, а
    отсчётная точка Brier и log loss: числа метода №3 обязаны быть не хуже «доля
    классов в прошлом».

    Точечный шаг — тем же правилом `E[step] = Σ_c P(c)·μ_c` со средними шагами
    обучающего окна: другого способа свести распределение к шагу у базлайна нет,
    а без него строка выпала бы из RMSE/MAE и DM-таблицы (§7). Единицы — б.п.

    **Меньше двух классов — вырожденное окно и здесь** (бриф Task 5 требовал
    правило явно для метода №3; распространено на базлайн осознанно). Частоты
    из одного класса дают вырожденное распределение (0/1/0), у которого нет
    второй точки опоры: энтропия нулевая, `E[step]` — просто средний шаг этого
    класса, а Brier и log loss такой строки несравнимы с методом №3 (он на том
    же окне и пропущен). Одна конвенция вырожденности держит счётчики пропусков
    двух строк сопоставимыми: они пропускают одни и те же фолды. Тройка
    проверяется `_require_distribution` — как у метода №3.
    """
    _require_two_classes(train, "climatology")
    counts = _class_counts(train)
    probs = tuple(count / len(train) for count in counts)
    _require_distribution(probs, "climatology")
    return Forecast(_expected_step(probs, _class_means(train)), probs)


# Бенчмарки B1–B3 в порядке отчёта (спека §5) — порядок задан спекой и от него
# зависят строки таблиц отчёта и CSV, а не алфавит.
BENCHMARKS = (
    ("hold", predict_hold),
    ("repeat", predict_repeat),
    ("ar1", fit_ar1),
)

# Строки отчёта «метод × окно»: сначала обязательные бенчмарки B1–B3 (спека
# §5), затем методы §6 и вероятностный базлайн §7 — правило Тейлора (метод №2),
# `climatology` (отсчётная точка Brier и log loss метода №3) и сам метод №3.
# walk_forward, таблицы отчёта, CSV, DM/MZ/PT и режимный блок знают только
# «метод × окно» и не различают бенчмарк и метод.
METHODS = BENCHMARKS + (
    ("taylor", fit_taylor),
    ("climatology", fit_climatology),
    ("ordered_logit", fit_ordered_logit),
)

# Имена бенчмарков без меток — для сравнений DM («против каждого бенчмарка»).
BENCHMARK_METHODS = tuple(method for method, _ in BENCHMARKS)

# Методы, которые умеют взвешивать наблюдения (спека §7). Бенчмарки B1–B3 и
# базлайн `climatology` — эталоны без весов: их прогноз не зависит от того,
# какие наблюдения модель «слушает тише», иначе сравнение потеряло бы опору.
WEIGHTED_METHODS = ("taylor", "ordered_logit")

# Метки методов в отчёте: бенчмарки названы номерами спеки (§5), имена
# методов №2–3 (Tasks 4–5) идут в отчёт как есть.
METHOD_LABELS = {"hold": "B1 hold", "repeat": "B2 repeat", "ar1": "B3 AR(1)"}


def method_label(method: str) -> str:
    """Метка метода для отчёта: у бенчмарков — номер спеки, у прочих — имя."""
    return METHOD_LABELS.get(method, method)


def walk_forward(
    rows: list,
    method: str,
    fit_predict: FitPredict,
    min_train: int = MIN_TRAIN,
    rolling: Optional[int] = None,
    weights: Optional[dict] = None,
) -> tuple:
    """Walk-forward по одному методу и одному окну (спека §7).

    Фолд `i` обучается ТОЛЬКО на прошлых заседаниях (`rows[:i]`) и прогнозирует
    `rows[i]`; первый фолд — `min_train`-е по счёту заседание. Единый
    train/test split спекой запрещён, и здесь он не выражается вовсе, а не
    отключается флагом.

    Скользящее окно входит в прогон ПОЛНЫМ: первый фолд — когда набрано
    `rolling` заседаний (`max(min_train, rolling)`), иначе «окно 40»
    начиналось бы с окна в `min_train` и окном 40 не было бы. Выборка короче
    обучения даёт `([], 0)`: отчёт этого шага обязан рендериться и без фолдов.

    Вырожденное обучающее окно (`DegenerateWindowError` от метода) — ПРОПУСК
    фолда со счётчиком, а не падение прогона: у ordered-модели в окне может
    оказаться один класс, у AR(1) — неизменный уровень, и это свойство окна, а
    не ошибка данных. Любое другое исключение метода пробрасывается: это баг
    кода или порча данных, и молча пропускать его нельзя.

    `weights` — веса наблюдений по дате заседания (спека §7). Взвешиваются
    ТОЛЬКО методы из `WEIGHTED_METHODS` (`taylor`, `ordered_logit`): каждому
    фолду строится вектор весов обучающего окна по `row.decision.meeting_date`,
    а отсутствующая дата получает 1.0. Бенчмарки B1–B3 и `climatology`
    вызываются с двумя аргументами, как раньше, — их прогноз от весов не
    зависит. `weights=None` — прежний путь байт-в-байт.

    Пропуски весов считаются здесь: одна и та же дата повторяется в обучающих
    окнах многих фолдов, поэтому предупреждение агрегируется по уникальным
    датам (иначе счётчик раздулся бы числом окон).

    Возврат — `(фолды, пропущено)`: фолды по возрастанию даты
    (`Prediction.window` несёт метку окна, по которой отчёт группирует строки
    «метод × окно»), и число пропущенных фолдов — оно попадает в сводку отчёта
    и не даёт прочитать «метод не пропустил ничего» там, где он пропустил.
    """
    start = min_train if rolling is None else max(min_train, rolling)
    window = window_label(rolling)
    weighted = weights is not None and method in WEIGHTED_METHODS
    missing_weights = set()
    predictions = []
    skipped = 0
    for index in range(start, len(rows)):
        train = rows[:index] if rolling is None else rows[index - rolling:index]
        try:
            if weighted:
                train_weights, missing = weights_for_rows(train, weights, warn=False)
                missing_weights.update(missing)
                forecast = fit_predict(train, rows[index], train_weights)
            else:
                forecast = fit_predict(train, rows[index])
        except DegenerateWindowError:
            skipped += 1
            continue
        decision = rows[index].decision
        predictions.append(
            Prediction(
                meeting_date=decision.meeting_date,
                actual_step_bp=float(decision.step_bp),
                actual_y3=decision.y3,
                method=method,
                window=window,
                step_bp=float(forecast.step_bp),
                probs=forecast.probs,
            )
        )
    if missing_weights:
        log.warning(
            "веса шоков (%s, %s): %d заседаний обучающих окон без веса — "
            "принят вес 1.0: %s",
            method,
            window,
            len(missing_weights),
            _missing_weight_note(sorted(missing_weights)),
        )
    return predictions, skipped


def method_predictions(
    rows: list, min_train: int = MIN_TRAIN, weights: Optional[dict] = None
) -> tuple:
    """Бенчмарки B1–B3 и методы §6 по обоим окнам — строки «метод × окно».

    Оба окна считаются всегда (спека §7): отчёт показывает их рядом, а выбор
    окна по ошибке не делается. Порядок обхода — окно, затем метод; от него
    зависит порядок строк в отчёте и в CSV, и он же задаёт группировку.
    Список строк — `METHODS` (бенчмарки §5, затем методы §6).

    `weights` — веса наблюдений для взвешиваемых методов (`WEIGHTED_METHODS`,
    спека §7); бенчмарки и `climatology` их не получают. `None` — прежний
    путь без весов байт-в-байт.

    Возврат — `(фолды, пропущено)`: счётчик вырожденных окон по ключу
    «(метод, окно)» для сводки отчёта. У B1 и B2 он нулевой по построению, у
    методов с оцениваемой регрессией (B3, №2–3) ненулевым бывает на выборке с
    неизменными регрессорами (и тогда это факт отчёта, а не повод уронить
    прогон).
    """
    predictions = []
    fold_skips = {}
    for window in (None, ROLLING_WINDOW):
        for method, fit_predict in METHODS:
            folds, skipped = walk_forward(
                rows,
                method,
                fit_predict,
                min_train=min_train,
                rolling=window,
                weights=weights,
            )
            predictions.extend(folds)
            fold_skips[(method, window_label(window))] = skipped
    return predictions, fold_skips


def weighted_method_predictions(rows: list, min_train: int = MIN_TRAIN) -> list:
    """Фолды ТОЛЬКО взвешиваемых методов БЕЗ весов — база сравнения (спека §7).

    Отчёт с `--shock-weights` показывает weighted vs unweighted RMSE/MAE и
    DM-HLN по `taylor`/`ordered_logit`. Невзвешенная база — тот же walk-forward
    на тех же окнах, но с прежним OLS/`OrderedModel`; бенчмарки в сравнение не
    входят: они и так не взвешиваются, и их числа уже есть в основной таблице.
    """
    predictions = []
    for window in (None, ROLLING_WINDOW):
        for method, fit_predict in METHODS:
            if method not in WEIGHTED_METHODS:
                continue
            folds, _ = walk_forward(
                rows, method, fit_predict, min_train=min_train, rolling=window
            )
            predictions.extend(folds)
    return predictions


# --- Метрики §7 ----------------------------------------------------------------


def _sign(step) -> int:
    """Класс направления шага: {−1, 0, +1} — та же конвенция, что у `y3`."""
    return (step > 0) - (step < 0)


def _pairs(predicted: list, actual: list, what: str) -> list:
    """Пары «прогноз, факт» с проверкой длин.

    Молчаливый `zip` по короткому списку исказил бы метрику: лишние фолды
    выпали бы из среднего, и число в отчёте было бы не про те данные. Поэтому
    расхождение длин и пустой список — ошибка прогона.
    """
    if len(predicted) != len(actual):
        raise ValueError(f"{what}: {len(predicted)} прогнозов против {len(actual)} фактов")
    if not predicted:
        raise ValueError(f"{what}: пустой список прогнозов")
    return list(zip(predicted, actual))


def _require_distribution(triple, what: str) -> None:
    """Проверяет, что тройка — распределение вероятностей классов.

    Сумма, отличная от единицы, означает, что модель отдала не распределение;
    усреднять такое в Brier и log loss нельзя — числа отчёта были бы не про
    ту модель, что стоит рядом в таблице.

    Не-конечные компоненты (NaN, ±inf) отвергаются ЯВНО через `math.isfinite`:
    сравнения `p < 0.0` и `p > 1.0` для NaN ложны, и тройка с NaN проходила бы
    проверку диапазона молча — `log_loss_multi` вернул бы nan, который «уехал»
    бы в среднее без исключения (в отличие от нуля, который ловит
    `LOG_LOSS_EPS`). Контракт: нарушенная тройка — `ValueError` на выходе метода
    или метрики, а НЕ пропуск фолда: это дефект кода, а не свойство окна.

    Проверка суммы считает конечность САМА (`isfinite` + отклонение от единицы):
    `abs(nan − 1) > 1e-9` — False, и одна она NaN бы пропустила. Проверки
    независимы друг от друга и от порядка строк.
    """
    total = sum(triple)
    if len(triple) != len(_CLASS_ORDER) or any(
        not math.isfinite(probability) or probability < 0.0 or probability > 1.0
        for probability in triple
    ):
        raise ValueError(f"{what}: {triple!r} — не вероятности классов {{−1, 0, +1}}")
    if not math.isfinite(total) or abs(total - 1.0) > 1e-9:
        raise ValueError(f"{what}: сумма вероятностей {total!r} ≠ 1")


def rmse_bp(errors: list) -> float:
    """RMSE прогноза шага в б.п.; `errors` — ошибки (прогноз − факт).

    Обёртка над `eval_utils.rmse` (перенос один-в-один): имя, аргумент и
    конвенция прежние, тесты `key_rate_model_test.py` не меняются.
    """
    return eu.rmse(errors)


def mae_bp(errors: list) -> float:
    """MAE прогноза шага в б.п.; `errors` — ошибки (прогноз − факт)."""
    return eu.mae(errors)


def hit_rate(predicted: list, actual: list) -> float:
    """Доля совпадений направления прогноза и факта (шаги в б.п., спека §7)."""
    pairs = _pairs(predicted, actual, "hit rate")
    return sum(1 for pred, act in pairs if _sign(pred) == _sign(act)) / len(pairs)


def confusion3(predicted: list, actual: list) -> list:
    """Матрица 3×3 «факт × прогноз» по направлениям (строки — факт).

    Порядок строк и столбцов — `_CLASS_ORDER` (−1, 0, +1), то есть «снижение,
    сохранение, повышение».
    """
    matrix = [[0 for _ in _CLASS_ORDER] for _ in _CLASS_ORDER]
    for pred, act in _pairs(predicted, actual, "матрица 3×3"):
        matrix[_class_index(_sign(act), "матрица 3×3")][
            _class_index(_sign(pred), "матрица 3×3")
        ] += 1
    return matrix


def qwk(predicted: list, actual: list) -> float:
    """Quadratic weighted kappa по направлениям (шаги в б.п., спека §7).

    Классы — `_CLASS_ORDER`, квадратичные веса `w_ij = (i − j)²/(k − 1)²`;
    ожидаемая матрица — произведение маргиналов, делённое на n. Полное
    совпадение даёт 1, а постоянный прогноз при непостоянном факте — 0:
    метрика не вознаграждает отсутствие информации (так выглядит B1 на
    режимном блоке).

    Вырождение: все прогнозы и факты в одном классе — ожидаемое расхождение
    нулевое, kappa не определён. В этом случае наблюдённое расхождение тоже
    нулевое (прогноз не ошибся ни разу), и соглашение — 1.0.
    """
    pairs = _pairs(predicted, actual, "QWK")
    size = len(pairs)
    classes = len(_CLASS_ORDER)
    observed = [[0] * classes for _ in range(classes)]
    actual_counts = [0] * classes
    predicted_counts = [0] * classes
    for pred, act in pairs:
        row = _class_index(_sign(act), "QWK")
        column = _class_index(_sign(pred), "QWK")
        observed[row][column] += 1
        actual_counts[row] += 1
        predicted_counts[column] += 1
    numerator = 0.0
    denominator = 0.0
    for row in range(classes):
        for column in range(classes):
            weight = (row - column) ** 2 / (classes - 1) ** 2
            numerator += weight * observed[row][column]
            denominator += weight * actual_counts[row] * predicted_counts[column] / size
    if denominator == 0.0:
        return 1.0
    return 1.0 - numerator / denominator


def brier_multi(probs: list, y_true: list) -> float:
    """Brier (multiclass): среднее Σ_k (p_k − 1[y = k])² по фолдам.

    `probs` — вероятности классов в порядке `_CLASS_ORDER` (−1, 0, +1),
    `y_true` — классы решений.
    """
    if len(probs) != len(y_true) or not probs:
        raise ValueError(f"Brier: {len(probs)} прогнозов против {len(y_true)} фактов")
    total = 0.0
    for triple, outcome in zip(probs, y_true):
        _require_distribution(triple, "Brier")
        target = _class_index(outcome, "Brier")
        total += sum(
            (probability - (1.0 if index == target else 0.0)) ** 2
            for index, probability in enumerate(triple)
        )
    return total / len(probs)


def log_loss_multi(probs: list, y_true: list) -> float:
    """Log loss (multiclass): среднее −ln p(факт) по фолдам.

    Вероятность факта снизу ограничена `LOG_LOSS_EPS`: без порога нулевая
    вероятность дала бы бесконечность, и всё среднее «уехало» бы в inf.
    """
    if len(probs) != len(y_true) or not probs:
        raise ValueError(f"log loss: {len(probs)} прогнозов против {len(y_true)} фактов")
    total = 0.0
    for triple, outcome in zip(probs, y_true):
        _require_distribution(triple, "log loss")
        probability = triple[_class_index(outcome, "log loss")]
        total -= math.log(max(probability, LOG_LOSS_EPS))
    return total / len(probs)


@dataclass(frozen=True)
class ReliabilityBin:
    """Бин надёжностной таблицы: уверенность прогноза против точности.

    `lo`/`hi` — границы бина по вероятности argmax-класса (последний бин
    замкнут справа); `mean_prob` — средняя уверенность бина, `accuracy` —
    доля попаданий argmax-класса в факт.
    """

    lo: float
    hi: float
    count: int
    mean_prob: float
    accuracy: float


def reliability_bins(probs: list, y_true: list, n_bins: int = 5) -> list:
    """Надёжностная таблица по уверенности прогноза (спека §7).

    Фолды раскладываются по `n_bins` равным бинам вероятности argmax-класса
    («уверенности»); в каждом бине — средняя уверенность и фактическая доля
    попаданий. При уверенности 1.0 модель обязана угадывать всегда, при 0.5 —
    в половине случаев; расхождение и есть мера пере- или недоуверенности.

    Ничья argmax разрешается в пользу младшего класса (порядок
    `_CLASS_ORDER`) — у упорядоченных вероятностей она мера нулевая. Пустые
    бины в результат не попадают: у строки без наблюдений нет ни счёта, ни
    долей, и в таблице отчёта она была бы шумом.
    """
    if len(probs) != len(y_true) or not probs:
        raise ValueError(
            f"надёжностная таблица: {len(probs)} прогнозов против {len(y_true)} фактов"
        )
    if n_bins < 1:
        raise ValueError(f"надёжностная таблица: бинов {n_bins} — меньше одного")
    buckets = [[] for _ in range(n_bins)]
    for triple, outcome in zip(probs, y_true):
        _require_distribution(triple, "надёжностная таблица")
        best = max(range(len(_CLASS_ORDER)), key=lambda index: triple[index])
        confidence = triple[best]
        hit = _CLASS_ORDER[best] == outcome
        position = min(int(confidence * n_bins), n_bins - 1)
        buckets[position].append((confidence, hit))
    bins = []
    for position, bucket in enumerate(buckets):
        if not bucket:
            continue
        bins.append(
            ReliabilityBin(
                lo=position / n_bins,
                hi=(position + 1) / n_bins,
                count=len(bucket),
                mean_prob=sum(confidence for confidence, _ in bucket) / len(bucket),
                accuracy=sum(1.0 for _, hit in bucket if hit) / len(bucket),
            )
        )
    return bins


# --- Тесты значимости и бутстрап §7 -------------------------------------------


def diebold_mariano(err_a: list, err_b: list, h: int = 1) -> tuple:
    """DM-тест (поправка HLN) на равенство точности двух прогнозов (спека §7).

    Тонкая обёртка над `eval_utils.diebold_mariano`: путь (statsmodels с
    `harvey_adj=True`), соглашение о вырождении (одинаковые/нулевая дисперсия
    разницы потерь — `(0.0, 1.0)` или `(±inf, 0.0)`) и docstring перенесены
    туда целиком; здесь остались имя, аргументы, значение `h` по умолчанию и
    поведение, которое проверяют тесты `key_rate_model_test.py`.
    """
    return eu.diebold_mariano(err_a, err_b, h)


def pesaran_timmermann(pred_sign: list, actual_sign: list) -> tuple:
    """Pesaran–Timmermann по знаку прогноза и факта (спека §7).

    Путь — библиотечный: `statsmodels.stats.diagnostic.pesaran_timmermann`
    (нормальная статистика; порядок аргументов библиотеки — «факт, прогноз»,
    направление классифицируется по `> 0`, как и класс `y3`). Тест отвечает,
    зависит ли попадание по знаку от самих знаков, и вырождается, когда
    прогнозы или факты знака не меняют (так выглядит B1 «ставка не
    изменится»): библиотека поднимает `ValueError`. Соглашение, как у DM на
    одинаковых ошибках, — `(0.0, 1.0)`: направленных различий не заявлено.

    Знаки подаются уже сведёнными к {−1, 0, +1}: у нуля здесь то же значение
    «ставка не изменилась», что и у класса `y3`.
    """
    if len(pred_sign) != len(actual_sign):
        raise ValueError(f"PT: {len(pred_sign)} прогнозов против {len(actual_sign)} фактов")
    if not pred_sign:
        raise ValueError("PT: пустой список знаков")
    try:
        result = sm.stats.diagnostic.pesaran_timmermann(actual_sign, pred_sign)
    except ValueError:
        # Единственный ValueError, достижимый здесь, — «оценка дисперсии
        # неположительна»: длины и непустота проверены выше, значения — знаки,
        # а ряд короче двух точек на живом прогоне невозможен.
        return (0.0, 1.0)
    return (float(result.statistic), float(result.pvalue))


@dataclass(frozen=True)
class MincerZarnowitz:
    """Результат MZ-регрессии «факт на прогноз» (спека §7).

    `intercept`/`slope` — коэффициенты (`fact_bp = a + b·pred_bp`);
    `p_value` — совместная гипотеза `a = 0 ∧ b = 1` на HAC-ковариации.
    """

    intercept: float
    slope: float
    p_value: float


def mincer_zarnowitz(actual: list, predicted: list) -> Optional[MincerZarnowitz]:
    """Mincer–Zarnowitz: регрессия «факт на прогноз» с ковариацией HAC (§7).

    Несмещённость и точность прогноза проверяются совместно (`a = 0 ∧ b = 1`)
    F-тестом на той же HAC-конфигурации, что у AR(1) и методов №2–3.

    Вырожденный вход возвращает `None` (в отчёте — «—»), а не исключение:
    регрессия не определена, если точек меньше `MIN_OLS_OBS` или прогноз не
    варьирует (константа коллинеарна свободному члену — так выглядит B1).
    Точная подгонка (остатков нет в пределах машинной погрешности) — тоже
    вырождение: дисперсия оценки нулевая, и F-статистика не определена.
    Соглашение: гипотеза не отклоняется (p = 1.0), если выполнена точно, и
    отклоняется (p = 0.0), если нет. Без него точная подгонка при верной H0
    получала бы ложное p ≈ 0.0001 из одной лишь погрешности округления.
    """
    if len(actual) != len(predicted):
        raise ValueError(f"MZ: {len(actual)} фактов против {len(predicted)} прогнозов")
    if len(actual) < MIN_OLS_OBS or len(set(predicted)) < 2:
        return None
    result = _ols_hac(actual, predicted)
    intercept = float(result.params[0])
    slope = float(result.params[1])
    scale = max(1.0, sum(value * value for value in actual))
    if float(result.ssr) <= MZ_EXACT_RTOL * scale:
        exact = abs(intercept) <= MZ_EXACT_ATOL and abs(slope - 1.0) <= MZ_EXACT_ATOL
        return MincerZarnowitz(intercept, slope, 1.0 if exact else 0.0)
    joint = result.f_test((np.array([[1.0, 0.0], [0.0, 1.0]]), np.array([0.0, 1.0])))
    return MincerZarnowitz(intercept, slope, float(np.squeeze(joint.pvalue)))


# Тип результата бутстрапа — тоже из общего модуля: `km.BootstrapDiff`
# остаётся прежним именем (обёртка ниже возвращает именно его).
BootstrapDiff = eu.BootstrapDiff


def block_bootstrap_rmse_diff(
    err_a: list, err_b: list, block: int = 4, n: int = 2000, seed: int = SEED
) -> BootstrapDiff:
    """95%-интервал и p-value разницы RMSE двух рядов ошибок (спека §7).

    Тонкая обёртка над `eval_utils.block_bootstrap_rmse_diff`: реализация,
    конвенция p и квантили перенесены туда целиком, а здесь остались прежнее
    имя и значения по умолчанию `block=4, n=2000, seed=SEED` — они часть
    спеки §7 этого прогона, а не общего модуля.
    """
    return eu.block_bootstrap_rmse_diff(err_a, err_b, block, n, seed)


# --- Отчёт и артефакты §8 -----------------------------------------------------


@dataclass(frozen=True)
class Skipped:
    """Заседания вне выборки, по причинам (спека §3/§4).

    Три списка — три разные причины, и отчёт печатает их раздельно: «вне
    выборки» (окно исхода ещё не наблюдаемо), «пропущено» без ставки на d−1 и
    «пропущено» без данных по признакам. Слитый счётчик скрывал бы, какая
    именно проверка не пройдена.
    """

    excluded: tuple
    missing_rate: tuple
    missing_features: tuple


def _predictions_by_group(predictions: list) -> dict:
    """«метод × окно» → фолды, в порядке первого появления.

    Порядок задаёт прогон (`method_predictions` и строки методов №2–3), и
    от него зависят строки таблиц отчёта: сортировка по имени сделала бы
    отчёт зависимым от алфавита, а не от спецификации.
    """
    groups = {}
    for prediction in predictions:
        groups.setdefault((prediction.method, prediction.window), []).append(prediction)
    return groups


def _fold_counts(predictions: list) -> dict:
    """Число фолдов по окнам (полное — по бенчмаркам, максимум по методам)."""
    counts = {}
    for (_, window), group in _predictions_by_group(predictions).items():
        counts[window] = max(counts.get(window, 0), len(group))
    return counts


def _group_errors(group: list) -> list:
    """Ошибки фолдов (прогноз − факт) в б.п."""
    return [prediction.step_bp - prediction.actual_step_bp for prediction in group]


def _group_steps(group: list) -> tuple:
    """(прогнозы, факты) шага в б.п. для метрик и тестов значимости."""
    return (
        [prediction.step_bp for prediction in group],
        [prediction.actual_step_bp for prediction in group],
    )


def _probabilistic(group: list) -> Optional[dict]:
    """Вероятностные метрики группы или `None`, если распределений нет.

    Распределение даёт не каждый метод (у вырожденных бенчмарков его нет,
    спека §5), поэтому метрики считаются только там, где вероятности есть у
    КАЖДОГО фолда группы: усечённая группа дала бы смесь двух определений.
    Пустая группа — тоже `None`: у неё нет ни прогнозов, ни распределения.
    """
    if not group or any(prediction.probs is None for prediction in group):
        return None
    probs = [prediction.probs for prediction in group]
    outcomes = [prediction.actual_y3 for prediction in group]
    return {
        "brier": brier_multi(probs, outcomes),
        "log_loss": log_loss_multi(probs, outcomes),
        "bins": reliability_bins(probs, outcomes),
    }


def _fmt(value: Optional[float], places: int = 2) -> str:
    """Число для таблицы отчёта: `None` — «—», бесконечность — «±inf»."""
    if value is None:
        return "—"
    if math.isinf(value):
        return "+inf" if value > 0 else "−inf"
    formatted = f"{value:.{places}f}"
    # «-0.00» — артефакт округления малой отрицательной величины: знака в
    # разрядах таблицы нет, и минус читался бы как ошибка знака.
    if float(formatted) == 0.0:
        return formatted.lstrip("-")
    return formatted


def _fmt_p(pvalue: Optional[float]) -> str:
    """p-value для таблицы: менее 1e-4 — «<0.0001» (разряды округления не нужны)."""
    if pvalue is None:
        return "—"
    if pvalue < 0.0001:
        return "<0.0001"
    return f"{pvalue:.4f}"


_METRIC_HEADER = (
    "| метод | окно | n | RMSE, б.п. | MAE, б.п. | hit rate | QWK | Brier | log loss |",
    "|---|---|---|---|---|---|---|---|---|",
)
_FOLD_SUMMARY_HEADER = (
    "| метод | окно | фолдов | пропущено (вырожденное окно) |",
    "|---|---|---|---|",
)
_SIGNIFICANCE_HEADER = (
    "| метод | окно | против | ΔRMSE, б.п. | DM | DM p | 95% ДИ ΔRMSE | бутстр. p |",
    "|---|---|---|---|---|---|---|---|",
)
_MZ_PT_HEADER = (
    "| метод | окно | n | MZ a | MZ b | MZ p (a=0 ∧ b=1) | PT | PT p |",
    "|---|---|---|---|---|---|---|---|",
)
_RELIABILITY_HEADER = (
    "| метод | окно | бин уверенности | n | средняя уверенность | точность |",
    "|---|---|---|---|---|---|",
)
_LONG_RUN_HEADER = (
    "| параметр | значение |",
    "|---|---|",
)
_CONSENSUS_WAVE_HEADER = (
    "| год | медиана | p10 | p90 | ответов |",
    "|---|---:|---:|---:|---:|",
)
_CONSENSUS_BUCKET_HEADER = (
    "| бакет | n | cons RMSE | cons MAE | cons bias "
    "| hold RMSE | hold MAE | hold bias | DM | p |",
    "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
)
_CONSENSUS_AUDIT_HEADER = (
    "| год | волна | доступна | бакет | медиана | p10 | p90 | ответов "
    "| факт | ошибка cons | ошибка hold |",
    "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
)
_CONSENSUS_B4_METRIC_HEADER = (
    "| n | RMSE, б.п. | MAE, б.п. | hit rate | QWK |",
    "|---:|---:|---:|---:|---:|",
)
_CONSENSUS_B4_COMPARISON_HEADER = (
    "| против | окно | n | ΔRMSE, б.п. | DM | p |",
    "|---|---|---:|---:|---:|---:|",
)


def _long_run_rows(rows: list) -> list:
    """Строки блока долгосрочных импликаций правила Тейлора (спека §6).

    Блок описывает ФОРМУ свободной оценки на переданных строках (отчёт подаёт
    полную выборку). Вырожденное окно и ρ = 1 — не падение отчёта: причину
    называет одна строка «не оценены» — в отчёте остаётся видно, что блок не
    посчитан и почему, а не пропадает молча.
    """
    try:
        long_run = taylor_long_run(rows)
    except DegenerateWindowError as error:
        return [f"| долгосрочные импликации | не оценены: {error} |"]
    return [
        f"| заседаний в оценке | {long_run.n} |",
        f"| ρ (инерция, при rate_before) | {_fmt(long_run.rho, 4)} |",
        f"| c (свободный член) | {_fmt(long_run.c, 4)} |",
        f"| a (при pi_saar) | {_fmt(long_run.a_pi, 4)} |",
        f"| b (при gap) | {_fmt(long_run.b_gap, 4)} |",
        f"| r* = c/(1−ρ) | {_fmt(long_run.r_star)} |",
        f"| α = a/(1−ρ) | {_fmt(long_run.alpha)} |",
        f"| β = b/(1−ρ) | {_fmt(long_run.beta)} |",
    ]


def _table(header: tuple, rows: list) -> list:
    """Строки таблицы markdown; без фолдов таблица не рисуется, а помечается."""
    if not rows:
        return ["_(фолдов нет.)_"]
    return list(header) + list(rows)


def _fold_summary_row(method: str, window: str, group: list, fold_skips: dict) -> str:
    """Строка сводки «метод × окно»: сделанные прогнозы и пропущенные окна.

    «фолдов» — реально сделанные прогнозы (они же `n` в таблице метрик),
    «пропущено» — вырожденные обучающие окна, на которых метод не оценился.
    Пустая группа здесь — не отсутствие строки, а «фолдов 0»: метод, вырожденный
    на ВСЕХ окнах, обязан остаться в перечне со счётчиком пропусков (спека §6 —
    «метод на этом шаге пропускается с записью в отчёт»).
    """
    return (
        f"| {method_label(method)} | {window} | {len(group)} "
        f"| {fold_skips.get((method, window), 0)} |"
    )


def _metric_row(method: str, window: str, group: list) -> str:
    """Строка таблицы метрик «метод × окно» (спека §7).

    Пустая группа даёт строку с пометкой «0 фолдов» (и прочерками вместо
    метрик): так выглядит и метод, вырожденный на всех окнах, и режимный блок у
    метода без заседаний 2024–2026. Метрики для пустой группы не определены, но
    строка — часть состава таблицы, и пропускать её значило бы прятать метод.
    """
    if not group:
        return f"| {method_label(method)} | {window} | 0 фолдов | — | — | — | — | — | — |"
    predicted, actual = _group_steps(group)
    errors = _group_errors(group)
    probabilistic = _probabilistic(group)
    brier = _fmt(probabilistic["brier"], 4) if probabilistic else "—"
    log_loss = _fmt(probabilistic["log_loss"], 4) if probabilistic else "—"
    return (
        f"| {method_label(method)} | {window} | {len(group)} "
        f"| {_fmt(rmse_bp(errors))} | {_fmt(mae_bp(errors))} "
        f"| {_fmt(hit_rate(predicted, actual), 3)} | {_fmt(qwk(predicted, actual), 3)} "
        f"| {brier} | {log_loss} |"
    )


def _significance_rows(groups: dict) -> list:
    """Строки DM и бутстрапа: каждая строка против каждого бенчмарка (§7).

    Сравнение метода с самим собой не выводится (это тождество «0, 1.0»), а
    бенчмарки между собой выводятся: это часть картины «кто из обязательных
    бенчмарков точнее», и спека §5 фиксирует их порядок.

    Парность: ошибки сравниваются на ОБЩИХ заседаниях двух строк — если у одной
    стороны фолд пропущен (вырожденное окно, Tasks 4–5), он выпадает у обеих, и
    DM не сравнивает потери на разных наборах. Пустое пересечение — ошибка
    прогона (сравнивать нечего), а не строка с пустыми числами.
    """
    rows = []
    for (method, window), group in groups.items():
        for benchmark in BENCHMARK_METHODS:
            if benchmark == method:
                continue
            reference = groups.get((benchmark, window))
            if reference is None:
                continue
            reference_errors = {
                prediction.meeting_date: prediction.step_bp - prediction.actual_step_bp
                for prediction in reference
            }
            method_errors = {
                prediction.meeting_date: prediction.step_bp - prediction.actual_step_bp
                for prediction in group
            }
            # Парность — на ОБЩИХ заседаниях: если у одной из сторон фолд
            # пропущен (вырожденное окно, Tasks 4–5), он выпадает у обеих, иначе
            # DM сравнивал бы потери на разных наборах. Пустое пересечение —
            # ошибка прогона: сравнивать нечего.
            shared = [day for day in method_errors if day in reference_errors]
            if not shared:
                raise ValueError(
                    f"DM: у «{method}» и «{benchmark}» ({window}) нет общих заседаний "
                    f"— сравнивать нечего"
                )
            errors_a = [method_errors[day] for day in shared]
            errors_b = [reference_errors[day] for day in shared]
            statistic, pvalue = diebold_mariano(errors_a, errors_b)
            bootstrap = block_bootstrap_rmse_diff(errors_a, errors_b)
            rows.append(
                f"| {method_label(method)} | {window} | {method_label(benchmark)} "
                f"| {_fmt(bootstrap.rmse_diff)} | {_fmt(statistic)} | {_fmt_p(pvalue)} "
                f"| [{_fmt(bootstrap.low)}, {_fmt(bootstrap.high)}] "
                f"| {_fmt_p(bootstrap.p_value)} |"
            )
    return rows


def _mz_pt_rows(groups: dict) -> list:
    """Строки MZ и PT по методам и окнам (спека §7)."""
    rows = []
    for (method, window), group in groups.items():
        predicted, actual = _group_steps(group)
        mz = mincer_zarnowitz(actual, predicted)
        statistic, pvalue = pesaran_timmermann(
            [_sign(step) for step in predicted], [_sign(step) for step in actual]
        )
        rows.append(
            f"| {method_label(method)} | {window} | {len(group)} "
            f"| {_fmt(mz.intercept if mz else None, 3)} "
            f"| {_fmt(mz.slope if mz else None, 3)} "
            f"| {_fmt_p(mz.p_value if mz else None)} "
            f"| {_fmt(statistic, 3)} | {_fmt_p(pvalue)} |"
        )
    return rows


def _reliability_rows(groups: dict) -> list:
    """Строки надёжностной таблицы по методам, дающим вероятности (§7)."""
    rows = []
    for (method, window), group in groups.items():
        probabilistic = _probabilistic(group)
        if probabilistic is None:
            continue
        for bucket in probabilistic["bins"]:
            rows.append(
                f"| {method_label(method)} | {window} "
                f"| {_fmt(bucket.lo, 2)}–{_fmt(bucket.hi, 2)} | {bucket.count} "
                f"| {_fmt(bucket.mean_prob, 3)} | {_fmt(bucket.accuracy, 3)} |"
            )
    return rows


_SHOCK_WEIGHTS_HEADER = (
    "| Состав | Значение |",
    "|---|---|",
)
_SHOCK_COMPARISON_HEADER = (
    "| метод | окно | n | RMSE без весов, б.п. | RMSE с весами, б.п. "
    "| MAE без весов, б.п. | MAE с весами, б.п. | DM (HLN) | DM p |",
    "|---|---|---|---|---|---|---|---|---|",
)
_SHOCK_LOWERED_HEADER = (
    "| заседание | вес | фактический шаг, б.п. |",
    "|---|---|---|",
)


def _shock_weights_section(
    sample: list,
    predictions: list,
    unweighted_predictions: list,
    weights: dict,
    source: Optional[str],
) -> list:
    """Раздел «Веса шоков» отчёта при `--shock-weights` (спека §7).

    Состав — по требованию шага: источник и покрытие, распределение весов,
    weighted vs unweighted RMSE/MAE по `WEIGHTED_METHODS` и DM-HLN (h=1) через
    `eval_utils.diebold_mariano`, топ-10 пониженных заседаний.

    Сравнение — на ОБЩИХ фолдах метода и окна: взвешенная и невзвешенная
    подгонки могут пропустить разные вырожденные окна, и усреднение по разным
    наборам заседаний сделало бы DM недействительным. Распределение — по
    ПРИМЕНЁННЫМ весам выборки (включая 1.0 для дат, которых нет в CSV): так
    видно и пониженные заседания, и фактический вес обучения.
    """
    values, missing = weights_for_rows(sample, weights, warn=False)
    explicit = len(sample) - len(missing)
    if values:
        distribution = (
            f"{_fmt(min(values), 4)} / {_fmt(float(np.median(values)), 4)} / "
            f"{_fmt(max(values), 4)}"
        )
    else:
        distribution = "—"
    lowered = sorted(
        ((value, row) for row, value in zip(sample, values) if value < 1.0),
        key=lambda pair: (pair[0], pair[1].decision.meeting_date),
    )
    weighted_groups = _predictions_by_group(predictions)
    unweighted_groups = _predictions_by_group(unweighted_predictions)
    comparison_rows = []
    for method in WEIGHTED_METHODS:
        for window in (WINDOW_EXPANDING, window_label(ROLLING_WINDOW)):
            weighted_errors = {
                prediction.meeting_date: prediction.step_bp - prediction.actual_step_bp
                for prediction in weighted_groups.get((method, window), [])
            }
            unweighted_errors = {
                prediction.meeting_date: prediction.step_bp - prediction.actual_step_bp
                for prediction in unweighted_groups.get((method, window), [])
            }
            shared = sorted(
                day for day in weighted_errors if day in unweighted_errors
            )
            if not shared:
                comparison_rows.append(
                    f"| {method_label(method)} | {window} | 0 "
                    f"| — | — | — | — | — | — |"
                )
                continue
            errors_weighted = [weighted_errors[day] for day in shared]
            errors_unweighted = [unweighted_errors[day] for day in shared]
            statistic, pvalue = eu.diebold_mariano(
                errors_weighted, errors_unweighted, h=1
            )
            comparison_rows.append(
                f"| {method_label(method)} | {window} | {len(shared)} "
                f"| {_fmt(rmse_bp(errors_unweighted))} "
                f"| {_fmt(rmse_bp(errors_weighted))} "
                f"| {_fmt(mae_bp(errors_unweighted))} "
                f"| {_fmt(mae_bp(errors_weighted))} "
                f"| {_fmt(statistic)} | {_fmt_p(pvalue)} |"
            )
    lines = [
        "",
        "## Веса шоков",
        "",
        "Веса наблюдений из CSV `meeting_date,weight` (спека §7): правило "
        "Тейлора оценивается WLS, упорядоченный логит — взвешенной MLE; "
        "бенчмарки B1–B3 и `climatology` весов не получают. Отсутствующая в "
        "CSV дата получает нейтральный вес 1.0 и входит в счётчик покрытия. "
        "Текущий прогноз обучается на полной выборке без весов — флаг меняет "
        "walk-forward.",
        "",
    ]
    lines += _table(
        _SHOCK_WEIGHTS_HEADER,
        [
            f"| источник | {source or '—'} |",
            f"| весов в файле | {len(weights)} |",
            f"| заседаний в выборке | {len(sample)} |",
            f"| покрыто явным весом | {explicit} |",
            f"| по умолчанию 1.0 (нет в CSV) | {len(missing)} |",
            f"| распределение w: min / median / max | {distribution} |",
            f"| пониженных (w < 1) | {len(lowered)} |",
        ],
    )
    lines += [
        "",
        "Weighted vs unweighted на ОБЩИХ фолдах метода и окна: RMSE/MAE "
        "считаются по одним и тем же заседаниям, DM-HLN (h=1) сравнивает "
        "квадраты ошибок (положительная статистика — взвешенная подгонка хуже).",
        "",
    ]
    lines += _table(_SHOCK_COMPARISON_HEADER, comparison_rows)
    lines += ["", "Топ-10 пониженных заседаний (наименьший вес первым):", ""]
    if lowered:
        lines += _table(
            _SHOCK_LOWERED_HEADER,
            [
                f"| {row.decision.meeting_date.isoformat()} | {_fmt(value, 4)} "
                f"| {_fmt(float(row.decision.step_bp))} |"
                for value, row in lowered[:10]
            ],
        )
        if len(lowered) > 10:
            lines += [
                "",
                f"_Показаны 10 из {len(lowered)} пониженных заседаний._",
            ]
    else:
        lines.append("_Пониженных заседаний нет: все веса равны 1.0._")
    return lines


def render_report(
    sample: list,
    skipped: Skipped,
    predictions: list,
    forecast: Optional[str] = None,
    today: Optional[date] = None,
    fold_skips: Optional[dict] = None,
    shock_weights: Optional[dict] = None,
    shock_weights_source: Optional[str] = None,
    unweighted_predictions: Optional[list] = None,
    consensus: Optional[str] = None,
) -> str:
    """Markdown-отчёт прогона (спека §8).

    Состав: выборка и пропуски, walk-forward по двум окнам со сводкой «метод ×
    окно» (сделанные и пропущенные фолды; перечень строк — по списку `METHODS`,
    поэтому метод, вырожденный на всех окнах, виден с нулём фолдов), таблица
    «метод × окно» с метриками §7 и её режимный срез 2024–2026 (тем же перечнем
    `METHODS`; у метода без фолдов — строка «0 фолдов» с прочерками),
    долгосрочные импликации правила Тейлора (форма свободной оценки, спека §6),
    DM (поправка HLN) против каждого бенчмарка вместе с бутстрап-интервалом
    разницы RMSE, Mincer–Zarnowitz и Pesaran–Timmermann, режимный блок
    2024–2026, надёжностная таблица (если у методов есть распределения) и «что
    не проверено».

    Строки методов №2–3 приходят сюда теми же `Prediction`, что и бенчмарки
    (Tasks 4–5): таблицы и разделы считаются по «метод × окно» и не знают имён
    методов, кроме меток в `METHOD_LABELS`. Блок долгосрочных импликаций —
    единственное место, где отчёт считает по САМОЙ выборке (`taylor_long_run`),
    а не по фолдам: он описывает форму правила, а не прогнозы. `forecast` —
    готовый markdown-блок текущего прогноза (Task 6) или `None`, если прогноза
    в прогоне ещё нет; `today` — дата прогона (по умолчанию системная), от неё
    зависит только строка версии набора данных; `fold_skips` — счётчики
    вырожденных окон по ключу «(метод, окно)» (`method_predictions`), без
    них сводка показывает нули — так отчёт не приписывает методу пропусков,
    которых не считали. `shock_weights` — веса из `--shock-weights`
    (`None` — раздела «Веса шоков» нет и отчёт прежний), `shock_weights_source`
    — путь источника для раздела, `unweighted_predictions` — фолды
    взвешиваемых методов БЕЗ весов (`weighted_method_predictions`) для
    weighted vs unweighted RMSE/MAE и DM-HLN.

    `consensus` — готовый markdown-блок «Консенсус-бенчмарк №4» (Task 5) или
    `None`, если раздел не запрошен. Параметр добавлен в конец сигнатуры:
    существующие вызовы не меняются. Блок вставляется ПЕРЕД «Текущим
    прогнозом» — сначала измерение опроса, затем прогноз; при `None` раздела
    нет вовсе (тесты и вызовы без консенсуса не рисуют пустую заготовку).
    """
    today = today or date.today()
    fold_skips = fold_skips or {}
    groups = _predictions_by_group(predictions)
    counts = _fold_counts(predictions)
    regime_sample = [
        row for row in sample if REGIME_FROM <= row.decision.meeting_date <= REGIME_TO
    ]

    if sample:
        span = f" ({sample[0].decision.meeting_date} … {sample[-1].decision.meeting_date})"
    else:
        span = ""
    lines = [
        "# Модель ключевой ставки v0 — бэктест",
        "",
        f"Дата прогона: {today.isoformat()}. Версия набора данных v0 = дата прогона "
        f"+ размер выборки: {len(sample)} заседаний. Спека: "
        "`docs/superpowers/specs/2026-10-15-key-rate-model-v0-design.md` "
        "(§5 — бенчмарки, §7 — walk-forward/метрики/тесты, §8 — артефакты).",
        "",
        "## Выборка",
        "",
        "| Состав | Значение |",
        "|---|---|",
        f"| заседаний в выборке | {len(sample)}{span} |",
        # Пропуски ПЕРЕЧИСЛЯЮТСЯ, а не только считаются (спека §4): дата и
        # причина — то, по чему дефект данных ищется в источниках.
        f"| вне выборки (окно исхода не наблюдаемо) | {len(skipped.excluded)}"
        f"{_dates_suffix(list(skipped.excluded))} |",
        f"| пропущено (нет ставки на d−1) | {len(skipped.missing_rate)}"
        f"{_dates_suffix(list(skipped.missing_rate))} |",
        f"| пропущено (нет данных по признакам) | {len(skipped.missing_features)}"
        f"{_dates_suffix(list(skipped.missing_features))} |",
        "",
        "## Walk-forward",
        "",
        "Оба окна идут в отчёт целиком: выбор окна по ошибке не делается "
        "(спека §7). Каждый фолд переобучается только на прошлом, горизонт "
        "прогноза — 1 заседание.",
        "",
    ]
    if counts:
        lines.append(
            f"* расширяющееся окно (минимум {MIN_TRAIN} заседаний в обучении): "
            f"{counts.get(WINDOW_EXPANDING, 0)} фолдов на метод;"
        )
        lines.append(
            f"* скользящее окно {ROLLING_WINDOW} заседаний: "
            f"{counts.get(window_label(ROLLING_WINDOW), 0)} фолдов на метод."
        )
        lines += [
            "",
            "Вырожденное обучающее окно (мало точек, один класс у ordered-модели "
            "и т.п.) — фолд пропускается и считается, а не роняет прогон:",
            "",
        ]
        lines += _table(
            _FOLD_SUMMARY_HEADER,
            [
                # Перечень — по списку METHODS, а не по сделанным фолдам: метод,
                # вырожденный на ВСЕХ окнах, обязан быть виден вместе со
                # счётчиком пропусков (спека §6 — пропуск с записью в отчёт).
                _fold_summary_row(method, window, groups.get((method, window), []), fold_skips)
                for window in (WINDOW_EXPANDING, window_label(ROLLING_WINDOW))
                for method, _ in METHODS
            ],
        )
    else:
        lines.append(
            f"* фолдов нет: выборка короче обучения (минимум {MIN_TRAIN} заседаний)."
        )

    lines += [
        "",
        "## Метрики: метод × окно",
        "",
        "Строки — по списку `METHODS` × окна: у метода с пометкой «0 фолдов» ни один "
        "фолд не оценился (вырожденное окно — счётчик в сводке walk-forward выше), "
        "и метрики такого окна не определены.",
        "",
    ]
    lines += _table(
        _METRIC_HEADER,
        [
            _metric_row(method, window, groups.get((method, window), []))
            for window in (WINDOW_EXPANDING, window_label(ROLLING_WINDOW))
            for method, _ in METHODS
        ],
    )

    lines += [
        "",
        "## Долгосрочные импликации правила Тейлора",
        "",
        "Метод №2 — правило `i_t = c + ρ·i_{t−1} + a·π + b·gap` (спека §6); "
        "блок докладывает его коэффициенты и устойчивое состояние. Вывод "
        "формул — в docstring `taylor_long_run`; Тейлоровы значения не "
        "навязываются, и величины приводятся как есть, без ограничений на "
        "свободную оценку (ρ ≈ 1 делает преобразование чувствительным к малым "
        "изменениям оценки). Оценка — на полной выборке, вне walk-forward: "
        "блок описывает ФОРМУ правила, а не прогнозные фолды.",
        "",
    ]
    lines += _table(_LONG_RUN_HEADER, _long_run_rows(sample))

    if shock_weights is not None:
        lines += _shock_weights_section(
            sample,
            predictions,
            unweighted_predictions or [],
            shock_weights,
            shock_weights_source,
        )

    lines += [
        "",
        "## Тесты значимости: DM (поправка HLN) и блочный бутстрап",
        "",
        "DM: `err_a` — строка таблицы, `err_b` — бенчмарк; положительные "
        "статистика и ΔRMSE значат, что метод ХУЖЕ бенчмарка. Путь — "
        "библиотечный (`statsmodels.tsa.stattools.diebold_mariano_test`, "
        "`harvey_adj=True`); лаги HAC — правило библиотеки "
        "`max(h − 1, ⌈T^{1/3}⌉)`. Соглашение при нулевой дисперсии разницы "
        "потерь: `(0.0, 1.0)` — различий нет, `(±inf, 0.0)` — преимущество "
        f"бесспорно. Бутстрап — блочный (блок 4 заседания, 2000 ресэмплов, "
        f"сид {SEED}), p — двусторонняя доля ресэмплов по разные стороны нуля.",
        "",
    ]
    lines += _table(_SIGNIFICANCE_HEADER, _significance_rows(groups))

    lines += [
        "",
        "## Mincer–Zarnowitz и Pesaran–Timmermann",
        "",
        "MZ — регрессия «факт на прогноз» с ковариацией HAC "
        f"(maxlags={HAC_MAXLAGS}); p — совместная гипотеза `a = 0 ∧ b = 1`. "
        "PT — зависят ли попадания по знаку от самих знаков "
        "(`statsmodels.stats.diagnostic.pesaran_timmermann`); значение "
        "«0.000 / 1.0000» — тест не определён (знаки не варьируют), а «—» — "
        "регрессия MZ не определена (константный прогноз).",
        "",
    ]
    lines += _table(_MZ_PT_HEADER, _mz_pt_rows(groups))

    lines += [
        "",
        "## Режимный блок 2024–2026",
        "",
        f"Прошедших заседаний 2024–2026 в выборке: {len(regime_sample)}. "
        "Гиперпараметры на блоке не подбираются (спека §7); в тексте спеки "
        "заседаний 24 — октябрь и декабрь 2026 на дату прогона ещё будущие.",
        "",
    ]
    lines += _table(
        _METRIC_HEADER,
        [
            _metric_row(
                method,
                window,
                [  # блок — тот же расчёт на фолдах 2024–2026
                    prediction
                    for prediction in groups.get((method, window), [])
                    if REGIME_FROM <= prediction.meeting_date <= REGIME_TO
                ],
            )
            for window in (WINDOW_EXPANDING, window_label(ROLLING_WINDOW))
            for method, _ in METHODS
        ],
    )

    reliability_rows = _reliability_rows(groups)
    if reliability_rows:
        lines += [
            "",
            "## Надёжностная таблица",
            "",
            "Бины — по уверенности прогноза (вероятность argmax-класса); "
            "«средняя уверенность» против фактической точности бина (спека §7).",
            "",
        ]
        lines += _table(_RELIABILITY_HEADER, reliability_rows)

    if consensus is not None:
        # Раздел консенсуса — готовый блок со своим заголовком; вставляется
        # перед текущим прогнозом (сначала измерение опроса, затем прогноз).
        lines += ["", str(consensus).strip()]

    if forecast is not None:
        lines += ["", "## Текущий прогноз", "", str(forecast).strip()]

    lines += [
        "",
        "## Что не проверено",
        "",
        "- **As-of — приближение, а не point-in-time слой**: ревизии "
        "объясняющих рядов и точные даты публикаций не учтены, полноценный "
        "PIT-слой — следующий пункт фазы 4 (спека §4).",
        "- **2013 год вне выборки**: страница `cal_mp` начинается с 2014 "
        "(спека §11).",
        "- **MAPE не применяется**: шаг в б.п. пересекает нуль (спека §7).",
        "- **Бенчмарки B1–B3 распределений не дают** (вырожденные): Brier, "
        "log loss и надёжностная таблица к ним не применяются (спека §5).",
        "- **Метод №2 распределения не даёт** (точечный шаг): вероятностные "
        "метрики к строке `taylor` не применяются — они у вероятностных строк "
        "`climatology` и `ordered_logit` (спека §6/§7).",
        "- **Сам скрипт в журнал (`forecast_log`) не пишет**: текущий прогноз — "
        "локальный артефакт `key_rate_model_forecast.json`; запись выполняет "
        "отдельный writer `scripts/key_rate_forecast_log.py` через ingest "
        "(пункт `key-rate/phase-4-forecast-log`).",
        "- **Гиперпараметры не подбираются**: ни на полной выборке, ни на "
        "режимном блоке 2024–2026; обе схемы окна — проверка робастности, а "
        "не выбор лучшей (спека §7).",
        "- **Консенсус-бенчмарк №4 — годовой горизонт, а не шаг заседания**: "
        "медиана макроопроса сравнивается с фактической среднегодовой ставкой; "
        "B4 — механическая проекция остатка года, а не прогноз решения "
        "(`docs/superpowers/specs/2026-10-19-key-rate-consensus-benchmark-design.md` "
        "§3).",
    ]
    return "\n".join(lines) + "\n"


def _probs_cell(probs) -> str:
    """Вероятности классов для CSV: «0.1|0.8|0.1»; `None` — пустая ячейка."""
    if probs is None:
        return ""
    return "|".join(_csv_cell(probability) for probability in probs)


def write_predictions(
    path: Path, predictions: list, weights: Optional[dict] = None
) -> None:
    """Пишет построчный CSV прогнозов (артефакт §8).

    Одна строка — один фолд (метод × окно × заседание); вероятности — одной
    ячейкой в порядке классов (−1, 0, +1), чтобы CSV читался и глазами, и
    pandas без разбора вложенных структур.

    `weights` — веса шоков из CSV `--shock-weights`: при них в КОНЕЦ колонок
    добавляется `weight` — вес ПРОГНОЗИРУЕМОГО заседания из CSV
    (отсутствующая дата — 1.0). В обучении СОБСТВЕННОГО фолда этой строки он
    не участвует: фолд `i` учится только на прошлых заседаниях, и вес строки
    `i` применяется лишь в последующих фолдах, где она попадает в обучение.
    У невзвешенных методов (бенчмарки B1–B3, `climatology`) ячейка пустая:
    колонка не должна приписывать им вес, которого они не используют.
    `weights=None` — прежний набор колонок и тот же файл байт-в-байт.
    """
    columns = PREDICTION_COLUMNS
    if weights is not None:
        columns = PREDICTION_COLUMNS + (SHOCK_WEIGHT_COLUMN,)
    rows = []
    for prediction in predictions:
        row = {
            "meeting_date": prediction.meeting_date.isoformat(),
            "method": prediction.method,
            "window": prediction.window,
            "actual_step_bp": prediction.actual_step_bp,
            "actual_y3": prediction.actual_y3,
            "step_bp": prediction.step_bp,
            "probs": _probs_cell(prediction.probs),
        }
        if weights is not None:
            if prediction.method in WEIGHTED_METHODS:
                row[SHOCK_WEIGHT_COLUMN] = float(
                    weights.get(prediction.meeting_date, 1.0)
                )
            else:
                row[SHOCK_WEIGHT_COLUMN] = ""
        rows.append(row)
    _write_csv(path, columns, rows)


def clear_stale_artifacts(out_dir: Path) -> list:
    """Убирает артефакты ПРОШЛОГО прогона; возвращает имена удалённых файлов.

    Ранний выход `run_summary` (пустой календарь, пустая выборка) не пишет
    ничего — ни отчёта, ни CSV, ни JSON, — но файлы предыдущего прогона лежат в
    каталоге и читались бы как результат ЭТОГО: даты в именах файлов нет, а
    `key_rate_model_forecast.json` ещё и выглядит как «текущий прогноз». Это тот
    же класс дефекта, что закрыт для ветки «прогноза нет» нормального пути
    (`write_forecast_artifact`), и лечится здесь одинаково для ВСЕХ трёх
    артефактов: отчёт и CSV — тоже снимок прошлого прогона, а не «частично
    текущий». Отсутствие файлов — не ошибка.

    Возврат — имена для строки консоли раннего выхода (пусто, если чистить было
    нечего): молчание о подчистке неотличимо от её отсутствия.
    """
    removed = []
    for name in ARTIFACT_NAMES:
        path = out_dir / name
        if path.exists():
            path.unlink()
            removed.append(name)
    return removed


def _print_removed_artifacts(removed: list) -> None:
    """Строка консоли об удалённых артефактах прошлого прогона (пусто — нет строки)."""
    if removed:
        print(f"  устаревшие артефакты прошлого прогона удалены: {', '.join(removed)}")


# --- Текущий прогноз на ближайшее заседание (спека §8) ------------------------
#
# Прогноз на ближайшее заседание — не фолд walk-forward: методы обучаются на
# ПОЛНОЙ выборке, и это «выход первого среза» (спека §1). Фактов будущего
# заседания не существует, поэтому строка прогноза собирается с плейсхолдером
# решения (`forecast_row`), а входы снимаются as-of даты прогона (§4).

# Оговорка прогноза (поле `note` JSON и раздел отчёта): артефакт локальный,
# в журнал прогнозов он НЕ пишется — это отдельный пункт фазы 4 (спека §8).
FORECAST_NOTE = (
    "текущий прогноз v0 (as-of — приближение, спека §4): сам скрипт в журнал "
    "не пишет — запись в forecast_log выполняет отдельный writer "
    "scripts/key_rate_forecast_log.py"
)

# Методы текущего прогноза (спека §1: выход первого среза — методы №2 и №3):
# №2 даёт точечный шаг, №3 — вероятности классов. Порядок ключей `methods` —
# он же контракт артефакта, пинованный тестом.
FORECAST_METHODS = (("taylor", fit_taylor), ("ordered_logit", fit_ordered_logit))

# Ключи классов в `methods.ordered_logit.probs`: те же классы {−1, 0, +1}, что
# в `_CLASS_ORDER`, но строкой и со знаком у положительного класса — как в
# формате §8. Ключи — контракт артефакта (тест пинует состав и порядок) и
# совпадают с разметкой вероятностей в разделе «Текущий прогноз» отчёта.
FORECAST_CLASS_KEYS = ("-1", "0", "+1")

# Входы прогнозной строки в порядке, в котором они называются в пометке о
# недоступности (`forecast_inputs`).
FORECAST_INPUTS = ("rate_before", "pi_saar", "gap")

# Плейсхолдер фактов прогнозного заседания: известна только «ставка до», а
# фактов (уровень после, шаг, класс) не существует — и выдумывать их нельзя.
# Все три поля `Decision` несут ОДИН маркер — NaN, а не «нулевой шаг» и не
# «сохранение»: ноль выглядел бы правдоподобным значением, а NaN, если его
# прочитать, отказывает явно (класс `nan` не проходит `_class_index`,
# вероятность nan отсекает `_require_distribution`). Аннотации `Decision`
# описывают НАБЛЮДЁННУЮ строку: у прогнозной читаются только `rate_before` и
# признаки `x` — инвариант пинован тестом, который подменяет фактические поля
# мусором (`rate_after`, `step_bp`, `y3`) и требует ТОГО ЖЕ прогноза.
FORECAST_UNKNOWN_FACT = float("nan")

_FORECAST_HEADER = (
    "| Метод | Прогноз |",
    "|---|---|",
)


@dataclass(frozen=True)
class CurrentForecast:
    """Текущий прогноз на ближайшее заседание (спека §8).

    `meeting_date` — ближайшее заседание строго позже даты прогона, `data_asof`
    — дата среза данных (она же дата прогноза: в v0 прогноз считается на живых
    данных в момент прогона, и раздельные поля оставлены под будущий PIT-слой).
    `rate_before` — ставка на дату среза (уровень, % годовых): writer журнала
    прогнозов переводит шаг методов (`step_bp`) в ожидаемую ставку
    `rate_before + step_bp/100` и не читает ClickHouse повторно.
    `sample_size`/`sample_first`/`sample_last` — размер выборки обучения и срок
    наблюдения (они же «версия набора данных v0» вместе с датой прогноза).
    `methods` — тело методов для JSON (`forecast_payload`), `skipped` — методы,
    не попавшие в прогноз, с причиной, `note` — оговорка артефакта
    (`FORECAST_NOTE`) плюс причины пропусков.
    """

    meeting_date: date
    data_asof: date
    rate_before: Optional[float]
    sample_size: int
    sample_first: date
    sample_last: date
    methods: dict
    skipped: tuple
    note: str


def forecast_inputs(today: date, key_rate: list, cpd: list, ibk: list) -> tuple:
    """Входы прогноза as-of `today` в порядке `FORECAST_INPUTS`.

    Ставка — последняя действующая на дату среза (лага нет, `_available_on_the_day`),
    `pi_saar` и `gap` — последние точки, доступные по правилам §4
    (`available_from_cpd`, `available_from_survey`). Недоступный вход — `None`:
    ноль на его месте выглядел бы измеренным.
    """
    return (
        asof(key_rate, _available_on_the_day, today),
        asof(cpd, available_from_cpd, today),
        asof(ibk, available_from_survey, today),
    )


def forecast_row(meeting: date, inputs: tuple) -> Optional[Row]:
    """Строка прогноза на `meeting`: готовые входы as-of, факты — плейсхолдер.

    `inputs` — ровно то, что вернул `forecast_inputs(...)`: тройка
    `(rate_before, pi_saar_m, gap)` в порядке `FORECAST_INPUTS`. Входы
    принимаются готовыми намеренно: второй их расчёт здесь был бы вторым
    ответом на один вопрос — `current_forecast` считает входы один раз, по ним
    же называет недоступные и строит пометку.

    Фактов будущего заседания не существует (§3): `Decision` строки несёт только
    `rate_before`, а `rate_after`/`step_bp`/`y3` — ОДИН маркер
    `FORECAST_UNKNOWN_FACT` (NaN). Методы читают только `rate_before` и `x`; это
    не соглашение в комментарии, а инвариант, пинованный тестом (подмена
    фактической части мусором не меняет прогноз).

    `None` — хотя бы один вход недоступен (`forecast_inputs` отдаёт `None`):
    строку тогда не собрать, и методы в прогноз не попадают, а прогон не падает.

    `pi_saar` приводится к годовому тем же `annualize_saar`, что и признаки
    выборки: единицы прогноза и обучения обязаны совпадать.
    """
    rate_before, monthly, gap = inputs
    if rate_before is None or monthly is None or gap is None:
        return None
    unknown = Decision(
        meeting_date=meeting,
        rate_before=rate_before,
        rate_after=FORECAST_UNKNOWN_FACT,
        step_bp=FORECAST_UNKNOWN_FACT,
        y3=FORECAST_UNKNOWN_FACT,
    )
    return Row(unknown, annualize_saar(monthly), gap)


def forecast_payload(forecast: Forecast) -> dict:
    """Тело метода в JSON-прогнозе: шаг в б.п. или вероятности классов и E[step].

    Точечный прогноз (`probs is None`, метод №2) даёт `{"step_bp"}`; метод №3 —
    `{"probs": {"-1": …, "0": …, "+1": …}, "expected_step_bp": …}` — форму
    артефакта из §8. `expected_step_bp` — то же `E[step]` в БАЗИСНЫХ пунктах,
    что у `Forecast.step_bp` (метрики walk-forward и артефакт в одной шкале).
    """
    if forecast.probs is None:
        return {"step_bp": forecast.step_bp}
    return {
        "probs": {
            key: float(probability)
            for key, probability in zip(FORECAST_CLASS_KEYS, forecast.probs)
        },
        "expected_step_bp": forecast.step_bp,
    }


def forecast_cell(payload: dict) -> str:
    """Числа прогноза метода одной строкой (отчёт и консоль).

    Метод №2 — шаг в б.п.; метод №3 — вероятности классов в процентах (ключи
    те же, что в JSON-артефакте: отчёт и артефакт сверяются построчно) и
    `E[step]` в б.п.
    """
    if "probs" in payload:
        percents = ", ".join(
            f"P({key}) = {_fmt(probability * 100.0, 2)}%"
            for key, probability in payload["probs"].items()
        )
        return f"{percents}; E[step] = {_fmt(payload['expected_step_bp'])} б.п."
    return f"шаг {_fmt(payload['step_bp'])} б.п."


def current_forecast(
    rows: list,
    meetings: list,
    key_rate: list,
    cpd: list,
    ibk: list,
    today: date,
) -> Optional[CurrentForecast]:
    """Текущий прогноз на ближайшее заседание (спека §8, Task 6).

    Ближайшее заседание — первая дата СТРОГО позже `today` среди `meetings`
    (будущие включительно): `min`, а не индекс списка, — порядок `meetings` не
    важен. Заседание ровно на дату прогноза не выбирается. Нет будущих
    заседаний — `None`: JSON не пишется, прогон не падает (сообщение печатает
    вызывающий); исключения здесь нет намеренно.

    Признаки — as-of даты среза по правилам §4 (`forecast_inputs`), фактов
    заседания нет (`forecast_row`), методы §6 обучаются на ПОЛНОЙ выборке
    `rows` — это выход первого среза, а не фолд walk-forward. Пустая выборка —
    `ValueError` с причиной: срок наблюдения и методы на ней не определены
    (защита от `rows[0]`/`rows[-1]`), а не «пустой прогноз».

    Метод не попадает в `methods` и называется в `skipped` (причина — и в
    `note`), если входы недоступны на дату среза или выборка вырождена для
    метода (`DegenerateWindowError`): спека §6 — «метод на этом шаге
    пропускается с записью в отчёт», и прогноз — тот же шаг. Если так выпадают
    ВСЕ методы, прогноз остаётся, но артефакт по нему не пишется
    (`write_forecast_artifact`): JSON без единого метода — не прогноз.
    """
    if not rows:
        raise ValueError(
            "текущий прогноз: выборка обучения пуста — срок наблюдения и методы "
            "на ней не определены"
        )
    upcoming = [day for day in meetings if day > today]
    if not upcoming:
        return None
    meeting = min(upcoming)
    inputs = forecast_inputs(today, key_rate, cpd, ibk)
    missing = [name for name, value in zip(FORECAST_INPUTS, inputs) if value is None]
    methods = {}
    skipped = []
    if missing:
        reason = (
            f"на дату среза {today.isoformat()} недоступны входы: "
            f"{', '.join(missing)}"
        )
        skipped = [(method, reason) for method, _ in FORECAST_METHODS]
    else:
        row = forecast_row(meeting, inputs)
        for method, fit_predict in FORECAST_METHODS:
            try:
                methods[method] = forecast_payload(fit_predict(rows, row))
            except DegenerateWindowError as error:
                skipped.append((method, str(error)))
    note = FORECAST_NOTE
    if skipped:
        note += "; " + "; ".join(
            f"{method}: {reason}" for method, reason in skipped
        )
    return CurrentForecast(
        meeting_date=meeting,
        data_asof=today,
        rate_before=inputs[0],
        sample_size=len(rows),
        sample_first=rows[0].decision.meeting_date,
        sample_last=rows[-1].decision.meeting_date,
        methods=methods,
        skipped=tuple(skipped),
        note=note,
    )


# --- Консенсус-бенчмарк в отчёте: текущая волна, мост и B4 (спека §4/§5) -----


@dataclass(frozen=True)
class ConsensusBridge:
    """Числа текущего консенсуса для отчёта (спека §3.3/§4).

    `ytd_average`/`ytd_days` — фактическая средняя года `today.year` с 1 января
    по дату прогона (календарные дни). `projection` — B4-проекция остатка года
    по текущей волне (§3.2), `None`, когда её guards не выполнены;
    `projection_reason` называет причину. `model_level` и `implied_avg` —
    сценарий удержания после ближайшего заседания (§3.3); `None` —
    `ordered_logit` не посчитан, заседание в другом году или сценарий не
    определён; `scenario_reason` называет причину.
    """

    meeting_date: Optional[date]
    rate_before: Optional[float]
    ytd_average: float
    ytd_days: int
    projection: Optional[ConsensusPath]
    projection_reason: Optional[str]
    model_level: Optional[float]
    implied_avg: Optional[float]
    scenario_reason: Optional[str]


def consensus_bridge(
    key_rate: list,
    consensus: list,
    forecast: Optional[CurrentForecast],
    today: date,
) -> Optional[ConsensusBridge]:
    """Числа текущего консенсуса для раздела отчёта, или `None`.

    Средняя с начала года считается по фактическому пути ставки
    (`sum_rates_between`); без покрытия 1 января она не определена — `None`
    (раздел назовёт причину, а не нарисует нулевую среднюю). B4-проекция
    остатка — `_consensus_path_with_reason` для ближайшего заседания: те же
    guards §3.2, а волна обязана быть доступна уже на `today` (текущая
    проекция не заглядывает вперёд, в отличие от бэктеста B4). Причина
    отсутствия проекции/сценария переносится в `projection_reason`/
    `scenario_reason`: отчёт печатает её, а не общий список догадок.
    Сценарий удержания — `hold_scenario_average` по `expected_step_bp` метода
    `ordered_logit`.
    """
    first_day = date(today.year, 1, 1)
    if _rate_asof(key_rate, first_day) is None:
        return None
    ytd_days = (today - first_day).days + 1
    ytd_average = sum_rates_between(key_rate, first_day, today) / ytd_days
    meeting_date = forecast.meeting_date if forecast is not None else None
    rate_before = forecast.rate_before if forecast is not None else None
    projection = None
    projection_reason = None
    model_level = None
    implied_avg = None
    scenario_reason = None
    if forecast is None:
        # Ближайшего будущего заседания нет: ни проекции, ни сценария.
        projection_reason = "ближайшего будущего заседания нет"
        scenario_reason = projection_reason
    elif rate_before is None:
        projection_reason = "ставка перед ближайшим заседанием недоступна"
        scenario_reason = projection_reason
    else:
        # Текущая проекция — as-of даты прогона: волна, опубликованная между
        # `today` и заседанием, была бы заглядыванием вперёд. Бэктест B4 живёт
        # по правилу §3.2 «до заседания» и потому считается по полному ряду
        # отдельно (`consensus_path_predictions`).
        projection, projection_reason = _consensus_path_with_reason(
            consensus, key_rate, forecast.meeting_date, rate_before, asof=today
        )
        if forecast.meeting_date.year != today.year:
            # Формула сценария делит на N_Y года `today`: у заседания
            # следующего года нет среднегодовой текущего года.
            scenario_reason = (
                f"ближайшее заседание {forecast.meeting_date.isoformat()} — в "
                f"следующем календарном году, а сценарий вменяет среднегодовую "
                f"{today.year}"
            )
        else:
            payload = forecast.methods.get("ordered_logit") or {}
            expected_step = payload.get("expected_step_bp")
            if expected_step is None:
                scenario_reason = (
                    "в текущем прогнозе нет `ordered_logit` (метод пропущен "
                    "или его входы недоступны)"
                )
            else:
                candidate = rate_before + expected_step / 100.0
                implied_avg = hold_scenario_average(
                    key_rate, today, forecast.meeting_date, rate_before, candidate
                )
                if implied_avg is None:
                    scenario_reason = (
                        "сценарий не определён: нет ставки на дату прогона или "
                        "1 января года либо уровень не конечен"
                    )
                else:
                    model_level = candidate
    return ConsensusBridge(
        meeting_date=meeting_date,
        rate_before=rate_before,
        ytd_average=ytd_average,
        ytd_days=ytd_days,
        projection=projection,
        projection_reason=projection_reason,
        model_level=model_level,
        implied_avg=implied_avg,
        scenario_reason=scenario_reason,
    )


def _consensus_aggregate_rows(observations: list) -> list:
    """Строки таблицы годового бенчмарка: непустые бакеты в порядке спеки.

    `_table` на пустом списке печатает пометку «фолдов нет»; пустые бакеты в
    таблицу не попадают вовсе — строка с нулём наблюдений выглядела бы
    измерением.
    """
    rows = []
    for bucket in CONSENSUS_BUCKETS:
        aggregate = aggregate_consensus(observations, bucket)
        if aggregate is None:
            continue
        rows.append(
            f"| {aggregate.bucket} | {aggregate.n} "
            f"| {_fmt(aggregate.consensus_rmse_bp)} "
            f"| {_fmt(aggregate.consensus_mae_bp)} "
            f"| {_fmt(aggregate.consensus_bias_bp)} "
            f"| {_fmt(aggregate.hold_rmse_bp)} "
            f"| {_fmt(aggregate.hold_mae_bp)} "
            f"| {_fmt(aggregate.hold_bias_bp)} "
            f"| {_fmt(aggregate.dm_stat)} | {_fmt_p(aggregate.dm_p)} |"
        )
    return rows


def render_consensus_section(
    observations: list,
    current: Optional[CurrentConsensus],
    bridge: Optional[ConsensusBridge],
    b4_predictions: list,
    b4_metrics_value: Optional[B4Metrics],
    b4_comparisons: list,
    today: date,
    b4_skips: Optional[list] = None,
    consensus_waves: Optional[int] = None,
) -> str:
    """Markdown-раздел «Консенсус-бенчмарк №4» (спека §5).

    Состав: вводные (источник, правило доступности, годовая средняя ≠ шаг),
    текущая волна (медианы/p10/p90/answers, средняя с начала года, B4-проекция
    остатка и сценарий удержания), годовой бенчмарк по бакетам, построчный
    аудит, режимный срез 2024–2025, B4 (метрики, сравнения, пропуски по
    причинам) и «что не проверено». Пустой консенсус — строка «данных нет»,
    а не падение: это штатная ветка офлайн-прогона.

    Числа приходят готовыми (`observations`, `current`, `bridge`,
    `b4_predictions`, `b4_metrics_value`, `b4_comparisons`): рендерер только
    форматирует их через `_table`/`_fmt`, как и остальные таблицы отчёта.
    `b4_skips` — `consensus_path_skips(...)`: пары `(meeting_date, причина)`.
    Пропуски группируются по стабильным меткам (`CONSENSUS_SKIP_REASONS`),
    печатается `причина — N (min_date … max_date)`; счётчик «без B4-фолда» —
    это `len(b4_skips)`, поэтому сумма по причинам совпадает с пропусками.
    `consensus_waves` различает пустую витрину (`0`) и витрину с волнами, но
    без доступных волн/завершённых лет (`>0`).
    Причины отсутствия текущей проекции/сценария берутся из
    `bridge.projection_reason`/`scenario_reason`, а не угадываются.
    """
    lines = [
        "## Консенсус-бенчмарк №4 (медиана макроопроса)",
        "",
        "Источник — витрина `v_cbr_mo_br` (ряды "
        "`key_rate_avg_<год>_median|p10|p90|answers`); `date` — дата волны "
        "(1-е число месяца). Публикация наступает через 2–6 дней после полевого "
        f"периода, поэтому волна доступна только с `wave_date + "
        f"{CONSENSUS_PUBLICATION_LAG_DAYS} дней` — консервативно, без "
        "заглядывания вперёд. Годовая средняя (медиана опроса) — не шаг "
        "заседания: сравнивать её с шагом нельзя, поэтому основной результат — "
        "годовой горизонт, а B4 — механическая проекция остатка года на шаг.",
        "",
    ]
    if not observations and current is None:
        if consensus_waves:
            lines.append(
                "_данных нет: волны в витрине есть, но нет доступных волн и "
                "завершённых лет для бенчмарка._"
            )
        else:
            lines.append("_данных нет: витрина `v_cbr_mo_br` пуста._")
        return "\n".join(lines) + "\n"

    if current is not None:
        lines += [
            "### Текущий консенсус",
            "",
            f"Последняя доступная волна — **{current.wave_date.isoformat()}** "
            f"(доступна с **{current.available_from.isoformat()}**), "
            f"устарела на **{current.stale_days}** дн.",
            "",
        ]
        if current.stale_days > CONSENSUS_STALE_AFTER_DAYS:
            lines += [
                "Волна старше естественного цикла (8 волн/год ≈ 6–7 недель): "
                "последняя публикация могла быть пропущена, и текущий консенсус "
                "не подменяется более свежими данными.",
                "",
            ]
        lines += _table(
            _CONSENSUS_WAVE_HEADER,
            [
                f"| {wave.year} | {_fmt(wave.median)} | {_fmt(wave.p10)} "
                f"| {_fmt(wave.p90)} | "
                f"{wave.answers if wave.answers is not None else '—'} |"
                for wave in current.rows
            ],
        )
    else:
        lines += [
            "### Текущий консенсус",
            "",
            "_Текущей волны нет: ни одна волна ещё не опубликована на дату "
            "прогона._",
        ]

    current_year_median = None
    if current is not None:
        current_year_median = next(
            (wave.median for wave in current.rows if wave.year == today.year), None
        )
    if bridge is not None:
        lines += [
            "",
            f"Средняя с начала {today.year} года "
            f"(1 января … {today.isoformat()}, календарные дни): "
            f"**{_fmt(bridge.ytd_average)} %**.",
        ]
        if bridge.projection is not None:
            path = bridge.projection
            lines.append(
                f"B4-проекция остатка {path.year} года по медиане "
                f"{_fmt(path.annual_median)}: **{_fmt(path.implied_level)} %** "
                f"(волна {path.wave_date.isoformat()}, осталось "
                f"{path.days_remaining} дн.; шаг к ставке перед заседанием "
                f"{path.meeting_date.isoformat()} — {_fmt(path.step_bp)} б.п.)."
            )
        else:
            reason = bridge.projection_reason or "причина не определена"
            lines.append(
                f"B4-проекция остатка года не определена: {reason}. Guards: "
                "нет волны на год заседания с публикацией до заседания; волна "
                "ещё не опубликована на дату среза; ряд ставки не покрывает "
                "дату доступности волны или 1 января года заседания; на "
                f"остаток года меньше {CONSENSUS_MIN_DAYS_REMAINING} дней; "
                "`r_implied` вне (−100, 100) %."
            )
    else:
        # `bridge is None` — ряд ставки не покрывает 1 января года `today`;
        # без него не собрать ни среднюю с начала года, ни текущую проекцию.
        lines += [
            "",
            "Средняя с начала года не определена: ряд ставки не покрывает "
            "1 января текущего года.",
            "B4-проекция остатка года не определена: ряд ставки не покрывает "
            "1 января текущего года (и дату доступности волны).",
        ]

    lines += ["", "### Сценарий удержания", ""]
    if bridge is None:
        lines += [
            "_Сценарий не оценён: ряд ставки не покрывает начало текущего года._"
        ]
    elif bridge.implied_avg is None:
        reason = bridge.scenario_reason or "причина не определена"
        lines += [f"_Сценарий не оценён: {reason}._"]
    else:
        difference = (
            (bridge.implied_avg - current_year_median) * 100.0
            if current_year_median is not None
            else None
        )
        meeting = (
            bridge.meeting_date.isoformat() if bridge.meeting_date is not None else "—"
        )
        lines += _table(
            _LONG_RUN_HEADER,
            [
                f"| ближайшее заседание | {meeting} |",
                f"| ставка перед заседанием | {_fmt(bridge.rate_before)} % |",
                f"| уровень после заседания (`ordered_logit`) "
                f"| {_fmt(bridge.model_level)} % |",
                f"| вменённая среднегодовая {today.year} "
                f"| {_fmt(bridge.implied_avg)} % |",
                f"| медиана консенсуса на {today.year} "
                f"| {_fmt(current_year_median)} % |",
                f"| разница (сценарий − консенсус) | {_fmt(difference)} б.п. |",
            ],
        )
        lines += [
            "",
            "Сценарий удержания — не траектория и не тест значимости; "
            "в журнал прогнозов он не пишется.",
        ]

    lines += [
        "",
        "### Годовой бенчмарк",
        "",
        "Пара — волна с медианой на завершённый год и фактическая среднегодовая "
        "ставка; ошибки в б.п., положительная — консенсус завысил. Наивный hold "
        "— «ставка не меняется» по правилу §3.1. DM (HLN, `h=1`): положительная "
        "статистика значит, что консенсус ХУЖЕ hold; при `n < 2` и нулевой "
        "дисперсии разницы потерь тест не определён («—»).",
        "",
    ]
    lines += _table(_CONSENSUS_BUCKET_HEADER, _consensus_aggregate_rows(observations))

    lines += [
        "",
        "### Построчный аудит волн",
        "",
        "Все пары выборки: волна, её горизонт (бакет), медиана и ошибки против "
        "факта; p10/p90/answers взяты из волны и в метрики не входят.",
        "",
    ]
    lines += _table(
        _CONSENSUS_AUDIT_HEADER,
        [
            f"| {observation.year} | {observation.wave_date.isoformat()} "
            f"| {observation.available_from.isoformat()} | {observation.bucket} "
            f"| {_fmt(observation.median)} | {_fmt(observation.p10)} "
            f"| {_fmt(observation.p90)} "
            f"| {observation.answers if observation.answers is not None else '—'} "
            f"| {_fmt(observation.actual)} "
            f"| {_fmt(observation.consensus_error_bp)} "
            f"| {_fmt(observation.hold_error_bp)} |"
            for observation in observations
        ],
    )

    regime_observations = [
        observation
        for observation in observations
        if CONSENSUS_REGIME_FROM <= observation.year <= CONSENSUS_REGIME_TO
    ]
    lines += [
        "",
        f"### Режимный срез {CONSENSUS_REGIME_FROM}–{CONSENSUS_REGIME_TO}",
        "",
        "Тот же годовой бенчмарк на завершённых годах текущего режима; срез "
        "описательный, гиперпараметры на нём не подбираются.",
        "",
    ]
    lines += _table(
        _CONSENSUS_BUCKET_HEADER, _consensus_aggregate_rows(regime_observations)
    )

    lines += [
        "",
        "### B4: проекция годовой медианы на шаг заседания",
        "",
        "B4 — механическая проекция: остаток года заполняется одной ставкой, "
        "при которой годовая средняя совпала бы с медианой; это не прогноз "
        "заседания. В `METHODS`, JSON прогноза и журнал B4 не входит; "
        "положительные DM и ΔRMSE значат, что B4 хуже строки сравнения. "
        f"Фолдов B4: {len(b4_predictions)}.",
        "",
    ]
    if b4_metrics_value is None:
        lines.append("_B4 не определено ни на одном заседании._")
    else:
        lines += _table(
            _CONSENSUS_B4_METRIC_HEADER,
            [
                f"| {b4_metrics_value.n} | {_fmt(b4_metrics_value.rmse_bp)} "
                f"| {_fmt(b4_metrics_value.mae_bp)} "
                f"| {_fmt(b4_metrics_value.hit_rate, 3)} "
                f"| {_fmt(b4_metrics_value.qwk, 3)} |"
            ],
        )
    lines += ["", "Сравнение с методами и бенчмарками на общих заседаниях:", ""]
    lines += _table(
        _CONSENSUS_B4_COMPARISON_HEADER,
        [
            f"| {method_label(comparison.against)} | {comparison.window} "
            f"| {comparison.n} "
            f"| {_fmt(comparison.delta_rmse_bp)} | {_fmt(comparison.dm_stat)} "
            f"| {_fmt_p(comparison.dm_p)} |"
            for comparison in b4_comparisons
        ],
    )
    skipped = list(b4_skips or [])
    reason_dates = {}
    for meeting_date, reason in skipped:
        reason_dates.setdefault(reason, []).append(meeting_date)
    missing_suffix = " — сумма по причинам ниже." if skipped else "."
    lines += [
        "",
        f"Заседаний выборки без B4-фолда: **{_fmt(len(skipped), 0)}**"
        f"{missing_suffix}",
    ]
    if skipped:
        # Метки из реестра идут в его порядке; неизвестная метка не теряется,
        # иначе сумма строк разошлась бы со счётчиком пропусков.
        ordered_reasons = [
            reason for reason in CONSENSUS_SKIP_REASONS if reason in reason_dates
        ]
        ordered_reasons += [
            reason for reason in reason_dates if reason not in CONSENSUS_SKIP_REASONS
        ]
        lines.append("")
        for reason in ordered_reasons:
            dates = reason_dates[reason]
            lines.append(
                f"- {reason} — {len(dates)} "
                f"({min(dates).isoformat()} … {max(dates).isoformat()})"
            )
    lines += [
        "",
        "### Что не проверено",
        "",
        "- **Малая выборка**: экс-анте — 4 пары, внутригодовые бакеты — 8–10; "
        "p-value индикативный, превосходство без значимости не заявляется.",
        "- **Режимные сдвиги 2022 и 2024–2025**: режимный срез описательный, "
        "смена режима может объяснять часть ошибки.",
        "- **Дата публикации волны**: +6 дней — консервативное правило; точные "
        "даты публикаций волн не проверены (`source_publications` не хранит "
        "волны).",
        "- **Годовая средняя ≠ шаг заседания**: медиана опроса — не прогноз "
        "решения; B4 лишь механически проецирует её на шаг.",
        "- **B4 — механика, а не траектория**: остаток года заполняется одной "
        "ставкой; результат чувствителен к округлению медианы и длине хвоста.",
    ]
    return "\n".join(lines) + "\n"


def render_forecast(forecast: CurrentForecast) -> str:
    """Markdown-блок раздела «Текущий прогноз» (артефакт §8, Task 6).

    Состав — по требованию шага: заседание, дата среза данных, срок наблюдения
    выборки («версия набора данных v0»), методы со числами (вероятности классов
    в процентах, `E[step]`/шаг в базисных пунктах) и оговорка `note` — включая
    «в журнал не пишем»; метод, не попавший в прогноз, печатается строкой
    «не оценён» с причиной, а не исчезает.

    Блок передаётся в `render_report(forecast=…)` готовым текстом — так отчёт не
    знает о внутренней форме прогноза, как и о форме фолдов walk-forward.
    """
    sample_span = (
        f"{forecast.sample_first.isoformat()} … {forecast.sample_last.isoformat()}"
    )
    lines = [
        f"Ближайшее заседание — **{forecast.meeting_date.isoformat()}**; данные "
        f"среза — на **{forecast.data_asof.isoformat()}** (признаки as-of — "
        f"приближение, спека §4). Выборка обучения: {forecast.sample_size} "
        f"заседаний ({sample_span}).",
        "",
    ]
    reasons = dict(forecast.skipped)
    lines += _table(
        _FORECAST_HEADER,
        [
            f"| {method_label(method)} | "
            + (
                forecast_cell(forecast.methods[method])
                if method in forecast.methods
                else f"не оценён: {reasons.get(method, 'причина не указана')}"
            )
            + " |"
            for method, _ in FORECAST_METHODS
        ],
    )
    lines += [
        "",
        f"Оговорка: {forecast.note}.",
    ]
    return "\n".join(lines) + "\n"


def forecast_json(forecast: CurrentForecast) -> str:
    """Текст JSON-артефакта `key_rate_model_forecast.json` (спека §8).

    Формат: `forecast_date`, `meeting_date`, `data_asof`, `rate_before`,
    `methods`, `sample_size`, `note`. `forecast_date` — дата прогноза, она же в
    v0 дата среза данных `data_asof` (прогноз считается на живых данных в дату
    прогона); поля разделены под будущий PIT-слой. `rate_before` — ставка на
    дату среза: writer журнала (`scripts/key_rate_forecast_log.py`) переводит
    шаги методов в ожидаемую ставку без повторного чтения ClickHouse. Других
    полей нет: ни отметок времени, ни путей — JSON детерминирован при (данные,
    дата прогона), и это пиновано тестом двух прогонов.

    `methods` — методы §6 (`FORECAST_METHODS`): у каждого либо тело
    (`forecast_payload`), либо его нет вовсе — пропуск с причиной виден в
    `note`. `sample_size` вместе с `forecast_date` образует «версию набора
    данных» v0 (спека §8).
    """
    payload = {
        "forecast_date": forecast.data_asof.isoformat(),
        "meeting_date": forecast.meeting_date.isoformat(),
        "data_asof": forecast.data_asof.isoformat(),
        "rate_before": forecast.rate_before,
        "methods": forecast.methods,
        "sample_size": forecast.sample_size,
        "note": forecast.note,
    }
    return json.dumps(payload, ensure_ascii=False, indent=2) + "\n"


def write_forecast_artifact(path: Path, forecast: Optional[CurrentForecast]) -> bool:
    """Пишет JSON прогноза или УБИРАЕТ устаревший артефакт; `False` — писать нечего.

    Артефакт — снимок прогноза на дату прогона. Ветка «прогноз не сделан» (нет
    будущего заседания) и ветка «ни одного метода в прогнозе нет» (недоступны
    входы или выборка вырождена для всех методов §6) обязаны не молчать, а
    УДАЛИТЬ файл прошлого прогона: оставленный, он был бы прочитан как текущий
    прогноз. Пустой `methods` — тоже не прогноз, поэтому JSON по нему не
    пишется (формула «прогноз есть» — «есть хотя бы один метод»).

    Возврат: `True` — файл записан, `False` — файла нет (в том числе удалён
    только что). Сообщение об итоге печатает вызывающий: причину он знает
    (`forecast`), а функция занята файлом.
    """
    if forecast is None or not forecast.methods:
        path.unlink(missing_ok=True)
        return False
    path.write_text(forecast_json(forecast), encoding="utf-8")
    return True


# --- Экспорт фикстур и прогон по умолчанию -------------------------------------


def _csv_cell(value) -> str:
    """Ячейка CSV для значения из JSON-ответа.

    `None` → пустая ячейка (отличимо от измеренного нуля); float — `repr`
    (полная точность, значения читаются обратно бит-в-бит); всё остальное —
    `str` (даты и целые).
    """
    if value is None:
        return ""
    if isinstance(value, float):
        return repr(value)
    return str(value)


def _write_csv(path: Path, columns: tuple, rows: list) -> None:
    """Пишет CSV с LF-переводами строк (фикстуры и артефакты прогона).

    `csv.writer` по умолчанию завершает строки CRLF, а в репозитории принят LF:
    с CRLF файл на диске расходится с тем, что лежит в git (нормализация
    `core.autocrlf=input`), и на машине без нормализации повторный экспорт
    показывал бы фикстуры изменёнными. `lineterminator` пинует LF явно.
    """
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(columns)
        for row in rows:
            writer.writerow([_csv_cell(row.get(column)) for column in columns])


def _with_fixture_extra_meetings(rows: list) -> list:
    """Добавляет к срезу `events_calendar` тестовую ручную строку-триггер.

    Строка объявлена в `FIXTURE_MEETINGS_EXTRA` и добавляется ИМЕННО экспортом:
    правка CSV руками была бы затёрта следующей регенерацией, а требование
    «регенерация даёт тот же файл» — часть контракта фикстур. Строки
    сортируются по дате, чтобы файл читался как обычный срез календаря.
    """
    extra = [
        {"event_date": day.isoformat(), "category": category, "title": title}
        for day, category, title in FIXTURE_MEETINGS_EXTRA
    ]
    return sorted(list(rows) + extra, key=lambda row: str(row.get("event_date", "")))


def export_fixtures(directory: Path) -> int:
    """Пишет пять CSV-фикстур офлайн-тестов в `directory`.

    Ряды серий — срез `[FIXTURE_FROM, FIXTURE_TO]`; список заседаний и резюме
    выгружаются целиком: список нужен вместе с ручными строками-триггерами
    (фильтр по title), а резюме малы и целиком образуют пару «ставка+дельта»
    для сверки ярлыков.

    Ноль строк в ответе — ОШИБКА ЭКСПОРТА (`RuntimeError`), а не расхождение
    выборки: пустая фикстура молча ослабила бы тесты. Все пять запросов
    выполняются и проверяются ДО первой записи, поэтому неудачный экспорт не
    оставляет каталога с половиной файлов.
    """
    directory.mkdir(parents=True, exist_ok=True)
    exports = (
        ("meetings.csv", MEETINGS_SQL),
        ("key_rate_2013_2016.csv", FIXTURE_KEY_RATE_SQL),
        ("cpd_core_2013_2016.csv", FIXTURE_CPD_CORE_SQL),
        ("ibk_2013_2016.csv", FIXTURE_IBK_SQL),
        ("mp_summary.csv", MP_SUMMARY_SQL),
    )
    planned = []
    for name, sql in exports:
        rows = ch_query(sql)
        if not rows:
            raise RuntimeError(f"фикстура {name}: запрос вернул 0 строк")
        if name == "meetings.csv":
            rows = _with_fixture_extra_meetings(rows)
        planned.append((name, FIXTURE_COLUMNS[name], rows))

    print(f"Фикстуры записаны в {directory}:")
    for name, columns, rows in planned:
        _write_csv(directory / name, columns, rows)
        first = _csv_cell(rows[0].get(columns[0]))
        last = _csv_cell(rows[-1].get(columns[0]))
        print(f"  {name:<24} {len(rows):>5} строк, {first} … {last}")
    return 0


def _dates_suffix(days: list) -> str:
    """Перечисление дат для строки сводки и таблицы отчёта.

    Пустой список — пустая строка: у пустого перечня нет ни тире, ни запятых,
    и строка «0» не превращается в «0 — ».
    """
    if not days:
        return ""
    return " — " + ", ".join(day.isoformat() for day in days)


def run_summary(
    rolling: bool,
    today: Optional[date] = None,
    out_dir: Path = ROOT,
    ml_log: Optional[bool] = None,
    logger: Optional[object] = None,
    run_name: Optional[str] = None,
    shock_weights: Optional[dict] = None,
    shock_weights_source: Optional[str] = None,
) -> int:
    """Полный прогон: выборка, walk-forward, текущий прогноз, артефакты (§7/§8).

    `today` — дата, относительно которой календарь делится на прошедшие и
    предстоящие заседания (по умолчанию системная), и дата среза текущего
    прогноза (признаки — as-of `today`, спека §4). Прошедшее заседание —
    то, решение по которому ЦБ уже объявил; наблюдаемо ли уже ЕГО ИСХОД
    (окно `min(d+7, next−1)`), решает `build_decisions` и называет в
    «вне выборки». Предстоящие в выборку решений не подаются вовсе: у них нет
    ни решения, ни исхода, и печатаются они отдельной строкой; ближайшее из них
    — заседание текущего прогноза.

    `rolling` переключает ОКНО консольной таблицы метрик (`--rolling` —
    скользящее 40, по умолчанию — расширяющееся); в отчёт и CSV всегда идут
    оба окна (спека §7: выбор окна по ошибке не делается). `out_dir` — каталог
    артефактов: корень репозитория в живом прогоне, временный каталог в тестах.

    Состав выборки обязан сходиться с ожидаемыми числами приёмки (108
    заседаний, 0 пропущенных, 0 «вне выборки», диапазон 2014-02-14 … 2026-09-11).
    Бенчмарки B1–B3, метод №2, метод №3 и вероятностный базлайн `climatology`
    идут в прогон одной строкой каждый (список `METHODS`): отчёт, DM/MZ/PT, CSV
    и режимный блок считаются по «метод × окно» и имён методов не знают.

    Артефакты: отчёт (с разделами «Консенсус-бенчмарк №4» — включая перечень
    пропусков B4 по причинам — и «Текущий прогноз», если прогноз сделан) и CSV
    фолдов; `key_rate_model_forecast.json` пишется, только если прогноз есть И в
    нём посчитан хотя бы один метод. Иначе JSON не пишется, а прежний файл
    (`write_forecast_artifact`) УДАЛЯЕТСЯ: оставленный снимок прошлого прогона
    был бы прочитан как текущий прогноз. Консоль в обеих ветках называет причину
    прямо (исключения нет: ни отсутствие будущих заседаний, ни вырожденная для
    всех методов выборка — не ошибка).

    Ранние выходы (пустой календарь, пустая выборка) возвращают 1 ДО записи
    артефактов — и потому убирают файлы прошлого прогона
    (`clear_stale_artifacts`, все три: отчёт и CSV тоже снимок прошлого прогона),
    называя удалённое в консоли. Иначе «последний прогон» на диске врал бы о
    результате этого.

    `ml_log`/`logger`/`run_name` — журнал эксперимента
    (`scripts/lib/ml_experiment.py`): при `ml_log=True` (или `ML_LOG=1`, если
    параметр не задан) создаётся прогон `key_rate_model` с паспортом,
    fold-предсказаниями в базисных пунктах и артефактами; явный `logger`
    используется как есть (duck typing). По умолчанию (`ML_LOG` не задан) в
    ClickHouse ничего не пишется — только чтение и локальные артефакты.

    `shock_weights` — веса заседаний из `--shock-weights` (`load_shock_weights`)
    или `None`: при них walk-forward `taylor`/`ordered_logit` взвешивается,
    отчёт получает раздел «Веса шоков», а CSV — колонку `weight`. Без них
    отчёт/CSV/JSON прежние байт-в-байт.
    """
    today = today or date.today()
    if ml_log is None:
        ml_log = os.environ.get("ML_LOG") == "1"
    config = {
        "rolling": rolling,
        "min_train": MIN_TRAIN,
        "rolling_window": ROLLING_WINDOW,
        "methods": [method for method, _ in METHODS],
        "forecast_methods": [method for method, _ in FORECAST_METHODS],
    }
    if shock_weights is not None:
        # Конфиг журнала меняется ТОЛЬКО при флаге: официальный прогон без
        # весов обязан остаться прежним и в паспорте эксперимента.
        config["shock_weights"] = shock_weights_source or "argument"
        config["shock_weights_dates"] = len(shock_weights)

    def start_run(sample_meta, sample):
        """Создаёт прогон журнала: имя по окну, общий конфиг и дата среза."""
        return mlx.start_run(
            experiment="key_rate_model",
            run_name=(
                os.environ.get("ML_RUN_NAME")
                or run_name
                or f"key_rate_model__{window_label(ROLLING_WINDOW if rolling else None)}"
            ),
            config=config,
            data_asof=today,
            sample_meta=mlx.json_safe(sample_meta),
            sample=mlx.json_safe(sample),
            spec_path=SPEC_PATH,
            enabled=True,
        )

    def finish(status: str, notes: str) -> None:
        """Пометить прогон, не меняя исход: сбой журнала — только warning.

        Журнал — побочный слой: ни сбой `finish`, ни ошибка записи в ClickHouse
        не должны превращать удачный прогон в non-zero exit (сам `finish`
        `ml_experiment` дополнительно fail-soft: возвращает `"local"`).
        """
        if logger is None:
            return
        try:
            logger.finish(status, notes)
        except Exception as exc:
            log.warning(
                "не удалось пометить прогон %s: %s: %s",
                status, type(exc).__name__, exc,
            )

    meetings = load_meetings()
    if not meetings:
        # Ранний выход не пишет артефактов: файлы прошлого прогона обязаны
        # исчезнуть, иначе они читались бы как результат этого (см.
        # `clear_stale_artifacts`). Журнал создаётся ДО выхода: пустой
        # календарь — тоже прогон, и его статус — `empty`.
        if logger is None and ml_log:
            logger = start_run({"rows": 0}, [])
        _print_removed_artifacts(clear_stale_artifacts(out_dir))
        print("events_calendar: канонических заседаний нет — проверить импорт календаря")
        finish("empty", "канонических заседаний нет")
        return 1
    upcoming = [day for day in meetings if day > today]
    past = [day for day in meetings if day <= today]

    deltas = load_mp_summary_deltas()
    # Ряды читаются по одному разу: ключевая ставка нужна и выборке, и
    # прогнозной строке, и консенсус-бенчмарку; ИПЦ и ИБК — признакам выборки и
    # признакам прогноза.
    key_rate = load_key_rate()
    # Консенсус-бенчмарк №4 (спека §2/§3): волны опроса читаются один раз;
    # годовые пары считаются по завершённым годам, B4 — по строкам выборки.
    # Пустая витрина — штатная ветка: раздел отчёта скажет «данных нет».
    consensus = load_consensus()
    observations = consensus_observations(consensus, key_rate)
    decisions, excluded, missing_rate = build_decisions(past, key_rate, deltas)
    cpd = load_cpd_core()
    ibk = load_ibk()
    rows, missing_features = build_rows(decisions, cpd, ibk)
    b4 = consensus_path_predictions(rows, consensus, key_rate)
    b4_skips = consensus_path_skips(rows, consensus, key_rate)
    # Инвариант перечня пропусков (спека §5.6): каждая строка выборки даёт
    # ровно один B4-фолд либо одну строку пропуска с причиной.
    if len(b4) + len(b4_skips) != len(rows):
        raise SampleError(
            f"B4: {len(b4)} фолдов + {len(b4_skips)} пропусков "
            f"≠ {len(rows)} строк выборки"
        )
    current = current_consensus(consensus, today)
    # Walk-forward считается до проверки «выборка пуста»: счётчик вырожденных
    # окон входит в sample_meta паспорта, а на пустой выборке он нулевой.
    # С `--shock-weights` основная таблица — взвешенная; невзвешенная база
    # считается отдельно и только для взвешиваемых методов (раздел «Веса шоков»).
    predictions, fold_skips = method_predictions(rows, weights=shock_weights)
    unweighted_predictions = (
        weighted_method_predictions(rows) if shock_weights is not None else None
    )
    sample_meta = {
        "rows": len(rows),
        "first": rows[0].decision.meeting_date.isoformat() if rows else None,
        "last": rows[-1].decision.meeting_date.isoformat() if rows else None,
        "excluded": len(excluded),
        "missing_rate": len(missing_rate),
        "missing_features": len(missing_features),
        "fold_skips": sum(fold_skips.values()),
        # Консенсус-бенчмарк: волн (уникальных дат), годовых пар и B4-фолдов —
        # те же числа, что в строке консоли и разделе отчёта.
        "consensus_waves": len({wave.wave_date for wave in consensus}),
        "consensus_pairs": len(observations),
        "b4_folds": len(b4),
    }
    if logger is None and ml_log:
        logger = start_run(
            sample_meta,
            [
                {
                    "meeting_date": row.decision.meeting_date.isoformat(),
                    "rate_before": row.decision.rate_before,
                    "pi_saar": row.pi_saar,
                    "gap": row.gap,
                }
                for row in rows
            ],
        )
    if not rows:
        _print_removed_artifacts(clear_stale_artifacts(out_dir))
        print("выборка пуста: ни одного решения с наблюдаемым исходом и признаками")
        finish(
            "empty",
            "выборка пуста: ни одного решения с наблюдаемым исходом и признаками",
        )
        return 1

    delta_map = dict(deltas)
    checked = sum(
        1 for decision in decisions if delta_map.get(decision.meeting_date) is not None
    )

    try:
        forecast = current_forecast(rows, meetings, key_rate, cpd, ibk, today)
        # Мост текущего консенсуса: средняя с начала года, B4-проекция остатка
        # и сценарий удержания по `ordered_logit` (если метод посчитан).
        bridge = consensus_bridge(key_rate, consensus, forecast, today)
        consensus_text = render_consensus_section(
            observations,
            current,
            bridge,
            b4,
            b4_metrics(b4),
            b4_significance(b4, predictions),
            today,
            # Перечень пропусков B4 — `(дата, причина)`; счётчик «без
            # B4-фолда» рендерер выводит из его длины, поэтому сумма по
            # причинам всегда сходится с `len(rows) - len(b4)`.
            b4_skips=b4_skips,
            consensus_waves=sample_meta["consensus_waves"],
        )
        report_path = out_dir / REPORT_NAME
        predictions_path = out_dir / PREDICTIONS_NAME
        forecast_path = out_dir / FORECAST_NAME
        report_path.write_text(
            render_report(
                rows,
                Skipped(tuple(excluded), tuple(missing_rate), tuple(missing_features)),
                predictions,
                render_forecast(forecast) if forecast is not None else None,
                today=today,
                fold_skips=fold_skips,
                shock_weights=shock_weights,
                shock_weights_source=shock_weights_source,
                unweighted_predictions=unweighted_predictions,
                consensus=consensus_text,
            ),
            encoding="utf-8",
        )
        write_predictions(predictions_path, predictions, weights=shock_weights)
        forecast_written = write_forecast_artifact(forecast_path, forecast)

        print(f"Модель ключевой ставки v0 — выборка решений (дата прогона {today.isoformat()})")
        print(f"  канонических заседаний:                      {len(meetings)}"
              f" ({meetings[0]} … {meetings[-1]})")
        print(f"  предстоящих (позже даты прогона):            {len(upcoming)}"
              + _dates_suffix(upcoming))
        print(f"  в выборке решений:                           {len(rows)}"
              f" ({rows[0].decision.meeting_date} … {rows[-1].decision.meeting_date})")
        print(f"  вне выборки (окно исхода не наблюдаемо):     {len(excluded)}"
              + _dates_suffix(excluded))
        print(f"  пропущено (нет ставки на d−1):               {len(missing_rate)}"
              + _dates_suffix(missing_rate))
        print(f"  пропущено (нет данных по признакам):         {len(missing_features)}"
              + _dates_suffix(missing_features))
        print(f"  сверено шагов с cbr_mp_summary:              {checked} из {len(deltas)}"
              f" (у самого раннего резюме дельты нет)")

        counts = _fold_counts(predictions)
        print(f"  фолдов (расширяющееся окно, min_train={MIN_TRAIN}):   "
              f"{counts.get(WINDOW_EXPANDING, 0)}")
        print(f"  фолдов (скользящее окно, {ROLLING_WINDOW}):               "
              f"{counts.get(window_label(ROLLING_WINDOW), 0)}")
        print(f"  пропущено фолдов (вырожденное окно):         {sum(fold_skips.values())}")
        print(f"  консенсус-бенчмарк: волн "
              f"{sample_meta['consensus_waves']}, годовых пар "
              f"{sample_meta['consensus_pairs']}, B4-фолдов "
              f"{sample_meta['b4_folds']}")
        if shock_weights is not None:
            applied, missing = weights_for_rows(rows, shock_weights, warn=False)
            lowered = sum(1 for value in applied if value < 1.0)
            print(f"  веса шоков ({shock_weights_source or 'аргумент'}): "
                  f"покрыто {len(rows) - len(missing)} из {len(rows)} заседаний, "
                  f"w min/median/max = {min(applied):.4f}/"
                  f"{float(np.median(applied)):.4f}/{max(applied):.4f}, "
                  f"пониженных {lowered}")

        selected = window_label(ROLLING_WINDOW if rolling else None)
        print(f"  метрики окна «{selected}» (в отчёте и CSV — оба окна):")
        printed = False
        for (method, window), group in _predictions_by_group(predictions).items():
            if window != selected:
                continue
            predicted, actual = _group_steps(group)
            errors = _group_errors(group)
            print(f"    {method_label(method):<16} RMSE {rmse_bp(errors):>8.2f}  "
                  f"MAE {mae_bp(errors):>8.2f}  hit {hit_rate(predicted, actual):.3f}")
            printed = True
        if not printed:
            print("    (фолдов нет — выборка короче обучения)")

        if forecast is None:
            print("  прогноз на ближайшее заседание:              не сделан — заседаний "
                  "позже даты прогона нет")
        else:
            print(f"  прогноз на ближайшее заседание:              {forecast.meeting_date}"
                  f" (данные среза {forecast.data_asof})")
            reasons = dict(forecast.skipped)
            for method, _ in FORECAST_METHODS:
                if method in forecast.methods:
                    print(f"    {method_label(method):<16} "
                          f"{forecast_cell(forecast.methods[method])}")
                else:
                    print(f"    {method_label(method):<16} не оценён: "
                          f"{reasons.get(method, 'причина не указана')}")
        print(f"  отчёт:                                        {report_path}")
        print(f"  прогнозы:                                     {predictions_path}")
        if forecast_written:
            print(f"  прогноз (JSON):                               {forecast_path}")
        else:
            reason = (
                "заседаний позже даты прогона нет"
                if forecast is None
                else "ни один метод не посчитан"
            )
            print(f"  прогноз (JSON):                               не пишется ({reason}); "
                  "устаревший файл, если был, удалён")
        if logger is not None:
            # Журнал эксперимента — побочный слой: его сбой НЕ меняет код
            # возврата. Отчёт и CSV к этому моменту уже записаны, поэтому
            # ValueError `PredictionRow` (например, на нечисловом прогнозе) не
            # должен превращать успешный прогон в non-zero exit: помечаем run
            # failed и идём дальше.
            try:
                grouped = _predictions_by_group(predictions)
                # Спека §3.1: `windows` — СПИСОК; у каждого окна своё число
                # фолдов и вложенный словарь моделей, а `models` — общий
                # список методов прогона.
                windows = []
                for window in (WINDOW_EXPANDING, window_label(ROLLING_WINDOW)):
                    per_method = {}
                    for method, _ in METHODS:
                        group = grouped.get((method, window), [])
                        if group:
                            errors = _group_errors(group)
                            predicted, actual = _group_steps(group)
                            values = {
                                "rmse": rmse_bp(errors),
                                "mae": mae_bp(errors),
                                "hit": hit_rate(predicted, actual),
                                "n": len(group),
                            }
                        else:
                            values = {"rmse": None, "mae": None, "hit": None, "n": 0}
                        per_method[method] = values
                    windows.append(
                        {
                            "window": window,
                            # «Фолды окна» — максимум по методам: у метода с
                            # вырожденными окнами фолдов меньше, и число окна
                            # не должно зависеть от выбранного метода.
                            "folds": max(
                                (values["n"] for values in per_method.values()),
                                default=0,
                            ),
                            "models": per_method,
                        }
                    )
                # Пропуски фолдов — отдельный словарь «окно → метод → N»:
                # ключи-кортежи `fold_skips` в JSON стали бы строками вида
                # "('hold', 'expanding')".
                skips = {}
                for (method, window), skipped in fold_skips.items():
                    skips.setdefault(window, {})[method] = int(skipped)
                logger.log_metrics(
                    {
                        "primary_metric": "rmse_bp",
                        "sample": sample_meta,
                        "models": [method for method, _ in METHODS],
                        "windows": windows,
                        "fold_skips": skips,
                    }
                )
                logger.log_predictions(
                    [
                        mlx.PredictionRow(
                            model=prediction.method,
                            window=prediction.window,
                            target_date=prediction.meeting_date,
                            predicted=prediction.step_bp,
                            actual=prediction.actual_step_bp,
                            unit="bp",
                            meta={
                                "probs": (
                                    list(prediction.probs)
                                    if prediction.probs is not None
                                    else None
                                ),
                                "actual_y3": prediction.actual_y3,
                            },
                        )
                        for prediction in predictions
                    ]
                )
                logger.log_artifacts(
                    [
                        report_path,
                        predictions_path,
                        forecast_path if forecast_written else None,
                    ]
                )
                logger.finish(
                    "finished",
                    f"walk-forward: {len(predictions)} фолдов, "
                    f"{len(rows)} заседаний; текущий прогноз: "
                    f"{'рассчитан' if forecast is not None else 'не рассчитан'}",
                )
            except Exception as exc:
                log.warning(
                    "журнал эксперимента не записан: %s: %s",
                    type(exc).__name__, exc,
                )
                try:
                    logger.finish(
                        "failed", f"logging: {type(exc).__name__}: {exc}"
                    )
                except Exception as finish_exc:
                    log.warning(
                        "не удалось пометить прогон failed: %s: %s",
                        type(finish_exc).__name__, finish_exc,
                    )
        return 0
    except Exception as exc:
        # Реальная ошибка эксперимента (а не сбой журнала): паспорт получает
        # `failed`, исключение уходит вызывающему как раньше.
        finish("failed", f"{type(exc).__name__}: {exc}")
        raise


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Модель ключевой ставки: выборка решений, as-of, walk-forward, "
                    "текущий прогноз, отчёт."
    )
    parser.add_argument(
        "--export-fixtures",
        metavar="DIR",
        default=None,
        help="записать CSV-фикстуры офлайн-тестов в каталог DIR и выйти",
    )
    parser.add_argument(
        "--rolling",
        action="store_true",
        help="метрики консольной сводки по скользящему окну 40 (по умолчанию — "
             "по расширяющемуся); в отчёт и CSV идут оба окна",
    )
    parser.add_argument(
        "--shock-weights",
        metavar="PATH",
        default=None,
        help="CSV meeting_date,weight с весами наблюдений для taylor/ordered_logit "
             "(0 < w <= 1; спека классификатора шоков §7); без флага артефакты "
             "и числа не меняются",
    )
    args = parser.parse_args(argv)
    try:
        if args.export_fixtures:
            return export_fixtures(Path(args.export_fixtures))
        shock_weights = None
        if args.shock_weights:
            try:
                shock_weights = load_shock_weights(Path(args.shock_weights))
            except (OSError, ValueError) as exc:
                # Файл весов — вход модели: битая дата/вес или отсутствующий
                # файл останавливают прогон понятным сообщением, а не
                # трейсбеком и не молчаливым весом 1.0.
                print(f"ошибка весов шоков: {exc}", file=sys.stderr)
                return 2
        # ML_LOG/ML_RUN_NAME читаются внутри run_summary (`ml_log=None`): явный
        # параметр здесь не нужен, и без переменной поведение прогона прежнее.
        return run_summary(
            rolling=args.rolling,
            shock_weights=shock_weights,
            shock_weights_source=args.shock_weights,
        )
    except SampleError as exc:
        # Расхождение ярлыка со сводкой ЦБ — ожидаемая диагностируемая ошибка
        # прогона: у неё уже есть готовый текст (заседание и оба числа), и ей
        # место в stderr с ненулевым кодом, а не трейсбеком на весь экран.
        print(f"ошибка выборки решений: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
