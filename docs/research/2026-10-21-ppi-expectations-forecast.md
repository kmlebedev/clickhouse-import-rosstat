# Ценовые ожидания как признаки прогноза ИПЦ/ИЦП

Research-эксперимент Task 2 issue #318. Протокол предрегистрирован в [docs/superpowers/specs/2026-10-21-ppi-note-followups-design.md](../../docs/superpowers/specs/2026-10-21-ppi-note-followups-design.md) и [docs/superpowers/plans/2026-10-21-ppi-note-followups.md](../../docs/superpowers/plans/2026-10-21-ppi-note-followups.md); production-модели, `forecast_log`, DDL/витрины и `ppi_sa` не меняются.

## Паспорт прогона

- run id: `9121928b-2c45-47a6-b889-68a7926a410a`
- experiment / run: `ppi_expectations_forecast` / `ppi_expectations_forecast__direct_h136`
- branch / SHA / dirty: `feat/ppi-expectations-forecast` / `7f494062ead5c98162afe8d55f5037892a5c8f5d` / `0`
- config hash: `e8d5ec1dff038e625b671a1fd9a671bf307edf4c974518c572a6a5f5a1675d5b`; sample hash: `32299db6d2019ce4634fafa4fb7691eef2eb487312ca71aab86e4435bb669330`
- data as-of: `2026-09-01`

## Данные

| target | source | first | last | n |
|---|---|---|---|---|
| cpi_all | rosstat_ipc_mes.percent - 100 (NSA m/m, pp) | 1991-01-01 | 2026-09-01 | 429 |
| ppi_core | ppi_sa.mm_sa (ppi-sa-2026-v1, pp) | 2021-02-01 | 2026-08-01 | 67 |
| ppi_food | ppi_sa.mm_sa (ppi-sa-2026-v1, pp) | 2021-02-01 | 2026-08-01 | 67 |
| ppi_nonfood | ppi_sa.mm_sa (ppi-sa-2026-v1, pp) | 2021-02-01 | 2026-08-01 | 67 |
| ppi_total | ppi_sa.mm_sa (ppi-sa-2026-v1, pp) | 2013-02-01 | 2026-08-01 | 163 |

Survey points: 104481; publication quality: {'fallback': 488, 'date': 106}.

## Покрытие фолдов

| target | horizon | folds | first_target | last_target | skipped | train_end |
|---|---|---|---|---|---|---|
| cpi_all | 1 | 258 | 2005-04-01 | 2026-09-01 | 0 | 2026-06-01 |
| cpi_all | 3 | 254 | 2005-08-01 | 2026-09-01 | 0 | 2026-02-01 |
| cpi_all | 6 | 248 | 2006-02-01 | 2026-09-01 | 0 | 2025-08-01 |
| ppi_core | 1 | 28 | 2024-05-01 | 2026-08-01 | 0 | 2026-05-01 |
| ppi_core | 3 | 24 | 2024-09-01 | 2026-08-01 | 0 | 2026-01-01 |
| ppi_core | 6 | 18 | 2025-03-01 | 2026-08-01 | 0 | 2025-07-01 |
| ppi_food | 1 | 28 | 2024-05-01 | 2026-08-01 | 0 | 2026-05-01 |
| ppi_food | 3 | 24 | 2024-09-01 | 2026-08-01 | 0 | 2026-01-01 |
| ppi_food | 6 | 18 | 2025-03-01 | 2026-08-01 | 0 | 2025-07-01 |
| ppi_nonfood | 1 | 28 | 2024-05-01 | 2026-08-01 | 0 | 2026-05-01 |
| ppi_nonfood | 3 | 24 | 2024-09-01 | 2026-08-01 | 0 | 2026-01-01 |
| ppi_nonfood | 6 | 18 | 2025-03-01 | 2026-08-01 | 0 | 2025-07-01 |
| ppi_total | 1 | 124 | 2016-05-01 | 2026-08-01 | 0 | 2026-05-01 |
| ppi_total | 3 | 120 | 2016-09-01 | 2026-08-01 | 0 | 2026-01-01 |
| ppi_total | 6 | 114 | 2017-03-01 | 2026-08-01 | 0 | 2025-07-01 |

## Модели: RMSE / MAE

| target | horizon | model | n | rmse | mae |
|---|---|---|---|---|---|
| cpi_all | 1 | b1_last | 258 | 0.862646 | 0.483178 |
| cpi_all | 1 | b2_ar1 | 258 | 0.651666 | 0.370256 |
| cpi_all | 1 | b2_ar1_expectations | 258 | 0.611572 | 0.335109 |
| cpi_all | 1 | b3_enet | 258 | 0.652982 | 0.370507 |
| cpi_all | 1 | b3_enet_expectations | 258 | 0.578893 | 0.310651 |
| cpi_all | 3 | b1_last | 254 | 0.973814 | 0.559921 |
| cpi_all | 3 | b2_ar1 | 254 | 0.664233 | 0.394057 |
| cpi_all | 3 | b2_ar1_expectations | 254 | 0.64245 | 0.381484 |
| cpi_all | 3 | b3_enet | 254 | 0.663656 | 0.395623 |
| cpi_all | 3 | b3_enet_expectations | 254 | 0.630349 | 0.370962 |
| cpi_all | 6 | b1_last | 248 | 1.0153 | 0.590484 |
| cpi_all | 6 | b2_ar1 | 248 | 0.658766 | 0.401886 |
| cpi_all | 6 | b2_ar1_expectations | 248 | 0.646329 | 0.381868 |
| cpi_all | 6 | b3_enet | 248 | 0.657878 | 0.401664 |
| cpi_all | 6 | b3_enet_expectations | 248 | 0.637862 | 0.378168 |
| ppi_core | 1 | b1_last | 28 | 0.520088 | 0.43459 |
| ppi_core | 1 | b2_ar1 | 28 | 0.765763 | 0.586605 |
| ppi_core | 1 | b2_ar1_expectations | 28 | 0.836307 | 0.647053 |
| ppi_core | 1 | b3_enet | 28 | 0.579859 | 0.488807 |
| ppi_core | 1 | b3_enet_expectations | 28 | 0.760218 | 0.603423 |
| ppi_core | 3 | b1_last | 24 | 0.65393 | 0.527754 |
| ppi_core | 3 | b2_ar1 | 24 | 0.875697 | 0.70933 |
| ppi_core | 3 | b2_ar1_expectations | 24 | 1.06721 | 0.881635 |
| ppi_core | 3 | b3_enet | 24 | 0.622069 | 0.527934 |
| ppi_core | 3 | b3_enet_expectations | 24 | 0.622069 | 0.527934 |
| ppi_core | 6 | b1_last | 18 | 0.746227 | 0.551762 |
| ppi_core | 6 | b2_ar1 | 18 | 0.960671 | 0.804753 |
| ppi_core | 6 | b2_ar1_expectations | 18 | 1.02797 | 0.897004 |
| ppi_core | 6 | b3_enet | 18 | 0.696431 | 0.61624 |
| ppi_core | 6 | b3_enet_expectations | 18 | 0.701722 | 0.619704 |
| ppi_food | 1 | b1_last | 28 | 0.516574 | 0.42269 |
| ppi_food | 1 | b2_ar1 | 28 | 0.748971 | 0.598203 |
| ppi_food | 1 | b2_ar1_expectations | 28 | 0.797617 | 0.599629 |
| ppi_food | 1 | b3_enet | 28 | 0.63179 | 0.552367 |
| ppi_food | 1 | b3_enet_expectations | 28 | 0.749254 | 0.554049 |
| ppi_food | 3 | b1_last | 24 | 0.738064 | 0.605921 |
| ppi_food | 3 | b2_ar1 | 24 | 0.863898 | 0.740974 |
| ppi_food | 3 | b2_ar1_expectations | 24 | 0.968137 | 0.760657 |
| ppi_food | 3 | b3_enet | 24 | 0.705686 | 0.653149 |
| ppi_food | 3 | b3_enet_expectations | 24 | 0.716895 | 0.663748 |
| ppi_food | 6 | b1_last | 18 | 0.836595 | 0.681917 |
| ppi_food | 6 | b2_ar1 | 18 | 0.909857 | 0.760083 |
| ppi_food | 6 | b2_ar1_expectations | 18 | 0.917869 | 0.808818 |
| ppi_food | 6 | b3_enet | 18 | 0.716323 | 0.654831 |
| ppi_food | 6 | b3_enet_expectations | 18 | 0.728318 | 0.657309 |
| ppi_nonfood | 1 | b1_last | 28 | 0.75422 | 0.6282 |
| ppi_nonfood | 1 | b2_ar1 | 28 | 0.966169 | 0.758041 |
| ppi_nonfood | 1 | b2_ar1_expectations | 28 | 1.08227 | 0.828289 |
| ppi_nonfood | 1 | b3_enet | 28 | 0.676862 | 0.594991 |
| ppi_nonfood | 1 | b3_enet_expectations | 28 | 0.925428 | 0.725068 |
| ppi_nonfood | 3 | b1_last | 24 | 0.808017 | 0.608079 |
| ppi_nonfood | 3 | b2_ar1 | 24 | 1.05194 | 0.86318 |
| ppi_nonfood | 3 | b2_ar1_expectations | 24 | 1.40669 | 1.16835 |
| ppi_nonfood | 3 | b3_enet | 24 | 0.689837 | 0.611861 |
| ppi_nonfood | 3 | b3_enet_expectations | 24 | 0.675465 | 0.586503 |
| ppi_nonfood | 6 | b1_last | 18 | 0.856461 | 0.629152 |
| ppi_nonfood | 6 | b2_ar1 | 18 | 1.06779 | 0.888297 |
| ppi_nonfood | 6 | b2_ar1_expectations | 18 | 1.22422 | 1.02117 |
| ppi_nonfood | 6 | b3_enet | 18 | 0.736649 | 0.647275 |
| ppi_nonfood | 6 | b3_enet_expectations | 18 | 0.736649 | 0.647275 |
| ppi_total | 1 | b1_last | 124 | 3.20014 | 2.23661 |
| ppi_total | 1 | b2_ar1 | 124 | 2.40013 | 1.84011 |
| ppi_total | 1 | b2_ar1_expectations | 124 | 2.92587 | 2.15747 |
| ppi_total | 1 | b3_enet | 124 | 2.25867 | 1.67205 |
| ppi_total | 1 | b3_enet_expectations | 124 | 2.2948 | 1.72866 |
| ppi_total | 3 | b1_last | 120 | 3.18684 | 2.45683 |
| ppi_total | 3 | b2_ar1 | 120 | 2.41144 | 1.7987 |
| ppi_total | 3 | b2_ar1_expectations | 120 | 3.28837 | 2.33275 |
| ppi_total | 3 | b3_enet | 120 | 2.28217 | 1.68938 |
| ppi_total | 3 | b3_enet_expectations | 120 | 2.34674 | 1.73972 |
| ppi_total | 6 | b1_last | 114 | 3.1288 | 2.36746 |
| ppi_total | 6 | b2_ar1 | 114 | 2.48109 | 1.86887 |
| ppi_total | 6 | b2_ar1_expectations | 114 | 3.24613 | 2.36086 |
| ppi_total | 6 | b3_enet | 114 | 2.3276 | 1.74037 |
| ppi_total | 6 | b3_enet_expectations | 114 | 2.32483 | 1.74198 |

## Primary-контрасты

| target | horizon | base_model | expectations_model | n | delta | dm_pvalue | holm_pvalue | ci_low | ci_high | mde |
|---|---|---|---|---|---|---|---|---|---|---|
| cpi_all | 1 | b2_ar1 | b2_ar1_expectations | 258 | 0.0400945 | 0.287772 | 1 | -0.0166401 | 0.0961676 | 0.0727303 |
| cpi_all | 3 | b2_ar1 | b2_ar1_expectations | 254 | 0.0217825 | 0.416545 | 1 | -0.03183 | 0.0768057 | 0.0958162 |
| cpi_all | 6 | b2_ar1 | b2_ar1_expectations | 248 | 0.0124378 | 0.623894 | 1 | -0.0423132 | 0.0576431 | 0.0557198 |
| ppi_core | 1 | b2_ar1 | b2_ar1_expectations | 28 | -0.0705444 | 0.55807 | 1 | -0.338532 | 0.222112 | 0.43298 |
| ppi_core | 3 | b2_ar1 | b2_ar1_expectations | 24 | -0.19151 | 0.205218 | 1 | -0.462485 | 0.0349864 | 0.359366 |
| ppi_core | 6 | b2_ar1 | b2_ar1_expectations | 18 | -0.0673016 | 0.760522 | 1 | -0.405197 | 0.213942 | 0.351534 |
| ppi_food | 1 | b2_ar1 | b2_ar1_expectations | 28 | -0.0486462 | 0.77013 | 1 | -0.318315 | 0.255655 | 0.393673 |
| ppi_food | 3 | b2_ar1 | b2_ar1_expectations | 24 | -0.104239 | 0.600874 | 1 | -0.42239 | 0.249091 | 0.388163 |
| ppi_food | 6 | b2_ar1 | b2_ar1_expectations | 18 | -0.00801232 | 0.962059 | 1 | -0.295024 | 0.241601 | 0.316694 |
| ppi_nonfood | 1 | b2_ar1 | b2_ar1_expectations | 28 | -0.116104 | 0.473037 | 1 | -0.453985 | 0.270644 | 0.590889 |
| ppi_nonfood | 3 | b2_ar1 | b2_ar1_expectations | 24 | -0.354755 | 0.0199336 | 0.598008 | -0.58366 | -0.139232 | 0.409438 |
| ppi_nonfood | 6 | b2_ar1 | b2_ar1_expectations | 18 | -0.156431 | 0.503955 | 1 | -0.481687 | 0.104823 | 0.376852 |
| ppi_total | 1 | b2_ar1 | b2_ar1_expectations | 124 | -0.525737 | 0.130979 | 1 | -1.17787 | 0.0625097 | 0.722578 |
| ppi_total | 3 | b2_ar1 | b2_ar1_expectations | 120 | -0.87693 | 0.071129 | 1 | -1.56871 | -0.199237 | 1.00758 |
| ppi_total | 6 | b2_ar1 | b2_ar1_expectations | 114 | -0.765036 | 0.131254 | 1 | -1.53571 | -0.15773 | 1.02964 |
| cpi_all | 1 | b3_enet | b3_enet_expectations | 258 | 0.074089 | 0.0811213 | 1 | 0.0232436 | 0.133963 | 0.0895122 |
| cpi_all | 3 | b3_enet | b3_enet_expectations | 254 | 0.0333071 | 0.210042 | 1 | -0.0122137 | 0.0893751 | 0.0854989 |
| cpi_all | 6 | b3_enet | b3_enet_expectations | 248 | 0.0200162 | 0.431672 | 1 | -0.0345695 | 0.0679438 | 0.0578553 |
| ppi_core | 1 | b3_enet | b3_enet_expectations | 28 | -0.180359 | 0.139373 | 1 | -0.396951 | 0.0437826 | 0.339358 |
| ppi_core | 3 | b3_enet | b3_enet_expectations | 24 | 0 | 1 | 1 | 0 | 0 | 0 |
| ppi_core | 6 | b3_enet | b3_enet_expectations | 18 | -0.00529105 | 0.436244 | 1 | -0.0163573 | 0.00317265 | 0.0123192 |
| ppi_food | 1 | b3_enet | b3_enet_expectations | 28 | -0.117465 | 0.501956 | 1 | -0.383716 | 0.195775 | 0.366817 |
| ppi_food | 3 | b3_enet | b3_enet_expectations | 24 | -0.0112087 | 0.218044 | 1 | -0.0258292 | 0 | 0.0142798 |
| ppi_food | 6 | b3_enet | b3_enet_expectations | 18 | -0.0119954 | 0.712883 | 1 | -0.0644191 | 0.0452842 | 0.0679641 |
| ppi_nonfood | 1 | b3_enet | b3_enet_expectations | 28 | -0.248566 | 0.235635 | 1 | -0.57203 | 0.00571326 | 0.574099 |
| ppi_nonfood | 3 | b3_enet | b3_enet_expectations | 24 | 0.014372 | 0.334114 | 1 | 0 | 0.0504747 | 0.0334292 |
| ppi_nonfood | 6 | b3_enet | b3_enet_expectations | 18 | 0 | 1 | 1 | 0 | 0 | 0 |
| ppi_total | 1 | b3_enet | b3_enet_expectations | 124 | -0.0361299 | 0.0698227 | 1 | -0.0793828 | 0.00244574 | 0.0451894 |
| ppi_total | 3 | b3_enet | b3_enet_expectations | 120 | -0.064576 | 0.27253 | 1 | -0.170108 | 0.0109727 | 0.120575 |
| ppi_total | 6 | b3_enet | b3_enet_expectations | 114 | 0.00276578 | 0.304015 | 1 | -0.0013409 | 0.00874874 | 0.00692141 |

## Secondary consumer

_Нет строк._

## Robustness 2022–2023

| target | horizon | base_model | expectations_model | n | delta | dm_pvalue | ci_low | ci_high | mde |
|---|---|---|---|---|---|---|---|---|---|
| cpi_all | 1 | b2_ar1 | b2_ar1_expectations | 24 | 0.147298 | 0.279774 | -0.14053 | 0.398181 | 0.284414 |
| cpi_all | 3 | b2_ar1 | b2_ar1_expectations | 24 | 0.115731 | 0.573488 | -0.224236 | 0.418222 | 0.803903 |
| cpi_all | 6 | b2_ar1 | b2_ar1_expectations | 24 | 0.0392669 | 0.745031 | -0.167754 | 0.231746 | 0.413846 |
| ppi_total | 1 | b2_ar1 | b2_ar1_expectations | 24 | -0.491498 | 0.670978 | -2.77455 | 1.3947 | 2.1697 |
| ppi_total | 3 | b2_ar1 | b2_ar1_expectations | 24 | -2.61089 | 0.0348787 | -4.33907 | -0.796326 | 2.9599 |
| ppi_total | 6 | b2_ar1 | b2_ar1_expectations | 24 | -2.45244 | 0.217434 | -4.50314 | -0.275584 | 3.85184 |
| cpi_all | 1 | b3_enet | b3_enet_expectations | 24 | 0.184204 | 0.222851 | -0.013579 | 0.437573 | 0.348664 |
| cpi_all | 3 | b3_enet | b3_enet_expectations | 24 | 0.170855 | 0.418219 | -0.142554 | 0.439378 | 0.747492 |
| cpi_all | 6 | b3_enet | b3_enet_expectations | 24 | 0.0697633 | 0.591659 | -0.13872 | 0.276312 | 0.455263 |
| ppi_total | 1 | b3_enet | b3_enet_expectations | 24 | -0.0258699 | 0.104889 | -0.0541408 | -0.000137598 | 0.0440758 |
| ppi_total | 3 | b3_enet | b3_enet_expectations | 24 | -0.241422 | 0.241559 | -0.505649 | 0 | 0.425769 |
| ppi_total | 6 | b3_enet | b3_enet_expectations | 24 | 0.0102961 | 0.398813 | -0.00114992 | 0.0331909 | 0.0304759 |

Robustness status: `reported (descriptive only)`; минимум 6 folds на пару.

## Сравнение с запиской

- CPI all: delta +0.0124…+0.0741 п.п.; положительных 6/6; Holm-значимых положительных 0/6.
- PPI food (primary consumer): delta -0.1175…-0.0080 п.п.; положительных 0/6; Holm-значимых положительных 0/6.
- Записка ЦБ ожидает выигрыш от ценовых ожиданий, особенно в потребительском секторе. В этом предрегистрированном прогоне consumer-выигрыш не подтверждается.
- Результат относится к aggregate survey-блокам и latest-vintage targets; industry consumer и полный PIT ревизий не проверены.

## Ограничения

- Survey as-of соблюдён строго: `published_at < cutoff`, boundary 8-е M+1 23:59:59 MSK.
- CPI/PPI targets — **latest-vintage**: полный real-time/PIT по ревизиям таргета не проверен.
- `quality='fallback'` сохраняется в аудите и не выдаётся за точную дату публикации.
- Secondary consumer: `FAIL_CLOSED` — industry publication calendar is missing; secondary consumer is not evaluated.
- Robustness 2022–2023 описательная и не меняет primary-вывод.

## Вердикт

`iterate`. Это research-рекомендация по предрегистрированному правилу, не production promotion; registry/alias и `forecast_log` не затрагивались.
