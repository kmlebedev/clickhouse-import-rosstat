# Ограниченный резолв прогнозов ставки на dev

Пункт key-rate/phase-4-rate-resolver-catchup, #601, role:go, Depends on #598
(merged #600, 0b62eb0). Следующий P0 из аудита каналов #593.

## Baseline и границы

В v_forecast_accuracy 15 строк:12 cbr_rate*,1 nav,2 xau_q_avg. Восемь прогнозов
ставки на23.10.2026 не имеют actual; факт в v_cbr_macro на26.10.2026 равен14%.
Четыре старых rate-строки уже сверены. Dashboard19/24 stale; RUONIA обновлена
до12.11.2026. Догон прочих рядов, ML, прогнозы будущих заседаний и staging сюда
не входят. Общий резолвер пересчитывает золото: среднее может отличиться от
сохранённого на машинное округление, поэтому он не подходит для этой приёмки.

## Контракт

FORECAST_RESOLVE_SCOPE читается os.Getenv только в Import:
- пусто / all: штатное поведение без изменений, обе ветки;
- cbr_rate: только rateResolvable/rateAfter и rate-записи одним батчем;
  resolvable/windowStats для золота вообще не вызываются;
- любое другое значение: ошибка до запросов/батчей.

Алгоритм расчёта факта/ошибки, SQL-фильтр cbr_rate*, правила shadow/canary,
схема, витрины, гранты, расписание и default DAG не меняются. Полный режим
остаётся обёрткой существующей оркестрации; ограничение применяется до чтения
золотого журнала. Это фильтр семейства метрик, не отдельного заседания/модели.
Все уже наступившие rate-прогнозы по прежним SQL правилам могут обновляться.

## Приёмка

1. Офлайн: cbr_rate не читает/не пишет gold даже при ошибке gold store; сохраняет
   predicted, даты, probabilities/model/data_version/prompt_version; error_bp
   равен (predicted-actual)*100; повтор0. default/all сохраняет обе ветки.
   Неизвестный scope отклоняется до любого обращения к store/conn.
2. make all, отдельные uncached race-тесты forecast, docs-check, dagu validate.
3. До записи: baseline всех15 строк v_forecast_accuracy, всех12 rate-строк
   v_ml_live_accuracy, dashboard с доступными probabilities, факты для всех
   target_date. Проверить loopback writeendpoint через make. Независимое review.
4. FORECAST_RESOLVE_SCOPE=cbr_rate make import STAT=forecast_resolve:
   ожидается8 записей; 4 ранее resolved rate-строки и3 non-rate строки неизменны.
   Сверить predicted/даты/model/data_version/prompt_version и арифметику всех
   error_bp через MCP. Probabilities: живьём доступны последние production-строки
   dashboard, остальные проверяются тестами переноса, не заявлять их полную
   независимую live-сверку через отсутствующие колонки витрины.
5. Повтор0, полный снимок неизменен. Три forecast-индикатора dashboard перестают
   быть stale; divergence остаётся stale, поскольку цель октябрь не декабрь.
6. Целевой MCP и global smoke с честным учётом посторонних сбоев; протокол,
   финальное независимое review, один PR. Done только после merge.
