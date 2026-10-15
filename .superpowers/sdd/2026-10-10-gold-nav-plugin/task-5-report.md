# Task 5 report: репозиторий плагина gold-nav

Status: DONE_WITH_CONCERNS. Commit `27e20fdf2e64361e8b5887460992f0889abfec4d` в `/home/claude/gold-nav` (ветка master, первый коммит). Репозиторий локальный, на GitHub не публиковался.

## Файлы

| Файл | Содержание |
|---|---|
| `kimi.plugin.json` | Manifest verbatim из брифа. `python3 -m json.tool` проходит. |
| `README.md` | Установка (Kimi Code / Kimi Work), переменные окружения, обход Review Focus #4, команды, известные ограничения, блок «Результат проверки» (placeholder). |
| `skills/gold-forecast-session/SKILL.md` | Сценарная сессия, 6 шагов; JSON-шаблон run'а; отправка на ingest; проверка через MCP. |
| `skills/dcf-methodology/SKILL.md` | Методология rev.2 (ARCHITECTURE §6.4, §7): sum-of-parts, НДПИ, AISC, двухконтурная ставка, price deck, сценарии, ресурсы вне LOM, рыночный мост, календарь триггеров. |
| `commands/session.md` | `/gold-nav:session`: запускает скилл, перед записью просит подтверждение. |
| `commands/verify.md` | `/gold-nav:verify`: сверка `forecast_log` с фактом, отчёт локально. |

Сверка с ingest выполнена автоматически: ключи JSON-шаблона совпадают с json-тегами `ModelRun` в `ingest/model_run.go` (16 полей, ни одного лишнего и ни одного пропущенного). Сумма `probabilities` в шаблоне = 100. Секреты: grep по `ghp_`, `sk-`, `Bearer <длинный токен>` ничего не нашёл.

## Соответствие шагов спеки §6 и SKILL.md

| Шаг спеки | Где в SKILL `gold-forecast-session` |
|---|---|
| 1. РФ-входы: `v_model_inputs` + `v_gold_dashboard` | Шаг 1: два запроса, проверка NULL в `last_run_*`, флаги и `stale`. |
| 2. Мировые входы, две ветки | Шаг 2: Kimi Code (`yahoo_finance`, `fred_query` с `realtime_end`), Kimi Work (Global Finance Data, IMF, `^TNX` вместо FRED). Ограничения источников. |
| 3. Календарь и пороги | Шаг 3: `v_events_calendar` с `event_date >= today()`, сопоставление порогов из `threshold`, `trigger_type` и `trigger_ref`. |
| 4. Сценарии, нормировка к 100 | Шаг 4: low/high/point, базовая сетка Q4-2026, вероятности 25/40/35, ключи `bull/base/bear`. |
| 5. NAV (упрощённый AISC×production до фазы 3) | Шаг 5: формула, НДПИ, `upside_pct`, фраза «NAV упрощённый» в `comment`. |
| 6. POST JSON на ingest, markdown локально | Шаг 6: `run_id` = UUID сессии, шаблон, `curl` с `$GOLD_NAV_INGEST_TOKEN`, коды ответов, `manual_series`, проверка через `v_model_inputs`, файл `gold-forecast-<дата>.md`. |

## Отклонения от брифа и почему

1. **Trailer Co-Authored-By.** Бриф требует `Claude Opus 5.5`. Системная инструкция по атрибуции требует `Claude Haiku 5.5`, и это совпадает с моделью, которой я являюсь. Использован `Haiku 5.5`. Если нужен Opus, исправить: `git -C /home/claude/gold-nav commit --amend`.
2. **Переменные `GOLD_NAV_INGEST_URL`.** В брифе только `${GOLD_NAV_INGEST_TOKEN}`. Без URL запрос не отправить, поэтому добавлена вторая переменная. В README и SKILL описаны обе.
3. **`verify` и витрина `v_forecast_log`.** Бриф велит читать `forecast_log`. Грант `kimi_reader` покрывает только `v_*` (ARCHITECTURE §6.5, ACCESS_DENIED), а витрины для `forecast_log` в Task 4 нет. Команда написана через `v_forecast_log` и явно помечает её как предусловие. Без неё команда сообщает «сверка недоступна» и к сырой таблице не обращается. Это нужно решить до приёмки (см. ниже).
4. **Упрощённый NAV без входных данных по активам.** Формула AISC×production требует `production`, `AISC_base`, `NetDebt`, число акций. Этих полей в `v_model_inputs` нет. Скилл велит спрашивать у пользователя и не записывать NAV, который не считали. Бриф этого явно не требовал, поэтому помечаю как решение.
5. **Поле `wacc`.** В `ModelRun` есть `wacc`. Schema-комментарий говорит, что локальная ставка идёт в `comment`/JSON, а ARCHITECTURE запрещает WACC/CAPM. В скилле `wacc` = локальный ОФЗ-контур, `discount_rate` = индустриальный. Имя поля историческое, это пояснено в скилле и в README. Требуется подтверждение владельца модели.
6. **Один `price_deck` на run.** Поле в `ModelRun` одно, а модель предполагает 3 deck'а × 2 ставки. Поэтому один run = один deck. Остальные результаты уходят в `comment`. Это не противоречит брифу, но стоит отметить.
7. **NAV-бета** не имеет поля в `ModelRun` (`nav_beta_gold` есть в DDL, но не в struct), поэтому пишется в `comment`.
8. **Ручные ряды.** В шаг 6 добавлен необязательный `POST /v1/manual_series` для индикаторов из `v_gold_dashboard`. Это соответствует спеке §7 и §5.2, в брифе шага не было.

## Review Focus

- **#4 (подстановка `${GOLD_NAV_MCP_TOKEN}`).** README содержит раздел «Если подстановка не работает» с ручной правкой установленной копии манифеста и предупреждением не коммитить её. Проверки в Kimi Work не выполнялись (см. ниже).
- **#2 (идемпотентность run_id).** SKILL велит сгенерировать UUID один раз на сессию и повторять его при ретрае.
- **#1, #3, #5** касаются витрин и ingest, в плагине учтены: NULL в `last_run_*` трактуется как «модель ещё не считалась», `stale = 1` не выдаётся за актуальное значение, даты в `manual_series` в формате `YYYY-MM-DD`.

## Самопроверка

- Манифест проверен `python3 -m json.tool`.
- Шаблон JSON распарсен и сверен с полями `ModelRun`, сумма вероятностей = 100.
- Все ссылки на поля views сверены с `views/model_inputs.go`, `views/gold_dashboard.go`, `calendar/events.go`: `last_run_id`, `last_nav_per_share`, `last_run_date`, `plzl_close`, `gold_moex_fix_usd`, `stale`, `flag`, `threshold`, `event_date`, `threshold`, `title`, `category`.
- Шаг 5 Kimi Work и проверка `${GOLD_NAV_MCP_TOKEN}` не выполнялись (запрещено брифом, на машине пользователя).
- Live-запросов к ClickHouse и ingest не делалось.
- Не проверено: что Kimi принимает `$ARGUMENTS`-подобные команды (в `session.md` аргументы не используются, поэтому риска нет); что Kimi поддерживает поля `skills`/`commands` как пути (взяты из брифа).

## Открытые вопросы (до публикации на GitHub)

1. Заменить `PLACEHOLDER_HOST` в `kimi.plugin.json` на реальный хост автора. Бриф требует оставить его в коммите, поэтому сейчас он там.
2. Создать витрину `v_forecast_log` (Task 4 или отдельная правка), иначе `/gold-nav:verify` не работает.
3. Подтвердить смысл `wacc` (см. отклонение 5).
4. Решить, нужны ли параметры активов в виде витрины, иначе NAV каждый раз спрашивается у пользователя.

## Пользовательские проверки (шаг 5, не выполнялись)

На машине пользователя, в `/home/claude/gold-nav` или в клоне репозитория:

```
kimi-daimon kimi-plugin register-personal /путь/к/gold-nav
```

Затем в Kimi Work «Плагины → Персональный»: плагин виден и включается; `${GOLD_NAV_MCP_TOKEN}` подставляется (если нет, применить обход из README); `/gold-nav:session` доходит до шага 6. Результат записать в раздел «Результат проверки» в `README.md`.

Также для приёмки (спека §8): `GC=F` через Global Finance Data возвращает данные; негативные проверки (сырой запрос → ACCESS_DENIED, невалидный JSON → 400, без токена → 401) — через ingest и MCP на сервере автора.

## Fix round 1

### Finding 1: /gold-nav:verify читал несуществующий v_forecast_log
- Canonical name по ARCHITECTURE.md §6.3 — `v_forecast_accuracy`. Создана вьюха в main repo.
- `views/forecast_accuracy.go` (new): `util.View{Name: "v_forecast_accuracy", Tables: ["forecast_log"]}`, SELECT `forecast_log FINAL` с колонками `metric, forecast_date, target_date, predicted, actual, error_pct`, плюс `abs_error_pct = abs(error_pct)` и `is_resolved = toUInt8(actual IS NOT NULL)`. Типы берутся из DDL `forecast_log` (`ingest/schema.go`). Комментарии ко вьюхе и каждой колонке в стиле `views/model_inputs.go`.
- `views/gold_views.go`: вьюха добавлена в список `gold_views` импортёра рядом с двумя существующими, новый импортёр не создавался. Обновлён комментарий пакета.
- `sql/mcp_kimi_reader.sql`: добавлен `GRANT SELECT ON default.v_forecast_accuracy TO kimi_reader;`.
- Проверки: `gofmt -l views` чисто (rc=0). `go vet ./views/` не выполнен: Go пытался скачать toolchain go1.27.2 и упал (нет доступа к proxy.golang.org). Файлы перечитаны вручную: импорты, имена и поля `util.View` совпадают с `gold_dashboard.go`.
- Commit main repo: `4d5923f9515c0c5facf95f4802842543f2d7ce39` ("add v_forecast_accuracy view").
- Плагин: `commands/verify.md` переведён на `v_forecast_accuracy`, колонки `abs_error_pct`, `is_resolved`; шаг 2 и 3 используют `is_resolved` и `error_pct`. README: блок про `v_forecast_log` заменён на ссылку на `v_forecast_accuracy`.

### Finding 2: валюта NAV не была определена
- `skills/gold-forecast-session/SKILL.md`, шаг 5: добавлено правило валют. FCF_t считается в USD. Перевод в RUB по курсу `usdrub_path` с ключами горизонта из `gold_scenario.horizon`. Период без курса не оценивается, пропуск пишется в `comment`. NetDebt в RUB. `nav_per_share`, `nav_bull`, `nav_base`, `nav_bear` в RUB на акцию. `market_price` = `plzl_close` в RUB. Описание `usdrub_path` в шаге 6 уточнено.
- `skills/dcf-methodology/SKILL.md`, §2: добавлен раздел «Валюта» с тем же правилом и формулой `upside_pct`. §5: сказано, как дисконтируется каждый контур. Индустриальный: USD-поток по USD-ставке, NPV в RUB по курсу. Локальный: RUB-поток по ОФЗ-ставке.
- Проверка ×100: `upside_pct = (nav_per_share / plzl_close − 1) × 100`. Пример из `ingest/server_test.go`: 12500 / 6800 − 1 = 0.8382, ×100 = 83.8, совпадает с `upside_pct: 83.8`. Процентная конвенция подтверждена.
- Commit gold-nav: `7bbc075c84bea0139d43dc73db3c130809a87177` ("fix verify view and nav currency"). Предыдущий коммит `27e20fd` не менялся.
- Проверки: `python3 -m json.tool kimi.plugin.json` проходит. Старое имя `v_forecast_log` в плагине не осталось, кроме README-ссылки, которая теперь на `v_forecast_accuracy`.

### Открытые вопросы после fix round 1
- НДПИ в USD или RUB. В плагине НДПИ считается по USD-базе с переводом по курсу периода, как и остальной FCF. Налог фактически платится в рублях. Нужно подтверждение владельца модели.
- `go vet` не запускался, см. выше. Перед merge нужна сборка `make all` на машине с Go.
- `PLACEHOLDER_HOST` в манифесте по-прежнему нужно заменить перед публикацией.

## Fix round 2

- Противоречие НДПИ устранено по решению контроллера. `НДПИ = база + 10% × max(gold_usd − 1900, 0)` в USD/oz. Вычитается в USD внутри USD-FCF. Рублёвая сумма = USD-сумма × `usdrub_path` того же периода, только для справки, в NAV не идёт.
- `skills/dcf-methodology/SKILL.md`: §3 переписан по этому правилу. §2 («Валюта») согласован: НДПИ_t в USD, перевод FCF_t по ключу периода, одно число NPV по ключу `gold_scenario.horizon`. §5 (индустриальный контур): «курс оценки» заменён на ключ `gold_scenario.horizon`.
- `skills/gold-forecast-session/SKILL.md`, шаг 5: правило перевода уточнено. Для периода t курс берётся по ключу этого периода, для единого числа run'а по ключу `gold_scenario.horizon`. НДПИ_t вычитается в USD, рублёвая сумма справочная.
- Прочее не менялось.
- Проверки: `python3 -m json.tool kimi.plugin.json` проходит.
- Commit gold-nav: `4b88571331bfe9135fef6a76c6145d8334e2f0ee` ("align ndpi currency in methodology").
