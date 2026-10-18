# Конфигурация и окружение

> Часть документации проекта; входная точка — [README.md](../README.md).

## Переменные окружения

| Переменная | Назначение |
|---|---|
| `CLICKHOUSE_URL` | DSN подключения (обязательна). Не выводится в логах |
| `CLICKHOUSE_IMPORT_STAT` | Фильтр импортёров через запятую по имени (`Name()`). Пусто — все импортёры |
| `LOG_LEVEL` | Уровень logrus: `debug`, `info`, `warn`, `error` |
| `CERT_FILES` | PEM-сертификаты через запятую (нац. УЦ РФ для gos-сайтов) |
| `BLS_API_KEY` | Регистрационный ключ BLS (необязателен; без него работает, но лимиты строже). Не выводится в логах |
| `BEA_API_KEY` | UserID BEA API (обязателен для `bea`; без него импортёр завершается ошибкой). Не выводится в логах |
| `EIA_API_KEY` | Ключ EIA API v2 (обязателен для `eia`; без него импортёр завершается ошибкой, ключ маскируется в текстах ошибок). Не выводится в логах |
| `LLM_BASE_URL`, `LLM_API_KEY`, `MP_SUMMARY_LLM_MODEL` | LLM-слой разметки каналов `cbr_mp_summary`: base URL Anthropic-совместимого API (**без** `/v1` — путь `v1/messages` дописывает официальный SDK `anthropic-sdk-go`, и `/v1` в base дал бы `/v1/v1/messages`; у DeepSeek такой base — `https://api.deepseek.com/anthropic`), ключ и имя модели. Нужны все три; пустая любая / отказ провайдера / невалидный JSON — `warn` и деградация до regex-слоя (импорт не падает, догон следующим прогоном). Значения — только в окружении (`~/.config/rosstat/env`), в репозиторий и логи не попадают: SDK шлёт ключ заголовком `x-api-key`, а не в URL/query |
| `LLM_MODEL` | Модель экспериментального LLM-прогноза ключевой ставки (`scripts/key_rate_llm_forecast.py`, цель `make key-rate-llm-forecast`, issue [#189](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/189)); работает вместе с общими `LLM_BASE_URL`/`LLM_API_KEY`, транспорт — OpenAI-совместимый `POST <base>/v1/chat/completions` (`temperature=0`, `max_tokens=8192`, `response_format=json_object` с фолбэком). Нужны все три; пустая любая — `MissingEnvError` с именами переменных и код 1 (ручной экспериментальный прогон). Значения — только в окружении, в репозиторий и логи не попадают. Скрипт в ClickHouse не пишет; `--log` готовит payload для ingest #127 |
| `NEWS_LLM_PROFILE` | Профиль LLM-разметки видимого потока новостей (`news_annotation`: Telegram и RSS): пусто/`default` — общие `LLM_BASE_URL`/`LLM_API_KEY`/`NEWS_LLM_MODEL` и Anthropic SDK; `glm` — `GLM_LLM_BASE_URL`/`GLM_LLM_API_KEY`/`GLM_LLM_MODEL` и OpenAI-совместимый `/v1/chat/completions` (`response_format=json_object`, `reasoning_effort=none`). Неизвестный профиль — warn и пропуск разметки |
| `NEWS_LLM_MODEL` | Модель LLM-разметки видимого потока новостей для профиля `default`; работает вместе с общими `LLM_BASE_URL` и `LLM_API_KEY` (base Anthropic-совместимого API **без** `/v1` — путь `v1/messages` дописывает SDK). Любая пустая / отказ провайдера / невалидный ответ после одного ретрая — `warn` и пропуск LLM-шага: импорт зелёный, разметка догоняется следующим прогоном. Фактическое имя модели пишется в `news_annotation.model`, редакция промпта (`newsPromptVersion = 2`) — в `prompt_version`; обе входят в ключ, поэтому смена модели или промпта не затирает старую разметку. Значения — только в окружении, в репозиторий и логи не попадают |
| `GLM_LLM_BASE_URL`, `GLM_LLM_API_KEY`, `GLM_LLM_MODEL` | Профиль `NEWS_LLM_PROFILE=glm`: base гейтвея Joingonka (без `/v1`; путь `/v1/chat/completions` добавляет транспорт), ключ и имя модели (`zai-org/GLM-5.3-Flash`). Ключ едет заголовком `Authorization: Bearer`, не в URL/query. Живой прогон 1.10 принят 2026-10-17 этим профилем (44/44 сообщения `v_news_flash`, 25 с тегами); RSS-разметка — пункт `phase-2-rss-llm-annotation` (2026-10-18, `prompt_version=2`, реестр 46 тегов). Отчёт сравнения — `docs/research/2026-10-17-mp-summary-llm-comparison.md` (пункт 1.8) |
| `NEWS_LLM_BACKLOG_LIMIT` | Предел кандидатов LLM-разметки за один прогон `news_watch`: целое 1…1000, по умолчанию 50. Штатно ограничивает стоимость после простоя; разовый бэкфилл — увеличенное значение (например, 500). Нечисло или значение вне диапазона — warn и 50 |
| `ML_LOG` | Включает запись прогона ML-эксперимента в ClickHouse: ровно `1` — `cpi_nowcast`/`key_rate_model`/`cpi_monthly` пишут паспорт в `ml_runs` и fold-предсказания в `ml_predictions`; пусто/иное — поведение скриптов побайтно прежнее (в ClickHouse не пишут). Если ClickHouse недоступен, прогон не падает: паспорт и предсказания остаются в локальном fallback `build/ml-runs/<run_id>/` |
| `ML_RUN_NAME` | Человекочитаемое имя прогона в `ml_runs.run_name`; пусто — имя по умолчанию из скрипта (`cpi_nowcast__start…__embargo…` / `key_rate_model__…` / `cpi_monthly__<sample>__start…__embargo…`). Действует только вместе с `ML_LOG=1` |
| `ML_LOCAL_DIR` | Каталог локального fallback журнала ML-экспериментов (по умолчанию `build/ml-runs`); внутри создаётся `<run_id>/run.json` + `predictions.jsonl`. Действует только вместе с `ML_LOG=1` |
| `INGEST_TOKEN` | Bearer-токен ingest-endpoint, обязателен для `cmd/ingest` (без него процесс не стартует). Не выводится в логах |
| `INGEST_ADDR` | Адрес HTTP-сервера `cmd/ingest`, по умолчанию `:8081` |
| `INGEST_RATE_PER_MIN` | Лимит запросов `cmd/ingest` в минуту (token bucket, burst 10), по умолчанию 60; превышение — `429` |
| `NEWS_WATCH_MODE` | Режим импортёра `news_watch`: пусто или `all` — полный upsert окна всех лент (ручная пересборка), `since` — режим расписания dagu: только отсутствующие в `news_events` позиции по ключу `(date, source, title)`, `check` — проверка дыр/окон без записи. Неизвестное значение — ошибка до сети и до БД |
| `EDISCLOSURE_INPUT_DIR` | Каталог снапшота коллектора `scripts/edisclosure_collect.py` — `manifest.json` + `documents.jsonl` (импортёр `edisclosure_documents`; по умолчанию `build/edisclosure`, то есть выход `make edisclosure-collect`). Коллектор пишет снапшот только локально; оператор копирует его на staging в `/root/dagu/build/edisclosure` — этот абсолютный путь задаёт шаг `dagu/edisclosure.yaml` (dagu запускает шаг в своём рабочем каталоге, поэтому относительный `build/edisclosure` там не находится). Снапшот читается и валидируется до сети и БД: ошибка любой строки — fail-fast без единого запроса. Отсутствие каталога — ошибка прогона, кроме `EDISCLOSURE_ALLOW_MISSING=1` (ставит только шаг dagu); существующий каталог без `manifest.json`/`documents.jsonl` или с битым файлом — всегда ошибка. Для `make mcp-check` приёмочный импорт запускайте с `ARCHIVE_BACKEND` (например, `local`): проверка строки витрины требует непустой `archive_key`, а без архива он пуст |
| `EDISCLOSURE_RECHECK` | Глубокая проверка содержимого документов e-disclosure: ровно `1` — скачать все документы снапшота заново и переписать строку при смене `sha256` или метаданных карточки **или** при пустом `archive_key` и включённом архиве (самолечение: строка, записанная без архива, получает версию в архиве); пусто/иное — обычный гейт по таблице `edisclosure_documents` (неизменившиеся документы не скачиваются). Лечит замену файла под тем же `fileid`, которую гейт по метаданным не видит; ручной/квартальный прогон |
| `EDISCLOSURE_ALLOW_MISSING` | Ровно `1` — отсутствие `EDISCLOSURE_INPUT_DIR` считается no-op (`0` документов) вместо ошибки; флаг ставит только шаг `dagu/edisclosure.yaml`, чтобы ежемесячное расписание не падало на хосте, куда коллектор ещё не доставил снапшот. Ручной `make import` флаг не ставит и падает громко |
| `EDISCLOSURE_OUT` | Каталог выхода коллектора `scripts/edisclosure_collect.py` (по умолчанию `build/edisclosure`); переопределяется ещё и флагом `--out`. Импортёром не читается: он берёт снапшот из `EDISCLOSURE_INPUT_DIR`, поэтому для нестандартного каталога задайте обе переменные |
| `ARCHIVE_BACKEND`, `ARCHIVE_YANDEX_TOKEN`, `ARCHIVE_YANDEX_DIR`, `ARCHIVE_LOCAL_DIR`, `ARCHIVE_ACTION`, `ARCHIVE_LIST_SOURCE`, `ARCHIVE_RECONCILE`, `ARCHIVE_REGISTRY_OUT`, `ARCHIVE_REGISTRY_ALLOW_EMPTY` | Винтажный архив версий файлов источников (пакет `archive/`, ARCHITECTURE §2–§3): `ARCHIVE_BACKEND` = `local` \| `yandex` \| пусто = архивация выключена; `ARCHIVE_YANDEX_DIR` — корень архива на Яндекс.Диске (по умолчанию `disk:/rosstat-archive`), `ARCHIVE_LOCAL_DIR` — корень локального бэкенда (dev-прогон, приёмка). `ARCHIVE_ACTION` = `registry` \| `list` \| `restore` (неизвестное значение — ошибка): `registry` — обновить git-зеркало реестра (`make archive-registry`; путь зеркала переопределяет `ARCHIVE_REGISTRY_OUT`; пустой реестр поверх непустого зеркала режим не пишет — затирание разрешает только `ARCHIVE_REGISTRY_ALLOW_EMPTY=1` с warn); `list` — напечатать версии реестра (`make archive-list`; фильтр `ARCHIVE_LIST_SOURCE=<источник>`, а с `ARCHIVE_RECONCILE=1` — сверка «объект без строки реестра», нужен бэкенд с листингом, сегодня `local`); `restore` — восстановление БД из архива (`make archive-restore STAT=<импортёр>`; процесс не завершается, а продолжает обычный путь импортёров, поэтому пустой `CLICKHOUSE_IMPORT_STAT` в этом режиме — ошибка). Токен — секрет: только в окружении, в репозиторий и логи не попадает; как получить и куда положить — «Ключи и окружение» |

Поведение при запуске: неизвестное имя в `CLICKHOUSE_IMPORT_STAT` даёт предупреждение; ошибка любого импортёра даёт код выхода `1`, остальные импортёры при этом выполняются.

Импортёр `financial` читает `TICKER`, `FINANCIAL_DATA_DIR`, `INVESTING_EMAIL`, `INVESTING_PASSWORD`; эти переменные устаревают вместе с legacy-контуром.

`DCF_RUN_ID` — переменная окружения **разового прогона** `dcf_engine`, а не строка конфигурации источника: UUID прогона, который уходит в `nav_by_asset.run_id` и связывает строки с записью `model_runs` (её создаёт ingest-endpoint). Передавайте её прямо в команде — `DCF_RUN_ID=$(uuidgen | tr A-Z a-z) make import STAT=dcf_engine`; в `~/.config/rosstat/env` её хранить не нужно. Без `DCF_RUN_ID` движок считает и печатает тело `POST /v1/model_run`, но не пишет ни строки (см. «Что импортируется»). Импортёр `dcf_mine_plans` переменных окружения не имеет (только `CLICKHOUSE_URL`).

Переменные стендов Grafana (`GRAFANA_TEST_URL`/`GRAFANA_TEST_TOKEN`, `GRAFANA_STAGING_URL`/`GRAFANA_STAGING_TOKEN`, `GRAFANA_ORG_ID`) к импортёрам не относятся — они нужны целям `make grafana-*` (дашборды как код, деплой и smoke). Значения живут в том же `~/.config/rosstat/env`, `make env-check` печатает только «задан/не задан»; описание — в разделе «Grafana».

## Ключи и окружение

Ключи и пароли лежат в одном файле вне репозитория: `~/.config/rosstat/env` (права `600`, каталог `700`). Формат:

```bash
export BEA_API_KEY=<your-key>
export EIA_API_KEY=<your-key>
# export GOST_APP_ID=<токен gtn.access.appid: JS-конфиг страницы реестра СБКТС, standarts.adminsHeaders>
export CLICKHOUSE_URL=clickhouse://localhost:9000/default
export CLICKHOUSE_PASSWORD=<kimi_reader-password>
# export CH_IMPORT_PASSWORD=<пароль пользователя, читающего сырые таблицы>
# export BLS_API_KEY=<your-key>
# export ARCHIVE_YANDEX_TOKEN=<OAuth-токен Яндекс.Диска: оба scope — cloud_api:disk.write и cloud_api:disk.read>
# ARCHIVE_BACKEND в этот файл НЕ добавлять: бэкенд выбирается аргументом make (см. «Винтажный архив»)
# export INGEST_TOKEN=<ingest-token>
# export INGEST_RATE_PER_MIN=60
# export GRAFANA_TEST_TOKEN=<токен сервис-аккаунта Editor локального стенда :3000>
# export GRAFANA_STAGING_TOKEN=<токен сервис-аккаунта Editor на staging, доступ через туннель :3001>
# export GRAFANA_ORG_ID=1
```

- `CLICKHOUSE_URL` — **основной** способ задать адрес: это DSN, из которого Go-код (`clickhouse.ParseDSN`) и `scripts/extrapolation_aisc.py` берут хост, порт, пользователя и базу. Дискретные `CLICKHOUSE_HOST`/`CLICKHOUSE_PORT`/`CLICKHOUSE_USER`/`CLICKHOUSE_DATABASE` в файле обычно **не задаются**; скрипт читает их как переопределение поверх DSN;
- `CLICKHOUSE_PASSWORD` — пароль **`kimi_reader`**, read-only пользователя витрин (`make mcp-check`). Сырые таблицы ему закрыты: чтение `databook_polyus` и подобных идёт под другим пользователем, поэтому у скриптов есть **`CH_IMPORT_PASSWORD`**. Если пользователь из DSN пароля не имеет (локальный `default`), переменную задавать не нужно — подставлять вместо неё `CLICKHOUSE_PASSWORD` нельзя, это чужой пароль и он даёт 401.

- Makefile подключает файл сам (`-include`), поэтому любая `make`-цель видит ключи без `export` в shell; путь можно переопределить: `make ROSSTAT_ENV=/путь/к/файлу ...`;
- в терминале: `set -a; . ~/.config/rosstat/env; set +a`;
- проверка без вывода значений: `make env-check`;
- локальный импорт одного источника: `make import STAT=bea` (собирает бинарник под macOS в `build/clickhouse-import-rosstat-native`);
- Kimi Code: ключи для MCP задаются в `~/.kimi-code/mcp.json` (блок `env`), для остальных команд Kimi вызывает `make`, поэтому файл окружения подхватывается автоматически;
- Dagu (расписания): секреты описаны в `~/dagu-dev/base.yaml` через provider `file`, каждый ключ — отдельный файл `~/dagu-dev/secrets/<name>` (`600`). Для BEA: `bea_api_key`; для EIA: `eia_api_key`; при добавлении ключа нового импортёра — новый файл и строка в `secrets:`;
- файлы `*.env`, `secrets/` и ключи в коде не коммитить; `CLICKHOUSE_PASSWORD` только в файле окружения (`~/.config/rosstat/env`), в `mcp.json` его нет.

## MCP для Kimi

MCP для Kimi (агент читает витрины `v_*`, без записи):

- установить `uv`: `brew install uv`;
- создать пользователя и выдать права: `make mcp-user` (берёт `CLICKHOUSE_PASSWORD` из файла окружения, выполняет `sql/mcp_kimi_reader.sql`; пароль в выводе не печатается; повторный запуск не меняет уже созданного пользователя);
- записать `~/.kimi-code/mcp.json` (права `600`); пароль в файл не пишется, команда сама подгружает `~/.config/rosstat/env`:

```json
{
  "mcpServers": {
    "clickhouse": {
      "command": "bash",
      "args": ["-c", "set -a; . \"$HOME/.config/rosstat/env\"; set +a; exec uv run --with mcp-clickhouse --python 3.12 mcp-clickhouse"],
      "env": {
        "CLICKHOUSE_HOST": "localhost",
        "CLICKHOUSE_PORT": "8123",
        "CLICKHOUSE_SECURE": "false",
        "CLICKHOUSE_VERIFY": "false",
        "CLICKHOUSE_USER": "kimi_reader",
        "CLICKHOUSE_DATABASE": "default",
        "CLICKHOUSE_MCP_SERVER_TRANSPORT": "stdio"
      }
    }
  }
}
```

- проверить в Kimi командой `/mcp-config` в новой сессии: сервер `clickhouse` должен быть подключён;
- новые ряды и витрины: см. AGENTS.md, правило 11. Legacy-таблицы `cbr_*`, `rosstat`, `minfin`, `gold_prices` агенту доступны через групповые витрины `v_cbr_macro`, `v_rosstat_macro`, `v_minfin_budget`, `v_gold_prices` (создаются, когда все таблицы группы импортированы), описания рядов — в `v_series_catalog`. Витрины фазы 3 (`v_gold_forecasts`, `v_gold_etf_flows`), wgc-контура (`v_cb_gold_dynamics`) и контура новостей (`v_news_events`, `v_news_flash`, `v_news_message_versions`, `v_news_tags`; сырые `news_events`, `news_message_version` и `news_annotation` агенту не выданы) выданы тем же грантом в `sql/mcp_kimi_reader.sql`, как и витрины контура ключевой ставки `v_cbr_survey` (мониторинг предприятий ЦБ), `v_cbr_mo_br` (макроэкономический опрос ЦБ — консенсус аналитиков), `v_cbr_mp_summary`/`v_cbr_mp_summary_blocks`/`v_cbr_mp_summary_channels` (резюме обсуждения ключевой ставки: шапка решения, дословный текст и разметка 11 каналов; сырые `cbr_mp_summary`, `cbr_mp_summary_block` и `cbr_mp_summary_channel`, `macro_series` и служебная `source_publications` агенту не выданы) и `v_cbr_bop` (платёжный баланс РФ двумя выгрузками ЦБ в одной long-форме: `source` — имя таблицы, единицы различаются, ноль — значение; сырые `cbr_bop_est` и `cbr_bop_standard` агенту **не выданы**: агент читает ряды только через витрину, а единицы, знак и привязку даты — в `v_series_catalog` по `(source, series)`), и `v_cbr_dip_bulletin` (приложения ДИП в long-форме с винтажными колонками: агент читает точку как «значение, каким его опубликовал выпуск `release` от `publication_date`», и один месяц лежит в ней столько раз, сколько выпусков его напечатало; сырая `cbr_dip_bulletin` агенту **не выдана**: доступ даёт только витрина, а каталог 8 рядов — в `v_series_catalog` по `(source, series)`), и `v_cbr_mt_forecast` (диапазоны среднесрочного прогноза ЦБ dedicated-колонками `meeting_date, release_date, url, indicator, year, lo, hi`: агент читает строку как «диапазон `lo`–`hi` прогноза на год `year`, каким его зафиксировал выпуск `release_date`», `lo = hi` — точечный прогноз; сырая `cbr_mt_forecast` агенту **не выдана**: доступ даёт только витрина, а каталог 16 рядов — в `v_series_catalog` по `(source, series)`); их чтение, каталоги `webbridge` (20 рядов), `wgc` (34 ряда), `cbr_survey` (13 рядов), `cbr_mo_br`, `cbr_bop_est` (51 ряд), `cbr_bop_standard` (34 ряда), `cbr_dip_bulletin` (8 рядов) и `cbr_mt_forecast` (16 рядов) и запрет сырых `gold_forecasts`, `news_events`, `news_message_version`, `news_annotation`, `cbr_bop_est`/`cbr_bop_standard`, `cbr_dip_bulletin`, `cbr_mt_forecast` и `cbr_mp_summary*` проверяет `make mcp-check`.

## MCP GoLand

MCP GoLand (опционально, ускоряет работу Kimi Code с кодом):

- зачем: агент получает инструменты IDE — ошибки и инспекции файла по индексу GoLand (`get_file_problems`), сигнатуры символов (`get_symbol_info`), семантический поиск и иерархию вызовов (`search_symbol`, `analyze_calls`), безопасный rename (`rename_refactoring`); как и когда ими пользоваться — в AGENTS.md, раздел «Инструменты GoLand (MCP `jetbrains`)». Без них агент работает обычными `Grep`/`Read`/`Edit`, настройка не обязательна;
- требование: GoLand 2025.2 или новее — MCP-сервер встроен (плагин MCP Server включён по умолчанию); npm-пакет `@jetbrains/mcp-proxy` deprecated и не нужен;
- настройка:
  1. открыть этот репозиторий в GoLand;
  2. Settings | Tools | MCP Server → включить **Enable MCP Server**;
  3. там же: **Copy HTTP Stream Config** — в буфере окажется URL вида `http://127.0.0.1:<port>/stream`;
  4. добавить запись в `~/.kimi-code/mcp.json` (URL из шага 3, порт подставить свой):

  ```json
  {
    "mcpServers": {
      "jetbrains": {
        "url": "http://127.0.0.1:<port>/stream"
      }
    }
  }
  ```

  5. начать новую сессию Kimi Code (`/new`) и проверить `/mcp-config`: сервер `jetbrains` подключён;
- порт выдаётся динамически и может смениться после перезапуска GoLand: если инструменты `jetbrains` пропали, повторите шаги 3–4;
- инструменты видны только пока GoLand запущен с открытым проектом.
