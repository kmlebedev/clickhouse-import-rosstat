# Staging: паритет MCP-витрин и уведомления dagu — дизайн

Issue: [#384](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/384), пункт `infra/1.7`.

## 1. Проблема

На staging (`palmshell`) MCP-агент видит 3 витрины вместо контрактных 82. В ClickHouse хоста 25 базовых таблиц и 4 view; `kimi_reader` имеет гранты на 58 объектов, но большинства из них уже нет. Dagu при этом рапортует `succeeded` за 0 секунд, а `.out`/`.err` шагов — 0 байт.

## 2. Причина

`/root/.config/dagu/base.yaml` — дефолтный файл dagu без секции `env`. Dagu не передаёт шагам окружение systemd, поэтому `run: "$ROSSTAT_IMPORT_BIN"` раскрывается в пустую команду. Пустая команда завершается кодом 0, DAG получает `succeeded`, импорт не выполняется. Это тихий отказ, а не ошибка источника.

Второй разрыв: `deploy/staging/` не содержит base.yaml, поэтому `make deploy-staging` не может исправить или воспроизвести конфигурацию; `docs/OPERATIONS.md` при этом утверждает, что `ROSSTAT_IMPORT_BIN` и `CLICKHOUSE_URL` задаются в base.yaml.

Третий разрыв: уведомлений нет. В base.yaml нет `smtp`/`mail_on`/`error_mail`, в Grafana — только ML-правила без contact point. О падении импорта владелец не узнаёт.

## 3. Решение

### 3.1. Base DAG как deploy-артефакт

`deploy/staging/dagu-base.yaml` — шаблон base DAG. Содержит:

- текущие дефолты dagu (`type: graph`, `max_active_steps`, `catchup_window`, `log_output`, retention);
- `env` со ссылками `${…}` на переменные `/etc/clickhouse-import-rosstat.env`: `ROSSTAT_IMPORT_BIN`, `CLICKHOUSE_URL`, ключи BEA/BLS/EIA, GOST, LLM, архив;
- `CERT_FILES` — три российских CA (`russian_trusted_*.crt`) плюс промежуточный `litessl_rsa_ca_2025.pem` для `sberindex.ru`; `deploy-staging-dagu-base` копирует их в `/root/dagu/certs/`. Без первых `rosstat.gov.ru` отдаёт `x509: certificate signed by unknown authority`, без LiteSSL — `sber_consumper_spending_index` (живые сбои 2026-10-25);
- `smtp`, `error_mail`, `mail_on.failure: true` — нативные письма dagu при падении DAG.

Секретов в файле нет. `make deploy-staging-dagu-base` устанавливает его идемпотентно (sha256), делает `.bak` и перезапускает dagu, только если сервис уже активен (при первом deploy сервис ещё не включён — порядок `deploy-staging` не нарушается).

`deploy-staging-data` копирует датапаки, без которых падают `databook_polyus` и `databook_ugk`: `polyus/data/polyus_datapack_fy2025_new.xlsx` и `financial/data/ЮГК_databook_2024.xlsx` — в `/root/...` и `/root/dagu/...` (CLI-прогон и сервисный `WorkingDirectory`). Оба файла — артефакты репозитория, не секреты.

### 3.2. Уведомления

SMTP-параметры — в env-файле: `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`, `MAIL_FROM`, `MAIL_TO` (адрес шлюза CMC email→SMS). Dagu 2.18.2 раскрывает `${…}` в env шагов, но **не в smtp-полях**: `deploy/staging/render-dagu-base.py` при установке подставляет литералы из env-файла на хосте, поэтому секреты не попадают ни в git, ни в шаблон. `error_mail.attach_logs: true` прикладывает логи шага. Проверка — отдельная цель `deploy-staging-notify-test` с заведомо падающим DAG; в каждый deploy она не входит. Живой тест с локальным SMTP-sink подтвердил письмо `[DAGU palmshell]` (осталась реальная доставка через шлюз CMC).

### 3.3. Защита от регрессии

- `deploy-staging-preflight` проверяет непустые SMTP-переменные и наличие `ROSSTAT_IMPORT_BIN` в установленном base.yaml (значения не печатаются).
- `deploy-staging-smoke` запускает временный DAG `check_env` с `test -n "$ROSSTAT_IMPORT_BIN" && test -n "$CLICKHOUSE_URL" && echo ENV_OK`: если окружение снова перестанет доходить до шага, smoke падает, а не рапортует успех.

### 3.4. Паритет витрин

Цель — 83 витрины: 82 контракта рабочего дерева (`sql/mcp_kimi_reader.sql`; в `main` на момент ветки 77 грантов — `domrf`/`weekly_cpi_sa` доезжают отдельными PR) + `v_edisclosure_documents` (грант в dev есть, в контракт не внесён — исправляется этим PR). `v_ofz_issue_analytics` — артефакт ветки OFZ, вне контракта; `v_vacancies` — legacy staging-only, не удаляется без отдельного решения.

Витрины, которые нельзя получить импортом (ML-реестр, live-точность, forecast/DCF-артефакты), допускаются schema-only: существование, `DESCRIBE`, `SHOW CREATE VIEW`, грант. Это фиксируется в отчёте, а не выдаётся за data-паритет.

Отдельные известные исключения (source drift, не регрессия staging):

- `rosstat_labour_force_mes` — страница `rosstat.gov.ru/labour_force` сменила ссылку (`trud-1_15-s.xlsx` → `trud_1_15-72.xlsx`), импортёр падает до записи; issue [#337](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/337). Ряд в `v_rosstat_macro` отсутствует до закрытия.
- `fedstat_weekly_prices` — источник отдал 60 568 точек против 56 133 в гейте покрытия, импортёр отказался писать (`Imported 0 rows`); issue [#387](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/387). Витрина `v_fedstat_weekly_prices` до закрытия может быть пустой; остальные блоки ЕМИСС импортируются штатно.
- `v_eia_petroleum`/`v_bls_macro` — HTTP 403 с IP palmshell при тех же ключах, что на dev (хэши `EIA_API_KEY`/`BLS_API_KEY` совпали). Витрины созданы, но пусты; это блокировка источника по IP/UA, не конфигурация staging.
- `rosstat_ipc_weeks_bulletins` — 177 из 502 выпусков: часть Wayback-снимков 2009 не отдаёт `title`, импортёр завершает прогон ошибкой, уже записанные 10 114 строк остаются. Из-за недобора входного ряда `weekly_cpi_sa` отказывается считать («точек входа 506, ожидалось не меньше 760»), поэтому `v_weekly_cpi_sa` отсутствует; повторный прогон bulletins — отдельная задача.
- `domrf_mortgage_market` — антибот ServicePipe отдаёт JS-оболочку вместо страницы; `v_domrf_price_index` импортирован (20 700 точек), `v_domrf_mortgage` может быть неполной. Это известное ограничение источника (ARCHITECTURE §6.1).
- `news_watch` — таймауты `t.me` с palmshell; 115 строк записано, LLM-разметка пропущена (`NEWS_LLM_MODEL` не задан). Витрины новостей созданы, но без новых Telegram-позиций.
- `polyus_financial_metrics` — 9 PDF-отчётов не скачались/не разобрались; `v_company_financials` частична. Датапаки Polyus/ЮГК теперь копирует `deploy-staging-data`.
- `v_imf_macro` — ручной ряд через ingest (`source='imf'`), импортёра в DAG нет; витрина schema-only.

## 4. Границы

- Не меняем dev-данные: аудит dev — read-only.
- Не трогаем ClickHouse/Grafana-сервисы хоста и не делаем `apt upgrade`.
- Не добавляем systemd `OnFailure`/Grafana-heartbeat для самого процесса dagu — отдельный пункт (остаточный риск).
- Не переносим MLflow-артефакты и ML-прогоны между dev и staging.

## 5. Приёмка

1. Все 83 витрины существуют на staging, `DESCRIBE`/`SHOW CREATE VIEW` совпадают с dev, гранты `kimi_reader` выданы, MCP `list_tables`/`run_query` работают.
2. Реальный прогон dagu пишет `Imported …` (не 0 байт) и меняет данные.
3. Контролируемое падение DAG присылает письмо на шлюз CMC; владелец подтверждает SMS.
4. `make all`, `make docs-check`, `dagu validate` — зелёные.
5. `make deploy-staging-dagu-base` идемпотентен: повторный прогон не трогает файл.

## 6. Риски

- Ошибка в base.yaml ломает планировщик → `.bak` + `systemctl restart dagu`.
- Импорт внешних источников долгий/нестабильный → прогон по DAG, продолжение после падения, точечный повтор.
- SMTP-шлюз может отклонять письма → тест до массового прогона; при отказе уведомления не считаются принятыми.
- `attach_logs` может приложить лог с чувствительными данными → логи импортёров не печатают секреты; тестовое письмо проверяется владельцем.
