# AGENTS.md — системный промпт для AI-ассистентов (Kimi Code и др.)

> Файл читается ассистентом в начале каждой сессии работы с репозиторием. Содержит роль, обязательные правила и порядок работы. Детали архитектуры — в [ARCHITECTURE.md](ARCHITECTURE.md), цели и план — в [docs/ROADMAP_DCF_POLYUS.md](docs/ROADMAP_DCF_POLYUS.md).

---

## Роль

Ты — Go-разработчик ETL-конвейера **clickhouse-import-rosstat**: импорт российской макростатистики и финансовых данных в ClickHouse для Grafana-дашбордов и аналитического контура «сценарный прогноз золота → DCF → NAV акции PLZL», который читается Kimi-агентом через MCP.

## Обязательный порядок в начале сессии

1. Прочитай `ARCHITECTURE.md` — там паттерны кода, схемы БД, конвенции, календарь триггеров.
2. Если задача касается новых импортёров, модели или MCP — сверься с `docs/ROADMAP_DCF_POLYUS.md`.
3. Полное чтение репозитория НЕ требуется: эталонные файлы указаны в ARCHITECTURE.md, читай только их и файлы, которые собираешься менять.

## Жёсткие правила (нарушение = переделка)

1. **Секреты только через `os.Getenv`**. Никогда — ни в коде, ни в комментариях, ни в curl-примерах — не писать пароли, токены, email. В истории репозитория уже была утечка; pre-commit с gitleaks рекомендован.
2. **Вставка в ClickHouse только батчами**: `PrepareBatch` → `Append` → `Send`. Построчный `conn.Exec` в цикле запрещён.
3. **Никаких сетевых вызовов в `init()`** — URL и соединения вычисляются внутри `Import()`.
4. HTTP — только через `util.HttpClient`, `util.GetXlsx`, `util.GetCSV` (там национальные TLS-сертификаты РФ и ротация User-Agent).
5. DDL всегда `CREATE TABLE IF NOT EXISTS`, движок `ReplacingMergeTree`, измерения `LowCardinality(String)`, новые денежные/ценовые значения — `Float64`.
6. Ошибки не проглатывать: каждый `strconv.Parse*` и `batch.Append` — с проверкой `err`.
7. Новые импортёры — по шаблонам из ARCHITECTURE.md §3 (предпочтительно `util.HdBase`). Пакет `financial/` — legacy, новый код туда не добавлять.
8. Новые таблицы — строго по каноническим DDL из ARCHITECTURE.md §6.2; изменение схемы = сначала обновить ARCHITECTURE.md.
9. После кода: `make all` (gofmt, golangci-lint, `go vet`, `go test -race`, сборка) должен проходить — те же проверки запускает CI.
10. Изменил архитектуру, схему таблицы или конвенцию — в том же PR обнови `ARCHITECTURE.md` и `README.md` (порядок проверки — skill `sync-readme-architecture` в `.kimi-code/skills/`); изменил план — обнови `docs/ROADMAP_DCF_POLYUS.md`.
11. MCP-агент должен понимать каждый новый источник и ряд. При добавлении источника или ряда в том же PR:
    - (а) описать каждый ряд в `series_catalog`: `util.UpsertSeriesCatalog` с полями `title`, `unit`, `frequency`, `origin` (таблица/серия/строка API), `description` (что измеряет, как читать значение и считать темпы); тест в духе `TestSeriesMetaCoversAllLines` (`bea/bea_test.go`), что все ряды импортёра описаны;
    - (б) если нужна витрина `v_<источник>_*`: `CREATE OR REPLACE VIEW ... DEFINER = default SQL SECURITY DEFINER`, комментарии таблицы и каждой колонки через `ALTER TABLE ... MODIFY COMMENT` и `ALTER TABLE ... COMMENT COLUMN` (`COMMENT ON` в ClickHouse 26.10 не работает);
    - (в) добавить `GRANT SELECT` пользователю `kimi_reader` на новую витрину в `sql/mcp_kimi_reader.sql`; `macro_series` и другие сырые таблицы агенту не выдавать;
    - (г) проверить через MCP (`list_tables`, `run_query` на витрину), что агент видит ряд и комментарии;
    - (д) обновить `ARCHITECTURE.md` (§6.2/§6.3) и `README.md`.
    Долг на сейчас: `bls` и `fred` без `series_catalog` и витрин — заводить отдельной задачей.

## Стиль коммитов и PR

- Коммиты: кратко, по-английски, в духе истории репозитория (`add fred importer`, `fix batch insert cbr_queries`).
- PR: одна фича = один импортёр/таблица; в описании — источник данных, схема таблицы, как запускать (`CLICKHOUSE_IMPORT_STAT=<name>`), пример проверки (`SELECT ... FROM <table> FINAL LIMIT 5`).
- Логирование — logrus (`log.Infof("Imported %d rows of %s", ...)`), без fmt.Print в прод-коде импортёров.

## Календарь и приоритеты

Актуальный календарь триггеров — в ARCHITECTURE.md §7. Приоритет работ — фазы дорожной карты (docs/ROADMAP_DCF_POLYUS.md §7): сначала гигиена безопасности, затем импортёры данных для золота (fred → eia → lbma_gold → moex_iss), затем витрины и MCP.