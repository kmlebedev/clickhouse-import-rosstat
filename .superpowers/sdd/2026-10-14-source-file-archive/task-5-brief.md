### Task 5: Режим `ARCHIVE_ACTION=registry`, Makefile, staging-env, git-зеркало

**Files:**
- Modify: `main.go`, `Makefile` (цель `import` ~строка 166, блок `env-check` 156–162), `deploy/staging/clickhouse-import-rosstat.env.example`
- Create: `docs/archive/README.md`

**Interfaces:**
- Consumes: `archive.FromEnv`, `archive.RegistryKey` (Task 1/3); `util.HttpClient` (существующий).
- Produces: env-режим `ARCHIVE_ACTION=registry` (без подключения к ClickHouse); `ARCHIVE_REGISTRY_OUT` (default `docs/archive/registry.jsonl`); цель `make archive-registry`.

- [ ] **Step 1: Реализовать режим в `main.go`**

В начале `main()`, до `clickhouse.ParseDSN`: если `ARCHIVE_ACTION == "registry"` — `runArchiveRegistry(ctx)` и выход. `runArchiveRegistry`: `archive.FromEnv(util.HttpClient)`; `store == nil` → ошибка «ARCHIVE_BACKEND не задан»; `Exists(RegistryKey)` → нет файла: `log.Warnf("реестр пуст")` и запись пустого файла; есть — `Get`; `os.MkdirAll` + `os.WriteFile(out, body, 0o644)`; лог: число строк и путь. Обычный прогон импортёров — без изменений.

- [ ] **Step 2: Makefile**

```make
# Git-зеркало реестра винтажного архива (make archive-registry [ARCHIVE_BACKEND=yandex]):
# скачивает registry.jsonl из хранилища в docs/archive/; коммит — ручной/периодический.
archive-registry:
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY)-native .
	ARCHIVE_ACTION=registry ./$(BUILD_DIR)/$(BINARY)-native
```

В `env-check` — строка вида остальных: `ARCHIVE_YANDEX_TOKEN задан / не задан (нужен при ARCHIVE_BACKEND=yandex)`; в `.PHONY` — `archive-registry`.

- [ ] **Step 3: Staging-env и README зеркала**

`deploy/staging/clickhouse-import-rosstat.env.example` — блок с комментариями: `ARCHIVE_BACKEND=yandex`, `ARCHIVE_YANDEX_TOKEN=REPLACE_ME` (значение — OAuth-токен приложения, scope `cloud_api:disk.write`), `ARCHIVE_YANDEX_DIR=disk:/rosstat-archive`; пометить, что токен обязателен при включённом бэкенде и что архив отключён при пустом `ARCHIVE_BACKEND`. `docs/archive/README.md` — что это зеркало реестра `registry.jsonl` из хранилища, что истина — файл на Диске, как обновлять (`make archive-registry`), что коммит периодический/ручной (на staging нет git-дерева, автокоммит невозможен), формат строки (перечень полей).

- [ ] **Step 4: Проверки**

Run: `gofmt -l main.go`, `go build ./...`, `go vet ./...`, `go test -race -count=1 ./archive/ ./util/`; `make archive-registry` без `ARCHIVE_BACKEND` → ненулевой код и понятная ошибка (проверить вручную, зафиксировать вывод); `make env-check` → строка `ARCHIVE_YANDEX_TOKEN` присутствует.

- [ ] **Step 5: Commit**

```bash
git add main.go Makefile deploy/staging/clickhouse-import-rosstat.env.example docs/archive/README.md
git commit -m "add archive registry mirror target and staging env"
```

---

