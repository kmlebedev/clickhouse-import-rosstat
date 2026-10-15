### Task 1: Исправить `main.go` (код выхода, фильтр, ParseDSN)

**Files:**
- Modify: `main.go:25-48`

**Interfaces:**
- Consumes: `chimport.Stats []ImportStat`, `ImportStat.Name() string`, `ImportStat.Import(ctx, conn) (int64, error)`.
- Produces: нет (точка входа).

- [ ] **Step 1: Зафиксировать базовую сборку**

Run: `go build ./... && go vet ./...`
Expected: без ошибок.

- [ ] **Step 2: Реализовать фильтр**

Разбор `CLICKHOUSE_IMPORT_STAT`: `strings.Split` по `,`, каждый элемент обрезать `strings.TrimSpace`, пустые отбрасывать. Если список пуст, запускаются все импортёры. Если имя из списка не совпало ни с одним `Name()`, вызвать `log.Warnf("no importer named %q", name)`. Код выхода при этом не меняется.

- [ ] **Step 3: Реализовать код выхода и ParseDSN**

- Завести флаг `failed`. При `err != nil` от `stat.Import` выставлять его, продолжать цикл. После цикла, если `failed`, вызвать `os.Exit(1)`.
- Ошибку `clickhouse.ParseDSN` проверять. При ошибке вызывать `log.Fatal("invalid CLICKHOUSE_URL")` с фиксированным текстом, без значения DSN.
- Существующие `log.Errorf` и `log.Infof` оставить.

- [ ] **Step 4: Проверить поведение**

Выполнить после `make build`:

1. `CLICKHOUSE_URL=invalid ./build/clickhouse-import-rosstat; echo $?` — ожидается ненулевой код, в выводе нет строки DSN.
2. Если ClickHouse доступен: `CLICKHOUSE_IMPORT_STAT=` — запускаются все импортёры (в логе видны `Imported ... rows of ...` для каждого).
3. Если ClickHouse доступен: `CLICKHOUSE_IMPORT_STAT=no_such_table ./build/clickhouse-import-rosstat; echo $?` — в логе есть предупреждение `no importer named`, код `0`.
4. Падение одного импортёра: при недоступном источнике (например, `CERT_FILES` с несуществующим путём, если импортёр его использует) и `CLICKHOUSE_IMPORT_STAT` с двумя именами — код `1`, второй импортёр выполнен.

Expected: все четыре пункта соответствуют описанию.

- [ ] **Step 5: Сборка**

Run: `go build ./... && go vet ./...`
Expected: без ошибок.

