### Task 3: Реестр `registry.jsonl`

**Files:**
- Create: `archive/registry.go`
- Test: `archive/registry_test.go`

**Interfaces:**
- Consumes: `Store` (Task 1), `LocalStore` (Task 1).
- Produces: `RegistryKey = "registry.jsonl"`; `Entry{Source, Name, Fingerprint, SHA256 string; Size int64; URL, Key string; FirstSeen time.Time}` с JSON-тегами `source,name,fingerprint,sha256,size,url,key,first_seen`; `NewRegistry(store Store) *Registry`; `(*Registry).Ensure(ctx context.Context, e Entry) error`; `(*Registry).Entries() []Entry` (копия).

- [ ] **Step 1: Написать падающие тесты**

- `TestRegistryEnsureCreatesMissingFile` — пустое хранилище: `Ensure` создаёт `registry.jsonl` с одной строкой; формат — одна JSON-строка с `\n`, все восемь полей (Review Focus №1).
- `TestRegistryEnsureIsIdempotent` — повторный `Ensure` той же записи (тот же `Key`, тот же `SHA256`) не меняет байты файла (сравнить содержимое до/после).
- `TestRegistryEnsureReplacesChangedContent` — тот же `Key`, другой `SHA256` → строка заменена, число строк не выросло.
- `TestRegistryAppendsNewKeys` — второй ключ добавляется строкой ниже, порядок — по добавлению.
- `TestRegistryEntriesReturnsCopy` — мутация результата `Entries()` не меняет состояние реестра.

- [ ] **Step 2: Проверить, что тесты падают**

Run: `go test -run TestRegistry -count=1 ./archive/` — Expected: FAIL (`undefined: NewRegistry`).

- [ ] **Step 3: Реализовать `archive/registry.go`**

Ленивое чтение: при первом `Ensure` — `Exists`, затем `Get`; ошибка чтения (не «нет файла») — ошибка; битая строка JSONL — ошибка (реестр — источник истины, молча терять строки нельзя). Запись — `json.Marshal` каждой строки + `\n`, один `Put` всего файла; мьютекс на процесс (штатный писатель один — последовательная цепочка dagu; конкурентный ручной прогон самолечится префлайтом, спека §3.4).

- [ ] **Step 4: Прогнать пакет**

Run: `go test -race -count=1 ./archive/` — Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add archive/registry.go archive/registry_test.go
git commit -m "add archive registry with jsonl upsert"
```

---

