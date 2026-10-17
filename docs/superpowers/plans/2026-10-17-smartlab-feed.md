# smartlab-feed Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Две ленты smart-lab (`KonstantinLebedev`, `spydell-telegram`) собираются `news_watch` под `source='smartlab'`; полный текст поста хранится в новой колонке `news_events.description` (с идемпотентной миграцией существующей таблицы) и виден агенту через `v_news_events`; фильтр витрины расширен стемами ставки.

**Architecture:** Additive-изменение контура `news/`: список лент 5 → 7 (обе ленты smart-lab под общим `source='smartlab'`, различает `url` — конвенция двух фидов ЦБ под `'cbr'`); `description` разбирается RSS-парсером и пишется в `news_events`; канонический DDL §6.2 ARCHITECTURE.md получает колонку, существующая таблица расширяется `ALTER … ADD COLUMN IF NOT EXISTS` (паттерн `gostSchemaStatements`), `INSERT` переходит на явный список колонок.

**Tech Stack:** Go, `clickhouse-go/v2`, `encoding/xml`, RE2 (фильтр витрины), ClickHouse `ReplacingMergeTree`, `dagu`, Python (`scripts/mcp_check.py`).

**Spec:** `docs/superpowers/specs/2026-10-17-smartlab-feed-design.md`

## Global Constraints

- Правила AGENTS.md: запись только батчем `PrepareBatch → Append → Send`; сетевых вызовов в `init()` нет; HTTP — только через `util`; каждый `Append`/`Parse*` — с проверкой `err`; DDL — `CREATE TABLE IF NOT EXISTS`; изменение схемы — сначала канон `ARCHITECTURE.md` §6.2, потом копия.
- Тесты — офлайн (без сети и БД): ленты подменяются `httptest`, соединение — фейком `newsFakeConn`.
- URL-ы лент — пакетные переменные (подмена в тестах); порядок лент — контракт лога (`feeds()`).
- Фикстуры — реальные окна лент (обрезанные), даты `+0300`; пост `/blog/1363355.php` обязан быть в spydell-фикстуре.
- Команды с окружением — только через `make`; секреты не печатать.
- Коммиты: кратко, по-английски, в стиле истории репозитория.

## Review Focus

1. **Лента без подмены URL в офлайн-тестах уходит в сеть** (тихий флейк вместо падения): пин — guard внутри `pointFeedsAt` (каждое `feeds()` URL обязано начинаться с `srv.URL`) + счётчик хитов в тесте неверного режима (Task 1).
2. **Позиционный INSERT после миграции живой таблицы ломается** (у старой таблицы колонка физически в конце): пин — явный список колонок в `newsInsert` и тест `inserted()` на 6 значений (Task 2).
3. **Отсутствующий/пустой `description` роняет позицию или ленту**: пин — расширенный `TestItemsFromDocSkipsInvalidItems` (валидная позиция с пустым описанием остаётся; описание тримится) (Task 2).
4. **Фикстура spydell потеряла пост-пример при обрезании**: пин — в фикстуре есть item с URL `/blog/1363355.php` и непустым `Description` (Task 2).
5. **Расширение фильтра теряет существующие темы** (золотой контур): пин — негативные кейсы остаются вне, позитивные (наличные/вклады) входят (Task 3).

---

### Task 1: Ленты smart-lab и фикстуры

**Files:**
- Modify: `news/news.go:28-48`
- Create: `news/testdata/smartlab_lebedev.rss`, `news/testdata/smartlab_spydell.rss`
- Modify: `news/news_test.go` (`pointFeedsAt` ~625, `TestImportRejectsUnknownModeBeforeDB` ~50, `TestFeedsHaveFixtures` ~207, пины порядка ~198)

**Interfaces:**
- Consumes: существующие `feed`-структура и `feeds()` (`news/news.go`).
- Produces: переменные `feedSmartlabLebedevURL`, `feedSmartlabSpydellURL`; строки `feeds()`: `{Key: "smartlab_lebedev", Source: "smartlab", URL: feedSmartlabLebedevURL}` и `{Key: "smartlab_spydell", Source: "smartlab", URL: feedSmartlabSpydellURL}` — после `tass`, порядок фиксирован; фикстуры с ключами лент.

- [ ] **Step 1: Обновить тесты (падающие)**

В `news/news_test.go`:
- `pointFeedsAt`: 5 переменных → 7 (`old := [7]string{…, feedSmartlabLebedevURL, feedSmartlabSpydellURL}`, подставить `srv.URL + "/smartlab_lebedev"` и `+ "/smartlab_spydell"`, восстановить оба). В конце хелпера — guard:
```go
for _, f := range feeds() {
    if !strings.HasPrefix(f.URL, srv.URL) {
        t.Fatalf("лента %s не подменена (%q): офлайн-тест ушёл бы в сеть", f.Key, f.URL)
    }
}
```
- `TestImportRejectsUnknownModeBeforeDB`: список из 5 старых переменных → 7 (сохранить/подставить/восстановить обе новые).
- `TestFeedsHaveFixtures`: `want 5` → `7`.
- Пины порядка в `TestFetchFeedsUsesFixturesAndIsolatesFailure`: ключи `…,tass,smartlab_lebedev,smartlab_spydell`; метки `…,tass,smartlab,smartlab`.

- [ ] **Step 2: Убедиться, что тесты падают**

Run: `go test ./news/ -run 'TestFeedsHaveFixtures|TestFetchFeedsUsesFixturesAndIsolatesFailure' -v`
Expected: FAIL — фикстур smartlab нет / `feeds() = 5 лент`.

- [ ] **Step 3: Создать фикстуры из живых лент**

```bash
curl -sS 'https://smart-lab.ru/rss/personal_blog/KonstantinLebedev/' -o /tmp/smartlab_lebedev.rss
curl -sS 'https://smart-lab.ru/rss/personal_blog/spydell-telegram/' -o /tmp/smartlab_spydell.rss
python3 - <<'EOF'
for src, dst in [("/tmp/smartlab_lebedev.rss", "news/testdata/smartlab_lebedev.rss"),
                 ("/tmp/smartlab_spydell.rss", "news/testdata/smartlab_spydell.rss")]:
    data = open(src, encoding="utf-8").read()
    pos = 0
    for _ in range(3):
        pos = data.index("</item>", pos) + len("</item>")
    with open(dst, "w", encoding="utf-8") as f:
        f.write(data[:pos] + "</channel></rss>\n")
EOF
```
Обе фикстуры — первые 3 item-а целиком (полные `description`, шапка канала как есть). У spydell первый item — пост `/blog/1363355.php`.

- [ ] **Step 4: Добавить ленты в `news/news.go`**

Объявить переменные рядом с существующими:
```go
feedSmartlabLebedevURL = "https://smart-lab.ru/rss/personal_blog/KonstantinLebedev/"
feedSmartlabSpydellURL = "https://smart-lab.ru/rss/personal_blog/spydell-telegram/"
```
В `feeds()` — две строки после `tass`; комментарий `feeds()` дополнить: обе ленты smart-lab пишутся под `Source: "smartlab"`, различает `url` (как у двух лент ЦБ).

- [ ] **Step 5: Прогнать тесты**

Run: `go test ./news/ -race`
Expected: PASS. Проверить в логе, что guard из Step 1 не срабатывает.

- [ ] **Step 6: Commit**

```bash
git add news/news.go news/news_test.go news/testdata/smartlab_lebedev.rss news/testdata/smartlab_spydell.rss
git commit -m "news: add smart-lab feeds with fixtures (key rate phase 1.4)"
```

---

### Task 2: `description` — разбор, схема, запись

**Files:**
- Modify: `news/parse.go`, `news/schema.go`, `news/news.go`, `news/parse_test.go`, `news/schema_test.go`, `news/news_test.go`
- Modify: `ARCHITECTURE.md` (§6.2, блок `news_events` — канон раньше копии)

**Interfaces:**
- Consumes: `feedSmartlab*URL`, фикстуры (Task 1); `rssItem`/`newsItem`/`insertItems`/`newsViewStatements` (текущие).
- Produces:
  - `newsItem.Description string` (порядок полей: Date, Source, Title, URL, Description);
  - `newsAlterStatements[]string` = один стейтмент `ALTER TABLE news_events ADD COLUMN IF NOT EXISTS description String AFTER url`;
  - `newsSchemaStatements() []string` = `[newsEventsDdl] + newsAlterStatements`;
  - `newsInsert = "INSERT INTO news_events (date, source, title, url, description, sentiment)"`;
  - витрина `v_news_events`: 9 стейтментов, колонка `description` между `url` и `sentiment`.

- [ ] **Step 1: Тесты (падающие)**

`news/schema_test.go`:
- `TestNewsEventsDdlPinsCanonicalKey`: добавить в `want` строку `"description String"`.
- Новый `TestNewsAlterStatementsPinMigration`: `newsAlterStatements` — ровно один стейтмент, дословно `ALTER TABLE news_events ADD COLUMN IF NOT EXISTS description String AFTER url`; `newsSchemaStatements()` длиной 2, первый — `newsEventsDdl` (образец — пин ALTER в `craw/gost_test.go:249`).
- `TestNewsViewStatementsShape`: `want 8` → `9` (комментарий к сообщению обновить), в список колонок добавить `"description"`.

`news/parse_test.go`:
- Новый `TestItemsFromDocParsesDescription` на фикстуре `smartlab_spydell.rss`: у item с URL, содержащим `/blog/1363355.php`, — `Description` непуст, без краевых пробелов, начинается с `"По оценке Банка России"` и содержит `"устойчивая инфляция"` и `"Замедление обеспечили овощи и фрукты"`.
- `TestItemsFromDocSkipsInvalidItems`: в синтетической ленте у валидной позиции добавить `<description>  Текст описания  </description>`; проверить `Description == "Текст описания"` (трим); у позиции без `<description>` — пустая строка, позиция не пропущена.

`news/news_test.go`:
- `newsInsertedRow` + `inserted()`: 6 значений, порядок `(date, source, title, url, description, sentiment)`; комментарии обновить.

Run: `go test ./news/` → FAIL.

- [ ] **Step 2: Канон `ARCHITECTURE.md` §6.2 (раньше копии)**

В блоке `CREATE TABLE IF NOT EXISTS news_events` — колонка после `url`:
```sql
    description String,              -- текст позиции из ленты: у smart-lab — полный текст поста
                                     -- (HTML после разбора XML-сущностей); у новостных лент — краткое
                                     -- описание позиции; '' — описания нет
```

- [ ] **Step 3: `news/parse.go`**

`rssItem` — поле `Description string \`xml:"description"\``; обновить комментарий структуры (описание позиции теперь хранится). `newsItem` — поле `Description string`. В `itemsFromDoc` — `Description: strings.TrimSpace(it.Description)` (внутренние переводы строк и разметка сохраняются: это текст поста; пустое — `''`, не пропуск и не ошибка).

- [ ] **Step 4: `news/schema.go`**

- DDL-копия: колонка `description String` после `url` (дословно как канон, комментарии в блоке — как §6.2).
- `newsAlterStatements` (комментарий: миграция живой пятиколоночной таблицы, `ADD COLUMN IF NOT EXISTS` идемпотентен, поэтому выполняется каждый прогон; порядок — как в DDL) и `newsSchemaStatements()` по образцу `gostSchemaStatements` (`craw/gost.go:127-145`).
- `newsViewStatements`: в SELECT — `description` между `url` и `sentiment`; `COMMENT COLUMN description` — «текст позиции из ленты: полный текст поста smart-lab (HTML) или краткое описание новости; '' — описания нет; при цитировании — атрибуция автора и smart-lab»; комментарий таблицы дополнить smart-lab. Итого 9 стейтментов.

- [ ] **Step 5: `news/news.go`**

- `Import`: вместо `conn.Exec(ctx, newsEventsDdl)` — цикл по `newsSchemaStatements()` (все режимы, до чтения лент — как сейчас).
- `newsInsert` — явный список колонок: `INSERT INTO news_events (date, source, title, url, description, sentiment)`.
- `insertItems`: `batch.Append(it.Date, it.Source, it.Title, it.URL, it.Description, (*float32)(nil))`; комментарий обновить (порядок — контракт с явным списком колонок).

- [ ] **Step 6: Прогнать тесты**

Run: `go test ./news/ -race`
Expected: PASS (в т.ч. композиция трёх режимов на фейках).

- [ ] **Step 7: Commit**

```bash
git add news/ ARCHITECTURE.md
git commit -m "news: store smartlab post text in news_events.description (phase 1.4)"
```

---

### Task 3: Фильтр витрины — стемы ставки

**Files:**
- Modify: `news/schema.go` (`newsFilter`), `news/schema_test.go` (`TestNewsFilterMatchesThesisStems`)

**Interfaces:**
- Consumes: `newsFilter` (константа), тест позитивных/негативных заголовков.
- Produces: расширенная константа: к текущему списку добавляется `|вклад|депозит|наличн|кредит|ОФЗ|RGBI|RUONIA|RUSFAR`.

- [ ] **Step 1: Тест (падающий)**

В позитивный список `TestNewsFilterMatchesThesisStems` добавить: `"Наличные бьют рекорд в России, вклады населения не растут"`, `"Ставки по депозитам обновили максимум"`, `"Доходность ОФЗ снизилась"`, `"RUONIA опустилась ниже ключевой"`. Негативные кейсы («погода в Москве», «футбольный матч») не трогать.

Run: `go test ./news/ -run TestNewsFilterMatchesThesisStems -v` → FAIL.

- [ ] **Step 2: Реализация**

Дописать стемы в конец `newsFilter` (`news/schema.go`); комментарий константы дополнить: расширение — стемы ставки/денежного рынка (вклады, депозиты, наличные, кредит, ОФЗ, RGBI, RUONIA, RUSFAR); фильтр остаётся осознанно широким.

- [ ] **Step 3: Прогнать тесты**

Run: `go test ./news/ -run TestNewsFilter -v` → PASS.

- [ ] **Step 4: Commit**

```bash
git add news/schema.go news/schema_test.go
git commit -m "news: extend thesis filter with rate stems (phase 1.4)"
```

---

### Task 4: MCP-проверка и комментарий расписания

**Files:**
- Modify: `scripts/mcp_check.py` (блок «Контур событий РФ-канала», после проверки сырой `news_events` ~строка 328)
- Modify: `dagu/news.yaml` (шапка-комментарий)

**Interfaces:**
- Consumes: существующие хелперы `text`, `result_rows`, `expect`; константа `newsInsert`… — нет, только витрина.
- Produces: проверка `v_news_events`: `SELECT count() FROM v_news_events WHERE source = 'smartlab' AND description != ''` > 0; сообщение-подсказка при провале: «прогнать `make import STAT=news_watch`» (образец — проверка каталога bold ~строка 300).

- [ ] **Step 1: Добавить проверку в `scripts/mcp_check.py`**

Запрос + разбор через `result_rows`; fail, если строк нет/`count` нулевой, с подсказкой прогнать импорт; `failed += not expect(...)`. Комментарий: посты smart-lab — образцы публикаций и рыночный разбор (`KEY_RATE_PUBLICATIONS.md` §12).

- [ ] **Step 2: Синтаксис-проверка**

Run: `python3 -m py_compile scripts/mcp_check.py` → без ошибок.

- [ ] **Step 3: Обновить комментарий `dagu/news.yaml`**

В шапке-комментарии — 7 лент (…, TASS, smart-lab ×2: блог KonstantinLebedev и spydell-telegram); у smart-lab полный текст поста уходит в `description`. Расписание/режим/окна без изменений.

- [ ] **Step 4: Проверка dagu (если бинарь есть)**

Run: `dagu validate dagu/news.yaml` → exit 0 (если `dagu` не установлен — отметить в отчёте, не блокировать).

- [ ] **Step 5: Commit**

```bash
git add scripts/mcp_check.py dagu/news.yaml
git commit -m "news: mcp check for smartlab posts (phase 1.4)"
```

---

### Task 5: Живая приёмка (в сессии, через `make`)

**Files:** нет правок кода.

- [ ] **Step 1:** `make all` → зелёный (gofmt, golangci-lint, `go vet`, `go test -race`, обе сборки).
- [ ] **Step 2:** `make import STAT=news_watch` (режим `all`) → в логе `smartlab_lebedev` и `smartlab_spydell` со счётчиками позиций; записаны строки.
- [ ] **Step 3:** `NEWS_WATCH_MODE=since make import STAT=news_watch` → повтор идемпотентен (`0 строк` / «новых позиций нет»).
- [ ] **Step 4:** `NEWS_WATCH_MODE=check make import STAT=news_watch` → покрытие по `smartlab`, дыр нет.
- [ ] **Step 5:** MCP (`kimi_reader`): `SELECT date, title, url, length(description) FROM v_news_events WHERE source='smartlab' ORDER BY date DESC LIMIT 20` → посты видны; строка с `url LIKE '%1363355%'` — непустой `description`; `SELECT count() FROM news_events` → ACCESS_DENIED.
- [ ] **Step 6:** `make mcp-check` → «ПРОВЕРКА ПРОЙДЕНА».
- [ ] **Step 7:** Записать числа (команда → результат) — они войдут в роадмап и PR.

Если ClickHouse не отвечает — попросить владельца поднять (`make ch-up`); приёмку не подменять.

---

### Task 6: Документы

**Files:**
- Modify: `docs/KEY_RATE_PUBLICATIONS.md` (§4, §6, новый §12), `docs/ROADMAP_KEY_RATE.md` (§4.2 №6, §5, §6 фаза 1.4, цепочка фазы 2), `ARCHITECTURE.md` (§2, §3, §6.3, §7), `README.md`

**Interfaces:** Consumes: числа приёмки (Task 5); Produces: актуальные документы.

- [ ] **Step 1: `KEY_RATE_PUBLICATIONS.md`**

- §4, после абзаца «Отличие от недельных постов…» — указатель: посты-образцы и рыночный разбор читаются из `v_news_events` (`source='smartlab'`), см. §12.
- §6, строка «Посты-образцы и рыночный разбор» → ✅: два фида smart-lab (`KonstantinLebedev`, `spydell-telegram`) под `source='smartlab'`, полный текст поста — колонка `description` витрины; фаза 1.4.
- Новый §12 «Посты-образцы и рыночный разбор»: живой список — запрос к `v_news_events` (`source='smartlab'`, `ORDER BY date DESC`); канонический пример — spydell `/blog/1363355.php` (16.10.2026) с таблицей «приём поста → раздел шаблона §4»: разложение ИПЦ по вкладам категорий (п.2), сравнение с нормой 2017–2019 и устойчивая инфляция тремя мерами (п.2), бюджетный импульс в цифрах (п.2), блоки чувствительности к ставке (п.3), взгляд на решение ЦБ (п.1, п.4–5); правила: атрибуция автора/даты и smart-lab, чужой взгляд ≠ наш прогноз, вероятности и журнал — наши (§2, §8); «рыночный разбор» — внешний взгляд рынка в связке с §4 п.4–5; результат проверки 2026-10-17: регулярные разборы инфляции/бюджета — у `spydell-telegram`, у `KonstantinLebedev` в окне — собственные публикации по модели.

- [ ] **Step 2: `ROADMAP_KEY_RATE.md`**

- §6 фаза 1.4 → ✅ с датой и числами приёмки (Task 5).
- §4.2 №6: оба фида + `source='smartlab'`, статус ✅ фаза 1.4.
- §5, строка `news_events (source='smartlab')` → ✅ фаза 1.4.
- Цепочка фазы 2: упоминание «→ №6 smart-lab (в news-контур)» → пометить сделанным фазой 1.4.

- [ ] **Step 3: `ARCHITECTURE.md` + `README.md`**

По скилу `sync-readme-architecture`: `ARCHITECTURE.md` §2 (пакет `news/`: 7 лент, `description`), §3 (строка `news_watch`: 7 лент + хранение текста), §6.3 (витрина: колонка `description`, стемы фильтра), §7 (ленты: + smart-lab ×2); `README.md` — пакет news, лимитации (`grep -n 'news' README.md` — все места, где 5 лент / описание не хранится / окна).

- [ ] **Step 4: Commit**

```bash
git add docs/KEY_RATE_PUBLICATIONS.md docs/ROADMAP_KEY_RATE.md ARCHITECTURE.md README.md
git commit -m "docs: sample posts and market analysis in key rate publications (phase 1.4)"
```

---

### Task 7: Ревью, верификация, PR

- [ ] **Step 1:** `superpowers:requesting-code-review` — финальное ревью всей ветки (модель `deepseek/deepseek-flash`); замечания разобрать через `receiving-code-review` (fix-раунды — по циклу subagent-driven-development).
- [ ] **Step 2:** `superpowers:verification-before-completion` — сверить критерий приёмки пункта («посты видны агенту; повторный прогон идемпотентен») с фактическими выводами Task 5.
- [ ] **Step 3:** Push ветки и один PR (владелец мержит); в описании — источник, схема, как запускать, пример проверки; протокол живой приёмки — комментарием в PR (стандарт `IMPORTER_PACKAGE_STANDARD.md` §16.1 п. 8).

## Self-Review

- Spec coverage: ленты+фикстуры (Task 1), `description` разбор/схема/миграция/запись/витрина (Task 2), фильтр (Task 3), MCP+расписание (Task 4), приёмка (Task 5), документы §6/§12/роадмап/ARCHITECTURE/README (Task 6), ревью/PR (Task 7). Вне объёма из спеки — не в задачах (сознательно).
- Step scan: каждый шаг — одно действие с проверяемым результатом; значения (URL, стемы, фрагменты поста, имена тестов) — дословно из спеки и разведки.
- Type consistency: `Description` (newsItem/rssItem), `newsAlterStatements`/`newsSchemaStatements`, `newsInsert` с 6 колонками, `newsViewStatements` = 9 — согласованы между задачами.
- Review Focus: пять строк выше — у каждой есть пин в тестах Task 1–3.
- Proportion: план описывает решения (имена, значения, пины), а не тела функций.
