# Спека: `rosstat_bi_cpi_weekly` — устойчивый экспорт BI Росстата и гейт версии

**Пункт:** `key-rate/phase-2-source-fixes` (фаза 2, follow-up).
**Issue:** [#338](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/338).
**Роль:** `role:go`.
**Дата:** 27 октября 2026 г.
**Ветка:** `fix/rosstat-bi-cpi-weekly`, worktree `.worktrees/rosstat-bi-cpi-weekly-fix`.
**Сопутствующие документы:** [ROADMAP_KEY_RATE.md](../../ROADMAP_KEY_RATE.md) фаза 2, [IMPORTER_PACKAGE_STANDARD.md](../../IMPORTER_PACKAGE_STANDARD.md) §15/§16, [ARCHITECTURE.md](../../../ARCHITECTURE.md) §6.1/§8 п. 13, спека источника [2026-10-20-rosstat-bi-cpi-weekly-design.md](2026-10-20-rosstat-bi-cpi-weekly-design.md).

---

## 1. Дефект

`make import STAT=rosstat_bi_cpi_weekly` (и группа `key_rate_sources`) падал на экспорте BI-портала Росстата:

```text
rosstat_bi_cpi_weekly: export RPC: Post "https://bi.rosstat.gov.ru/biportal/grid": EOF
```

Найдено живым прогоном `make import STAT=key_rate_sources` 2026-10-21 (PR #333). Тот же импортёр 2026-10-20 отдавал `Imported 398 rows` — то есть между двумя прогонами изменилось не поведение кода.

## 2. Разведка (2026-10-27, живые запросы)

| Проверка | Результат |
|---|---|
| `GET https://bi.rosstat.gov.ru/biportal/` и дашборд `Dashboard/Prices_week` | `200`, `BI Portal`, 6,5 КБ; маркеров антибота (spid/ServicePipe, captcha, Qrator, Variti, Stormwall) нет |
| `POST /biportal/grid` с текущим `biCpiWeeklyExportPayload` (curl) | `200`, `//OK[1,["tmp/exp<uuid>/Индекс потребительских цен.csv"],0,7]`, 114 B — контракт экспорта не менялся, `X-GWT-Permutation` и policy-hash актуальны |
| Вариации запроса: UA Chrome 109, `Cookie: redirect_cookie=1`, `--http2`, `--no-alpn`, `Accept:`, `Accept-Encoding: gzip`, `Connection: keep-alive`, `--tls-max 1.2` | все `200 //OK` — ни заголовки, ни ALPN, ни версия HTTP не триггер |
| 12 POST подряд | 12 × `200` — rate limit/антибот не срабатывает |
| `GET` tmp-CSV | `200`, 80 555 B, **399 точек**, последняя `19.10.26 = 100.05` — источник свежий |
| Два независимых экспорта | байт-в-байт (`sha256 b7dd67bd…91cfc`); `ETag` вшивает mtime и меняется, `Last-Modified` — время генерации → заголовочный отпечаток версии непригоден, пригоден хеш содержимого |
| **Прогон Go-клиента** (`util.PostFileBytes`, `util.GetFileBytes`, `tls.Dial`) | **всё зелёное**: `//OK`, 80 555 B / 400 строк CSV, `tls.Dial` — TLS 1.3 (`0x0303`, cipher `0xc030`). Дефект не воспроизводится, стек Go порталом не отбивается |
| Тулчейн | `go1.27.2` с 2026-10-03; `go.mod` 2026-10-21 менял только зависимости (`fix: vulncheck`) |

**Вывод:** источник, контракт и клиент исправны; 2026-10-21 портал оборвал соединение в конкретном прогоне. Претензия к коду не в «неверном эндпоинте», а в том, что **один транспортный обрыв валит весь прогон**: у экспорт-RPC и скачивания CSV нет ни одной повторной попытки. Плюс повторный прогон не является no-op-ом по отчёту (печатает `Imported 399 rows`, хотя данные не меняются), а версия содержимого нигде не фиксируется.

## 3. Решение

### 3.1. Транспорт: ограниченные ретраи

- `biCpiWeeklyAttempts = 3`, паузы 2 с и 5 с, ожидание прерывается `ctx`.
- Ретраится только транспорт (`PostFileBytes`, `GetFileBytes`): ошибка сети/обрыв; ошибки разбора `//OK` и CSV не повторяются — повтор не сделает битый ответ валидным.
- `attempts` и `sleep` — переменные пакета (прецедент `util.fileTimeout`): тест подменяет их, не растягивая прогон.
- Итоговая ошибка называет класс отказа и число попыток, чтобы оператор видел «портал закрыл соединение», а не голый `EOF`.
- Клиент остаётся `util.HttpClient` (правило 4 AGENTS.md): проба показала, что он проходит; `NewDefaultTLSClient` не нужен и не вводится.

### 3.2. Гейт версии по хешу содержимого

Свой полный цикл `Import()` (прецеденты `weeklysa/importer.go`, `fedstat/ipc.go`), без шаблонного `util.ClickHouseImport.Import`:

```text
DDL → export-RPC (ретраи) → GET CSV (ретраи) → разбор → SHA-256 точек →
гейт по source_publications → INSERT батчем → AfterImport → SavePublication
```

- `biCpiWeeklyVersionName = "Индекс потребительских цен.csv"` — литерал, а не `path.Base(url)`: публичный URL percent-encoded, basename из него нестабилен по виду.
- `biCpiWeeklyFingerprint(points)` — `sha256` по каноничным строкам `2006-01-02\t<strconv.FormatFloat(value, 'g', -1, 64)>`, отсортированным по `(дата, значение)`; точки уже дедуплицированы парсером, поэтому порядок строк источника на отпечаток не влияет. Формат числа — кратчайшее представление (`'g', -1`), а не `.17g`: строка отпечатка закреплена тестом, и смена формата сменила бы версию всех точек.
- `biCpiWeeklyShouldSkip(known bool, knownPoints, dbPoints uint64) bool` — no-op только если версия известна **и** `SELECT count() FROM rosstat_bi_cpi_weekly FINAL` совпал с записанным: ручная чистка таблицы даёт полный self-heal, а не тихий no-op.
- no-op-путь перепубликует производные (`AfterImport`: каталог + `v_rosstat_macro`) и возвращает `0, nil` → `Imported 0 rows of rosstat_bi_cpi_weekly` без ошибки.
- полный путь: `PrepareBatch` → `Append` → `Send`, на ошибке до `Send` — `Abort` (правило 2); после успеха — `AfterImport`, затем `util.SavePublication` с `Source = rosstat_bi_cpi_weekly`, `Url` текущего tmp-CSV (аудит).
- `PublicationCountPoints` **не заполняется**: шаблонного HEAD-префлайта нет (URL новый на каждый POST), версия пишется вручную — как у `weeklysa`. Литерал `countPoints` в `rosstat/importers.go` остаётся пустым, энфорсер реестра остаётся зелёным.

Побочный эффект: `weeklysa` считает `published_at = toDate(maxOrNull(first_seen))` по `source_publications` обоих входов — у BI это поле перестанет быть `NULL`. Гейт `weekly_cpi_sa` (SHA-256 входов + пин скрипта) от этого не сбрасывается, полного перегона нет.

### 3.3. Полный прогон без точек — ошибка

- Парсер уже падает на пустой выгрузке, выгрузке без шапки и на неверной первой дате.
- В цикле добавляется явный инвариант: полный путь, записавший `0` строк, — ошибка (`полный прогон не дал точек — источник недодал историю`, формулировка `fedstatRequireFullFetch`). `main.go` логирует ошибку, затем `Imported 0 rows` и выходит с кодом 1.

## 4. Что не меняется

- Имя таблицы `rosstat_bi_cpi_weekly`, DDL, ключ `ReplacingMergeTree`, ряд `Индекс потребительских цен (недельная оценка)`, каталог и витрина `v_rosstat_macro` — без изменений.
- Парсер CSV (`biCpiWeeklyParseCSV`), разбор `//OK` (`biCpiWeeklyExportPath`) и сборка публичного URL — по смыслу те же.
- Payload/заголовки RPC, `Referer`, `biCpiWeeklyMinDate` — те же.
- Архива версий у BI нет: файл одноразовый, версия — хеш содержимого. `ARCHIVE_ACTION=restore` импортёр **отклоняет явной ошибкой** до DDL и сети (прецедент `rosstat_okved2`): иначе режим восстановления молча пошёл бы полным живым импортом, хотя по контракту источник в нём не спрашивается.

## 5. Тесты

- Ретраи: `httptest`-сервер, рвущий соединение на первой попытке (`http.Hijacker` + `Close`) и отвечающий `//OK` на второй → успех; сервер, рвущий всегда → ошибка после 3 попыток (с `sleep` = no-op).
- Гейт: `biCpiWeeklyShouldSkip` (4 случая), `biCpiWeeklyFingerprint` (порядок не влияет, значение влияет, повтор стабилен).
- 0 точек: только шапка → ошибка; пустое тело → ошибка (уже есть, сохраняется).
- Фикстуры: живой ответ RPC `rosstat/testdata/bi_cpi_weekly_rpc_ok.txt` + spot `19.10.26 = 100.05` в CSV-фикстуре.

## 6. Приёмка

1. `make import STAT=rosstat_bi_cpi_weekly` — `Imported N rows`, `count() FINAL = N`, `2019-01-09 … 2026-10-19`, spot `19.10.2026 = 100.05`.
2. Повтор — `Imported 0 rows` без ошибки, `count() FINAL` не изменился.
3. Полный прогон без точек — ненулевой код и явная ошибка (unit-тест; живой источник нуля точек не отдаёт).
4. `make all`, `make mcp-check` — ПРОВЕРКА ПРОЙДЕНА, `dagu validate dagu/rosstat.yaml` — exit 0, `make docs-check`.
5. Протокол — комментарием в PR.

## 7. Риски

- Ретраи экспорт-RPC создают на портале мусорные `tmp/exp<uuid>` — безвредно, файлы временные.
- Портал может снова сменить сборку (`X-GWT-Permutation`, policy-hash) — импортёр обязан падать явно; это уже так и остаётся.
- Отпечаток считается по разобранным точкам, поэтому смена локали выгрузки (ru/en) отпечаток не меняет — это осознанно.
