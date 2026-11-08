# PDF-фолбэк агрегата недельного ИПЦ (`rosstat_bi_cpi_weekly`)

**Дата:** 2026-11-08. **Пункт:** `rosstat/1.13`, issue [#546](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/546). **Зависимость:** `rosstat/1.12` (issue [#539](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/539), PR #541) — общий `util.Fallback` и CPI-PDF новостной ленты уже в `main`.

## 1. Зачем

`weekly_cpi_sa` стоит на последней точке своего входа и не двигается, хотя товарные недельные ряды обновлены:

| Ряд | Последняя точка | Комментарий |
|---|---|---|
| `rosstat_ipc_weeks` (детальные товары) | 2026-11-02, 111 рядов | не вход SA |
| Агрегат «Индекс потребительских цен (недельная оценка)» | 2026-10-26, 100.08 | вход SA; на 02.11 отсутствует |
| `weekly_cpi_sa` | `date=2026-10-31`, `as_of=2026-10-26`, `partial=1` | 210 строк: 209 полных + nowcast; `sa_4w=0.9366 %`, `saar=12.88 %` |

На 2026-11-02 в `v_rosstat_macro` 111 рядов, на 2026-10-26 — 112; единственный отсутствующий ряд — агрегат. Причина: агрегат 2019+ берётся только из `rosstat_bi_cpi_weekly` (BI), BI недоступен, а PDF-фолбэк 1.12 пишет 108 товарных строк и агрегат не разбирает. Повторный `make import STAT=weekly_cpi_sa` — no-op по SHA-256 входа: без новой точки агрегата SA не сдвинется.

В CPI-PDF агрегат есть: 2026-11-02 — **99,96**; 2026-10-26 — **100,08**, что совпадает с BI в `v_rosstat_macro`. Это тот же ряд и та же семантика (индекс к предыдущей дате регистрации, 100 = без изменения).

**Цель:** при недоступном BI `rosstat_bi_cpi_weekly` добирает агрегат из CPI-PDF, чтобы `weekly_cpi_sa` поднялся до `date=2026-11-30`, `as_of=2026-11-02`, `partial=1`, 211 строк.

## 2. Механизм

Фолбэк вызывается **внутри** `BICpiWeeklyStat.Import`, а не отдельным шагом DAG: в `type: chain` шаг после падающего BI не запустится (тот же довод, что у 1.12). Используется общий `util.Fallback` (стандарт импортёров §5.2.1), без второй копии цикла.

```
BICpiWeeklyStat.Import
  ├─ restore-guard и DDL (без изменений)
  ├─ biCpiWeeklyFetchPoints: export-RPC → CSV → разбор
  │    └─ ошибка → warn + biCpiWeeklyFallback
  └─ успех → прежний гейт версии, батч, AfterImport, SavePublication
```

`biCpiWeeklyFallback`:

```go
fallback := &util.Fallback{
    Template: util.ClickHouseImport{
        TableName:              rosstatBICpiWeeklyTable,
        AfterImport:            s.AfterImport,
        PublicationCountPoints: biCpiWeeklyCountPoints,
    },
    Documents:       func(ctx context.Context) ([]util.FallbackDocument, error) {
        return ipcWeeksCPIDocumentsFor(ctx, biCpiWeeklyPDFContext)
    },
    Parse:           biCpiWeeklyPDFRows,
    KnownDatesQuery: biCpiWeeklyKnownDatesQuery,
    Append:          biCpiWeeklyPDFAppendRow,
}
return fallback.Import(ctx, conn)
```

- `Documents` — CPI-выпуски `central-news` (тот же источник, что у 1.12); берутся только PDF, HTML-выпуски не разбираются.
- `KnownDatesQuery` — `SELECT DISTINCT date FROM rosstat_bi_cpi_weekly FINAL WHERE series = 'Индекс потребительских цен (недельная оценка)'`.
- `Append` — батч `(series, date, value)`; значение `Float64`, как в DDL таблицы.
- `Template.PublicationCountPoints` включает шаблонную версию: на каждый импортированный PDF пишется строка `source_publications` (`source='rosstat_bi_cpi_weekly'`, `name` = basename PDF, HEAD-отпечаток, URL, points). При включённом `ARCHIVE_BACKEND` PDF сохраняется в винтажный архив; при выключенном — только строка версии. Если шаблон уйдёт в no-op по версии PDF при отсутствующей дате, `res.Imported` включит общий `count()` таблицы; post-cycle проверка дат `util.Fallback` делает такой рассинхрон громкой ошибкой, а штатный путь (дата отсутствует → версия новая) считает строки точно.

**Политика ошибок** (как у `rosstat_ipc_weeks`):

- BI упал, фолбэк успешен — warn, успех (шаг не роняет цепочку `dagu/rosstat.yaml`).
- BI упал, фолбэк упал — `errors.Join(primaryErr, fallbackErr)`.
- BI успешен — фолбэк не вызывается; ошибки вставки/публикации не подменяются фолбэком.
- Все даты фолбэка уже в таблице — warn, `Imported 0 rows`.

## 3. Разбор агрегата

Новая чистая функция `ipcWeeksNewsAggregate(text string) (float64, error)` в `rosstat/ipc_weeks_news.go`:

- строки нормализуются `strings.Fields` (как в `ipcWeeksNewsParse`);
- ищется строка `Индекс потребительских цен` и следующая за ней `(оценка) …`, либо одна строка `Индекс потребительских цен (оценка) …`;
- первое число после `(оценка)` разбирается `ipcWeeksNewsValue` (запятая → точка, NaN/Inf запрещены);
- отсутствие строки, дубль и нечисло — ошибка.

Дата и целостность выпуска не дублируются: `biCpiWeeklyPDFTextRows` сначала зовёт существующий `ipcWeeksNewsParse` (он проверяет таблицу «Индексы потребительских цен», дату шапки и 108 строк), затем берёт дату из первой точки и сверяет с `doc.Date`. `biCpiWeeklyPDFRows` — обёртка: `util.PDFText(..., util.PDFLayout)` + чистая часть.

`ipcWeeksNewsReleases` и `ipcWeeksCPIDocuments` получают вариант `...For(page/ctx, context string)`, чтобы диагностика BI-фолбэка шла с префиксом `rosstat_bi_cpi_weekly`, а не `rosstat_ipc_weeks`; старые функции остаются обёртками с прежним контекстом, поведение 1.12 не меняется.

## 4. Провенанс и совместимость

- Нормальная версия BI (`name='Индекс потребительских цен.csv'`, SHA-256 разобранных точек) не меняется; PDF-версии живут отдельными строками с basename PDF.
- `weeklysa` не меняется: `published_at` nowcast-строки берётся `toDate(maxOrNull(first_seen))` по обоим источникам, поэтому PDF-версия автоматически становится датой появления агрегата.
- При восстановлении BI его CSV перезапишет PDF-точку по ключу `(series, date)` (`ReplacingMergeTree`); PDF-строка версии остаётся аудиторским следом.
- Новых таблиц, витрин, грантов и шагов DAG нет; `v_rosstat_macro` и `dagu/rosstat.yaml` не меняются по составу.

## 5. Границы и риски

- Фолбэк включается только при сбое fetch/parse BI; «BI ответил, но срез старый» — вне пункта.
- Лента `central-news` отдаёт 20 карточек (~5 месяцев): пропуски старше окна ленты фолбэк не закрывает; ограничение фиксируется в `docs/LIMITATIONS.md`.
- Значение из PDF может быть ревизовано BI позже; это ожидаемо и перекрывается ключом `(series, date)`.
- `pdftotext` (poppler-utils) уже обязателен в production с 1.12; новых зависимостей нет.
- При включённом `ARCHIVE_BACKEND` PDF-версия сохраняется только как аудит; `ARCHIVE_ACTION=restore` для `rosstat_bi_cpi_weekly` по-прежнему не поддержан, и архивные PDF не участвуют в восстановлении.

## 6. Приёмка

- `make import STAT=rosstat_bi_cpi_weekly` при недоступном BI → фолбэк, `Imported 1 rows` (2026-11-02 = 99.96), в `source_publications` строка с `name=ipc11_02.pdf`; повтор → `Imported 0 rows`.
- `v_rosstat_macro` на 2026-11-02 — 112 ряда, агрегат 99.96.
- `make import STAT=weekly_cpi_sa` → 211 строк, `date=2026-11-30`, `as_of=2026-11-02`, `partial=1`; витрина — 633 строки / 3 ряда.
- `make all`, `make docs-check`, `dagu validate dagu/rosstat.yaml` — зелёные; `make mcp-check` — без новых регрессий.
- Если BI восстановится до приёмки, живой фолбэк не проверяется: зафиксировать «не проверено» и подтвердить тестами.
