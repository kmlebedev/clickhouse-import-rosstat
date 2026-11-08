# util.Fallback: общий резервный PDF-источник

**Дата:** 2026-11-08. **Пункт:** PDF-фолбэк для `rosstat_ipc_weeks` и перевод на него `cbr_infl_exp_op`.

## 1. Зачем

Два источника устроены одинаково: основной XLSX обновляется позже PDF, а период нужен уже сейчас.

- `cbr_infl_exp_op`: пока книга `Infl_exp_ГГ-ММ.xlsx` не вышла, оперативная точка берётся из `inFOM_ГГ-ММ.pdf`; гейт — `max(date)` по общей `cbr_infl_exp`.
- `rosstat_ipc_weeks`: при 404/устаревшем `Nedel_ipc.xlsx` неделя берётся из PDF-выпуска «Об оценке индекса потребительских цен» в новостной ленте.

Одноразовая копия цикла «PDF → гейт → шаблон → производные» уже дважды повторяется. Выносим её в `util`.

## 2. `util.Fallback`

```go
type FallbackDocument struct {
    Date time.Time
    URL  string
}
type FallbackRow struct {
    Series string
    Date   time.Time
    Value  float64
}
type Fallback struct {
    Template        ClickHouseImport
    Documents       func(ctx context.Context) ([]FallbackDocument, error)
    Parse           func(ctx context.Context, doc FallbackDocument, body []byte) ([]FallbackRow, error)
    KnownDatesQuery string
    Append          func(batch driver.Batch, row FallbackRow) error
}
func (f *Fallback) Import(ctx context.Context, conn driver.Conn) (FallbackResult, error)
```

Гейт — **множество дат** из целевой таблицы (`SELECT DISTINCT date … FINAL`), а не `max(date)`: пробел в середине набора тоже должен лечиться. `FallbackMissing` дедуплицирует документы по дате и сортирует по возрастанию.

На каждый недостающий документ — локальная копия `ClickHouseImport`: `DataUrl` + `BodyFunc`; `CreateTable=nil`; `ImportFunc/CrawFunc/BeforeImport=nil`; `AfterImport` и `PublicationSource/PublicationCountPoints` сохраняются. Поэтому у CBR продолжают работать версия и винтажный архив PDF, а производные публикуются **до** строки версии, как требует ARCHITECTURE §8 п. 13.

После цикла даты недостающих документов перечитываются: если шаблон вернул no-op (версия известна, точек нет), это громкая ошибка, а не тихий пропуск. На no-op-пути (все даты известны) `AfterImport` вызывается один раз — каталог и витрина лечатся следующим прогоном.

## 3. `util.PDFText`

Общий вызов `pdftotext`: проверка `%PDF-` до `exec`, таймаут 2 минуты, stdin/stdout, ошибки с подсказкой `poppler-utils`. Режимы: `PDFLayout` (`-layout -nopgbrk`) для rosstat и `PDFBBoxFirstPage` (`-bbox -f 1 -l 1`) для CBR. `cbr_mt_forecast` пока сохраняет свой извлекатель — это не фолбэк, консолидация отдельным пунктом.

## 4. CBR

`cbr_infl_exp_op` остаётся отдельным шагом и сохраняет `Name()`, `PublicationSource` и `PublicationCountPoints`. `Import` — тонкая обёртка над `util.Fallback`; `Documents` возвращает один PDF, `Parse` — две строки оперативной точки, `KnownDatesQuery` — `SELECT DISTINCT date FROM cbr_infl_exp FINAL`. Поведение то же; отличие — `AfterImport` вызывается и на no-op-пути.

## 5. Rosstat

`rosstat_ipc_weeks` — шаг цепочки, поэтому фолбэк вызывается **внутри** `IpcWeeksStat.Import` после основной попытки: отдельный шаг после падающего XLSX в `type: chain` не запустится.

- `Documents` — первая страница `central-news` с запросом «Об оценке индекса потребительских цен»; только `.pdf`-выпуски.
- `Parse` — `pdftotext -layout` → таблица «Индексы потребительских цен»: 108 строк, первая колонка («к предыдущей дате регистрации») — недельный индекс; дата шапки сверяется с датой титула.
- Канонизация — 26 перекрытий/алиасов под имена XLSX 2026; неизвестное имя — warn и запись под именем источника.
- В PDF нет 4 рядов XLSX: бензин АИ-92/95/98 и «Поездка на отдых в ОАЭ». Это ограничение источника, а не ошибка разбора.

Проверено на живых PDF 2026-11-02 и 2026-10-26: по 108 строк, значения за 2026-10-26 совпали с `v_rosstat_macro` до 0,01.

## 6. Ошибки

- XLSX упал, лента прочиталась — успех с warn.
- XLSX упал, лента тоже — `errors.Join`.
- XLSX успешен, есть недостающие даты, PDF не импортировался — ошибка.
- XLSX успешен, фолбэк недоступен, недостающих дат не обнаружено — warn, успех.

## 7. Границы

Новой таблицы, импортёра и шага DAG нет. Схема, каталог и витрины не меняются (только `Origin`/`Description` товарных рядов). `pdftotext` обязателен в production; тесты — на текстовых/bbox-фикстурах.
