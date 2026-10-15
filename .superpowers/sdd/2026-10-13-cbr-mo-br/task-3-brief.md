### Task 3: Парсер `cbr/mo_br_parse.go` + дескрипторы `cbr/mo_br_meta.go` + тесты

**Files:**
- Create: `cbr/mo_br_parse.go`, `cbr/mo_br_meta.go`, `cbr/mo_br_parse_test.go`, `cbr/mo_br_series_meta_test.go`
- Test fixture: `cbr/testdata/mo_br_full.xlsx` (Task 1)

**Interfaces:**
- Consumes: `*excelize.File`; `util.SeriesMeta`; фактические числа/споты фикстуры из Task 1.
- Produces (используют T4/T6):
  - `type moBrPoint struct { Series string; Date time.Time; Value float64 }`
  - `func parseMoBr(xlsx *excelize.File) ([]moBrPoint, []util.SeriesMeta, error)` — чистая (без сети); адаптивная: шапка волн = строка с ≥3 датами (дата-ячейка — raw-число, `ExcelDateToTime` которого даёт год 2000–2100: значения-прогнозы 0–100 отсеиваются); блоки статистик — метки колонки B; годы — даты колонки D в блоке; спецлисты 16–17 — статистика × волны без годов; «-»/пусто — пропуск. **Обход листов — по `GetSheetList`**: лист с «Навигация» в имени (в живом файле — `Навигация (Navigation) `, с хвостовым пробелом) пропускается; неизвестный лист-показатель — ошибка всегда; **полная книга** (навигационный лист есть — так устроен живой файл ЦБ, спека §3.2) обязана содержать все 17 листов реестра — недостающие → ошибка; **сокращённая книга** (фикстура, навигационного листа нет) разбирается как есть. Прочие fail-fast: нет шапки волн на листе / нет ни одного блока из 6 статистик на листе / 0 точек после разбора → ошибка.
  - `moBrIndicators` — 17 дескрипторов (лист, base, title, unit) ровно из таблицы спеки §2; `moBrSource = "cbr_mo_br"`; суффиксы статистик (`median|p10|p90|min|max|answers`) и шаблонные описания статистик.

- [ ] **Step 1: Написать падающие тесты** (`cbr/mo_br_parse_test.go`, `cbr/mo_br_series_meta_test.go`)

На фикстуре (`excelize.OpenFile("testdata/mo_br_full.xlsx")`):
- `TestParseMoBrFixtureRows`: число рядов и точек = фактическим из Task 1 (для сентябрьской фикстуры — 114 / 2 058); число рядов по листам (54 / 54 / 6);
- `TestParseMoBrSpotValues`: споты из Task 1 (например `key_rate_avg_2026_median` на `2026-09-01` = 14.5; `key_rate_avg_2027_median` = 12.43; `cpi_dec_2026_median` = 6.6; `neutral_rate_median` = 8.0; плюс по одному споту `_p10` и `_answers` — фактические из фикстуры);
- `TestParseMoBrMetaTwoSided`: множество `Series` из точек == множество `Series` каталога (обе стороны);
- `TestParseMoBrSkipsDashes`: в точках нет рядов со пустым значением; число точек листа «3» = 900 (или фактическое);
- fail-fast (конструирование на копии фикстуры / in-memory книгах): (а) копия фикстуры + `xlsx.NewSheet("Навигация (Navigation) ")` (книга становится «полной») + `xlsx.DeleteSheet("3")` → ошибка перечисляет отсутствующие листы-показатели полной книги, включая «3»; (б) in-memory книга с **всеми 17** именами листов-показателей без данных и шапки → ошибка «нет шапки волн»; отдельно — in-memory лист-показатель с шапкой, но без меток статистик → «нет блоков»; и лист с шапкой и блоком, где все значения «-» → «0 точек»; (в) копия фикстуры + `xlsx.NewSheet("99")` → ошибка «неизвестный лист-показатель».
- `cbr/mo_br_series_meta_test.go` (без фикстуры; образец `cbr/mp_survey_series_meta_test.go`): все 17 дескрипторов определены, base-имена уникальны и с префиксом по спеке; у каждой сгенерированной SeriesMeta заполнены Source/Series/Title/Unit/Frequency/Origin/Description; описания статистик различаются.

- [ ] **Step 2: Прогнать — FAIL**

Run: `go test ./cbr/ -run 'MoBr' -v` → компиляция падает (`parseMoBr` не определён).

- [ ] **Step 3: Реализовать `cbr/mo_br_meta.go` и `cbr/mo_br_parse.go`**

Дескрипторы — дословно из спеки §2 (номер листа, показатель, единица); генерация SeriesMeta: Series = `<base>_<year>_<stat>` (год — из колонки D; у листов 16–17 без года), Description = шаблон «прогноз аналитиков: <title> на <year> год; <расшифровка статистики>», Unit — из дескриптора. Парсер — единственный проход по строкам листа с состоянием «текущий блок статистики» (метки B), сбор непустых числовых ячеек по колонкам-волнам; годы — из колонки D первой строки каждого блока; спецлисты — блок из одной строки на статистику. Frequency у рядов — `"8/год (волны опроса)"`.

- [ ] **Step 4: Прогнать — PASS**

Run: `go test ./cbr/ -run 'MoBr' -v && go test ./cbr/ -count=1 && gofmt -l cbr/` → зелёные.

- [ ] **Step 5: Commit**

```bash
git add cbr/mo_br_parse.go cbr/mo_br_meta.go cbr/mo_br_parse_test.go cbr/mo_br_series_meta_test.go
git commit -m "cbr: mo_br parser, indicator registry and catalog generation + tests"
```

---

