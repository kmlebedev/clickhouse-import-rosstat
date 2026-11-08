# План: `cbr_int_rate_loans_ind` на файл `loans_ind_new.xlsx`

**Дата:** 8 ноября 2026 г.
**Спека:** [2026-11-08-cbr-int-rate-loans-ind-source-move-design.md](../specs/2026-11-08-cbr-int-rate-loans-ind-source-move-design.md).
**Пункт:** `key-rate/phase-2-retail-loan-rates-new-file` (issue #551).
**Ветка:** `fix/key-rate-retail-loan-rates-new-file`, worktree `.worktrees/key-rate-retail-loan-rates-new-file`.

---

## Task 1. Спека и план — первым коммитом

- [x] Спека §1–§6.
- [x] План Task 1–6.
- Коммит: `docs: spec and plan for cbr_int_rate_loans_ind source move`.

## Task 2. Разбор: URL и раскладка колонок

Файл `cbr/int_rate_loans_ind.go`.

- [ ] `cbrIntRateLoansIndDataUrl` → `https://www.cbr.ru/vfs/statistics/pdko/int_rat/loans_ind_new.xlsx`;
      комментарий — про 404 старой страницы, новый адрес раздела и замороженный старый файл.
- [ ] Тип `cbrIntRateLoansIndColumn{Series, Group, Header string}`.
- [ ] `cbrIntRateLoansIndTerms` — прежние 8 сроков (единственная запись текстов).
- [ ] Константы групп `в разрезе сроков` / `по всем срокам`, префикс `Всего (кредиты физлицам): `.
- [ ] `cbrIntRateLoansIndAggregate` — `{все виды кредитов → Всего (кредиты физлицам): по всем срокам}`,
      `{в том числе автокредиты → Автокредиты: по всем срокам}`.
- [ ] `cbrIntRateLoansIndLayout` — 10 записей в порядке колонок (8 сроков + 2 агрегата).
- [ ] `cbrIntRateLoansIndNames` — серии раскладки; удалить `cbrIntRateLoansIndGroups`
      и прежний `cbrIntRateLoansIndSeriesNames`.
- [ ] `cbrIntRateLoansIndGroupStarts` — по подписям групп, выведенным из раскладки.
- [ ] `cbrIntRateLoansIndColumns`: якорь → строка групп → проверка смещений групп → строка
      заголовков → сверка 10 заголовков → имена из `Series` → инвариант `base+len(names)`.
- [ ] Комментарии разбора данных — про новую шапку; логика пропусков не меняется.

Проверка: `go test ./cbr/ -run IntRateLoansInd` — падает на старых ожиданиях (ожидаемо,
чинится в Task 4); компиляция — `go build ./cbr/`.

## Task 3. Каталог 18 рядов и витрина

Файлы `cbr/series_meta.go`, `cbr/views.go`.

- [ ] `cbrIntRateLoansIndOrigin` → новый файл; `cbrIntRateLoansIndRetroOrigin` — старый.
- [ ] `cbrIntRateLoansIndLiveMeta` — 10 записей из раскладки: origin = группа + `срок «…»`
      либо `колонка «…»`; описание — уровень/пропуск/месячный файл с лагом, разрыв автокредитов.
- [ ] `cbrIntRateLoansIndRetiredMeta` — 8 записей `Автокредиты: <term>` с ретро-origin,
      последней точкой 2025-12-30 и рядом-продолжением.
- [ ] `cbrIntRateLoansIndSeriesMeta` = live + retired (18) — его получает `AfterImport`.
- [ ] `cbr/views.go` — комментарий колонки `series` у `v_cbr_macro`: агрегат «по всем срокам».

Проверка: `go build ./cbr/`.

## Task 4. Фикстура и тесты

- [ ] `cbr/testdata/loans_ind_new.xlsx` — скачанный файл целиком; удалить `cbr/testdata/loans_ind.xlsx`.
- [ ] `intRateLoansIndWantNames` — 10 литералов; месяцы 2014-01…2026-08; по 152 точки на ряд;
      `intRateLoansIndTotalPoints = 1520`.
- [ ] Spot: 28.04 / 18.31 / 19.06 / 15.32 за 2014-01-30; 18.25 / 18.65 за 2026-08-30.
- [ ] Синтетическая шапка под новый лист; fail-fast: якорь, подпись группы, заголовок срока,
      заголовок агрегата, метка периода, нечисло, NaN; контрольный подтест — 10 точек.
- [ ] Каталог: двусторонний контракт по live-meta; отдельный тест прекращённых (origin ретро,
      description про прекращение и продолжение); тест формата origin (группа/срок/колонка).
- [ ] Мутации фикстуры (пустая ячейка, `0`, `-`, короткая строка) — по колонкам B…K.

Проверка: `go test ./cbr/ -run IntRateLoansInd -v` — зелёный.

## Task 5. MCP-проверки и документация

- [ ] `scripts/mcp_check.py`: живые `%/int_rat/loans_ind_new.xlsx%` → 10; прекращённые
      `%/int_rat/loans_ind.xlsx%` → 8; комментарии и `detail` обновлены.
- [ ] `docs/IMPORTERS.md` (строка пакета + абзац розничных ставок), `ARCHITECTURE.md`
      (§4, §6.1 строка и DDL-комментарий, §6.3, §7), `README.md`, `docs/ROADMAP_KEY_RATE.md`
      (§4.1 таблица, §4.2 №8, Фаза 2 — новый пункт со статусом).
- [ ] Пункт роадмапа `key-rate/phase-2-retail-loan-rates-new-file` в Фазе 2.

Проверка: `make docs-check`.

## Task 6. Приёмка, PR

- [ ] `make all` (gofmt, golangci-lint, `go vet`, `go test -race`, сборка).
- [ ] `make import STAT=cbr_int_rate_loans_ind` → `Imported 1520 rows`.
- [ ] MCP `v_cbr_macro`: 10 живых рядов × 152 точки (max 2026-08-30), 8 прекращённых
      (max ≤ 2025-12-30), сумма 2 464.
- [ ] Повтор импорта → no-op.
- [ ] `make mcp-check`.
- [ ] PR: пункт, `Closes #551` вне backticks, источник, схема (без изменений), запуск,
      протокол живой приёмки — комментарием в PR; `Status = In Review`, поле `PR`.
