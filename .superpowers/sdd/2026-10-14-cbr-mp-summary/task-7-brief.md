### Task 7: Календарь заседаний (cal_mp → events_calendar)

**Files:**
- Modify: `calendar/events.go`
- Create: `calendar/events_test.go`, `dagu/calendar.yaml`

**Interfaces:**
- Consumes: фикстура `calendar/testdata/cal_mp.html` (Task 2); `util.GetFile`; `util.CreateView` (существующая `eventsView`).
- Produces:
  - `const calMPUrl = "https://www.cbr.ru/dkp/cal_mp/"`
  - `func parseCalMP(page []byte) ([]time.Time, error)` — карточки-заседания (текст содержит «Заседание Совета директоров» и «денежно-кредитной политике») с полной датой «\<D\> \<месяц\> \<YYYY\> года»; даты без года (старые табличные секции) игнорируются; дедупликация и сортировка; 0 заседаний → ошибка;
  - в `Import()`: после создания таблицы и витрины — fetch `calMPUrl` → `parseCalMP` → INSERT канонических строк (`category='cbr'`, `title='СД ЦБ РФ: заседание по ключевой ставке'`, `event_time='13:30'`, `threshold='{}'`, `status='pending'`) одним батчем (`PrepareBatch → Append(err!) → Send`); пофетчевая ошибка — warn + успешный выход (таблица/витрина уже созданы), ошибка разбора — ошибка прогона.
- Новый `dagu/calendar.yaml` по образцу одншаговых DAG (bold/gold): `schedule: "7 10 * * *"`, `overlap_policy: skip`, шаг `events_calendar`, `retry_policy: 3 × 300`.

- [ ] **Step 1: Написать падающие тесты**: `TestParseCalMPFixture` — даты сортированы, содержат 2026-10-23, 2026-12-18 и восемь дат 2027 (02-12, 03-19, 04-30, 06-11, 07-30, 09-10, 10-22, 12-17), все — из карточек-заседаний; карточки пресс-конференций в результат не попали (проверка: в результате нет дат, которых нет в списке выше — по фикстуре 2022–2025 заседаний будет больше, пин — «включает 2024-02-16, 2022-02-28», фактические из Task 2); `TestParseCalMPFailFast` — html без заседаний → ошибка.

- [ ] **Step 2: Прогнать — FAIL**.

- [ ] **Step 3: Реализовать** `parseCalMP` (goquery; месяцы — общая карта с Task 3? — своя локальная копия в `calendar/`, чтобы не тянуть `cbr/`) и дописать `Import()`; создать `dagu/calendar.yaml`.

- [ ] **Step 4: Прогнать — PASS**: `go test ./calendar/ -count=1 && go build ./... && gofmt -l calendar/ && dagu validate dagu/calendar.yaml` → exit 0.

- [ ] **Step 5: Commit**: `git add calendar/events.go calendar/events_test.go dagu/calendar.yaml && git commit -m "calendar: fetch CB meeting calendar from cal_mp into events_calendar"`.

---

