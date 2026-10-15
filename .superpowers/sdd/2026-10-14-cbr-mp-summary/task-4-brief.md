### Task 4: Реестр 11 каналов и regex-разметка

**Files:**
- Create: `cbr/mp_summary_channels.go`
- Test: `cbr/mp_summary_channels_test.go`

**Interfaces:**
- Consumes: `mpSummaryBlock` (Task 3).
- Produces (используют Task 5–6):
  - `type mpChannel struct { Slug, Title, Sign string; Patterns []*regexp.Regexp }`
  - `var mpChannels = []mpChannel{…}` — ровно 11 из спеки §3.4 (slug/имя/направление/паттерны; паттерны по стемам, например для `labor_market`: `безработиц`, `рынок труда`, `дефицит кадров`, `заработн`; для `expectations`: `инфляционн\w* ожидан`, `ценов\w* ожидан`; и т.п.);
  - `type mpChannelMatch struct { Channel, Kind string; Seq int; Text string }`
  - `func tagMPChannels(blocks []mpSummaryBlock) []mpChannelMatch` — пункты `bullet` из блока «Инфляционные риски» → `kind=risk_pro|risk_dez` (по тому, в чей список попал пункт: после подстроки «проинфляционные риски включают» / «дезинфляционные риски включают»); предложения прозы (`paragraph`) остальных блоков → `kind=prose`; `Seq` — сквозной; `Text` — дословный (нормализованный) текст пункта/предложения.

- [ ] **Step 1: Написать падающие тесты**: `TestMPChannelsRegistry` (11 записей, slug уникальны, у каждой непустые Title/Sign/Patterns); `TestTagMPChannelsHoldFixture` — на 23092026: есть матчи `expectations` с текстом, содержащим «инфляционн» и «ожидан»; есть ≥1 `risk_pro` и ≥1 `risk_dez`; `labor_market` содержит предложение с «безработиц» (фактическое из Task 2 — пин); негативный: предложение про нефть/Brent не попало в `labor_market`; `TestTagMPChannelsCounts` — числа матчей по каналам на фикстуре hold — фактические из Task 2.

- [ ] **Step 2: Прогнать — FAIL**.

- [ ] **Step 3: Реализовать** реестр и `tagMPChannels` (сплит прозы на предложения: по `(?<=[.!?])\s+` после нормализации; один матч на канал на предложение — дедупликация по `(Channel, Kind, Text)`).

- [ ] **Step 4: Прогнать — PASS**: `go test ./cbr/ -run TestMPChannels -v && go test ./cbr/ -count=1 && gofmt -l cbr/`.

- [ ] **Step 5: Commit**: `git add cbr/mp_summary_channels.go cbr/mp_summary_channels_test.go && git commit -m "cbr: 11-channel registry and regex tagging for mp summary"`.

---

