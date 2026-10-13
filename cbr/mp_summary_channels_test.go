package cbr

import (
	"strings"
	"testing"
)

// mpRegistrySlugs — 11 slug'ов реестра каналов в порядке таблицы спеки §3.4.
func mpRegistrySlugs() []string {
	return []string{
		"current_inflation",
		"core_inflation",
		"expectations",
		"labor_market",
		"lending_transmission",
		"output_gap",
		"budget_impulse",
		"money_supply",
		"external_fx",
		"savings_behavior",
		"autonomous_factors",
	}
}

// mpChannelCorpus — склейка нормализованных текстов всех текстовых узлов документа.
// Каждая цитата разметки обязана входить в неё дословно (иначе сломается сверка LLM-слоя).
func mpChannelCorpus(blocks []mpSummaryBlock) string {
	var sb strings.Builder
	for _, b := range blocks {
		sb.WriteString(b.Text)
		sb.WriteString("\n")
	}
	return sb.String()
}

// mpBlockBullets — склейка текстов пунктов списка одного блока: источник risk_-матчей.
func mpBlockBullets(blocks []mpSummaryBlock, block string) string {
	var sb strings.Builder
	for _, b := range blocks {
		if b.Block == block && b.Kind == mpKindBullet {
			sb.WriteString(b.Text)
			sb.WriteString("\n")
		}
	}
	return sb.String()
}

// mpMatchTexts — тексты матчей канала (все виды), в порядке следования.
func mpMatchTexts(matches []mpChannelMatch, channel string) []string {
	var out []string
	for _, m := range matches {
		if m.Channel == channel {
			out = append(out, m.Text)
		}
	}
	return out
}

// mpHasMatchText — есть ли матч канала (любого вида) ровно с этим текстом.
func mpHasMatchText(matches []mpChannelMatch, channel, text string) bool {
	for _, m := range matches {
		if m.Channel == channel && m.Text == text {
			return true
		}
	}
	return false
}

// mpHasTextWith — есть ли матч канала (любого вида) с подстрокой sub (регистронезависимо).
func mpHasTextWith(matches []mpChannelMatch, channel, sub string) bool {
	sub = strings.ToLower(sub)
	for _, m := range matches {
		if m.Channel == channel && strings.Contains(strings.ToLower(m.Text), sub) {
			return true
		}
	}
	return false
}

// mpMatchCounts — число матчей по каналам (все виды).
func mpMatchCounts(matches []mpChannelMatch) map[string]int {
	out := make(map[string]int, len(mpChannels))
	for _, m := range matches {
		out[m.Channel]++
	}
	return out
}

// mpCheckMatchShared — общие инварианты разметки: непустой текст, дословность (Text —
// подстрока нормализованного документа), Seq — сквозной неубывающий с 1, канал — из реестра,
// вид — из ожидаемых, дублей (Channel, Kind, Text) нет.
func mpCheckMatchShared(t *testing.T, blocks []mpSummaryBlock, matches []mpChannelMatch) {
	t.Helper()
	corpus := mpChannelCorpus(blocks)
	slugs := map[string]bool{}
	for _, s := range mpRegistrySlugs() {
		slugs[s] = true
	}
	kinds := map[string]bool{mpKindRiskPro: true, mpKindRiskDez: true, mpKindProse: true}
	type key struct{ channel, kind, text string }
	seen := map[key]bool{}
	lastSeq := 0
	for i, m := range matches {
		if m.Text == "" {
			t.Errorf("матч %d (%s): пустой текст", i, m.Channel)
		}
		if !strings.Contains(corpus, m.Text) {
			t.Errorf("матч %d (%s): текст не дословный: %q", i, m.Channel, m.Text)
		}
		if !slugs[m.Channel] {
			t.Errorf("матч %d: канал %q не из реестра", i, m.Channel)
		}
		if !kinds[m.Kind] {
			t.Errorf("матч %d (%s): вид %q не из risk_pro|risk_dez|prose", i, m.Channel, m.Kind)
		}
		if m.Seq <= 0 {
			t.Errorf("матч %d (%s): Seq=%d, ожидался сквозной с 1", i, m.Channel, m.Seq)
		}
		if m.Seq < lastSeq {
			t.Errorf("матч %d (%s): Seq=%d убывает после %d", i, m.Channel, m.Seq, lastSeq)
		}
		lastSeq = m.Seq
		k := key{m.Channel, m.Kind, m.Text}
		if seen[k] {
			t.Errorf("матч %d: дубль (Channel, Kind, Text) = (%s, %s, %q)", i, m.Channel, m.Kind, m.Text)
		}
		seen[k] = true
	}
}

// TestMPChannelsRegistry — реестр каналов: ровно 11 записей из спеки §3.4, slug'и уникальны,
// у каждой записи непустые Title/Sign/Patterns, узоры — непустые строки в нижнем регистре
// (применяются к нормализованному тексту, приведённому к нижнему регистру).
func TestMPChannelsRegistry(t *testing.T) {
	if len(mpChannels) != 11 {
		t.Fatalf("каналов %d, ожидалось 11", len(mpChannels))
	}
	want := mpRegistrySlugs()
	if len(want) != 11 {
		t.Fatalf("эталонный список slug'ов: %d записей, ожидалось 11", len(want))
	}
	seen := map[string]bool{}
	for i, ch := range mpChannels {
		if ch.Slug == "" || ch.Title == "" || ch.Sign == "" {
			t.Errorf("канал %d (%q): пустое поле Slug/Title/Sign (%+v)", i, ch.Slug, ch)
		}
		if seen[ch.Slug] {
			t.Errorf("канал %d: slug %q повторяется", i, ch.Slug)
		}
		seen[ch.Slug] = true
		if i < len(want) && ch.Slug != want[i] {
			t.Errorf("канал %d: slug %q, ожидался %q (порядок спеки §3.4)", i, ch.Slug, want[i])
		}
		if len(ch.Patterns) == 0 {
			t.Errorf("канал %q: пустой набор узоров", ch.Slug)
		}
		for j, re := range ch.Patterns {
			if re == nil || re.String() == "" {
				t.Errorf("канал %q: узор %d пустой", ch.Slug, j)
				continue
			}
			if re.String() != strings.ToLower(re.String()) {
				t.Errorf("канал %q: узор %d %q не в нижнем регистре", ch.Slug, j, re.String())
			}
		}
	}
	for _, slug := range want {
		if !seen[slug] {
			t.Errorf("в реестре нет канала %q", slug)
		}
	}
}

// TestMPChannelsHoldFixture — разметка резюме 23.09.2026 (hold): качественные пины по
// трём источникам матчей — инфляционные ожидания (проза, блок 1), пункты списков рисков
// (risk_pro/risk_dez, блок 4), рынок труда (проза); негативный пин — предложение про нефть
// не попадает в labor_market. Цитаты — дословно из task-2-report.md §3.2.
func TestMPChannelsHoldFixture(t *testing.T) {
	doc, err := parseMPSummary(mpFixture(t, "mp_summary_23092026.html"))
	if err != nil {
		t.Fatalf("parseMPSummary: %v", err)
	}
	matches := tagMPChannels(doc.Blocks)
	if len(matches) == 0 {
		t.Fatal("tagMPChannels: ноль матчей на документе")
	}
	mpCheckMatchShared(t, doc.Blocks, matches)

	// (а) expectations — проза: дословный кандидат §3.2 (блок 1, «Основные факты»).
	const expectationsPin = "Инфляционные ожидания населения в августе снизились, тогда как ценовые ожидания бизнеса в августе \u2013 сентябре несколько выросли."
	lowPin := strings.ToLower(expectationsPin)
	if !strings.Contains(lowPin, "ожидан") || !strings.Contains(lowPin, "инфляционн") {
		t.Fatalf("пин expectations не содержит «инфляционн»/«ожидан»: %q", expectationsPin)
	}
	if !mpHasMatchText(matches, "expectations", expectationsPin) {
		t.Errorf("expectations: нет матча с текстом %q (есть: %v)",
			expectationsPin, mpMatchTexts(matches, "expectations"))
	}
	if !mpHasTextWith(matches, "expectations", "ожидан") {
		t.Errorf("expectations: нет ни одного матча со словом «ожидан»")
	}

	// (б) списки рисков блока 4: пункты до заголовка «дезинфляционные …» — risk_pro,
	// после — risk_dez; оба вида непусты, матчи берутся только из пунктов (не из абзацев).
	var pro, dez []string
	bulletCorpus := mpBlockBullets(doc.Blocks, "Инфляционные риски")
	for _, m := range matches {
		switch m.Kind {
		case mpKindRiskPro:
			pro = append(pro, m.Text)
		case mpKindRiskDez:
			dez = append(dez, m.Text)
		}
	}
	if len(pro) == 0 {
		t.Error("risk_pro: ноль матчей в блоке «Инфляционные риски»")
	}
	if len(dez) == 0 {
		t.Error("risk_dez: ноль матчей в блоке «Инфляционные риски»")
	}
	for _, m := range matches {
		if m.Kind != mpKindRiskPro && m.Kind != mpKindRiskDez {
			continue
		}
		if !strings.Contains(bulletCorpus, m.Text) {
			t.Errorf("%s: текст %q не из пунктов списка блока 4", m.Kind, m.Text)
		}
	}
	// Дословные первые предложения пунктов 3 (проинфляционный) и 8 (дезинфляционный).
	const (
		riskProPin = "Усиление дисбаланса спроса и предложения."
		riskDezPin = "Значительное замедление роста внутреннего спроса."
	)
	if !mpHasMatchText(matches, "output_gap", riskProPin) {
		t.Errorf("risk_pro: нет матча %q (output_gap): %v", riskProPin, pro)
	}
	if !mpHasMatchText(matches, "output_gap", riskDezPin) {
		t.Errorf("risk_dez: нет матча %q (output_gap): %v", riskDezPin, dez)
	}

	// (в) labor_market — проза: дословные кандидаты §3.2 (безработица и напряжённость).
	const (
		laborPin  = "Дефицит работников сокращался, безработица несколько выросла, темпы роста номинальных и реальных заработных плат замедлились."
		laborPin2 = "Напряженность на рынке труда продолжала постепенно снижаться."
	)
	for _, pin := range []string{laborPin, laborPin2} {
		if !mpHasMatchText(matches, "labor_market", pin) {
			t.Errorf("labor_market: нет матча с текстом %q (есть: %v)", pin, mpMatchTexts(matches, "labor_market"))
		}
	}
	if !mpHasTextWith(matches, "labor_market", "безработиц") {
		t.Error("labor_market: нет матча со словом «безработиц»")
	}

	// (г) негативный пин: предложение про нефть (блок 3) размечено как external_fx, но не
	// как labor_market — иначе регексы рынка труда ловят «топливный рынок»/«рынок» вообще.
	const oilPin = "Цены на нефть складывались выше июльского прогноза на фоне сохраняющихся ограничений судоходства через Ормузский пролив."
	if !mpHasMatchText(matches, "external_fx", oilPin) {
		t.Errorf("external_fx: нет матча с нефтяным предложением %q", oilPin)
	}
	if mpHasMatchText(matches, "labor_market", oilPin) {
		t.Errorf("labor_market ошибочно захватил предложение про нефть: %q", oilPin)
	}
	for _, m := range matches {
		if m.Channel != "labor_market" {
			continue
		}
		low := strings.ToLower(m.Text)
		if strings.Contains(low, "нефт") || strings.Contains(low, "brent") {
			t.Errorf("labor_market: нефтяное предложение %q", m.Text)
		}
	}

	// Одно предложение может попасть в несколько каналов: пункт 3 списка проинфляционных
	// рисков говорит и о перегреве спроса, и о кредитовании, и о бюджете, и о рынке труда.
	const multiPin = "Внутренний спрос может оставаться высоким под влиянием ускорения кредитования, значительного бюджетного импульса и продолжающегося роста заработных плат темпами выше производительности труда."
	var multi []string
	for _, m := range matches {
		if m.Text == multiPin {
			multi = append(multi, m.Channel)
		}
	}
	if len(multi) < 2 {
		t.Errorf("предложение %q размечено каналами %v, ожидалось ≥2 (пересечение каналов)",
			multiPin, multi)
	}
}

// TestMPChannelsCounts — числа матчей по каналам на резюме 23.09.2026 (hold). Значения —
// фактические из реализации (зафиксированы после зелёных качественных пинов выше), чтобы
// правка узора, меняющая охват, падала здесь с конкретным числом.
func TestMPChannelsCounts(t *testing.T) {
	doc, err := parseMPSummary(mpFixture(t, "mp_summary_23092026.html"))
	if err != nil {
		t.Fatalf("parseMPSummary: %v", err)
	}
	matches := tagMPChannels(doc.Blocks)
	got := mpMatchCounts(matches)
	// Фактические числа реализации на 23.09.2026 (hold); сумма = 158.
	want := map[string]int{
		"current_inflation":    16,
		"core_inflation":       12,
		"expectations":         10,
		"labor_market":         8,
		"lending_transmission": 34,
		"output_gap":           20,
		"budget_impulse":       22,
		"money_supply":         4,
		"external_fx":          25,
		"savings_behavior":     6,
		"autonomous_factors":   1,
	}
	if len(want) != len(mpChannels) {
		t.Fatalf("пин покрывает %d каналов, в реестре %d", len(want), len(mpChannels))
	}
	total := 0
	for slug, n := range want {
		if got[slug] != n {
			t.Errorf("канал %s: матчей %d, ожидалось %d", slug, got[slug], n)
		}
		total += n
	}
	if total != len(matches) {
		t.Errorf("сумма пина по каналам %d, матчей всего %d", total, len(matches))
	}
	for slug, n := range got {
		if _, ok := want[slug]; !ok && n > 0 {
			t.Errorf("канал %q вне пина: %d матчей", slug, n)
		}
	}
	// Виды: пункты списков рисков (проинфляционный + дезинфляционный списки блока 4) дают
	// risk_pro/risk_dez, все остальные абзацы и пункты — prose.
	wantKinds := map[string]int{
		mpKindProse:   133,
		mpKindRiskPro: 19,
		mpKindRiskDez: 6,
	}
	kinds := map[string]int{}
	for _, m := range matches {
		kinds[m.Kind]++
	}
	for kind, n := range wantKinds {
		if kinds[kind] != n {
			t.Errorf("вид %s: матчей %d, ожидалось %d", kind, kinds[kind], n)
		}
	}
	if len(kinds) != len(wantKinds) {
		t.Errorf("виды матчей %v, ожидались только %v", kinds, wantKinds)
	}
}
