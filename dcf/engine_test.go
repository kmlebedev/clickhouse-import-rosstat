package dcf

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

// TestResolveRunID — Review Focus 2 и 3: run_id приходит только из DCF_RUN_ID,
// и мусор в него попасть не должен.
//
// Пустая строка — ошибка, а не повод сгенерировать UUID: сгенерированный run_id
// не совпал бы с клиентским run_id в POST /v1/model_run, и строки nav_by_asset
// повисли бы без своей строки model_runs — то есть прогон стал бы неотличим от
// чужого. Не-UUID отсекается здесь, до расчёта: ClickHouse отвергнет такую
// строку сам, но уже в середине батча (спека §4).
func TestResolveRunID(t *testing.T) {
	want, _ := uuid.Parse("8f14e45f-ceea-467a-9e1c-2c3b1b1c2b1c")
	if got, err := resolveRunID("8f14e45f-ceea-467a-9e1c-2c3b1b1c2b1c"); err != nil || got != want {
		t.Fatalf("валидный UUID: got %v, err %v", got, err)
	}
	if _, err := resolveRunID(""); err == nil {
		t.Fatal("пустой DCF_RUN_ID обязан давать ошибку: молчаливая генерация run_id " +
			"отвязала бы nav_by_asset от строки model_runs")
	}
	if _, err := resolveRunID("not-a-uuid"); err == nil {
		t.Fatal("не-UUID обязан отсекаться до расчёта")
	}
}

// TestNavRowsKeepsAllDecksAndContours — Review Focus 4 и 5: ключ
// (run_id, deck, asset, contour) обязан различать все комбинации, поэтому
// navRows не имеет права «схлопывать» деки и контуры в одну строку.
//
// Имя отличается от TestNavByAssetKeyKeepsDecksAndContours из schema_test.go
// намеренно: там проверяется DDL, здесь — поведение расчёта; одноимённая
// функция в одном пакете не собралась бы.
//
// Проверяются два монотонных инварианта: дешёвый дек (consensus_lt $3 000)
// даёт NPV ниже дорогого (spot_flat $4 000), а более высокая ставка (local)
// не завышает NPV относительно индустриальной. Оба держатся и на неверном числе
// строк, поэтому проверка количества обязана идти первой.
//
// План здесь ОДНОГОДИЧНЫЙ намеренно (как в брифе): ставка в npvLOM дисконтирует
// по НОМЕРУ года плана, поэтому на единственном году (yearIndex 0) множитель
// 1/(1+rate)^0 = 1, и оба контура дают одно и то же число. Строгая монотонность
// «local < industrial» проверяется отдельным многолетним тестом
// (TestNavRowsLocalContourLowersMultiYearNpv) — на одногодичном плане её требовать
// значило бы требовать расхождения там, где его нет по формуле.
func TestNavRowsKeepsAllDecksAndContours(t *testing.T) {
	plans := []MinePlanRecord{
		{Company: "PLZL", Asset: "Olimpiada", Years: []MinePlanYear{{Year: 2027, ProductionKoz: 100, AISC: 1000}}},
		{Company: "PLZL", Asset: "Blagodatnoye", Years: []MinePlanYear{{Year: 2027, ProductionKoz: 50, AISC: 1100}}},
	}
	decks := map[string][]DeckYear{
		"spot_flat":    {{2027, 4000}},
		"consensus_lt": {{2027, 3000}},
		"own_scenario": {{2027, 4600}},
	}
	rates := DiscountRates{Industrial: 0.05, Local: 0.16}
	p := defaultParams
	p.ProfitTaxPct, p.NdpiBaseUSDPerOz = 0, 0

	rows, err := navRows(uuid.New(), plans, decks, rates, p)
	if err != nil {
		t.Fatal(err)
	}
	// 2 актива × 3 deck × 2 контура = 12 строк; ни одна пара не перетёрта
	if len(rows) != 12 {
		t.Fatalf("rows = %d, want 12: ключ (run_id, deck, asset, contour) обязан различать все комбинации", len(rows))
	}

	seen := map[string]bool{}
	for _, r := range rows {
		key := r.Deck + "|" + r.Asset + "|" + r.Contour
		if seen[key] {
			t.Fatalf("дубль ключа %s — deck/contour перетираются", key)
		}
		seen[key] = true

		if r.StageHaircut != nil {
			t.Fatalf("stage_haircut = %v, want nil: стадийные haircut'ы вне этого среза", *r.StageHaircut)
		}
	}
	// Деки обязаны присутствовать все три, а не только те, что есть в мапе по
	// случаю: расчёт по умолчанию — по всем трём контурам цены (§6.4).
	for _, deck := range []string{"spot_flat", "consensus_lt", "own_scenario"} {
		if !seen[deck+"|Olimpiada|industrial"] {
			t.Errorf("дека %q нет в результате: §6.4 требует все три", deck)
		}
	}

	byKey := map[string]float64{}
	for _, r := range rows {
		byKey[r.Deck+"|"+r.Asset+"|"+r.Contour] = r.NPVUSDmln
	}
	// дешёвый deck даёт меньший NPV: consensus_lt $3 000 против spot_flat $4 000
	if !(byKey["consensus_lt|Olimpiada|industrial"] < byKey["spot_flat|Olimpiada|industrial"]) {
		t.Fatal("NPV на consensus_lt обязан быть ниже, чем на spot_flat")
	}
	// Одногодичный план: дисконт не действует (yearIndex 0), поэтому высокая ставка
	// локального контура НЕ обязана занижать NPV — она обязана лишь не завышать его
	// (не перепутаны ставки контуров). Строгое «<» — в многолетнем тесте ниже.
	if byKey["spot_flat|Olimpiada|local"] > byKey["spot_flat|Olimpiada|industrial"] {
		t.Fatal("локальный контур (ставка выше) не имеет права давать NPV ВЫШЕ индустриального")
	}
	// На одногодичном плане оба контура обязаны совпасть ровно: множитель дисконта
	// для yearIndex 0 — единица, и любое расхождение означало бы, что ставка влияет
	// на поток как-то помимо дисконта (например, подмешана в налоговую базу).
	if byKey["spot_flat|Olimpiada|local"] != byKey["spot_flat|Olimpiada|industrial"] {
		t.Fatalf("одногодичный план: контуры обязаны совпасть, local %v, industrial %v",
			byKey["spot_flat|Olimpiada|local"], byKey["spot_flat|Olimpiada|industrial"])
	}

	// Ставка в строке — ставка своего контура, а не одна и та же для обоих:
	// иначе два контура различимы только по имени, и «разные ставки» в
	// nav_by_asset были бы вымыслом.
	for _, r := range rows {
		switch r.Contour {
		case ContourIndustrial:
			if r.DiscountRate != rates.Industrial {
				t.Errorf("industrial: discount_rate = %v, want %v", r.DiscountRate, rates.Industrial)
			}
		case ContourLocal:
			if r.DiscountRate != rates.Local {
				t.Errorf("local: discount_rate = %v, want %v", r.DiscountRate, rates.Local)
			}
		default:
			t.Errorf("неизвестный контур %q", r.Contour)
		}
	}
}

// TestNavRowsLocalContourLowersMultiYearNpv — строгая проверка «высокая ставка
// снижает NPV». Нужен именно многолетний план: на одногодичном дисконт не
// действует (см. комментарий выше), и инвариант был бы неотличим от нулевой
// функции. Здесь годы 1 и 2 дисконтируются по-разному, поэтому локальный контур
// обязан дать строго меньше индустриального.
func TestNavRowsLocalContourLowersMultiYearNpv(t *testing.T) {
	plans := []MinePlanRecord{{Company: "PLZL", Asset: "Olimpiada", Years: []MinePlanYear{
		{Year: 2027, ProductionKoz: 100, AISC: 1000},
		{Year: 2028, ProductionKoz: 100, AISC: 1000},
		{Year: 2029, ProductionKoz: 100, AISC: 1000},
	}}}
	decks := map[string][]DeckYear{"spot_flat": {{2027, 4000}, {2028, 4000}, {2029, 4000}}}
	rates := DiscountRates{Industrial: 0.05, Local: 0.16}
	p := defaultParams
	p.ProfitTaxPct, p.NdpiBaseUSDPerOz = 0, 0

	rows, err := navRows(uuid.New(), plans, decks, rates, p)
	if err != nil {
		t.Fatal(err)
	}

	var industrial, local float64
	for _, r := range rows {
		switch r.Contour {
		case ContourIndustrial:
			industrial = r.NPVUSDmln
		case ContourLocal:
			local = r.NPVUSDmln
		}
	}
	if !(local < industrial) {
		t.Fatalf("на многолетнем плане локальный контур обязан давать NPV строго ниже: local %v, industrial %v",
			local, industrial)
	}
}

// TestNavRowsEmptyDeckForAssetSkipsWithoutPanic — дек, присутствующий в мапе,
// но без года этого актива, не должен ронять расчёт. Контракт: цена
// неизвестна → строку всё равно пишем с NPV 0 (пропущенный год npvLOM
// игнорирует с предупреждением), но именно строку, а не панику на пустом
// слайсе. Так «дека есть, цены нет» не превращается в дыру в nav_by_asset,
// которую потом не отличить от «актив не считали».
func TestNavRowsEmptyDeckForAssetSkipsWithoutPanic(t *testing.T) {
	plans := []MinePlanRecord{{Company: "PLZL", Asset: "Sukhoi Log", Years: []MinePlanYear{{Year: 2030, ProductionKoz: 400, AISC: 900}}}}
	decks := map[string][]DeckYear{"spot_flat": nil} // в мапе есть, годов нет
	rates := DiscountRates{Industrial: 0.05, Local: 0.16}

	rows, err := navRows(uuid.New(), plans, decks, rates, defaultParams)
	if err != nil {
		t.Fatalf("пустой список годов дека не ошибка: %v", err)
	}
	if len(rows) != 2 { // 1 актив × 1 дек × 2 контура
		t.Fatalf("rows = %d, want 2", len(rows))
	}
	for _, r := range rows {
		if r.NPVUSDmln != 0 {
			t.Errorf("без цены года NPV обязан быть 0, got %v", r.NPVUSDmln)
		}
	}
}

// TestNavRowsIsDeterministic — порядок строк обязан быть воспроизводимым:
// итерация по map в Go рандомизирована, поэтому navRows сортирует имена
// деков. Не отсортировать — значит получить разный порядок вставки в
// nav_by_asset между прогонами, а для ReplacingMergeTree и для diff'а двух
// прогонов это лишний источник шума.
func TestNavRowsIsDeterministic(t *testing.T) {
	plans := []MinePlanRecord{{Company: "PLZL", Asset: "Olimpiada", Years: []MinePlanYear{{Year: 2027, ProductionKoz: 100, AISC: 1000}}}}
	decks := map[string][]DeckYear{
		"spot_flat":    {{2027, 4000}},
		"consensus_lt": {{2027, 3000}},
		"own_scenario": {{2027, 4600}},
	}
	rates := DiscountRates{Industrial: 0.05, Local: 0.16}

	first, err := navRows(uuid.Nil, plans, decks, rates, defaultParams)
	if err != nil {
		t.Fatal(err)
	}
	order := func(rows []NavRow) string {
		var b strings.Builder
		for _, r := range rows {
			b.WriteString(r.Deck + "|" + r.Asset + "|" + r.Contour + ";")
		}
		return b.String()
	}
	want := order(first)
	for i := 0; i < 8; i++ { // несколько прогонов ловят случайный порядок map
		got, err := navRows(uuid.Nil, plans, decks, rates, defaultParams)
		if err != nil {
			t.Fatal(err)
		}
		if order(got) != want {
			t.Fatalf("порядок строк плавает между прогонами: %s != %s", order(got), want)
		}
	}
	// Деки отсортированы лексикографически — предсказуемо и не зависит от map.
	if first[0].Deck != "consensus_lt" || first[1].Deck != "consensus_lt" ||
		first[2].Deck != "own_scenario" || first[4].Deck != "spot_flat" {
		t.Fatalf("деки не отсортированы: первые строки %+v", first[:4])
	}
}

// TestPlanEmptyDoesNotFail — Review Focus 1: пустой mine_plans не ошибка, иначе
// `make import STAT=dcf_engine` и DAG краснеют на dev-БД, где сида ещё нет.
//
// Второй и третий кейсы проверяют контракт, ставший реальным в Finding 3: проверка
// полноты деков ПРЕДУПРЕЖДАЕТ, а не падает. Неполный набор деков — recoverable
// состояние БД (деки, которых нет, не считаются, остальные считаются), и падение
// здесь краснило бы DAG на порядке шагов, а не на данных. Поэтому все кейсы ниже
// обязаны вернуть nil; видимость пропажи обеспечивает лог, а не error.
func TestPlanEmptyDoesNotFail(t *testing.T) {
	if err := checkInputs(nil, nil); err != nil {
		t.Fatalf("пустой mine_plans — не ошибка: %v", err)
	}
	if err := checkInputs([]MinePlanRecord{{Company: "PLZL", Asset: "Olimpiada"}}, nil); err != nil {
		t.Fatalf("непустой план с пустыми деками — предупреждение, а не ошибка: %v", err)
	}
	if err := checkInputs([]MinePlanRecord{{Company: "PLZL", Asset: "Olimpiada"}},
		map[string][]DeckYear{deckSpotFlat: {{Year: 2027, GoldUSD: 4000}}}); err != nil {
		t.Fatalf("частичный набор деков — предупреждение, а не ошибка: %v", err)
	}
}
