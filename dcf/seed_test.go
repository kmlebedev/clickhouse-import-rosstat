package dcf

import (
	"math"
	"strings"
	"testing"
)

// TestAssertSpotPriceRejectsNonPositive — Finding 1 ревью: сид spot_flat берёт
// цену через `argMax(usd, date) FROM gold_prices`, а argMax по ПУСТОМУ множеству
// возвращает нулевое значение типа (0 для Float64), НЕ NULL и без исключения
// (проверено на живом ClickHouse: `SELECT argMax(number, number+1) FROM numbers(0)`
// → 0). gold_prices наполняется отдельным шагом DAG, поэтому без этой проверки
// прогон сида до импорта золота молча записал бы spot_flat с ценой $0/oz — вход
// всего расчёта, занижённый в бесконечность раз, при успешном логе сида.
//
// Проверка вынесена в чистую assertSpotPrice, потому что воспроизвести «таблица
// есть, но пуста» на живой БД в юнит-тесте нельзя, а сама граница — здесь.
func TestAssertSpotPriceRejectsNonPositive(t *testing.T) {
	// Ноль — главный случай: именно его даёт argMax по пустой gold_prices.
	if err := assertSpotPrice(0); err == nil {
		t.Fatal("spot = 0 обязан быть ошибкой: argMax по пустому множеству даёт 0, " +
			"и spot_flat записался бы с ценой $0/oz")
	}
	// Отрицательная цена — тот же класс мусора, что и ноль.
	if err := assertSpotPrice(-1); err == nil {
		t.Fatal("отрицательный spot обязан быть ошибкой")
	}

	// Нечисловой мусор: NaN не проходит сравнение `> 0` — тоже ошибка.
	if err := assertSpotPrice(math.NaN()); err == nil {
		t.Fatal("NaN обязан быть ошибкой: !(NaN > 0) истинно")
	}

	// Сообщение обязано называть вход (gold_prices / moex_fix_usd), иначе дежурный
	// по DAG не поймёт, что наполнять, и будет искать причину в сиде.
	err := assertSpotPrice(0)
	if err == nil {
		t.Fatal("ожидалась ошибка на нулевом spot")
	}
	for _, want := range []string{"gold_prices", "moex_fix_usd", "spot_flat"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("сообщение об ошибке не упоминает %q: %q", want, err.Error())
		}
	}

	// Реальная цена — не ошибка.
	for _, ok := range []float64{0.01, 4250.10, 1e6} {
		if err := assertSpotPrice(ok); err != nil {
			t.Errorf("положительный spot %v не должен быть ошибкой: %v", ok, err)
		}
	}
}

// TestPlanYears — горизонт сида берётся из планов, а не из часов.
func TestPlanYears(t *testing.T) {
	plans := []MinePlanRecord{
		{Company: "PLZL", Asset: "A", Years: []MinePlanYear{{Year: 2030}, {Year: 2028}}},
		{Company: "PLZL", Asset: "B", Years: []MinePlanYear{{Year: 2029}, {Year: 2028}}},
	}
	got := planYears(plans)
	want := []uint16{2028, 2029, 2030}
	if len(got) != len(want) {
		t.Fatalf("planYears = %v, want %v (уникальные годы, отсортированы)", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("planYears[%d] = %d, want %d", i, got[i], want[i])
		}
	}
	if got := planYears(nil); len(got) != 0 {
		t.Fatalf("пустой план: planYears = %v, want пусто", got)
	}
}

// TestPlanYearsRejectsImplausibleYear — Finding 1 финального ревью: год плана вне
// окна minPlanYear..maxPlanYear не должен молча сеяться.
//
// Сценарий провала без правки: мусорный год (1969, 2100) попадал в planYears,
// сид писал дек price_decks ровно на этот год, checkYearCoverage видел
// пересечение «год-в-год», возвращал nil — и прогон заканчивался успехом с
// бессмысленным NAV. Это ровно тот класс молчаливого провала, против которого
// срез и делался: гард защищал «деки расходятся с планами», но не «сам план
// осмыслен».
//
// Проверяются обе половины защиты: (1) planYears выбрасывает мусорный год из
// сеива; (2) checkYearCoverage уводит план из ОДНИХ мусорных лет в ошибку, даже
// если дек стоит ровно на мусорный год.
func TestPlanYearsRejectsImplausibleYear(t *testing.T) {
	// Границы окна — санитарная проверка, что они заданы осмысленно и не
	// перепутаны местами (иначе тест ниже был бы вакуумным).
	if minPlanYear >= maxPlanYear {
		t.Fatalf("окно годов задано наоборот: minPlanYear=%d, maxPlanYear=%d", minPlanYear, maxPlanYear)
	}

	if plausiblePlanYear(1969) {
		t.Errorf("1969 обязан быть неправдоподобным: датапак Полюса начинается с 2007, плана 1969 года быть не может")
	}
	if plausiblePlanYear(2100) {
		t.Errorf("2100 обязан быть неправдоподобным: самый длинный опубликованный горизонт кончается в 2040-х")
	}
	if !plausiblePlanYear(minPlanYear) {
		t.Errorf("левая граница окна %d обязана быть включительно правдоподобной", minPlanYear)
	}
	if plausiblePlanYear(maxPlanYear) {
		t.Errorf("правая граница окна %d исключена: 2100 — канонический мусорный год", maxPlanYear)
	}

	// (1) Мусорный год ВЫПАДАЕТ из списка годов сида; валидные остаются.
	plans := []MinePlanRecord{
		{Company: "PLZL", Asset: "A", Years: []MinePlanYear{{Year: 2028}, {Year: 2100}, {Year: 1969}}},
	}
	got := planYears(plans)
	want := []uint16{2028}
	if len(got) != len(want) {
		t.Fatalf("planYears = %v, want %v — мусорные годы 1969/2100 не должны сеяться", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("planYears[%d] = %d, want %d", i, got[i], want[i])
		}
	}

	// (2) План из ОДНИХ мусорных лет при деке ровно на мусорный год: пересечение
	// «год-в-год» больше не спасает — это обязано быть ошибкой, а не nil.
	junk := []MinePlanRecord{{Company: "PLZL", Asset: "A", Years: []MinePlanYear{{Year: 2100}}}}
	junkDeck := map[string][]DeckYear{"spot_flat": {{Year: 2100, GoldUSD: 4000}}}
	if err := checkYearCoverage(junk, junkDeck); err == nil {
		t.Fatal("план из мусорного года 2100 с деком РОВНО на 2100 обязан дать ошибку: " +
			"иначе сид засеет дек на 2100, гард увидит пересечение и прогон завершится " +
			"успехом с бессмысленным NAV")
	}

	// Мусорный год в planYearSet не попадает — прямое утверждение контракта, на
	// котором стоит решение checkYearCoverage выше.
	if _, ok := planYearSet(junk)[2100]; ok {
		t.Fatal("planYearSet не должен считать мусорный 2100 годом плана")
	}
}
