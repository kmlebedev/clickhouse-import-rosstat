package dcf

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

// TestModelRunJSONMirrorsIngestContract — тело POST /v1/model_run обязано быть
// валидным по контракту ingest.ModelRun (ingest/model_run.go): trigger_type из
// допустимых, price_deck из трёх, вероятности с ключами bull/base/bear и суммой
// 100 ±0.1, gold_scenario.horizon разбираемого формата, у каждого сценария есть
// point (именно его взвешивает weightedGoldPoint).
//
// Тест офлайн и намеренно разбирает JSON, а не сравнивает строку: движок печатает
// тело, которое человек отправит в ingest, и расхождение тега или ключа должно
// падать здесь, а не 400-й после ручной отправки. Проверяются только теги и
// значения, задаваемые движком: nav_* и market_price в теле нулевые намеренно.
func TestModelRunJSONMirrorsIngestContract(t *testing.T) {
	plans := []MinePlanRecord{
		{Company: "PLZL", Asset: "Olimpiada", Years: []MinePlanYear{{Year: 2027}, {Year: 2028}}},
	}
	decks := map[string][]DeckYear{"own_scenario": {{Year: 2027, GoldUSD: 4600}}}

	body := modelRunJSON(plans, decks, DiscountRates{Industrial: 0.05, Local: 0.16}, defaultParams)
	if body == "" {
		t.Fatal("modelRunJSON вернул пустое тело")
	}

	var decoded map[string]any
	if err := json.Unmarshal([]byte(body), &decoded); err != nil {
		t.Fatalf("тело не JSON: %v (%s)", err, body)
	}

	if decoded["trigger_type"] != "manual" {
		t.Errorf("trigger_type = %v, want manual", decoded["trigger_type"])
	}
	if decoded["price_deck"] != "own_scenario" {
		t.Errorf("price_deck = %v, want own_scenario", decoded["price_deck"])
	}
	// run_id пуст: без DCF_RUN_ID прогон не пишется, и выдуманный UUID отвязал бы
	// nav_by_asset от model_runs.
	if decoded["run_id"] != "" {
		t.Errorf("run_id = %v, want пустую строку", decoded["run_id"])
	}

	probabilities, ok := decoded["probabilities"].(map[string]any)
	if !ok {
		t.Fatalf("probabilities нет или это не объект: %v", decoded["probabilities"])
	}
	var sum float64
	for _, key := range []string{"bull", "base", "bear"} {
		value, present := probabilities[key]
		if !present {
			t.Fatalf("probabilities[%q] отсутствует", key)
		}
		number, isNumber := value.(float64)
		if !isNumber {
			t.Fatalf("probabilities[%q] = %v, want число", key, value)
		}
		sum += number
	}
	if math.Abs(sum-100) > 0.1 {
		t.Errorf("сумма вероятностей = %v, want 100 ±0.1", sum)
	}

	scenario, ok := decoded["gold_scenario"].(map[string]any)
	if !ok {
		t.Fatalf("gold_scenario нет или это не объект: %v", decoded["gold_scenario"])
	}
	horizon, ok := scenario["horizon"].(string)
	if !ok || !strings.HasPrefix(horizon, "YE-") {
		t.Errorf("gold_scenario.horizon = %v, want YE-YYYY", scenario["horizon"])
	}
	if year := time.Now().UTC().Format("2006"); !strings.HasSuffix(horizon, year) {
		t.Errorf("horizon = %q, want текущий год %s: зашитый год устарел бы молча", horizon, year)
	}
	for _, name := range []string{"bull", "base", "bear"} {
		block, ok := scenario[name].(map[string]any)
		if !ok {
			t.Fatalf("gold_scenario.%s = %v, want объект", name, scenario[name])
		}
		if _, hasPoint := block["point"].(float64); !hasPoint {
			t.Errorf("gold_scenario.%s.point = %v, want число (его взвешивает ingest)", name, block["point"])
		}
	}

	// Вход прогона виден в теле: без него печать не отличалась бы от прогона вслепую.
	inputs, ok := decoded["inputs"].(map[string]any)
	if !ok {
		t.Fatalf("inputs нет или это не объект: %v", decoded["inputs"])
	}
	assets, ok := inputs["assets"].([]any)
	if !ok || len(assets) != 1 {
		t.Fatalf("inputs.assets = %v, want один актив", inputs["assets"])
	}
	decksList, ok := inputs["decks"].([]any)
	if !ok || len(decksList) != 1 || decksList[0] != "own_scenario" {
		t.Fatalf("inputs.decks = %v, want [own_scenario]", inputs["decks"])
	}

	// Контуры попадают в тело оба: §13.1 требует писать оба результата, и читаются они
	// только по паре чисел — discount_rate = индустриальный, wacc = локальный.
	if decoded["discount_rate"] != 0.05 {
		t.Errorf("discount_rate = %v, want индустриальный контур 0.05", decoded["discount_rate"])
	}
	if decoded["wacc"] != 0.16 {
		t.Errorf("wacc = %v, want локальный контур 0.16", decoded["wacc"])
	}
}

// TestCheckInputsEmptyPlanIsNotAnError — вторая половина Review Focus 1: Import
// вызывает checkInputs ТОЛЬКО при непустом плане, но и сам вызов на пустом входе
// обязан возвращать nil — иначе кто-то, переставив вызов, снова сделал бы пустой
// план фатальным.
func TestCheckInputsEmptyPlanIsNotAnError(t *testing.T) {
	if err := checkInputs(nil, map[string][]DeckYear{"spot_flat": {{Year: 2027, GoldUSD: 4000}}}); err != nil {
		t.Fatalf("checkInputs(nil, decks) = %v, want nil", err)
	}
}
