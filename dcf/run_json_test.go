package dcf

import (
	"bytes"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
)

// ingestModelRunBody — теневая копия ingest.ModelRun: те же json-теги и типы
// (ingest/model_run.go). Нужна не для сравнения, а как цель строгого декодера:
// ingest принимает тело через json.Decoder с DisallowUnknownFields (ingest/server.go,
// ARCHITECTURE §6.5), поэтому проверять «тело примет ingest» надо тем же способом —
// иначе лишний ключ, незаметный map-разбору, обернётся 400 у человека с напечатанным
// телом в руках. Именно так в первой редакции прошёл top-level inputs, которого в
// контракте нет.
type ingestModelRunBody struct {
	RunID         string             `json:"run_id"`
	TriggerType   string             `json:"trigger_type"`
	TriggerRef    string             `json:"trigger_ref"`
	GoldScenario  map[string]any     `json:"gold_scenario"`
	UsdrubPath    map[string]any     `json:"usdrub_path"`
	PriceDeck     string             `json:"price_deck"`
	DiscountRate  float64            `json:"discount_rate"`
	Wacc          float64            `json:"wacc"`
	NavPerShare   float64            `json:"nav_per_share"`
	NavBull       float64            `json:"nav_bull"`
	NavBase       float64            `json:"nav_base"`
	NavBear       float64            `json:"nav_bear"`
	MarketPrice   float64            `json:"market_price"`
	UpsidePct     float64            `json:"upside_pct"`
	Comment       string             `json:"comment"`
	Probabilities map[string]float64 `json:"probabilities"`
}

// TestModelRunJSONMirrorsIngestContract — тело POST /v1/model_run обязано, во-первых,
// декодироваться строгим декодером ingest (никаких лишних ключей), а во-вторых —
// нести значения, которые ValidateModelRun пропустит: trigger_type из допустимых,
// price_deck из трёх, вероятности ровно bull/base/bear с суммой 100 ±0.1,
// gold_scenario.horizon разбираемого формата и point в каждом сценарии (его
// взвешивает weightedGoldPoint).
//
// Тест офлайн. Строгость декодера — не украшение: DisallowUnknownFields стоит первым
// на пути запроса, до ValidateModelRun, поэтому «лишний ключ не мешает» — неверное
// допущение именно этого сервера (ingest/server.go, decodeBody).
func TestModelRunJSONMirrorsIngestContract(t *testing.T) {
	plans := []MinePlanRecord{
		{Company: "PLZL", Asset: "Olimpiada", Years: []MinePlanYear{{Year: 2027}, {Year: 2028}}},
	}
	decks := map[string][]DeckYear{"own_scenario": {{Year: 2027, GoldUSD: 4600}}}

	body := modelRunJSON(plans, decks, DiscountRates{Industrial: 0.05, Local: 0.16}, defaultParams)
	if body == "" {
		t.Fatal("modelRunJSON вернул пустое тело")
	}

	decoder := json.NewDecoder(bytes.NewReader([]byte(body)))
	decoder.DisallowUnknownFields()

	var decoded ingestModelRunBody
	if err := decoder.Decode(&decoded); err != nil {
		t.Fatalf("ingest отвергнет тело (DisallowUnknownFields): %v (%s)", err, body)
	}

	if decoded.TriggerType != "manual" {
		t.Errorf("trigger_type = %q, want manual", decoded.TriggerType)
	}
	if decoded.PriceDeck != "own_scenario" {
		t.Errorf("price_deck = %q, want own_scenario", decoded.PriceDeck)
	}
	// run_id пуст: без DCF_RUN_ID прогон не пишется, и выдуманный UUID отвязал бы
	// nav_by_asset от model_runs. Валидация ingest отклонит пустой id — это и есть
	// ожидаемая метка «впиши id перед POST».
	if decoded.RunID != "" {
		t.Errorf("run_id = %q, want пустую строку", decoded.RunID)
	}

	var sum float64
	seen := map[string]bool{}
	for key := range decoded.Probabilities {
		seen[key] = true
	}
	for _, key := range []string{"bull", "base", "bear"} {
		if !seen[key] {
			t.Fatalf("probabilities[%q] отсутствует (ключи: %v)", key, seen)
		}
	}
	if len(seen) != 3 {
		t.Errorf("probabilities содержит лишние ключи: %v", seen)
	}
	for _, value := range decoded.Probabilities {
		sum += value
	}
	if math.Abs(sum-100) > 0.1 {
		t.Errorf("сумма вероятностей = %v, want 100 ±0.1", sum)
	}

	horizon, ok := decoded.GoldScenario["horizon"].(string)
	if !ok || !strings.HasPrefix(horizon, "YE-") {
		t.Errorf("gold_scenario.horizon = %v, want YE-YYYY", decoded.GoldScenario["horizon"])
	}
	if year := time.Now().UTC().Format("2006"); !strings.HasSuffix(horizon, year) {
		t.Errorf("horizon = %q, want текущий год %s: зашитый год устарел бы молча", horizon, year)
	}
	for _, name := range []string{"bull", "base", "bear"} {
		block, ok := decoded.GoldScenario[name].(map[string]any)
		if !ok {
			t.Fatalf("gold_scenario.%s = %v, want объект", name, decoded.GoldScenario[name])
		}
		if _, hasPoint := block["point"].(float64); !hasPoint {
			t.Errorf("gold_scenario.%s.point = %v, want число (его взвешивает ingest)", name, block["point"])
		}
	}
	if len(decoded.UsdrubPath) != 0 {
		t.Errorf("usdrub_path непуст: %v — курс в NPV этого среза не входит", decoded.UsdrubPath)
	}

	// Ставка: discount_rate — индустриальный контур (поле §6.2 описано как «0.05 real
	// USD база + надбавки»), wacc — ноль. CAPM/WACC для золотодобычи запрещён (§6.4),
	// а локальная ставка живёт в comment (§6.2), не в wacc.
	if decoded.DiscountRate != 0.05 {
		t.Errorf("discount_rate = %v, want индустриальный контур 0.05", decoded.DiscountRate)
	}
	if decoded.Wacc != 0 {
		t.Errorf("wacc = %v, want 0: WACC для золотодобычи не применяется (§6.4)", decoded.Wacc)
	}

	// Локальный контур и вход прогона обязаны быть видны в comment: отдельного поля
	// контура в контракте ingest нет, а без счётчиков входа напечатанное тело не
	// отличить от прогона вслепую (inputs-ключ для этого не годится — он лишний).
	if !strings.Contains(decoded.Comment, "локальная 0.1600") {
		t.Errorf("comment не содержит локальную ставку: %q", decoded.Comment)
	}
	if !strings.Contains(decoded.Comment, "1 актив,") {
		t.Errorf("comment не содержит число активов: %q", decoded.Comment)
	}
	if !strings.Contains(decoded.Comment, "1 дек;") {
		t.Errorf("comment не содержит число деков: %q", decoded.Comment)
	}
}

// TestPlural — счётчики входа в comment читает человек, поэтому окончание обязано
// быть верным, включая исключения русского счёта (11–14 — «активов», а не «актив»).
func TestPlural(t *testing.T) {
	cases := []struct {
		n    int
		want string
	}{
		{1, "актив"}, {2, "актива"}, {4, "актива"},
		{5, "активов"}, {11, "активов"}, {12, "активов"}, {14, "активов"},
		{21, "актив"}, {22, "актива"}, {25, "активов"}, {111, "активов"}, {121, "актив"},
	}
	for _, c := range cases {
		if got := plural(c.n, "актив", "актива", "активов"); got != c.want {
			t.Errorf("plural(%d) = %q, want %q", c.n, got, c.want)
		}
	}
}

// TestCheckInputsEmptyPlanIsNotAnError — вторая половина Review Focus 1, обновлена
// под Finding 3: Import вызывает checkInputs ТОЛЬКО при непустом плане, и сам вызов
// на пустом плане обязан вернуть nil (иначе перестановка вызова сделала бы пустой
// план фатальным). Карта с ОДНИМ каноническим деком — это одновременно проверка
// нового контракта: неполный набор деков предупреждает, но не падает.
func TestCheckInputsEmptyPlanIsNotAnError(t *testing.T) {
	if err := checkInputs(nil, map[string][]DeckYear{deckSpotFlat: {{Year: 2027, GoldUSD: 4000}}}); err != nil {
		t.Fatalf("checkInputs(nil, decks) = %v, want nil", err)
	}
}

// TestCheckInputsMissingCanonicalDecksWarnButDoNotFail — Finding 3 финального
// ревью: молчаливая потеря канонического дека превращала гарантию «активы × 3 деки
// × 2 контура» в «× 2 × 2», и пропавший дек в nav_by_asset был неотличим от «не
// считали». Проверяем наблюдаемый контракт: функция называет пропавшие деки в логе
// (перехватываем logrus) и при этом возвращает nil — warning, а не error.
//
// Именно лог здесь — носитель инварианта: сигнатура error намеренно не меняется
// (частичный набор деков recoverable), поэтому тест ловит регрессию, только если
// предупреждения нет.
func TestCheckInputsMissingCanonicalDecksWarnButDoNotFail(t *testing.T) {
	plans := []MinePlanRecord{{Company: "PLZL", Asset: "Olimpiada"}}

	cases := []struct {
		name      string
		decks     map[string][]DeckYear
		wantInLog []string // имена деков, которые обязаны быть названы
	}{
		{
			name:      "нет двух канонических деков",
			decks:     map[string][]DeckYear{deckSpotFlat: {{Year: 2027, GoldUSD: 4000}}},
			wantInLog: []string{deckConsensusLT, deckOwnScenario},
		},
		{
			name:      "пустая карта деков после сида",
			decks:     map[string][]DeckYear{},
			wantInLog: nil, // пустая карта — отдельная, более сильная формулировка
		},
		{
			name: "все три канонических дека — предупреждения нет",
			decks: map[string][]DeckYear{
				deckSpotFlat:    {{Year: 2027, GoldUSD: 4000}},
				deckConsensusLT: {{Year: 2027, GoldUSD: 3000}},
				deckOwnScenario: {{Year: 2027, GoldUSD: 4600}},
			},
			wantInLog: nil,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var buf strings.Builder
			prevOut := log.StandardLogger().Out
			log.SetOutput(&buf)
			t.Cleanup(func() { log.SetOutput(prevOut) })

			if err := checkInputs(plans, c.decks); err != nil {
				t.Fatalf("неполный набор деков — предупреждение, а не ошибка: %v", err)
			}

			if len(c.wantInLog) == 0 {
				if strings.Contains(buf.String(), "нет канонических деков") {
					t.Fatalf("полный/пустой набор деков не должен давать предупреждение о пропаже: %q", buf.String())
				}

				return
			}

			got := buf.String()
			for _, name := range c.wantInLog {
				if !strings.Contains(got, name) {
					t.Errorf("предупреждение не называет пропавший дек %q: %q", name, got)
				}
			}
		})
	}
}
