package dcf

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	log "github.com/sirupsen/logrus"
)

// Допущения сценарной сетки — те же числа, что записаны в каноническом блоке §6.4
// ARCHITECTURE.md (сценарии золота Q4-2026: bull $4 600–5 000 25%, base $4 000–4 600
// 40%, bear $3 750–4 050 35%). Вынесены в константы, чтобы тело POST /v1/model_run
// не расходилось с документом при правке одного из них.
const (
	scenarioBullPointUSD = 4800.0
	scenarioBullLowUSD   = 4600.0
	scenarioBullHighUSD  = 5000.0

	scenarioBasePointUSD = 4300.0
	scenarioBaseLowUSD   = 4000.0
	scenarioBaseHighUSD  = 4600.0

	scenarioBearPointUSD = 3900.0
	scenarioBearLowUSD   = 3750.0
	scenarioBearHighUSD  = 4050.0
)

// scenarioProbabilitiesPct — вероятности сценариев, % (сумма ровно 100, как требует
// ValidateModelRun с допуском 0.1).
var scenarioProbabilitiesPct = map[string]float64{
	"bull": 25,
	"base": 40,
	"bear": 35,
}

// ingestModelRun — зеркало ingest.ModelRun (ingest/model_run.go): те же json-теги и
// те же типы. Отдельная структура, а не импорт ingest, потому что dcf/ не имеет
// права зависеть от ingest-контура: движок обязан собираться без серверного кода,
// а совпадение контракта достаточно проверять тестом на JSON. Поля результата
// (nav_*, market_price, upside_pct) в теле есть, но нулевые: их заполняет тот, кто
// считает NAV/акцию, — движок в model_runs не пишет.
type ingestModelRun struct {
	RunID string `json:"run_id"`
	// Inputs — не поле ingest.ModelRun: это фактический вход прогона (планы и деки),
	// вложенный в тело, чтобы напечатанный JSON не был неотличим от прогона вслепую.
	// ValidateModelRun читает только известные ему поля и лишний ключ игнорирует.
	Inputs        inputsJSON         `json:"inputs"`
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

// inputsJSON — фактический вход расчёта: сколько активов и по скольким годам, какие
// деки прочитаны. Годы и имена активов нужны, чтобы человек по одному взгляду на
// напечатанное тело видел, что план не пуст и LOM-горизонт тот, который он засеял.
type inputsJSON struct {
	Assets []assetPlanJSON `json:"assets"`
	Decks  []string        `json:"decks"`
}

// assetPlanJSON — один актив во входе: компания, имя и годы LOM-плана.
type assetPlanJSON struct {
	Company string   `json:"company"`
	Asset   string   `json:"asset"`
	Years   []uint16 `json:"years"`
}

// modelRunJSON собирает тело POST /v1/model_run по контракту ingest.ModelRun
// (ingest/model_run.go) — то, что движок печатает, когда DCF_RUN_ID не задан.
//
// Это НЕ запись: движок в model_runs не пишет (спека §3.4, Review Focus 2). Смысл
// функции в том, чтобы прогон без run_id не потерялся: человек берёт напечатанный
// JSON, дописывает результат расчёта (nav_*) и отправляет в ingest-endpoint, получая
// версию модели в журнале. Поэтому доменные поля (входы, допущения, горизонт,
// вероятности) уже заполнены фактическими значениями, а поля результата нулевые.
//
// run_id пуст намеренно: без DCF_RUN_ID строки nav_by_asset не пишутся, и выдуманный
// здесь UUID привязал бы их к несуществующему прогону. Пустое поле — метка «впиши id
// перед POST», и ValidateModelRun её отклонит.
func modelRunJSON(plans []MinePlanRecord, decks map[string][]DeckYear, rates DiscountRates, p Params) string {
	// Горизонт — конец ТЕКУЩЕГО года, а не литерал: сетка вероятностей привязана к
	// тому году, в котором считается NAV, и зашитый год молча устарел бы в январе.
	// Формат YE-YYYY разбирает scenarioTargetDate в ingest.
	horizon := fmt.Sprintf("YE-%d", time.Now().UTC().Year())

	body := ingestModelRun{
		RunID:        "",
		Inputs:       inputsFrom(plans, decks),
		TriggerType:  "manual",
		TriggerRef:   "make import STAT=dcf_engine",
		GoldScenario: goldScenarioJSON(horizon),
		// usdrub_path пуст: курс в NPV этого среза не входит (все деньги — USD), и
		// выдумывать путь курса значило бы записать в model_runs допущение, которого
		// в расчёте не было.
		UsdrubPath: map[string]any{},
		// price_deck — 'own_scenario' (см. §13.4): движок считает все три дека сразу, но
		// в поле контракта, которое принимает одно значение, честнее указать тот дек,
		// который модель считает основным, чем произвольно брать первый из мапы.
		PriceDeck: "own_scenario",
		// Контуры: два числа — два контура, каждый в своём поле. discount_rate —
		// индустриальный (5% real USD + надбавки, поле называется «ставка
		// дисконтирования» и в ARCHITECTURE §6.2 описано именно как 0.05 real USD база);
		// wacc — локальный (ОФЗ + премии). Так оба числа, которые §13.1 требует
		// записывать, попадают в тело; какой контур где — читается по этой паре,
		// потому что отдельного поля контура в контракте ingest нет.
		DiscountRate: rates.Industrial,
		Wacc:         rates.Local,
		// nav_* и market_price — нули: их проставляет вызывающий после расчёта NAV/акции.
		NavPerShare: 0,
		NavBull:     0,
		NavBase:     0,
		NavBear:     0,
		MarketPrice: 0,
		UpsidePct:   0,
		Comment: fmt.Sprintf(
			"dcf_engine: LOM-NAV по %d активам; контуры industrial %.4f / local %.4f; "+
				"допущения: НДПИ база %.2f USD/oz + %.1f%% выше %.0f USD/oz, налог на прибыль %.1f%%, ΔWC %.0f дней",
			len(plans), rates.Industrial, rates.Local,
			p.NdpiBaseUSDPerOz, p.NdpiSurchargePct*100, p.NdpiThresholdUSD,
			p.ProfitTaxPct*100, p.WorkingCapitalDays,
		),
		Probabilities: map[string]float64{
			"bull": scenarioProbabilitiesPct["bull"],
			"base": scenarioProbabilitiesPct["base"],
			"bear": scenarioProbabilitiesPct["bear"],
		},
	}

	encoded, err := json.Marshal(body)
	if err != nil {
		// На этой структуре marshal не может не сработать, но ошибку не глушим: печать
		// невалидного тела отправила бы человека в ingest с мусором.
		log.Errorf("modelRunJSON: %v", err)

		return ""
	}

	return string(encoded)
}

// goldScenarioJSON собирает gold_scenario в формате ingest: horizon плюс по блоку на
// каждый сценарий с point (его берёт weightedGoldPoint) и коридором low/high.
// Сценарии в формате WGC Gold Outlook (§13.5): сценарий — не одно число, а коридор
// с mid-точкой, и именно точка взвешивается вероятностями.
//
// Ошибки у функции нет намеренно: единственный вход — метка горизонта, а её формат
// проверяет ingest (scenarioTargetDate) на своей стороне. Дублировать его правила
// здесь значило бы завести вторую копию парсера, которая разойдётся с ingest.
func goldScenarioJSON(horizon string) map[string]any {
	return map[string]any{
		"horizon": horizon,
		"bull":    scenarioBlock(scenarioBullPointUSD, scenarioBullLowUSD, scenarioBullHighUSD),
		"base":    scenarioBlock(scenarioBasePointUSD, scenarioBaseLowUSD, scenarioBaseHighUSD),
		"bear":    scenarioBlock(scenarioBearPointUSD, scenarioBearLowUSD, scenarioBearHighUSD),
	}
}

// scenarioBlock — блок одного сценария: point и коридор low/high.
func scenarioBlock(point, low, high float64) map[string]any {
	return map[string]any{"point": point, "low": low, "high": high}
}

// inputsFrom переносит фактический вход в тело: активы с годами LOM (годы — в порядке
// плана, то есть хронологическом, потому что readMinePlans читает ORDER BY year) и
// отсортированные имена деков. Сортировка — ради воспроизводимости печати: у map'а
// порядок обхода случаен, и одинаковый прогон печатал бы разные тела.
func inputsFrom(plans []MinePlanRecord, decks map[string][]DeckYear) inputsJSON {
	assets := make([]assetPlanJSON, 0, len(plans))
	for _, plan := range plans {
		years := make([]uint16, 0, len(plan.Years))
		for _, year := range plan.Years {
			years = append(years, year.Year)
		}

		assets = append(assets, assetPlanJSON{Company: plan.Company, Asset: plan.Asset, Years: years})
	}

	names := make([]string, 0, len(decks))
	for name := range decks {
		names = append(names, name)
	}
	sort.Strings(names)

	return inputsJSON{Assets: assets, Decks: names}
}
