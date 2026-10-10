package dcf

import (
	"encoding/json"
	"fmt"
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
// те же типы, ни одним полем больше. Отдельная структура, а не импорт ingest, потому
// что dcf/ не имеет права зависеть от ingest-контура: движок обязан собираться без
// серверного кода, а совпадение контракта проверяется тестом на строгий декодер.
//
// Лишних полей здесь быть не может: ingest декодирует тело с DisallowUnknownFields
// (ingest/server.go, ARCHITECTURE §6.5), то есть неизвестный ключ — 400 ещё до
// валидации. Всё, что движок хочет сообщить сверх контракта (число активов и деков,
// локальная ставка), уходит в comment — поле, которое в контракте есть.
type ingestModelRun struct {
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

// modelRunJSON собирает тело POST /v1/model_run по контракту ingest.ModelRun
// (ingest/model_run.go) — то, что движок печатает, когда DCF_RUN_ID не задан.
//
// Это НЕ запись: движок в model_runs не пишет (спека §3.4, Review Focus 2). Смысл
// функции в том, чтобы прогон без run_id не потерялся: человек берёт напечатанный
// JSON, дописывает результат расчёта (nav_*) и отправляет в ingest-endpoint, получая
// версию модели в журнале. Поэтому доменные поля (допущения, горизонт, вероятности)
// уже заполнены фактическими значениями, а поля результата нулевые.
//
// Функция детерминирована с точностью до года горизонта: он берётся из текущего года.
//
// Тело обязано состоять РОВНО из полей контракта — ingest декодирует с
// DisallowUnknownFields (ARCHITECTURE §6.5), и любой лишний ключ превращает
// напечатанное тело в 400. Всё сверх контракта — только через comment.
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
		// discount_rate — индустриальный контур (в ARCHITECTURE §6.2 поле описано как
		// «0.05 real USD база + надбавки»). wacc остаётся нулём: CAPM/WACC для
		// золотодобычи применять запрещено (§6.4), поэтому локальный контур в comment,
		// а не в этом поле.
		DiscountRate: rates.Industrial,
		Wacc:         0,
		// nav_* и market_price — нули: их проставляет вызывающий после расчёта NAV/акции.
		NavPerShare: 0,
		NavBull:     0,
		NavBase:     0,
		NavBear:     0,
		MarketPrice: 0,
		UpsidePct:   0,
		// Счётчики входа и локальная ставка — только в comment: отдельного поля под
		// них контракт ingest не имеет, а лишний ключ он отвергает (DisallowUnknownFields).
		Comment: fmt.Sprintf(
			"dcf_engine: LOM-NAV; вход: %d %s, %d %s; контуры industrial %.4f / локальная %.4f; "+
				"допущения: НДПИ база %.2f USD/oz + %.1f%% выше %.0f USD/oz, налог на прибыль %.1f%%, ΔWC %.0f дней, ИПЦ %d лет",
			len(plans), plural(len(plans), "актив", "актива", "активов"),
			len(decks), plural(len(decks), "дек", "дека", "деков"),
			rates.Industrial, rates.Local,
			p.NdpiBaseUSDPerOz, p.NdpiSurchargePct*100, p.NdpiThresholdUSD,
			p.ProfitTaxPct*100, p.WorkingCapitalDays, len(p.Ipc),
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

// plural выбирает русское окончание по числу: без него в comment появляется
// «2 активтива» или «1 декека» — мелочь, но comment читает человек, и он же
// единственное место, где видны счётчики входа: лишних ключей контракт ingest не
// принимает (DisallowUnknownFields).
func plural(n int, one, few, many string) string {
	switch {
	case n%100 >= 11 && n%100 <= 14:
		return many
	case n%10 == 1:
		return one
	case n%10 >= 2 && n%10 <= 4:
		return few
	default:
		return many
	}
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
