package dcf

import (
	"math"

	log "github.com/sirupsen/logrus"
)

// Params — параметры налоговой и рабочей модели. Все числа, которые могут
// измениться от решения регулятора или от допущения аналитика, вынесены сюда:
// зашитые в код ставки переживают смену налогового режима и дают тихо неверный
// NPV (ARCHITECTURE.md §6.4).
type Params struct {
	// NdpiBaseUSDPerOz — базовая ставка НДПИ в долларах на унцию при цене ниже
	// порога (до 2025 — адвалорная, далее — специфическая часть формулы).
	NdpiBaseUSDPerOz float64
	// NdpiSurchargePct — надбавка к НДПИ с цены выше порога (с 2025 — 0.10).
	NdpiSurchargePct float64
	// NdpiThresholdUSD — порог цены золота, с которого включается надбавка.
	NdpiThresholdUSD float64
	// ProfitTaxPct — налог на прибыль.
	ProfitTaxPct float64
	// WorkingCapitalDays — оборот рабочего капитала в днях (для денежного потока).
	WorkingCapitalDays float64
}

// defaultParams — допущения модели по умолчанию: порог надбавки НДПИ 1900 USD/oz
// и ставка 10% (налоговое правило с 2025). Базовая ставка, налог на прибыль и
// рабочий капитал — предположения аналитика, они задаются явно при построении
// сценария; здесь нули, чтобы отсутствие явного допущения было заметно.
var defaultParams = Params{
	NdpiBaseUSDPerOz:   0,
	NdpiSurchargePct:   0.10,
	NdpiThresholdUSD:   1900,
	ProfitTaxPct:       0,
	WorkingCapitalDays: 0,
}

// ndpiPerOz — НДПИ на унцию: база плюс надбавка с превышения порога. Порог и
// ставка берутся из Params, а не зашиты литералами: это налоговое число, оно
// изменится, и тогда достаточно будет поправить допущение, а не формулу.
func ndpiPerOz(gold float64, p Params) float64 {
	return p.NdpiBaseUSDPerOz + p.NdpiSurchargePct*max(gold-p.NdpiThresholdUSD, 0)
}

// escalate — эскалация базовой величины по накопленному ИПЦ: множители за годы
// строго до yearIndex. Если индекс за пределами ряда, множителей больше нет —
// возвращаем накопленное, а не паникуем: горизонт деку и ряда ИПЦ могут не
// совпадать, и хвост без прогноза ИПЦ остаётся в ценах последнего известного года.
func escalate(base float64, ipc []float64, yearIndex int) float64 {
	value := base
	for i := 0; i < yearIndex && i < len(ipc); i++ {
		value *= 1 + ipc[i]
	}

	return value
}

// MinePlanYear — один год life-of-mine плана актива (таблица mine_plans, §6.2).
// Единицы намеренно разные и повторяют источник: ProductionKoz — тысячи унций,
// AISC/TCC — USD/унц, capex и стоимость закрытия — USD млн. Смешение этих единиц
// даёт NPV, ошибочный в тысячи раз, поэтому пересчёт к млн собран в одном месте —
// в npvLOM, а не разбросан по вызывающим.
type MinePlanYear struct {
	Year            uint16
	ProductionKoz   float64
	TCC             float64
	AISC            float64
	CapexSustaining float64
	CapexProject    float64
	ClosureCosts    float64
}

// DeckYear — цена золота ценового дека на год (таблица price_decks, §6.2).
// Год обязателен: без него цены дека не к чему привязать, и план года нельзя
// посчитать корректно.
type DeckYear struct {
	Year    uint16
	GoldUSD float64
}

// DiscountRates — двухконтурная ставка (§13.1): индустриальная (5% real USD +
// надбавки, сопоставима с глобальным P/NAV) и локальная (ОФЗ + премии, объясняет
// цену RU-инвестора). Оба результата пишутся в model_runs, поэтому различимы.
type DiscountRates struct {
	Industrial float64
	Local      float64
}

// npvLOM — NPV актива по life-of-mine плану, USD млн. Формула (§4.1):
//
//	FCF_t = Prod(koz)×(gold − AISC) − НДПИ×Prod − налог − capex − ΔWC
//
// Рудник истощаем: perpetual TV НЕТ, последний год просто несёт хвост закрытия
// (отрицательный, рекультивация налогово вычитаема — поэтому он входит в
// налоговую базу, а не вычитается после налога, §13.3).
//
// ΔWC моделируется как ОДНОСТОРОННЕЕ вложение (знаковая нагрузка оборотного
// капитала): загрузка пропорциональна выручке и не опускается ниже нуля. Обратный
// случай — высвобождение капитала, т.е. приток (+) при падении выручки — намеренно
// НЕ моделируется: односторонняя нагрузка консервативна (не даёт NPV «зарабатывать»
// на убыточном/сжимающемся году) и не требует предположения о скорости возврата
// капитала в хвосте LOM, которого спецификация не задаёт. Поэтому в формуле выше
// стоит «−ΔWC», а не «±ΔWC» роадмапа.
//
// Единицы входа — как в спеке §3.3: ProductionKoz — тыс. унц, AISC/TCC — USD/унц,
// а capex, хвост закрытия и ΔWC — УЖЕ млн USD. Произведение koz×USD/oz даёт тыс.
// USD, поэтому все деньги внутри года приводятся к одной единице (тыс. USD) явным
// множителем thousand, и только итоговый поток делится на 1000 в млн. Так налоговая
// база max(FCF, 0) и ставка дисконтирования видят одни и те же числа; смешение
// млн и тыс. даёт NPV, ошибочный в 1000 раз, а тесты-инварианты «больше/меньше»
// такое смешение не ловят.
//
// Год плана без записи в deck пропускается с предупреждением: цена неизвестна,
// и молчаливый ноль золота дал бы тихо заниженный NPV вместо явного пропуска.
func npvLOM(plan []MinePlanYear, deck []DeckYear, rate float64, ipc []float64, p Params) float64 {
	// млн USD → тыс. USD для полей, которые спек задаёт в млн (capex, closure).
	const thousand = 1000.0

	priceByYear := make(map[uint16]float64, len(deck))
	for _, d := range deck {
		priceByYear[d.Year] = d.GoldUSD
	}

	var npv float64
	yearIndex := 0 // порядковый номер года плана — для эскалации ИПЦ и дисконта
	for i, y := range plan {
		// Индекс года считаем по календарю плана: пропущенный год всё равно
		// сдвигает и эскалацию, и дисконт — время идёт независимо от наличия цены.
		gold, ok := priceByYear[y.Year]
		if !ok {
			log.Warnf("npvLOM: нет цены дека на %d год — год пропущен", y.Year)
			yearIndex++
			continue
		}

		// Затраты эскалируются ИПЦ накопленным за предыдущие годы плана (§6.4,
		// TCC/AISC привязаны к инфляции РФ); выручка — в номинале дека.
		aisc := escalate(y.AISC, ipc, yearIndex)

		revenue := y.ProductionKoz * (gold - aisc)   // тыс. USD
		ndpi := y.ProductionKoz * ndpiPerOz(gold, p) // тыс. USD

		// capex и хвост закрытия — в млн USD, приводим к тыс. USD.
		opCash := revenue - ndpi - (y.CapexSustaining+y.CapexProject)*thousand

		// Хвост закрытия — в последнем году LOM и ДО налога: рекультивация
		// налогово вычитаема (§13.3). Отрицательный — уменьшает поток и налог.
		if i == len(plan)-1 {
			opCash += y.ClosureCosts * thousand
		}

		// ΔWC: загрузка оборотного капитала пропорциональна выручке (дни выручки).
		// Приток от высвобождения капитала на убыточном году не моделируем — ΔWC
		// может быть только вложением, иначе модель «зарабатывала» бы на убытке.
		beforeTax := opCash
		if p.WorkingCapitalDays > 0 {
			if wc := revenue * p.WorkingCapitalDays / 365; wc > 0 {
				beforeTax -= wc
			}
		}

		// Налог на прибыль — с положительного потока до налога (спек §3.3).
		fcf := beforeTax - max(beforeTax, 0)*p.ProfitTaxPct // тыс. USD

		npv += fcf / thousand / math.Pow(1+rate, float64(yearIndex)) // млн USD
		yearIndex++
	}

	return npv
}
