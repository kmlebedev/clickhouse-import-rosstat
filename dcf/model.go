package dcf

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
