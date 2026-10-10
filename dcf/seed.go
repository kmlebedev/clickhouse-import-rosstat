package dcf

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	log "github.com/sirupsen/logrus"
)

// Дек-константы и долгосрочные ценовые якоря. Имена совпадают со строками
// deck в ARCHITECTURE §6.4 / §6.2 — расхождение здесь означало бы, что расчёт и
// сид используют разные ключи, и один из трёх деков молча выпал бы из NAV.
const (
	deckSpotFlat    = "spot_flat"
	deckConsensusLT = "consensus_lt"
	deckOwnScenario = "own_scenario"

	// consensusLTAnchorUSD — LT-якорь системного консенсуса: BMO $3 000, Scotia
	// $2 600 с 2029 (§6.4). Берём более высокий из двух, чтобы сид не занижал
	// консенсусный дек.
	consensusLTAnchorUSD = 3000.0
	// ownScenarioAnchorUSD — LT-хвост собственного сценария (§6.4, база bull
	// $4 600–5 000): консенсус и собственный взгляд не должны совпадать, иначе
	// третий дек не несёт информации.
	ownScenarioAnchorUSD = 4600.0

	// priceDeckSeedHorizonYears — сид ставить не на текущий год, а на его конец:
	// горизонт LOM-плана начинается со следующего года, и цена, поставленная на
	// текущий, была бы за пределами любого плана. Берём конец текущего года —
	// ближайшая дата, начиная с которой цена актуальна для планов вперёд.
	priceDeckSeedHorizonYears = 0
)

// spotFlatSelect — последняя цена MOEX-фикса из gold_prices. argMax(usd, date) —
// одна строка без сортировки всего ряда; FINAL обязателен (ReplacingMergeTree).
// Плоский спот — намеренно одно число без прогноза: он и называется flat.
const spotFlatSelect = `SELECT argMax(usd, date) FROM gold_prices FINAL WHERE venue='moex_fix_usd'`

// seedPriceDecks засевает price_decks тремя деками (§6.4) ОДНИМ батчем.
//
// Вызывается только на пустой таблице (решение в Import): непустой дек — это
// выбранный аналитиком вход, и перезапись сидом молча подменила бы расчёт.
//
// Год: текущий год (time.Now().UTC().Year()) — но плюс нулевой горизонт, то есть
// именно текущий. Если вернуть горизонт >0 (скажем, +10 лет), цена дека легла бы
// на годы ВНЕ консервативного LOM-плана, и npvLOM пропустил бы все годы с
// предупреждением «нет цены дека» — NPV вышел бы нулевым при непустых данных.
// Плоская цена без кривой — это одно число, и оно честно живёт в текущем году.
// (priceDeckSeedHorizonYears оставлен рядом как именованный ноль: менять год сида
// — осознанное решение, а не случайная правка литерала.)
//
// published — сегодня: цена прочитана из gold_prices или взята из LT-якорей
// сегодня, и published = дата ЭТОЙ версии дека. Он входит в ключ
// ReplacingMergeTree (deck, year, published), поэтому повторный сид с другой
// датой не перетирает предыдущий, а добавляет версию — история цен сохраняется.
func seedPriceDecks(ctx context.Context, conn driver.Conn) error {
	spot := 0.0
	if err := conn.QueryRow(ctx, spotFlatSelect).Scan(&spot); err != nil {
		// gold_prices может отсутствовать или быть пустой (импорт ёлки — отдельный
		// шаг DAG). Не глушим: без цены спота сид неполон, и молчаливый ноль дал бы
		// спот-дек с нулевым золотом.
		return fmt.Errorf("сид price_decks: прочитать последнюю цену gold_prices (venue='moex_fix_usd'): %w", err)
	}

	year := uint16(time.Now().UTC().Year()) + priceDeckSeedHorizonYears
	published := time.Now().UTC()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO price_decks")
	if err != nil {
		return fmt.Errorf("сид price_decks: %w", err)
	}

	for _, deck := range []struct {
		name  string
		gold  float64
		label string
	}{
		{deckSpotFlat, spot, "MOEX-фикс, последняя цена"},
		{deckConsensusLT, consensusLTAnchorUSD, "консенсус LT (BMO $3 000; Scotia $2 600 с 2029)"},
		{deckOwnScenario, ownScenarioAnchorUSD, "собственный LT-сценарий (§6.4, база bull)"},
	} {
		if err = batch.Append(deck.name, year, deck.gold, published); err != nil {
			_ = batch.Abort()

			return fmt.Errorf("сид price_decks %s: %w", deck.name, err)
		}

		log.Infof("price_decks сид: %s = %.2f USD/oz на %d (%s)", deck.name, deck.gold, year, deck.label)
	}

	if err = batch.Send(); err != nil {
		return fmt.Errorf("сид price_decks: %w", err)
	}

	return nil
}
