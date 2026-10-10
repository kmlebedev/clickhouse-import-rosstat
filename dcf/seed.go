package dcf

import (
	"context"
	"fmt"
	"sort"
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
)

// planYears — отсортированный уникальный список годов LOM-планов: горизонт, на
// котором сид обязан выложить ценовые деки.
//
// Множество, а не срез «как пришло»: два актива (или годы одного актива) могут
// делить календарный год, а дек — это ровно одна строка на год; запись по
// дубликату года дала бы две версии с разным published и сделала бы цену года
// непредсказуемой после FINAL. Сортировка — потому что порядок строк сида должен
// быть воспроизводим между прогонами (порядок map в Go случаен).
func planYears(plans []MinePlanRecord) []uint16 {
	seen := make(map[uint16]struct{})
	for _, plan := range plans {
		for _, y := range plan.Years {
			seen[y.Year] = struct{}{}
		}
	}

	years := make([]uint16, 0, len(seen))
	for year := range seen {
		years = append(years, year)
	}

	sort.Slice(years, func(i, j int) bool { return years[i] < years[j] })

	return years
}

// assertSpotPrice — чистая проверка цены спота перед сидом. Вынесена отдельно,
// чтобы её можно было покрыть офлайн-тестом: живой ClickHouse на пустой
// gold_prices не падает, а ВОЗВРАЩАЕТ 0, и именно эту границу надо закрепить.
//
// Почему ноль — ошибка, а не «дек на нуле»: argMax по ПУСТОМУ множеству даёт
// нулевое значение типа (для Float64 — 0), а не NULL, и не бросает исключение
// (проверено: SELECT argMax(number, number+1) FROM numbers(0) → 0). gold_prices
// наполняется ОТДЕЛЬНЫМ шагом DAG, поэтому прогон сида до него превратил бы
// spot_flat в дек с ценой $0/oz — тихо заниженный вход всего расчёта, который в
// логе выглядел бы успешным сидом. Отрицательная цена — тот же класс мусора.
func assertSpotPrice(spot float64) error {
	if !(spot > 0) {
		return fmt.Errorf("сид price_decks: последняя цена spot_flat (gold_prices, venue='moex_fix_usd') = %v, "+
			"а не положительное число: пустая или ненаполненная gold_prices даёт argMax = 0, "+
			"и spot_flat записался бы с ценой $0/oz; сначала наполните gold_prices "+
			"(make import STAT=gold), затем повторите сид", spot)
	}

	return nil
}

// spotFlatSelect — последняя цена MOEX-фикса из gold_prices. argMax(usd, date) —
// одна строка без сортировки всего ряда; FINAL обязателен (ReplacingMergeTree).
// Плоский спот — намеренно одно число без прогноза: он и называется flat.
const spotFlatSelect = `SELECT argMax(usd, date) FROM gold_prices FINAL WHERE venue='moex_fix_usd'`

// seedPriceDecks засевает price_decks тремя деками (§6.4) ОДНИМ батчем на весь
// горизонт планов.
//
// Вызывается только на пустой таблице (решение в Import): непустой дек — это
// выбранный аналитиком вход, и перезапись сидом молча подменила бы расчёт.
//
// Годы: из LOM-планов (planYears), а не из часов. Горизонт обязан приходить из
// планов, иначе годы плана и годы дека разъедутся, npvLOM пропустит все годы без
// цены с предупреждением «нет цены дека», и NPV выйдет нулевым при непустых
// данных. Следствие: price_decks покрывает РОВНО годы, на которых считается NAV.
// Пустой список годов — ошибка, а не тихий ноль строк: Import доходит сюда только
// при непустом mine_plans, поэтому пустые годы означают баг у вызывающего, и
// молчаливое «сид написал 0 строк» спрятало бы его за успешным логом.
//
// published — сегодня: цена прочитана из gold_prices или взята из LT-якорей
// сегодня, и published = дата ЭТОЙ версии дека. Он входит в ключ
// ReplacingMergeTree (deck, year, published), поэтому повторный сид с другой
// датой не перетирает предыдущий, а добавляет версию — история цен сохраняется.
func seedPriceDecks(ctx context.Context, conn driver.Conn, years []uint16) error {
	if len(years) == 0 {
		return fmt.Errorf("сид price_decks: пустой горизонт годов — Import вызывает сид только при непустом mine_plans, " +
			"поэтому пустые годы означают ошибку выше по стеку, а не «сеять нечего»")
	}

	spot := 0.0
	if err := conn.QueryRow(ctx, spotFlatSelect).Scan(&spot); err != nil {
		// Ошибка чтения (нет таблицы gold_prices) — отдельный случай от «таблица
		// есть, но пуста»: пустую ловит assertSpotPrice ниже, потому что Scan по
		// ней проходит успешно и кладёт 0.
		return fmt.Errorf("сид price_decks: прочитать последнюю цену gold_prices (venue='moex_fix_usd'): %w", err)
	}

	// Явная защита от нулевого спота: argMax по пустому множеству = 0, и без
	// этой проверки spot_flat записался бы с ценой $0/oz (см. assertSpotPrice).
	if err := assertSpotPrice(spot); err != nil {
		return err
	}

	published := time.Now().UTC()

	decks := []struct {
		name  string
		gold  float64
		label string
	}{
		{deckSpotFlat, spot, "MOEX-фикс, последняя цена"},
		{deckConsensusLT, consensusLTAnchorUSD, "консенсус LT (BMO $3 000; Scotia $2 600 с 2029)"},
		{deckOwnScenario, ownScenarioAnchorUSD, "собственный LT-сценарий (§6.4, база bull)"},
	}

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO price_decks")
	if err != nil {
		return fmt.Errorf("сид price_decks: %w", err)
	}

	// Порядок строк детерминирован: годы уже отсортированы (planYears), деки — в
	// фиксированном порядке выше, чтобы diff двух сидов был осмысленным.
	for _, year := range years {
		for _, deck := range decks {
			if err = batch.Append(deck.name, year, deck.gold, published); err != nil {
				_ = batch.Abort()

				return fmt.Errorf("сид price_decks %s: %w", deck.name, err)
			}

			log.Infof("price_decks сид: %s = %.2f USD/oz на %d (%s)", deck.name, deck.gold, year, deck.label)
		}
	}

	// Abort на ошибке Send НЕ вызываем — так же, как все остальные батч-писатели
	// репозитория (polyus/import.go, ingest/clickhouse.go, util/series_catalog.go):
	// Send() завершает и закрывает батч даже при ошибке сервера, повторный Abort
	// после него смысла не имеет. Асимметрия с веткой Append намеренная: там батч
	// ещё открыт, и без Abort соединение осталось бы с незакрытым батчем.
	if err = batch.Send(); err != nil {
		return fmt.Errorf("сид price_decks: %w", err)
	}

	return nil
}
