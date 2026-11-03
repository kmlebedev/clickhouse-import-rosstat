package transaq

import "github.com/kmlebedev/clickhouse-import-rosstat/util"

// instrumentKindExpr — тип инструмента строки. Строки до ofz-market/1.7 имеют
// ”: тогда в ленте были только акции TQBR и ОФЗ TQOB, поэтому вывод по board
// для них точный и бэкфилл не нужен.
const instrumentKindExpr = "if(instrument_kind = '', multiIf(board = 'TQOB', 'ofz', board = 'FUT', 'future', 'share'), instrument_kind)"

// instrumentKindComment — общий комментарий колонки обеих витрин.
const instrumentKindComment = "Тип инструмента: share — акции IMOEX (TQBR), fund — фонды денежного рынка " +
	"AKMM/LQDT/SBMM (тоже TQBR, отличаются суффиксом ETF в каталоге), ofz — ОФЗ (TQOB), future — " +
	"индексные фьючерсы MIX/RTS/MXI/IMOEXF (FUT), other — прочие режимы. Фильтр «акции» — " +
	"instrument_kind = 'share', а не board = 'TQBR'"

var transaqNetFlowsView = util.View{
	Name:   "v_transaq_net_flows",
	Tables: []string{transaqNetFlowsTable},
	Select: "SELECT minute, sec_code, board, " + instrumentKindExpr + " AS instrument_kind, " +
		"buy_lots, sell_lots, " +
		"toInt64(buy_lots) - toInt64(sell_lots) AS net_lots, " +
		"buy_value_rub, sell_value_rub, " +
		"buy_value_rub - sell_value_rub AS net_value_rub, " +
		"trade_count, gap_suspected, loaded_at " +
		"FROM transaq_net_flows FINAL",
	Comment: "Минутные объёмы покупок и продаж по публичным сделкам MOEX " +
		"(лента transaq_trades внешнего сервиса transaq-clickhouse-exporter): " +
		"одна строка на код, режим торгов и минуту (Europe/Moscow). Сторона сделки " +
		"buy_sell приходит только из этого потока — MOEX ISS историю сделок без " +
		"агрессора не отдаёт. В ленте акции IMOEX и фонды денежного рынка (TQBR), " +
		"ОФЗ (TQOB) и индексные фьючерсы (FUT); тип — instrument_kind. Объёмы в " +
		"лотах, не в бумагах (у фьючерсов — в контрактах); нетто не хранится, а " +
		"выводится со знаком. Рублёвый объём — оценка из снимочного каталога " +
		"transaq_securities: price × лоты × lotsize, для FUT — price × контракты × " +
		"стоимость пункта (point_cost × 10^decimals / 100); у RTS стоимость пункта " +
		"зависит от курса USD/RUB на момент снимка каталога. При отсутствии " +
		"инструмента в каталоге сделка даёт только лоты. Лента событийная: разрыв " +
		"сессии не догоняется, дубли до background merge сняты дедупликацией по " +
		"полному ключу сделки. Нетто считается только внутри торговой сессии: " +
		"переносить его через сессию нельзя. Это аналитика, а не сигнал и не " +
		"рекомендация.",
	Columns: map[string]string{
		"minute":          "Минута сделок (Europe/Moscow, toStartOfMinute)",
		"sec_code":        "Код бумаги Transaq (например, SBER или SU26238RMFS9)",
		"board":           "Режим торгов MOEX: TQBR — акции и фонды, TQOB — ОФЗ, FUT — фьючерсы FORTS",
		"instrument_kind": instrumentKindComment,
		"buy_lots":        "Куплено, лоты (sumIf(quantity) по сделкам с buy_sell = 'B'); у фьючерсов — контракты; не бумаги и не рубли",
		"sell_lots":       "Продано, лоты (buy_sell = 'S')",
		"net_lots":        "Нетто buy_lots − sell_lots, лоты со знаком: >0 — перевес покупок по инициативе",
		"buy_value_rub":   "Оценка объёма покупок, руб.: Σ price × лоты × lotsize, для FUT — Σ price × контракты × point_cost × 10^decimals / 100; сделки без строки каталога в сумму не входят",
		"sell_value_rub":  "Оценка объёма продаж, руб.",
		"net_value_rub":   "Нетто buy_value_rub − sell_value_rub, руб. со знаком",
		"trade_count":     "Число сделок бакета (после дедупликации по ключу сделки)",
		"gap_suspected":   "1 — эвристика разрыва ленты: в бакете торг возобновился после паузы > 10 минут внутри 10:00–19:00 МСК; 0 — разрыва не видно. Эвристика не доказывает потерю сделок и не отличает паузу рынка от reconnect; у фьючерсов вечерний клиринг 18:45–19:05 может дать метку в бакете 18:5x",
		"loaded_at":       "Время расчёта бакета (Europe/Moscow); версия строки ReplacingMergeTree — повторный расчёт заменяет бакет",
	},
}

var transaqNetFlowsDailyView = util.View{
	Name:   "v_transaq_net_flows_daily",
	Tables: []string{transaqNetFlowsTable},
	Select: "SELECT date, sec_code, board, instrument_kind, buy_lots, sell_lots, " +
		"toInt64(buy_lots) - toInt64(sell_lots) AS net_lots, " +
		"buy_value_rub, sell_value_rub, " +
		"buy_value_rub - sell_value_rub AS net_value_rub, " +
		"trade_count, minutes_with_trades, gap_suspected, first_trade, last_trade " +
		"FROM (SELECT toDate(minute) AS date, sec_code, board, " +
		"any(" + instrumentKindExpr + ") AS instrument_kind, " +
		"sum(buy_lots) AS buy_lots, sum(sell_lots) AS sell_lots, " +
		"sum(buy_value_rub) AS buy_value_rub, sum(sell_value_rub) AS sell_value_rub, " +
		"toUInt32(sum(trade_count)) AS trade_count, " +
		"toUInt32(count()) AS minutes_with_trades, " +
		"max(gap_suspected) AS gap_suspected, " +
		"min(minute) AS first_trade, max(minute) AS last_trade " +
		"FROM transaq_net_flows FINAL " +
		"GROUP BY date, sec_code, board)",
	Comment: "Дневные объёмы покупок и продаж по публичным сделкам MOEX — сумма " +
		"минутных бакетов v_transaq_net_flows по коду, режиму торгов и дате " +
		"(Europe/Moscow). Единицы и тип инструмента instrument_kind те же: лоты " +
		"(у фьючерсов — контракты) и оценка рублей через lotsize или, для FUT, " +
		"через стоимость пункта. " +
		"minutes_with_trades меньше полного числа торговых минут, если по " +
		"инструменту не было сделок; first_trade/last_trade — границы фактических " +
		"сделок, а не сессии. Дата включает вечерние сделки (19:05–23:50 МСК): " +
		"агрегатор пересчитывает их каждые 15 минут, как основную сессию. " +
		"gap_suspected = 1, если " +
		"хотя бы один минутный бакет дня помечен разрывом ленты. Это аналитика, а " +
		"не сигнал и не рекомендация.",
	Columns: map[string]string{
		"date":                "Дата сделок (Europe/Moscow)",
		"sec_code":            "Код бумаги Transaq",
		"board":               "Режим торгов MOEX: TQBR — акции и фонды, TQOB — ОФЗ, FUT — фьючерсы FORTS",
		"instrument_kind":     instrumentKindComment,
		"buy_lots":            "Куплено за день, лоты",
		"sell_lots":           "Продано за день, лоты",
		"net_lots":            "Нетто за день, лоты со знаком",
		"buy_value_rub":       "Оценка объёма покупок за день, руб. (Σ price × лоты × lotsize; для FUT — через стоимость пункта)",
		"sell_value_rub":      "Оценка объёма продаж за день, руб.",
		"net_value_rub":       "Нетто за день, руб. со знаком",
		"trade_count":         "Число сделок за день",
		"minutes_with_trades": "Число минут с хотя бы одной сделкой",
		"gap_suspected":       "1 — хотя бы один минутный бакет дня помечен разрывом ленты (> 10 минут внутри 10:00–19:00 МСК)",
		"first_trade":         "Минута первой сделки дня; NULL — сделок не было",
		"last_trade":          "Минута последней сделки дня; NULL — сделок не было",
	},
}
