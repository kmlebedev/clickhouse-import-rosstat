package transaq

import "github.com/kmlebedev/clickhouse-import-rosstat/util"

var transaqNetFlowsView = util.View{
	Name:   "v_transaq_net_flows",
	Tables: []string{transaqNetFlowsTable},
	Select: "SELECT minute, sec_code, board, buy_lots, sell_lots, " +
		"toInt64(buy_lots) - toInt64(sell_lots) AS net_lots, " +
		"buy_value_rub, sell_value_rub, " +
		"buy_value_rub - sell_value_rub AS net_value_rub, " +
		"trade_count, gap_suspected, loaded_at " +
		"FROM transaq_net_flows FINAL",
	Comment: "Минутные объёмы покупок и продаж по публичным сделкам MOEX " +
		"(лента transaq_trades внешнего сервиса transaq-clickhouse-exporter): " +
		"одна строка на код, режим торгов и минуту (Europe/Moscow). Сторона сделки " +
		"buy_sell приходит только из этого потока — MOEX ISS историю сделок без " +
		"агрессора не отдаёт. Объёмы в лотах, не в бумагах; нетто не хранится, а " +
		"выводится со знаком. Рублёвый объём — оценка price × лоты × lotsize из " +
		"снимочного каталога transaq_securities: при отсутствии инструмента в " +
		"каталоге или lotsize = 0 сделка даёт только лоты, а для FORTS умножение на " +
		"lotsize не равно денежному обороту контракта. Лента событийная: разрыв " +
		"сессии не догоняется, дубли до background merge сняты дедупликацией по " +
		"полному ключу сделки. Нетто считается только внутри торговой сессии: " +
		"переносить его через сессию нельзя. Это аналитика, а не сигнал и не " +
		"рекомендация.",
	Columns: map[string]string{
		"minute":         "Минута сделок (Europe/Moscow, toStartOfMinute)",
		"sec_code":       "Код бумаги Transaq (например, SBER или SU26238RMFS9)",
		"board":          "Режим торгов MOEX: TQBR для акций, TQOB для ОФЗ; другие режимы — если попадут в ленту",
		"buy_lots":       "Куплено, лоты (sumIf(quantity) по сделкам с buy_sell = 'B'); не бумаги и не рубли",
		"sell_lots":      "Продано, лоты (buy_sell = 'S')",
		"net_lots":       "Нетто buy_lots − sell_lots, лоты со знаком: >0 — перевес покупок по инициативе",
		"buy_value_rub":  "Оценка объёма покупок, руб.: Σ price × лоты × lotsize; сделки без lotsize в сумму не входят",
		"sell_value_rub": "Оценка объёма продаж, руб.",
		"net_value_rub":  "Нетто buy_value_rub − sell_value_rub, руб. со знаком",
		"trade_count":    "Число сделок бакета (после дедупликации по ключу сделки)",
		"gap_suspected":  "1 — эвристика разрыва ленты: в бакете торг возобновился после паузы > 10 минут внутри 10:00–19:00 МСК; 0 — разрыва не видно. Эвристика не доказывает потерю сделок и не отличает паузу рынка от reconnect",
		"loaded_at":      "Время расчёта бакета (Europe/Moscow); версия строки ReplacingMergeTree — повторный расчёт заменяет бакет",
	},
}

var transaqNetFlowsDailyView = util.View{
	Name:   "v_transaq_net_flows_daily",
	Tables: []string{transaqNetFlowsTable},
	Select: "SELECT date, sec_code, board, buy_lots, sell_lots, " +
		"toInt64(buy_lots) - toInt64(sell_lots) AS net_lots, " +
		"buy_value_rub, sell_value_rub, " +
		"buy_value_rub - sell_value_rub AS net_value_rub, " +
		"trade_count, minutes_with_trades, gap_suspected, first_trade, last_trade " +
		"FROM (SELECT toDate(minute) AS date, sec_code, board, " +
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
		"(Europe/Moscow). Единицы те же: лоты и оценка рублей через lotsize. " +
		"minutes_with_trades меньше полного числа торговых минут, если по " +
		"инструменту не было сделок; first_trade/last_trade — границы фактических " +
		"сделок, а не сессии. Дневная сессия включает вечерние сделки (19:00–23:50 " +
		"МСК), если они попали в ленту: ночной прогон агрегатора не ходит, вечер " +
		"догоняется следующим утренним пересчётом окна. gap_suspected = 1, если " +
		"хотя бы один минутный бакет дня помечен разрывом ленты. Это аналитика, а " +
		"не сигнал и не рекомендация.",
	Columns: map[string]string{
		"date":                "Дата сделок (Europe/Moscow)",
		"sec_code":            "Код бумаги Transaq",
		"board":               "Режим торгов MOEX: TQBR — акции, TQOB — ОФЗ",
		"buy_lots":            "Куплено за день, лоты",
		"sell_lots":           "Продано за день, лоты",
		"net_lots":            "Нетто за день, лоты со знаком",
		"buy_value_rub":       "Оценка объёма покупок за день, руб. (Σ price × лоты × lotsize)",
		"sell_value_rub":      "Оценка объёма продаж за день, руб.",
		"net_value_rub":       "Нетто за день, руб. со знаком",
		"trade_count":         "Число сделок за день",
		"minutes_with_trades": "Число минут с хотя бы одной сделкой",
		"gap_suspected":       "1 — хотя бы один минутный бакет дня помечен разрывом ленты (> 10 минут внутри 10:00–19:00 МСК)",
		"first_trade":         "Минута первой сделки дня; NULL — сделок не было",
		"last_trade":          "Минута последней сделки дня; NULL — сделок не было",
	},
}
