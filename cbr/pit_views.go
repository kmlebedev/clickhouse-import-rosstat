package cbr

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

// cbrPitView — нормализованный long-слой point-in-time над винтажной cbr_dip_bulletin
// (source = имя таблицы, единственный источник): одна строка = одна версия значения.
// period_date — отчётный период, а не дата публикации; published_at — дата, когда версия
// стала публичной; vintage/vintage_label — идентификатор и номер выпуска источника.
// Слой не копирует данные, а читает таблицу динамически, поэтому новый выпуск появляется
// в нём сразу после импорта. Срез «что было известно строго до даты» — v_point_in_time_asof.
var cbrPitView = util.View{
	Name:   "v_point_in_time",
	Tables: []string{cbrDipTable},
	Select: `SELECT 'cbr_dip_bulletin' AS source, series, date AS period_date, value,
       publication_date AS published_at, release_id AS vintage, release AS vintage_label
FROM cbr_dip_bulletin FINAL`,
	Comment: "Нормализованный point-in-time слой значений с датой публикации: одна строка — " +
		"одна версия значения. Сейчас источник один — cbr_dip_bulletin (8 рядов ДИП ЦБ); " +
		"source совпадает с source в v_series_catalog. period_date — отчётный период (для ДИП " +
		"последний день месяца), а не дата публикации: published_at — дата, когда версия стала " +
		"публичной, и она может быть позже периода. vintage — идентификатор версии (release_id " +
		"выпуска), vintage_label — номер выпуска источника («26-07»). Для среза «что было " +
		"известно строго до даты» использовать v_point_in_time_asof(as_of='YYYY-MM-DD'); сама " +
		"витрина отдаёт текущее состояние таблицы целиком, включая все выпуски",
	Columns: map[string]string{
		"source":        "Источник слоя: всегда cbr_dip_bulletin; совпадает с source в v_series_catalog",
		"series":        "Имя ряда (8 рядов ДИП). Описания и единицы — в v_series_catalog (source='cbr_dip_bulletin')",
		"period_date":   "Отчётный период (для ДИП — последний день месяца), не дата публикации. Сопоставлять по toStartOfMonth(period_date)",
		"value":         "Значение версии (Float64), %: то, каким его опубликовал выпуск vintage на дату published_at",
		"published_at":  "Дата, когда версия стала публичной (для ДИП — publication_date выпуска). Граница среза в v_point_in_time_asof строгая: published_at < as_of",
		"vintage":       "Идентификатор версии (для ДИП — release_id выпуска); второй элемент тай-брейка (published_at, vintage) в as-of",
		"vintage_label": "Читаемая метка версии — номер выпуска источника («26-07»)",
	},
}

// cbrPitAsOfView — параметризованная витрина ClickHouse: срез «что было известно строго до
// as_of». Граница строгая (publication_date < {as_of:Date}), версия выбирается argMax по
// (published_at, vintage), поэтому два выпуска одной даты не дают произвольную строку, а
// отсутствие ряда в новом выпуске не удаляет последнюю известную версию. Колонковых
// комментариев нет: у параметризованной витрины ClickHouse нет схемы без аргументов, и
// COMMENT COLUMN для неё возвращает NOT_FOUND_COLUMN_IN_BLOCK (проверено на 26.10).
var cbrPitAsOfView = util.View{
	Name:   "v_point_in_time_asof",
	Tables: []string{cbrDipTable},
	Select: `SELECT source, series, period_date,
       argMax(value, (published_at, vintage))         AS value,
       max(published_at)                              AS published_at,
       argMax(vintage, (published_at, vintage))       AS vintage,
       argMax(vintage_label, (published_at, vintage)) AS vintage_label
FROM (
    SELECT 'cbr_dip_bulletin' AS source, series, date AS period_date, value,
           publication_date AS published_at, release_id AS vintage, release AS vintage_label
    FROM cbr_dip_bulletin FINAL
    WHERE publication_date < {as_of:Date}
)
GROUP BY source, series, period_date`,
	Comment: "Срез point-in-time: что было известно строго до даты as_of. Форма вызова — " +
		"v_point_in_time_asof(as_of='YYYY-MM-DD'). Граница строгая: publication_date < as_of, " +
		"поэтому значение, опубликованное ровно в дату as_of, в срез не входит (решение ЦБ " +
		"публикуется в 13:30 и может быть раньше приложения). Для каждого (source, series, " +
		"period_date) берётся последняя версия по тай-брейку (published_at, vintage): два " +
		"выпуска одной даты не дают произвольную строку, а отсутствие ряда в новом выпуске " +
		"значит «этот выпуск его не показывает» — последняя известная версия остаётся видимой. " +
		"Пример ревизии unemployment_sa за 2023-11-30: as_of='2024-02-07' → 2.885274 (выпуск " +
		"24-01), as_of='2026-09-02' → 2.916127 (выпуск 26-06). У параметризованной витрины нет " +
		"схемы без аргументов, поэтому колонковых комментариев у неё нет — смысл колонок " +
		"описан в v_point_in_time",
	Columns: map[string]string{},
}

// cbrPitVersionView — версия набора данных для записи рядом с прогнозом
// (forecast_log.data_version): одна строка на source, data_version — hex(groupBitXor(
// cityHash64(...))) по всем строкам источника. Свёртка вероятностная, но стабильна на
// повторе и меняется при добавлении, ревизии или удалении любой строки.
var cbrPitVersionView = util.View{
	Name:   "v_point_in_time_version",
	Tables: []string{cbrDipTable},
	Select: `SELECT source,
       count()                                    AS points,
       uniqExact(vintage)                         AS vintages,
       min(published_at)                          AS first_publication,
       max(published_at)                          AS last_publication,
       hex(groupBitXor(cityHash64(series, period_date, published_at, vintage, vintage_label, value))) AS data_version
FROM (
    SELECT 'cbr_dip_bulletin' AS source, series, date AS period_date, value,
           publication_date AS published_at, release_id AS vintage, release AS vintage_label
    FROM cbr_dip_bulletin FINAL
)
GROUP BY source`,
	Comment: "Версия набора данных point-in-time слоя: одна строка на source. data_version — " +
		"hex(groupBitXor(cityHash64(...))) по всем строкам источника: 16 hex-символов, " +
		"меняется при добавлении, ревизии или удалении любой строки и стабилен на повторе " +
		"(вероятностная свёртка, а не криптографический хэш). points — число точек, vintages — " +
		"число версий, first_publication/last_publication — границы дат публикации. Для " +
		"forecast_log.data_version рекомендована строка pit:cbr_dip_bulletin:<data_version>: " +
		"по ней прогноз сопоставляется с точным состоянием слоя",
	Columns: map[string]string{
		"source":            "Источник слоя: всегда cbr_dip_bulletin",
		"points":            "Число точек (строк-версий) в источнике",
		"vintages":          "Число различных версий: uniqExact(vintage)",
		"first_publication": "Самая ранняя дата публикации среди версий",
		"last_publication":  "Самая поздняя дата публикации среди версий",
		"data_version":      "Версия набора: hex(groupBitXor(cityHash64(...))), 16 hex-символов; стабильна на повторе, меняется при любой ревизии. Для forecast_log.data_version рекомендована строка pit:cbr_dip_bulletin:<data_version>",
	},
}

// cbrPitViews — определения слоя в порядке публикации: нормализованная витрина, срез as-of
// и версия набора.
var cbrPitViews = []util.View{cbrPitView, cbrPitAsOfView, cbrPitVersionView}

// ensurePitViews создаёт или пересоздаёт все витрины слоя (порядок cbrPitViews). Вызывается
// прогоном cbr_dip_bulletin после цикла выпусков и на no-op-пути: витрины — производная
// существующей таблицы, и самолечение работает без новых выпусков. Ошибка останавливает слой,
// но не откатывает уже записанные выпуски — Import присоединяет её к общим ошибкам прогона.
func ensurePitViews(ctx context.Context, conn driver.Conn) error {
	for _, v := range cbrPitViews {
		created, err := util.CreateView(ctx, conn, v)
		if err != nil {
			return fmt.Errorf("%s: %w", v.Name, err)
		}
		log.Infof("%s created: %t", v.Name, created)
	}
	return nil
}
