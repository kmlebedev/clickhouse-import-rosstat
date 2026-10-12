package craw

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Витрины v_gost_* — читательский контур реестра СБКТС (правило 11 AGENTS.md): сырая
// gost_vehicle_safety_certificate агенту не выдаётся, читать реестр он обязан через витрину.
// Витрин две, потому что вопросы к реестру разные: «какие сертификаты выданы» (строки) и
// «сколько машин въехало» (дневной поток) — второй считается uniqExact по номерам, а не
// count() строк таблицы (изменения сертификата дают вторую строку при том же номере).
//
// Публикуются витрины импортёром в начале пишущего прогона (до первого обращения к API) — по образцу
// news/, чтобы правка определения уезжала вместе с импортом, а не отдельным ручным шагом.

// Имена витрин — контракт с sql/mcp_kimi_reader.sql (гранты), ARCHITECTURE.md §6.3
// и scripts/mcp_check.py: промах в имени — это витрина, которой агент не видит.
const (
	// gostRowsView — строковая витрина реестра: один сертификат — одна строка (FINAL
	// схлопывает вытесненные копии ReplacingMergeTree).
	gostRowsView = "v_gost_vehicle_safety_certificate"
	// gostDailyView — дневной поток выдачи: день выдачи × марка.
	gostDailyView = "v_gost_vehicle_safety_daily"
)

// gostViewStatements — стейтменты витрин по порядку: два определения, затем комментарии
// таблиц и колонок (ALTER TABLE … MODIFY COMMENT / COMMENT COLUMN — DDL-стиль проекта,
// не COMMENT ON). Комментарии — часть контракта витрины: агент читает их как описание
// данных, и именно в них живут оговорки (лаг публикации, строки до бэкфилла, amendments, год
// выпуска 0, uniqExact, пустой день).
//
// В строковой витрине нет ORDER BY: 3.4 млн строк не сортируются на каждом запросе, вместо
// этого комментарий велит фильтровать по `date`. Технический `uid` в витрину не выводится —
// он провенанс импортёра, а не данные о сертификате. Дневная витрина, наоборот, сортирована
// (строк ~дни × марки): `date DESC` — свежий день сверху, `mark` — стабильный порядок внутри дня.
var gostViewStatements = []string{
	`CREATE OR REPLACE VIEW ` + gostRowsView + ` DEFINER = default SQL SECURITY DEFINER AS
	SELECT date, publication_date, mark, type, certificate_number, ecoclass, enginetrademark, yearofmanufacturing, nameofmanufacturer
	FROM ` + gostTable + ` FINAL`,

	`CREATE OR REPLACE VIEW ` + gostDailyView + ` DEFINER = default SQL SECURITY DEFINER AS
	SELECT date, mark, uniqExact(certificate_number) AS certificates
	FROM ` + gostTable + ` FINAL
	GROUP BY date, mark
	ORDER BY date DESC, mark`,

	"ALTER TABLE " + gostRowsView + " MODIFY COMMENT 'Реестр СБКТС Росстандарта (свидетельства о безопасности конструкции ТС): источник — REST API портала gost.ru, импортёр gost_vehicle_safety_certificate, режимы since (ежечасно) / full (история с 2016). `date` — дата выдачи СБКТС (МСК); `publication_date` — дата батча публикации у источника; строка может обновляться (ReplacingMergeTree FINAL); изменения сертификата дают вторую строку при том же номере; в витрине нет технического `uid`; фильтруйте по `date` — витрина не сортирована'",

	"ALTER TABLE " + gostRowsView + " COMMENT COLUMN date '`date` — дата выдачи (МСК)'",
	"ALTER TABLE " + gostRowsView + " COMMENT COLUMN publication_date '`publication_date` — дата публикации среза у источника (лаг к выдаче ~2 суток); у строк до бэкфилла — 1970-01-01'",
	"ALTER TABLE " + gostRowsView + " COMMENT COLUMN mark '`mark` — марка ТС (`product`)'",
	"ALTER TABLE " + gostRowsView + " COMMENT COLUMN type '`type` — тип/модель ТС (`type`)'",
	"ALTER TABLE " + gostRowsView + " COMMENT COLUMN certificate_number '`certificate_number` — номер СБКТС («ТС RU …»)'",
	"ALTER TABLE " + gostRowsView + " COMMENT COLUMN ecoclass '`ecoclass` — экологический класс, `-` — не применимо'",
	"ALTER TABLE " + gostRowsView + " COMMENT COLUMN enginetrademark '`enginetrademark` — двигатель (модель, топливо: дизель/бензин/электро)'",
	"ALTER TABLE " + gostRowsView + " COMMENT COLUMN yearofmanufacturing '`yearofmanufacturing` — год выпуска, `0` — не указан или строка до бэкфилла'",
	"ALTER TABLE " + gostRowsView + " COMMENT COLUMN nameofmanufacturer '`nameofmanufacturer` — производитель'",

	"ALTER TABLE " + gostDailyView + " MODIFY COMMENT 'Дневной поток выдачи СБКТС: одна строка на (день выдачи × марка), `certificates` — число уникальных номеров (`uniqExact`), а не строк таблицы; счётчик импорта автомобилей по реестру Росстандарта; пустой день отсутствует (не 0)'",

	"ALTER TABLE " + gostDailyView + " COMMENT COLUMN date '`date` — день выдачи (МСК)'",
	"ALTER TABLE " + gostDailyView + " COMMENT COLUMN mark '`mark` — марка ТС'",
	"ALTER TABLE " + gostDailyView + " COMMENT COLUMN certificates '`certificates` — выдано сертификатов за день (уникальные номера)'",
}

// gostPublishViews публикует витрины: стейтменты исполняются по порядку (определение →
// комментарии), первый сбой останавливает публикацию. Ошибка фатальна для прогона: витрина,
// определения которой нет, — это чтение агента по устаревшей или отсутствующей схеме, а
// не «прогон прошёл без витрин». Вызывается только в пишущих режимах (since/full).
func gostPublishViews(ctx context.Context, conn driver.Conn) error {
	for _, stmt := range gostViewStatements {
		if err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("%s: публикация витрин: %w", gostTable, err)
		}
	}
	return nil
}
