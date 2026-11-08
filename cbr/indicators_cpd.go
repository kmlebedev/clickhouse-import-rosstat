package cbr

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Todo update data source https://www.cbr.ru/statistics/ddkp/aipd/
// Показатели сезонно сглаженной динамики потребительских цен
// const indicatorsCpdDataUrl = "https://www.cbr.ru/Content/Document/File/108632/indicators_cpd.xlsx"

const indicatorsCpdUrl = "https://www.cbr.ru/statistics/ddkp/aipd/"

// cbrIndicatorsCpdTable — таблица импортёра: имя попадает в текст ошибки пустого разбора,
// чтобы дежурный по логу видел, чей ряд остался без обновления.
const cbrIndicatorsCpdTable = "cbr_indicators_cpd"

// cbrIndicatorsCpdCountPoints — количественный чек версии (спека §3.5): число точек
// своего ряда. Литерал, как и таблица: запрос — часть утверждения «эти точки мои».
// FINAL обязателен — иначе count() считал бы несмёрженные части.
const cbrIndicatorsCpdCountPoints = "SELECT count() FROM cbr_indicators_cpd FINAL"

// indicatorsCpdLinkSelector — ссылка на indicators_cpd.xlsx на странице aipd по имени
// файла, а не по позиции блока: на снапшоте 2026-11-04 в начале .col-md-23 обнаружился
// <script type='application/ld+json'>, позиционный div:nth-child(6) сместился на блок
// est_infl.xlsx, и импортёр падал на чужом файле (sheet Лист1 does not exist). Имя файла
// стабильно, id в URL меняется при обновлении — тот же приём селектора, что у
// cbrCreditM2xLinkSelector (домен здесь хардкодом, у credit_m2x — константа cbrUrl).
const indicatorsCpdLinkSelector = `a.referenceable[href$="indicators_cpd.xlsx"]`

type indicatorsCpdStat struct {
	util.ClickHouseImport
}

// Import берёт адрес актуального XLSX со страницы aipd: id файла в URL меняется при
// обновлении, поэтому ссылку ищем на странице, а не хардкодим. Вызывается из Import():
// сетевые вызовы в init() запрещены (ARCHITECTURE.md §8.6). Производные публикуются шаблоном
// после точек и до строки версии; на no-op-пути шаблон их не зовёт (возвращает count до
// AfterImport), поэтому их публикует эта обёртка — иначе на уже заполненной БД
// v_cbr_core_quarterly не появилась бы до следующей ревизии файла ЦБ.
func (s *indicatorsCpdStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	page, err := util.GetPage(indicatorsCpdUrl)
	if err != nil {
		return count, fmt.Errorf("indicatorsCpd: %s: %w", indicatorsCpdUrl, err)
	}
	href, err := util.FindLink(page, indicatorsCpdLinkSelector)
	if err != nil {
		return count, fmt.Errorf("indicatorsCpd: не найдена ссылка на xlsx на %s: %w", indicatorsCpdUrl, err)
	}
	s.DataUrl = "https://www.cbr.ru" + href
	count, err = s.ClickHouseImport.Import(ctx, conn)
	// Производные публикуются шаблоном после точек и до строки версии; повторный вызов здесь
	// страхует no-op-путь, на котором шаблон отдаёт число точек, не доходя до AfterImport: на уже
	// заполненной БД v_cbr_core_quarterly иначе не появилась бы до следующей ревизии файла ЦБ.
	// На полном пути вызов второй (идемпотентный: upsert каталога + CREATE OR REPLACE VIEW), и
	// сбой первого после записи версии лечится следующим прогоном, а не ждёт новой ревизии.
	if derivedErr := afterCbrIndicatorsCpd(ctx, conn); derivedErr != nil && err == nil {
		err = derivedErr
	}
	return count, err
}

// cbrIndicatorsCpdImporter — сезонно сглаженная динамика потребительских цен;
// регистрируется реестром cbr/importers.go.
var cbrIndicatorsCpdImporter = &indicatorsCpdStat{ClickHouseImport: util.ClickHouseImport{
	TableName: cbrIndicatorsCpdTable,
	CreateTable: []string{`CREATE TABLE IF NOT EXISTS %s (
              name LowCardinality(String)
			, date Date
			, value Float32
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);`},
	ImportFunc:             indicatorsCpdImport,
	AfterImport:            afterCbrIndicatorsCpd,
	PublicationCountPoints: cbrIndicatorsCpdCountPoints,
}}

// cbrCoreQuarterlySeriesMeta — каталог квартального ряда «Базовый ИПЦ:SAAR_q» (спека
// 2026-11-08-cpi-quarterly-metrics §5.1): производного от месячного SA м/м «Базового ИПЦ», но с
// годовой единицей и привязкой даты квартала. source — 'cbr' (общая витрина v_cbr_macro, где
// живут месячные ряды файла), поэтому у cbr в каталоге появляется ряд, которого нет в v_cbr_macro:
// он живёт в v_cbr_core_quarterly, и origin это называет. Винтажей нет — cbr_indicators_cpd
// хранит текущую редакцию файла, и это оговорено в описании, а не замалчивается.
var cbrCoreQuarterlySeriesMeta = []util.SeriesMeta{{
	Source:    cbrSource,
	Series:    "Базовый ИПЦ:SAAR_q",
	Title:     "Базовая инфляция: квартальный SAAR",
	Unit:      "% годовых",
	Frequency: "Q",
	Origin: "cbr.ru/statistics/ddkp/aipd, indicators_cpd.xlsx, лист «Лист1», строка «Базовый ИПЦ»; " +
		"витрина v_cbr_core_quarterly (среднее годовых темпов из SA м/м за квартал)",
	Description: "Квартальное среднее годовых темпов из сезонно очищенных месячных темпов " +
		"«Базового ИПЦ»: (pow(1 + SA м/м / 100, 12) − 1) × 100 по каждому месяцу, затем среднее за " +
		"квартал (3к26 — 6,21 %, ЦБ называет 6,2). value файла — уже SA м/м в процентах, повторная " +
		"сезонная корректировка не применяется. Дата точки — quarter_start, первый день отчётного " +
		"квартала (2026-07-01 = III кв. 2026), читаемая метка — колонка quarter («3к26»); это не " +
		"каноническая привязка каталога «1-е число квартала, следующего за отчётным». months = 3 — " +
		"полный квартал; у 4к26 строки нет до публикации октябрьской базовой (14.11) — «н/д», не " +
		"ноль. Винтажей нет: cbr_indicators_cpd — текущая редакция файла, поэтому цитируемый в " +
		"старом релизе квартал может расходиться сильнее, чем на 0,2 п.п. (2к26 в июльском релизе " +
		"— 4,2 % против 4,57 % текущей редакции): это ревизия файла, а не ошибка формулы.",
}}

// https://www.cbr.ru/analytics/dkp/dinamic/
// indicatorsCpdImport разбирает лист «Лист1»: строки «Все товары и услуги» и «Базовый ИПЦ»
// раскладываются по месяцам из строки заголовка (date = первое число месяца − 1). Пустой
// лист/отсутствие обеих строк/ноль точек — ошибка (fail-fast): молчаливый «0 rows» выглядел
// бы успешным прогоном, а ряд остался бы без обновления.
func indicatorsCpdImport(xlsx *excelize.File, batch driver.Batch) error {
	rows, err := xlsx.GetRows("Лист1")
	if err != nil {
		return err
	}
	fileds := []string{"Все товары и услуги", "Базовый ИПЦ"}
	count := 0
	for i, row := range rows {
		if len(row) < 1 || !slices.Contains(fileds, strings.TrimSpace(row[0])) {
			continue
		}
		for j, rowCol := range rows[i][2:] {
			date, err := time.Parse("01/06", rows[0][j+2])
			if err != nil {
				return err
			}
			value, err := strconv.ParseFloat(rowCol, 64)
			if err != nil {
				return err
			}
			if err = batch.Append(row[0], date.AddDate(0, 1, -1), value); err != nil {
				return err
			}
			count++
		}
	}
	if count == 0 {
		return fmt.Errorf("%s: разбор листа «Лист1» не дал ни одной точки", cbrIndicatorsCpdTable)
	}
	return nil
}
