package domrf

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
)

// Импортёр domrf_mortgage_market — месячный рынок ипотеки РФ из книги «Общая таблица по РФ»
// (лист «общая таблица_помесячно»): выдачи, ДДУ, портфель, просрочка и ставки за 2006-01 … 2026-09.
//
// Ссылка на книгу берётся со страницы аналитики каждый прогон (блок statistical_series):
// адрес файла — /api/v2/content/download/<id>/<token>/, токен меняется с выпуском, статичного
// литерала в коде нет.
type domrfMortgageMarket struct {
	util.ClickHouseImport
}

const (
	// domrfMarketTable — имя таблицы: TableName шаблона, Name() и ключ версии точки контроля.
	domrfMarketTable = "domrf_mortgage_market"
	// domrfMarketDdl — DDL в %s-форме (стандарт §5.1): имя подставляет шаблон.
	domrfMarketDdl = `CREATE TABLE IF NOT EXISTS %s (
			  series LowCardinality(String)
			, date Date
			, value Float64
		) ENGINE = ReplacingMergeTree ORDER BY (series, date);
	`
	// domrfMarketCountPoints — количественный чек точки контроля публикации. FINAL обязателен:
	// без него count() считает несмёрженные части и растёт после повторного импорта.
	domrfMarketCountPoints = "SELECT count() FROM " + domrfMarketTable + " FINAL"
	// domrfMarketBlock — имя атрибута main#app, из которого берётся ссылка на книгу.
	domrfMarketBlock = "statistical_series"
)

// domrfMarketBlockJSON — блок statistical_series страницы: подзаголовок с датой данных и файл
// книги. props.file.date — дата публикации выпуска, props.sub_title — «Данные на 01.09.2026»;
// оба поля идут в журнал, чтобы в логе прогона было видно, какой выпуск прочитан.
type domrfMarketBlockJSON struct {
	Props struct {
		SubTitle string `json:"sub_title"`
		File     struct {
			Src  string `json:"src"`
			Date string `json:"date"`
		} `json:"file"`
	} `json:"props"`
}

func (s *domrfMortgageMarket) Name() string {
	return domrfMarketTable
}

// beforeImport вычисляет DataUrl во время Import(), а не в init(): сетевые вызовы в init()
// запрещены (ARCHITECTURE.md §8.6). Страница качается через domrfGetPage — с повторами на
// JS-оболочку ServicePipe.
func (s *domrfMortgageMarket) beforeImport(_ context.Context, _ driver.Conn) error {
	page, err := domrfGetPage(domrfAnalyticsPage)
	if err != nil {
		return fmt.Errorf("%s: %w", domrfMarketTable, err)
	}
	blocks, err := domrfBlocks(page)
	if err != nil {
		return fmt.Errorf("%s: %w", domrfMarketTable, err)
	}
	block, err := domrfBlock[domrfMarketBlockJSON](blocks, domrfMarketBlock)
	if err != nil {
		return fmt.Errorf("%s: %w", domrfMarketTable, err)
	}
	if block.Props.File.Src == "" {
		return fmt.Errorf("%s: блок %q без файла книги", domrfMarketTable, domrfMarketBlock)
	}
	s.DataUrl = domrfSite + block.Props.File.Src
	log.Infof("%s: книга %s (публикация %s, %s)", domrfMarketTable, s.DataUrl, block.Props.File.Date, block.Props.SubTitle)
	return nil
}

// importXls разбирает книгу в батч. Разбор чистый (domrfMarketPoints), запись — единственная
// точка касания БД: ошибка Append называет точку, чтобы сбой не выглядел «0 строк».
func (s *domrfMortgageMarket) importXls(xlsx *excelize.File, batch driver.Batch) error {
	points, err := domrfMarketPoints(xlsx)
	if err != nil {
		return fmt.Errorf("%s: %w", domrfMarketTable, err)
	}
	for i, point := range points {
		if err = batch.Append(point.Series, point.Date, point.Value); err != nil {
			return fmt.Errorf("%s: append точки %d (%s, %s): %w", domrfMarketTable, i+1, point.Series, point.Date.Format("2006-01-02"), err)
		}
	}
	log.Infof("%s: разобрано %d точек (%d рядов)", domrfMarketTable, len(points), len(domrfMarketSeriesList))
	return nil
}

// afterImport публикует производные — каталог рядов и витрины пакета (стандарт §5.2: точки →
// производные → строка версии). Каталог общий для трёх импортёров: каждый прогон перезаписывает
// все описания, поэтому каталог сходится, даже если в расписании отработал только один импортёр.
func (s *domrfMortgageMarket) afterImport(ctx context.Context, conn driver.Conn) error {
	return afterDomrfMacro(ctx, conn)
}

func newDomrfMortgageMarketImporter() *domrfMortgageMarket {
	s := &domrfMortgageMarket{}
	s.ClickHouseImport = util.ClickHouseImport{
		TableName:              domrfMarketTable,
		CreateTable:            []string{domrfMarketDdl},
		PublicationCountPoints: domrfMarketCountPoints,
		BeforeImport:           s.beforeImport,
		ImportFunc:             s.importXls,
		AfterImport:            s.afterImport,
	}
	return s
}
