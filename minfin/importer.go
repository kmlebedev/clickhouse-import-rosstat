package minfin

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
)

const minfinURL = "https://minfin.gov.ru"

// minfinFileImporter — общий шаблон импортёров пакета: BeforeImport вычисляет
// ссылку на актуальный XLSX со страницы каталога, ImportFunc разбирает книгу,
// AfterImport публикует каталог рядов и витрину v_minfin_budget. Собственного
// Import() у импортёров нет: полный цикл (DDL -> ссылка -> префлайт -> точки ->
// архив -> производные -> версия) ведёт util.ClickHouseImport.
type minfinFileImporter struct {
	util.ClickHouseImport
	pageURL  string
	selector string
}

func newMinfinFileImporter(
	table, pageURL, selector string,
	createTable []string,
	importFunc func(*excelize.File, driver.Batch) error,
	meta []util.SeriesMeta,
	countPoints string,
) *minfinFileImporter {
	s := &minfinFileImporter{pageURL: pageURL, selector: selector}
	s.ClickHouseImport = util.ClickHouseImport{
		TableName:   table,
		CreateTable: createTable,
		ImportFunc:  importFunc,
		BeforeImport: func(context.Context, driver.Conn) error {
			return s.resolveDataURL()
		},
		AfterImport: func(ctx context.Context, conn driver.Conn) error {
			return publishFedbud(ctx, conn, meta)
		},
		PublicationCountPoints: countPoints,
	}
	return s
}

func (s *minfinFileImporter) resolveDataURL() error {
	page, err := util.GetPage(s.pageURL)
	if err != nil {
		return fmt.Errorf("%s: страница %s: %w", s.TableName, s.pageURL, err)
	}
	dataURL, err := minfinLink(page, s.pageURL, s.selector, s.TableName)
	if err != nil {
		return err
	}
	s.DataUrl = dataURL
	return nil
}

// minfinLink отделяет разбор ссылки от сети: тест проверяет тот же селектор на
// реальной HTML-фикстуре, не скачивая страницу (стандарт §11.3).
func minfinLink(page []byte, pageURL, selector, table string) (string, error) {
	href, err := util.FindLink(page, selector)
	if err != nil {
		return "", fmt.Errorf("%s: страница %s: %w", table, pageURL, err)
	}
	if strings.HasPrefix(href, "http://") || strings.HasPrefix(href, "https://") {
		return href, nil
	}
	return minfinURL + href, nil
}

// minfinSeriesCreate — канонический DDL таблиц пакета. Миграция legacy Float32 ->
// Float64 — второй стейтмент для таблиц, созданных до правила 5 AGENTS.md: на
// новой таблице она no-op, на существующей переводит колонку до первого импорта,
// на уже Float64 безопасно повторяется (mutations_sync делает её детерминированной).
func minfinSeriesCreate(migrate bool) []string {
	ddl := []string{`CREATE TABLE IF NOT EXISTS %s (
              name LowCardinality(String)
			, date Date
			, value Float64
		) ENGINE = ReplacingMergeTree ORDER BY (name, date);`}
	if migrate {
		ddl = append(ddl, `ALTER TABLE %s MODIFY COLUMN value Float64 SETTINGS mutations_sync = 2`)
	}
	return ddl
}
