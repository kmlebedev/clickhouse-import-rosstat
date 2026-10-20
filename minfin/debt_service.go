package minfin

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

const (
	debtServiceTagURL           = minfinURL + "/ru/document?TAG_ID_4%5B%5D=76"
	debtServiceDocSel           = `a[href*="osnovnye_pokazateli_ispolneniya_federalnogo_byudzheta"]`
	debtServiceFileSel          = `a[href*="indicatorsBudget_"]`
	debtServiceIndicatorsSeries = "Расходы на обслуживание"
)

// debtServiceIndicatorsBackfill — финальные годовые выпуски indicatorsBudget:
// 01.01.2025 покрывает 2024 год, 01.01.2026 — 2025. Более ранние годовые файлы
// на сайте отсутствуют; 2022–2023 — названный пробел source report §5.
var debtServiceIndicatorsBackfill = []struct {
	year int
	url  string
}{
	{2024, "https://minfin.gov.ru/common/upload/library/2025/01/main/indicatorsBudget_01.01.2025.xlsx"},
	{2025, "https://minfin.gov.ru/common/upload/library/2026/01/main/indicatorsBudget_01.01.2026.xlsx"},
}

// minfinDebtService — мультифайловый импортёр: бюджетная строка обслуживания
// (тот же *_mes.xlsx, что у minfin_fed_bud_mes) и факт заимствований из
// indicatorsBudget_*.xlsx. Оба файла обновляются раз в месяц, шаг DAG —
// monthly; префлайт не объявлен, повтор перезаписывает точки.
type minfinDebtService struct {
	util.ClickHouseImport
}

var debtServiceImporter = &minfinDebtService{ClickHouseImport: util.ClickHouseImport{
	TableName:   debtServiceTable,
	CreateTable: minfinSeriesCreate(false),
	AfterImport: func(ctx context.Context, conn driver.Conn) error {
		return publishOfzStress(ctx, conn, debtServiceSeriesMeta)
	},
}}

func (s *minfinDebtService) Import(ctx context.Context, conn driver.Conn) (int64, error) {
	for _, stmt := range s.CreateTable {
		if err := conn.Exec(ctx, fmt.Sprintf(stmt, s.TableName)); err != nil {
			return 0, err
		}
	}

	var total int64
	budgetURL, err := resolveMinfinFile(minfinURL+fedbudExecutePath, fedbudMesLinkSelector, debtServiceTable)
	if err != nil {
		return total, err
	}
	budget := util.ClickHouseImport{
		TableName:   s.TableName,
		CreateTable: s.CreateTable,
		DataUrl:     budgetURL,
		ImportFunc:  debtServiceBudgetImport,
		AfterImport: s.AfterImport,
	}
	imported, err := budget.Import(ctx, conn)
	if err != nil {
		return total, fmt.Errorf("%s: бюджетная строка обслуживания: %w", debtServiceTable, err)
	}
	total += imported

	for _, file := range debtServiceIndicatorsBackfill {
		points, err := util.CountPoints(ctx, conn, fmt.Sprintf(
			"SELECT count() FROM %s FINAL WHERE name = '%s' AND date >= '%d-01-01' AND date <= '%d-12-01'",
			debtServiceTable, debtServiceIndicatorsSeries, file.year, file.year))
		if err != nil {
			return total, fmt.Errorf("%s: гейт показателей %d: %w", debtServiceTable, file.year, err)
		}
		if points > 0 {
			continue
		}
		inner := util.ClickHouseImport{
			TableName:   s.TableName,
			CreateTable: s.CreateTable,
			DataUrl:     file.url,
			ImportFunc:  parseIndicatorsBudget,
			AfterImport: s.AfterImport,
		}
		imported, err = inner.Import(ctx, conn)
		if err != nil {
			return total, fmt.Errorf("%s: показатели %d: %w", debtServiceTable, file.year, err)
		}
		total += imported
	}

	indicatorsURL, err := resolveIndicatorsBudgetFileURL()
	if err != nil {
		return total, err
	}
	indicators := util.ClickHouseImport{
		TableName:   s.TableName,
		CreateTable: s.CreateTable,
		DataUrl:     indicatorsURL,
		ImportFunc:  parseIndicatorsBudget,
		AfterImport: s.AfterImport,
	}
	imported, err = indicators.Import(ctx, conn)
	if err != nil {
		return total, fmt.Errorf("%s: факт заимствований: %w", debtServiceTable, err)
	}
	return total + imported, nil
}

// resolveIndicatorsBudgetFileURL — тег «Госдолг» → документ «Основные показатели
// исполнения федерального бюджета … в части государственных внутренних
// заимствований» → XLSX. Первый матч на первых страницах тега — последний выпуск;
// имя файла indicatorsBudget_<DD.MM.YYYY>.xlsx содержит дату публикации и
// меняется, поэтому ссылка берётся со страницы.
func resolveIndicatorsBudgetFileURL() (string, error) {
	docURL, err := resolveFirstTagDoc(debtServiceTagURL, debtServiceDocSel, debtServiceTable, 5)
	if err != nil {
		return "", err
	}
	docPage, err := util.GetPage(docURL)
	if err != nil {
		return "", fmt.Errorf("%s: страница документа %s: %w", debtServiceTable, docURL, err)
	}
	return minfinLink(docPage, docURL, debtServiceFileSel, debtServiceTable)
}
