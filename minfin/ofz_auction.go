package minfin

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

const (
	// ofzAuctionTagURL — тег «Аукцион»: на странице первым идёт последний годовой
	// документ «Результаты проведенных аукционов … в <год> году».
	ofzAuctionTagURL       = minfinURL + "/ru/document?TAG_ID_4%5B%5D=1349"
	ofzAuctionDocSelector  = `a[href*="rezultaty_provedennykh_auktsionov"]`
	ofzAuctionFileSelector = `a[href$=".xlsx"]`
	ofzAuctionCountPoints  = "SELECT count() FROM minfin_ofz_auction FINAL"
)

// ofzHistoricalAuctionFiles — финальные годовые файлы, которые не обновляются.
// 2023 опубликован в 2024/01, 2024 — в 2025/01, 2025 — в 2026/01. Для 2024
// страница документа в теге 1349 не найдена (source report §2), поэтому URL
// файла зафиксирован здесь; это названное отступление, а не реконструкция ссылки.
var ofzHistoricalAuctionFiles = []struct {
	year int
	url  string
}{
	{2023, "https://minfin.gov.ru/common/upload/library/2024/01/main/INTERNET_Auction_Results_rus_2023_20231231.xlsx"},
	{2024, "https://minfin.gov.ru/common/upload/library/2025/01/main/INTERNET_Auction_Results_rus_2024_20241231.xlsx"},
	{2025, "https://minfin.gov.ru/common/upload/library/2026/01/main/INTERNET_Auction_Results_rus_2025_20251231.xlsx"},
}

// minfinOfzAuction — мультифайловый импортёр: один шаблон util.ClickHouseImport
// на файл (прецедент cbr_dip_bulletin). Внешний Import() нужен потому, что
// шаблон умеет один DataUrl, а история аукционов — это набор годовых файлов;
// префлайт и архив объявлены только у текущего файла, финальные годы закрыты
// гейтом по своей таблице.
type minfinOfzAuction struct {
	util.ClickHouseImport
}

var ofzAuctionImporter = &minfinOfzAuction{ClickHouseImport: util.ClickHouseImport{
	TableName:   ofzAuctionTable,
	CreateTable: []string{ofzAuctionDdl},
	AfterImport: func(ctx context.Context, conn driver.Conn) error {
		return publishOfzStress(ctx, conn, ofzAuctionSeriesMeta)
	},
}}

// Import — DDL → исторические годы (если года ещё нет в таблице) → текущий
// год. Текущий файл резолвится со страницы тега, поэтому смена имени файла и
// каталога library/<год>/<месяц>/main/ не ломает импорт. Повтор текущего файла
// без изменений даёт no-op по HEAD-префлайту; исторические файлы повторно не
// качаются.
func (s *minfinOfzAuction) Import(ctx context.Context, conn driver.Conn) (int64, error) {
	for _, stmt := range s.CreateTable {
		if err := conn.Exec(ctx, fmt.Sprintf(stmt, s.TableName)); err != nil {
			return 0, err
		}
	}

	var total int64
	for _, file := range ofzHistoricalAuctionFiles {
		points, err := util.CountPoints(ctx, conn,
			fmt.Sprintf("SELECT count() FROM %s FINAL WHERE toYear(date) = %d", ofzAuctionTable, file.year))
		if err != nil {
			return total, fmt.Errorf("%s: гейт года %d: %w", ofzAuctionTable, file.year, err)
		}
		if points > 0 {
			log.Infof("%s: год %d уже импортирован (%d точек) — файл не качается", ofzAuctionTable, file.year, points)
			continue
		}
		inner := util.ClickHouseImport{
			TableName:   s.TableName,
			CreateTable: s.CreateTable,
			DataUrl:     file.url,
			ImportFunc:  parseOfzAuction,
			AfterImport: s.AfterImport,
		}
		imported, err := inner.Import(ctx, conn)
		if err != nil {
			return total, fmt.Errorf("%s: год %d: %w", ofzAuctionTable, file.year, err)
		}
		total += imported
	}

	dataURL, err := resolveOfzAuctionFileURL()
	if err != nil {
		return total, err
	}
	log.Infof("%s: текущий годовой файл %s", ofzAuctionTable, dataURL)
	inner := util.ClickHouseImport{
		TableName:              s.TableName,
		CreateTable:            s.CreateTable,
		DataUrl:                dataURL,
		ImportFunc:             parseOfzAuction,
		AfterImport:            s.AfterImport,
		PublicationCountPoints: ofzAuctionCountPoints,
	}
	imported, err := inner.Import(ctx, conn)
	if err != nil {
		return total, err
	}
	return total + imported, nil
}

// resolveOfzAuctionFileURL — тег 1349 → документ последнего годового файла →
// ссылка на XLSX. Ссылки берутся со страниц, а не строятся по дате: имя файла
// содержит дату публикации, а каталог library/<год>/<месяц>/main/ меняется.
func resolveOfzAuctionFileURL() (string, error) {
	page, err := util.GetPage(ofzAuctionTagURL)
	if err != nil {
		return "", fmt.Errorf("%s: страница тега %s: %w", ofzAuctionTable, ofzAuctionTagURL, err)
	}
	docURL, err := minfinLink(page, ofzAuctionTagURL, ofzAuctionDocSelector, ofzAuctionTable)
	if err != nil {
		return "", err
	}
	docPage, err := util.GetPage(docURL)
	if err != nil {
		return "", fmt.Errorf("%s: страница документа %s: %w", ofzAuctionTable, docURL, err)
	}
	return minfinLink(docPage, docURL, ofzAuctionFileSelector, ofzAuctionTable)
}
