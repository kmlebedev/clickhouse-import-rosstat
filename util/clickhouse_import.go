package util

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/xuri/excelize/v2"
)

const HttpUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"

type ClickHouseImport struct {
	TableName   string
	CreateTable string
	DataUrl     string
	TimeLayout  string
	CrawFunc    func(crawUrl string, conn driver.Conn) error
	ImportFunc  func(xlsx *excelize.File, batch driver.Batch) error
}

func (s *ClickHouseImport) Name() string {
	return s.TableName
}

func (s *ClickHouseImport) ImportXls(ctx context.Context, dataUrl string, conn driver.Conn) (count int64, err error) {
	xlsx, err := GetXlsx(dataUrl)
	if err != nil {
		return 0, err
	}
	defer xlsx.Close()
	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", s.TableName))
	if err != nil {
		return count, err
	}
	if err = s.ImportFunc(xlsx, batch); err != nil {
		return count, err
	}
	if err = batch.Send(); err != nil {
		return count, err
	}
	return count, nil
}

func (s *ClickHouseImport) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, fmt.Sprintf(s.CreateTable, s.TableName)); err != nil {
		return 0, err
	}
	switch {
	case s.CrawFunc != nil:
		if err = s.CrawFunc(s.DataUrl, conn); err != nil {
			return count, err
		}
	case s.ImportFunc != nil:
		return s.ImportXls(ctx, s.DataUrl, conn)
	}

	return count, nil
}
