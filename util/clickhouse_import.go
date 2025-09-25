package util

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/xuri/excelize/v2"
)

type ClickHouseImport struct {
	TableName   string
	CreateTable string
	DataUrl     string
	TimeLayout  string
	ImportFunc  func(xlsx *excelize.File, conn driver.Batch) error
}

func (s *ClickHouseImport) Name() string {
	return s.TableName
}

func (s *ClickHouseImport) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, fmt.Sprintf(s.CreateTable, s.TableName)); err != nil {
		return 0, err
	}

	xlsx, err := GetXlsx(s.DataUrl)
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
