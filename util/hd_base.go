package util

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/xuri/excelize/v2"
	"time"
)

type HdBase struct {
	TableName         string
	CreateTable       string
	DataUrl           string
	DataUrlTimeFormat bool
	ImportFunc        func(xlsx *excelize.File, conn driver.Batch) error
}

func (s *HdBase) Name() string {
	return s.TableName
}

func (s *HdBase) GetDataUrl() string {
	if s.DataUrlTimeFormat {
		return fmt.Sprintf(s.DataUrl, time.Now().Format("01/02/2006"))
	} else {
		return s.DataUrl
	}
}

// Import(ctx context.Context, conn driver.Conn) (count int64, err error)
func (s *HdBase) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, fmt.Sprintf(s.CreateTable, s.TableName)); err != nil {
		return 0, err
	}

	xlsx, err := GetXlsx(s.GetDataUrl())
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
