package cbr

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/xuri/excelize/v2"
	"time"
)

type hdBase struct {
	name        string
	createTable string
	dataUrl     string
	importFunc  func(xlsx *excelize.File, conn driver.Batch) error
}

func (s *hdBase) Name() string {
	return s.name
}
func (s *hdBase) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, fmt.Sprintf(s.createTable, s.name)); err != nil {
		return 0, err
	}
	xlsx, err := util.GetXlsx(fmt.Sprintf(s.dataUrl, time.Now().Format("01/02/2006")))
	if err != nil {
		return 0, err
	}
	defer xlsx.Close()
	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", s.name))
	if err != nil {
		return count, err
	}
	if err = s.importFunc(xlsx, batch); err != nil {
		return count, err
	}
	if err = batch.Send(); err != nil {
		return count, err
	}
	return count, nil
}
