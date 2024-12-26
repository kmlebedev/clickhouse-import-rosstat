package chimport

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type ImportStat interface {
	Name() string
	Import(ctx context.Context, conn driver.Conn) (count int64, err error)
}

var (
	Stats []ImportStat
)
