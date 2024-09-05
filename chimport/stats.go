package chimport

import (
	"context"
	"database/sql"
)

type ImportStat interface {
	Name() string
	Import(ctx context.Context, conn *sql.DB) (count int64, err error)
}

var (
	Stats []ImportStat
)
