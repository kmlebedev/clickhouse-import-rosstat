package util

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"strconv"
)

func Import(ctx context.Context, conn driver.Conn, ddl string, insert string, table *[][]string) (count int64, err error) {
	if err = conn.Exec(ctx, ddl); err != nil {
		return count, err
	}
	batch, err := conn.PrepareBatch(ctx, insert)
	if err != nil {
		return count, err
	}
	for _, row := range *table {
		value, _ := strconv.ParseFloat(row[3], 32)
		if err = batch.Append(row[0], row[1], float32(value)); err != nil {
			return count, err
		}
		count++
	}
	if err = batch.Send(); err != nil {
		return count, err
	}
	return count, nil
}
