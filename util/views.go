package util

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// View — витрина для MCP-агента с комментариями таблицы и колонок.
// Tables — таблицы-источники группы: витрина создаётся, только когда все они уже есть.
type View struct {
	Name    string
	Tables  []string
	Select  string
	Comment string
	Columns map[string]string
}

// CreateView создаёт или пересоздаёт витрину с комментариями. Если хотя бы одной
// таблицы группы ещё нет (импортёры запускаются по одному), витрина пропускается.
func CreateView(ctx context.Context, conn driver.Conn, v View) (created bool, err error) {
	quoted := make([]string, len(v.Tables))
	for i, t := range v.Tables {
		quoted[i] = "'" + t + "'"
	}
	var existing uint64
	row := conn.QueryRow(ctx, fmt.Sprintf(
		"SELECT count() FROM system.tables WHERE database = currentDatabase() AND name IN (%s)",
		strings.Join(quoted, ", ")))
	if err = row.Scan(&existing); err != nil {
		return false, err
	}
	if existing < uint64(len(v.Tables)) {
		return false, nil
	}
	stmts := []string{
		fmt.Sprintf("CREATE OR REPLACE VIEW %s DEFINER = default SQL SECURITY DEFINER AS %s", v.Name, v.Select),
		fmt.Sprintf("ALTER TABLE %s MODIFY COMMENT '%s'", v.Name, escapeComment(v.Comment)),
	}
	for col, comment := range v.Columns {
		stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s COMMENT COLUMN %s '%s'", v.Name, col, escapeComment(comment)))
	}
	for _, stmt := range stmts {
		if err = conn.Exec(ctx, stmt); err != nil {
			return false, fmt.Errorf("%s: %w", v.Name, err)
		}
	}
	return true, nil
}

func escapeComment(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), `'`, `\'`)
}
