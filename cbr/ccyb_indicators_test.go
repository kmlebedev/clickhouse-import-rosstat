package cbr

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	log "github.com/sirupsen/logrus"
)

// TestCbrCcybStaleDays: порог предупреждения о замороженном ряде считается по календарным
// суткам; точка «из будущего» даёт 0, а не отрицательный разрыв.
func TestCbrCcybStaleDays(t *testing.T) {
	now := time.Date(2026, time.November, 14, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		last time.Time
		want int
	}{
		{time.Date(2024, time.October, 1, 0, 0, 0, 0, time.UTC), 774},
		{time.Date(2026, time.November, 13, 0, 0, 0, 0, time.UTC), 1},
		{time.Date(2026, time.November, 14, 0, 0, 0, 0, time.UTC), 0},
		{time.Date(2027, time.January, 1, 0, 0, 0, 0, time.UTC), 0},
	}
	for _, c := range cases {
		if got := cbrCcybStaleDays(c.last, now); got != c.want {
			t.Errorf("разрыв на %s = %d дн., want %d", c.last.Format("2006-01-02"), got, c.want)
		}
	}
	if cbrCcybStaleLimit <= 60 {
		t.Errorf("порог %d дн. слишком мал: файл ЦБ обновляется раз в месяц-квартал", cbrCcybStaleLimit)
	}
}

// TestCbrCcybStaleDaysSessionTimezone — регрессия P1: clickhouse-go отдаёт Date в таймзоне
// сессии (Asia/Yekaterinburg, UTC+5), поэтому .UTC().Truncate(24h) сдвигал
// 2024-10-01T00:00+05:00 на предыдущие сутки и давал 775 дн. вместо земляных 774
// (dateDiff('day', max(date), today()) в dev-ClickHouse). Оба времени — в зоне сессии,
// 09:17 — тон ежедневного прогона DAG.
func TestCbrCcybStaleDaysSessionTimezone(t *testing.T) {
	yek := time.FixedZone("YEK", 5*3600)
	last := time.Date(2024, time.October, 1, 0, 0, 0, 0, yek)
	now := time.Date(2026, time.November, 14, 9, 17, 0, 0, yek)
	if got := cbrCcybStaleDays(last, now); got != 774 {
		t.Errorf("разрыв на %s = %d дн., want 774 (dateDiff по календарным суткам)",
			last.Format(time.RFC3339), got)
	}
}

// TestWarnCbrCcybStaleEmptyTable — регрессия P2: max(date) по пустой Date-колонке ClickHouse
// возвращает 1970-01-01, а не NULL, поэтому IsZero() не отсекал пустую таблицу и сторож
// печатал ложное «ряд заморожен» с разрывом ~20 771 день.
func TestWarnCbrCcybStaleEmptyTable(t *testing.T) {
	var logged bytes.Buffer
	prevOut := log.StandardLogger().Out
	log.SetOutput(&logged)
	t.Cleanup(func() { log.SetOutput(prevOut) })

	conn := &ccybFakeConn{last: time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)}
	now := time.Date(2026, time.November, 14, 9, 17, 0, 0, time.UTC)
	if err := warnCbrCcybStale(context.Background(), conn, now); err != nil {
		t.Fatalf("warnCbrCcybStale: %v", err)
	}
	if strings.Contains(logged.String(), "последняя точка") {
		t.Errorf("пустая таблица (max(date) = 1970-01-01) дала предупреждение: %s", logged.String())
	}
}

// TestWarnCbrCcybStaleMessage — предупреждение содержит факты (дату, разрыв 774, порог) и
// реальные указатели: docs/IMPORTERS.md и гипотезу про Last-Modified. Обрыв CCYB
// фиксируется и в docs/LIMITATIONS.md (issue #578), поэтому слово LIMITATIONS не запрещено.
func TestWarnCbrCcybStaleMessage(t *testing.T) {
	var logged bytes.Buffer
	prevOut := log.StandardLogger().Out
	log.SetOutput(&logged)
	t.Cleanup(func() { log.SetOutput(prevOut) })

	yek := time.FixedZone("YEK", 5*3600)
	conn := &ccybFakeConn{last: time.Date(2024, time.October, 1, 0, 0, 0, 0, yek)}
	now := time.Date(2026, time.November, 14, 9, 17, 0, 0, yek)
	if err := warnCbrCcybStale(context.Background(), conn, now); err != nil {
		t.Fatalf("warnCbrCcybStale: %v", err)
	}
	msg := logged.String()
	for _, want := range []string{
		"последняя точка 2024-10-01 — 774 дн. назад (порог 120)",
		"docs/IMPORTERS.md",
		"гипотеза:",
		"проверьте Last-Modified",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("предупреждение не содержит %q: %s", want, msg)
		}
	}
}

// ccybFakeConn — фейковое соединение сторожа: объявлен только QueryRow max(date); остальные
// методы унаследованы nil-заглушками driver.Conn (вызов — паника: фейк его не обещал).
type ccybFakeConn struct {
	driver.Conn
	last time.Time
}

func (c *ccybFakeConn) QueryRow(context.Context, string, ...any) driver.Row {
	return ccybFakeRow{last: c.last}
}

// ccybFakeRow — ответ max(date); объявлен только Scan.
type ccybFakeRow struct {
	driver.Row
	last time.Time
}

func (r ccybFakeRow) Scan(dest ...any) error {
	if len(dest) != 1 {
		return fmt.Errorf("фейковый ответ max(date): приёмников %d, ожидался один", len(dest))
	}
	target, ok := dest[0].(*time.Time)
	if !ok {
		return fmt.Errorf("фейковый ответ max(date): приёмник %T, ожидался *time.Time", dest[0])
	}
	*target = r.last
	return nil
}
