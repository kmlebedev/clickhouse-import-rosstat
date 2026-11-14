package cbr

import (
	"testing"
	"time"
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
