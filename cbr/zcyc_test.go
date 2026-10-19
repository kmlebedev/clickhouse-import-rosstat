package cbr

import (
	"math"
	"strings"
	"testing"
	"time"
)

// zcycCheckPoint сверяет точку разбора (тенор + дата) со значением фикстуры.
func zcycCheckPoint(t *testing.T, points []zcycPoint, tenor string, date time.Time, want float64) {
	t.Helper()
	for _, p := range points {
		if p.tenor != tenor || !p.date.Equal(date) {
			continue
		}
		if math.Abs(p.value-want) > 1e-9 {
			t.Errorf("точка (%q, %s) = %v, want %v", tenor, date.Format("2006-01-02"), p.value, want)
		}
		return
	}
	t.Errorf("в разборе нет точки (%q, %s)", tenor, date.Format("2006-01-02"))
}

// TestZcycRows2003 — разбор реальной страницы 2003 года: 12 дат × 12 теноров,
// spot-значения первой и последней строки фикстуры.
func TestZcycRows2003(t *testing.T) {
	points, err := zcycRows(readCbrTestdata(t, "zcyc_2003.html"))
	if err != nil {
		t.Fatalf("zcycRows: %v", err)
	}
	if len(points) != 144 {
		t.Fatalf("точек %d, want 144 (12 дат × 12 теноров)", len(points))
	}
	if points[0].tenor != "0.25" || !points[0].date.Equal(time.Date(2003, time.December, 30, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("первая точка = (%q, %s), want (\"0.25\", 2003-12-30): порядок источника — от свежей к старой, теноры слева направо", points[0].tenor, points[0].date.Format("2006-01-02"))
	}
	last := points[len(points)-1]
	if last.tenor != "30" || !last.date.Equal(time.Date(2003, time.January, 4, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("последняя точка = (%q, %s), want (\"30\", 2003-01-04)", last.tenor, last.date.Format("2006-01-02"))
	}

	zcycCheckPoint(t, points, "0.25", time.Date(2003, time.December, 30, 0, 0, 0, 0, time.UTC), 3.95)
	zcycCheckPoint(t, points, "10", time.Date(2003, time.December, 30, 0, 0, 0, 0, time.UTC), 8.51)
	zcycCheckPoint(t, points, "0.25", time.Date(2003, time.January, 4, 0, 0, 0, 0, time.UTC), 11.68)
	zcycCheckPoint(t, points, "30", time.Date(2003, time.January, 4, 0, 0, 0, 0, time.UTC), 13.32)
}

// TestZcycRows2026 — разбор реальной страницы 2026 года: 3 даты × 12 теноров,
// spot-значения включают 10-летний тенор.
func TestZcycRows2026(t *testing.T) {
	points, err := zcycRows(readCbrTestdata(t, "zcyc_2026.html"))
	if err != nil {
		t.Fatalf("zcycRows: %v", err)
	}
	if len(points) != 36 {
		t.Fatalf("точек %d, want 36 (3 даты × 12 теноров)", len(points))
	}
	date := time.Date(2026, time.October, 16, 0, 0, 0, 0, time.UTC)
	zcycCheckPoint(t, points, "10", date, 16.51)
	zcycCheckPoint(t, points, "0.25", date, 12.30)
	zcycCheckPoint(t, points, "30", date, 16.34)
}

// TestZcycRowsEmptyRange — реальный ответ ЦБ на пустой диапазон: таблицы нет, но
// маркер формы UniDbQuery есть — это валидный пустой ряд, а не ошибка.
func TestZcycRowsEmptyRange(t *testing.T) {
	points, err := zcycRows(readCbrTestdata(t, "zcyc_empty.html"))
	if err != nil {
		t.Fatalf("zcycRows: %v", err)
	}
	if len(points) != 0 {
		t.Errorf("точек %d, want 0 (пустой диапазон ЦБ)", len(points))
	}
}

// TestZcycRowsBrokenPage — заглушка без маркера формы UniDbQuery: ошибка, а не
// пустой ряд (WAF/сломанная страница не должна выглядеть успешным прогоном).
func TestZcycRowsBrokenPage(t *testing.T) {
	if _, err := zcycRows([]byte(`<html><body>forbidden</body></html>`)); err == nil {
		t.Fatal("ожидалась ошибка, а заглушка без маркера формы вернула пустой ряд без ошибки")
	}
}

// TestZcycRowsBadValue — минимальная валидная разметка с нечисловым значением:
// непустая непарсящаяся ячейка — ошибка (пустая была бы пропуском). ParseFloat
// принимает NaN/Inf без ошибки, поэтому они проверяются отдельно: доходность
// не может быть не конечным числом.
func TestZcycRowsBadValue(t *testing.T) {
	const head = `<div class="table-wrapper"><table class="data">` +
		`<tr><th rowspan="2">Дата</th><th colspan="12">Срок до погашения, лет</th></tr>` +
		`<tr><th>0,25</th><th>0,5</th><th>0,75</th><th>1</th><th>2</th><th>3</th>` +
		`<th>5</th><th>7</th><th>10</th><th>15</th><th>20</th><th>30</th></tr>`
	const tail = `</table></div>`
	for _, tc := range []struct {
		name  string
		value string
		want  string
	}{
		{"не число", "не-число", "cbr_zcyc"},
		{"NaN", "NaN", "не является конечным числом"},
		{"Inf", "Inf", "не является конечным числом"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			page := head + `<tr><td>16.10.2026</td><td>` + tc.value + `</td></tr>` + tail
			_, err := zcycRows([]byte(page))
			if err == nil {
				t.Fatalf("ожидалась ошибка на значении %q", tc.value)
			}
			if !strings.Contains(err.Error(), "cbr_zcyc") {
				t.Errorf("текст ошибки не называет таблицу cbr_zcyc: %v", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("текст ошибки не содержит %q: %v", tc.want, err)
			}
		})
	}
}

// TestZcycRowsDuplicateTenor — две колонки заголовка нормализуются в один
// канонический тенор (1 и 1,00): это ошибка разметки, а не дублирование точек.
func TestZcycRowsDuplicateTenor(t *testing.T) {
	const page = `<div class="table-wrapper"><table class="data">` +
		`<tr><th rowspan="2">Дата</th><th colspan="2">Срок до погашения, лет</th></tr>` +
		`<tr><th>1</th><th>1,00</th></tr>` +
		`<tr><td>16.10.2026</td><td>12,30</td><td>12,40</td></tr>` +
		`</table></div>`
	_, err := zcycRows([]byte(page))
	if err == nil {
		t.Fatal("ожидалась ошибка: тенор 1 встречается в заголовке дважды")
	}
	for _, want := range []string{`"1"`, `"1,00"`, "дважды"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("текст ошибки не содержит %q (нужны оба заголовка и причина): %v", want, err)
		}
	}
}

// TestZcycSeriesMetaCoversParsedRows — двусторонний контракт каталога и разбора:
// все 12 теноров фикстуры описаны в cbrZcycSeriesMeta, и каждый описанный ряд
// встречается в разборе.
func TestZcycSeriesMetaCoversParsedRows(t *testing.T) {
	checkCbrSeriesMeta(t, cbrSource, cbrZcycSeriesMeta, func(rec *appendRecorder) error {
		points, err := zcycRows(readCbrTestdata(t, "zcyc_2003.html"))
		if err != nil {
			return err
		}
		for _, p := range points {
			if err := rec.Append(p.tenor, p.date, p.value); err != nil {
				return err
			}
		}
		return nil
	})
}

// TestZcycChunks — окна запроса: 6 смежных непересекающихся окон по 4 года,
// первое начинается с from, последнее кончается to; вырожденные диапазоны.
func TestZcycChunks(t *testing.T) {
	from := time.Date(2003, time.January, 4, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, time.October, 19, 0, 0, 0, 0, time.UTC)

	chunks := zcycChunks(from, to)
	if len(chunks) != 6 {
		t.Fatalf("окон %d, want 6", len(chunks))
	}
	want := [][2]string{
		{"2003-01-04", "2007-01-03"},
		{"2007-01-04", "2011-01-03"},
		{"2011-01-04", "2015-01-03"},
		{"2015-01-04", "2019-01-03"},
		{"2019-01-04", "2023-01-03"},
		{"2023-01-04", "2026-10-19"},
	}
	if !chunks[0][0].Equal(from) {
		t.Errorf("первое окно начинается %s, want from = %s", chunks[0][0].Format("2006-01-02"), from.Format("2006-01-02"))
	}
	if !chunks[len(chunks)-1][1].Equal(to) {
		t.Errorf("последнее окно кончается %s, want to = %s", chunks[len(chunks)-1][1].Format("2006-01-02"), to.Format("2006-01-02"))
	}
	for i, chunk := range chunks {
		start, end := chunk[0], chunk[1]
		if start.After(end) {
			t.Errorf("окно %d: начало %s позже конца %s", i, start.Format("2006-01-02"), end.Format("2006-01-02"))
		}
		if !end.Before(start.AddDate(cbrZcycChunkYears, 0, 0)) {
			t.Errorf("окно %d (%s..%s) длиннее %d лет", i, start.Format("2006-01-02"), end.Format("2006-01-02"), cbrZcycChunkYears)
		}
		if i > 0 {
			prevEnd := chunks[i-1][1]
			if !start.Equal(prevEnd.AddDate(0, 0, 1)) {
				t.Errorf("окно %d начинается %s, а предыдущее кончается %s: окна должны быть смежными без пересечений", i, start.Format("2006-01-02"), prevEnd.Format("2006-01-02"))
			}
		}
		if got := start.Format("2006-01-02") + ".." + end.Format("2006-01-02"); got != want[i][0]+".."+want[i][1] {
			t.Errorf("окно %d = %s, want %s..%s", i, got, want[i][0], want[i][1])
		}
	}

	if got := zcycChunks(to, from); len(got) != 0 {
		t.Errorf("from > to: окон %d, want 0", len(got))
	}

	day := time.Date(2026, time.October, 19, 0, 0, 0, 0, time.UTC)
	single := zcycChunks(day, day)
	if len(single) != 1 {
		t.Fatalf("одиночный день: окон %d, want 1", len(single))
	}
	if !single[0][0].Equal(day) || !single[0][1].Equal(day) {
		t.Errorf("одиночный день: окно %s..%s, want %s..%s",
			single[0][0].Format("2006-01-02"), single[0][1].Format("2006-01-02"),
			day.Format("2006-01-02"), day.Format("2006-01-02"))
	}
}

// TestZcycPointsInRange — фильтр окна: ЦБ игнорирует диапазон, когда он пуст или
// лежит в будущем, и отдаёт таблицу с последней доступной строкой. Точки вне
// [from, to] отбрасываются (на границе from — остаются), порядок сохраняется;
// пустой вход и перевёрнутый диапазон дают пустой результат.
func TestZcycPointsInRange(t *testing.T) {
	date := func(day int) time.Time { return time.Date(2026, time.October, day, 0, 0, 0, 0, time.UTC) }
	from, to := date(17), date(19)
	points := []zcycPoint{
		{tenor: "10", date: date(15), value: 1}, // до окна
		{tenor: "10", date: from, value: 2},     // граница from — включается
		{tenor: "10", date: date(18), value: 3}, // внутри
		{tenor: "10", date: date(20), value: 4}, // после to
	}

	got := zcycPointsInRange(points, from, to)
	if len(got) != 2 {
		t.Fatalf("точек в окне %d, want 2 (граница from и внутри)", len(got))
	}
	if !got[0].date.Equal(from) || got[0].value != 2 {
		t.Errorf("первая точка = (%s, %v), want (%s, 2)",
			got[0].date.Format("2006-01-02"), got[0].value, from.Format("2006-01-02"))
	}
	if !got[1].date.Equal(date(18)) || got[1].value != 3 {
		t.Errorf("вторая точка = (%s, %v), want (%s, 3)",
			got[1].date.Format("2006-01-02"), got[1].value, date(18).Format("2006-01-02"))
	}

	if got := zcycPointsInRange(nil, from, to); len(got) != 0 {
		t.Errorf("пустой вход: точек %d, want 0", len(got))
	}
	if got := zcycPointsInRange(points, to, from); len(got) != 0 {
		t.Errorf("from > to: точек %d, want 0", len(got))
	}
}

// TestZcycStart — last → from, firstRun: clickhouse-go отдаёт Date с таймзоной
// сессии, а окна и точки — UTC. Без нормализации last в UTC-дату (regression:
// FixedZone +05) from уезжает на предыдущие сутки и последний день окна
// выпадает из фильтра zcycPointsInRange.
func TestZcycStart(t *testing.T) {
	plusFive := time.FixedZone("UTC+5", 5*60*60)
	from, firstRun := zcycStart(time.Date(2026, time.October, 16, 0, 0, 0, 0, plusFive))
	if firstRun {
		t.Error("last = 2026-10-16 +05: firstRun = true, want false")
	}
	want := time.Date(2026, time.October, 17, 0, 0, 0, 0, time.UTC)
	if !from.Equal(want) {
		t.Errorf("from = %s, want %s (следующий календарный день в UTC)",
			from.Format(time.RFC3339), want.Format(time.RFC3339))
	}

	from, firstRun = zcycStart(time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC))
	if !firstRun {
		t.Error("epoch: firstRun = false, want true")
	}
	if want := time.Date(2003, time.January, 4, 0, 0, 0, 0, time.UTC); !from.Equal(want) {
		t.Errorf("epoch: from = %s, want %s", from.Format(time.RFC3339), want.Format(time.RFC3339))
	}
}
