package calendar

import (
	"os"
	"strings"
	"testing"
	"time"
)

// calMPFixture читает HTML-фикстуру календаря ЦБ calendar/testdata/cal_mp.html
// (загружена живьём 2026-10-14, разбор разметки — task-2-report.md §4/§5.8).
func calMPFixture(t *testing.T) []byte {
	t.Helper()
	page, err := os.ReadFile("testdata/cal_mp.html")
	if err != nil {
		t.Fatalf("чтение testdata/cal_mp.html: %v", err)
	}
	return page
}

// calMPDates — множество дат результата в ISO-виде: проверки «входит / не входит»
// не зависят от общего числа заседаний (календарь растёт, пин количества запрещён).
func calMPDates(dates []time.Time) map[string]bool {
	out := make(map[string]bool, len(dates))
	for _, d := range dates {
		out[d.Format("2006-01-02")] = true
	}
	return out
}

// TestParseCalMPFixture — контракт разбора календаря заседаний по фикстуре.
// Пины дат — из task-2-report.md §4.1 (68 карточек с полной датой: 64 обычных
// по 8/год в 2020–2027 + 4 внеочередных).
func TestParseCalMPFixture(t *testing.T) {
	dates, err := parseCalMP(calMPFixture(t))
	if err != nil {
		t.Fatalf("parseCalMP(фикстура): %v", err)
	}
	if len(dates) == 0 {
		t.Fatalf("parseCalMP(фикстура) вернул 0 дат")
	}

	// Результат отсортирован и уникален — контракт интерфейса.
	for i := 1; i < len(dates); i++ {
		if !dates[i].After(dates[i-1]) {
			t.Errorf("даты %d и %d не отсортированы/дублируются: %s, %s",
				i-1, i, dates[i-1].Format("2006-01-02"), dates[i].Format("2006-01-02"))
		}
	}

	got := calMPDates(dates)
	want := []string{
		// обычные заседания: пин из Task 2 + весь будущий год 2027 (8 заседаний)
		"2024-02-16",
		"2026-10-23",
		"2026-12-18",
		"2027-02-12", "2027-03-19", "2027-04-30", "2027-06-11",
		"2027-07-30", "2027-09-10", "2027-10-22", "2027-12-17",
		// внеочередные заседания: в заголовке строчная «з» («Внеочередное заседание …»),
		// а месяц в дате обёрнут в <span> — «15&nbsp;<span>августа</span> 2023 года».
		"2022-02-28", "2022-04-08", "2022-05-26", "2023-08-15",
	}
	for _, w := range want {
		if !got[w] {
			t.Errorf("в результате нет %s: карточка-заседание потеряна", w)
		}
	}

	// Карточки-незаседания не должны протекать: «Доклад о ДКП», резюме обсуждения
	// (в т.ч. 2026-11-05 — карточка со временем пресс-конференции) и заседания из
	// табличных секций 2014–2018 без года в дате.
	notMeetings := []string{
		"2023-02-20", // Доклад о денежно-кредитной политике
		"2024-02-27", // Резюме обсуждения ключевой ставки
		"2026-11-05", // Резюме обсуждения ключевой ставки + прогноз пресс-конференции
		"2026-12-30", // Резюме обсуждения ключевой ставки
		"2027-02-25", // Резюме обсуждения ключевой ставки
		"2027-12-29", // Резюме обсуждения ключевой ставки
	}
	for _, bad := range notMeetings {
		if got[bad] {
			t.Errorf("в результате незаседание: карточка %s протекла в события", bad)
		}
	}

	// Даты без года (карточки 2019 и таблицы 2014–2018) игнорируются: год не
	// подставляется ни из лейбла таба, ни из текущего года.
	for d := range got {
		if strings.Compare(d, "2020-01-01") < 0 {
			t.Errorf("дата %s раньше 2020: год дописан из таба/текущей даты", d)
		}
	}
}

// TestParseCalMPFailFast — html без карточек-заседаний (и пустая страница) — ошибка,
// иначе dagu покажет зелёный прогон при потерянном календаре.
func TestParseCalMPFailFast(t *testing.T) {
	cases := []struct {
		name string
		page string
	}{
		{"пустая страница", ""},
		{"html без событий", `<html><body><div class="main-events_day">
			<div class="date col-md-5">5&nbsp;ноября 2026 года</div>
			<div class="main-events"><div class="main-event">
			<div class="info col-md-15">Резюме обсуждения ключевой ставки</div>
			</div></div></div></body></html>`},
		{"только доклад о ДКП", `<html><body><div class="main-events_day">
			<div class="date col-md-5">20&nbsp;февраля 2023 года</div>
			<div class="main-events"><div class="main-event">
			<div class="title"><span>Доклад о&nbsp;денежно-кредитной политике</span></div>
			</div></div></div></body></html>`},
		{"заседание без года в дате", `<html><body><div class="main-events_day">
			<div class="date col-md-5">22 марта</div>
			<div class="main-events"><div class="main-event">
			<div class="title"><span>Заседание Совета директоров по&nbsp;денежно-кредитной политике</span></div>
			</div></div></div></body></html>`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dates, err := parseCalMP([]byte(c.page))
			if err == nil {
				t.Errorf("parseCalMP вернул %v без ошибки, ожидалась ошибка (0 заседаний)", dates)
			}
		})
	}
}

// TestParseCalMPSkipsYearlessCards — карточка-заседание без года в дате не попадает
// в результат, но не роняет разбор, если рядом есть дата с годом (2019 в фикстуре
// лежит в той же разметке, что и 2020+).
func TestParseCalMPSkipsYearlessCards(t *testing.T) {
	page := `<html><body>
	<div class="main-events_day">
	<div class="date col-md-5">8&nbsp;февраля</div>
	<div class="main-events"><div class="main-event">
	<div class="title"><span>Заседание Совета директоров по&nbsp;денежно-кредитной политике</span></div>
	</div></div></div>
	<div class="main-events_day">
	<div class="date col-md-5">7&nbsp;февраля 2020 года</div>
	<div class="main-events"><div class="main-event">
	<div class="title"><span>Заседание Совета директоров по&nbsp;денежно-кредитной политике</span></div>
	</div></div></div>
	</body></html>`
	dates, err := parseCalMP([]byte(page))
	if err != nil {
		t.Fatalf("parseCalMP: %v", err)
	}
	if len(dates) != 1 || dates[0].Format("2006-01-02") != "2020-02-07" {
		t.Errorf("parseCalMP = %v, ожидалась одна дата 2020-02-07 (карточка без года игнорируется)", dates)
	}
}
