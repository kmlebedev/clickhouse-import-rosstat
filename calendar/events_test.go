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
// по 8/год в 2020–2027 + 4 внеочередных) и task-1 (50 дат старой эры: таблицы
// 2014–2018 + карточки без года 2019 — год из лейбла вкладки). Состав старой
// эры — по замороженной фикстуре: внеочередное заседание 2014 года помечено
// 15 декабря, а не 16-м (по нему пресс-релиз вышел 16.12.2014 00:39).
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
		// 2014: таблица вкладки t8, дата без года + год из лейбла
		"2014-02-14",
		"2014-03-03", // внеочередное
		"2014-03-14",
		"2014-04-25",
		"2014-06-16",
		"2014-07-25",
		"2014-09-12",
		"2014-10-31",
		"2014-12-11",
		"2014-12-15", // внеочередное (в спеке §2.3 — 12-16, истина — фикстура)
		// 2015
		"2015-01-30",
		"2015-03-13",
		"2015-04-30",
		"2015-06-15",
		"2015-07-31",
		"2015-09-11",
		"2015-10-30",
		"2015-12-11",
		// 2016
		"2016-01-29",
		"2016-03-18",
		"2016-04-29",
		"2016-06-10",
		"2016-07-29",
		"2016-09-16",
		"2016-10-28",
		"2016-12-16",
		// 2017
		"2017-02-03",
		"2017-03-24",
		"2017-04-28",
		"2017-06-16",
		"2017-07-28",
		"2017-09-15",
		"2017-10-27",
		"2017-12-15",
		// 2018: формулировка темы без «вопросам» — «… по денежно-кредитной политике»
		"2018-02-09",
		"2018-03-23",
		"2018-04-27",
		"2018-06-15",
		"2018-07-27",
		"2018-09-14",
		"2018-10-26",
		"2018-12-14",
		// 2019: карточки без года + год из лейбла
		"2019-02-08",
		"2019-03-22",
		"2019-04-26",
		"2019-06-14",
		"2019-07-26",
		"2019-09-06",
		"2019-10-25",
		"2019-12-13",
	}
	for _, w := range want {
		if !got[w] {
			t.Errorf("в результате нет %s: заседание потеряно", w)
		}
	}

	// Полный состав замороженной фикстуры — 118 дат: 68 карточек 2020–2027,
	// 42 строки таблиц 2014–2018 и 8 карточек 2019. Вместе с пинами выше счётчик
	// закрепляет состав целиком: ни потерянных дат, ни лишних.
	if len(dates) != 118 {
		t.Errorf("parseCalMP(фикстура) = %d дат, ожидалось 118 (фикстура заморожена)", len(dates))
	}

	// Незаседания не должны протекать: «Доклад о ДКП» (включая строки таблиц
	// 2014–2018), резюме обсуждения (в т.ч. 2026-11-05 — карточка со временем
	// пресс-конференции) и пресс-конференции.
	notMeetings := []string{
		"2014-02-18", // Доклад о денежно-кредитной политике (таблица 2014)
		"2014-06-18", // Доклад о денежно-кредитной политике (таблица 2014)
		"2014-09-16", // Доклад о денежно-кредитной политике (таблица 2014)
		"2019-04-01", // Публикация Доклада о денежно-кредитной политике (карточка 2019)
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

	// Нижняя граница — 2014-02-14, самая ранняя дата страницы. Год вкладки
	// подставляется, но раньше первого заседания 2014 года ничего не появляется.
	for d := range got {
		if strings.Compare(d, "2014-02-14") < 0 {
			t.Errorf("дата %s раньше 2014-02-14: год из вкладки подставлен неверно", d)
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

// TestParseCalMPSkipsYearlessCards — карточка-заседание без года в дате вне панели
// с вкладкой не попадает в результат (год взять неоткуда), но не роняет разбор,
// если рядом есть дата с годом. Карточки 2019 в фикстуре лежат внутри панели t3
// с вкладкой «2019 год» — там год подставляется (см. TestParseCalMPPanelYears).
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

// TestParseCalMPPanelYears — год карточек без даты берётся из вкладки панели:
// панель с вкладкой «2019 год» даёт год своим карточкам, содержимое панели без
// вкладки пропускается, а пересечение годов (год выложен дважды) схлопывается дедупом.
func TestParseCalMPPanelYears(t *testing.T) {
	page := `<html><body>
	<div class="tabs"><a class="tab" href="#t1" data-text="2019 год">2019</a>
	<a class="tab" href="#t3" data-text="2019 год">2019</a></div>
	<div data-tabs-content="t1">
	<div class="main-events_day">
	<div class="date col-md-5">8&nbsp;февраля</div>
	<div class="main-events"><div class="main-event">
	<div class="title"><span>Заседание Совета директоров по&nbsp;денежно-кредитной политике</span></div>
	</div></div></div>
	</div>
	<div data-tabs-content="t2">
	<div class="main-events_day">
	<div class="date col-md-5">7&nbsp;февраля</div>
	<div class="main-events"><div class="main-event">
	<div class="title"><span>Заседание Совета директоров по&nbsp;денежно-кредитной политике</span></div>
	</div></div></div>
	<table class="data"><tbody>
	<tr><td>1&nbsp;февраля</td><td>Заседание Совета директоров по&nbsp;денежно-кредитной политике</td></tr>
	</tbody></table>
	</div>
	<div data-tabs-content="t3">
	<div class="main-events_day">
	<div class="date col-md-5">8&nbsp;февраля</div>
	<div class="main-events"><div class="main-event">
	<div class="title"><span>Заседание Совета директоров по&nbsp;денежно-кредитной политике</span></div>
	</div></div></div>
	</div>
	</body></html>`
	dates, err := parseCalMP([]byte(page))
	if err != nil {
		t.Fatalf("parseCalMP: %v", err)
	}
	if len(dates) != 1 || dates[0].Format("2006-01-02") != "2019-02-08" {
		t.Errorf("parseCalMP = %v, ожидалась одна дата 2019-02-08 (год из вкладки t1, панель t2 без вкладки пропущена, t3 — дедуп)", dates)
	}
}

// TestParseCalMPOldTable — таблица старой эры (2014–2018): дата из первой ячейки
// строки без года, строки-продолжения rowspan и пустая ячейка даты действуют
// предыдущей датой, событие — из второй ячейки; доклады и пресс-конференции
// заседаниями не считаются.
func TestParseCalMPOldTable(t *testing.T) {
	page := `<html><body>
	<div class="tabs"><a class="tab" href="#t8" data-text="2014 год">2014</a></div>
	<div data-tabs-content="t8"><div class="table-wrapper"><table class="data"><tbody>
	<tr><td>14&nbsp;февраля</td><td>Заседание Совета директоров Банка России по&nbsp;вопросам денежно-кредитной политики</td></tr>
	<tr><td></td><td>&nbsp;</td></tr>
	<tr><td rowspan="2">3&nbsp;марта</td><td>Внеочередное заседание Совета директоров Банка России по&nbsp;вопросам денежно-кредитной политики</td></tr>
	<tr><td><a href="/press/event/?id=5200">Пресс-конференция</a></td></tr>
	<tr><td>18&nbsp;февраля</td><td>Доклад о&nbsp;денежно-кредитной политике</td></tr>
	</tbody></table></div></div>
	</body></html>`
	dates, err := parseCalMP([]byte(page))
	if err != nil {
		t.Fatalf("parseCalMP: %v", err)
	}
	want := []string{"2014-02-14", "2014-03-03"}
	if len(dates) != len(want) {
		t.Fatalf("parseCalMP = %v, ожидались даты %v", dates, want)
	}
	for i, w := range want {
		if got := dates[i].Format("2006-01-02"); got != w {
			t.Errorf("дата %d = %s, ожидалась %s", i, got, w)
		}
	}
}
