package calendar

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/PuerkitoBio/goquery"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

const (
	eventsTable = "events_calendar"
	eventsDdl   = `CREATE TABLE IF NOT EXISTS ` + eventsTable + ` (
	    event_date Date,
	    event_time Nullable(String),
	    category LowCardinality(String), -- 'fomc','cpi','nfp','eia','wgc','cbr','polyus_ir','moex_rebalance','gov_rf','bank_results'
	    title String,
	    threshold String,                -- JSON, напр. '{"crack":">50"}'
	    status LowCardinality(String) DEFAULT 'pending'  -- pending|done|verified
	) ENGINE = ReplacingMergeTree ORDER BY (event_date, category, title);
	`

	// calMPUrl — страница календаря заседаний Совета директоров ЦБ по ключевой ставке.
	calMPUrl = "https://www.cbr.ru/dkp/cal_mp/"
	// calMPInsert — колонки перечислены явно: так строка батча не зависит от порядка
	// и DEFAULT-ов DDL.
	calMPInsert = "INSERT INTO " + eventsTable + " (event_date, event_time, category, title, threshold, status)"
	// Канонические значения строк-заседаний: время публикации пресс-релиза — 13:30 МСК
	// (в карточках календаря указано дословно), порог не задан.
	calMPCategory  = "cbr"
	calMPTitle     = "СД ЦБ РФ: заседание по ключевой ставке"
	calMPEventTime = "13:30"
	calMPThreshold = "{}"
	calMPStatus    = "pending"
)

// eventsView — витрина календаря событий-триггеров для MCP-агента: и триггеры золото/NAV,
// и даты заседаний СД ЦБ по ключевой ставке. Сид триггеров живёт в sql/events_calendar_q4_2026.sql
// (идемпотентен за счёт ReplacingMergeTree), канонические строки заседаний ЦБ (category='cbr',
// из cbr.ru/dkp/cal_mp/) импортёр заводит сам — именно их читает агент контура ключевой ставки.
var eventsView = util.View{
	Name:    "v_events_calendar",
	Tables:  []string{eventsTable},
	Select:  `SELECT * FROM ` + eventsTable + ` FINAL`,
	Comment: "Календарь событий-триггеров прогноза золота/NAV, дат заседаний СД ЦБ по ключевой ставке (category='cbr', канонические строки — из cbr.ru/dkp/cal_mp/, их читает агент контура ставки; рядом ручные строки-триггеры с другим title) и дат раскрытия банковских результатов (category='bank_results', подтверждённые/ожидаемые даты публикаций Сбера и ВТБ); status: pending|done|verified",
	Columns: map[string]string{
		"event_date": "Дата события (Date)",
		"event_time": "Время события (местное/UTC, строка; NULL — время не фиксировано)",
		"category":   "Категория триггера: fomc/cpi/nfp/eia/wgc/cbr/polyus_ir/moex_rebalance/gov_rf; bank_results — даты раскрытия банковских результатов (Сбер, ВТБ)",
		"title":      "Название события и действие модели при его наступлении",
		"threshold":  "Пороги срабатывания в формате JSON, напр. '{\"crack\":\">50\"}'",
		"status":     "Статус отработки: pending — ожидается, done — обработано, verified — сверено с фактом",
	},
}

type eventsCalendar struct {
}

func (s *eventsCalendar) Name() string {
	return eventsTable
}

func (s *eventsCalendar) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, eventsDdl); err != nil {
		return count, err
	}
	created, err := util.CreateView(ctx, conn, eventsView)
	if err != nil {
		return count, err
	}
	log.Infof("View %s created: %t", eventsView.Name, created)

	page, err := fetchCalMPPage()
	if err != nil {
		// Деградация: таблица и витрина уже созданы, а календарь — надстройка над ручным
		// сидом (sql/events_calendar_q4_2026.sql); недоступность ЦБ не должна ронять прогон.
		log.Warnf("Календарь заседаний ЦБ (%s) недоступен, события не обновлены: %v", calMPUrl, err)
		return count, nil
	}
	dates, err := parseCalMP(page)
	if err != nil {
		// Ошибка разбора (в т.ч. 0 заседаний) — ошибка прогона: молчаливый «0 rows»
		// на смене разметки выглядел бы зелёным прогоном с потерянным календарём.
		return count, err
	}

	batch, err := conn.PrepareBatch(ctx, calMPInsert)
	if err != nil {
		return count, err
	}
	for _, date := range dates {
		if err = batch.Append(date, calMPEventTime, calMPCategory, calMPTitle, calMPThreshold, calMPStatus); err != nil {
			_ = batch.Abort()
			return count, err
		}
		count++
	}
	if err = batch.Send(); err != nil {
		return count, err
	}
	log.Infof("Cal_mp: %d meeting dates inserted into %s", count, eventsTable)
	return count, nil
}

// fetchCalMPPage скачивает страницу календаря заседаний. Читатель закрывается всегда,
// ошибки получения и чтения возвращаются обе.
func fetchCalMPPage() ([]byte, error) {
	reader, err := util.GetFile(calMPUrl)
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()
	page, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return page, nil
}

// calMPDateRe — полная дата карточки: «<D> <месяц> <YYYY> года» после нормализации текста.
var calMPDateRe = regexp.MustCompile(`^(\d{1,2})\s+([а-яё]+)\s+(\d{4})\s+года$`)

// calMPPanelDateRe — дата без года («14 февраля»): карточки 2019 и таблицы 2014–2018,
// год берётся из лейбла вкладки панели.
var calMPPanelDateRe = regexp.MustCompile(`^(\d{1,2})\s+([а-яё]+)$`)

// calMPTabYearRe — год в лейбле вкладки: data-text="2014 год".
var calMPTabYearRe = regexp.MustCompile(`^(\d{4})\s+год$`)

// calMPMonths — русские месяцы в родительном падеже. Локальная копия: пакеты независимы,
// calendar/ не тянет cbr/.
var calMPMonths = map[string]time.Month{
	"января":   time.January,
	"февраля":  time.February,
	"марта":    time.March,
	"апреля":   time.April,
	"мая":      time.May,
	"июня":     time.June,
	"июля":     time.July,
	"августа":  time.August,
	"сентября": time.September,
	"октября":  time.October,
	"ноября":   time.November,
	"декабря":  time.December,
}

// calMPMakeDate собирает дату из частей. time.Date нормализует несуществующие даты
// («32 января» → 1 февраля), поэтому день и месяц сверяются обратной сборкой.
func calMPMakeDate(day int, monthName string, year int) (time.Time, bool) {
	month, ok := calMPMonths[monthName]
	if !ok {
		return time.Time{}, false
	}
	date := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	if date.Day() != day || date.Month() != month {
		return time.Time{}, false
	}
	return date, true
}

// parseCalMPDate разбирает полную дату карточки («13 февраля 2026 года»).
func parseCalMPDate(raw string) (time.Time, bool) {
	m := calMPDateRe.FindStringSubmatch(strings.ToLower(calMPText(raw)))
	if m == nil {
		return time.Time{}, false
	}
	day, err := strconv.Atoi(m[1])
	if err != nil {
		return time.Time{}, false
	}
	year, err := strconv.Atoi(m[3])
	if err != nil {
		return time.Time{}, false
	}
	return calMPMakeDate(day, m[2], year)
}

// parseCalMPPanelDate разбирает дату без года («14 февраля») с годом из лейбла вкладки —
// карточки 2019 и таблицы 2014–2018.
func parseCalMPPanelDate(raw string, year int) (time.Time, bool) {
	m := calMPPanelDateRe.FindStringSubmatch(strings.ToLower(calMPText(raw)))
	if m == nil {
		return time.Time{}, false
	}
	day, err := strconv.Atoi(m[1])
	if err != nil {
		return time.Time{}, false
	}
	return calMPMakeDate(day, m[2], year)
}

// calMPIsMeeting — карточка-заседание (2019 и 2020+): заголовок «Заседание Совета директоров …»
// и тема («… по ключевой ставке» у 2021+, «… по денежно-кредитной политике» у 2019–2020).
// Предикат карточек не менялся: формулировки таблиц 2014–2018 иные, для них — calMPIsMeetingTable.
// Проверка регистронезависимая: у внеочередных заседаний заголовок начинается со строчной
// «з» («Внеочередное заседание Совета директоров Банка России по ключевой ставке»), и
// регистрозависимый фильтр теряет 2022-02-28, 2022-04-08, 2022-05-26 и 2023-08-15.
func calMPIsMeeting(text string) bool {
	low := strings.ToLower(calMPText(text))
	return strings.Contains(low, "заседание совета директоров") &&
		(strings.Contains(low, "ключевой ставке") || strings.Contains(low, "денежно-кредитной политике"))
}

// calMPIsMeetingTable — заседание в таблице старой эры (2014–2018). Тема берётся основой без
// падежного окончания: у таблиц 2014–2017 — «… по вопросам денежно-кредитной политики»
// (родительный падеж), у таблицы 2018 — «… по денежно-кредитной политике». Пара «заседание
// совета директоров» ∧ тема отсекает строки «Доклад о денежно-кредитной политике» и
// пресс-конференции.
func calMPIsMeetingTable(text string) bool {
	low := strings.ToLower(calMPText(text))
	return strings.Contains(low, "заседание совета директоров") &&
		(strings.Contains(low, "ключевой ставке") || strings.Contains(low, "денежно-кредитной политик"))
}

// calMPText нормализует текст карточки: неразрывные пробелы (`&nbsp;`, в т.ч. внутри даты
// внеочередного заседания: «15&nbsp;<span>августа</span> 2023 года») — в обычные,
// пробелы схлопнуты.
func calMPText(s string) string {
	return strings.Join(strings.Fields(strings.ReplaceAll(s, "\u00a0", " ")), " ")
}

// calMPTabYears — карта «ключ вкладки → год» по лейблам a.tab: href="#<key>" и
// data-text="YYYY год". Вкладки без года в карту не попадают — их панели пропускаются.
func calMPTabYears(doc *goquery.Document) map[string]int {
	years := make(map[string]int)
	doc.Find("a.tab").Each(func(_ int, tab *goquery.Selection) {
		key, ok := strings.CutPrefix(tab.AttrOr("href", ""), "#")
		if !ok || key == "" {
			return
		}
		m := calMPTabYearRe.FindStringSubmatch(calMPText(tab.AttrOr("data-text", "")))
		if m == nil {
			return
		}
		year, err := strconv.Atoi(m[1])
		if err != nil {
			return
		}
		years[key] = year
	})
	return years
}

// calMPCardYear — год карточки без года в дате: из вкладки панели, в которой карточка лежит
// ([data-tabs-content=<key>]). Карточка вне панели и панель без вкладки года не дают.
func calMPCardYear(day *goquery.Selection, years map[string]int) (int, bool) {
	panel := day.Closest("[data-tabs-content]")
	if panel.Length() == 0 {
		return 0, false
	}
	year, ok := years[panel.AttrOr("data-tabs-content", "")]
	return year, ok
}

// calMPTableDates разбирает таблицу старой эры (2014–2018): дата — из первой ячейки строки
// («14 февраля», без года), событие — из второй; пустая или отсутствующая ячейка даты
// (эффект rowspan) означает, что действует предыдущая дата, а непустая нераспознанная —
// что строка своей даты не получила (дата сбрасывается). year — год вкладки панели.
func calMPTableDates(table *goquery.Selection, year int, add func(time.Time)) {
	var current time.Time
	table.Find("tr").Each(func(_ int, tr *goquery.Selection) {
		cells := tr.Find("td")
		var dateCell, eventCell *goquery.Selection
		switch cells.Length() {
		case 0:
			return
		case 1:
			// Строка-продолжение rowspan: ячейки даты нет, дата — предыдущая.
			eventCell = cells.Eq(0)
		default:
			dateCell = cells.Eq(0)
			eventCell = cells.Eq(1)
		}
		if dateCell != nil {
			raw := dateCell.Text()
			if date, ok := parseCalMPPanelDate(raw, year); ok {
				current = date
			} else if calMPText(raw) != "" {
				// Непустая, но нераспознанная ячейка даты (полная дата, опечатка, другой
				// формат) — не продолжение rowspan: сброс, чтобы событие строки не получило
				// предыдущую дату молча.
				current = time.Time{}
			}
		}
		if current.IsZero() || !calMPIsMeetingTable(eventCell.Text()) {
			return
		}
		add(current)
	})
}

// parseCalMP разбирает страницу календаря заседаний ЦБ по трём разметкам: карточки с полной
// датой (2020+), карточки без года (2019) и таблицы старой эры (2014–2018); у двух последних
// год берётся из лейбла вкладки панели. Возвращает дедуплицированные отсортированные даты;
// 0 заседаний — ошибка (признак смены разметки, а не пустого календаря).
func parseCalMP(page []byte) ([]time.Time, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(page))
	if err != nil {
		return nil, fmt.Errorf("cal_mp: разбор HTML: %w", err)
	}
	seen := make(map[string]bool)
	var dates []time.Time
	add := func(date time.Time) {
		key := date.Format("2006-01-02")
		if seen[key] {
			return
		}
		seen[key] = true
		dates = append(dates, date)
	}
	years := calMPTabYears(doc)

	// Карточки-заседания: полная дата — как и раньше, в любой части страницы; без года
	// (2019) — год из вкладки панели, карточки вне панели с вкладкой года не получают.
	doc.Find("div.main-events_day").Each(func(_ int, day *goquery.Selection) {
		if !calMPIsMeeting(day.Text()) {
			return
		}
		raw := day.Find("div.date").First().Text()
		if date, ok := parseCalMPDate(raw); ok {
			add(date)
			return
		}
		year, ok := calMPCardYear(day, years)
		if !ok {
			return
		}
		if date, ok := parseCalMPPanelDate(raw, year); ok {
			add(date)
		}
	})

	// Таблицы старой эры (2014–2018): год — из вкладки панели, панель без вкладки
	// (или вкладка без года) пропускается — как и в остальном разборе.
	doc.Find("[data-tabs-content]").Each(func(_ int, panel *goquery.Selection) {
		year, ok := years[panel.AttrOr("data-tabs-content", "")]
		if !ok {
			return
		}
		panel.Find("table.data").Each(func(_ int, table *goquery.Selection) {
			calMPTableDates(table, year, add)
		})
	})

	if len(dates) == 0 {
		return nil, fmt.Errorf("cal_mp: ни одного заседания не найдено (%s)", calMPUrl)
	}
	sort.Slice(dates, func(i, j int) bool { return dates[i].Before(dates[j]) })
	return dates, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &eventsCalendar{})
}
