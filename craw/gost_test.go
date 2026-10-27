package craw

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	log "github.com/sirupsen/logrus"
)

// Тесты импортёра СБКТС офлайн: соединение — фейковое (gostFakeConn), источник — httptest
// (общая обвязка gostNewTestAPI из gost_api_test.go). Ни сети, ни ClickHouse, ни токена:
// appid подставной. Пинятся контракты обвязки: разбор окружения (до сети), форма схемы,
// включительность дневного окна, якорь since (включая пустую таблицу), дневной батч,
// агрегация ошибок дней и различие «дыра» / «текстовый шум» в check.

// gostIssueMS — миллисекунды даты выдачи документов тестов: 2026-10-08 21:00 UTC = 09.10.2026 МСК.
// Дата выдачи в срез дня не входит (день среза даёт дата публикации), важен только непустой
// указатель: у документа без даты выдачи строки нет вовсе.
const gostIssueMS int64 = 1791493200000

// gostForeignPublication — дата публикации «чужого» документа: ручка ищет текстом и отдаёт
// документы соседних окон, а клиентский фильтр дня их отбрасывает. Форма — чистая дата без
// времени и зоны: именно так выглядел документ, уронивший живой июньский прогон 2026 года
// (одна такая дата среди 35 обычных делала день «невалидным»), поэтому тесты обвязки гоняют
// эту форму через весь путь от разбора до решения по дню.
const gostForeignPublication = "01.01.2020"

// gostTestNow — подменённые часы прогона: 12.10.2026 12:00 UTC, то есть «сегодня МСК» = 12.10.2026,
// «вчера МСК» = 11.10.2026 (МСК = UTC+3, дата прогона 12.10.2026). Обе границы тестируются
// явно: окно since кончается сегодняшним днём, дефолт full/check — вчерашним.
var gostTestNow = time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)

// gostEnv выставляет окружение конфига: сначала все переменные прогона пустые (тест не должен
// зависеть от окружения машины), затем — переданные значения.
func gostEnv(t *testing.T, values map[string]string) {
	t.Helper()
	for _, name := range []string{gostModeEnv, gostFromEnv, gostToEnv, gostSettleEnv, gostAppIDEnv} {
		t.Setenv(name, "")
	}
	for name, value := range values {
		t.Setenv(name, value)
	}
}

// gostPinClock фиксирует «сейчас» прогона: от него зависят окно since и дефолт GOST_TO
// (вчера МСК), а с реальными часами тест зависел бы от дня запуска.
func gostPinClock(t *testing.T, now time.Time) {
	t.Helper()
	prev := gostClock
	gostClock = func() time.Time { return now }
	t.Cleanup(func() { gostClock = prev })
}

// gostCaptureLog подменяет вывод logrus буфером: строки отчёта check и предупреждения
// (в том числе «шум: N → M» и «история не загружена») — часть контракта режимов, и проверяются
// они по тексту.
func gostCaptureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	logger := log.StandardLogger()
	prevOut, prevFormatter := logger.Out, logger.Formatter
	log.SetOutput(&buf)
	log.SetFormatter(&log.TextFormatter{DisableColors: true, DisableTimestamp: true})
	t.Cleanup(func() {
		log.SetOutput(prevOut)
		log.SetFormatter(prevFormatter)
	})
	return &buf
}

// gostTime разбирает день теста в той же шкале, что и прогон: UTC-полночь.
func gostTime(t *testing.T, day string) time.Time {
	t.Helper()
	parsed, err := time.Parse(gostDateLayout, day)
	if err != nil {
		t.Fatalf("разбор дня %q: %v", day, err)
	}
	return parsed
}

func TestGostConfigDefaults(t *testing.T) {
	gostPinClock(t, gostTestNow)

	t.Run("since: режим и хвост по умолчанию", func(t *testing.T) {
		gostEnv(t, map[string]string{gostAppIDEnv: gostTestAppID})
		cfg, err := gostConfigFromEnv()
		if err != nil {
			t.Fatalf("конфиг пустого окружения: %v", err)
		}
		if cfg.Mode != gostModeSince {
			t.Errorf("Mode = %q, ожидался %q", cfg.Mode, gostModeSince)
		}
		if cfg.SettleDays != gostDefaultSettleDays {
			t.Errorf("SettleDays = %d, ожидалось %d", cfg.SettleDays, gostDefaultSettleDays)
		}
		if cfg.AppID != gostTestAppID {
			t.Errorf("AppID = %q, ожидался %q", cfg.AppID, gostTestAppID)
		}
		// В since границы не задаются: окно считает якорь таблицы. Дефолт диапазона здесь
		// выглядел бы настройкой, которая ничего не делает.
		if !cfg.From.IsZero() || !cfg.To.IsZero() {
			t.Errorf("в since границы не нужны: From=%s To=%s", cfg.From, cfg.To)
		}
	})

	t.Run("full: вся история по умолчанию", func(t *testing.T) {
		gostEnv(t, map[string]string{gostModeEnv: gostModeFull, gostAppIDEnv: gostTestAppID})
		cfg, err := gostConfigFromEnv()
		if err != nil {
			t.Fatalf("конфиг full: %v", err)
		}
		wantFrom, wantTo := gostTime(t, "2016-01-01"), gostTime(t, "2026-10-11")
		if !cfg.From.Equal(wantFrom) {
			t.Errorf("From = %s, ожидалось %s", cfg.From.Format(gostDateLayout), wantFrom.Format(gostDateLayout))
		}
		if !cfg.To.Equal(wantTo) {
			t.Errorf("To = %s, ожидалось %s (вчера МСК при часах %s)",
				cfg.To.Format(gostDateLayout), wantTo.Format(gostDateLayout), gostTestNow.Format(time.RFC3339))
		}
	})
}

func TestGostConfigRejectsUnknownMode(t *testing.T) {
	gostEnv(t, map[string]string{gostModeEnv: "fullscan", gostAppIDEnv: gostTestAppID})
	_, err := gostConfigFromEnv()
	if err == nil {
		t.Fatal("неизвестный режим принят без ошибки")
	}
	if !strings.Contains(err.Error(), "неизвестный режим (since|full|check)") {
		t.Errorf("ошибка %q не перечисляет режимы", err)
	}
}

func TestGostConfigRejectsBadDate(t *testing.T) {
	cases := []struct {
		name  string
		env   string
		value string
	}{
		{"FROM", gostFromEnv, "01.01.2016"},
		{"TO", gostToEnv, "2026/10/09"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gostEnv(t, map[string]string{
				gostModeEnv:  gostModeFull,
				gostAppIDEnv: gostTestAppID,
				c.env:        c.value,
			})
			_, err := gostConfigFromEnv()
			if err == nil {
				t.Fatalf("битая дата %s=%q принята", c.env, c.value)
			}
			if !strings.Contains(err.Error(), c.env) || !strings.Contains(err.Error(), gostDateLayout) {
				t.Errorf("ошибка %q не называет переменную %s и формат %s", err, c.env, gostDateLayout)
			}
		})
	}
}

func TestGostConfigRejectsBadSettle(t *testing.T) {
	for _, value := range []string{"-1", "три"} {
		t.Run(value, func(t *testing.T) {
			gostEnv(t, map[string]string{gostSettleEnv: value, gostAppIDEnv: gostTestAppID})
			_, err := gostConfigFromEnv()
			if err == nil {
				t.Fatalf("хвост %q принят", value)
			}
			if !strings.Contains(err.Error(), gostSettleEnv) {
				t.Errorf("ошибка %q не называет переменную %s", err, gostSettleEnv)
			}
		})
	}
}

func TestGostConfigRequiresAppID(t *testing.T) {
	// Пробелы — то же «пусто»: значение из env приходит с хвостом перевода строки.
	gostEnv(t, map[string]string{gostAppIDEnv: "   "})
	_, err := gostConfigFromEnv()
	if err == nil {
		t.Fatal("пустой GOST_APP_ID принят")
	}
	if !strings.Contains(err.Error(), gostAppIDEnv) {
		t.Errorf("ошибка %q не называет переменную %s", err, gostAppIDEnv)
	}
	if !strings.Contains(err.Error(), "standarts.adminsHeaders") {
		t.Errorf("ошибка %q не подсказывает, где взять токен", err)
	}
}

func TestGostConfigRejectsEmptyRange(t *testing.T) {
	gostEnv(t, map[string]string{
		gostModeEnv:  gostModeFull,
		gostFromEnv:  "2026-06-03",
		gostToEnv:    "2026-06-01",
		gostAppIDEnv: gostTestAppID,
	})
	_, err := gostConfigFromEnv()
	if err == nil {
		t.Fatal("перевёрнутый диапазон принят")
	}
	if !strings.Contains(err.Error(), "пустой диапазон") {
		t.Errorf("ошибка %q не сообщает о пустом диапазоне", err)
	}
}

func TestGostSchemaStatementsShape(t *testing.T) {
	columns := []string{
		"mark", "type", "date", "certificate_number", "ecoclass", "enginetrademark",
		"yearofmanufacturing", "nameofmanufacturer", "uid", "publication_date",
	}
	stmts := gostSchemaStatements()
	if len(stmts) != 1+len(columns)-4 {
		t.Fatalf("стейтментов %d, ожидалось %d (CREATE + ALTER на шесть новых колонок)", len(stmts), 1+len(columns)-4)
	}

	create := stmts[0]
	if !strings.Contains(create, "CREATE TABLE IF NOT EXISTS "+gostTable) {
		t.Errorf("стейтмент 0 не создаёт таблицу %s: %q", gostTable, create)
	}
	if !strings.Contains(create, "ENGINE = ReplacingMergeTree") {
		t.Errorf("стейтмент 0 без движка ReplacingMergeTree: %q", create)
	}
	if !strings.Contains(create, "ORDER BY (mark, type, date, certificate_number)") {
		t.Errorf("стейтмент 0 с другим ключом: %q", create)
	}
	for _, column := range columns {
		// Колонка встречается ровно один раз: определение без суффикса (вроде publication_date
		// вместо date) обязано валить тест, а не создавать таблицу с чужим ключом.
		def := regexp.MustCompile(`(?m)^\s*,?\s*` + column + `\s+[A-Za-z]`)
		if n := len(def.FindAllString(create, -1)); n != 1 {
			t.Errorf("колонка %s: определений %d, ожидалось 1 (CREATE: %q)", column, n, create)
		}
	}

	for i, column := range columns[4:] {
		stmt := stmts[i+1]
		prefix := "ALTER TABLE " + gostTable + " ADD COLUMN IF NOT EXISTS " + column + " "
		if !strings.HasPrefix(stmt, prefix) {
			t.Errorf("стейтмент %d = %q, ожидался префикс %q", i+1, stmt, prefix)
		}
		if !strings.HasSuffix(stmt, " AFTER "+columns[i+3]) {
			t.Errorf("стейтмент %d = %q: ожидалась позиция AFTER %s", i+1, stmt, columns[i+3])
		}
	}
}

func TestGostDaysBetween(t *testing.T) {
	assertDays := func(t *testing.T, days []time.Time, want ...string) {
		t.Helper()
		if len(days) != len(want) {
			t.Fatalf("дней %d, ожидалось %d (%v)", len(days), len(want), want)
		}
		for i, day := range want {
			if got := days[i].Format(gostDateLayout); got != day {
				t.Errorf("день %d = %s, ожидался %s", i, got, day)
			}
			if days[i].Location() != time.UTC || days[i].Hour() != 0 {
				t.Errorf("день %d = %s: ожидалась UTC-полночь", i, days[i])
			}
		}
	}

	t.Run("включительно", func(t *testing.T) {
		days, err := gostDaysBetween(gostTime(t, "2026-06-01"), gostTime(t, "2026-06-03"))
		if err != nil {
			t.Fatalf("диапазон трёх дней: %v", err)
		}
		assertDays(t, days, "2026-06-01", "2026-06-02", "2026-06-03")
	})

	t.Run("один день", func(t *testing.T) {
		days, err := gostDaysBetween(gostTime(t, "2026-06-01"), gostTime(t, "2026-06-01"))
		if err != nil {
			t.Fatalf("диапазон одного дня: %v", err)
		}
		assertDays(t, days, "2026-06-01")
	})

	t.Run("пустой диапазон", func(t *testing.T) {
		if _, err := gostDaysBetween(gostTime(t, "2026-06-03"), gostTime(t, "2026-06-01")); err == nil {
			t.Error("начало позже конца принято без ошибки")
		}
	})

	t.Run("предел длины", func(t *testing.T) {
		from := gostTime(t, "2016-01-01")
		days, err := gostDaysBetween(from, from.AddDate(0, 0, gostMaxRangeDays-1))
		if err != nil {
			t.Fatalf("ровно %d дней: %v", gostMaxRangeDays, err)
		}
		if len(days) != gostMaxRangeDays {
			t.Errorf("дней %d, ожидалось %d", len(days), gostMaxRangeDays)
		}
		if _, err = gostDaysBetween(from, from.AddDate(0, 0, gostMaxRangeDays)); err == nil {
			t.Errorf("диапазон из %d дней принят без ошибки", gostMaxRangeDays+1)
		}
	})
}

func TestGostSinceDaysAnchors(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name       string
		maxDate    string // пусто — NULL
		maxPub     string // пусто — NULL
		settle     int
		wantEmpty  bool
		wantDays   []string
		wantAnchor string // ожидаемая подстрока записанного вызова якоря
	}{
		{
			// Якорь — дата выдачи: хвост перекрывает лаг публикации, а сама выдача новее дня среза.
			// Верхняя граница — сегодня МСК (12.10): часовой прогон обязан ловить и дневной батч
			// текущего дня, поэтому окно шире, чем «до вчера».
			name: "якорь по дате выдачи", maxDate: "2026-10-09", maxPub: "2026-10-05",
			settle: 3, wantDays: []string{"2026-10-06", "2026-10-07", "2026-10-08", "2026-10-09", "2026-10-10", "2026-10-11", "2026-10-12"},
			wantAnchor: "maxOrNull(date)",
		},
		{
			// Даты выдачи нет (все строки пришли без неё) — якорем становится дата публикации.
			name: "якорь по дате публикации", maxPub: "2026-10-08",
			settle: 1, wantDays: []string{"2026-10-07", "2026-10-08", "2026-10-09", "2026-10-10", "2026-10-11", "2026-10-12"},
			wantAnchor: "maxOrNull(date)",
		},
		{
			// Пустая таблица: не обход всей истории, а окно последних семи дней и флаг для warn.
			name: "пустая таблица — последняя неделя", settle: 3, wantEmpty: true,
			wantDays:   []string{"2026-10-06", "2026-10-07", "2026-10-08", "2026-10-09", "2026-10-10", "2026-10-11", "2026-10-12"},
			wantAnchor: "maxOrNull(date)",
		},
		{
			// Якорь из будущего (часы источника вперёд) прижимается к сегодня: «завтра» читать нечего.
			name: "якорь из будущего", maxPub: "2026-12-01",
			settle: 3, wantDays: []string{"2026-10-09", "2026-10-10", "2026-10-11", "2026-10-12"},
			wantAnchor: "maxOrNull(date)",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			conn := &gostFakeConn{}
			if c.maxDate != "" {
				day := gostTime(t, c.maxDate)
				conn.maxDate = &day
			}
			if c.maxPub != "" {
				day := gostTime(t, c.maxPub)
				conn.maxPub = &day
			}

			days, empty, err := gostSinceDays(ctx, conn, c.settle, gostTestNow)
			if err != nil {
				t.Fatalf("окно since: %v", err)
			}
			if empty != c.wantEmpty {
				t.Errorf("tableEmpty = %v, ожидалось %v", empty, c.wantEmpty)
			}
			if len(days) != len(c.wantDays) {
				t.Fatalf("дней %d (%v), ожидалось %d", len(days), gostDayStrings(days), len(c.wantDays))
			}
			for i, want := range c.wantDays {
				if got := days[i].Format(gostDateLayout); got != want {
					t.Errorf("день %d = %s, ожидался %s", i, got, want)
				}
			}
			calls := conn.callList()
			if len(calls) != 1 || !strings.Contains(calls[0], c.wantAnchor) {
				t.Errorf("якорь читается одним запросом maxOrNull(date): вызовы %v", calls)
			}
		})
	}
}

func TestGostSinceDaysCapsStaleAnchor(t *testing.T) {
	t.Run("свежий якорь — обычное окно до сегодня", func(t *testing.T) {
		fresh := gostTime(t, "2026-10-09")
		conn := &gostFakeConn{maxDate: &fresh}

		days, _, err := gostSinceDays(context.Background(), conn, 3, gostTestNow)
		if err != nil {
			t.Fatalf("окно since: %v", err)
		}
		// Верхняя граница — сегодняшний день МСК (12.10.2026), а не вчерашний: часовой прогон
		// обязан ловить и дневной батч текущего дня.
		if len(days) != 7 || !days[0].Equal(gostTime(t, "2026-10-06")) || !days[6].Equal(gostTime(t, "2026-10-12")) {
			t.Errorf("дни %v, ожидалось 2026-10-06…2026-10-12 (7 дней)", gostDayStrings(days))
		}
	})

	t.Run("старый якорь — окно обрезано", func(t *testing.T) {
		stale := gostTime(t, "2026-01-01")
		conn := &gostFakeConn{maxDate: &stale}
		logs := gostCaptureLog(t)

		days, empty, err := gostSinceDays(context.Background(), conn, 3, gostTestNow)
		if err != nil {
			t.Fatalf("окно since: %v", err)
		}
		if empty {
			t.Error("tableEmpty = true, ожидалось false: строки в таблице есть")
		}
		if len(days) != gostMaxSinceDays {
			t.Fatalf("дней %d, ожидалось ровно %d", len(days), gostMaxSinceDays)
		}
		if !days[0].Equal(gostTime(t, "2026-09-13")) || !days[len(days)-1].Equal(gostTime(t, "2026-10-12")) {
			t.Errorf("окно %v, ожидалось 2026-09-13…2026-10-12", gostDayStrings(days))
		}
		for _, want := range []string{"таблица отстала", "2026-01-01", "GOST_MODE=full"} {
			if !strings.Contains(logs.String(), want) {
				t.Errorf("предупреждение без %q: %s", want, logs.String())
			}
		}
	})
}

func TestGostWriteDayOneBatch(t *testing.T) {
	rows := []gostRow{
		{
			Mark: "Toyota", Type: "ZGE25", Date: gostTime(t, "2026-10-09"),
			CertificateNumber: "ТС RU А-JP.ОМ80.00001", EcoClass: "4", EngineTrademark: "2ZR-FE",
			YearOfManufacturing: 2016, NameOfManufacturer: "TOYOTA", UID: "uid-1",
			PublicationDate: gostTime(t, "2026-10-11"),
		},
		{
			Mark: "Haval", Type: "Jolion", Date: gostTime(t, "2026-10-09"),
			CertificateNumber: "ТС RU А-CN.ОМ80.00002", EcoClass: "-", EngineTrademark: "",
			YearOfManufacturing: 0, NameOfManufacturer: "GREAT WALL MOTOR", UID: "uid-2",
			PublicationDate: gostTime(t, "2026-10-11"),
		},
	}

	conn := &gostFakeConn{}
	count, err := gostWriteDay(context.Background(), conn, rows)
	if err != nil {
		t.Fatalf("запись дня: %v", err)
	}
	if count != int64(len(rows)) {
		t.Errorf("count = %d, ожидалось %d", count, len(rows))
	}

	batches := conn.batchList()
	if len(batches) != 1 {
		t.Fatalf("батчей %d, ожидался один на день: %v", len(batches), conn.callList())
	}
	batch := batches[0]
	if batch.query != gostInsert {
		t.Errorf("батч открыт запросом %q, ожидался %q", batch.query, gostInsert)
	}
	if !batch.sent {
		t.Error("батч не отправлен: Send не вызван")
	}
	inserted := batch.inserted(t)
	if len(inserted) != len(rows) {
		t.Fatalf("строк в батче %d, ожидалось %d", len(inserted), len(rows))
	}
	for i := range rows {
		if inserted[i] != rows[i] {
			t.Errorf("строка %d = %+v, ожидалась %+v", i, inserted[i], rows[i])
		}
	}
}

// TestGostRunDaysToleratesTransientDay — 500 одного дня не роняет часовой прогон:
// день уходит в warning и будет повторён следующим прогоном, а данные остальных
// дней записываются. Постоянные ошибки проверяет TestGostRunDaysFailsOnPermanentDay.
func TestGostRunDaysToleratesTransientDay(t *testing.T) {
	days := []time.Time{
		gostTime(t, "2026-10-05"),
		gostTime(t, "2026-10-06"),
		gostTime(t, "2026-10-07"),
	}
	ta := gostNewTestAPI(t, gostDayServer(t, map[string]gostDaySpec{
		"05.10.2026": {Count: 0},
		"06.10.2026": {Status: http.StatusInternalServerError},
		"07.10.2026": {Count: 1},
	}))
	conn := &gostFakeConn{}
	logs := gostCaptureLog(t)

	count, err := gostRunDays(context.Background(), conn, ta.api, days)
	if err != nil {
		t.Fatalf("временный 500 дня не должен ронять прогон: %v", err)
	}
	if count != 1 {
		t.Errorf("записано строк %d, ожидалась 1 (день с данными)", count)
	}
	batches := conn.batchList()
	if len(batches) != 1 {
		t.Fatalf("батчей %d, ожидался один (пустой день и временно недоступный день без записи): %v", len(batches), conn.callList())
	}
	inserted := batches[0].inserted(t)
	if len(inserted) != 1 || !inserted[0].PublicationDate.Equal(gostTime(t, "2026-10-07")) {
		t.Errorf("в батче %+v, ожидалась одна строка дня 2026-10-07", inserted)
	}
	for _, want := range []string{"публикаций нет", "временно не прочитан", "2026-10-06", "повтор в следующем прогоне"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("лог не содержит %q: %s", want, logs.String())
		}
	}
}

// TestGostRunDaysFailsOnPermanentDay — 400 не лечится повтором и обязан ронять прогон:
// временная терпимость не должна превращаться в молчаливый пропуск постоянной ошибки.
func TestGostRunDaysFailsOnPermanentDay(t *testing.T) {
	days := []time.Time{
		gostTime(t, "2026-10-05"),
		gostTime(t, "2026-10-06"),
	}
	ta := gostNewTestAPI(t, gostDayServer(t, map[string]gostDaySpec{
		"05.10.2026": {Status: http.StatusBadRequest},
		"06.10.2026": {Count: 1},
	}))
	conn := &gostFakeConn{}

	count, err := gostRunDays(context.Background(), conn, ta.api, days)
	if err == nil {
		t.Fatal("постоянная 400 не попала в ошибку прогона")
	}
	if !strings.Contains(err.Error(), "2026-10-05") || !strings.Contains(err.Error(), "не прочитаны дни (1)") {
		t.Errorf("ошибка %q не перечисляет постоянный сбой дня", err)
	}
	if count != 1 {
		t.Errorf("записано строк %d, ожидалась 1 (успешный день)", count)
	}
}

// TestGostRunDaysAbortsOnAuthError — систематический отказ доступа (401) прерывает обход на
// первом же дне, а не копит агрегат: при отозванном или неверном GOST_APP_ID каждый следующий
// из тысяч дней ждала бы та же ошибка, а подсказка про токен тонула бы в мегабайтном тексте.
func TestGostRunDaysAbortsOnAuthError(t *testing.T) {
	days := []time.Time{
		gostTime(t, "2026-10-05"),
		gostTime(t, "2026-10-06"),
		gostTime(t, "2026-10-07"),
	}
	ta := gostNewTestAPI(t, gostDayServer(t, map[string]gostDaySpec{
		"05.10.2026": {Status: http.StatusUnauthorized},
		"06.10.2026": {Count: 1},
		"07.10.2026": {Count: 1},
	}))
	conn := &gostFakeConn{}
	logs := gostCaptureLog(t)

	count, err := gostRunDays(context.Background(), conn, ta.api, days)
	if err == nil {
		t.Fatal("отказ авторизации не прервал прогон")
	}
	for _, want := range []string{
		"авторизация отклонена (проверьте GOST_APP_ID)",
		"прервано на дне 2026-10-05",
		"дней не прочитано 3",
		"http status 401",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ошибка %q не содержит %q", err, want)
		}
	}
	if count != 0 {
		t.Errorf("записано строк %d, ожидалось 0", count)
	}
	// Обход прерван на первом дне: остальные дни не запрашивались вовсе.
	if got := ta.requestCount(); got != 1 {
		t.Errorf("запросов %d, ожидался 1: авторизация обходом не чинится", got)
	}
	if batches := conn.batchList(); len(batches) != 0 {
		t.Errorf("батчей %d, ожидалось 0: прерывание до первой записи", len(batches))
	}
	if !strings.Contains(logs.String(), "авторизация отклонена") {
		t.Errorf("прерывание не попало в лог: %s", logs.String())
	}
}

// TestGostRunDaysCapsFailedDayList — список неудачных дней в агрегате усечён первыми десятью:
// на полном прогоне упавших дней бывают сотни, и полный перечень утопил бы первопричину,
// которая у них, как правило, одна на всех. Здесь же пин того, что 500-е дни обход не прерывают:
// запрошены все дни и агрегат собран целиком.
func TestGostRunDaysCapsFailedDayList(t *testing.T) {
	var days []time.Time
	specs := make(map[string]gostDaySpec)
	for i := 1; i <= 12; i++ {
		day := gostTime(t, fmt.Sprintf("2026-10-%02d", i))
		days = append(days, day)
		specs[day.Format(gostQueryDayLayout)] = gostDaySpec{Status: http.StatusInternalServerError}
	}
	ta := gostNewTestAPI(t, gostDayServer(t, specs))
	conn := &gostFakeConn{}

	count, err := gostRunDays(context.Background(), conn, ta.api, days)
	if err == nil {
		t.Fatal("дни со сбоем не попали в ошибку прогона")
	}
	if !strings.Contains(err.Error(), "не прочитаны дни (12)") {
		t.Errorf("ошибка %q не называет число сбойных дней", err)
	}
	if !strings.Contains(err.Error(), "и ещё 2") {
		t.Errorf("ошибка %q не усекает список дней", err)
	}
	if !strings.Contains(err.Error(), "2026-10-10") {
		t.Errorf("ошибка %q не перечисляет первые десять дней", err)
	}
	if strings.Contains(err.Error(), "2026-10-11") || strings.Contains(err.Error(), "2026-10-12") {
		t.Errorf("ошибка %q перечисляет дни сверх первых десяти", err)
	}
	if got := ta.requestCount(); got != len(days)*gostMaxTries {
		t.Errorf("запросов %d, ожидалось %d: 500-е дни обход не прерывают", got, len(days)*gostMaxTries)
	}
	if count != 0 {
		t.Errorf("записано строк %d, ожидалось 0", count)
	}
}

func TestGostRunDaysForeignDateOnlyDayWarns(t *testing.T) {
	days := []time.Time{gostTime(t, "2026-10-05"), gostTime(t, "2026-10-06")}
	// Текстовый query ручки матчит дату в любом поле документа, поэтому у дня может не быть
	// своих публикаций, а count — ненулевой: все четыре совпадения относятся к другим датам
	// публикации. Это штатное поведение источника, и день не ошибка — иначе часовой DAG
	// краснел бы на каждом таком дне, пока окно его не проедет.
	ta := gostNewTestAPI(t, gostDayServer(t, map[string]gostDaySpec{
		"05.10.2026": {Count: 4, Noise: 4},
		"06.10.2026": {Count: 1},
	}))
	conn := &gostFakeConn{}
	logs := gostCaptureLog(t)

	count, err := gostRunDays(context.Background(), conn, ta.api, days)
	if err != nil {
		t.Fatalf("день из одних чужих дат публикации уронил прогон: %v", err)
	}
	if count != 1 {
		t.Errorf("записано строк %d, ожидалась 1 (день с данными)", count)
	}
	if batches := conn.batchList(); len(batches) != 1 {
		t.Errorf("батчей %d, ожидался 1: день без своих публикаций не пишется", len(batches))
	}
	if !strings.Contains(logs.String(), "публикаций нет — отброшено 4 из 4 (чужой даты: 4, невалидных: 0)") {
		t.Errorf("день без своих публикаций не отмечен warn-ом с разбивкой: %s", logs.String())
	}
	if strings.Contains(logs.String(), "не разобраны") {
		t.Errorf("чужие даты объявлены невалидными документами: %s", logs.String())
	}
}

// TestGostRunDaysAllInvalidDocsFail — невалиден КАЖДЫЙ документ дня (Invalid == Count): это
// систематическая поломка формата или данных, и день обязан попасть в ошибку прогона. Именно
// этот случай удерживает правило «fail loud» — частичная невалидность дня прогон не роняет.
func TestGostRunDaysAllInvalidDocsFail(t *testing.T) {
	days := []time.Time{gostTime(t, "2026-10-05"), gostTime(t, "2026-10-06")}
	// Ни одного документа с датой выдачи: срез пуст, Invalid == Count.
	ta := gostNewTestAPI(t, gostDayServer(t, map[string]gostDaySpec{
		"05.10.2026": {Count: 3, Invalid: 3},
		"06.10.2026": {Count: 1},
	}))
	conn := &gostFakeConn{}

	count, err := gostRunDays(context.Background(), conn, ta.api, days)
	if err == nil {
		t.Fatal("день, не разобранный целиком, не попал в ошибку прогона")
	}
	for _, want := range []string{
		"2026-10-05",
		"все 3 документов дня невалидны",
		"не прочитаны дни (1)",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ошибка %q не содержит %q", err, want)
		}
	}
	// Проход продолжается: следующий день записан, несмотря на сбойный.
	if count != 1 {
		t.Errorf("записано строк %d, ожидалась 1", count)
	}
	if batches := conn.batchList(); len(batches) != 1 {
		t.Errorf("батчей %d, ожидался 1: сбойный день не пишется, следующий пишется", len(batches))
	}
}

// TestGostRunDaysWarnsInvalidBreakdown — невалидные документы видны и у дня с непустым срезом:
// warn печатает разбивку отброшенных, иначе поломка формата тонула бы в общем Skipped.
func TestGostRunDaysWarnsInvalidBreakdown(t *testing.T) {
	days := []time.Time{gostTime(t, "2026-10-05")}
	// Один свой документ, один чужой даты и один невалидный (нет даты выдачи).
	ta := gostNewTestAPI(t, gostDayServer(t, map[string]gostDaySpec{
		"05.10.2026": {Count: 3, Invalid: 1, Noise: 1},
	}))
	conn := &gostFakeConn{}
	logs := gostCaptureLog(t)

	count, err := gostRunDays(context.Background(), conn, ta.api, days)
	if err != nil {
		t.Fatalf("день с одним невалидным документом уронил прогон (срез непустой): %v", err)
	}
	if count != 1 {
		t.Errorf("записано строк %d, ожидалась 1", count)
	}
	for _, want := range []string{"чужой даты: 1, невалидных: 1", "отброшено 2 документов из 3"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("warn дня без %q: %s", want, logs.String())
		}
	}
}

// TestGostRunDaysPartlyInvalidNoisyDayWarns — день с пустым срезом, где невалидны лишь отдельные
// документы (Invalid < Count), успешен: одиночный мусорный документ в шумовом дне — сбой одной
// записи источника, и валить на нём весь прогон не за что. Warn обязан нести обе цифры: по одной
// невалидности не отличить систематическую поломку от чужой даты публикации.
func TestGostRunDaysPartlyInvalidNoisyDayWarns(t *testing.T) {
	// Один документ из 36 невалиден (нет даты выдачи), остальные 35 — чужих дат публикации.
	days := []time.Time{gostTime(t, "2026-10-05")}
	ta := gostNewTestAPI(t, gostDayServer(t, map[string]gostDaySpec{
		"05.10.2026": {Count: 36, Invalid: 1, Noise: 35},
	}))
	conn := &gostFakeConn{}
	logs := gostCaptureLog(t)

	count, err := gostRunDays(context.Background(), conn, ta.api, days)
	if err != nil {
		t.Fatalf("невалидность части дня уронила прогон: %v", err)
	}
	if count != 0 {
		t.Errorf("записано строк %d, ожидалось 0: писать нечего", count)
	}
	// День не в списке неудачных: ни ошибки дня, ни агрегата.
	if strings.Contains(logs.String(), "документов дня невалидны") {
		t.Errorf("частично невалидный день помечен поломкой: %s", logs.String())
	}
	if batches := conn.batchList(); len(batches) != 0 {
		t.Errorf("батчей %d, ожидалось 0: писать нечего", len(batches))
	}
	for _, want := range []string{
		"публикаций нет — отброшено 36 из 36",
		"чужой даты: 35, невалидных: 1",
	} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("warn дня без %q: %s", want, logs.String())
		}
	}
}

func TestGostCheckDaysMatchNoError(t *testing.T) {
	days := []time.Time{gostTime(t, "2026-10-09")}
	ta := gostNewTestAPI(t, gostDayServer(t, map[string]gostDaySpec{"09.10.2026": {Count: 5}}))
	conn := &gostFakeConn{counters: map[string]uint64{"2026-10-09": 5}}

	checked, err := gostCheckDays(context.Background(), conn, ta.api, days)
	if err != nil {
		t.Fatalf("совпавший день дал ошибку: %v", err)
	}
	if checked != 1 {
		t.Errorf("проверено дней %d, ожидался 1", checked)
	}
	// Сверка совпавших дней — только count: полный день не выкачивается, иначе check по
	// диапазону тянул бы гигабайты вместо одного запроса на день.
	requests := ta.requests()
	if len(requests) != 1 || requests[0].Query.Get("size") != "1" {
		t.Errorf("запросы дня %d (%v), ожидался один с size=1", len(requests), requests)
	}
	if calls := conn.callList(); len(calls) != 1 || !strings.Contains(calls[0], "uniqExact(certificate_number)") {
		t.Errorf("счётчики таблицы читаются одним запросом: вызовы %v", calls)
	}
}

func TestGostCheckDaysReportsGap(t *testing.T) {
	days := []time.Time{gostTime(t, "2026-10-09")}
	ta := gostNewTestAPI(t, gostDayServer(t, map[string]gostDaySpec{"09.10.2026": {Count: 5}}))
	conn := &gostFakeConn{counters: map[string]uint64{"2026-10-09": 3}}
	logs := gostCaptureLog(t)

	checked, err := gostCheckDays(context.Background(), conn, ta.api, days)
	if err == nil {
		t.Fatal("расхождение счётчиков не стало ошибкой check")
	}
	if !strings.Contains(err.Error(), "2026-10-09") || !strings.Contains(err.Error(), "3 против 5") {
		t.Errorf("ошибка %q не называет день и расхождение «3 против 5»", err)
	}
	if checked != 1 {
		t.Errorf("проверено дней %d, ожидался 1", checked)
	}
	if !strings.Contains(logs.String(), "дыр 1") {
		t.Errorf("сводка не сообщает о дыре: %s", logs.String())
	}
}

func TestGostCheckDaysTextNoiseNotGap(t *testing.T) {
	days := []time.Time{gostTime(t, "2026-10-09")}
	// count=7: два документа — текстовые совпадения чужой даты публикации, и клиентский
	// фильтр дня оставляет ровно те же пять строк, что в таблице.
	ta := gostNewTestAPI(t, gostDayServer(t, map[string]gostDaySpec{"09.10.2026": {Count: 7, Noise: 2}}))
	conn := &gostFakeConn{counters: map[string]uint64{"2026-10-09": 5}}
	logs := gostCaptureLog(t)

	checked, err := gostCheckDays(context.Background(), conn, ta.api, days)
	if err != nil {
		t.Fatalf("текстовый шум объявлен дырой: %v", err)
	}
	if checked != 1 {
		t.Errorf("проверено дней %d, ожидался 1", checked)
	}
	if !strings.Contains(logs.String(), "шум: 7 → 5") {
		t.Errorf("отчёт не различает текстовый шум: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "дыр 0") {
		t.Errorf("сводка не сообщает об отсутствии дыр: %s", logs.String())
	}
}

// gostCertNumber — номер i-го документа дня: тем же форматом его собирает gostDayDoc, и по нему
// фикстуры фейковой таблицы (dayNums/numbers) стыкуются с дневным срезом источника.
func gostCertNumber(id int) string { return fmt.Sprintf("ТС RU А-JP.ОМ80.%05d", id) }

// TestGostCheckDaysRepublicationNotGap — расхождение счётчиков объясняется перепубликацией:
// документ лежит в таблице, но под более поздней датой публикации, потому что ключ таблицы её не
// хранит. Такой день — не дыра: он виден строкой отчёта и счётчиком перепубликаций.
func TestGostCheckDaysRepublicationNotGap(t *testing.T) {
	days := []time.Time{gostTime(t, "2026-10-09")}
	ta := gostNewTestAPI(t, gostDayServer(t, map[string]gostDaySpec{"09.10.2026": {Count: 5}}))
	conn := &gostFakeConn{
		counters: map[string]uint64{"2026-10-09": 3},
		dayNums: map[string][]string{"2026-10-09": {
			gostCertNumber(1), gostCertNumber(2), gostCertNumber(3),
		}},
		// Номера 4 и 5 в таблице есть, но помечены другой (более поздней) датой публикации.
		numbers: map[string]bool{gostCertNumber(4): true, gostCertNumber(5): true},
	}
	logs := gostCaptureLog(t)

	checked, err := gostCheckDays(context.Background(), conn, ta.api, days)
	if err != nil {
		t.Fatalf("перепубликация объявлена дырой: %v", err)
	}
	if checked != 1 {
		t.Errorf("проверено дней %d, ожидался 1", checked)
	}
	for _, want := range []string{
		"2 документов перепубликованы под другой датой публикации",
		gostCertNumber(4),
		"реальных пропусков 0",
		"дыр 0, перепубликаций 2",
	} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("отчёт о перепубликации без %q: %s", want, logs.String())
		}
	}
}

// TestGostCheckDaysAllNumbersStoredNoGap — каждый номер дневного среза в таблице этого дня
// публикации есть, хотя счётчик таблицы больше среза (ручка часть своих документов текстом не
// нашла): расхождения нет, и перепубликаций в этот день тоже нет.
func TestGostCheckDaysAllNumbersStoredNoGap(t *testing.T) {
	days := []time.Time{gostTime(t, "2026-10-09")}
	ta := gostNewTestAPI(t, gostDayServer(t, map[string]gostDaySpec{"09.10.2026": {Count: 5}}))
	conn := &gostFakeConn{
		counters: map[string]uint64{"2026-10-09": 7},
		dayNums: map[string][]string{"2026-10-09": {
			gostCertNumber(1), gostCertNumber(2), gostCertNumber(3), gostCertNumber(4),
			gostCertNumber(5), gostCertNumber(6), gostCertNumber(7),
		}},
	}
	logs := gostCaptureLog(t)

	checked, err := gostCheckDays(context.Background(), conn, ta.api, days)
	if err != nil {
		t.Fatalf("полный день объявлен расхождением: %v", err)
	}
	if checked != 1 {
		t.Errorf("проверено дней %d, ожидался 1", checked)
	}
	if !strings.Contains(logs.String(), "все номера дневного среза есть в таблице") {
		t.Errorf("отчёт не отмечает, что все номера дня в таблице: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "дыр 0, перепубликаций 0") {
		t.Errorf("сводка не названа: %s", logs.String())
	}
}

// TestGostCheckDaysRealGapAfterRepublication — часть отсутствующих номеров дня в таблице есть под
// другой датой (перепубликация), а одного нет вовсе: день обязан попасть в ошибку, и в списке
// пропусков должен стоять только настоящий пропуск. Счётчик перепубликаций при этом тоже растёт.
func TestGostCheckDaysRealGapAfterRepublication(t *testing.T) {
	days := []time.Time{gostTime(t, "2026-10-09")}
	ta := gostNewTestAPI(t, gostDayServer(t, map[string]gostDaySpec{"09.10.2026": {Count: 5}}))
	conn := &gostFakeConn{
		counters: map[string]uint64{"2026-10-09": 3},
		dayNums: map[string][]string{"2026-10-09": {
			gostCertNumber(1), gostCertNumber(2), gostCertNumber(3),
		}},
		// Номер 4 — перепубликован, номера 5 в таблице нет вовсе.
		numbers: map[string]bool{gostCertNumber(4): true},
	}
	logs := gostCaptureLog(t)

	checked, err := gostCheckDays(context.Background(), conn, ta.api, days)
	if err == nil {
		t.Fatal("настоящий пропуск не стал ошибкой check")
	}
	if checked != 1 {
		t.Errorf("проверено дней %d, ожидался 1", checked)
	}
	if !strings.Contains(err.Error(), "2026-10-09") || !strings.Contains(err.Error(), gostCertNumber(5)) {
		t.Errorf("ошибка %q не называет день и настоящий пропуск %s", err, gostCertNumber(5))
	}
	if !strings.Contains(err.Error(), "реальных пропусков 1") {
		t.Errorf("ошибка %q не называет число настоящих пропусков", err)
	}
	if strings.Contains(err.Error(), gostCertNumber(4)) {
		t.Errorf("перепубликованный номер объявлен пропуском: %q", err)
	}
	for _, want := range []string{
		"перепубликованы под другой датой публикации",
		"реальных пропусков 1",
		"дыр 1, перепубликаций 1",
	} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("отчёт без %q: %s", want, logs.String())
		}
	}
}

func TestGostCheckDaysSkippedDayFails(t *testing.T) {
	days := []time.Time{gostTime(t, "2026-10-08"), gostTime(t, "2026-10-09")}
	ta := gostNewTestAPI(t, gostDayServer(t, map[string]gostDaySpec{
		"08.10.2026": {Count: 5},
		"09.10.2026": {Status: http.StatusInternalServerError},
	}))
	conn := &gostFakeConn{counters: map[string]uint64{"2026-10-08": 5}}
	logs := gostCaptureLog(t)

	checked, err := gostCheckDays(context.Background(), conn, ta.api, days)
	if err == nil {
		t.Fatal("недоступный день не попал в ошибку check")
	}
	if !strings.Contains(err.Error(), "2026-10-09") {
		t.Errorf("ошибка %q не называет недоступный день", err)
	}
	// Остальные дни сверены: отказ ручки на одном дне не отменяет отчёт по другим.
	if checked != 1 {
		t.Errorf("проверено дней %d, ожидался 1", checked)
	}
	if !strings.Contains(logs.String(), "проверено 1 дней, дыр 0") {
		t.Errorf("сводка не названа: %s", logs.String())
	}
}

func TestGostCheckDaysLegacyWarnOnly(t *testing.T) {
	gostPinClock(t, gostTestNow)
	gostEnv(t, map[string]string{
		gostModeEnv:  gostModeCheck,
		gostFromEnv:  "2026-10-09",
		gostToEnv:    "2026-10-09",
		gostAppIDEnv: gostTestAppID,
	})
	// Легаси-строки в таблице есть (42), но в окно сверки они не попадают: у них
	// publication_date = 1970-01-01. Предупреждение — единственный их след в прогоне.
	gostNewTestAPI(t, gostDayServer(t, map[string]gostDaySpec{"09.10.2026": {Count: 5}}))
	conn := &gostFakeConn{legacy: 42, counters: map[string]uint64{"2026-10-09": 5}}
	logs := gostCaptureLog(t)

	count, err := (&gostImporter{}).Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("легаси-строки стали ошибкой прогона: %v", err)
	}
	if count != 1 {
		t.Errorf("проверено дней %d, ожидался 1", count)
	}
	for _, want := range []string{"1970-01-01", "GOST_MODE=full"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("предупреждение о строках до бэкфилла без %q: %s", want, logs.String())
		}
	}
}

func TestGostImportCheckDoesNotWrite(t *testing.T) {
	gostEnv(t, map[string]string{
		gostModeEnv:  gostModeCheck,
		gostFromEnv:  "2026-10-09",
		gostToEnv:    "2026-10-09",
		gostAppIDEnv: gostTestAppID,
	})
	gostNewTestAPI(t, gostDayServer(t, map[string]gostDaySpec{"09.10.2026": {Count: 0}}))
	conn := &gostFakeConn{counters: map[string]uint64{}}

	count, err := (&gostImporter{}).Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("check по пустому дню: %v", err)
	}
	if count != 1 {
		t.Errorf("проверено дней %d, ожидался 1", count)
	}

	calls := conn.callList()
	var execs []string
	for _, call := range calls {
		switch {
		case strings.HasPrefix(call, "preparebatch:"):
			t.Errorf("check открыл батч: %s", call)
		case strings.Contains(call, "VIEW"):
			t.Errorf("check публикует витрину: %s", call)
		case strings.HasPrefix(call, "exec: "):
			execs = append(execs, strings.TrimPrefix(call, "exec: "))
		}
	}
	// Exec в check — только DDL таблицы: схема обязана существовать до сверки (на чистой базе
	// прогон check иначе упал бы на неизвестной таблице).
	want := gostSchemaStatements()
	if len(execs) != len(want) {
		t.Fatalf("Exec-вызовов %d, ожидалось %d (DDL таблицы)", len(execs), len(want))
	}
	for i := range want {
		if execs[i] != want[i] {
			t.Errorf("Exec %d = %q, ожидался %q", i, execs[i], want[i])
		}
	}
}

func TestGostImportSinceEmptyTableWarns(t *testing.T) {
	gostPinClock(t, gostTestNow)
	gostEnv(t, map[string]string{gostModeEnv: gostModeSince, gostAppIDEnv: gostTestAppID})
	gostNewTestAPI(t, gostEveryDayServer(t, gostDaySpec{Count: 1}))
	conn := &gostFakeConn{} // пустая таблица: якорь NULL, якорь публикаций NULL
	logs := gostCaptureLog(t)

	count, err := (&gostImporter{}).Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("since по пустой таблице: %v", err)
	}
	if count != gostEmptyTableDays {
		t.Errorf("записано строк %d, ожидалось %d (окно последней недели)", count, gostEmptyTableDays)
	}
	batches := conn.batchList()
	if len(batches) != gostEmptyTableDays {
		t.Fatalf("батчей %d, ожидалось %d (день — батч)", len(batches), gostEmptyTableDays)
	}
	for _, want := range []string{"история не загружена", "GOST_MODE=full"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("предупреждение пустой таблицы без %q: %s", want, logs.String())
		}
	}
}

func TestGostImportUnknownModeFailsBeforeDB(t *testing.T) {
	gostEnv(t, map[string]string{gostModeEnv: "bogus", gostAppIDEnv: gostTestAppID})
	conn := &gostFakeConn{}

	count, err := (&gostImporter{}).Import(context.Background(), conn)
	if err == nil {
		t.Fatal("неизвестный режим принят")
	}
	if count != 0 {
		t.Errorf("count = %d, ожидался 0", count)
	}
	// Опечатка в режиме обязана стоить нуля обращений к БД: ни DDL, ни запросов.
	if calls := conn.callList(); len(calls) != 0 {
		t.Errorf("до ошибки конфига соединение уже использовано: %v", calls)
	}
}

func TestGostImportLegacyQueryFails(t *testing.T) {
	gostEnv(t, map[string]string{gostModeEnv: gostModeSince, gostAppIDEnv: gostTestAppID})
	conn := &gostFakeConn{queryErr: errors.New("соединение потеряно")}

	_, err := (&gostImporter{}).Import(context.Background(), conn)
	if err == nil {
		t.Fatal("ошибка чтения легаси-строк проглочена")
	}
	if !strings.Contains(err.Error(), "легаси-строки не посчитаны") || !strings.Contains(err.Error(), "соединение потеряно") {
		t.Errorf("ошибка %q не описывает сбой счёта легаси-строк", err)
	}
	if batches := conn.batchList(); len(batches) != 0 {
		t.Errorf("до счёта легаси-строк открыт батч: %d", len(batches))
	}
}

// gostDayStrings — дни для сообщений об ошибках: список в формате прогона.
func gostDayStrings(days []time.Time) []string {
	out := make([]string, 0, len(days))
	for _, day := range days {
		out = append(out, day.Format(gostDateLayout))
	}
	return out
}

// gostFakeBatch — батч в память: тест проверяет число и состав записанных строк.
type gostFakeBatch struct {
	driver.Batch
	query string
	rows  [][]any
	sent  bool
	abort bool
}

func (b *gostFakeBatch) Append(v ...any) error {
	b.rows = append(b.rows, append([]any(nil), v...))
	return nil
}

func (b *gostFakeBatch) Send() error { b.sent = true; return nil }

func (b *gostFakeBatch) Abort() error { b.abort = true; return nil }

func (b *gostFakeBatch) Rows() int { return len(b.rows) }

// inserted разбирает записанные строки по колонкам DDL: порядок аргументов Append — контракт
// (он же порядок колонок), и перепутанные поля обязаны валить тест, а не уезжать в базу.
func (b *gostFakeBatch) inserted(t *testing.T) []gostRow {
	t.Helper()
	rows := make([]gostRow, 0, len(b.rows))
	for i, v := range b.rows {
		if len(v) != 10 {
			t.Fatalf("строка батча %d: значений %d, want 10 (mark, type, date, certificate_number, ecoclass, enginetrademark, yearofmanufacturing, nameofmanufacturer, uid, publication_date)", i, len(v))
		}
		mark, ok := v[0].(string)
		if !ok {
			t.Fatalf("строка батча %d: mark = %T (want string)", i, v[0])
		}
		typ, ok := v[1].(string)
		if !ok {
			t.Fatalf("строка батча %d: type = %T (want string)", i, v[1])
		}
		date, ok := v[2].(time.Time)
		if !ok {
			t.Fatalf("строка батча %d: date = %T (want time.Time)", i, v[2])
		}
		number, ok := v[3].(string)
		if !ok {
			t.Fatalf("строка батча %d: certificate_number = %T (want string)", i, v[3])
		}
		ecoclass, ok := v[4].(string)
		if !ok {
			t.Fatalf("строка батча %d: ecoclass = %T (want string)", i, v[4])
		}
		engine, ok := v[5].(string)
		if !ok {
			t.Fatalf("строка батча %d: enginetrademark = %T (want string)", i, v[5])
		}
		year, ok := v[6].(uint16)
		if !ok {
			t.Fatalf("строка батча %d: yearofmanufacturing = %T (want uint16)", i, v[6])
		}
		manufacturer, ok := v[7].(string)
		if !ok {
			t.Fatalf("строка батча %d: nameofmanufacturer = %T (want string)", i, v[7])
		}
		uid, ok := v[8].(string)
		if !ok {
			t.Fatalf("строка батча %d: uid = %T (want string)", i, v[8])
		}
		pub, ok := v[9].(time.Time)
		if !ok {
			t.Fatalf("строка батча %d: publication_date = %T (want time.Time)", i, v[9])
		}
		rows = append(rows, gostRow{
			Mark: mark, Type: typ, Date: date, CertificateNumber: number, EcoClass: ecoclass,
			EngineTrademark: engine, YearOfManufacturing: year, NameOfManufacturer: manufacturer, UID: uid,
			PublicationDate: pub,
		})
	}
	return rows
}

// gostFakeRows — строки ответа фейкового соединения: очередь «установщиков» приёмников.
// Пустой список — пустой ответ (таблицы нет или в окне нет строк).
type gostFakeRows struct {
	driver.Rows
	setters []func(dest ...any) error
	idx     int
	err     error // её отдаёт Err() — обрыв обхода после Next
}

func (r *gostFakeRows) Next() bool {
	if r.idx >= len(r.setters) {
		return false
	}
	r.idx++
	return true
}

func (r *gostFakeRows) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	return r.setters[r.idx-1](dest...)
}

func (r *gostFakeRows) Err() error { return r.err }

func (r *gostFakeRows) Close() error { return nil }

// gostFakeRow — одна строка (QueryRow): Scan — тот же установщик приёмников, а заданная ошибка
// возвращается из Scan (как у живого драйвера: QueryRow об ошибке сообщает именно её).
type gostFakeRow struct {
	driver.Row
	scan func(dest ...any) error
	err  error
}

func (r *gostFakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	return r.scan(dest...)
}

func (r *gostFakeRow) Err() error { return r.err }

// gostFakeConn — фейковое соединение импортёра: встраивание driver.Conn оставляет
// неиспользуемые методы nil (паника, если тест зайдёт дальше сценария), а переопределены ровно
// четыре метода прогона: Exec (DDL), QueryRow (якорь since и легаси-строки), Query (счётчики и
// номера check) и PrepareBatch (запись дня). Ответы выбираются по виду запроса, а не по номеру
// вызова.
type gostFakeConn struct {
	driver.Conn
	mu       sync.Mutex
	calls    []string
	maxDate  *time.Time // NULL — пустая таблица: строк с настоящей датой выдачи нет
	maxPub   *time.Time // NULL — настоящих publication_date нет (только дефолт 1970-01-01)
	legacy   uint64
	counters map[string]uint64   // счётчики uniqExact(certificate_number) по дню publication_date
	dayNums  map[string][]string // номера таблицы по дню publication_date (запрос номеров дня)
	numbers  map[string]bool     // номера, встречающиеся в таблице вообще (запрос перепубликаций)
	queryErr error               // Query/QueryRow вернут её вместо строк
	rowsErr  error               // её отдаст rows.Err()
	batches  []*gostFakeBatch
}

func (c *gostFakeConn) record(call string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, call)
}

func (c *gostFakeConn) callList() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.calls...)
}

func (c *gostFakeConn) batchList() []*gostFakeBatch {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*gostFakeBatch(nil), c.batches...)
}

func (c *gostFakeConn) Exec(_ context.Context, query string, _ ...any) error {
	c.record("exec: " + query)
	return nil
}

func (c *gostFakeConn) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.record("preparebatch: " + query)
	batch := &gostFakeBatch{query: query}
	c.mu.Lock()
	c.batches = append(c.batches, batch)
	c.mu.Unlock()
	return batch, nil
}

func (c *gostFakeConn) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	c.record("query: " + query)
	if c.queryErr != nil {
		return nil, c.queryErr
	}
	switch {
	case strings.Contains(query, "uniqExact(certificate_number)"):
		var from, to time.Time
		if len(args) == 2 {
			from, _ = args[0].(time.Time)
			to, _ = args[1].(time.Time)
		}
		return &gostFakeRows{setters: gostCounterSetters(c.counters, from, to), err: c.rowsErr}, nil
	case strings.Contains(query, "WHERE publication_date = ?"):
		// Номера дня публикации: аргумент — сам день, набор — из dayNums.
		if len(args) != 1 {
			return nil, fmt.Errorf("фейковый запрос номеров дня: аргументов %d (want 1)", len(args))
		}
		day, ok := args[0].(time.Time)
		if !ok {
			return nil, fmt.Errorf("фейковый запрос номеров дня: аргумент %T (want time.Time)", args[0])
		}
		return &gostFakeRows{setters: gostNumberSetters(c.dayNums[day.Format(gostDateLayout)]), err: c.rowsErr}, nil
	case strings.Contains(query, "certificate_number IN (?)"):
		// Номера, встречающиеся в таблице вообще: список приходит одним аргументом-срезом.
		if len(args) != 1 {
			return nil, fmt.Errorf("фейковый запрос номеров таблицы: аргументов %d (want 1)", len(args))
		}
		numbers, ok := args[0].([]string)
		if !ok {
			return nil, fmt.Errorf("фейковый запрос номеров таблицы: аргумент %T (want []string)", args[0])
		}
		return &gostFakeRows{setters: gostKnownNumberSetters(c.numbers, numbers), err: c.rowsErr}, nil
	}
	return nil, fmt.Errorf("gostFakeConn: неожиданный запрос %q", query)
}

func (c *gostFakeConn) QueryRow(_ context.Context, query string, _ ...any) driver.Row {
	c.record("queryrow: " + query)
	switch {
	case strings.Contains(query, "maxOrNull(date)"):
		return &gostFakeRow{err: c.queryErr, scan: func(dest ...any) error {
			if len(dest) != 2 {
				return fmt.Errorf("фейковый якорь: приёмников %d (want 2)", len(dest))
			}
			issue, ok := dest[0].(**time.Time)
			if !ok {
				return fmt.Errorf("фейковый якорь: приёмник max(date) %T (want **time.Time)", dest[0])
			}
			pub, ok := dest[1].(**time.Time)
			if !ok {
				return fmt.Errorf("фейковый якорь: приёмник publication_date %T (want **time.Time)", dest[1])
			}
			*issue, *pub = c.maxDate, c.maxPub
			return nil
		}}
	case strings.Contains(query, "1970-01-01"):
		return &gostFakeRow{err: c.queryErr, scan: func(dest ...any) error {
			if len(dest) != 1 {
				return fmt.Errorf("фейковые легаси-строки: приёмников %d (want 1)", len(dest))
			}
			count, ok := dest[0].(*uint64)
			if !ok {
				return fmt.Errorf("фейковые легаси-строки: приёмник count() %T (want *uint64)", dest[0])
			}
			*count = c.legacy
			return nil
		}}
	}
	return &gostFakeRow{err: c.queryErr, scan: func(...any) error {
		return fmt.Errorf("gostFakeConn: неожиданный QueryRow %q", query)
	}}
}

// gostCounterSetters — строки ответа запроса счётчиков: только дни окна [from, to], у которых
// в таблице есть строки (GROUP BY по дню не отдаёт пустых дней).
func gostCounterSetters(counters map[string]uint64, from, to time.Time) []func(dest ...any) error {
	setters := make([]func(dest ...any) error, 0, len(counters))
	for day := from; !day.After(to); day = day.AddDate(0, 0, 1) {
		count, ok := counters[day.Format(gostDateLayout)]
		if !ok {
			continue
		}
		// Переменные цикла в Go ≥1.22 у каждой итерации свои: замыкание захватывает их
		// безопасно, копия не нужна.
		setters = append(setters, func(dest ...any) error {
			if len(dest) != 2 {
				return fmt.Errorf("фейковые счётчики: приёмников %d (want 2)", len(dest))
			}
			d, ok := dest[0].(*time.Time)
			if !ok {
				return fmt.Errorf("фейковые счётчики: приёмник publication_date %T (want *time.Time)", dest[0])
			}
			n, ok := dest[1].(*uint64)
			if !ok {
				return fmt.Errorf("фейковые счётчики: приёмник uniqExact %T (want *uint64)", dest[1])
			}
			*d, *n = day, count
			return nil
		})
	}
	return setters
}

// gostNumberSetters — строки ответа запроса номеров дня публикации: по одному *string на номер.
func gostNumberSetters(numbers []string) []func(dest ...any) error {
	setters := make([]func(dest ...any) error, 0, len(numbers))
	for _, number := range numbers {
		// Переменные цикла в Go ≥1.22 у каждой итерации свои: замыкание захватывает их
		// безопасно, копия не нужна.
		setters = append(setters, func(dest ...any) error {
			if len(dest) != 1 {
				return fmt.Errorf("фейковые номера дня: приёмников %d (want 1)", len(dest))
			}
			n, ok := dest[0].(*string)
			if !ok {
				return fmt.Errorf("фейковые номера дня: приёмник certificate_number %T (want *string)", dest[0])
			}
			*n = number
			return nil
		})
	}
	return setters
}

// gostKnownNumberSetters — строки ответа запроса «какие номера есть в таблице вообще»: только те
// из запрошенных, что лежат в множестве таблицы, в порядке запроса (порядок строк ответа в
// ClickHouse не гарантирован, и тест не должен зависеть от него).
func gostKnownNumberSetters(known map[string]bool, numbers []string) []func(dest ...any) error {
	setters := make([]func(dest ...any) error, 0, len(numbers))
	for _, number := range numbers {
		if !known[number] {
			continue
		}
		setters = append(setters, func(dest ...any) error {
			if len(dest) != 1 {
				return fmt.Errorf("фейковые номера таблицы: приёмников %d (want 1)", len(dest))
			}
			n, ok := dest[0].(*string)
			if !ok {
				return fmt.Errorf("фейковые номера таблицы: приёмник certificate_number %T (want *string)", dest[0])
			}
			*n = number
			return nil
		})
	}
	return setters
}

// gostDaySpec — форма ответа ручки на день: Count — сколько совпадений ручка называет (count
// ответа), Noise — сколько документов из них пришли с чужой, но разбираемой датой публикации
// (в count они входят, клиентский фильтр дня их отбрасывает), Invalid — сколько документов
// не разбираются вовсе (нет даты выдачи), Status — статус вместо ответа.
type gostDaySpec struct {
	Count   int
	Noise   int
	Invalid int
	Status  int
}

// gostDayServer отдаёт заданные формы дней; неизвестный день — 404, чтобы «прочитал лишний
// день» падало ошибкой прогона, а не тихим успехом.
func gostDayServer(t *testing.T, days map[string]gostDaySpec) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		day := r.URL.Query().Get("query")
		spec, ok := days[day]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		gostDayResponse(t, w, r, day, spec)
	}
}

// gostEveryDayServer отдаёт одну форму на любой день: нужна тестам, где день запроса
// вычисляет импортёр (окно since по пустой таблице).
func gostEveryDayServer(t *testing.T, spec gostDaySpec) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		gostDayResponse(t, w, r, r.URL.Query().Get("query"), spec)
	}
}

// gostDayResponse собирает ответ ручки: документы дня среза плюс, если он задан, текстовый шум.
// count ответа — полное число совпадений (Count), а не размер страницы: так ведёт себя ручка.
func gostDayResponse(t *testing.T, w http.ResponseWriter, r *http.Request, day string, spec gostDaySpec) {
	t.Helper()
	if spec.Status != 0 {
		w.WriteHeader(spec.Status)
		return
	}
	size, err := strconv.Atoi(r.URL.Query().Get("size"))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	n := spec.Count
	if n > size {
		n = size
	}
	docs := make([]gostDoc, 0, n)
	for i := 0; i < n; i++ {
		switch {
		case i < spec.Invalid:
			docs = append(docs, gostInvalidDoc(day, i+1))
		case i < spec.Invalid+spec.Noise:
			docs = append(docs, gostForeignDoc(day, i+1))
		default:
			docs = append(docs, gostDayDoc(day, i+1))
		}
	}
	// Тело собирается без t: хендлер живёт в горутине сервера, и t.Fatalf оттуда недопустим.
	count := spec.Count
	body, err := json.Marshal(gostSearchResponse{Count: &count, Data: docs})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(body)
}

// gostDayDoc — документ, попадающий в срез дня: дата публикации равна дню запроса.
func gostDayDoc(day string, id int) gostDoc {
	issue := gostIssueMS
	return gostDoc{ID: int64(id), Fields: gostFields{
		Product:                  "Toyota",
		Type:                     "ZGE25",
		NumberOfCertificate:      fmt.Sprintf("ТС RU А-JP.ОМ80.%05d", id),
		DateOfIssueOfCertificate: &issue,
		EcoClass:                 "4",
		EngineTrademark:          "2ZR-FE",
		YearOfManufacturing:      "2016",
		NameOfManufacturer:       "TOYOTA",
		UniqueIdentifier:         fmt.Sprintf("uid-%05d", id),
		DateOfPublication:        day + " 00:00:00 ZE3",
	}}
}

// gostForeignDoc — документ того же ответа, но чужого дня публикации: ручка ищет текстом и
// отдаёт соседние окна, а в срез дня такой документ не входит.
func gostForeignDoc(day string, id int) gostDoc {
	doc := gostDayDoc(day, id)
	doc.Fields.DateOfPublication = gostForeignPublication
	return doc
}

// gostInvalidDoc — документ, который срез дня не разберёт вовсе: нет даты выдачи. Так выглядит
// поломка формата или данных (а не штатное окно текстового поиска), и день из таких документов
// обязан валить прогон.
func gostInvalidDoc(day string, id int) gostDoc {
	doc := gostDayDoc(day, id)
	doc.Fields.DateOfIssueOfCertificate = nil
	return doc
}
