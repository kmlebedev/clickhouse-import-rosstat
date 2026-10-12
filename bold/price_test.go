package bold

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/gold"
)

// Тесты импортёра цены офлайн: источник — httptest-сервер по фикстурам bold/testdata, база —
// фейковое соединение (те же хелперы, что у тестов ETF-импортёра: boldNewTestServer,
// boldFakeConn, boldTestClock). Пинятся четыре вида контракта пакета: режим и число запросов
// (один в любом режиме), площадка записи (venue='bold' в общей gold_prices), прогон-нечего-
// обновлять без DDL и батчей, и запись каталога цены из пакета gold (импортёру принадлежит
// публикация записи, а не её текст).

// Пины фактов спеки §8 на фикстурах Task 1: цена 2026-10-09 — 4216.3, предыдущий торговый день
// 2026-10-08 — 4199.4 (те же числа, что проверяет parse_test.go на уровне разбора).
const (
	boldPinPrice      = 4216.3
	boldPinPricePrior = 4199.4
)

// boldPriceHistoryBody — тело файла истории цены для полного пути: две даты 2026-10-08 и
// 2026-10-09. Общая фикстура bold_price_history.json урезана до трёх дат ради тестов
// ETF-контура (2015-01-01 там — день без units/aum); для цены та строка тоже точка, и чтобы
// состав батча был пином, а не следствием размера общей фикстуры, здесь остаются две даты
// 2026 года (на живых данных полный путь пишет весь ряд — 3 138 точек с 2014-10-01).
func boldPriceHistoryBody(t *testing.T) []byte {
	t.Helper()
	return boldMutateHistory(t, "bold_price_history.json", func(data []map[string]any) []map[string]any {
		out := make([]map[string]any, 0, len(data))
		for _, row := range data {
			if row["date"] == boldPinEmpty {
				continue
			}
			out = append(out, row)
		}
		return out
	})
}

// boldPriceTestStat — импортёр цены прогона: httptest-сервер и фиксированные часы (boldTestNow).
// Часы здесь — вход решения о режиме, а не только лог: см. TestBoldPriceImportFullWhenStoredIsStale.
func boldPriceTestStat(server *boldTestServer) boldGoldPrice {
	return boldGoldPrice{apiBase: server.base(), now: boldTestClock()}
}

// boldPriceBatchValue — значение цены из строки батча gold_prices по (venue, date).
func boldPriceBatchValue(t *testing.T, batch *boldFakeBatch, venue, date string) float64 {
	t.Helper()
	want := testDate(t, date)
	for _, row := range batch.rows {
		if row[0] != venue {
			continue
		}
		if d, ok := row[1].(time.Time); ok && d.Equal(want) {
			value, ok := row[2].(float64)
			if !ok {
				t.Fatalf("значение цены %s@%s типа %T", venue, date, row[2])
			}
			return value
		}
	}
	t.Fatalf("в батче нет цены %s@%s (%d строк)", venue, date, len(batch.rows))
	return 0
}

func TestBoldGoldPriceName(t *testing.T) {
	if got := (&boldGoldPrice{}).Name(); got != "bold_gold_price" {
		t.Fatalf("Name() = %q, ожидалось bold_gold_price", got)
	}
}

// TestBoldGoldPriceIsRegistered: без записи в chimport.Stats импортёр не найдётся ни в dagu, ни в
// `make import STAT=bold_gold_price` (прецедент — TestBoldEtfFlowsIsRegistered). Пакет
// подключается к main.go пустым импортом, поэтому единственный след регистрации — этот init().
func TestBoldGoldPriceIsRegistered(t *testing.T) {
	for _, stat := range chimport.Stats {
		if stat.Name() != boldGoldPriceName {
			continue
		}
		if _, ok := stat.(*boldGoldPrice); !ok {
			t.Fatalf("под именем %q зарегистрирован %T, ожидался *boldGoldPrice", boldGoldPriceName, stat)
		}
		return
	}
	names := make([]string, 0, len(chimport.Stats))
	for _, stat := range chimport.Stats {
		names = append(names, stat.Name())
	}
	t.Fatalf("импортёр %q не зарегистрирован в chimport.Stats: %v", boldGoldPriceName, names)
}

// TestBoldPriceImportFullWritesVenue: пустая площадка (оба максимума NULL) уводит импорт на
// полную историю — один запрос файла истории и батч gold_prices с двумя точками площадки
// venue='bold'. Саботаж «писать venue='comex_front'» валит этот тест: подмена площадки была бы
// тихой подменой смысла исторического ряда comex_front (3 совпадения из 13 контрольных дат).
// Саботаж «спрашивать price-latest в полном режиме» тоже виден здесь: фикстуры этого пути нет,
// и сервер ответил бы 404.
func TestBoldPriceImportFullWritesVenue(t *testing.T) {
	server := boldNewTestServer(t, map[string][]byte{boldPathPrice: boldPriceHistoryBody(t)})
	conn := newBoldFakeConn() // пустая очередь — площадка bold пуста
	stat := boldPriceTestStat(server)

	count, err := stat.Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if count != 2 {
		t.Errorf("Import вернул %d строк, ожидалось 2 — полный путь пишет весь файл (08.10 и 09.10)", count)
	}

	batch := conn.batchFor(t, boldGoldPricesInsert)
	if len(batch.rows) != 2 {
		t.Fatalf("строк в %s: %d, ожидалось 2", boldPriceTable, len(batch.rows))
	}
	for _, row := range batch.rows {
		if row[0] != boldPriceVenue {
			t.Fatalf("venue строки = %v, ожидалась %q", row[0], boldPriceVenue)
		}
		if _, ok := row[1].(time.Time); !ok {
			t.Fatalf("date строки типа %T", row[1])
		}
		if _, ok := row[2].(float64); !ok {
			t.Fatalf("usd строки типа %T", row[2])
		}
	}
	if got := boldPriceBatchValue(t, batch, boldPriceVenue, boldPinDate); got != boldPinPrice {
		t.Errorf("цена %s = %v, ожидалось %v", boldPinDate, got, boldPinPrice)
	}
	if got := boldPriceBatchValue(t, batch, boldPriceVenue, boldPinPrior); got != boldPinPricePrior {
		t.Errorf("цена %s = %v, ожидалось %v", boldPinPrior, got, boldPinPricePrior)
	}
	if !batch.sent {
		t.Error("батч цены обязан быть отправлен (Send)")
	}

	// Гвардейский запрос максимума пинится текстом: у kimi_reader нет SELECT на gold_prices
	// (проверено: чтение идёт через витрину v_gold_prices), поэтому проверить его иначе нечем.
	// Площадка в запросе обязана быть своя: gold_prices делят три источника (gold, ingest и
	// этот), и максимум всей таблицы увёл бы импорт в no-op из-за чужих свежих точек.
	calls := conn.callList()
	if boldTestCallIndex(calls, "queryrow: "+boldPriceStoredMaxQuery) < 0 {
		t.Errorf("максимум площадки не прочитан запросом %q: %v", boldPriceStoredMaxQuery, calls)
	}
	// Копия канонического DDL gold_prices идёт до батча (на чистой базе таблицы ещё нет),
	// каталог — после Send (описание ряда, которого в таблице нет, — обещание данных).
	ddl := boldTestCallIndex(calls, "exec: CREATE TABLE IF NOT EXISTS "+boldPriceTable)
	firstBatch := boldTestCallIndex(calls, "preparebatch: "+boldGoldPricesInsert)
	catalog := boldTestCallIndex(calls, "preparebatch: INSERT INTO series_catalog")
	if ddl < 0 || firstBatch < 0 || catalog < 0 {
		t.Fatalf("порядок обращений: %v", calls)
	}
	if ddl > firstBatch {
		t.Errorf("DDL обязана идти до батча: %v", calls)
	}
	if catalog < firstBatch {
		t.Errorf("каталог обязан писаться после батча: %v", calls)
	}

	// Режим стоит один запрос, и это файл истории: пробного price-latest нет — иначе полный путь
	// был бы двухзапросным.
	if got := server.requested(); len(got) != 1 || got[0] != boldPathPrice {
		t.Errorf("запросы полного пути: %v, ожидался один %q", got, boldPathPrice)
	}
}

// TestBoldPriceImportIncrementalWritesOneDay: площадка за 08.10, дата источника 09.10, разрыв
// один календарный день — инкремент: один запрос price-latest и одна точка 09.10. Саботаж
// «ходить в файл истории и писать весь ряд» виден здесь: фикстуры gold/price у сервера нет, и
// такой запрос вернул бы 404, а не тихо дописал лишние дни.
func TestBoldPriceImportIncrementalWritesOneDay(t *testing.T) {
	server := boldNewTestServer(t, map[string][]byte{boldPathPriceLatest: testFixture(t, "bold_price_latest.json")})
	conn := newBoldFakeConn(boldFakeDate(t, boldPinPrior))
	stat := boldPriceTestStat(server)

	count, err := stat.Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if count != 1 {
		t.Errorf("Import вернул %d строк, ожидалось 1 — инкремент пишет только дату источника", count)
	}

	batch := conn.batchFor(t, boldGoldPricesInsert)
	if len(batch.rows) != 1 {
		t.Fatalf("строк в %s: %d, ожидалась 1 за 09.10", boldPriceTable, len(batch.rows))
	}
	if got := batch.rows[0][0]; got != boldPriceVenue {
		t.Errorf("venue строки = %v, ожидалась %q", got, boldPriceVenue)
	}
	if date := batch.rows[0][1].(time.Time); !date.Equal(testDate(t, boldPinDate)) {
		t.Errorf("дата точки = %s, ожидалась %s", date.Format(boldDateLayout), boldPinDate)
	}
	if got := boldPriceBatchValue(t, batch, boldPriceVenue, boldPinDate); got != boldPinPrice {
		t.Errorf("цена %s = %v, ожидалось %v", boldPinDate, got, boldPinPrice)
	}
	if got := server.requested(); len(got) != 1 || got[0] != boldPathPriceLatest {
		t.Errorf("запросы инкремента: %v, ожидался один %q", got, boldPathPriceLatest)
	}
}

// TestBoldPriceImportFullWhenStoredIsStale: сохранённый максимум старше порога полного пути
// (06.10 против дня прогона 10.10) — режим полный, потому что инкремент не может исключить
// пропуск дней, а «пробный» price-latest сделал бы полный путь двухзапросным. Файл истории
// пишется целиком, включая дни не новее сохранённого максимума: перезапись идемпотентна и
// заодно лечит прошлые дыры.
func TestBoldPriceImportFullWhenStoredIsStale(t *testing.T) {
	server := boldNewTestServer(t, map[string][]byte{boldPathPrice: boldPriceHistoryBody(t)})
	conn := newBoldFakeConn(boldFakeDate(t, "2026-10-06"))
	stat := boldPriceTestStat(server)

	count, err := stat.Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if count != 2 {
		t.Errorf("Import вернул %d строк, ожидалось 2 — полный путь пишет весь файл, а не один день", count)
	}

	batch := conn.batchFor(t, boldGoldPricesInsert)
	if len(batch.rows) != 2 {
		t.Fatalf("строк в %s: %d, ожидалось 2 (08.10 и 09.10)", boldPriceTable, len(batch.rows))
	}
	if got := server.requested(); len(got) != 1 || got[0] != boldPathPrice {
		t.Errorf("запросы полного пути: %v, ожидался один %q без пробного %q", got, boldPathPrice, boldPathPriceLatest)
	}
}

// TestBoldPriceImportModeIsZoneAgnostic (Ruling 13): порог 96 ч считается по моментам времени в
// UTC, а не по календарным полям зоны процесса. Сохранённый максимум — дата UTC-календаря
// (00:00 UTC), поэтому «та же граница, выраженная в другом поясе» обязана дать то же решение:
// иначе машина разработчика в +05:00 и сервер в −07:00 выбирали бы разные режимы в один и тот же
// момент. Сервер отдаёт оба набора фикстур, а режим читается по единственному запрошенному пути:
// инкремент стоит одного price-latest, полный путь — одного price.json.
func TestBoldPriceImportModeIsZoneAgnostic(t *testing.T) {
	const stored = "2026-10-06" // 00:00 UTC — от него и отсчитывается порог
	cases := []struct {
		name     string
		now      time.Time
		wantPath string
		wantRows int64
	}{
		{
			name:     "ровно порог 96 ч — инкремент",
			now:      time.Date(2026, time.October, 10, 0, 0, 0, 0, time.UTC),
			wantPath: boldPathPriceLatest,
			wantRows: 1,
		},
		{
			name:     "минута сверх порога — полный путь",
			now:      time.Date(2026, time.October, 10, 0, 1, 0, 0, time.UTC),
			wantPath: boldPathPrice,
			wantRows: 2,
		},
		{
			// Тот же момент, что и в первом случае, но в поясе −07:00: локальные сутки у него
			// 2026-10-09. Сравнение по календарю зоны дало бы другую ветку — по моментам её нет.
			name:     "та же граница в поясе −07:00 (локальные сутки другого дня)",
			now:      time.Date(2026, time.October, 9, 17, 0, 0, 0, time.FixedZone("PDT", -7*60*60)),
			wantPath: boldPathPriceLatest,
			wantRows: 1,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			server := boldNewTestServer(t, map[string][]byte{
				boldPathPrice:       boldPriceHistoryBody(t),
				boldPathPriceLatest: testFixture(t, "bold_price_latest.json"),
			})
			conn := newBoldFakeConn(boldFakeDate(t, stored))
			stat := boldGoldPrice{apiBase: server.base(), now: func() time.Time { return c.now }}

			count, err := stat.Import(context.Background(), conn)
			if err != nil {
				t.Fatalf("Import: %v", err)
			}
			if count != c.wantRows {
				t.Errorf("Import вернул %d строк, ожидалось %d", count, c.wantRows)
			}
			if got := server.requested(); len(got) != 1 || got[0] != c.wantPath {
				t.Errorf("запросы прогона: %v, ожидался один %q (режим зависит от момента, а не от пояса)", got, c.wantPath)
			}
		})
	}
}

// TestBoldPriceImportNoopWhenUpToDate: дата источника не новее сохранённого максимума — 0 строк,
// ни одной ошибки, и данных не касаемся: ни DDL таблицы цены, ни батча INSERT в неё. Соединение
// трогается чтением максимума площадки и публикацией каталога — на no-op он перепубликовывается
// (Ruling 19), чтобы исправленный текст описания доезжал до агента без новой точки. Источник
// опрашивается одним запросом price-latest. Здесь же живёт проверка «точка новее сохранённого
// максимума»: саботаж «снять условие latestDate.After(stored)» валит именно этот тест (прогон
// переписал бы точку 09.10 поверх уже сохранённой), а не инкрементный: там максимум 08.10, и
// точка дня пишется при любом варианте условия.
func TestBoldPriceImportNoopWhenUpToDate(t *testing.T) {
	server := boldNewTestServer(t, map[string][]byte{boldPathPriceLatest: testFixture(t, "bold_price_latest.json")})
	conn := newBoldFakeConn(boldFakeDate(t, boldPinDate))
	stat := boldPriceTestStat(server)

	count, err := stat.Import(context.Background(), conn)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if count != 0 {
		t.Errorf("Import вернул %d строк, ожидалось 0", count)
	}

	calls := conn.callList()
	if boldTestCallIndex(calls, "queryrow: "+boldPriceStoredMaxQuery) < 0 {
		t.Errorf("no-op прогон обязан прочитать максимум площадки: %v", calls)
	}
	for _, fragment := range []string{
		"exec: CREATE TABLE IF NOT EXISTS " + boldPriceTable,
		"preparebatch: " + boldGoldPricesInsert,
	} {
		if boldTestCallIndex(calls, fragment) >= 0 {
			t.Errorf("no-op прогон тронул данные (%q): %v", fragment, calls)
		}
	}

	// Каталог перепубликован: батч series_catalog (текст INSERT задаёт util.UpsertSeriesCatalog)
	// открыт, содержит одну запись — ряд gold/bold — и отправлен.
	catalog := conn.batchFor(t, "INSERT INTO series_catalog")
	if !catalog.sent {
		t.Error("батч каталога на no-op прогоне не отправлен")
	}
	if len(catalog.rows) != 1 {
		t.Errorf("строк каталога на no-op прогоне: %d, ожидалась 1 (ряд gold/bold)", len(catalog.rows))
	}

	if got := server.requested(); len(got) != 1 || got[0] != boldPathPriceLatest {
		t.Errorf("запросы прогона: %v, ожидался один %q", got, boldPathPriceLatest)
	}
}

// TestBoldPriceCatalogEntry: запись каталога цены (source='gold', series='bold') заполнена и
// называет главное для читателя: единицы, что это фьючерсная цена источника с нераскрытым
// методом и что подменять ею comex_front нельзя. Ряд живёт в таблице цены gold_prices, поэтому
// запись объявлена в пакете gold, а печатает её импортёр цены.
func TestBoldPriceCatalogEntry(t *testing.T) {
	m := gold.BoldPriceSeriesMeta()
	if m.Source != "gold" {
		t.Errorf("source = %q, ожидался gold", m.Source)
	}
	if m.Series != boldPriceVenue {
		t.Errorf("series = %q, ожидалась площадка %q (в gold_prices series == venue)", m.Series, boldPriceVenue)
	}
	if m.Frequency != "D" {
		t.Errorf("frequency = %q, ожидалось D", m.Frequency)
	}
	if !strings.Contains(m.Unit, "USD") {
		t.Errorf("unit = %q, ожидалась единица в USD", m.Unit)
	}
	if m.Title == "" || m.Origin == "" || m.Description == "" {
		t.Errorf("пустое поле записи каталога: %+v", m)
	}
	for _, fragment := range []string{"метод", "не раскрыт", "не спот", "comex_front", "bold.report"} {
		if !strings.Contains(m.Description, fragment) {
			t.Errorf("описание не называет %q: %s", fragment, m.Description)
		}
	}
	if !strings.Contains(m.Origin, boldPathPrice) {
		t.Errorf("origin = %q, ожидался эндпоинт %s", m.Origin, boldPathPrice)
	}
}
