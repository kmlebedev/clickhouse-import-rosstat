package moex

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// useMoexServer подменяет moexBaseUrl тестовым сервером (сеть в тестах — только httptest)
// и восстанавливает его по завершении теста.
func useMoexServer(t *testing.T, handler http.HandlerFunc) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	prev := moexBaseUrl
	t.Cleanup(func() { moexBaseUrl = prev })
	moexBaseUrl = server.URL
}

// writeIssBlocks отдаёт блоки ISS одним JSON-ответом.
func writeIssBlocks(t *testing.T, w http.ResponseWriter, blocks map[string]issBlock) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(blocks); err != nil {
		t.Errorf("write iss response: %v", err)
	}
}

func TestFetchKeyrateSecids(t *testing.T) {
	fixture := issFixture(t, "keyrate_securities.json")
	var paths []string
	useMoexServer(t, func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.String())
		writeIssBlocks(t, w, fixture)
	})

	got, err := fetchKeyrateSecids(context.Background())
	if err != nil {
		t.Fatalf("fetchKeyrateSecids: %v", err)
	}
	want := []string{"KKV6", "KKZ6", "KKG7"}
	if !slices.Equal(sortedCopy(got), sortedCopy(want)) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if want := []string{"/iss/securities.json?q=KEYRATE&iss.meta=off"}; !slices.Equal(paths, want) {
		t.Fatalf("unexpected requests %v, want %v", paths, want)
	}
}

func TestFetchKeyrateContract(t *testing.T) {
	fixture := issFixture(t, "keyrate_description.json")
	var paths []string
	useMoexServer(t, func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.String())
		writeIssBlocks(t, w, fixture)
	})

	got, err := fetchKeyrateContract(context.Background(), "KKV6")
	if err != nil {
		t.Fatalf("fetchKeyrateContract: %v", err)
	}
	want := keyrateContract{
		secid:      "KKV6",
		seriesName: "KEYRATE-10.26",
		firstTrade: moexDay("2026-09-29"),
		lastTrade:  moexDay("2026-10-23"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	if want := []string{"/iss/securities/KKV6.json?iss.meta=off"}; !slices.Equal(paths, want) {
		t.Fatalf("unexpected requests %v, want %v", paths, want)
	}
}

func TestFetchKeyrateContractHTTPError(t *testing.T) {
	useMoexServer(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})

	if _, err := fetchKeyrateContract(context.Background(), "KKV6"); err == nil {
		t.Fatal("expected error on http 500")
	} else if !strings.Contains(err.Error(), "500") {
		t.Fatalf("error text does not name the status: %v", err)
	}
}

func TestFetchKeyrateHistoryPaginates(t *testing.T) {
	fixture := issFixture(t, "keyrate_history.json")
	columns := fixture["history"].Columns
	rows := fixture["history"].Data
	// Стенд отдаёт страницу короче TOTAL (=3): PAGESIZE у ISS (100) параметром не понижается,
	// поэтому вторая страница достижима только по TOTAL из history.cursor — так пагинация и проверяется.
	cursor := issBlock{
		Columns: fixture["history.cursor"].Columns,
		Data:    [][]any{{0.0, 3.0, float64(moexPageLimit)}},
	}
	var paths []string
	useMoexServer(t, func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.String())
		switch r.URL.Query().Get("start") {
		case "0":
			writeIssBlocks(t, w, map[string]issBlock{"history": {Columns: columns, Data: rows[:2]}, "history.cursor": cursor})
		case "100":
			writeIssBlocks(t, w, map[string]issBlock{"history": {Columns: columns, Data: rows[2:]}, "history.cursor": cursor})
		default:
			t.Errorf("unexpected start %q", r.URL.Query().Get("start"))
			writeIssBlocks(t, w, map[string]issBlock{"history": {Columns: columns}, "history.cursor": cursor})
		}
	})

	from, _ := time.Parse(moexDate, "2026-09-29")
	days, err := fetchKeyrateHistory(context.Background(), "KKV6", from)
	if err != nil {
		t.Fatalf("fetchKeyrateHistory: %v", err)
	}
	want := []keyrateDay{
		{date: moexDay("2026-09-29"), close: 13.94, settle: 13.94, waprice: 13.94, volume: 28, numtrades: 19, openposition: 28},
		{date: moexDay("2026-09-30"), close: 13.94, settle: 13.95, waprice: 13.96, volume: 36, numtrades: 31, openposition: 68},
		{date: moexDay("2026-10-01"), close: 13.96, settle: 13.96, waprice: 13.95, volume: 33, numtrades: 10, openposition: 116},
	}
	if !reflect.DeepEqual(days, want) {
		t.Fatalf("got %+v, want %+v", days, want)
	}
	if len(paths) != 2 {
		t.Fatalf("got %d requests %v, want 2 (start=0, start=100)", len(paths), paths)
	}
	if !strings.HasPrefix(paths[0], "/iss/history/engines/futures/markets/forts/securities/KKV6.json?") ||
		!strings.Contains(paths[0], "from=2026-09-29") || !strings.Contains(paths[0], "start=0") {
		t.Fatalf("unexpected first request %q", paths[0])
	}
	if !strings.Contains(paths[1], "start=100") {
		t.Fatalf("second page must be requested with start=%d, got %q", moexPageLimit, paths[1])
	}
}

func TestFetchKeyrateHistoryEmpty(t *testing.T) {
	fixture := issFixture(t, "keyrate_history.json")
	columns := fixture["history"].Columns
	// TOTAL=0: дней по серии нет — запрос один, без второй страницы и без зацикливания.
	cursor := issBlock{
		Columns: fixture["history.cursor"].Columns,
		Data:    [][]any{{0.0, 0.0, float64(moexPageLimit)}},
	}
	var paths []string
	useMoexServer(t, func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.String())
		if len(paths) > 3 {
			// Зацикливание обязано валить тест падением, а не висеть до таймаута go test.
			t.Errorf("fetchKeyrateHistory does not stop on empty page: %v", paths)
			http.Error(w, "too many requests", http.StatusInternalServerError)
			return
		}
		writeIssBlocks(t, w, map[string]issBlock{"history": {Columns: columns}, "history.cursor": cursor})
	})

	from, _ := time.Parse(moexDate, "2026-09-29")
	days, err := fetchKeyrateHistory(context.Background(), "KKV6", from)
	if err != nil {
		t.Fatalf("fetchKeyrateHistory: %v", err)
	}
	if len(days) != 0 {
		t.Fatalf("got %+v, want no days", days)
	}
	if len(paths) != 1 {
		t.Fatalf("got %d requests %v, want 1", len(paths), paths)
	}
}

func TestGetBlocksReturnsAllBlocks(t *testing.T) {
	// Страница history и её курсор приходят одним ответом: оба блока обязан отдать один запрос.
	fixture := issFixture(t, "keyrate_history.json")
	requests := 0
	useMoexServer(t, func(w http.ResponseWriter, r *http.Request) {
		requests++
		writeIssBlocks(t, w, fixture)
	})

	blocks, err := getBlocks(context.Background(), "/iss/history/engines/futures/markets/forts/securities/KKV6.json?iss.meta=off")
	if err != nil {
		t.Fatalf("getBlocks: %v", err)
	}
	if requests != 1 {
		t.Fatalf("got %d requests, want 1", requests)
	}
	if !reflect.DeepEqual(blocks["history"], fixture["history"]) ||
		!reflect.DeepEqual(blocks["history.cursor"], fixture["history.cursor"]) {
		t.Fatalf("unexpected blocks %+v", blocks)
	}
}

func TestGetBlockMissingBlock(t *testing.T) {
	fixture := issFixture(t, "keyrate_history.json")
	useMoexServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeIssBlocks(t, w, fixture)
	})

	_, err := getBlock(context.Background(), "/iss/history/fake/KKV6.json?iss.meta=off", "marketdata")
	if err == nil {
		t.Fatal("expected error for missing block")
	}
	if want := `no block "marketdata" in response`; !strings.Contains(err.Error(), want) {
		t.Fatalf("error text changed: %v", err)
	}
}

func TestKeyrateSince(t *testing.T) {
	contract := keyrateContract{secid: "KKV6", firstTrade: moexDay("2026-09-29")}
	tests := []struct {
		name   string
		stored time.Time
		want   time.Time
	}{
		{"день после сохранённого", moexDay("2026-10-08"), moexDay("2026-10-09")},
		{"пустая таблица — с начала обращения", time.Time{}, moexDay("2026-09-29")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := keyrateSince(contract, tt.stored); !got.Equal(tt.want) {
				t.Fatalf("keyrateSince(stored=%s) = %s, want %s",
					tt.stored.Format(moexDate), got.Format(moexDate), tt.want.Format(moexDate))
			}
		})
	}
}

func TestKeyrateSeriesMetaPerContract(t *testing.T) {
	contracts := []keyrateContract{
		{secid: "KKV6", seriesName: "KEYRATE-10.26", firstTrade: moexDay("2026-09-29"), lastTrade: moexDay("2026-10-23")},
		{secid: "KKZ6", seriesName: "KEYRATE-12.26", firstTrade: moexDay("2026-09-29"), lastTrade: moexDay("2026-12-18")},
	}
	meta := keyrateSeriesMeta(contracts)
	// Серии меняются каждый квартал: запись заводится на каждую обнаруженную серию, статического списка нет.
	if len(meta) != len(contracts) {
		t.Fatalf("got %d записей каталога, want %d (по одной на серию)", len(meta), len(contracts))
	}
	fields := map[string]func(util.SeriesMeta) string{
		"Title":       func(m util.SeriesMeta) string { return m.Title },
		"Unit":        func(m util.SeriesMeta) string { return m.Unit },
		"Frequency":   func(m util.SeriesMeta) string { return m.Frequency },
		"Origin":      func(m util.SeriesMeta) string { return m.Origin },
		"Description": func(m util.SeriesMeta) string { return m.Description },
	}
	for i, m := range meta {
		c := contracts[i]
		if m.Source != moexSource || m.Series != c.secid {
			t.Errorf("запись %d: source=%q series=%q, want %q/%q", i, m.Source, m.Series, moexSource, c.secid)
		}
		for name, value := range fields {
			if value(m) == "" {
				t.Errorf("запись %s: поле %s пусто", c.secid, name)
			}
		}
		expiry := c.lastTrade.Format(moexDate)
		if !strings.Contains(m.Origin, expiry) {
			t.Errorf("запись %s: в Origin нет даты экспирации %s: %q", c.secid, expiry, m.Origin)
		}
		if !strings.Contains(m.Description, "Δ = (settle − ставка) × 100 б.п.") {
			t.Errorf("запись %s: в Description нет мнемоники вменённого изменения к заседанию: %q", c.secid, m.Description)
		}
	}
}

func TestKeyrateName(t *testing.T) {
	// Name() — то, что передаётся в CLICKHOUSE_IMPORT_STAT и id шага dagu/moex.yaml.
	if got := (&keyrateFutures{}).Name(); got != keyrateTable {
		t.Fatalf("Name() = %q, want %q", got, keyrateTable)
	}
}

func TestKeyrateFuturesIsRegistered(t *testing.T) {
	// Без записи в chimport.Stats импортёр не запустится ни из dagu/moex.yaml, ни через
	// CLICKHOUSE_IMPORT_STAT=keyrate_futures.
	for _, stat := range chimport.Stats {
		if stat.Name() == keyrateTable {
			return
		}
	}
	t.Fatalf("импортёр %q не зарегистрирован в chimport.Stats", keyrateTable)
}

func TestKeyrateViewContract(t *testing.T) {
	// Витрина читается агентом через MCP: у каждой колонки обязан быть комментарий (правило 11б),
	// а Tables называет все источники — без cbr_key_rate витрина публиковаться не должна.
	want := []string{"date", "secid", "series_name", "decision_date", "settle", "close",
		"volume", "openposition", "expected_change_pp", "expected_change_bp"}
	if len(keyrateView.Columns) != len(want) {
		t.Fatalf("колонок в комментариях %d, want %d", len(keyrateView.Columns), len(want))
	}
	for _, col := range want {
		if comment, ok := keyrateView.Columns[col]; !ok || comment == "" {
			t.Errorf("колонка %s не описана", col)
		}
	}
	if !slices.Contains(keyrateView.Tables, "cbr_key_rate") {
		t.Error("cbr_key_rate отсутствует в Tables: витрина без ряда ставки станет неполной")
	}
	// Спот — скалярный подзапрос (решение контроллера): CROSS JOIN в витрине не нужен.
	if !strings.Contains(keyrateView.Select, "(SELECT argMax(rate, date) FROM cbr_key_rate FINAL)") {
		t.Errorf("спот берётся не скалярным подзапросом:\n%s", keyrateView.Select)
	}
	if strings.Contains(keyrateView.Select, "CROSS JOIN") {
		t.Errorf("в витрине остался CROSS JOIN:\n%s", keyrateView.Select)
	}
	// Алиас ставится ДО FINAL: `FROM keyrate_futures FINAL f` — SYNTAX_ERROR ClickHouse 26.10
	// (проверено живым DESCRIBE), рабочая форма — `AS f FINAL`.
	if !strings.Contains(keyrateView.Select, "FROM "+keyrateTable+" AS f FINAL") ||
		!strings.Contains(keyrateView.Select, "INNER JOIN "+keyrateContractTable+" AS c FINAL") {
		t.Errorf("FINAL должен стоять после алиаса (AS f FINAL / AS c FINAL):\n%s", keyrateView.Select)
	}
}

func TestKeyrateDdlHasNoMinStepColumns(t *testing.T) {
	// MINSTEP/STEPPRICE (0,01 п.п. = 10 ₽ → 1 п.п. = 1 000 ₽) лежат в блоке securities движка forts,
	// а не в блоке description бумаги (живая проба 2026-10-13), поэтому в схему они не заводятся:
	// множитель зафиксирован в комментариях витрины и в описании ряда в series_catalog.
	schema := strings.ToLower(keyrateDdl + keyrateContractDdl)
	for _, column := range []string{"minstep", "stepprice"} {
		if strings.Contains(schema, column) {
			t.Errorf("колонка %s не должна попадать в схему", column)
		}
	}
	for _, want := range []string{"CREATE TABLE IF NOT EXISTS " + keyrateTable, "CREATE TABLE IF NOT EXISTS " + keyrateContractTable, "ENGINE = ReplacingMergeTree"} {
		if !strings.Contains(schema, strings.ToLower(want)) {
			t.Errorf("в DDL нет %q", want)
		}
	}
	if !strings.Contains(keyrateContractDdl, "last_trade Date") {
		t.Error("в паспорте серии нет даты экспирации (last_trade): витрина считает вменённое изменение к заседанию")
	}
}

func TestFetchKeyrateSecidsFiltersInvalid(t *testing.T) {
	// Код серии приходит из тела ответа ISS и подставляется в URL и в SQL-условие инкремента:
	// недопустимые коды обязаны отсекаться на выходе транспорта.
	block := issBlock{
		Columns: []string{"secid", "primary_boardid", "is_traded"},
		Data: [][]any{
			{"KKV6", "RFUD", 1.0},
			{"KKV6; DROP TABLE keyrate_futures; --", "RFUD", 1.0},
			{"KK V6", "RFUD", 1.0},
			{"KKH7", "RFUD", 1.0},
		},
	}
	useMoexServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeIssBlocks(t, w, map[string]issBlock{keyrateSecuritiesBlock: block})
	})

	got, err := fetchKeyrateSecids(context.Background())
	if err != nil {
		t.Fatalf("fetchKeyrateSecids: %v", err)
	}
	if want := []string{"KKV6", "KKH7"}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v (недопустимые коды обязаны отсеиваться)", got, want)
	}
}

func TestKeyrateSecidValidatedBeforeRequestAndSQL(t *testing.T) {
	// Недопустимый secid не должен доходить ни до сети, ни до SQL-условия инкремента.
	const bad = "KKV6'; DROP TABLE keyrate_futures; --"
	useMoexServer(t, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("недопустимый secid дошёл до сети: %s", r.URL.String())
		writeIssBlocks(t, w, map[string]issBlock{})
	})

	if _, err := fetchKeyrateContract(context.Background(), bad); err == nil {
		t.Error("fetchKeyrateContract: ожидалась ошибка на недопустимом secid")
	}
	if _, err := fetchKeyrateHistory(context.Background(), bad, moexDay("2026-09-29")); err == nil {
		t.Error("fetchKeyrateHistory: ожидалась ошибка на недопустимом secid")
	}
	// conn не используется: код проверяется до обращения к базе, поэтому nil здесь безопасен.
	if _, err := keyrateStoredDate(context.Background(), nil, keyrateContract{secid: bad}); err == nil {
		t.Error("keyrateStoredDate: ожидалась ошибка на недопустимом secid")
	}
}
