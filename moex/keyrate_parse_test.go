package moex

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"
)

// issFixture читает фикстуру ISS (moex/testdata/<name>) как набор блоков по именам — так же,
// как ответ ISS разбирает getBlocks.
func issFixture(t *testing.T, name string) map[string]issBlock {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	var blocks map[string]issBlock
	if err := json.Unmarshal(raw, &blocks); err != nil {
		t.Fatalf("decode fixture %s: %v", name, err)
	}
	return blocks
}

// issFixtureBlock — один блок фикстуры; отсутствие блока — ошибка теста, а не нулевой блок.
func issFixtureBlock(t *testing.T, name, block string) issBlock {
	t.Helper()
	b, ok := issFixture(t, name)[block]
	if !ok {
		t.Fatalf("fixture %s has no block %q", name, block)
	}
	return b
}

// moexDay — дата в UTC: так её возвращают парсеры пакета (fmt moexDate).
func moexDay(s string) time.Time {
	d, _ := time.Parse(moexDate, s)
	return d
}

// descriptionBlock собирает блок description ISS (пары name/value) из пар ключ-значение.
func descriptionBlock(pairs ...[2]string) issBlock {
	block := issBlock{Columns: []string{"name", "title", "value", "type", "sort_order", "is_hidden", "precision"}}
	for _, p := range pairs {
		block.Data = append(block.Data, []any{p[0], "", p[1], "string", 1.0, 0.0, nil})
	}
	return block
}

func sortedCopy(s []string) []string {
	out := append([]string(nil), s...)
	slices.Sort(out)
	return out
}

func TestParseKeyrateSecids(t *testing.T) {
	block := issFixtureBlock(t, "keyrate_securities.json", "securities")
	got, err := parseKeyrateSecids(block)
	if err != nil {
		t.Fatalf("parseKeyrateSecids: %v", err)
	}
	// Порядок строк задаёт ISS (в зафиксированной фикстуре — KKG7, KKV6, KKZ6), и он не
	// контрактный: импортёр обрабатывает серии независимо. Сравниваем состав серий.
	want := []string{"KKV6", "KKZ6", "KKG7"}
	if !slices.Equal(sortedCopy(got), sortedCopy(want)) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for _, secid := range got {
		if secid == "KEYRATE" || secid == "KKV6KKZ6" || secid == "KKZ6KKG7" {
			t.Errorf("secid %s must be filtered out (index or calendar spread)", secid)
		}
	}
}

func TestParseKeyrateSecidsNoColumns(t *testing.T) {
	block := issBlock{
		Columns: []string{"secid", "is_traded"},
		Data:    [][]any{{"KKV6", 1.0}},
	}
	if _, err := parseKeyrateSecids(block); err == nil {
		t.Fatal("expected error for block without primary_boardid")
	}
}

func TestParseKeyrateSecidsFilters(t *testing.T) {
	// Правило отбора серии (дизайн, решение 3): primary_boardid == RFUD, код начинается с KK,
	// is_traded == 1 (null у ISS значит «сведений о торгах нет» — серия не торгуется).
	block := issBlock{
		Columns: []string{"secid", "primary_boardid", "is_traded"},
		Data: [][]any{
			{"KKV6", "RFUD", 1.0},
			{"KEYRATE", "RTSI", 1.0},  // индекс ключевой ставки — не серия фьючерса
			{"KKV6KKZ6", "RFUD", 0.0}, // календарный спред: is_traded=0
			{"KKZ6KKG7", "RFUD", nil}, // календарный спред: is_traded отсутствует
			{"ABV6", "RFUD", 1.0},     // другая серия срочного рынка, не KEYRATE
			{"KKZ6", "RTSI", 1.0},     // серия не на площадке RFUD
			{"KKG7", "RFUD", "1"},     // неожиданный тип is_traded — ошибка
		},
	}
	if _, err := parseKeyrateSecids(block); err == nil {
		t.Fatal("expected error for non-numeric is_traded")
	}
	block.Data = block.Data[:len(block.Data)-1]
	got, err := parseKeyrateSecids(block)
	if err != nil {
		t.Fatalf("parseKeyrateSecids: %v", err)
	}
	if want := []string{"KKV6"}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestParseKeyrateContract(t *testing.T) {
	block := issFixtureBlock(t, "keyrate_description.json", "description")
	got, err := parseKeyrateContract(block)
	if err != nil {
		t.Fatalf("parseKeyrateContract: %v", err)
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
}

func TestParseKeyrateContractMissingKey(t *testing.T) {
	// Молчаливый zero time недопустим: без LSTDELDATE паспорт серии неполон.
	block := descriptionBlock(
		[2]string{"SECID", "KKV6"},
		[2]string{"SHORTNAME", "KEYRATE-10.26"},
		[2]string{"FRSTTRADE", "2026-09-29"},
		[2]string{"LSTTRADE", "2026-10-23"},
	)
	if _, err := parseKeyrateContract(block); err == nil {
		t.Fatal("expected error for description without LSTDELDATE")
	}
}

func TestParseKeyrateContractEmptyExpirationDate(t *testing.T) {
	// LSTDELDATE — дата экспирации (= дата заседания ЦБ); пустое значение откатывается на LSTTRADE.
	block := descriptionBlock(
		[2]string{"SECID", "KKV6"},
		[2]string{"SHORTNAME", "KEYRATE-10.26"},
		[2]string{"FRSTTRADE", "2026-09-29"},
		[2]string{"LSTTRADE", "2026-10-22"},
		[2]string{"LSTDELDATE", ""},
	)
	got, err := parseKeyrateContract(block)
	if err != nil {
		t.Fatalf("parseKeyrateContract: %v", err)
	}
	if !got.lastTrade.Equal(moexDay("2026-10-22")) {
		t.Fatalf("lastTrade %s, want LSTTRADE 2026-10-22", got.lastTrade.Format(moexDate))
	}
}

func TestParseKeyrateHistory(t *testing.T) {
	block := issFixtureBlock(t, "keyrate_history.json", "history")
	got, err := parseKeyrateHistory(block)
	if err != nil {
		t.Fatalf("parseKeyrateHistory: %v", err)
	}
	want := []keyrateDay{
		{date: moexDay("2026-09-29"), close: 13.94, settle: 13.94, waprice: 13.94, volume: 28, numtrades: 19, openposition: 28},
		{date: moexDay("2026-09-30"), close: 13.94, settle: 13.95, waprice: 13.96, volume: 36, numtrades: 31, openposition: 68},
		{date: moexDay("2026-10-01"), close: 13.96, settle: 13.96, waprice: 13.95, volume: 33, numtrades: 10, openposition: 116},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestParseKeyrateHistoryIgnoresLast(t *testing.T) {
	block := issFixtureBlock(t, "keyrate_history.json", "history")
	if columnIndex(block.Columns, "LAST") >= 0 {
		t.Fatal("fixture must not contain a LAST column: the parser must not rely on it")
	}
	plain, err := parseKeyrateHistory(block)
	if err != nil {
		t.Fatalf("parseKeyrateHistory: %v", err)
	}
	// Добавляем LAST=0, как ISS отдаёт день без сделок: цена дня — SETTLEPRICE, LAST не читается.
	withLast := issBlock{Columns: append(append([]string(nil), block.Columns...), "LAST")}
	for _, row := range block.Data {
		withLast.Data = append(withLast.Data, append(append([]any(nil), row...), 0.0))
	}
	got, err := parseKeyrateHistory(withLast)
	if err != nil {
		t.Fatalf("parseKeyrateHistory with LAST: %v", err)
	}
	if !reflect.DeepEqual(got, plain) {
		t.Fatalf("LAST changed the parse result: got %+v, want %+v", got, plain)
	}
}

func TestParseHistoryTotal(t *testing.T) {
	block := issFixtureBlock(t, "keyrate_history.json", "history.cursor")
	got, err := parseHistoryTotal(block)
	if err != nil {
		t.Fatalf("parseHistoryTotal: %v", err)
	}
	if got != 10 {
		t.Fatalf("got %d, want 10", got)
	}
}

func TestParseHistoryTotalNoColumns(t *testing.T) {
	block := issBlock{Columns: []string{"INDEX", "PAGESIZE"}, Data: [][]any{{0.0, 100.0}}}
	if _, err := parseHistoryTotal(block); err == nil {
		t.Fatal("expected error for cursor block without TOTAL")
	}
}

func TestValidSecid(t *testing.T) {
	tests := []struct {
		secid string
		want  bool
	}{
		{"KKV6", true},
		{"KKV6KKZ6", true},
		{"KKG7", true},
		{"kkv6", false},
		{"KK V6", false},
		{"", false},
		{"KK", false},
		{"KKV6-", false},
	}
	for _, tt := range tests {
		if got := validSecid(tt.secid); got != tt.want {
			t.Errorf("validSecid(%q) = %v, want %v", tt.secid, got, tt.want)
		}
	}
}
