package cbr

import (
	"os"
	"testing"
	"time"
)

// Фикстуры формы 101: маленький DBF (заголовок и дескрипторы реального B1.dbf,
// четыре записи Сбера — живая с числовым счётом, с пустым NUM_SC, с групповым
// счётом ITGAP и удалённая), срез страницы ЦБ с тремя архивами 101-*.rar и
// живой RAR выпуска 101-20260901.rar (358 КБ, внутри 082026B1.dbf).
const (
	form101DbfFixture  = "testdata/form101_sber.dbf"
	form101PageFixture = "testdata/form101_page.html"
	form101RarFixture  = "testdata/form101_20260901.rar"
)

func readForm101Fixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("чтение фикстуры %s: %v", name, err)
	}
	return data
}

// TestParseForm101DBF — поля, кодировка CP866 и числа на маленькой DBF-фикстуре.
// Удалённая запись и строка с пустым NUM_SC не должны попасть в разбор: первая
// помечена '*' (dBase III), вторая не является остатком по счёту.
func TestParseForm101DBF(t *testing.T) {
	rows, err := parseForm101DBF(readForm101Fixture(t, form101DbfFixture))
	if err != nil {
		t.Fatalf("parseForm101DBF: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("строк %d, want 2 (живая 501 + группа ITGAP; удалённая и пустой NUM_SC пропущены): %+v", len(rows), rows)
	}

	sber := rows[0]
	if sber.Regnum != 1481 {
		t.Errorf("REGN = %d, want 1481", sber.Regnum)
	}
	if sber.Account != "501" {
		t.Errorf("NUM_SC = %q, want 501 (NUL-дополнение поля снято)", sber.Account)
	}
	if sber.Plan != "А" {
		t.Errorf("PLAN = %q, want А (CP866)", sber.Plan)
	}
	if sber.ActivePassive != "2" {
		t.Errorf("A_P = %q, want 2", sber.ActivePassive)
	}
	if sber.BalanceTotal != 18001182 {
		t.Errorf("VITG = %v, want 18001182", sber.BalanceTotal)
	}

	group := rows[1]
	if group.Account != "ITGAP" {
		t.Errorf("NUM_SC второй строки = %q, want ITGAP (группа счетов)", group.Account)
	}
	if group.Plan != "Б" {
		t.Errorf("PLAN второй строки = %q, want Б (CP866)", group.Plan)
	}
	if group.BalanceTotal != 0 {
		t.Errorf("VITG второй строки = %v, want 0", group.BalanceTotal)
	}

	for _, row := range rows {
		if row.Account == "408" {
			t.Errorf("удалённая запись NUM_SC=408 попала в разбор: %+v", row)
		}
	}
}

// TestParseForm101DBFTruncated — обрыв файла внутри записи обязан быть ошибкой
// с номером записи, а не молчаливым «строк меньше».
func TestParseForm101DBFTruncated(t *testing.T) {
	data := readForm101Fixture(t, form101DbfFixture)
	// Отрезаем хвост последней записи: record count в заголовке её всё ещё ждёт.
	if _, err := parseForm101DBF(data[:len(data)-10]); err == nil {
		t.Fatal("parseForm101DBF на обрезанном файле вернул успех, want ошибку обрыва записи")
	}
}

// TestLatestForm101Link — выбор максимальной даты из нескольких 101-*.rar, а не
// последней ссылки страницы (в фикстуре свежайшая стоит в середине), и абсолютный
// URL для util.GetFile. Ноль ссылок — ошибка, а не пустой результат.
func TestLatestForm101Link(t *testing.T) {
	date, link, err := latestForm101Link(readForm101Fixture(t, form101PageFixture))
	if err != nil {
		t.Fatalf("latestForm101Link: %v", err)
	}
	wantDate := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	if !date.Equal(wantDate) {
		t.Errorf("дата = %s, want %s", date.Format(form101DayLayout), wantDate.Format(form101DayLayout))
	}
	wantLink := cbrUrl + "/vfs/credit/forms/101-20260901.rar"
	if link != wantLink {
		t.Errorf("ссылка = %q, want %q", link, wantLink)
	}

	if _, _, err = latestForm101Link([]byte("<html><body>нет ссылок</body></html>")); err == nil {
		t.Fatal("latestForm101Link без ссылок вернул успех, want ошибку смены разметки")
	}
}

// TestExtractForm101B1 — живой RAR выпуска 20260901: извлекается 082026B1.dbf,
// разбор даёт 25 464 записи, из них 194 строки Сбера (REGN=1481) — те же числа,
// что в спеке §2. Фикстура вендорская; при смене выпуска числа обновляются вместе
// с ней, а не подгоняются кодом.
func TestExtractForm101B1(t *testing.T) {
	dbf, err := extractForm101B1(readForm101Fixture(t, form101RarFixture))
	if err != nil {
		t.Fatalf("extractForm101B1: %v", err)
	}
	if len(dbf) == 0 {
		t.Fatal("извлечённый B1.dbf пуст")
	}
	rows, err := parseForm101DBF(dbf)
	if err != nil {
		t.Fatalf("parseForm101DBF(RAR): %v", err)
	}
	if len(rows) != 25464 {
		t.Errorf("строк в B1.dbf = %d, want 25464", len(rows))
	}
	sber := 0
	for _, row := range rows {
		if row.Regnum == 1481 {
			sber++
		}
	}
	if sber != 194 {
		t.Errorf("строк Сбера (REGN=1481) = %d, want 194", sber)
	}
}

// TestExtractForm101B1NoEntry — архив без *B1.dbf — ошибка errForm101NoB1,
// а не пустой срез: это смена состава архива.
func TestExtractForm101B1NoEntry(t *testing.T) {
	if _, err := extractForm101B1([]byte("не RAR")); err == nil {
		t.Fatal("extractForm101B1 на мусоре вернул успех, want ошибку")
	}
}

// TestForm101Newer — гейт по дате без ClickHouse: импорт нужен только когда
// отчётная дата строго новее записанной. Пустая таблица (нулевая дата) — всегда
// новее, повтор той же даты — no-op.
func TestForm101Newer(t *testing.T) {
	day := func(year int, month time.Month, d int) time.Time {
		return time.Date(year, month, d, 0, 0, 0, 0, time.UTC)
	}
	latest := day(2026, time.September, 1)
	cases := []struct {
		name  string
		known time.Time
		want  bool
	}{
		{"пустая таблица", time.Time{}, true},
		{"нулевая дата ClickHouse", day(1970, time.January, 1), true},
		{"предыдущий выпуск", day(2026, time.August, 1), true},
		{"та же дата", day(2026, time.September, 1), false},
		{"будущая дата в таблице", day(2026, time.October, 1), false},
	}
	for _, tc := range cases {
		if got := form101Newer(latest, tc.known); got != tc.want {
			t.Errorf("%s: form101Newer(%s, %s) = %v, want %v", tc.name,
				latest.Format(form101DayLayout), tc.known.Format(form101DayLayout), got, tc.want)
		}
	}
}
