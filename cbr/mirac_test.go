package cbr

import (
	"os"
	"testing"
)

// cbrMiracFixtureBody читает сохранённую страницу формы MIACR: обе фикстуры — живые
// ответы cbr.ru с Currency=1 (только рубли), скачаны 2026-11-14.
func cbrMiracFixtureBody(t *testing.T, path string) []byte {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("чтение фикстуры %s: %v", path, err)
	}
	return body
}

// cbrMiracIndex — точки в разрезе пары (инструмент, тенор) и дат.
func cbrMiracIndex(points []cbrMiracPoint) (map[string]float64, map[string]struct{}, map[string]struct{}) {
	byKey := map[string]float64{}
	dates := map[string]struct{}{}
	names := map[string]struct{}{}
	for _, point := range points {
		byKey[point.name+"/"+point.tenor+"/"+point.date.Format("2006-01-02")] = point.value
		dates[point.date.Format("2006-01-02")] = struct{}{}
		names[point.name] = struct{}{}
	}
	return byKey, dates, names
}

// TestCbrMiracOldFixture: окно 13.07.2000..31.08.2000 — исторический минимум формы.
// За июль 2000 строк нет (первая точка MIACR — 01.08.2000), поэтому в окне 23 торговых
// дня августа. В 2000-м у MIACR-IG и MIACR-B данных нет (их таблицы пусты), и точки даёт
// только MIACR.
func TestCbrMiracOldFixture(t *testing.T) {
	points, err := cbrMiracRows(cbrMiracFixtureBody(t, "testdata/mirac_2000_08.html"))
	if err != nil {
		t.Fatalf("разбор формы: %v", err)
	}
	byKey, dates, names := cbrMiracIndex(points)
	if len(dates) != 23 {
		t.Errorf("торговых дней %d, want 23", len(dates))
	}
	if len(names) != 1 {
		t.Errorf("инструментов %d (%v), want 1: в 2000-м MIACR-IG/B пусты", len(names), names)
	}
	spot := map[string]float64{
		"MIACR/1d/2000-08-31":     8.54,
		"MIACR/2-7d/2000-08-31":   12.00,
		"MIACR/8-30d/2000-08-31":  9.19,
		"MIACR/31-90d/2000-08-31": 12.32,
	}
	for key, want := range spot {
		got, ok := byKey[key]
		if !ok {
			t.Errorf("точки %q нет", key)
			continue
		}
		if got != want {
			t.Errorf("%s = %v, want %v", key, got, want)
		}
	}
	// Прочерк в форме — отсутствие значения, а не ноль: 91-180 и 181d-1y 31.08.2000 пусты.
	for _, tenor := range []string{"91-180d", "181d-1y"} {
		if _, ok := byKey["MIACR/"+tenor+"/2000-08-31"]; ok {
			t.Errorf("тенор %s за 2000-08-31 записан, хотя в форме прочерк", tenor)
		}
	}
}

// TestCbrMiracModernFixture: окно января 2026 — все три инструмента отдают значение
// однодневной ставки (15,83 / 15,82 / 15,89), остальные сроки в форме закрыты прочерком.
// Так проверяется и привязка инструмента к подписи таблицы: перестановка IG и B ловится
// значениями.
func TestCbrMiracModernFixture(t *testing.T) {
	points, err := cbrMiracRows(cbrMiracFixtureBody(t, "testdata/mirac_2026_01.html"))
	if err != nil {
		t.Fatalf("разбор формы: %v", err)
	}
	byKey, dates, names := cbrMiracIndex(points)
	if len(dates) != 15 {
		t.Errorf("торговых дней %d, want 15", len(dates))
	}
	if len(names) != 3 {
		t.Errorf("инструментов %d (%v), want 3", len(names), names)
	}
	spot := map[string]float64{
		"MIACR/1d/2026-01-30":    15.83,
		"MIACR-IG/1d/2026-01-30": 15.82,
		"MIACR-B/1d/2026-01-30":  15.89,
	}
	for key, want := range spot {
		got, ok := byKey[key]
		if !ok {
			t.Errorf("точки %q нет", key)
			continue
		}
		if got != want {
			t.Errorf("%s = %v, want %v", key, got, want)
		}
	}
}

// TestCbrMiracNoTable: страница формы без таблиц (пустой диапазон) — не ошибка, а nil;
// чужая страница без таблицы — ошибка, иначе WAF-заглушка выглядела бы «нет данных».
func TestCbrMiracNoTable(t *testing.T) {
	if points, err := cbrMiracRows([]byte("<html><body><input name=\"UniDbQuery.Posted\" value=\"True\"></body></html>")); err != nil || points != nil {
		t.Errorf("страница формы без таблиц: points=%v err=%v, want nil/nil", points, err)
	}
	if _, err := cbrMiracRows([]byte("<html><body>captcha</body></html>")); err == nil {
		t.Error("чужая страница без таблицы разобрана без ошибки")
	}
}
