package util

import (
	"net/url"
	"os"
	"testing"
	"time"
)

// TestUniDbQueryURL: существующие параметры base сохраняются, Posted/From/To
// проставляются, даты — в формате ЦБ 02.01.2006.
func TestUniDbQueryURL(t *testing.T) {
	const base = "https://www.cbr.ru/hd_base/zcyc_params/?x=1"
	from := time.Date(2003, time.January, 4, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, time.October, 19, 0, 0, 0, 0, time.UTC)

	got, err := UniDbQueryURL(base, from, to)
	if err != nil {
		t.Fatalf("UniDbQueryURL: %v", err)
	}

	u, err := url.Parse(got)
	if err != nil {
		t.Fatalf("разбор результата %q: %v", got, err)
	}
	q := u.Query()
	if v := q.Get("UniDbQuery.Posted"); v != "True" {
		t.Errorf("UniDbQuery.Posted = %q, ожидалось %q", v, "True")
	}
	if v := q.Get("UniDbQuery.From"); v != "04.01.2003" {
		t.Errorf("UniDbQuery.From = %q, ожидалось %q", v, "04.01.2003")
	}
	if v := q.Get("UniDbQuery.To"); v != "19.10.2026" {
		t.Errorf("UniDbQuery.To = %q, ожидалось %q", v, "19.10.2026")
	}
	if v := q.Get("x"); v != "1" {
		t.Errorf("x = %q, ожидалось %q (параметры base должны сохраняться)", v, "1")
	}
}

// Пустой base — ошибка: адрес формы не собирается из нуля, и импортёр должен
// упасть до сетевого запроса, а не уйти на чужой URL.
func TestUniDbQueryURLEmptyBase(t *testing.T) {
	from := time.Date(2003, time.January, 4, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, time.October, 19, 0, 0, 0, 0, time.UTC)

	if _, err := UniDbQueryURL("", from, to); err == nil {
		t.Fatal("пустой base должен давать ошибку")
	}
}

// Валидная страница формы ЦБ с пустым результатом содержит скрытый input
// UniDbQuery.Posted (таблицы при этом может не быть); WAF/error-заглушка — нет.
func TestIsUniDbQueryPage(t *testing.T) {
	body, err := os.ReadFile("testdata/unidbquery.html")
	if err != nil {
		t.Fatalf("чтение фикстуры: %v", err)
	}
	if !IsUniDbQueryPage(body) {
		t.Error("фикстура ЦБ должна распознаваться как страница UniDbQuery")
	}

	if IsUniDbQueryPage([]byte(`<html><body>forbidden</body></html>`)) {
		t.Error("WAF/error-заглушка не должна распознаваться как страница UniDbQuery")
	}
}
