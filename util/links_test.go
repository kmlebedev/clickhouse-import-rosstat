package util

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Happy path: 200 + HTML → тело возвращается целиком, ошибки нет. Заодно пиним
// User-Agent: GetPage обязан ехать через util.HttpClient, как GetFile/GetJSON
// (конвенция проекта, стандарт §4.1).
func TestGetPageReturnsBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("User-Agent") == "" {
			t.Error("запрос ушёл без User-Agent — хелпер обязан слать randomUA, как GetFile/GetJSON")
		}
		_, _ = io.WriteString(w, `<a href="/x.xlsx">x</a>`)
	}))
	defer server.Close()

	page, err := GetPage(server.URL)
	if err != nil {
		t.Fatalf("GetPage 200: %v", err)
	}
	if string(page) != `<a href="/x.xlsx">x</a>` {
		t.Errorf("GetPage() = %q, ожидалось тело страницы без изменений", page)
	}
}

// Пустое тело со статусом 200 — ошибка (GetFile уже так делает): пустой ответ
// неотличим от «данных нет», и молчаливый ноль оставил бы ряды без обновления.
func TestGetPageEmptyBodyFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()

	if _, err := GetPage(server.URL); err == nil {
		t.Fatal("пустое тело должно давать ошибку, а не тихий ноль")
	}
}

// TestFindLinkReturnsHref: href отдаётся как есть (относительный в этой проверке —
// префикс домена навешивает вызывающий).
func TestFindLinkReturnsHref(t *testing.T) {
	page := []byte(`<html><body><a href="/storage/x.xlsx">x</a></body></html>`)
	href, err := FindLink(page, `a[href$=".xlsx"]`)
	if err != nil {
		t.Fatalf("FindLink: %v", err)
	}
	if href != "/storage/x.xlsx" {
		t.Errorf("FindLink() = %q, ожидалось %q", href, "/storage/x.xlsx")
	}
}

// Совпадений нет — ошибка, и селектор в её тексте: дежурный по логу должен видеть,
// чем именно страница не подошла, не открывая код. %q в сообщении экранирует кавычки
// селектора, поэтому сверяем его литеральную часть.
func TestFindLinkMissingSelectorFails(t *testing.T) {
	page := []byte(`<html><body><a href="/a.pdf">x</a></body></html>`)
	href, err := FindLink(page, `a[href$=".xlsx"]`)
	if err == nil {
		t.Fatalf("ожидалась ошибка, получен href %q", href)
	}
	if !strings.Contains(err.Error(), `a[href$=`) {
		t.Errorf("ошибка %q не называет селектор", err)
	}
}

// Битый HTML — ошибка разбора, а не молчаливый ноль: иначе импортёр счёл бы
// страницу без ссылки и упал бы с менее понятным сообщением.
func TestFindLinkBadHTMLFails(t *testing.T) {
	if _, err := FindLink([]byte("<a href="), `a`); err == nil {
		t.Fatal("битый HTML должен давать ошибку разбора, а не тихий ноль")
	}
}
