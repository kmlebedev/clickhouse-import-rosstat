package util

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Happy path: 200 + валидный JSON → поля заполнены, ошибки нет. Заодно пиним
// User-Agent: fscdn.gold.org (как и rosstat) отвечает 403 ботовым запросам без
// заголовка, поэтому хелпер обязан слать randomUA так же, как GetFile/GetCSV.
// Проверка живёт в хендлере: он выполняет запрос, а t.Errorf безопасен из чужой
// горутины (t.Fatal там же уронил бы не тот фрейм).
func TestGetJSONDecodesBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("User-Agent") == "" {
			t.Error("запрос ушёл без User-Agent — WGC отвечает на такое 403")
		}
		_, _ = io.WriteString(w, `{"value":42}`)
	}))
	defer server.Close()

	var out struct {
		Value int `json:"value"`
	}
	if err := GetJSON(server.URL, &out); err != nil {
		t.Fatalf("GetJSON 200: %v", err)
	}
	if out.Value != 42 {
		t.Errorf("out.Value = %d, ожидалось 42", out.Value)
	}
}

// Не-200 обязан падать до разбора тела: HTML-заглушка WAF — не «пустой JSON», и
// ошибка разбора скрыла бы настоящую причину отказа. В текст ошибки попадают код
// и тело (по нему 403 WAF отличим от 403 «нет ключа»), но не URL: у будущих
// вызывающих в query бывает ключ, а ошибка уезжает в лог (урок eia) — контекст
// добавляет вызывающий.
func TestGetJSONNonOKStatusFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, "<html>blocked</html>")
	}))
	defer server.Close()

	var out struct {
		Value int `json:"value"`
	}
	err := GetJSON(server.URL, &out)
	if err == nil {
		t.Fatal("403 должен давать ошибку, а не тихий ноль с зелёным dagu")
	}
	for _, want := range []string{"403", "blocked"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ошибка %q не содержит %q", err, want)
		}
	}
	if strings.Contains(err.Error(), server.URL) {
		t.Errorf("ошибка содержит URL %q — ключ из query утёк бы в лог: %v", server.URL, err)
	}
}

// 200 с не-JSON телом (HTML вместо данных) — ошибка разбора, а не молчаливый
// ноль: иначе импортёр счёл бы выгрузку пустой и не ушёл в фолбэк.
func TestGetJSONMalformedBodyFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "not json")
	}))
	defer server.Close()

	var out struct {
		Value int `json:"value"`
	}
	if err := GetJSON(server.URL, &out); err == nil {
		t.Fatal("битый JSON должен давать ошибку, а не тихий ноль")
	}
}

// Пустое тело со статусом 200 — ошибка (как в GetFile): пустой ответ в фолбэк-ветке
// неотличим от «данных нет», и молчаливый ноль оставил бы ряды без обновления.
func TestGetJSONEmptyBodyFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()

	var out struct {
		Value int `json:"value"`
	}
	if err := GetJSON(server.URL, &out); err == nil {
		t.Fatal("пустое тело должно давать ошибку, а не тихий ноль")
	}
}
