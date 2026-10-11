package util

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// Happy path: 200 + валидный JSON → поля заполнены, ошибки нет. Заодно пиним
// User-Agent: хелпер обязан слать randomUA, как GetFile/GetCSV (конвенция проекта,
// правило 4 AGENTS.md). Обратного утверждения («источник без заголовка отдаёт 403»)
// здесь быть не должно: проверено 2026-10-12 — fscdn.gold.org отвечает 200 и без
// User-Agent, и с дефолтным curl/8. Проверка живёт в хендлере: он выполняет запрос,
// а t.Errorf безопасен из чужой горутины (t.Fatal там же уронил бы не тот фрейм).
func TestGetJSONDecodesBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("User-Agent") == "" {
			t.Error("запрос ушёл без User-Agent — хелпер обязан слать randomUA, как GetFile/GetCSV")
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
// ошибка разбора скрыла бы настоящую причину отказа. В свою ошибку статуса попадают
// код и тело (по нему 403 WAF отличим от 403 «нет ключа»), но не URL: у будущих
// вызывающих в query бывает ключ, а ошибка уезжает в лог (урок eia) — контекст
// добавляет вызывающий. Проверка ниже покрывает только ветку статуса: транспортная
// ошибка приходит из net/http как *url.Error и URL содержит, её маскирует вызывающий
// (в eia — maskKey).
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

// Дедлайн — не украшение: фолбэк импортёра срабатывает на ошибку, а зависший ответ
// сервера без таймаута ошибки не даёт, и `make import STAT=wgc_etf_flows` повис бы
// вместо ухода на локальную выгрузку. Подменяем jsonTimeout на короткий (переменная,
// а не константа — ровно ради этого), хендлер держит запрос дольше: ждём ошибку
// дедлайна, а не тишину.
func TestGetJSONTimeoutFails(t *testing.T) {
	const handlerDelay = 2 * time.Second
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		// select, а не голый time.Sleep: на разрыв клиента хендлер завершается сам,
		// и server.Close() в defer не ждёт полные 2 s впустую.
		select {
		case <-r.Context().Done():
		case <-time.After(handlerDelay):
		}
	}))
	defer server.Close()

	old := jsonTimeout
	jsonTimeout = 50 * time.Millisecond
	t.Cleanup(func() { jsonTimeout = old })

	var out struct {
		Value int `json:"value"`
	}
	err := GetJSON(server.URL, &out)
	if err == nil {
		t.Fatal("зависший ответ должен падать по дедлайну, а не ждать вечно")
	}
	// Именно ошибка дедлайна, а не любая другая: на неё и рассчитывает фолбэк —
	// иначе «получили ошибку» ничего не говорит о причине.
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("ошибка %v не является ошибкой дедлайна (context.DeadlineExceeded)", err)
	}
}

// errorSnippet — то, что дежурный увидит в логе при отказе API, поэтому важны все
// ветки: тело должно остаться узнаваемым, но коротким и одной строкой (многострочный
// HTML разорвал бы запись лога), а пробельное тело — не превратиться в пустоту после
// двоеточия.
func TestErrorSnippet(t *testing.T) {
	longASCII := strings.Repeat("a", 1000)
	longUTF8 := strings.Repeat("тело-WAF-", 200)
	tests := []struct {
		name          string
		body          string
		wantExact     string
		wantHas       string
		wantMaxBytes  int
		wantValidUTF8 bool
	}{
		{
			name:      "короткое тело проходит целиком",
			body:      `<html>blocked</html>`,
			wantExact: `<html>blocked</html>`,
		},
		{
			name:         "длинное тело обрезается до лимита",
			body:         longASCII,
			wantHas:      longASCII[:32],
			wantMaxBytes: maxErrorSnippet,
		},
		{
			name:          "обрезка многобайтового тела не рвёт руну",
			body:          longUTF8,
			wantHas:       "тело-WAF-",
			wantMaxBytes:  maxErrorSnippet,
			wantValidUTF8: true,
		},
		{
			name:      "переводы строк ужимаются в одну строку",
			body:      "line1\nline2\r\nline3",
			wantExact: "line1 line2 line3",
		},
		{
			name:      "тело из пробелов даёт заглушку",
			body:      " \n\t\r\n ",
			wantExact: emptyBodySnippet,
		},
		{
			name:      "пустое тело даёт заглушку",
			body:      "",
			wantExact: emptyBodySnippet,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := errorSnippet([]byte(tt.body))
			if tt.wantExact != "" && got != tt.wantExact {
				t.Errorf("errorSnippet() = %q, ожидалось %q", got, tt.wantExact)
			}
			if tt.wantHas != "" && !strings.Contains(got, tt.wantHas) {
				t.Errorf("errorSnippet() = %q, не содержит начала тела %q", got, tt.wantHas)
			}
			if tt.wantMaxBytes > 0 && len(got) > tt.wantMaxBytes {
				t.Errorf("errorSnippet() = %d байт, ожидалось не больше %d", len(got), tt.wantMaxBytes)
			}
			if tt.wantValidUTF8 && !utf8.ValidString(got) {
				t.Errorf("errorSnippet() = %q — не валидный UTF-8, обрезка разрезала руну", got)
			}
			if strings.ContainsAny(got, "\n\r") {
				t.Errorf("errorSnippet() = %q — не одна строка", got)
			}
		})
	}
}
