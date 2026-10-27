package util

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"
)

// Happy path: 200 + непустое тело → тело читается целиком, ошибки нет. Заодно пиним
// User-Agent: хелпер обязан слать randomUA (конвенция проекта, правило 4 AGENTS.md).
// Проверка живёт в хендлере: он выполняет запрос, а t.Errorf безопасен из чужой горутины
// (t.Fatal там же уронил бы не тот фрейм). Тест появился вместе с дедлайном: подмена
// запроса на NewRequestWithContext не должна сломать обычное скачивание.
func TestGetFileReturnsBody(t *testing.T) {
	const body = "выгрузка ЦБ"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("User-Agent") == "" {
			t.Error("запрос ушёл без User-Agent — хелпер обязан слать randomUA, как GetXML/GetJSON")
		}
		_, _ = io.WriteString(w, body)
	}))
	defer server.Close()

	reader, err := GetFile(server.URL)
	if err != nil {
		t.Fatalf("GetFile 200: %v", err)
	}
	defer func() { _ = reader.Close() }()
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("чтение тела GetFile: %v", err)
	}
	if string(got) != body {
		t.Errorf("тело %q, ожидалось %q", got, body)
	}
}

// Дедлайн — не украшение: зависший ответ ЦБ без таймаута ошибки не даёт, а GetFile общий
// для всех пакетов, и в цепочке dagu/cbr.yaml его ждёт третий шаг cbr_key_rate
// (overlap_policy: skip — зависание останавливает и десять шагов после него). Подменяем
// fileTimeout на короткий (переменная, а не константа — ровно ради этого), хендлер
// держит запрос дольше: ждём ошибку дедлайна, а не тишину.
func TestGetFileTimeoutFails(t *testing.T) {
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

	old := fileTimeout
	fileTimeout = 50 * time.Millisecond
	t.Cleanup(func() { fileTimeout = old })

	reader, err := GetFile(server.URL)
	if err == nil {
		_ = reader.Close()
		t.Fatal("зависший ответ должен падать по дедлайну, а не ждать вечно")
	}
	// Именно ошибка дедлайна, а не любая другая: на неё рассчитывает вызывающий —
	// иначе «получили ошибку» ничего не говорит о причине.
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("ошибка %v не является ошибкой дедлайна (context.DeadlineExceeded)", err)
	}
}

// GetFileBytes — тот же GetFile, но телом-срезом: байты нужны обеим рукам шаблона сразу —
// разбору (excelize.OpenReader) и винтажному архиву, а скачивать один и тот же файл дважды
// значило бы удвоить и трафик, и время прогона. Проверки те же, что у GetFile: тело целиком
// и User-Agent в запросе (конвенция проекта, правило 4 AGENTS.md).
func TestGetFileBytes(t *testing.T) {
	const body = "выгрузка ЦБ целиком"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("User-Agent") == "" {
			t.Error("запрос ушёл без User-Agent — хелпер обязан слать randomUA, как GetFile")
		}
		_, _ = io.WriteString(w, body)
	}))
	defer server.Close()

	got, err := GetFileBytes(server.URL)
	if err != nil {
		t.Fatalf("GetFileBytes 200: %v", err)
	}
	if string(got) != body {
		t.Errorf("тело %q, ожидалось %q", got, body)
	}
}

// GetFileBytesChecked — тот же GET, но со статусом: нужен источнику, который берёт
// одноразовый URL и повторяет запрос (BI Росстата). GetFileBytes 5xx-заглушку вернул бы
// «успешными» байтами, и ошибка всплыла бы уже в разборе, вне ретрая.
func TestGetFileBytesChecked(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("User-Agent") == "" {
			t.Error("запрос ушёл без User-Agent — хелпер обязан слать randomUA, как GetFile")
		}
		switch r.URL.Path {
		case "/ok":
			_, _ = io.WriteString(w, "a,b\n")
		case "/empty":
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "boom", http.StatusBadGateway)
		}
	}))
	defer server.Close()

	body, err := GetFileBytesChecked(server.URL + "/ok")
	if err != nil {
		t.Fatalf("GetFileBytesChecked 200: %v", err)
	}
	if string(body) != "a,b\n" {
		t.Errorf("тело %q, ожидалось a,b", body)
	}

	if _, err = GetFileBytesChecked(server.URL + "/bad"); err == nil {
		t.Error("ожидалась ошибка на не-2xx")
	} else if !strings.Contains(err.Error(), "502") {
		t.Errorf("ошибка %q не называет статус", err)
	}
	if _, err = GetFileBytesChecked(server.URL + "/empty"); err == nil {
		t.Error("ожидалась ошибка на пустое тело")
	}
}

// GetCSV — форма выгрузки Сбера целиком: ведущий UTF-8 BOM, шапка и значения в кавычках,
// разделитель `;`. BOM обязан сниматься в транспорте: encoding/csv видит его как
// незакавыченное начало поля и падает на первой же кавычке («bare " in non-quoted-field»),
// то есть на валидном файле получается ноль записей — живая выгрузка SberIndex именно такая
// (bank/testdata), и без снятия BOM импортёр молча не пишет ни строки. Пиним и User-Agent:
// конвенция проекта (правило 4 AGENTS.md).
func TestGetCSVStripsBOM(t *testing.T) {
	const body = "\xef\xbb\xbf\"date\";\"category\";\"value\"\n" +
		"\"2026-09-01\";\"Всего\";128.99\n" +
		"\"2026-10-11\";\"Все категории \";7.46266724\n"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("User-Agent") == "" {
			t.Error("запрос ушёл без User-Agent — GetCSV обязан слать randomUA, как GetFile")
		}
		_, _ = io.WriteString(w, body)
	}))
	defer server.Close()

	records, err := GetCSV(server.URL)
	if err != nil {
		t.Fatalf("GetCSV на CSV с BOM: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("записей %d, ожидалось 3 (шапка и две точки)", len(records))
	}
	wantHeader := []string{"date", "category", "value"}
	if !slices.Equal(records[0], wantHeader) {
		t.Errorf("шапка %q, ожидалась %q — BOM должен быть снят до разбора", records[0], wantHeader)
	}
	if got := records[2][2]; got != "7.46266724" {
		t.Errorf("значение второй точки %q, ожидалось %q: точность не должна теряться", got, "7.46266724")
	}
}
