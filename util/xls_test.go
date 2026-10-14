package util

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
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
