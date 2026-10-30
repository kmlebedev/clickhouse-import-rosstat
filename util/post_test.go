package util

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPostFileBytes(t *testing.T) {
	var gotMethod, gotBody, gotHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotHeader = r.Header.Get("X-Test")
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("чтение тела: %v", err)
		}
		gotBody = string(body)
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	body, err := PostFileBytes(server.URL, []byte("payload"), map[string]string{
		"Content-Type": "text/plain",
		"X-Test":       "1",
	})
	if err != nil {
		t.Fatalf("PostFileBytes: %v", err)
	}
	if string(body) != "ok" {
		t.Errorf("ответ %q, ожидался ok", body)
	}
	if gotMethod != http.MethodPost {
		t.Errorf("метод %q, ожидался POST", gotMethod)
	}
	if gotBody != "payload" {
		t.Errorf("тело %q, ожидался payload", gotBody)
	}
	if gotHeader != "1" {
		t.Errorf("заголовок X-Test = %q, ожидался 1", gotHeader)
	}
}

func TestPostFileBytesStatusError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusBadGateway)
	}))
	defer server.Close()

	if _, err := PostFileBytes(server.URL, []byte("payload"), nil); err == nil {
		t.Fatal("ожидалась ошибка на не-2xx")
	}
}

func TestPostFileBytesEmptyBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	if _, err := PostFileBytes(server.URL, []byte("payload"), nil); err == nil {
		t.Fatal("ожидалась ошибка на пустой ответ")
	}
}
