package util

import (
	"io"
	"os"
	"testing"

	log "github.com/sirupsen/logrus"
)

// TestMain приглушает вывод logrus на время тестов пакета: проверяемые ветви механизма точек
// контроля публикации намеренно логируют (warn про недоступный HEAD, строка no-op, warn про
// self-heal — спека §3.4), и эти строки в выводе `go test` — ожидаемый шум, а не сигнал.
// Прежние output и уровень восстанавливаются после прогона: тест не должен менять состояние
// логгера для последующих запусков в том же процессе.
func TestMain(m *testing.M) {
	prevOut := log.StandardLogger().Out
	prevLevel := log.GetLevel()
	log.SetOutput(io.Discard)
	log.SetLevel(log.ErrorLevel)

	code := m.Run()

	log.SetOutput(prevOut)
	log.SetLevel(prevLevel)
	os.Exit(code)
}
