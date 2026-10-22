package util

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

// X13Timeout — предел одного прогона X-13ARIMA-SEATS. Авто-ARIMA на месячном
// ряду укладывается в секунды, но зависший R не должен держать шаг DAG вечно.
const X13Timeout = 15 * time.Minute

// RunX13 пишет встроенный R-скрипт и вход во временный каталог и вызывает
// Rscript. Скрипт и данные встроены в бинарник, поэтому staging не зависит от
// scripts/ и docs/; внешняя зависимость — только R + seasonal.
//
// prefix — имя пакета/таблицы: из него собираются имена временных файлов и
// префикс строки журнала. Секреты конвейера (DSN, пароли, токены) в R не
// передаются: окружение Rscript — whitelist rEnvironment.
func RunX13(ctx context.Context, script, input []byte, prefix string) ([]byte, error) {
	rscript, err := exec.LookPath("Rscript")
	if err != nil {
		return nil, fmt.Errorf("не найден Rscript в PATH: нужен R с пакетом seasonal: %w", err)
	}
	dir, err := os.MkdirTemp("", prefix+"_x13_")
	if err != nil {
		return nil, fmt.Errorf("X-13: временный каталог: %w", err)
	}
	defer func() {
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			log.Warnf("%s: не удалось удалить временный каталог %s: %v", prefix, dir, rmErr)
		}
	}()

	scriptPath := filepath.Join(dir, prefix+"_x13.R")
	inputPath := filepath.Join(dir, "input.csv")
	outputPath := filepath.Join(dir, "output.csv")
	if writeErr := os.WriteFile(scriptPath, script, 0o600); writeErr != nil {
		return nil, fmt.Errorf("X-13: скрипт: %w", writeErr)
	}
	if writeErr := os.WriteFile(inputPath, input, 0o600); writeErr != nil {
		return nil, fmt.Errorf("X-13: вход: %w", writeErr)
	}

	runCtx, cancel := context.WithTimeout(ctx, X13Timeout)
	defer cancel()
	cmd := exec.CommandContext(runCtx, rscript, scriptPath, inputPath, outputPath)
	cmd.Env = rEnvironment()
	// Если R не закрывает pipes после Kill по контексту, Wait не должен висеть вечно.
	cmd.WaitDelay = 10 * time.Second
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if runErr := cmd.Run(); runErr != nil {
		return nil, fmt.Errorf("запуск X-13 (Rscript): %w: %s", runErr, tail(stderr.String(), 2000))
	}
	result, readErr := os.ReadFile(outputPath)
	if readErr != nil {
		return nil, fmt.Errorf("X-13: результат: %w", readErr)
	}
	log.Infof("%s: X-13 %s", prefix, strings.TrimSpace(stdout.String()))
	return result, nil
}

// rEnvironment — минимальное окружение Rscript: пути, локаль, временный каталог
// и пользовательская библиотека. Секреты конвейера в R не передаются.
func rEnvironment() []string {
	keys := []string{"PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "R_LIBS_USER", "R_LIBS", "R_LIBS_SITE"}
	env := make([]string, 0, len(keys))
	for _, key := range keys {
		if value, ok := os.LookupEnv(key); ok {
			env = append(env, key+"="+value)
		}
	}
	return env
}

// tail оставляет хвост вывода R: начало stderr часто занимает шум загрузки
// пакета, а причина — в конце.
func tail(text string, limit int) string {
	if len(text) <= limit {
		return strings.TrimSpace(text)
	}
	return strings.TrimSpace(text[len(text)-limit:])
}
