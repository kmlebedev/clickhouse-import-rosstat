package util

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	// maxErrorSnippet — сколько байт тела не-200 попадает в текст ошибки: тело
	// объясняет отказ (HTML WAF, JSON с кодом), но целиком в лог ему не место.
	maxErrorSnippet = 512
	// emptyBodySnippet — заглушка вместо тела в ошибке статуса: пустое тело само
	// по себе причина отказа, и пустое место после двоеточия читалось бы как сбой.
	emptyBodySnippet = "(пустое тело)"
)

// jsonTimeout — дедлайн одного запроса GetJSON. Переменная (а не константа),
// чтобы тест мог подставить короткий дедлайн вместо минутного. Прецедент —
// gold/gold.go (goldTimeout = 60 * time.Second). Дедлайн тут нужен именно ради
// фолбэка импортёра: фолбэк уходит на локальную выгрузку по ошибке, а зависший
// ответ без дедлайна ошибки не даёт — `make import STAT=wgc_etf_flows` повис бы
// вместо фолбэка.
var jsonTimeout = 60 * time.Second

// GetJSON скачивает url общим HttpClient (тот же User-Agent, что у GetFile/GetCSV)
// и разбирает ответ как JSON в out. Отличие от GetFile — статус проверяется явно:
// тело не-200 (HTML WAF, JSON ошибки) не является JSON, и ошибка разбора скрыла бы
// настоящую причину. В свою ошибку статуса попадают код и начало тела, но не URL:
// у вызывающих в query бывает ключ, а ошибка уезжает в лог (урок eia) — URL-контекст
// добавляет сам вызывающий. Ошибки транспорта приходят из net/http как *url.Error и
// URL содержат (в них он печатается целиком, вместе с query), поэтому ключ из URL
// обязан маскировать вызывающий — как это делает eia (maskKey); хелпер общий и про
// чужие параметры не знает. Пустое тело — тоже ошибка: пустой ответ неотличим от
// «данных нет» и не должен выглядеть успешным импортом. Запрос ограничен
// jsonTimeout: зависший ответ иначе не дал бы ошибки, а фолбэк ждёт именно ошибку.
func GetJSON(url string, out any) error {
	ctx, cancel := context.WithTimeout(context.Background(), jsonTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", randomUA)
	resp, err := HttpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("http status %d: %s", resp.StatusCode, errorSnippet(body))
	}
	if len(body) == 0 {
		return fmt.Errorf("пустое тело")
	}
	if err = json.Unmarshal(body, out); err != nil {
		return err
	}
	return nil
}

// errorSnippet сжимает тело ответа до одной строки не длиннее maxErrorSnippet
// байт: в ошибке достаточно начала, а переводы строк разорвали бы строку лога.
// Обрезка по байтам может задеть многобайтовую руну на стыке — её срезаем.
func errorSnippet(body []byte) string {
	if len(body) > maxErrorSnippet {
		body = body[:maxErrorSnippet]
	}
	snippet := strings.Join(strings.Fields(strings.ToValidUTF8(string(body), "")), " ")
	if snippet == "" {
		return emptyBodySnippet
	}
	return snippet
}
