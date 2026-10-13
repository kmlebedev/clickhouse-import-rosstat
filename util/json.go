package util

import (
	"bytes"
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

// postJSONTimeout — дедлайн одного запроса PostJSON. Тоже переменная, чтобы тест
// подставил короткий дедлайн вместо длинного. 65 s — не минута GetJSON: вызов LLM
// генерирует разметку резюме (десятки цитат и заметок) и отвечает заметно дольше
// обычного фида, а дедлайн нужен затем, чтобы зависший провайдер дал ошибку, по которой
// импортёр деградирует до регекс-слоя: без него шаг dagu повис бы вместе с провайдером.
var postJSONTimeout = 65 * time.Second

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

// PostJSON отправляет request как JSON-тело POST-запроса на url общим HttpClient (тот
// же случайный User-Agent, что у GetFile/GetJSON) и разбирает ответ как JSON в out —
// транспорт будущего LLM-слоя (интерфейс — OpenAI-совместимый /chat/completions).
// headers накладываются поверх Content-Type: ключ LLM едет заголовком Authorization, а
// не в query, поэтому в тексты ошибок он не попадает — ошибка статуса строится только из
// кода и начала тела (обрезка как у GetJSON), URL в неё не пишется (конвенция GetJSON: у
// вызывающих в URL бывает ключ, а ошибка уезжает в лог; URL-контекст добавляет
// вызывающий). Ошибки транспорта приходят из net/http как *url.Error и содержат URL —
// заголовки в такую ошибку net/http не добавляет, так что и секретов из headers там нет;
// сами заголовки хелпер в обёртки не подставляет. Пустое тело — тоже ошибка: пустой
// ответ неотличим от «разметка без каналов» и не должен выглядеть успешным вызовом.
// Запрос ограничен postJSONTimeout: на зависший ответ импортёр должен получить ошибку и
// уйти в деградацию до регекс-слоя, а не ждать его вечно.
func PostJSON(url string, headers map[string]string, request any, out any) error {
	jsonBody, err := json.Marshal(request)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), postJSONTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(jsonBody))
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", randomUA)
	req.Header.Set("Content-Type", "application/json")
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	resp, err := HttpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
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
