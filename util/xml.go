package util

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"time"
)

// xmlTimeout — дедлайн одного запроса GetXML. Переменная (а не константа), чтобы
// тест мог подставить короткий дедлайн вместо минутного. Прецедент — jsonTimeout
// (и gold/gold.go). Дедлайн нужен ради обхода сбойных лент: сбойная лента не должна
// держать весь прогон, а зависший ответ без дедлайна ошибки не даёт — импортёр ждал
// бы её вместо того, чтобы отчитаться о сбое одной ленты и продолжить с остальными.
var xmlTimeout = 60 * time.Second

// GetXML скачивает url общим HttpClient (тот же User-Agent, что у GetFile/GetCSV)
// и разбирает ответ как XML в out. Отличие от GetFile — статус проверяется явно:
// GetFile статус не смотрит, поэтому HTML-заглушка WAF читалась бы «успехом», а её
// разбор дал бы ошибку уже не по адресу. В свою ошибку статуса попадают код и начало
// тела, но не URL: ошибка уезжает в лог (урок eia) — URL-контекст добавляет сам
// вызывающий. Ошибки транспорта приходят из net/http как *url.Error и URL содержат,
// поэтому маскировать ключ из query обязан вызывающий — хелпер общий и про чужие
// параметры не знает. Пустое тело — тоже ошибка: пустой ответ неотличим от «в ленте
// нет новостей» и не должен выглядеть успешным прогоном. Запрос ограничен
// xmlTimeout: зависший ответ иначе не дал бы ошибки. Разбор идёт xml.Decoder, а не
// xml.Unmarshal: декодеру можно подменить таблицу сущностей на HTMLEntity, иначе
// именованная сущность в заголовке роняет разбор всей ленты, а не одного поля.
func GetXML(url string, out any) error {
	ctx, cancel := context.WithTimeout(context.Background(), xmlTimeout)
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
	decoder := xml.NewDecoder(bytes.NewReader(body))
	decoder.Entity = xml.HTMLEntity // ленты встречаются с &nbsp;: одна сущность не должна обнулять ленту целиком
	if err = decoder.Decode(out); err != nil {
		return err
	}
	return nil
}
