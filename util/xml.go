package util

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"golang.org/x/text/encoding/charmap"
)

// xmlTimeout — дедлайн одного запроса GetXML. Переменная (а не константа), чтобы
// тест мог подставить короткий дедлайн вместо минутного. Прецедент — jsonTimeout
// (и gold/gold.go). Дедлайн нужен ради обхода сбойных лент: сбойная лента не должна
// держать весь прогон, а зависший ответ без дедлайна ошибки не даёт — импортёр ждал
// бы её вместо того, чтобы отчитаться о сбое одной ленты и продолжить с остальными.
var xmlTimeout = 60 * time.Second

// GetXML скачивает url общим HttpClient (тот же User-Agent, что у GetFile/GetCSV)
// и разбирает ответ как XML в out (разбор и контракт приёмника — ParseXML: фиды ЦБ
// объявляют windows-1251, и без поддержки объявленной кодировки разбор падал бы с
// «encoding "windows-1251" declared but Decoder.CharsetReader is nil»). Отличие от
// GetFile — статус проверяется явно: GetFile статус не смотрит, поэтому HTML-заглушка
// WAF читалась бы «успехом», а её разбор дал бы ошибку уже не по адресу. В свою ошибку
// статуса попадают код и начало тела, но не URL: ошибка уезжает в лог (урок eia) —
// URL-контекст добавляет сам вызывающий. Ошибки транспорта приходят из net/http как
// *url.Error и URL содержат, поэтому маскировать ключ из query обязан вызывающий —
// хелпер общий и про чужие параметры не знает. Пустое тело — тоже ошибка: пустой ответ
// неотличим от «в ленте нет новостей» и не должен выглядеть успешным прогоном. Запрос
// ограничен xmlTimeout: зависший ответ иначе не дал бы ошибки.
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
	return ParseXML(body, out)
}

// ParseXML разбирает body как XML в out. Отдельный хелпер, потому что не всякий XML
// приходит по сети: GetXML — это fetch плюс ParseXML, и вызывающий с уже скачанными
// байтами (локальная выгрузка, тест) разбирает их тем же кодом, без повтора настройки
// декодера. Разбор идёт xml.Decoder, а не xml.Unmarshal: декодеру можно подменить
// таблицу сущностей на HTMLEntity, иначе именованная сущность в заголовке роняет
// разбор всей ленты, а не одного поля. CharsetReader учит декодер объявленным
// кодировкам XML-фидов ЦБ: XML_dynamic.asp объявляет windows-1251, и без него разбор
// падает с «encoding "windows-1251" declared but Decoder.CharsetReader is nil».
// Контракт приёмника: структура обязана пинить корень — поле `XMLName xml.Name` с
// тегом ожидаемого элемента (например `xml:"rss"`). Без пина encoding/xml не сверяет
// имя корня и пропускает неизвестные элементы, поэтому 200 с HTML-страницей WAF
// разберётся в нулевую структуру без ошибки — тот же «тихий ноль», ради которого
// статус проверяется явно, только теперь с честным 200. С пином чужой корень даёт
// ошибку декодирования, и вызывающий видит, что пришёл не тот документ.
func ParseXML(body []byte, out any) error {
	decoder := xml.NewDecoder(bytes.NewReader(body))
	decoder.Entity = xml.HTMLEntity // ленты встречаются с &nbsp;: одна сущность не должна обнулять ленту целиком
	decoder.CharsetReader = charsetReader
	return decoder.Decode(out)
}

// charsetReader переводит кодировку, объявленную в XML-декларации, в Reader с UTF-8 —
// так encoding/xml получает вход, который умеет разбирать. Кодировки фидов ЦБ:
// windows-1251 (XML_dynamic.asp, в том числе курс USD R01235) — через charmap;
// utf-8 — проход как есть (декларация UTF-8 до CharsetReader не доходит, ветка
// страхует регистр и синоним utf8). Любая другая — ошибка, а не «разберём как
// получилось»: байты koi8-r в UTF-8-полях стали бы мусорными значениями в ClickHouse,
// и молчаливый мусор нашёлся бы только на дашборде. В тексте ошибки — сама кодировка:
// иначе сбой ленты читается как «что-то пошло не так» без указания на перекодированный
// фид.
func charsetReader(charset string, input io.Reader) (io.Reader, error) {
	switch strings.ToLower(charset) {
	case "utf-8", "utf8":
		return input, nil
	case "windows-1251", "cp1251":
		return charmap.Windows1251.NewDecoder().Reader(input), nil
	default:
		return nil, fmt.Errorf("неподдерживаемая кодировка XML %q", charset)
	}
}
