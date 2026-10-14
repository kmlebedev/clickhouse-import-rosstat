package util

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

// FindLink возвращает href первой ссылки, найденной селектором на HTML-странице.
// href отдаётся как есть (абсолютный или относительный): префикс домена навешивает
// вызывающий — так же, как делал прежний HTML-транспорт. Пустой href или отсутствие совпадения —
// ошибка: без ссылки на XLSX импорт должен падать до записи в БД, а не тихо писать
// «0 rows» по устаревшему или чужому адресу.
func FindLink(page []byte, selector string) (string, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(page))
	if err != nil {
		return "", fmt.Errorf("FindLink: разбор страницы: %w", err)
	}
	href, ok := doc.Find(selector).First().Attr("href")
	if !ok || strings.TrimSpace(href) == "" {
		return "", fmt.Errorf("FindLink: по селектору %q ссылка не найдена", selector)
	}
	return href, nil
}

// GetPage скачивает HTML-страницу целиком через GetFile — общий хелпер пакетов
// импортёров (стандарт §4.1: все сетевые обращения — через util). Ридер закрывается
// всегда: ошибки чтения и закрытия возвращаются обе, иначе повреждённая страница
// выглядела бы успешным разбором.
func GetPage(pageURL string) ([]byte, error) {
	reader, err := GetFile(pageURL)
	if err != nil {
		return nil, err
	}
	page, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return page, nil
}
