package cbr

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// findLink возвращает href первой ссылки, найденной селектором на HTML-странице.
// href отдаётся как есть (абсолютный или относительный): префикс домена навешивает
// вызывающий — так же, как делал прежний HTML-транспорт. Пустой href или отсутствие совпадения —
// ошибка: без ссылки на XLSX импорт должен падать до записи в БД, а не тихо писать
// «0 rows» по устаревшему или чужому адресу.
func findLink(page []byte, selector string) (string, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(page))
	if err != nil {
		return "", fmt.Errorf("findLink: разбор страницы: %w", err)
	}
	href, ok := doc.Find(selector).First().Attr("href")
	if !ok || strings.TrimSpace(href) == "" {
		return "", fmt.Errorf("findLink: по селектору %q ссылка не найдена", selector)
	}
	return href, nil
}

// getPage скачивает HTML-страницу целиком через util.GetFile. Ридер закрывается всегда:
// ошибки чтения и закрытия возвращаются обе, иначе повреждённая страница выглядела бы
// успешным разбором.
func getPage(pageURL string) ([]byte, error) {
	reader, err := util.GetFile(pageURL)
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
