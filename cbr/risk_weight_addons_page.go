package cbr

import (
	"bytes"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

// riskAddonFile — матрица надбавок со страницы ЦБ: адрес, basename, подпись и
// дата начала действия документа. Дата начала действия — порядок версий в
// витрине: файл с более поздней датой обрабатывается позже и побеждает в argMax.
type riskAddonFile struct {
	URL          string
	Name         string
	Caption      string
	DocumentFrom time.Time
}

// riskAddonDateRe — первая дата подписи «с ДД.ММ.ГГГГ [по ДД.ММ.ГГГГ]».
var riskAddonDateRe = regexp.MustCompile(`(\d{2})\.(\d{2})\.(\d{4})`)

// cbrRiskWeightAddonsParsePage — разбор landing: только xlsx-ссылки матриц
// (/Collection/Collection/File/...). Подпись обязательна: по ней берётся дата
// начала действия документа; файл без даты — ошибка, а не «самый старый».
func cbrRiskWeightAddonsParsePage(body []byte) ([]riskAddonFile, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("%s: landing: %w", cbrRiskWeightAddonsTable, err)
	}

	seen := make(map[string]bool)
	var files []riskAddonFile
	var parseErr error
	doc.Find("a[href]").Each(func(_ int, s *goquery.Selection) {
		if parseErr != nil {
			return
		}
		href, ok := s.Attr("href")
		if !ok {
			return
		}
		href = strings.TrimSpace(href)
		if !strings.Contains(href, "/Collection/Collection/File/") ||
			!strings.HasSuffix(strings.ToLower(href), ".xlsx") {
			return
		}
		caption := strings.Join(strings.Fields(s.Text()), " ")
		if caption == "" {
			parseErr = fmt.Errorf("%s: xlsx-ссылка %q без подписи — нельзя определить дату документа",
				cbrRiskWeightAddonsTable, href)
			return
		}
		absolute, err := riskAddonAbsoluteURL(href)
		if err != nil {
			parseErr = fmt.Errorf("%s: ссылка %q: %w", cbrRiskWeightAddonsTable, href, err)
			return
		}
		if seen[absolute] {
			return
		}
		from, err := riskAddonCaptionDate(caption)
		if err != nil {
			parseErr = fmt.Errorf("%s: подпись %q: %w", cbrRiskWeightAddonsTable, caption, err)
			return
		}
		seen[absolute] = true
		files = append(files, riskAddonFile{
			URL:          absolute,
			Name:         path.Base(absolute),
			Caption:      caption,
			DocumentFrom: from,
		})
	})
	if parseErr != nil {
		return nil, parseErr
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("%s: на landing не найдено ни одной xlsx-матрицы", cbrRiskWeightAddonsTable)
	}

	// Старые документы первыми: более поздняя версия пишется позже и выигрывает
	// в argMax(version) витрины. При равной дате — по basename, чтобы порядок
	// не зависел от порядка ссылок на странице.
	sort.SliceStable(files, func(i, j int) bool {
		if files[i].DocumentFrom.Equal(files[j].DocumentFrom) {
			return files[i].Name < files[j].Name
		}
		return files[i].DocumentFrom.Before(files[j].DocumentFrom)
	})
	return files, nil
}

// riskAddonAbsoluteURL — относительный href страницы ЦБ → абсолютный URL.
func riskAddonAbsoluteURL(href string) (string, error) {
	u, err := url.Parse(href)
	if err != nil {
		return "", err
	}
	if u.IsAbs() {
		return u.String(), nil
	}
	base, err := url.Parse("https://www.cbr.ru")
	if err != nil {
		return "", err
	}
	return base.ResolveReference(u).String(), nil
}

// riskAddonCaptionDate — первая дата подписи. Подписи источника:
// «Таблица 2 с 15.10.2026», «Таблица 2 с 04.09.2026 по 14.10.2026»,
// «с 02.12.2024 по 31.01.2025» — во всех есть дата начала.
func riskAddonCaptionDate(caption string) (time.Time, error) {
	m := riskAddonDateRe.FindStringSubmatch(caption)
	if m == nil {
		return time.Time{}, fmt.Errorf("нет даты ДД.ММ.ГГГГ")
	}
	t, err := time.Parse("02.01.2006", m[0])
	if err != nil {
		return time.Time{}, fmt.Errorf("дата %q: %w", m[0], err)
	}
	return t, nil
}
