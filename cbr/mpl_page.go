package cbr

import (
	"bytes"
	"fmt"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

// mplFile — xlsx-файл landing МПЛ: лимиты или границы диапазонов, сегмент
// (банки/МФО), подпись и дата начала действия документа.
type mplFile struct {
	URL          string
	Name         string
	Caption      string
	Segment      string // bank | mfo
	Kind         string // limits | ranges
	DocumentFrom time.Time
}

// cbrMPLParsePage — разбор landing МПЛ: 10 xlsx-файлов (6 лимитов + 4 границы).
// Классификация по подписи; файл без сегмента или без даты — ошибка, а не
// «пропустим»: иначе лимит молча не попадёт в таблицу.
func cbrMPLParsePage(body []byte) (limits, ranges []mplFile, err error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
	if err != nil {
		return nil, nil, fmt.Errorf("%s: landing: %w", cbrMPLTable, err)
	}

	seen := make(map[string]bool)
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
		if !strings.Contains(href, "/Content/Document/File/") ||
			!strings.HasSuffix(strings.ToLower(href), ".xlsx") {
			return
		}
		caption := strings.Join(strings.Fields(s.Text()), " ")
		if caption == "" {
			return
		}
		lower := strings.ToLower(caption)
		var kind string
		switch {
		case strings.Contains(lower, "макропруденциальные лимиты"):
			kind = "limits"
		case strings.Contains(lower, "границы диапазонов"):
			kind = "ranges"
		default:
			// Прочие xlsx на странице к МПЛ не относятся; не ошибка.
			return
		}
		segment := ""
		switch {
		case strings.Contains(lower, "микрофинансовых"):
			segment = "mfo"
		case strings.Contains(lower, "банков"):
			segment = "bank"
		default:
			parseErr = fmt.Errorf("%s: подпись %q: не определён сегмент", cbrMPLTable, caption)
			return
		}
		absolute, err := mplAbsoluteURL(href)
		if err != nil {
			parseErr = fmt.Errorf("%s: ссылка %q: %w", cbrMPLTable, href, err)
			return
		}
		if seen[absolute] {
			return
		}
		from, err := mplCaptionDate(caption)
		if err != nil {
			parseErr = fmt.Errorf("%s: подпись %q: %w", cbrMPLTable, caption, err)
			return
		}
		seen[absolute] = true
		file := mplFile{
			URL:          absolute,
			Name:         path.Base(absolute),
			Caption:      caption,
			Segment:      segment,
			Kind:         kind,
			DocumentFrom: from,
		}
		if kind == "limits" {
			limits = append(limits, file)
		} else {
			ranges = append(ranges, file)
		}
	})
	if parseErr != nil {
		return nil, nil, parseErr
	}
	if len(limits) == 0 || len(ranges) == 0 {
		return nil, nil, fmt.Errorf("%s: на landing лимитов %d, границ %d — ожидались оба набора",
			cbrMPLTable, len(limits), len(ranges))
	}

	sortMPLFiles(limits)
	sortMPLFiles(ranges)
	return limits, ranges, nil
}

func sortMPLFiles(files []mplFile) {
	sort.SliceStable(files, func(i, j int) bool {
		if files[i].DocumentFrom.Equal(files[j].DocumentFrom) {
			return files[i].Name < files[j].Name
		}
		return files[i].DocumentFrom.Before(files[j].DocumentFrom)
	})
}

func mplAbsoluteURL(href string) (string, error) {
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

func mplCaptionDate(caption string) (time.Time, error) {
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
