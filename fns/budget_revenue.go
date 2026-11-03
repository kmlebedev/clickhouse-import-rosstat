package fns

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/PuerkitoBio/goquery"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

const (
	// fnsImporterName — Name(), имя таблицы, CLICKHOUSE_IMPORT_STAT и id шага
	// dagu/fns.yaml: всё одно имя, поэтому и фильтр ручного запуска однозначен.
	fnsImporterName = "fns_budget_revenue"
	fnsTable        = "fns_budget_revenue"
	// fnsSource — ключ каталога series_catalog и колонка source витрины: как у
	// импортёров собственных таблиц (cbr_ccyb_indicators, minfin_mesyats).
	fnsSource = "fns_budget_revenue"
	// fnsFormsURL — страница статистической налоговой отчётности: с неё снимаются
	// ссылки на годовые страницы сводной формы № 1-НМ. Префикс rn77 — переключатель
	// региона на сайте, а блок сводных отчётов в целом по РФ от него не зависит.
	fnsFormsURL = "https://www.nalog.gov.ru/rn77/related_activities/statistics_and_analytics/forms/"
	// fnsRegZipMarker — суффикс имени архива с разрезом по территориям: у каждого
	// отчётного месяца на странице года идёт пара `1nm<as-of>.xlsx` (свод по РФ) и
	// `1nm<as-of>reg.zip` (тот же отчёт по территориям).
	fnsRegZipMarker = "reg.zip"
	// fnsFromEnv — ремонт/бэкфилл с отчётного месяца включительно (YYYY-MM): гейт по
	// месяцам прошлое не перечитывает, а источник публикует ревизии (ut).
	fnsFromEnv = "FNS_1NM_FROM"
	// fnsFirstMonth — начало истории, которое берёт импортёр: архивы 2018–2019
	// содержат внутренние файлы в legacy `.xls`, разбор которых здесь не реализуется
	// (спека §2.4, решение владельца 2026-11-03).
	fnsFirstMonth = "2020-01"
	// fnsSelectMonths — гейт инкремента: какие отчётные месяцы уже лежат в таблице.
	// FINAL обязателен, иначе несмёрженные части ревизий дали бы «известный» месяц
	// дважды (стандарт §9.2).
	fnsSelectMonths = "SELECT DISTINCT date FROM " + fnsTable + " FINAL ORDER BY date"
	// fnsMaxYearPages — предохранитель обхода: годов у формы больше двух десятков
	// быть не может, а cap не даёт зациклиться на неодомированной разметке.
	fnsMaxYearPages = 40
	// fnsAggregateRowSelector — строки таблицы сводных отчётов в целом по РФ; блок
	// `#t1` отличает их от таблицы отчётов отдельного УФНС (`#t00`), которые в
	// объёме пункта не разбираются.
	fnsAggregateRowSelector = "div#t1 tr"
	// fnsFormIndexSelector — индекс формы в строке таблицы («1-НМ»): по нему, а не по
	// порядку строк, находится нужный отчёт.
	fnsFormIndexSelector = "div.tag_stat"
)

// fnsTableDdl — канонический DDL таблицы (ARCHITECTURE.md §6.2): хранится опубликованное
// значение — накопленным итогом с начала года, тыс. руб.; помесячное считает витрина.
var fnsTableDdl = []string{`CREATE TABLE IF NOT EXISTS %s (
				  date Date
				, region LowCardinality(String)
				, region_kind LowCardinality(String)
				, series LowCardinality(String)
				, value Float64
			) ENGINE = ReplacingMergeTree ORDER BY (series, region, date);
		`}

// fnsBudgetRevenue — импортёр-обёртка: полный цикл ведёт собственный Import(), потому
// что артефактов месяца много и шаблон умеет один DataUrl (прецеденты
// minfin_ofz_auction, cbr_dip_bulletin). Точки контроля публикации нет: отпечаток
// одного URL не отвечает на вопрос «что уже разобрано» (стандарт §9.3); винтажного
// архива тоже нет — он включается только объявленной точкой (§9.6). Отступления
// названы в спеке. Name() остаётся равным TableName, поэтому метод не переопределяется.
type fnsBudgetRevenue struct {
	util.ClickHouseImport
}

// fnsBudgetRevenueImporter — импортёр поступлений 1-НМ. Регистрация — реестром
// пакета (fns/importers.go, стандарт §8.1), персональных init() в файлах импортёров
// нет.
var fnsBudgetRevenueImporter = &fnsBudgetRevenue{ClickHouseImport: util.ClickHouseImport{
	TableName:   fnsTable,
	CreateTable: fnsTableDdl,
	DataUrl:     fnsFormsURL,
}}

// fnsArtifact — один архив отчётного месяца: URL, имя файла и признак ревизии.
type fnsArtifact struct {
	month    time.Time
	url      string
	name     string
	revision bool
}

// Import — DDL → старт инкремента → годовые страницы формы → архивы месяцев,
// которых ещё нет в таблице → разбор каждого шаблоном → производные. Возвращает
// число записанных точек; ноль — штатный повтор, а не ошибка.
func (s *fnsBudgetRevenue) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	for _, stmt := range s.CreateTable {
		if err = conn.Exec(ctx, fmt.Sprintf(stmt, s.TableName)); err != nil {
			return count, fmt.Errorf("%s: DDL: %w", fnsImporterName, err)
		}
	}
	start, known, err := fnsGate(ctx, conn)
	if err != nil {
		return count, err
	}
	artifacts, err := fnsCollectArtifacts(s.DataUrl, start, known)
	if err != nil {
		return count, err
	}
	if len(artifacts) == 0 {
		log.Infof("%s: новых отчётных месяцев нет (старт %s, в таблице %d месяцев)",
			fnsImporterName, fnsMonthLabel(start), len(known))
		// Производные публикуются и на пустой дельте: иначе первая пустая база не
		// получит ни каталога, ни витрины, а повторный прогон их уже не создаст.
		if err = afterFnsBudgetRevenue(ctx, conn); err != nil {
			return count, err
		}
		return 0, nil
	}
	for _, artifact := range artifacts {
		inner := util.ClickHouseImport{
			TableName:   s.TableName,
			CreateTable: s.CreateTable,
			DataUrl:     artifact.url,
			BodyFunc: func(body []byte, batch driver.Batch) error {
				return fnsParseBudgetRevenue(body, batch, artifact.month)
			},
		}
		points, err := inner.Import(ctx, conn)
		if err != nil {
			return count, fmt.Errorf("%s: архив %q (%s): %w",
				fnsImporterName, artifact.name, artifact.month.Format("2006-01"), err)
		}
		log.Infof("%s: %s — %d точек (%s)", fnsImporterName, artifact.month.Format("2006-01"), points, artifact.name)
		count += points
	}
	if err = afterFnsBudgetRevenue(ctx, conn); err != nil {
		return count, err
	}
	return count, nil
}

// fnsGate — с какого месяца импортировать и какие месяцы уже известны.
// FNS_1NM_FROM переопределяет гейт (включительно) и остаётся единственным путём
// ремонта: ревизия уже импортированного месяца гейтом не видна.
func fnsGate(ctx context.Context, conn driver.Conn) (time.Time, map[string]bool, error) {
	known := make(map[string]bool)
	var maxMonth time.Time
	rows, err := conn.Query(ctx, fnsSelectMonths)
	if err != nil {
		return time.Time{}, nil, fmt.Errorf("%s: months: %w", fnsImporterName, err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var date time.Time
		if err = rows.Scan(&date); err != nil {
			return time.Time{}, nil, fmt.Errorf("%s: months scan: %w", fnsImporterName, err)
		}
		known[fnsMonthKey(date)] = true
		if date.After(maxMonth) {
			maxMonth = date
		}
	}
	if err = rows.Err(); err != nil {
		return time.Time{}, nil, fmt.Errorf("%s: months: %w", fnsImporterName, err)
	}
	start, err := fnsStartMonth(strings.TrimSpace(os.Getenv(fnsFromEnv)), known, maxMonth)
	if err != nil {
		return time.Time{}, nil, err
	}
	return start, known, nil
}

// fnsStartMonth — решение о старте импорта, отделённое от запроса к базе: с ним
// выбор старта проверяется тестом без ClickHouse. FNS_1NM_FROM переопределяет гейт
// (включительно) и остаётся единственным путём ремонта: ревизия уже импортированного
// месяца гейтом не видна. Пустая таблица — старт с fnsFirstMonth, иначе следующий
// месяц после последнего записанного.
func fnsStartMonth(from string, known map[string]bool, maxMonth time.Time) (time.Time, error) {
	if from != "" {
		start, err := fnsParseMonth(from)
		if err != nil {
			return time.Time{}, fmt.Errorf("%s: %s=%q: ожидался формат YYYY-MM: %w", fnsImporterName, fnsFromEnv, from, err)
		}
		return start, nil
	}
	if len(known) == 0 {
		return fnsParseMonth(fnsFirstMonth)
	}
	return maxMonth.AddDate(0, 1, 0), nil
}

// fnsCollectArtifacts — артефакты, которые надо импортировать: от стартового месяца,
// пропуская уже известные, по одному архиву на месяц (ревизия важнее оригинала).
func fnsCollectArtifacts(formsURL string, start time.Time, known map[string]bool) ([]fnsArtifact, error) {
	page, err := util.GetPage(formsURL)
	if err != nil {
		return nil, fmt.Errorf("%s: страница форм %s: %w", fnsImporterName, formsURL, err)
	}
	years, err := fnsYearPages(page, formsURL)
	if err != nil {
		return nil, err
	}
	picked := make(map[string]fnsArtifact, 32)
	visited := 0
	for _, year := range years {
		// Месячный отчёт лежит на странице года, к которому относится отчётный месяц,
		// поэтому страниц раньше стартового года можно не спрашивать.
		if year.year < start.Year() {
			continue
		}
		if visited >= fnsMaxYearPages {
			return nil, fmt.Errorf("%s: годовых страниц больше %d — разметка страницы форм изменилась", fnsImporterName, fnsMaxYearPages)
		}
		visited++
		body, err := util.GetPage(year.url)
		if err != nil {
			return nil, fmt.Errorf("%s: страница %d года: %w", fnsImporterName, year.year, err)
		}
		for _, artifact := range fnsMonthlies(body, year.url) {
			if artifact.month.Before(start) || known[fnsMonthKey(artifact.month)] {
				continue
			}
			key := fnsMonthKey(artifact.month)
			if prev, ok := picked[key]; ok {
				// Один отчётный месяц может быть опубликован оригиналом и ревизией:
				// берём ревизию, а при равенстве — последнее имя, чтобы выборка была
				// детерминированной.
				if artifact.revision || (!prev.revision && artifact.name > prev.name) {
					picked[key] = artifact
				}
				continue
			}
			picked[key] = artifact
		}
	}
	artifacts := make([]fnsArtifact, 0, len(picked))
	for _, artifact := range picked {
		artifacts = append(artifacts, artifact)
	}
	// Хронологический порядок: точки пишутся по месяцам, и лог прогона читается
	// как история источника, а не как обход map.
	sort.Slice(artifacts, func(i, j int) bool { return artifacts[i].month.Before(artifacts[j].month) })
	return artifacts, nil
}

// fnsYearPage — страница годового сводного отчёта формы 1-НМ.
type fnsYearPage struct {
	year int
	url  string
}

// fnsYearPages — ссылки на годовые страницы из строки «1-НМ» блока сводных отчётов в
// целом по РФ. Год берётся из текста ссылки (источник подписывает им именно год),
// а не из адреса: адрес — непрозрачный идентификатор страницы, меняющийся каждый
// год. Ссылки открытых данных в той же строке отфильтровываются формой href.
func fnsYearPages(page []byte, formsURL string) ([]fnsYearPage, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(page))
	if err != nil {
		return nil, fmt.Errorf("%s: разбор страницы форм: %w", fnsImporterName, err)
	}
	var years []fnsYearPage
	found := false
	doc.Find(fnsAggregateRowSelector).Each(func(_ int, tr *goquery.Selection) {
		if found {
			return
		}
		index := strings.TrimSpace(tr.Find(fnsFormIndexSelector).First().Text())
		if index != "1-НМ" {
			return
		}
		found = true
		tr.Find("td").Last().Find("a[href]").Each(func(_ int, a *goquery.Selection) {
			href, _ := a.Attr("href")
			year, err := strconv.Atoi(strings.Trim(strings.TrimSpace(a.Text()), ";. "))
			if err != nil || year < 1990 || year > 2999 {
				return
			}
			absolute, err := fnsResolveURL(formsURL, href)
			if err != nil || absolute == formsURL {
				// Битая или пустая ссылка (`href="#"`) разрешается в адрес самой
				// страницы форм: принимая её, импортёр бесконечно перечитывал бы
				// страницу форм вместо годовых.
				return
			}
			if !fnsIsNumericPageID(absolute) {
				// Годовая страница источника — относительный числовой id; ссылки
				// открытых данных и прочие анкоры строки отбрасываются здесь.
				return
			}
			years = append(years, fnsYearPage{year: year, url: absolute})
		})
	})
	if !found {
		return nil, fmt.Errorf("%s: на странице %s не найдена строка формы 1-НМ в блоке сводных отчётов — разметка изменилась", fnsImporterName, formsURL)
	}
	if len(years) == 0 {
		return nil, fmt.Errorf("%s: строка формы 1-НМ не содержит ни одной годовой ссылки — разметка изменилась", fnsImporterName)
	}
	return years, nil
}

// fnsMonthlies — архивы с разрезом по территориям со страницы года. Ссылки всегда
// берутся со страницы: путь к файлам менялся минимум четыре раза
// (`docs/otchet/1nm/`, `docs\otchet/` с обратным слэшем, `svod/1_nm/`,
// `files/related_activities/.../1nm/`), поэтому конструировать адрес по дате нельзя.
func fnsMonthlies(page []byte, pageURL string) []fnsArtifact {
	var artifacts []fnsArtifact
	for _, href := range fnsRegZipHrefs(page) {
		name := pathBase(href)
		month, revision, ok := fnsReportingMonth(name)
		if !ok {
			log.Warnf("%s: архив %q не опознан по имени как месячный отчёт — пропущен", fnsImporterName, name)
			continue
		}
		absolute, err := fnsResolveURL(pageURL, href)
		if err != nil {
			log.Warnf("%s: ссылка %q не разобрана: %v", fnsImporterName, href, err)
			continue
		}
		artifacts = append(artifacts, fnsArtifact{month: month, url: absolute, name: name, revision: revision})
	}
	return artifacts
}

// fnsRegZipHref — ссылка на архив территорий; суффикс берется из fnsRegZipMarker,
// чтобы маркер был объявлен в одном месте.
var fnsRegZipHref = regexp.MustCompile(`href="([^"]*` + regexp.QuoteMeta(fnsRegZipMarker) + `)"`)

func fnsRegZipHrefs(page []byte) []string {
	matches := fnsRegZipHref.FindAllStringSubmatch(string(page), -1)
	hrefs := make([]string, 0, len(matches))
	for _, match := range matches {
		// Источник печатает путь и с обратным слэшем: «docs\otchet\…».
		hrefs = append(hrefs, strings.ReplaceAll(match[1], `\`, "/"))
	}
	return hrefs
}

// fnsMonthDigits — дата в имени архива: `1nm`, затем шесть или восемь цифр
// (ddmmyy либо ddmmyyyy) — обе формы живёт источник: `1nm010926reg.zip` и
// `1nm01062021reg.zip`. Между `1nm` и цифрами встречается подчёркивание
// (`1nm_010322reg.zip`). Захватывается вся цепочка цифр, а не альтернатива
// «шесть или восемь»: левая альтернатива съела бы шесть цифр из восьми и
// превратила 2021 год в 2020.
var fnsMonthDigits = regexp.MustCompile(`1nm_?(\d+)`)

// fnsReportingMonth — отчётный месяц по имени архива. Дата в имени — as-of, то есть
// первое число месяца, следующего за отчётным: архив `1nm010926reg.zip` (as-of
// 01.09.2026) отчитывается за август 2026, а `1nm010126reg.zip` (as-of 01.01.2026) —
// за декабрь 2025. Ревизия помечается фрагментом `ut` (и `ut_…`, и `…ut_reg.zip`).
func fnsReportingMonth(name string) (month time.Time, revision bool, ok bool) {
	match := fnsMonthDigits.FindStringSubmatch(strings.ToLower(name))
	if match == nil {
		return time.Time{}, false, false
	}
	digits := match[1]
	if len(digits) != 6 && len(digits) != 8 {
		return time.Time{}, false, false
	}
	day, err := strconv.Atoi(digits[0:2])
	if err != nil || day != 1 {
		// Отчёт формируется на первое число: другая дата — непривычный артефакт,
		// а не месячный отчёт.
		return time.Time{}, false, false
	}
	mo, err := strconv.Atoi(digits[2:4])
	if err != nil || mo < 1 || mo > 12 {
		return time.Time{}, false, false
	}
	year, err := strconv.Atoi(digits[4:])
	if err != nil {
		return time.Time{}, false, false
	}
	if len(digits) == 6 {
		year += 2000
	}
	asOf, err := fnsDate(year, mo, day)
	if err != nil {
		return time.Time{}, false, false
	}
	revision = strings.Contains(strings.ToLower(name), "ut")
	previous := asOf.AddDate(0, 0, -1)
	return time.Date(previous.Year(), previous.Month(), 1, 0, 0, 0, 0, time.UTC), revision, true
}

// fnsParseMonth — месяц формата YYYY-MM.
func fnsParseMonth(value string) (time.Time, error) {
	parsed, err := time.Parse("2006-01", value)
	if err != nil {
		return time.Time{}, err
	}
	return time.Date(parsed.Year(), parsed.Month(), 1, 0, 0, 0, 0, time.UTC), nil
}

// fnsDate — дата в UTC: сравнения месяцев и ключи гейта обязаны быть в одной зоне.
func fnsDate(year, month, day int) (time.Time, error) {
	candidate := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	if candidate.Day() != day || int(candidate.Month()) != month {
		return time.Time{}, fmt.Errorf("несуществующая дата %04d-%02d-%02d", year, month, day)
	}
	return candidate, nil
}

// fnsMonthKey — ключ месяца для множества гейта.
func fnsMonthKey(date time.Time) string { return date.UTC().Format("2006-01") }

// fnsMonthLabel — начало импорта для лога: нулевая дата читалась бы как «0001-01».
func fnsMonthLabel(start time.Time) string {
	if start.IsZero() {
		return "с начала истории"
	}
	return "с " + start.Format("2006-01")
}

// fnsResolveURL достраивает href до абсолютного адреса от страницы, на которой он
// найден: годовые ссылки относительные (`3832158/`), файловые — от корня сайта
// (`/html/sites/…`). Хост при этом остаётся хостом страницы: www.nalog.gov.ru
// отдаёт те же файлы, что и data.nalog.ru, а перебивка хоста лишила бы теста
// возможности подменить источник локальным сервером.
func fnsResolveURL(base, href string) (string, error) {
	baseURL, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("адрес страницы %q: %w", base, err)
	}
	if baseURL.Scheme == "" || baseURL.Host == "" {
		return "", fmt.Errorf("в адресе страницы %q нет схемы и хоста", base)
	}
	hrefURL, err := url.Parse(href)
	if err != nil {
		return "", fmt.Errorf("ссылка %q: %w", href, err)
	}
	resolved := baseURL.ResolveReference(hrefURL)
	if resolved.Scheme != "http" && resolved.Scheme != "https" {
		return "", fmt.Errorf("ссылка %q ведёт не на http(s): %q", href, resolved.String())
	}
	return resolved.String(), nil
}

// fnsIsNumericPageID — последний сегмент адреса страницы года: у источника это
// непрозрачный числовой идентификатор (`…/forms/16619097/`). Отфильтровать им нужно
// и ссылки открытых данных той же строки (`…/opendata/7707329152-nmpn/`), и всё, во
// что разрешаются битые анкоры.
func fnsIsNumericPageID(rawURL string) bool {
	segment := pathBase(rawURL)
	if segment == "" {
		return false
	}
	if _, err := strconv.Atoi(segment); err != nil {
		return false
	}
	return true
}

// pathBase — имя файла из URL (без query и хвостовых слэшей).
func pathBase(href string) string {
	cleaned := strings.SplitN(href, "?", 2)[0]
	cleaned = strings.TrimRight(cleaned, "/")
	index := strings.LastIndex(cleaned, "/")
	if index < 0 {
		return cleaned
	}
	return cleaned[index+1:]
}
