package edisclosure

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Разбор и валидация входа импортёра edisclosure_documents (спека §4.2, §5.3):
// manifest.json — маркер завершённого прогона коллектора, documents.jsonl — по
// строке на документ. Ошибка любой строки — fail-fast до сети и БД, поэтому весь
// разбор здесь чистый: ни HTTP, ни ClickHouse. Валидация повторяет контракт
// коллектора (scripts/edisclosure_collect.py): импортёр не доверяет входу и
// обязан отвергнуть снапшот, который коллектор не должен был записать.

const (
	// edisclosureDocumentsFile — имя файла документов: manifest.documents_file
	// обязан совпадать с ним дословно (спека §4.2, §5.3 п. 1).
	edisclosureDocumentsFile = "documents.jsonl"

	// edisclosureSchemaVersion — единственная поддерживаемая версия манифеста.
	// Новая версия контракта потребует явной правки импортёра, а не молчаливого
	// разбора по старым правилам.
	edisclosureSchemaVersion = 1

	// edisclosureDateLayout — формат дат карточки (спека §4.2, таблица полей).
	edisclosureDateLayout = "02.01.2006"

	// edisclosureDocumentHost и edisclosureDocumentPath — строгая форма ссылки
	// на файл документа (спека §4.2): FileLoad.ashx — динамический обработчик,
	// и любой другой адрес означает, что разбирается не тот источник.
	edisclosureDocumentHost = "e-disclosure.ru"
	edisclosureDocumentPath = "/portal/FileLoad.ashx"
)

// edisclosureSections — допустимые разделы карточки (спека §4.2). Набор
// объявлен кодом, а не берётся из manifest.sections: список в манифесте
// описывает запрошенные разделы прогона, но не расширяет контракт источника.
var edisclosureSections = []string{"IFRS", "RAS", "AFFILIATES", "AGM"}

// manifest — manifest.json коллектора (спека §4.2). Поля empty_sections в
// структуре нет: это аудит коллектора, импортёр его игнорирует.
type manifest struct {
	SchemaVersion int    `json:"schema_version"`
	GeneratedAt   string `json:"generated_at"`
	Collector     struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	} `json:"collector"`
	Sections      []string  `json:"sections"`
	Companies     []company `json:"companies"`
	DocumentCount int       `json:"document_count"`
	DocumentsFile string    `json:"documents_file"`
}

// company — компания манифеста: тикер — ключ строк documents.jsonl, company_id —
// id карточки e-disclosure, с которым сверяется строка документа.
type company struct {
	Ticker    string `json:"ticker"`
	CompanyID string `json:"company_id"`
	Name      string `json:"name"`
	URL       string `json:"url"`
}

// document — сырая строка documents.jsonl (спека §4.2). Size — человекочитаемая
// метка источника, импортёр её игнорирует; даты остаются строками до проверки
// формата.
type document struct {
	Ticker          string `json:"ticker"`
	CompanyID       string `json:"company_id"`
	Section         string `json:"section"`
	DocType         string `json:"doc_type"`
	Period          string `json:"period"`
	BasisDate       string `json:"basis_date"`
	PublicationDate string `json:"publication_date"`
	Size            string `json:"size"`
	FileID          string `json:"fileid"`
	URL             string `json:"url"`
	TableNum        string `json:"table_num"`
}

// doc — разобранный документ: даты приведены к time.Time, а пустая дата — к
// нулевому времени (в таблицу она уходит NULL). Size в doc нет: контракт
// импортёра его не использует.
type doc struct {
	Ticker          string
	CompanyID       string
	Section         string
	DocType         string
	Period          string
	BasisDate       time.Time
	PublicationDate time.Time
	FileID          string
	URL             string
	TableNum        string
}

// readInput читает вход импортёра из каталога dir: сначала manifest.json, затем
// названный в нём documents.jsonl (спека §5.3 п. 1). Порядок обязателен: манифест
// пишется последним и является маркером завершённого прогона, поэтому разбор
// документов без валидного манифеста не начинается.
func readInput(dir string) (manifest, []doc, error) {
	manifestData, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return manifest{}, nil, fmt.Errorf("manifest.json: %w", err)
	}
	m, err := parseManifest(manifestData)
	if err != nil {
		return manifest{}, nil, fmt.Errorf("manifest.json: %w", err)
	}
	documentsData, err := os.ReadFile(filepath.Join(dir, m.DocumentsFile))
	if err != nil {
		return manifest{}, nil, fmt.Errorf("%s: %w", m.DocumentsFile, err)
	}
	docs, err := parseDocuments(documentsData, m)
	if err != nil {
		return manifest{}, nil, err
	}
	return m, docs, nil
}

// parseManifest разбирает и валидирует manifest.json. Проверяются только
// правила, которые видны в самом манифесте; document_count сверяется с числом
// строк в parseDocuments. Неизвестные поля (empty_sections) игнорируются — это
// аудит коллектора, а не часть контракта импортёра.
func parseManifest(data []byte) (manifest, error) {
	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return manifest{}, fmt.Errorf("json: %w", err)
	}
	if m.SchemaVersion != edisclosureSchemaVersion {
		return manifest{}, fmt.Errorf("schema_version = %d, want %d", m.SchemaVersion, edisclosureSchemaVersion)
	}
	if m.DocumentsFile != edisclosureDocumentsFile {
		return manifest{}, fmt.Errorf("documents_file = %q, want %q", m.DocumentsFile, edisclosureDocumentsFile)
	}
	if len(m.Companies) == 0 {
		return manifest{}, fmt.Errorf("companies: список пуст — манифест не описывает ни одной компании")
	}
	return m, nil
}

// parseDocuments разбирает documents.jsonl и валидирует каждую строку по
// контракту спеки §4.2. Ошибки называют номер строки файла (1-based). Дубликаты
// ключа (ticker, section, fileid) схлопываются только при полном совпадении
// разобранных полей; расхождение — ошибка: какая из версий документа настоящая,
// разбор не знает. document_count манифеста сверяется с числом уникальных
// документов — ровно так его считает коллектор после дедупликации.
func parseDocuments(data []byte, m manifest) ([]doc, error) {
	companies := make(map[string]company, len(m.Companies))
	for _, c := range m.Companies {
		companies[c.Ticker] = c
	}
	seen := make(map[string]doc)
	docs := make([]doc, 0)
	scanner := bufio.NewScanner(bytes.NewReader(data))
	line := 0
	for scanner.Scan() {
		line++
		raw := bytes.TrimSpace(scanner.Bytes())
		if len(raw) == 0 {
			continue
		}
		var row document
		if err := json.Unmarshal(raw, &row); err != nil {
			return nil, fmt.Errorf("%s: строка %d: json: %w", edisclosureDocumentsFile, line, err)
		}
		parsed, err := parseDocument(row, companies)
		if err != nil {
			return nil, fmt.Errorf("%s: строка %d: %w", edisclosureDocumentsFile, line, err)
		}
		key := docKey(parsed)
		if previous, ok := seen[key]; ok {
			if !sameDoc(previous, parsed) {
				return nil, fmt.Errorf("%s: строка %d: дубликат (%s, %s, %s) расходится содержимым с предыдущей строкой",
					edisclosureDocumentsFile, line, parsed.Ticker, parsed.Section, parsed.FileID)
			}
			continue
		}
		seen[key] = parsed
		docs = append(docs, parsed)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("%s: чтение: %w", edisclosureDocumentsFile, err)
	}
	if len(docs) != m.DocumentCount {
		return nil, fmt.Errorf("%s: document_count манифеста = %d, а уникальных документов %d",
			edisclosureDocumentsFile, m.DocumentCount, len(docs))
	}
	return docs, nil
}

// parseDocument проверяет одну строку documents.jsonl и приводит её к doc.
// CompanyID заполняется из манифеста, если в строке его нет; если есть —
// сверяется с манифестом. Остальные правила — таблица полей спеки §4.2.
func parseDocument(row document, companies map[string]company) (doc, error) {
	if row.Ticker == "" {
		return doc{}, fmt.Errorf("ticker: пустое значение")
	}
	if row.Ticker != strings.ToUpper(row.Ticker) {
		return doc{}, fmt.Errorf("ticker %q: ожидается верхний регистр", row.Ticker)
	}
	company, ok := companies[row.Ticker]
	if !ok {
		return doc{}, fmt.Errorf("ticker %q отсутствует в manifest.companies", row.Ticker)
	}
	if row.CompanyID != "" && row.CompanyID != company.CompanyID {
		return doc{}, fmt.Errorf("company_id %q не совпадает с %q для %s",
			row.CompanyID, company.CompanyID, row.Ticker)
	}
	if !allowedSection(row.Section) {
		return doc{}, fmt.Errorf("section %q: допустимые %s", row.Section, strings.Join(edisclosureSections, ", "))
	}
	if strings.TrimSpace(row.DocType) == "" {
		return doc{}, fmt.Errorf("doc_type: пустое значение")
	}
	if !digitsOnly(row.FileID) {
		return doc{}, fmt.Errorf("fileid %q: только цифры", row.FileID)
	}
	if err := validateDocumentURL(row.FileID, row.URL); err != nil {
		return doc{}, err
	}
	basisDate, err := parseDate(row.BasisDate)
	if err != nil {
		return doc{}, fmt.Errorf("basis_date: %w", err)
	}
	publicationDate, err := parseDate(row.PublicationDate)
	if err != nil {
		return doc{}, fmt.Errorf("publication_date: %w", err)
	}
	return doc{
		Ticker:          row.Ticker,
		CompanyID:       company.CompanyID,
		Section:         row.Section,
		DocType:         row.DocType,
		Period:          row.Period,
		BasisDate:       basisDate,
		PublicationDate: publicationDate,
		FileID:          row.FileID,
		URL:             row.URL,
		TableNum:        row.TableNum,
	}, nil
}

// parseDate разбирает дату карточки в формате ДД.ММ.ГГГГ; пустая строка — это
// отсутствие даты, а не ошибка (в таблицу она пишется как NULL).
func parseDate(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	date, err := time.Parse(edisclosureDateLayout, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("дата %q: ожидается формат ДД.ММ.ГГГГ", s)
	}
	return date, nil
}

// validateDocumentURL проверяет строгую форму ссылки на файл документа (спека
// §4.2): https, host e-disclosure.ru, path /portal/FileLoad.ashx и Fileid в
// query, совпадающий с fileid строки. Порт (в том числе пустой) и userinfo
// отвергаются: это не адрес источника, а подмена или прокси.
func validateDocumentURL(fileID, rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("url %q: %w", rawURL, err)
	}
	if u.Scheme != "https" {
		return fmt.Errorf("url %q: нужен https", rawURL)
	}
	if u.User != nil {
		return fmt.Errorf("url %q: userinfo в URL запрещён", rawURL)
	}
	if u.Host != edisclosureDocumentHost {
		return fmt.Errorf("url %q: host должен быть %s", rawURL, edisclosureDocumentHost)
	}
	if u.Path != edisclosureDocumentPath {
		return fmt.Errorf("url %q: path должен быть %s", rawURL, edisclosureDocumentPath)
	}
	if got := u.Query().Get("Fileid"); got != fileID {
		return fmt.Errorf("url %q: Fileid (%q) не совпадает с fileid %q", rawURL, got, fileID)
	}
	return nil
}

// docKey — идентичность документа (ticker, section, fileid) строкой с
// разделителем NUL: значения полей не содержат его, поэтому ключ однозначен.
// Используется дедупликацией разбора и гейтом импортёра (Task 4).
func docKey(d doc) string {
	return d.Ticker + "\x00" + d.Section + "\x00" + d.FileID
}

// sameDoc — полное совпадение разобранных документов, кроме размера из карточки
// (Size импортёр игнорирует). Даты сравниваются как моменты времени.
func sameDoc(a, b doc) bool {
	return a.Ticker == b.Ticker &&
		a.CompanyID == b.CompanyID &&
		a.Section == b.Section &&
		a.DocType == b.DocType &&
		a.Period == b.Period &&
		a.BasisDate.Equal(b.BasisDate) &&
		a.PublicationDate.Equal(b.PublicationDate) &&
		a.FileID == b.FileID &&
		a.URL == b.URL &&
		a.TableNum == b.TableNum
}

// allowedSection — входит ли раздел в объявленный набор спеки §4.2.
func allowedSection(section string) bool {
	for _, allowed := range edisclosureSections {
		if section == allowed {
			return true
		}
	}
	return false
}

// digitsOnly — непустая строка только из ASCII-цифр: fileid идентифицирует
// документ, и все прочие символы в нём — признак чужой строки.
func digitsOnly(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
