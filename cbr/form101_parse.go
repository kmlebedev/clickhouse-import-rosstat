package cbr

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/nwaples/rardecode/v2"
	"golang.org/x/text/encoding/charmap"
)

// Разбор сырого артефакта формы 101 ЦБ: страница отчётности → ссылка на RAR →
// DBF остатков (B1.dbf). Все функции чистые — сеть не трогают, байты приходят
// от вызывающего (Import), поэтому тесты идут офлайн на фикстурах.

const (
	// form101LinkSel — ссылки на архивы формы 101 на странице отчётности ЦБ.
	// Селектор вынесен константой (стандарт §4.5): тот же href использует тест
	// на HTML-фикстуре.
	form101LinkSel = `a[href*="/vfs/credit/forms/101-"]`

	// form101DateLayout — формат даты в имени файла 101-YYYYMMDD.rar: отчётная
	// дата, а не месяц выпуска (101-20260901.rar → 2026-09-01).
	form101DateLayout = "20060102"

	// form101DayLayout — формат дат в текстах ошибок и логах импортёра.
	form101DayLayout = "2006-01-02"

	// form101EntrySuffix — имя DBF с остатками внутри RAR: 082026B1.dbf.
	// Сравнивается без учёта регистра: источник может отдать b1.dbf.
	form101EntrySuffix = "b1.dbf"
)

var (
	// form101LinkRe — дата отчётности из href вида /vfs/credit/forms/101-20260901.rar.
	// Ссылки 102-* (форма 102) и прочие файлы страницы не совпадают.
	form101LinkRe = regexp.MustCompile(`101-(\d{8})\.rar`)

	// errForm101NoB1 — в архиве нет файла *B1.dbf: это смена состава архива,
	// а не «остатков нет».
	errForm101NoB1 = errors.New("в RAR нет файла *B1.dbf")
)

// form101Row — одна строка остатков формы 101: счёт банка, план счетов,
// актив/пассив и итоговый остаток VITG. Единица — тыс. руб., как в источнике;
// маппинг счетов на метрики кредитования — следующий шаг, не этот.
type form101Row struct {
	Regnum        uint32
	Account       string
	Plan          string
	ActivePassive string
	BalanceTotal  float64
}

// form101Field — дескриптор поля DBF: имя, смещение в записи (после флага
// удаления) и длина в байтах.
type form101Field struct {
	name   string
	offset int
	length int
}

// parseForm101DBF разбирает dBase III (CP866) остатков формы 101. Заголовок:
// число записей uint32 @4, длина заголовка uint16 @8, длина записи uint16 @10.
// Дескрипторы полей идут с 32 до терминатора 0x0D. Числовые поля — ASCII,
// выровнены вправо; пустое числовое поле в dBase означает ноль. Удалённая
// запись (первый байт '*') пропускается. Строка с пустым NUM_SC не является
// остатком по счёту и пропускается: в источнике встречается строка-агрегат
// ITGAP без счёта. Любая структурная ошибка возвращается с контекстом
// записи/поля — молчаливая потеря строки недопустима (стандарт §6.4).
func parseForm101DBF(data []byte) ([]form101Row, error) {
	const (
		dbfHeaderMin    = 12 // служебная часть заголовка dBase III
		dbfFieldDescLen = 32 // длина одного дескриптора поля
		dbfHeaderFixed  = 32 // смещение первого дескриптора
		dbfTerminator   = 0x0D
		dbfDeleted      = '*'
		dbfLive         = ' '
	)
	if len(data) < dbfHeaderMin {
		return nil, fmt.Errorf("DBF формы 101: файл короче заголовка (%d байт)", len(data))
	}
	records := binary.LittleEndian.Uint32(data[4:8])
	headerLen := int(binary.LittleEndian.Uint16(data[8:10]))
	recordLen := int(binary.LittleEndian.Uint16(data[10:12]))
	if headerLen < dbfHeaderFixed+1 || headerLen > len(data) {
		return nil, fmt.Errorf("DBF формы 101: длина заголовка %d вне файла (%d байт)", headerLen, len(data))
	}
	if recordLen < 1 {
		return nil, fmt.Errorf("DBF формы 101: длина записи %d — нет даже флага удаления", recordLen)
	}

	fields := make(map[string]form101Field, 8)
	offset := 1 // первый байт записи — флаг удаления
	pos := dbfHeaderFixed
	for {
		if pos >= headerLen {
			return nil, fmt.Errorf("DBF формы 101: нет терминатора полей 0x0D (позиция %d, заголовок %d)", pos, headerLen)
		}
		if data[pos] == dbfTerminator {
			break
		}
		if pos+dbfFieldDescLen > headerLen {
			return nil, fmt.Errorf("DBF формы 101: дескрипторы полей обрываются на %d (заголовок %d)", pos, headerLen)
		}
		desc := data[pos : pos+dbfFieldDescLen]
		name := string(bytes.TrimRight(desc[:11], "\x00"))
		if name == "" {
			return nil, fmt.Errorf("DBF формы 101: пустое имя поля в дескрипторе @%d", pos)
		}
		length := int(desc[16])
		if length == 0 {
			return nil, fmt.Errorf("DBF формы 101: поле %q нулевой длины", name)
		}
		if _, exists := fields[name]; exists {
			return nil, fmt.Errorf("DBF формы 101: поле %q встречается дважды", name)
		}
		fields[name] = form101Field{name: name, offset: offset, length: length}
		offset += length
		pos += dbfFieldDescLen
	}
	if offset > recordLen {
		return nil, fmt.Errorf("DBF формы 101: сумма полей %d больше длины записи %d", offset, recordLen)
	}

	// Обязательные поля строки: без любого из них адрес остатка не построить.
	required := []string{"REGN", "PLAN", "NUM_SC", "A_P", "VITG"}
	for _, name := range required {
		if _, ok := fields[name]; !ok {
			return nil, fmt.Errorf("DBF формы 101: нет обязательного поля %q", name)
		}
	}

	// До аллокации: испорченный record count (0xFFFFFFFF) иначе заказал бы
	// гигантский срез до первой проверки границ.
	if uint64(headerLen)+uint64(records)*uint64(recordLen) > uint64(len(data)) {
		return nil, fmt.Errorf("DBF формы 101: записей %d × %d байт не помещаются в файл %d байт", records, recordLen, len(data))
	}
	rows := make([]form101Row, 0, records)
	for i := uint32(0); i < records; i++ {
		start := headerLen + int(i)*recordLen
		end := start + recordLen
		if end > len(data) {
			return nil, fmt.Errorf("DBF формы 101: запись %d обрывается: нужно до %d, файл %d байт", i, end, len(data))
		}
		rec := data[start:end]
		switch rec[0] {
		case dbfDeleted:
			continue
		case dbfLive:
		default:
			return nil, fmt.Errorf("DBF формы 101: запись %d: неизвестный флаг %q", i, rec[0])
		}

		regnumRaw, err := form101FieldText(rec, fields["REGN"])
		if err != nil {
			return nil, fmt.Errorf("DBF формы 101: запись %d: REGN: %w", i, err)
		}
		regnum, err := strconv.ParseUint(strings.TrimSpace(regnumRaw), 10, 32)
		if err != nil {
			return nil, fmt.Errorf("DBF формы 101: запись %d: REGN %q: %w", i, regnumRaw, err)
		}
		account, err := form101FieldText(rec, fields["NUM_SC"])
		if err != nil {
			return nil, fmt.Errorf("DBF формы 101: запись %d: NUM_SC: %w", i, err)
		}
		if account == "" {
			// Строка без счёта — не остаток по счёту (в источнике такие строки есть).
			continue
		}
		plan, err := form101FieldText(rec, fields["PLAN"])
		if err != nil {
			return nil, fmt.Errorf("DBF формы 101: запись %d: PLAN: %w", i, err)
		}
		activePassive, err := form101FieldText(rec, fields["A_P"])
		if err != nil {
			return nil, fmt.Errorf("DBF формы 101: запись %d: A_P: %w", i, err)
		}
		balance, err := form101FieldFloat(rec, fields["VITG"])
		if err != nil {
			return nil, fmt.Errorf("DBF формы 101: запись %d (REGN=%d, счёт %q): VITG: %w", i, regnum, account, err)
		}
		rows = append(rows, form101Row{
			Regnum:        uint32(regnum),
			Account:       account,
			Plan:          plan,
			ActivePassive: activePassive,
			BalanceTotal:  balance,
		})
	}
	return rows, nil
}

// form101FieldText декодирует символьное поле записи из CP866. Хвостовые NUL и
// пробелы снимаются: NUM_SC в реальном B1.dbf дополнен NUL-байтами ("501\x00\x00"),
// а не пробелами, и без снятия NUL счёт стал бы непечатаемой строкой.
func form101FieldText(rec []byte, field form101Field) (string, error) {
	raw := rec[field.offset : field.offset+field.length]
	raw = bytes.Trim(raw, "\x00 ")
	if len(raw) == 0 {
		return "", nil
	}
	decoded, err := charmap.CodePage866.NewDecoder().Bytes(raw)
	if err != nil {
		return "", fmt.Errorf("поле %q: декодирование CP866: %w", field.name, err)
	}
	return strings.TrimSpace(string(decoded)), nil
}

// form101FieldFloat разбирает числовое поле dBase (ASCII, выровнено вправо).
// Пустое поле — ноль по конвенции dBase. NaN и Inf отвергаются: в остатках
// формы 101 их быть не может, а в Float64 ClickHouse они ушли бы молча.
func form101FieldFloat(rec []byte, field form101Field) (float64, error) {
	text, err := form101FieldText(rec, field)
	if err != nil {
		return 0, err
	}
	raw := strings.TrimSpace(text)
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("значение %q: %w", raw, err)
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, fmt.Errorf("значение %q не конечно", raw)
	}
	return value, nil
}

// latestForm101Link выбирает самый свежий архив формы 101 на странице отчётности
// ЦБ. Ссылки отбираются селектором form101LinkSel, дата берётся из имени файла;
// возвращается абсолютный URL (href в разметке относительный) и дата отчётности.
// Ноль ссылок — ошибка, а не «новых данных нет»: это смена разметки страницы,
// и молчаливый no-op оставил бы импортёр без источника навсегда.
func latestForm101Link(page []byte) (time.Time, string, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(page))
	if err != nil {
		return time.Time{}, "", fmt.Errorf("страница формы 101: разбор html: %w", err)
	}
	var (
		latest   time.Time
		link     string
		parseErr error
	)
	doc.Find(form101LinkSel).Each(func(_ int, a *goquery.Selection) {
		if parseErr != nil {
			return
		}
		href, ok := a.Attr("href")
		if !ok {
			return
		}
		href = strings.TrimSpace(href)
		match := form101LinkRe.FindStringSubmatch(href)
		if match == nil {
			return
		}
		date, err := time.Parse(form101DateLayout, match[1])
		if err != nil {
			parseErr = fmt.Errorf("страница формы 101: ссылка %q: дата %q: %w", href, match[1], err)
			return
		}
		if latest.IsZero() || date.After(latest) {
			latest = date
			if strings.HasPrefix(href, "/") {
				href = cbrUrl + href
			}
			link = href
		}
	})
	if parseErr != nil {
		return time.Time{}, "", parseErr
	}
	if link == "" {
		return time.Time{}, "", fmt.Errorf("страница формы 101: по селектору %q не найдено ни одной ссылки 101-*.rar", form101LinkSel)
	}
	return latest, link, nil
}

// extractForm101B1 достаёт из RAR (v4/v5) файл остатков *B1.dbf. Архив формы 101
// содержит ещё NAMES.dbf и N1.dbf (банки), но импортёру нужен только B1.
// Суффикс сравнивается без учёта регистра; отсутствие файла — ошибка errForm101NoB1:
// это смена состава архива, а не пустой источник.
func extractForm101B1(rarBytes []byte) ([]byte, error) {
	reader, err := rardecode.NewReader(bytes.NewReader(rarBytes))
	if err != nil {
		return nil, fmt.Errorf("RAR формы 101: %w", err)
	}
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return nil, errForm101NoB1
		}
		if err != nil {
			return nil, fmt.Errorf("RAR формы 101: %w", err)
		}
		if header.IsDir || !strings.HasSuffix(strings.ToLower(header.Name), form101EntrySuffix) {
			continue
		}
		body, err := io.ReadAll(reader)
		if err != nil {
			return nil, fmt.Errorf("RAR формы 101: чтение %q: %w", header.Name, err)
		}
		if len(body) == 0 {
			return nil, fmt.Errorf("RAR формы 101: файл %q пуст", header.Name)
		}
		return body, nil
	}
}

// form101Newer сообщает, нужен ли импорт: дата отчётности со страницы строго
// новее максимальной уже записанной. Сравнение — по календарным суткам UTC:
// ClickHouse Date и дата из имени файла приходят без времени. Чистая функция,
// чтобы гейт «повтор — no-op» проверялся без ClickHouse.
func form101Newer(latest, known time.Time) bool {
	return form101Day(latest).After(form101Day(known))
}

// form101Day нормализует момент к суткам UTC — ключ даты в таблице и в имени файла.
func form101Day(t time.Time) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}
