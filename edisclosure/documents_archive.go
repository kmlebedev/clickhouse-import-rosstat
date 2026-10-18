package edisclosure

import (
	"bytes"
	"strings"
	"unicode"
)

// Файл отвечает за имя версии в винтажном архиве (спека §5.5): ключ объекта
// должен читаться глазами и однозначно называть документ, поэтому имя
// собирается из ключа идентичности и санитизированного периода, а расширение
// берётся из сигнатуры реально скачанного тела, а не из URL: FileLoad.ashx —
// динамический обработчик и расширения в пути не несёт.

// archiveName — имя объекта архива:
// «<ticker>_<section>_<period>_<fileid><ext>». Период — текст источника
// («2026, 6 месяцев»), поэтому перед подстановкой в имя он санитизируется;
// fileid и ticker уже проверены разбором и безопасны как компонент пути.
func archiveName(d doc, ext string) string {
	return d.Ticker + "_" + d.Section + "_" + sanitizePeriod(d.Period) + "_" + d.FileID + ext
}

// bodyExt — расширение по сигнатуре тела: zip, rar и pdf узнаются по первым
// байтам, всё остальное сохраняется как .bin. Content-Type ответа не
// используется: источник может отдать его неверно, а сигнатура — это сами
// байты, которые попадут в архив.
func bodyExt(body []byte) string {
	switch {
	case bytes.HasPrefix(body, []byte("PK\x03\x04")):
		return ".zip"
	case bytes.HasPrefix(body, []byte("Rar!\x1a\x07")):
		return ".rar"
	case bytes.HasPrefix(body, []byte("%PDF")):
		return ".pdf"
	default:
		return ".bin"
	}
}

// sanitizePeriod превращает период карточки в компонент имени: пробелы (в том
// числе неразрывные), запятые и слэши заменяются на '_', серия разделителей
// схлопывается в один, крайние срезаются. Пустой результат — «na»: пустой
// компонент дал бы имя с двойным подчёркиванием, по которому версию не
// прочитать. Точки и буквы периода сохраняются: «31.12.2020» — валидное имя.
func sanitizePeriod(s string) string {
	var b strings.Builder
	separator := false
	for _, r := range s {
		if unicode.IsSpace(r) || r == ',' || r == '/' {
			separator = true
			continue
		}
		if separator && b.Len() > 0 {
			b.WriteByte('_')
		}
		separator = false
		b.WriteRune(r)
	}
	if b.Len() == 0 {
		return "na"
	}
	return b.String()
}
