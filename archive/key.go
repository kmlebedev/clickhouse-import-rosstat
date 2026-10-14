package archive

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	// keyNameMaxBytes — потолок имени артефакта в ключе: 200 байт. Компонент пути в хранилище
	// ограничен 255 байтами (имя файла на Диске, NAME_MAX в ФС), а имя делит компонент с префиксом
	// версии (≤20 байт: «sha-» и 16 hex) и '_'. Кириллица — два байта на символ, поэтому потолок
	// режется по границе руны, а не по байту: см. sanitizeName.
	keyNameMaxBytes = 200

	// keyNameFallback — имя для пустого basename: имя приходит из URL, и пустой путь дал бы
	// ключ с хвостом «_» — такой объект не читается ни в логе, ни глазами в каталоге архива.
	keyNameFallback = "file"

	// keyPrefixHex — длина префикса версии в ключе: 16 символов отпечатка (64 бита). Версии одного
	// файла так различаются с большим запасом, и те же 16 символов видно в логе и в строке
	// source_publications — ключ архива и таблица версий должны называть версию одинаково.
	keyPrefixHex = 16
)

// Key — ключ объекта версии: «<источник>/<префикс версии>_<имя>». Префикс — из отпечатка версии,
// который даёт префлайт (util.HeadFingerprint): по нему архив и source_publications говорят об
// одной версии, а проверка «версия уже в архиве?» на no-op-пути обходится без чтения файла.
// Отпечатка нет (HEAD недоступен) → префикс из sha256 содержимого: без него две версии одного
// файла схлопнулись бы в один ключ, и новая вытеснила бы прежнюю — ровно та потеря, ради которой
// архив заведён.
func Key(source, basename, fingerprint string, sum [sha256.Size]byte) string {
	prefix := prefix16(fingerprint)
	if prefix == "" {
		prefix = "sha-" + prefix16(hex.EncodeToString(sum[:]))
	}
	return source + "/" + prefix + "_" + sanitizeName(basename)
}

// prefix16 — не более 16 символов. Отпечаток короче 16 — нештатная ситуация (HeadFingerprint
// всегда даёт 32 hex), но падать на срезе нельзя: сбой сборки ключа уронил бы импорт, тогда как
// короткий префикс версию всё равно называет, а пустой склеил бы разные версии одного файла.
func prefix16(s string) string {
	if len(s) > keyPrefixHex {
		return s[:keyPrefixHex]
	}
	return s
}

// sanitizeName готовит basename к роли компонента ключа: имя приходит из URL источника, а ключ
// становится путём в хранилище (у LocalStore — путём файловой системы), поэтому разделители пути
// и запрещённые в именах символы заменяются на '_'. Управляющие символы убираются вместе с ними:
// многострочные имена ломают и лог, и список объектов. Кириллица сохраняется — выгрузки ЦБ и
// Росстата названы по-русски, и переименование исказило бы аудит версии.
func sanitizeName(name string) string {
	clean := strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || strings.ContainsRune(`/\:*?"<>|`, r) {
			return '_'
		}
		return r
	}, name)
	if len(clean) > keyNameMaxBytes {
		// utf8.RuneStart: отступ от потолка до начала руны — разрезанная руна сделала бы имя
		// объекта невосстановимым (в ключе остались бы битые байты).
		cut := keyNameMaxBytes
		for cut > 0 && !utf8.RuneStart(clean[cut]) {
			cut--
		}
		clean = clean[:cut]
	}
	if clean == "" {
		return keyNameFallback
	}
	return clean
}
