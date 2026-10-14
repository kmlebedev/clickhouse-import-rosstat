package util

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"path"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	log "github.com/sirupsen/logrus"
)

// PublicationsTable — общая таблица точек контроля публикаций: одна строка = одна увиденная
// версия артефакта источника. Механизм общий для источников, чьи файлы обновляются реже
// прогонов, и включается полями шаблона util.ClickHouseImport (PublicationCountPoints /
// PublicationSource); фактический список потребителей — ARCHITECTURE §8 п. 13 (десять
// импортёров cbr/ и четыре вне него). Таблица внутренняя: агенту (kimi_reader) не выдаётся.
const PublicationsTable = "source_publications"

// PublicationsDdl — канонический DDL (ARCHITECTURE §6.2, спека §3.4). Литерал, а не %s-форма
// (как у пер-источниковых таблиц): таблица одна на все источники, %-шаблон здесь только
// размывал бы «канонический» статус. Комментарии колонок — часть спекового литерала и остаются
// в тексте SQL; ClickHouse их отбрасывает.
const PublicationsDdl = `CREATE TABLE IF NOT EXISTS source_publications (
    source LowCardinality(String),  -- источник-потребитель: 'cbr_mo_br', 'rosstat_ipc_mes', … (ключ версии: PublicationSource импортёра или его TableName)
    name LowCardinality(String),    -- имя артефакта: 'full.xlsx'
    fingerprint String,             -- md5 от (имя|ETag|Last-Modified|Content-Length) — отпечаток версии контента
    first_seen DateTime,            -- когда конвейер впервые увидел эту версию
    points UInt64,                  -- число точек ряда в таблице на момент версии (запрос PublicationCountPoints импортёра)
    url String                      -- URL артефакта (аудит)
) ENGINE = ReplacingMergeTree ORDER BY (source, name, fingerprint);
`

const (
	publicationsInsert = "INSERT INTO " + PublicationsTable
	// publicationsSelectPoints — ключ версии (source, name, fingerprint): имя входит в ключ,
	// потому что переименование файла у источника — тоже новая версия (спека §3.4, риски).
	publicationsSelectPoints = "SELECT points FROM " + PublicationsTable +
		" FINAL WHERE source = ? AND name = ? AND fingerprint = ? LIMIT 1"
)

// Publication — увиденная версия артефакта. FirstSeen — наблюдение конвейером (не дата
// публикации источника), Points — число записанных этой версией точек ряда.
type Publication struct {
	Source      string
	Name        string
	Fingerprint string
	Url         string
	FirstSeen   time.Time
	Points      uint64
}

// EnsurePublications создаёт общую таблицу версий, если её ещё нет. Идемпотентна: вызывается
// каждым прогоном импортёра до обращения к таблице.
func EnsurePublications(ctx context.Context, conn driver.Conn) error {
	if err := conn.Exec(ctx, PublicationsDdl); err != nil {
		return fmt.Errorf("%s: %w", PublicationsTable, err)
	}
	return nil
}

// HeadFingerprint возвращает отпечаток версии артефакта по URL: md5hex от
// «имя|ETag|Last-Modified|Content-Length». Content-MD5 источники (в частности ЦБ) не отдают,
// но композит заголовков меняется ровно при смене файла; пропажа заголовка не ломает — компонент
// остаётся пустым, а не выпадает из композита (иначе композит без ETag совпал бы с композитом с
// ETag). Не-2xx и ошибка сети — ошибка: отпечаток «из пустых заголовков» превратил бы битую
// ссылку в валидную версию, и импортёр счёл бы файл уже импортированным. Отпечаток — не секрет
// и не защита от подмены: он лишь отвечает на вопрос «это тот же файл, что импортировали?».
func HeadFingerprint(url string) (fingerprint string, err error) {
	// Дедлайн — как у GetFile (util/xls.go): HttpClient создан без Timeout, и зависший ответ
	// источника без дедлайна ошибки не даёт вовсе — импортёр ждал бы его вместо отчёта о сбое.
	// Тот же fileTimeout, а не своя константа: HEAD и GET ходят к одному хосту ради одного файла.
	ctx, cancel := context.WithTimeout(context.Background(), fileTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", randomUA)
	resp, err := HttpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return "", fmt.Errorf("HEAD %s: статус %d", url, resp.StatusCode)
	}
	// Тела у HEAD нет — в отличие от GetFile, пустое тело здесь норма, проверять нечего.
	// basename — из пути URL (как его считает и импортёр для name): смена имени файла меняет
	// отпечаток, а значит и версию.
	composite := path.Base(url) + "|" + resp.Header.Get("ETag") + "|" +
		resp.Header.Get("Last-Modified") + "|" + resp.Header.Get("Content-Length")
	sum := md5.Sum([]byte(composite))
	return hex.EncodeToString(sum[:]), nil
}

// PublicationKnown отвечает, импортировалась ли уже эта версия артефакта, и сколько точек она
// тогда записала: по этому числу импортёр сверяет фактическое содержимое витрины и решает,
// деградировать ли в полный импорт (self-heal). Отсутствие строки — не ошибка: это штатное
// «версия новая» (known = false).
func PublicationKnown(ctx context.Context, conn driver.Conn, source, name, fingerprint string) (points uint64, known bool, err error) {
	err = conn.QueryRow(ctx, publicationsSelectPoints, source, name, fingerprint).Scan(&points)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return points, true, nil
}

// SavePublication записывает увиденную версию артефакта. Вызывается только после успешного
// Send точек: строка версии — утверждение «эти точки записаны», и она не должна опережать данные.
// Одна строка всё равно идёт батчем (PrepareBatch → Append → Send) — правило проекта: без него
// нет единообразия и не работает err! на Append.
func SavePublication(ctx context.Context, conn driver.Conn, p Publication) error {
	batch, err := conn.PrepareBatch(ctx, publicationsInsert)
	if err != nil {
		return err
	}
	if err = batch.Append(p.Source, p.Name, p.Fingerprint, p.FirstSeen, p.Points, p.Url); err != nil {
		return err
	}
	return batch.Send()
}

// CountPoints выполняет countPoints-запрос импортёра и возвращает число точек его ряда. Один и
// тот же запрос служит и количественным чеком известной версии, и значением points записываемой
// версии (спека §3.2): сравнивать и записывать надо одну величину. batch.Rows() для этой роли не
// годится — он считает ячейки листа, а ReplacingMergeTree схлопывает дубли по ключу
// (cbr_households_b_mes: 9030 ячеек против 7350 точек), и записанное число не сошлось бы с
// count() никогда, то есть no-op не наступил бы ни разу.
func CountPoints(ctx context.Context, conn driver.Conn, query string) (points uint64, err error) {
	if err = conn.QueryRow(ctx, query).Scan(&points); err != nil {
		return 0, fmt.Errorf("count точек: %w", err)
	}
	return points, nil
}

// PublicationPreflight — лёгкая проверка «версия артефакта уже импортирована?»: HEAD по url даёт
// отпечаток версии (HeadFingerprint), source_publications отвечает, знакома ли она, и, если
// знакома, countPoints-запрос импортёра подтверждает, что точки на месте. Возврат — решение для
// вызывающего: noop = true значит «файл не скачивать и в БД не писать ничего, а points — число
// точек прогона»; непустой отпечаток без noop — «полный импорт, после которого записать версию».
//
// Ни одна из неудач не является ошибкой прогона — все ведут в полный импорт:
//   - HEAD недоступен (сеть, не-2xx): отпечатка нет, а значит и версию записывать нельзя — иначе
//     следующая волна считалась бы уже импортированной, и данные остались бы устаревшими;
//   - SELECT версии не удался: сбой диагностики старой записи не должен ронять ежедневный
//     прогон — полный импорт идемпотентен на ReplacingMergeTree, версия потом перезапишется;
//   - число точек не сошлось: версия записана, а точек в таблице не столько (ручная чистка,
//     недоехавший батч) — self-heal полным импортом, а не тихий no-op.
//
// Логи перенесены из первого потребителя (cbr/mo_br) дословно, включая текст no-op.
func PublicationPreflight(ctx context.Context, conn driver.Conn, source, url, countPoints string) (fingerprint string, noop bool, points uint64, err error) {
	fingerprint, headErr := HeadFingerprint(url)
	if headErr != nil {
		log.Warnf("%s: HEAD %s недоступен (%v) — полный импорт без записи версии", source, url, headErr)
		return "", false, 0, nil
	}
	if err = EnsurePublications(ctx, conn); err != nil {
		return "", false, 0, err
	}
	// basename — как и у записи версии в шаблоне: имя входит в ключ версии, поэтому смена имени
	// файла у источника — тоже новая версия (спека §3.4, риски).
	name := path.Base(url)
	recorded, known, knownErr := PublicationKnown(ctx, conn, source, name, fingerprint)
	if knownErr != nil {
		log.Warnf("%s: проверка версии %s в %s не удалась (%v) — полный импорт", source, fingerprint, PublicationsTable, knownErr)
		return fingerprint, false, 0, nil
	}
	if !known {
		return fingerprint, false, 0, nil
	}
	// Сбой запроса числа точек — реальная проблема БД (в отличие от недоступного SELECT версии):
	// без него не сходится ни чек, ни points будущей записи, поэтому прогон останавливается.
	dbPoints, countErr := CountPoints(ctx, conn, countPoints)
	if countErr != nil {
		return fingerprint, false, 0, fmt.Errorf("%s: %w", source, countErr)
	}
	if dbPoints == recorded {
		log.Infof("%s: no-op, версия %s уже импортирована, %d точек на месте", source, fingerprint, recorded)
		return fingerprint, true, recorded, nil
	}
	log.Warnf("%s: версия %s известна, но в %s %d точек вместо %d — полный импорт", source, fingerprint, countPoints, dbPoints, recorded)
	return fingerprint, false, 0, nil
}
