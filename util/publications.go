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
)

// PublicationsTable — общая таблица точек контроля публикаций: одна строка = одна увиденная
// версия артефакта источника. Механизм общий для источников, чьи файлы обновляются реже
// прогонов (первый потребитель — cbr_mo_br; спека §3.4, ARCHITECTURE §6.2). Таблица
// внутренняя: агенту (kimi_reader) не выдаётся.
const PublicationsTable = "source_publications"

// PublicationsDdl — канонический DDL (ARCHITECTURE §6.2, спека §3.4). Литерал, а не %s-форма
// (как у пер-источниковых таблиц): таблица одна на все источники, %-шаблон здесь только
// размывал бы «канонический» статус. Комментарии колонок — часть спекового литерала и остаются
// в тексте SQL; ClickHouse их отбрасывает.
const PublicationsDdl = `CREATE TABLE IF NOT EXISTS source_publications (
    source LowCardinality(String),  -- источник-потребитель: 'cbr_mo_br'
    name LowCardinality(String),    -- имя артефакта: 'full.xlsx'
    fingerprint String,             -- md5 от (имя|ETag|Last-Modified|Content-Length) — отпечаток версии контента
    first_seen DateTime,            -- когда конвейер впервые увидел эту версию
    points UInt64,                  -- число точек (строк series,date), записанных этой версией
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
