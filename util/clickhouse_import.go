package util

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/xuri/excelize/v2"
)

const HttpUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"

type ClickHouseImport struct {
	TableName   string
	CreateTable []string
	DataUrl     string
	TimeLayout  string
	CrawFunc    func(crawUrl string, conn driver.Conn) (count int64, err error)
	ImportFunc  func(xlsx *excelize.File, batch driver.Batch) error
	// BeforeImport вычисляет DataUrl во время Import(), а не в init():
	// сетевые вызовы в init() запрещены (ARCHITECTURE.md §8.6).
	BeforeImport func(ctx context.Context, conn driver.Conn) error
	// PublicationCountPoints — SQL, возвращающий число точек ряда импортёра. Непустое значение
	// включает точку контроля публикации: HEAD-префлайт по DataUrl, no-op известной версии и
	// строка версии в source_publications последней (ARCHITECTURE §6.2, спека §3.1). Запрос
	// обязан иметь FINAL, а у владельцев общей macro_series — ещё и WHERE source = '<…>':
	// иначе чек считал бы несмёрженные части или чужие ряды (спека §3.5).
	PublicationCountPoints string
	// PublicationSource — ключ версии в source_publications и префикс логов префлайта; пусто ->
	// TableName. Нужен импортёрам, чей Name() не равен TableName (cbr_mo_br, cbr_mp_survey
	// пишут в общую macro_series).
	PublicationSource string
	// AfterImport вызывается после успешного Send точек и до записи строки версии: здесь
	// импортёр публикует производные (series_catalog, витрину). Ошибка отменяет запись версии —
	// порядок «точки -> производные -> версия», иначе сбой публикации каталога заблокировал бы
	// self-heal до следующей волны источника (ARCHITECTURE §8 п. 13).
	AfterImport func(ctx context.Context, conn driver.Conn) error
}

// PublicationDecl — объявленная импортёром точка контроля публикации: ключ версии (Source), URL
// артефакта и запрос числа точек. Нулевое значение = префлайт выключен; по нему тест-энфорсер
// реестра импортёров отличает «объявление забыли» от «объявление не нужно» (спека §3.6).
type PublicationDecl struct {
	Source      string
	Url         string
	CountPoints string
}

// PublicationKey — ключ версии в source_publications и префикс логов префлайта.
// PublicationSource сильнее TableName: у владельцев общей macro_series таблица чужая для ключа —
// по ней версии двух импортёров смешались бы в одну строку.
func (s *ClickHouseImport) PublicationKey() string {
	if s.PublicationSource != "" {
		return s.PublicationSource
	}
	return s.TableName
}

// Publication — точка контроля публикации, как её объявил импортёр. Пустой
// PublicationCountPoints (все импортёры репозитория, кроме переведённых) даёт нулевое значение:
// префлайта нет.
func (s *ClickHouseImport) Publication() PublicationDecl {
	if s.PublicationCountPoints == "" {
		return PublicationDecl{}
	}
	return PublicationDecl{
		Source:      s.PublicationKey(),
		Url:         s.DataUrl,
		CountPoints: s.PublicationCountPoints,
	}
}

func (s *ClickHouseImport) Name() string {
	return s.TableName
}

func (s *ClickHouseImport) ImportXls(ctx context.Context, dataUrl string, conn driver.Conn) (count int64, err error) {
	xlsx, err := GetXlsx(dataUrl)
	if err != nil {
		return 0, err
	}
	defer func() { _ = xlsx.Close() }()
	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", s.TableName))
	if err != nil {
		return count, err
	}
	if err = s.ImportFunc(xlsx, batch); err != nil {
		return count, err
	}
	count = int64(batch.Rows())
	if err = batch.Send(); err != nil {
		return count, err
	}
	return count, nil
}

// Import — общий цикл импортёра: DDL -> BeforeImport -> префлайт -> точки -> AfterImport ->
// версия. Префлайт включается объявлением PublicationCountPoints: после no-op прогон
// возвращает число точек известной версии, не скачивая файл (CrawFunc/ImportXls не вызываются).
// Строка версии пишется последней и только при непустом отпечатке: недоступный HEAD — не сбой, а
// деградация к полному импорту без записи версии (ARCHITECTURE §8 п. 13, спека §3.1).
func (s *ClickHouseImport) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	for _, sql := range s.CreateTable {
		if err = conn.Exec(ctx, fmt.Sprintf(sql, s.TableName)); err != nil {
			return 0, err
		}
	}
	if s.BeforeImport != nil {
		if err = s.BeforeImport(ctx, conn); err != nil {
			return 0, err
		}
	}
	// Отпечаток объявлен здесь, а не выведен из Publication(): он говорит не «префлайт включён»,
	// а «версию после импорта можно записать» — и пуст при недоступном HEAD.
	var fingerprint string
	if s.PublicationCountPoints != "" {
		var noop bool
		var points uint64
		fingerprint, noop, points, err = PublicationPreflight(ctx, conn, s.PublicationKey(), s.DataUrl, s.PublicationCountPoints)
		if err != nil {
			return 0, err
		}
		if noop {
			return int64(points), nil
		}
	}
	switch {
	case s.CrawFunc != nil:
		count, err = s.CrawFunc(s.DataUrl, conn)
	case s.ImportFunc != nil:
		count, err = s.ImportXls(ctx, s.DataUrl, conn)
	default:
		return count, fmt.Errorf("%s: no CrawFunc or ImportFunc configured", s.TableName)
	}
	if err != nil {
		return count, err
	}
	// Производные — после точек и до строки версии: её ошибка оставляет версию незаписанной, и
	// следующий прогон повторит полный путь (идемпотентный), а не уйдёт в no-op с отсутствующим
	// у агента каталогом или витриной.
	if s.AfterImport != nil {
		if err = s.AfterImport(ctx, conn); err != nil {
			return count, err
		}
	}
	if fingerprint != "" {
		var points uint64
		// points версии — результат запроса импортёра, а не batch.Rows() (спека §3.2): только так
		// чек известной версии сойдётся на повторном прогоне.
		if points, err = CountPoints(ctx, conn, s.PublicationCountPoints); err != nil {
			return count, fmt.Errorf("%s: %w", s.PublicationKey(), err)
		}
		if err = SavePublication(ctx, conn, Publication{
			Source: s.PublicationKey(),
			// Имя — basename URL: оно входит в ключ версии, а у cbr_mo_br в БД уже лежит строка
			// с именем full.xlsx, и расхождение создало бы вторую версию того же файла.
			Name:        path.Base(s.DataUrl),
			Fingerprint: fingerprint,
			Url:         s.DataUrl,
			FirstSeen:   time.Now(),
			Points:      points,
		}); err != nil {
			return count, err
		}
	}
	return count, nil
}
