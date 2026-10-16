package util

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/archive"
	log "github.com/sirupsen/logrus"
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
	// BodyFunc — разбор скачанных шаблоном байтов источников, отдающих не книгу excelize,
	// а документ другого формата (HTML бюллетеней Росстата): байты качает шаблон
	// (GetFileBytes), и тот же срез уходит в разбор и в винтажный архив версии (§9.6) —
	// в отличие от CrawFunc, который качает файл сам и в архив не попадает.
	BodyFunc func(body []byte, batch driver.Batch) error
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
	// self-heal до следующей волны источника (ARCHITECTURE §8 п. 13). У импортёров с гейтом по
	// своей таблице (cbr_dip_bulletin, rosstat_ipc_weeks_bulletins) шаблон на no-op-пути не
	// вызывается вовсе: судьба производных там — за собственным Import() импортёра
	// (rosstat_ipc_weeks_bulletins публикует их и при пустой дельте, cbr_dip_bulletin — нет).
	AfterImport func(ctx context.Context, conn driver.Conn) error
}

// PublicationDecl — объявленная импортёром точка контроля публикации: ключ версии (Source), URL
// артефакта (Url) и запрос числа точек (CountPoints). Url — объявленный (статический) URL
// артефакта: у импортёров с вычисляемой ссылкой (cbr_mo_br и три rosstat-импортёра, читающие
// ссылку со страницы) DataUrl заполняется только внутри Import(), поэтому здесь поле пусто —
// отпечаток префлайт строит по тому же DataUrl шаблона, а не по этой копии. Нулевое значение =
// префлайт выключен; по нему тест-энфорсер реестра импортёров отличает «объявление забыли» от
// «объявление не нужно» (спека §3.6).
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

// abortBatch закрывает батч на пути ошибки ДО Send (ImportFunc/BodyFunc/Append): соединение с
// открытым батчем возвращается в пул драйвера только через Send или Abort, поэтому серия
// ошибок разбора исчерпала бы пул и подвесила прогон в acquire() — найдено живым прогоном
// rosstat_ipc_weeks_bulletins (Wayback отдавал 429, каждый сбой оставлял батч открытым).
// После Send абортить нельзя: драйвер возвращает соединение в пул дефером самого Send
// независимо от исхода (clickhouse-go/v2, conn_batch.go), а Abort после Send отвечает
// ErrBatchAlreadySent — это был бы ложный warn «не удалось закрыть батч», а не лечение утечки.
// Ошибка Abort не проглатывается молча (`_ =` запрещён стилем пакета): она логируется warn,
// но возвращаемой ошибкой вызывающего остаётся первопричина.
func abortBatch(batch driver.Batch, what string) {
	if aerr := batch.Abort(); aerr != nil {
		log.Warnf("%s: не удалось закрыть батч после ошибки: %v", what, aerr)
	}
}

// importXlsBody разбирает книгу из памяти и отправляет её батчем: PrepareBatch → ImportFunc →
// Send. Байты вместо URL — ради архива версий: тот же срез уходит и в разбор, и в архив, а
// повторный GET по DataUrl удвоил бы трафик и время прогона (GetFileBytes). Внутренняя рука
// шаблона: снаружи её никто не зовёт, и публичным контрактом она не является.
func (s *ClickHouseImport) importXlsBody(ctx context.Context, body []byte, conn driver.Conn) (count int64, err error) {
	xlsx, err := excelize.OpenReader(bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("excelize %v", err)
	}
	defer func() { _ = xlsx.Close() }()
	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", s.TableName))
	if err != nil {
		return count, err
	}
	if err = s.ImportFunc(xlsx, batch); err != nil {
		abortBatch(batch, s.TableName)
		return count, err
	}
	count = int64(batch.Rows())
	if err = batch.Send(); err != nil {
		return count, err
	}
	return count, nil
}

// importBody разбирает байты разбором BodyFunc: PrepareBatch → BodyFunc → Send. Байты приходят
// срезом от шаблона — тот же срез ушёл и в архив версии, поэтому восстановленная из архива
// версия описывает ровно те точки, что разобраны.
func (s *ClickHouseImport) importBody(ctx context.Context, body []byte, conn driver.Conn) (count int64, err error) {
	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", s.TableName))
	if err != nil {
		return count, err
	}
	if err = s.BodyFunc(body, batch); err != nil {
		abortBatch(batch, s.TableName)
		return count, err
	}
	count = int64(batch.Rows())
	if err = batch.Send(); err != nil {
		return count, err
	}
	return count, nil
}

// Import — общий цикл импортёра: DDL -> BeforeImport -> префлайт -> точки (CrawFunc, ImportFunc,
// для HTML — BodyFunc) -> архив -> AfterImport -> версия. Префлайт включается объявлением
// PublicationCountPoints: после no-op прогон возвращает число точек известной версии, не скачивая
// файл (CrawFunc/ImportFunc/BodyFunc не вызываются).
// Архив версий файлов включается парой «хранилище настроено (ARCHIVE_BACKEND) + объявлена точка
// контроля»: он сохраняет ту же версию, по которой префлайт принимает решения, — объектом в
// хранилище и строкой реестра, — и его сбой закрывает только запись версии (спека §3.3).
// Строка версии пишется последней и только при непустом отпечатке: недоступный HEAD — не сбой, а
// деградация к полному импорту без записи версии (ARCHITECTURE §8 п. 13, спека §3.1).
//
// Режим восстановления (ARCHIVE_ACTION=restore, util.RestoreVersion) переключает тот же цикл на
// архив (спека §3.1): байты ImportFunc/BodyFunc берутся из хранилища вместо GetFileBytes, префлайт
// не выполняется вовсе (HEAD некуда делать — источник не спрашивается), повторной архивации нет
// (объект и строка уже на месте по построению резолва), а отпечаток строки версии берётся из
// строки реестра — прежний путь записи версии не меняется. CrawFunc-импортёры в режим не ходят: их
// файл шаблон не качает и в архив не сохраняет — граница шага 1.1, и это ошибка с явным текстом.
func (s *ClickHouseImport) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	// Режим читается один раз на прогон: значение не меняется посреди импорта, а env читается в
	// момент действия, не в init() (ARCHITECTURE §8.6).
	restore := ArchiveRestoreMode()
	if restore && s.CrawFunc != nil {
		// Ошибка до DDL, до сети и до хранилища: восстанавливать нечего — файл качает и разбирает
		// сам импортёр, шаблон его байтов не видит, объекта в архиве нет и не будет (спека §3.1).
		return 0, fmt.Errorf("%s: режим восстановления (%s=%s) не поддержан CrawFunc-импортёром: байты из архива берёт только шаблон (ImportFunc/BodyFunc)", s.TableName, EnvArchiveAction, ArchiveActionRestore)
	}
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
	// а «версию после импорта можно записать» — и пуст при недоступном HEAD. В режиме
	// восстановления он приходит из строки реестра (RestoreVersion), а префлайт пропускается
	// целиком: HEAD по DataUrl был бы обращением к источнику, которого restore не делает.
	var fingerprint string
	if s.PublicationCountPoints != "" && !restore {
		var noop bool
		var points uint64
		fingerprint, noop, points, err = PublicationPreflight(ctx, conn, s.PublicationKey(), s.DataUrl, s.PublicationCountPoints)
		if err != nil {
			return 0, err
		}
		if noop && fingerprint != "" {
			// «Версия известна» и «версия в архиве» — одно утверждение механизма: no-op при
			// отсутствующем объекте означал бы версию, которую из архива уже не восстановить
			// (спека §3.2). Проверка стоит один Store.Exists и включается теми же условиями, что и
			// архивация: архива нет — лишних вызовов нет.
			//
			// s.ImportFunc/s.BodyFunc — не украшение: архив покрывает только импортёров, чей файл
			// качает шаблон (запись архива живёт в ветке ImportFunc/BodyFunc, CrawFunc его байтов не
			// видит). BodyFunc-импортёр ходит тем же путём, что ImportFunc — байты качает шаблон и
			// сохраняет их в архив, — поэтому «версия известна, но объекта нет» обязано давать полный
			// импорт и у него. У CrawFunc-импортёра
			// с точкой контроля (bank/sber_fin_rez.go) объекта в архиве нет и не будет, поэтому проверка
			// на его no-op-пути была бы вечной деградацией: no-op не наступал бы ни разу, пока
			// ARCHIVE_BACKEND настроен, — ежедневный полный краул. CrawFunc не трогается (спека §3.3).
			if rt := activeArchive(); rt != nil && (s.ImportFunc != nil || s.BodyFunc != nil) {
				if exists, archiveErr := rt.versionExists(ctx, s.PublicationKey(), s.DataUrl, fingerprint); archiveErr != nil {
					log.Warnf("%s: версия %s известна, но проверка архива не удалась (%v) — полный импорт", s.PublicationKey(), fingerprint, archiveErr)
					noop = false
				} else if !exists {
					log.Warnf("%s: версия %s известна, но в архиве её нет — полный импорт", s.PublicationKey(), fingerprint)
					noop = false
				}
			}
		}
		if noop {
			return int64(points), nil
		}
	}
	// archiveFailed — сбой архивации версии. Успешный импорт данных сбой архива не отменяет, но
	// строки версии тогда нет: она утверждала бы версию, которой нет в архиве, и следующий прогон
	// ушёл бы в no-op мимо невосстановимой версии (спека §3.3).
	var archiveFailed bool
	switch {
	case s.CrawFunc != nil:
		count, err = s.CrawFunc(s.DataUrl, conn)
	case s.ImportFunc != nil || s.BodyFunc != nil:
		// Байты качаются один раз: тот же срез идёт в разбор и в архив версии. В режиме
		// восстановления срез приходит из архива, и сеть не вызывается вовсе (ни GET, ни HEAD):
		// источник для восстановления не нужен — версия уже лежит в хранилище (спека §3.1).
		var body []byte
		if restore {
			var entry archive.Entry
			if body, entry, err = RestoreVersion(ctx, s.PublicationKey(), s.DataUrl); err != nil {
				return count, err
			}
			fingerprint = entry.Fingerprint
			log.Infof("%s: версия из архива: %s (%s)", s.PublicationKey(), entry.Key, s.DataUrl)
		} else if body, err = GetFileBytes(s.DataUrl); err != nil {
			return count, err
		}
		if s.BodyFunc != nil {
			log.Infof("Get file %+v", s.DataUrl)
			if count, err = s.importBody(ctx, body, conn); err != nil {
				return count, err
			}
		} else {
			log.Infof("Get xlsx file %+v", s.DataUrl)
			if count, err = s.importXlsBody(ctx, body, conn); err != nil {
				return count, err
			}
		}
		// Архив — сразу после успешного Send: файл разобран и точки записаны, поэтому HTML-заглушка
		// WAF и битый файл в архив не попадают. Ветка CrawFunc архива не получает: файл качает сам
		// импортёр, и шаблон его байтов не видит (спека §1.5, стандарт §9.6). В режиме восстановления
		// повторной архивации нет: объект и строка уже на месте — ими и найдена версия (спека §3.1).
		if rt := activeArchive(); rt != nil && s.PublicationCountPoints != "" && !restore {
			if archiveErr := rt.save(ctx, s.PublicationKey(), s.DataUrl, body, fingerprint); archiveErr != nil {
				log.Errorf("%s: версия %s не сохранена в архиве (%v) — строка версии не пишется, следующий прогон повторит импорт", s.PublicationKey(), fingerprint, archiveErr)
				archiveFailed = true
			}
		}
	default:
		return count, fmt.Errorf("%s: no CrawFunc, ImportFunc or BodyFunc configured", s.TableName)
	}
	if err != nil {
		return count, err
	}
	// Производные — после точек и до строки версии: её ошибка оставляет версию незаписанной, и
	// следующий прогон повторит полный путь (идемпотентный), а не уйдёт в no-op с отсутствующим
	// у агента каталогом или витриной. У импортёров с гейтом по своей таблице полный путь не
	// повторяется — гейт видит артефакт записанным и шаблон для него не зовёт: производные их
	// no-op-пути публикует собственный Import() импортёра (rosstat_ipc_weeks_bulletins), а архив
	// и строку версии у уже записанного артефакта лечит только ручной прогон (§8 п. 13).
	if s.AfterImport != nil {
		if err = s.AfterImport(ctx, conn); err != nil {
			return count, err
		}
	}
	// PublicationCountPoints != "" в условии для обычного пути всегда истинно при непустом
	// отпечатке (его даёт только объявленный префлайт), а в режиме восстановления отсекает
	// импортёра без объявления: points версии считать нечем, и строка версии ему не положена — как
	// и раньше. В restore отпечаток пришёл из строки реестра, и строка версии пишется тем же путём.
	if fingerprint != "" && !archiveFailed && s.PublicationCountPoints != "" {
		// Таблицу версий в обычном пути заводит префлайт (EnsurePublications), а в режиме
		// восстановления он пропущен: на чистой базе строку версии писать некуда. Один Exec здесь,
		// до CountPoints и самого INSERT, — ровно там, где строка версии действительно будет записана.
		if restore {
			if err = EnsurePublications(ctx, conn); err != nil {
				return count, err
			}
		}
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
