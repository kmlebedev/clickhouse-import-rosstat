package cbr

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/PuerkitoBio/goquery"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
)

// Импортёр приложений к бюллетеню ЦБ «О чём говорят тренды» (ДИП) — таблица cbr_dip_bulletin:
// восемь сезонно сглаженных рядов (SAAR-инфляция и рынок труда) вместе с винтажными колонками
// (номер выпуска и дата его публикации). Артефакт импортёра — не один файл, а набор выпусков
// (49 на момент спеки), поэтому у импортёра свой Import() (прецедент — cbr_mp_summary, спека
// §3.5): страницу выпусков разбирает cbr/dip_page.go, книгу — cbr/dip_parse.go, а файл каждого
// выпуска качает шаблон util.ClickHouseImport (DataUrl + ImportFunc) — иначе не получились бы
// ни архив версии файла, ни строка source_publications (стандарт §9.6, отступление 1 спеки §5).

const (
	// cbrDipDdl — DDL таблицы cbr_dip_bulletin (спека §3.1) в %s-форме шаблона: %s — TableName.
	// Ключ ReplacingMergeTree включает release_id — это и есть винтаж: значения одного выпуска
	// не затираются другим, а ревизия внутри выпуска (повторная публикация того же id)
	// перезаписывает точку. publication_date — атрибут выпуска, не часть ключа: дату публикации
	// ЦБ может поправить, id — нет.
	cbrDipDdl = `CREATE TABLE IF NOT EXISTS %s (
    series LowCardinality(String),      -- имя ряда, объявленное кодом (cbrDipSheets, стандарт §6.8)
    date Date,                          -- отчётный месяц: последний день месяца (спека §3.3)
    release_id LowCardinality(String),  -- id выпуска из URL приложения (/Collection/Collection/File/<id>/)
    release LowCardinality(String),     -- номер выпуска источника: «26-07» (подпись «№ 7 / Октябрь» + год)
    publication_date Date,              -- дата публикации выпуска (data-tooltip-content «Опубликовано …»)
    value Float64
) ENGINE = ReplacingMergeTree ORDER BY (series, date, release_id);
`

	// cbrDipCountPoints — количественный чек версии (стандарт §9.2): число точек своей таблицы.
	// FINAL обязателен — иначе count() считал бы несмёрженные части. Число растёт с каждым
	// выпуском, поэтому решением о no-op оно не служит (это делает гейт, спека §5.2), но
	// объявлено: без него не включатся архив версий и строки source_publications (§9.6).
	cbrDipCountPoints = "SELECT count() FROM cbr_dip_bulletin FINAL"

	// cbrDipKnownReleasesQuery — гейт прогона (спека §3.5, шаг 3): выпуски, уже лежащие в
	// таблице. Гейт заменяет HEAD-префлайт набора артефактов: PublicationCountPoints считает
	// точки всей таблицы, и чек известной версии не сошёлся бы ни разу — каждый прогон
	// пере-импортировал бы все 49 файлов. SELECT DISTINCT отвечает на тот же вопрос («этот
	// выпуск уже импортирован?») по своим данным и не делает ни одного HEAD на повторном
	// прогоне. Самолечение «выпуск пропал из таблицы» работает: гейт смотрит на данные, а не на
	// реестр версий (спека §5.2).
	cbrDipKnownReleasesQuery = "SELECT DISTINCT release_id FROM cbr_dip_bulletin FINAL"

	// cbrDipReleaseCountQuery — число строк выпуска: проверка, что успешный вызов шаблона
	// записал выпуск (спека §3.5, шаг 4). FINAL обязателен (стандарт §9.2) — как у
	// cbrDipCountPoints. release_id уходит плейсхолдером драйвера, а не литералом: id выпуска —
	// данные страницы, и в тексте запроса ему не место (прецедент значения-параметра —
	// mpLLMTaggedQuery, craw/gost.go).
	cbrDipReleaseCountQuery = "SELECT count() FROM cbr_dip_bulletin FINAL WHERE release_id = ?"
)

// cbrDipStat — импортёр cbr_dip_bulletin: встроенный util.ClickHouseImport ведёт таблицу, DDL,
// каталог рядов и витрину (afterCbrDip), объявляет точку контроля публикации; Import
// переопределён — у импортёра не один файл, а выпуски (спека §3.5). Name() не переопределён:
// имя импортёра совпадает с именем таблицы, как у большинства пакета.
type cbrDipStat struct {
	util.ClickHouseImport
}

// Import — полный цикл одного прогона (спека §3.5): DDL → страница выпусков → гейт по release_id
// → цикл по новым выпускам в хронологическом порядке (старые первыми): вызов шаблона на каждый
// выпуск и проверка, что строки выпуска в таблице есть (успех шаблона записи не гарантирует — он
// умеет ответить no-op, спека §3.5 шаг 4). Порядок шагов — часть контракта: DDL выполняется до
// сети и до гейта, потому что гейт обращается к таблице, а шаблонный DDL внутри цикла не
// отработал бы при нуле новых выпусков (прогон на чистой базе обязан отработать целиком). Сбой
// одного выпуска не отменяет остальные: ошибки копятся errors.Join с названием выпуска в
// тексте, прогон завершается ошибкой — dagu видит сбой и не пускает цепочку дальше (спека §3.5,
// шаг 5). Если новых выпусков нет — 0 строк и ни одного сетевого обращения к файлам, кроме
// страницы (её чтение — цена обнаружения нового выпуска, ≈140 КБ).
func (s *cbrDipStat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	// 1. DDL — до сети: шаблонный DDL внутри цикла отработал бы только при непустом списке
	// новых выпусков, а гейт уже обращается к таблице.
	for _, ddl := range s.CreateTable {
		if err = conn.Exec(ctx, fmt.Sprintf(ddl, s.TableName)); err != nil {
			return 0, fmt.Errorf("%s: DDL: %w", cbrDipTable, err)
		}
	}

	// 2. Страница выпусков: список {id, url, publication_date, release} в порядке страницы
	// (свежие первыми). Разбор страницы — fail-fast (пропавший блок выпусков или ссылка без
	// даты публикации — ошибка, а не «новых выпусков нет» при живом ЦБ).
	page, err := util.GetPage(dipPageUrl)
	if err != nil {
		return 0, fmt.Errorf("%s: %s: %w", cbrDipTable, dipPageUrl, err)
	}
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(page))
	if err != nil {
		return 0, fmt.Errorf("%s: %s: %w", cbrDipTable, dipPageUrl, err)
	}
	releases, err := dipParseReleases(doc)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", cbrDipTable, err)
	}

	// 3. Гейт: известные выпуски пропускаются по release_id — ключу винтажа, а не по имени
	// файла (stat_bulletin_23-07.xlsx встречается у двух выпусков, спека §2).
	known, err := cbrDipKnownReleases(ctx, conn)
	if err != nil {
		return 0, err
	}
	fresh := cbrDipNewReleases(releases, known)
	if len(fresh) == 0 {
		log.Infof("%s: новых выпусков нет (на странице %d, в таблице %d выпусков)", cbrDipTable, len(releases), len(known))
		return 0, nil
	}

	// 4. Цикл по новым выпускам. Ошибки копятся — сбой одного выпуска не отменяет остальные:
	// выпуски независимы (у каждого свой id, файл и строка версии), а пропущенный выпуск
	// подхватился бы следующим прогоном, если бы прогон молча прекращался на первом сбое.
	var errs []error
	for _, release := range fresh {
		// Локальная копия встроенного шаблона: DataUrl и ImportFunc зависят от выпуска, а
		// мутировать общую структуру импортёра нельзя (прецедент — moBrStat, cbr/mo_br.go).
		// Копия приносит объявленный префлайт (PublicationCountPoints): судьбу прогона он не
		// решает (no-op считает гейт), но без него не было бы архива версии файла выпуска и
		// строки source_publications (спека §5.2, §5.3).
		local := s.ClickHouseImport
		local.DataUrl = release.URL
		local.ImportFunc = cbrDipAppendPoints(release)
		points, importErr := local.Import(ctx, conn)
		if importErr != nil {
			errs = append(errs, fmt.Errorf("%s: %w", cbrDipContext(release), importErr))
			continue
		}
		// Успешный вызов шаблона — ещё не «выпуск записан» (спека §3.5, шаг 4): его префлайт
		// умеет вернуть no-op («версия известна и число точек совпало»), и тогда новый по гейту
		// выпуск остался бы молча неимпортированным — следующий прогон снова счёл бы его новым.
		// Проверка делает эту дыру громкой ошибкой выпуска, а continue сохраняет независимость
		// остальных выпусков.
		if err = cbrDipReleaseWritten(ctx, conn, release); err != nil {
			errs = append(errs, err)
			continue
		}
		count += points
		log.Infof("%s: выпуск %s (id %s, опубликован %s): импортировано %d точек",
			cbrDipTable, release.Release, release.ID, release.PublicationDate.Format("2006-01-02"), points)
	}
	// Итог: число точек записанных выпусков и агрегированная ошибка (errors.Join без ошибок —
	// nil, то есть успешный прогон).
	return count, errors.Join(errs...)
}

// cbrDipAppendPoints — ImportFunc шаблона для одного выпуска: обёртка cbrDipParseBook в строки
// батча. Выпуск нужен строке как винтаж (release_id, release, publication_date — свойства
// файла, а не книги внутри него), а ImportFunc шаблона знает только книгу и батч, поэтому
// обёртка возвращает замыкание на выпуск (стандарт §5.1). Шаблон качает файл один раз — тех же
// байтов хватает и на разбор, и на архив версии — и открывает батч одним PrepareBatch на
// выпуск (прецедент — cbrBopEstImport); Append с проверкой err обязателен: без неё строка
// терялась бы молча. Порядок значений — порядок колонок DDL.
func cbrDipAppendPoints(release dipRelease) func(xlsx *excelize.File, batch driver.Batch) error {
	return func(xlsx *excelize.File, batch driver.Batch) error {
		points, err := cbrDipParseBook(xlsx, release)
		if err != nil {
			return err
		}
		for _, point := range points {
			if err = batch.Append(point.Series, point.Date, release.ID, release.Release, release.PublicationDate, point.Value); err != nil {
				return fmt.Errorf("%s: ряд %q, %s: %w",
					cbrDipContext(release), point.Series, point.Date.Format("2006-01-02"), err)
			}
		}
		return nil
	}
}

// cbrDipReleaseWritten — проверка, что строки выпуска в таблице есть: число строк по release_id
// после успешного вызова шаблона (спека §3.5, шаг 4). Одного успеха шаблона мало: решения о
// составе выпусков его префлайт не принимает (гейт сильнее), но умеет вернуть no-op, если версия
// известна и число точек совпало, — тогда новый по гейту выпуск остался бы молча
// неимпортированным, а следующий прогон снова счёл бы его новым. Состояние практически
// недостижимо (число точек всей таблицы растёт с каждым выпуском, и совпасть с записанным при
// импорте выпуска оно может лишь в вырожденном случае), но тихая дыра обязана стать громкой
// ошибкой с именем выпуска и указанием, что делать: удалить строку выпуска из source_publications
// и повторить прогон. Ноль строк — ошибка, и сбой запроса — ошибка: «считаем, что записано»
// пропустило бы дыру в обоих случаях. release_id передаётся плейсхолдером драйвера, а не
// литералом: id выпуска — данные страницы (прецедент — mpLLMTaggedQuery, craw/gost.go).
func cbrDipReleaseWritten(ctx context.Context, conn driver.Conn, release dipRelease) error {
	var written uint64
	if err := conn.QueryRow(ctx, cbrDipReleaseCountQuery, release.ID).Scan(&written); err != nil {
		return fmt.Errorf("%s: число строк выпуска не прочитано: %w", cbrDipContext(release), err)
	}
	if written == 0 {
		return fmt.Errorf("%s: шаблон вернул no-op (версия известна), но строк выпуска в таблице нет — удалите строку выпуска из source_publications и повторите прогон", cbrDipContext(release))
	}
	return nil
}

// cbrDipKnownReleases — множество release_id выпусков, уже лежащих в таблице (гейт, спека §3.5
// шаг 3). Множество, а не список: в цикле важен только ответ «выпуск известен?». Ошибка чтения —
// ошибка прогона, а не «считаем, что известных нет»: пустое множество пере-импортировало бы все
// выпуски, и сбой диагностики обернулся бы полным прогоном.
func cbrDipKnownReleases(ctx context.Context, conn driver.Conn) (map[string]bool, error) {
	rows, err := conn.Query(ctx, cbrDipKnownReleasesQuery)
	if err != nil {
		return nil, fmt.Errorf("%s: выпуски таблицы не прочитаны: %w", cbrDipTable, err)
	}
	defer func() { _ = rows.Close() }()

	known := make(map[string]bool)
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("%s: строка выпусков таблицы не прочитана: %w", cbrDipTable, err)
		}
		known[id] = true
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: выпуски таблицы не дочитаны: %w", cbrDipTable, err)
	}
	return known, nil
}

// cbrDipNewReleases — выпуски страницы, которых нет в таблице, в хронологическом порядке (старые
// → новые): первый прогон наполняет историю детерминированным префиксом, а порядок не зависит
// от порядка страницы (та отдаёт свежие первыми, спека §3.5 шаг 4). Сортировка устойчивая, а
// ключ — дата публикации, и у двух выпусков одного дня порядок страницы сохраняется: результат
// детерминирован и без уникальности дат. Чистая функция без обращений к БД и сети: сценарии
// «все выпуски известны» (пусто → no-op прогона) и «все выпуски новые» пинятся тестом.
func cbrDipNewReleases(releases []dipRelease, known map[string]bool) []dipRelease {
	fresh := make([]dipRelease, 0, len(releases))
	for _, release := range releases {
		if !known[release.ID] {
			fresh = append(fresh, release)
		}
	}
	sort.SliceStable(fresh, func(i, j int) bool {
		return fresh[i].PublicationDate.Before(fresh[j].PublicationDate)
	})
	return fresh
}

// cbrDipBulletinImporter — приложения к бюллетеню ДИП: Name() = TableName = cbr_dip_bulletin,
// восемь объявленных рядов и винтажные колонки. Объявление точки контроля публикации живёт
// здесь, а не в Import(): URL артефакта — не поле структуры, а адрес выпуска, вычисляемый в
// Import() (прецедент — moBrImporter), и тест-энфорсер реестра читает объявление у
// зарегистрированного импортёра. После Import — производные: каталог восьми рядов
// (cbr/dip_meta.go) и витрина v_cbr_dip_bulletin (cbr/views.go). Регистрация — реестром
// cbr/importers.go; сетевых вызовов в init() нет.
var cbrDipBulletinImporter = &cbrDipStat{ClickHouseImport: util.ClickHouseImport{
	TableName:              cbrDipTable,
	CreateTable:            []string{cbrDipDdl},
	PublicationCountPoints: cbrDipCountPoints,
	AfterImport:            afterCbrDip(cbrDipSeriesMeta),
}}
