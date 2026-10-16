package util

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/archive"
	log "github.com/sirupsen/logrus"
)

// ARCHIVE_ACTION и его значения — контракт режима между main.go (какой путь запускать) и util
// (шаблон в режиме восстановления). Константы общие, а не литералы в каждом пакете: main.go
// отвергает неизвестное значение фатально, а вторая копия строки в шаблоне разошлась бы с первой
// молча — и restore пошёл бы обычным путём, с сетью и полным импортом. Значение читается в момент
// действия, а не в init() (ARCHITECTURE §8.6): у тестов есть t.Setenv.
const (
	EnvArchiveAction      = "ARCHIVE_ACTION"
	ArchiveActionRegistry = "registry"
	ArchiveActionList     = "list"
	ArchiveActionRestore  = "restore"
)

// ArchiveRestoreMode — работает ли шаблон в режиме восстановления из архива. Окружение читается в
// момент вызова: конфигурация в init() запрещена (ARCHITECTURE §8.6), а тест подставляет режим
// через t.Setenv уже после старта процесса — значение в переменной, а не в поле структуры, и
// импортёр о режиме не знает (шаблон один на всех).
func ArchiveRestoreMode() bool {
	return os.Getenv(EnvArchiveAction) == ArchiveActionRestore
}

// archiveRuntime — включённая архивация версий файлов: хранилище и его реестр. Собирается один
// раз на процесс (ленивый синглтон ниже): импортёры идут одним бинарником по очереди, а второй
// Registry на тот же реестр только добавил бы чтения файла (§3.4). Полей ровно два, и состояние
// версии через них не ходит: источник, url и отпечаток приходят аргументами save/versionExists —
// рантайм общий для всех импортёров, и «текущая версия» в его поле принадлежала бы тому, кто
// записал последним.
type archiveRuntime struct {
	store    archive.Store
	registry *archive.Registry
}

// archiveRuntimeForTests — подстановка хранилища тестами (образец — fileTimeout в util/xls.go):
// у прода переменная nil, и архивация собирается из окружения. Хук, а не поле шаблона: импортёры
// об архиве не знают и своего кода не меняют (спека §3.3).
var archiveRuntimeForTests *archiveRuntime

var (
	// archiveOnce — ленивая сборка: окружение читается на первом обращении, а не в init() —
	// сетевые вызовы и чтение конфигурации в init() запрещены (ARCHITECTURE §8.6), и импортёр без
	// точки контроля публикации архива не касается вовсе.
	archiveOnce sync.Once
	archiveRT   *archiveRuntime
)

// activeArchive — включённая архивация или nil: пустой ARCHIVE_BACKEND — штатное «архива нет»
// (nil, nil) — nil и здесь. Ошибка конфигурации (неизвестный бэкенд, пустой ARCHIVE_LOCAL_DIR,
// отсутствующий токен) логируется и выключает архивацию: недоступный архив не должен
// останавливать данные (спека §3.1). Ошибка показывается один раз на процесс — sync.Once держит
// её в логе прогона, не повторяя на каждой версии. Текст ошибки называет переменную окружения
// (его собирает archive.FromEnv) и не содержит значений: токен — секрет.
func activeArchive() *archiveRuntime {
	if archiveRuntimeForTests != nil {
		return archiveRuntimeForTests
	}
	archiveOnce.Do(func() {
		store, err := archive.FromEnv(HttpClient)
		if err != nil {
			log.Errorf("архив версий файлов выключен: %v", err)
			return
		}
		if store == nil {
			return
		}
		archiveRT = &archiveRuntime{store: store, registry: archive.NewRegistry(store)}
	})
	return archiveRT
}

// versionExists — лежит ли версия в архиве. Ключ собирается из тех же данных, что и у save
// (источник, basename URL, отпечаток), поэтому ответ относится ровно к той версии, которую
// записал бы save, и на no-op-пути не нужны ни файл, ни реестр — один Store.Exists (спека §3.2).
// Сумма sha256 передаётся нулевой: при непустом отпечатке она в ключ не входит (archive.Key), а
// при пустом сюда не приходят — no-op с неизвестным отпечатком невозможен.
func (rt *archiveRuntime) versionExists(ctx context.Context, source, url, fingerprint string) (bool, error) {
	return rt.store.Exists(ctx, archive.Key(source, path.Base(url), fingerprint, [sha256.Size]byte{}))
}

// save сохраняет версию файла: объект с байтами плюс строка реестра. Порядок «sha256 → ключ → Put →
// Registry.Ensure» неслучаен: строка реестра утверждает наличие объекта, поэтому объект пишется
// первым (§3.4). Объект записывается **всегда**, без Exists-предпроверки: ключ версии выводится из
// отпечатка HEAD, а отпечаток содержимое не покрывает (заголовки те же, тело — другое), и
// пропущенная запись оставила бы в хранилище байты прежней версии, а в реестре — sha256 новой: строка
// описывала бы содержимое, которого в архиве нет, и «файл + реестр» перестали бы быть самодостаточными
// для будущей проверки целостности (§3.2, шаг 1.2 роадмапа). Оба бэкенда перезаписывающие (LocalStore
// — атомарный rename, Яндекс.Диск — overwrite=true), поэтому инвариант «строка реестра описывает
// записанные байты» держится конструктивно. Цена — повторная загрузка идентичных байтов в редких
// ветках (смена версии, самолечение после потери объекта): при 150–250 МБ/год она незаметна. Строка
// реестра дописывается и когда объект уже лежал: реестр мог быть потерян или отстать от хранилища
// (ручной прогон, чистка), а Ensure сам решает, менять ли её — та же версия с тем же sha256 не меняет
// ничего (§3.4). Пропавшая строка при живом объекте так не восстанавливается: no-op-путь до save не
// доходит, и реконсиляция «объект без строки» — работа шага 1.2 (спека §3.4).
// Ошибка — сбой архивации: вызывающий (Import) из-за неё не пишет строку версии, и следующий
// прогон повторит архивацию вместе с импортом.
func (rt *archiveRuntime) save(ctx context.Context, source, url string, body []byte, fingerprint string) error {
	sum := sha256.Sum256(body)
	key := archive.Key(source, path.Base(url), fingerprint, sum)
	if err := rt.store.Put(ctx, key, body); err != nil {
		return err
	}
	return rt.registry.Ensure(ctx, archive.Entry{
		Source:      source,
		Name:        path.Base(url),
		Fingerprint: fingerprint,
		SHA256:      hex.EncodeToString(sum[:]),
		Size:        int64(len(body)),
		URL:         url,
		Key:         key,
		FirstSeen:   time.Now(),
	})
}

// RestoreVersion — версия файла из архива по паре (источник, url): строка реестра → объект
// хранилища → проверка целостности. Сеть при этом не вызывается вовсе — ни HEAD, ни GET
// (спека §3.1): восстановление обязано работать, когда живой источник недоступен, троттлит и
// версию с него уже не отдаёт. Резолв — по url, а не по отпечатку (спека §7): при восстановлении
// отпечаток HEAD неизвестен (HEAD не делается), а готовый ключ объекта лежит в строке реестра.
// Строк на одну пару может быть несколько — один файл под sha- и fingerprint-ключом (дубли
// содержимого, §3.4): берётся последняя по first_seen, при равенстве — последняя в файле, потому
// что файл упорядочен по first_seen и порядок строк значим (это и есть выбор «самой свежей
// увиденной версии»).
//
// Со строкой возвращается и строка реестра: вызывающий берёт оттуда fingerprint для
// source_publications — реестр и таблица версий называют версию одинаково, не спрашивая источник.
// Пустой fingerprint строки — не ошибка: версия записана по sha-ключу (HEAD в момент архивации был
// недоступен), и вызывающий по существующему правилу шаблона не пишет строку версии.
//
// sha256 полученных байтов обязан совпасть со строкой реестра: расхождение — ошибка, а не warn:
// реестр и объект объявлены самодостаточной парой («что именно импортировали»), и молча
// импортировать подменённый/усечённый объект под именем версии реестра нельзя (спека §3.1).
func RestoreVersion(ctx context.Context, source, url string) ([]byte, archive.Entry, error) {
	rt := activeArchive()
	if rt == nil {
		return nil, archive.Entry{}, fmt.Errorf("архив не настроен (%s)", archive.EnvBackend)
	}
	// Реестр читается целиком: он лежит в хранилище, версии ищутся по нему, и другого описания
	// объектов у резолва нет (истина — файл в хранилище, §3.4).
	entries, err := archive.LoadEntries(ctx, rt.store)
	if err != nil {
		return nil, archive.Entry{}, err
	}
	var latest archive.Entry
	matched := false
	for _, e := range entries {
		if e.Source != source || e.URL != url {
			continue
		}
		// !Before — «свежее или та же секунда»: при равном first_seen остаётся последняя строка
		// файла (строки упорядочены по first_seen, §3.4), а не первая попавшаяся.
		if !matched || !e.FirstSeen.Before(latest.FirstSeen) {
			latest, matched = e, true
		}
	}
	if !matched {
		return nil, archive.Entry{}, fmt.Errorf("в архиве нет версии для %s", url)
	}
	body, err := rt.store.Get(ctx, latest.Key)
	if err != nil {
		return nil, archive.Entry{}, err
	}
	sum := sha256.Sum256(body)
	if got := hex.EncodeToString(sum[:]); got != latest.SHA256 {
		return nil, archive.Entry{}, fmt.Errorf("archive: sha256 объекта %s не совпал со строкой реестра (%s против %s)", latest.Key, got, latest.SHA256)
	}
	return body, latest, nil
}
