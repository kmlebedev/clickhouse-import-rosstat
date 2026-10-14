package util

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"path"
	"sync"
	"time"

	"github.com/kmlebedev/clickhouse-import-rosstat/archive"
	log "github.com/sirupsen/logrus"
)

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
