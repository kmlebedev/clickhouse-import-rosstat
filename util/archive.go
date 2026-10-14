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

// save сохраняет версию файла: объект с байтами плюс строка реестра. Порядок «sha256 → ключ →
// Exists → Put → Registry.Ensure» неслучаен: строка реестра утверждает наличие объекта, поэтому
// объект пишется первым (§3.4). Exists-предпроверка — экономия трафика: при первом включении
// архива и после смены бэкенда версия уже импортирована в ClickHouse и дописывается полным
// прогоном, но тот же объект второй раз грузить незачем (Put того же ключа перезаписал бы те же
// байты). Строка реестра дописывается и когда объект уже лежит: реестр мог быть потерян или
// отстать от хранилища (ручной прогон, чистка), а Ensure сам решает, менять ли её — та же версия
// с тем же sha256 не меняет ничего (§3.4), поэтому самолечение реестра стоит одного чтения файла.
// Ошибка — сбой архивации: вызывающий (Import) из-за неё не пишет строку версии, и следующий
// прогон повторит архивацию вместе с импортом.
func (rt *archiveRuntime) save(ctx context.Context, source, url string, body []byte, fingerprint string) error {
	sum := sha256.Sum256(body)
	key := archive.Key(source, path.Base(url), fingerprint, sum)
	exists, err := rt.store.Exists(ctx, key)
	if err != nil {
		return err
	}
	if !exists {
		if err = rt.store.Put(ctx, key, body); err != nil {
			return err
		}
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
