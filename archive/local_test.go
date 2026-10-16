package archive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// Round-trip Put → Get → Exists: локальный бэкенд — это dev-прогон (ARCHIVE_BACKEND=local) и
// харнесс тестов, поэтому проверяется ровно то, на что опирается вызывающий: положили байты,
// прочитали те же байты, наличие видно.
func TestLocalRoundTrip(t *testing.T) {
	store := NewLocal(t.TempDir())
	ctx := context.Background()
	key := Key("cbr_m2", "monetary_agg_SA.xlsx", fp, sha256.Sum256([]byte("версия")))
	body := []byte("байты выгрузки")

	if store.Name() != "local" {
		t.Errorf("Name() = %q, ожидалось %q", store.Name(), "local")
	}
	if err := store.Put(ctx, key, body); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("Get() = %q, ожидалось %q", got, body)
	}
	exists, err := store.Exists(ctx, key)
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !exists {
		t.Error("Exists после Put = false, ожидалось true")
	}
}

// Отсутствие объекта — не ошибка (false, nil): Exists зовётся на каждом no-op-прогоне, и ошибка
// вместо ответа «нет» превращала бы штатное «версия новая» в сбой архивации. Get при этом обязан
// падать: пустые байты как версия файла — молчаливая потеря.
func TestLocalMissingKey(t *testing.T) {
	store := NewLocal(t.TempDir())
	ctx := context.Background()

	exists, err := store.Exists(ctx, "cbr_m2/none.xlsx")
	if err != nil {
		t.Fatalf("Exists отсутствующего ключа: %v", err)
	}
	if exists {
		t.Error("Exists отсутствующего ключа = true, ожидалось false")
	}
	if _, err = store.Get(ctx, "cbr_m2/none.xlsx"); err == nil {
		t.Error("Get отсутствующего ключа вернул байты без ошибки")
	}
}

// Ключ с «..» отвергается всеми операциями, и файла вне каталога архива не появляется. Ключи
// собирает Key из данных источника, но путь — единственное, что отделяет строку от файловой
// системы: объект, записанный рядом с каталогом архива, потерялся бы при его переносе (и молча
// разошёлся бы с реестром).
func TestLocalRejectsParentKey(t *testing.T) {
	dir := t.TempDir()
	store := NewLocal(dir)
	ctx := context.Background()
	escaped := filepath.Join(dir, "..", "escaped.xlsx")

	if err := store.Put(ctx, "../escaped.xlsx", []byte("нет")); err == nil {
		t.Error("Put ключа с «..» не дал ошибки")
	}
	if _, err := store.Get(ctx, "../escaped.xlsx"); err == nil {
		t.Error("Get ключа с «..» не дал ошибки")
	}
	if _, err := store.Exists(ctx, "../escaped.xlsx"); err == nil {
		t.Error("Exists ключа с «..» не дал ошибки")
	}
	if _, err := os.Stat(escaped); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("файл вне каталога архива существует (%v), ожидалось его отсутствие", err)
	}
}

// Вложенный ключ (…/<источник>/<префикс>_<имя>) создаёт подкаталог источника: каталог архива
// читается глазами, и версии одного источника лежат рядом — как и на Диске по тому же ключу.
func TestLocalNestedKey(t *testing.T) {
	dir := t.TempDir()
	store := NewLocal(dir)
	ctx := context.Background()
	key := "cbr_m2/x_y.xlsx"

	if err := store.Put(ctx, key, []byte("тело")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	info, err := os.Stat(filepath.Join(dir, "cbr_m2"))
	if err != nil {
		t.Fatalf("подкаталог источника не создан: %v", err)
	}
	if !info.IsDir() {
		t.Errorf("%s не каталог", filepath.Join(dir, "cbr_m2"))
	}
	got, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "тело" {
		t.Errorf("Get() = %q, ожидалось %q", got, "тело")
	}
}

// Объект появляется на целевом пути только целиком — запись идёт через временный файл рядом и
// rename. Иначе прерванная запись (kill, полный диск) оставила бы усечённый объект: Exists увидел
// бы его как «версия есть», no-op-путь пропустил бы повторную архивацию, и версия потерялась бы
// молча — а архив объявлен источником истины (спека §3.1). Наблюдаемое здесь: временных файлов
// после Put не остаётся, и повторная запись того же ключа отдаёт новое содержимое, а не дозапись
// (ключ и есть версия, но объект мог быть перезаписан — в том числе при самолечении архива).
func TestLocalPutIsAtomic(t *testing.T) {
	dir := t.TempDir()
	store := NewLocal(dir)
	ctx := context.Background()
	key := "cbr_m2/x_y.xlsx"

	if err := store.Put(ctx, key, []byte("первая версия")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	assertNoTempLeftovers(t, filepath.Join(dir, "cbr_m2"))
	if got, err := store.Get(ctx, key); err != nil {
		t.Fatalf("Get после первого Put: %v", err)
	} else if string(got) != "первая версия" {
		t.Errorf("Get() = %q, ожидалось %q", got, "первая версия")
	}
	// Права объекта: CreateTemp даёт 0600, а выгрузка в архиве обязана читаться как обычный файл —
	// 0644, как и предписывал бриф для записи целиком.
	if info, err := os.Stat(filepath.Join(dir, filepath.FromSlash(key))); err != nil {
		t.Fatalf("Stat объекта: %v", err)
	} else if perm := info.Mode().Perm(); perm != 0o644 {
		t.Errorf("права объекта %o, ожидалось 644", perm)
	}

	if err := store.Put(ctx, key, []byte("вторая версия")); err != nil {
		t.Fatalf("повторный Put: %v", err)
	}
	assertNoTempLeftovers(t, filepath.Join(dir, "cbr_m2"))
	if got, err := store.Get(ctx, key); err != nil {
		t.Fatalf("Get после повторного Put: %v", err)
	} else if string(got) != "вторая версия" {
		t.Errorf("Get() = %q, ожидалось %q (перезапись не должна дописывать и не должна оставлять хвост прежней версии)", got, "вторая версия")
	}
}

// assertNoTempLeftovers — временный файл записи лежит в том же каталоге, что и объект (rename
// атомарен только в пределах одной ФС), поэтому утечка .tmp видна прямо в каталоге архива.
// Мусор из них опасен тем, что его не считает объектом ни реестр, ни приёмка: он просто копится.
func assertNoTempLeftovers(t *testing.T, dir string) {
	t.Helper()
	leftovers, err := filepath.Glob(filepath.Join(dir, "*.tmp"))
	if err != nil {
		t.Fatalf("Glob %s: %v", dir, err)
	}
	if len(leftovers) != 0 {
		t.Errorf("после Put в %s остались временные файлы: %v", dir, leftovers)
	}
}

// List — ключи объектов хранилища в той же форме, что у реестра и Store («<источник>/<файл>»):
// листинг — вход реконсиляции «есть объект — нет строки реестра» (шаг 1.2), и сравнивать ключи она
// обязана дословно. Пропускаются *.tmp (осиротевшие после SIGKILL файлы атомарной записи: ни
// реестр, ни приёмка их объектами не считают) и сам registry.jsonl — он лежит в корне хранилища,
// но версией файла источника не является.
func TestLocalListSkipsTempAndRegistry(t *testing.T) {
	dir := t.TempDir()
	store := NewLocal(dir)
	ctx := context.Background()

	keys := []string{"cbr_m2/x_y.xlsx", "rosstat_ipc_weeks_bulletins/abcd1234_87.htm"}
	for _, key := range keys {
		if err := store.Put(ctx, key, []byte("тело")); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}
	if err := store.Put(ctx, RegistryKey, []byte("{}\n")); err != nil {
		t.Fatalf("Put %s: %v", RegistryKey, err)
	}
	// Осиротевший .tmp пишется мимо Put: так его оставляет убитый процесс — writeFileAtomic снимает
	// временный файл сам, а SIGKILL снять его уже нечем.
	tmp := filepath.Join(dir, "cbr_m2", "123456.tmp")
	if err := os.WriteFile(tmp, []byte("половина объекта"), 0o644); err != nil {
		t.Fatalf("подготовка %s: %v", tmp, err)
	}

	got, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if !slices.Equal(got, keys) {
		t.Errorf("List() = %v, ожидалось %v (лексикографический порядок обхода)", got, keys)
	}
}

// Отсутствующий корень — пустой список, не ошибка: архив мог ещё не наполняться, а реконсиляция на
// пустом хранилище обязана быть чистой, а не падать. Пустой существующий каталог — то же самое.
func TestLocalListEmptyOrMissingDir(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "нет-такого-каталога")
	for _, tt := range []struct {
		name string
		dir  string
	}{
		{name: "несуществующий каталог", dir: missing},
		{name: "пустой каталог", dir: t.TempDir()},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewLocal(tt.dir).List(context.Background())
			if err != nil {
				t.Fatalf("List %s: %v", tt.dir, err)
			}
			if len(got) != 0 {
				t.Errorf("List() = %v, ожидался пустой список", got)
			}
		})
	}
}
