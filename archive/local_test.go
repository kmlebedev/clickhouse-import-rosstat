package archive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
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
