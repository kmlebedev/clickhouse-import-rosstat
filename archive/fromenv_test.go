package archive

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Пустой ARCHIVE_BACKEND — архивация выключена, и это (nil, nil), а не ошибка: без архива
// конвейер работает как раньше (спека §3.1), а ошибка на каждом прогоне мусорила бы в логе у всех,
// кто архив не заводил. Клиент здесь и ниже (&http.Client{}) в сеть не ходит — local и «выключено»
// работают с файловой системой; но он часть сигнатуры FromEnv, потому что прод передаёт
// util.HttpClient с национальными TLS-сертификатами РФ (правило 4 AGENTS.md), и подменять её в
// тестах своим вариантом нельзя.
func TestFromEnvDisabled(t *testing.T) {
	t.Setenv(EnvBackend, "")

	store, err := FromEnv(&http.Client{})
	if err != nil {
		t.Fatalf("FromEnv без %s: %v", EnvBackend, err)
	}
	if store != nil {
		t.Errorf("FromEnv без %s вернул %v, ожидался nil", EnvBackend, store)
	}
}

// local → LocalStore на ARCHIVE_LOCAL_DIR, и каталог доезжает до бэкенда: запись обязана лечь
// именно в него — так dev-прогон и приёмка видят версии глазами, без Диска.
func TestFromEnvLocal(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(EnvBackend, "local")
	t.Setenv(EnvLocalDir, dir)

	store, err := FromEnv(&http.Client{})
	if err != nil {
		t.Fatalf("FromEnv %s=local: %v", EnvBackend, err)
	}
	if store == nil {
		t.Fatalf("FromEnv %s=local вернул nil", EnvBackend)
	}
	if store.Name() != "local" {
		t.Errorf("Name() = %q, ожидалось %q", store.Name(), "local")
	}
	key := Key("cbr_m2", "x.xlsx", fp, zeroSum)
	if err = store.Put(context.Background(), key, []byte("тело")); err != nil {
		t.Fatalf("Put в local-хранилище: %v", err)
	}
	if _, err = os.Stat(filepath.Join(dir, filepath.FromSlash(key))); err != nil {
		t.Errorf("объект не лёг в %s: %v", dir, err)
	}
}

// Пустой ARCHIVE_LOCAL_DIR — ошибка, называющая переменную: иначе дежурный по логу не знает, какую
// строку править, а хранилище молча оказалось бы каталогом по умолчанию.
func TestFromEnvLocalWithoutDir(t *testing.T) {
	t.Setenv(EnvBackend, "local")
	t.Setenv(EnvLocalDir, "")

	store, err := FromEnv(&http.Client{})
	if err == nil {
		t.Fatalf("ожидалась ошибка, получено хранилище %v", store)
	}
	if !strings.Contains(err.Error(), EnvLocalDir) {
		t.Errorf("ошибка %q не называет %s — по логу не понять, что править", err, EnvLocalDir)
	}
}

// yandex без токена останавливается на конфигурации: без токена хранилище не собрать, и текст
// ошибки называет переменную, чтобы дежурный правил одну строку, а не искал причину в API.
func TestFromEnvYandexRequiresToken(t *testing.T) {
	t.Setenv(EnvBackend, "yandex")
	t.Setenv(EnvYandexToken, "")

	store, err := FromEnv(&http.Client{})
	if err == nil {
		t.Fatalf("ожидалась ошибка, получено хранилище %v", store)
	}
	if !strings.Contains(err.Error(), EnvYandexToken) {
		t.Errorf("ошибка %q не называет %s", err, EnvYandexToken)
	}
}

// Токен задан: корень архива берётся из ARCHIVE_YANDEX_DIR, а при пустом значении — из
// DefaultYandexDir. Корень проверяется у полученного хранилища, а не по тексту ошибки: конфигурация
// закрыта, FromEnv отдаёт бэкенд Диска, и название корня важно именно в нём — по нему собирается
// путь на Диске, то есть ошибка здесь означала бы архив не в том каталоге (или запись в чужой).
// Значение токена при этом не попадает ни в текст ошибки, ни в лог: лог архивации читает дежурный,
// и секрет в логе — утечка (правило 1 AGENTS.md).
func TestFromEnvYandexRoot(t *testing.T) {
	const token = "SECRET_TOKEN_VALUE"
	t.Setenv(EnvBackend, "yandex")
	t.Setenv(EnvYandexToken, token)

	for _, c := range []struct{ name, dir, wantRoot string }{
		{"корень по умолчанию", "", DefaultYandexDir},
		{"корень из переменной", "disk:/custom-archive", "disk:/custom-archive"},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv(EnvYandexDir, c.dir)

			store, err := FromEnv(&http.Client{})
			if err != nil {
				t.Fatalf("FromEnv %s=yandex: %v", EnvBackend, err)
			}
			if store == nil {
				t.Fatalf("FromEnv %s=yandex вернул nil", EnvBackend)
			}
			if store.Name() != "yandex" {
				t.Errorf("Name() = %q, ожидалось %q", store.Name(), "yandex")
			}
			disk, ok := store.(*YandexDisk)
			if !ok {
				t.Fatalf("FromEnv вернул %T, ожидался *YandexDisk", store)
			}
			if disk.root != c.wantRoot {
				t.Errorf("корень архива = %q, ожидался %q", disk.root, c.wantRoot)
			}
			// Токен доехал до хранилища: без него Диск ответил бы 401, то есть архивации не было бы
			// вовсе. Значение здесь фиктивное — настоящий токен в тесты не попадает (правило 1
			// AGENTS.md), и проверять его в логе или тексте ошибки всё равно нечего.
			if disk.token != token {
				t.Error("токен из окружения не доехал до хранилища Диска")
			}
		})
	}
}

// Неизвестное значение ARCHIVE_BACKEND (опечатка) — ошибка с именем переменной и её значением:
// молчаливое «архивация выключена» заметили бы только при попытке восстановить версию, то есть
// тогда, когда файла у источника уже нет.
func TestFromEnvUnknownBackend(t *testing.T) {
	t.Setenv(EnvBackend, "ftp")

	store, err := FromEnv(&http.Client{})
	if err == nil {
		t.Fatalf("ожидалась ошибка, получено хранилище %v", store)
	}
	if !strings.Contains(err.Error(), EnvBackend) || !strings.Contains(err.Error(), "ftp") {
		t.Errorf("ошибка %q не называет %s и его значение", err, EnvBackend)
	}
}
