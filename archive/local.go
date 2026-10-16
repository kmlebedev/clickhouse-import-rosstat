package archive

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// LocalStore — хранилище на каталоге файловой системы: dev-прогоны (ARCHIVE_BACKEND=local) и
// харнесс тестов. Своей структуры не заводит — ключ и есть относительный путь, поэтому каталог
// архива читается глазами, а версии одного источника лежат рядом, как и на Диске.
//
// ctx у операций не проверяется: файловые вызовы отмены не умеют, а параметр — часть контракта
// Store, чтобы бэкенды оставались взаимозаменяемыми.
type LocalStore struct {
	Dir string
}

// Lister — бэкенд, умеющий перечислить объекты хранилища. Узкий интерфейс, а не метод Store:
// листинг есть не у всех бэкендов, и по нему main.go отличает те, для которых возможна
// реконсиляция «есть объект — нет строки реестра» (ARCHIVE_RECONCILE, спека §3.2). Сегодня его
// реализует LocalStore; листинг Яндекс.Диска (resources) назван остатком пункта 1.2 — у Диска
// это отдельная операция со страницами, а не обход каталога.
type Lister interface {
	List(ctx context.Context) ([]string, error)
}

// LocalStore обязан удовлетворять Store: бэкенды подставляются друг вместо друга (тесты util
// берут local вместо Диска), и расхождение сигнатур должно ломать сборку, а не прогон.
var _ Store = (*LocalStore)(nil)

// LocalStore обязан удовлетворять и Lister: реконсиляция спрашивает листинг через type-assertion,
// и сломанная сигнатура должна падать сборкой, а не тихо выключать сверку.
var _ Lister = (*LocalStore)(nil)

// NewLocal — хранилище в каталоге dir. Каталог здесь не создаётся: пустой каталог под архив ничего
// не значит, а Put заводит и его, и подкаталог источника (MkdirAll родителя) — так dev-прогон и
// приёмка получают дерево ровно из тех версий, что действительно сохранились.
func NewLocal(dir string) *LocalStore {
	return &LocalStore{Dir: dir}
}

func (s *LocalStore) Name() string {
	return "local"
}

func (s *LocalStore) Put(ctx context.Context, key string, body []byte) error {
	p, err := s.path(key)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return fmt.Errorf("archive local: каталог %s: %w", filepath.Dir(p), err)
	}
	// Запись идёт через временный файл и rename (writeFileAtomic), а не os.WriteFile: архив объявлен
	// источником истины (спека §3.1), и усечённый объект от прерванной записи хуже отсутствующего —
	// Exists увидел бы «версия есть», no-op-путь пропустил бы повторную архивацию, и версия
	// потерялась бы молча. Текст ошибки остаётся прежним: к ключу добавляется причина сбоя.
	if err = writeFileAtomic(p, body); err != nil {
		return fmt.Errorf("archive local: запись %s: %w", key, err)
	}
	return nil
}

// writeFileAtomic кладёт body в path так, чтобы в path никогда не оказалось половины объекта:
// временный файл в том же каталоге → Write → Chmod → Sync → Close → Rename. Временный файл обязан
// лежать рядом с целевым: rename атомарен в пределах одной ФС, в чужом каталоге он выродился бы в
// копирование — ту же неатомарную запись. При любой неудаче временный файл снимается: .tmp не
// считает объектом ни реестр, ни приёмка, он просто копился бы в каталоге архива навсегда.
// Осиротевший .tmp после убийства процесса (SIGKILL снять его уже нечем) безвреден по той же
// причине: ни реестр, ни префлайт на него не смотрят, а повторный Put перезапишет целевой объект.
func writeFileAtomic(path string, body []byte) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, "*.tmp")
	if err != nil {
		return fmt.Errorf("временный файл в %s: %w", dir, err)
	}
	tmp := f.Name()
	flush := func() error {
		if _, err := f.Write(body); err != nil {
			return fmt.Errorf("запись: %w", err)
		}
		// CreateTemp даёт 0600, а объект архива — обычная выгрузка: 0644, как и было у записи
		// целиком (иначе читать архив с чужого пользователя стало бы нельзя).
		if err := f.Chmod(0o644); err != nil {
			return fmt.Errorf("права: %w", err)
		}
		// Sync до rename: без него объект может пережить переименование только в кэше ФС и не дойти
		// до диска при падении машины — ровно та потеря версии, от которой спасает временный файл.
		if err := f.Sync(); err != nil {
			return fmt.Errorf("синхронизация: %w", err)
		}
		return f.Close()
	}
	if err = flush(); err != nil {
		_ = f.Close() // при сбое Write/Sync файл ещё открыт
		_ = os.Remove(tmp)
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("переименование: %w", err)
	}
	return nil
}

func (s *LocalStore) Get(ctx context.Context, key string) ([]byte, error) {
	p, err := s.path(key)
	if err != nil {
		return nil, err
	}
	body, err := os.ReadFile(p)
	if err != nil {
		return nil, fmt.Errorf("archive local: чтение %s: %w", key, err)
	}
	return body, nil
}

// Exists — отсутствие файла это «версии нет» (false, nil), а не ошибка: так на него смотрит и
// префлайт — «нет» отправляет в полный импорт и самолечение архива, тогда как ошибка означала бы
// сбой проверки. Дежурный по логу должен различать эти два случая.
func (s *LocalStore) Exists(ctx context.Context, key string) (bool, error) {
	p, err := s.path(key)
	if err != nil {
		return false, err
	}
	if _, err = os.Stat(p); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("archive local: проверка %s: %w", key, err)
	}
	return true, nil
}

// List — ключи всех объектов хранилища («<источник>/<файл>») для реконсиляции «есть объект — нет
// строки реестра». Реестр здесь не читается: листинг отвечает за фактическое содержимое
// хранилища, а не за его описание, — иначе реконсиляция считала бы реестр полным по построению
// (спека §3.2). Пропускаются `*.tmp` (временные файлы атомарной записи: объектом их не считает ни
// реестр, ни приёмка, а осиротевший после SIGKILL файл иначе копился бы в выводе вечно) и сам
// registry.jsonl (реестр лежит в корне хранилища рядом с каталогами источников, но объектом
// версии не является). Отсутствующий корень — пустой список, не ошибка: архив мог ещё не
// наполняться, и «объектов нет» — законный ответ. Обход лексикографический (filepath.WalkDir),
// поэтому вывод реконсиляции воспроизводим. ctx не проверяется: у файловых вызовов отмены нет —
// как и у остальных операций LocalStore.
func (s *LocalStore) List(ctx context.Context) ([]string, error) {
	root, err := filepath.Abs(s.Dir)
	if err != nil {
		return nil, fmt.Errorf("archive local: каталог %q: %w", s.Dir, err)
	}
	var keys []string
	err = filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || strings.HasSuffix(d.Name(), ".tmp") {
			return nil
		}
		rel, relErr := filepath.Rel(root, p)
		if relErr != nil {
			return fmt.Errorf("путь %s: %w", p, relErr)
		}
		// Ключ — в той же форме, что у реестра и Store: «/»-разделители и относительный путь,
		// поэтому строка реестра и объект сравниваются дословно.
		if key := filepath.ToSlash(rel); key != RegistryKey {
			keys = append(keys, key)
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("archive local: обход %s: %w", s.Dir, err)
	}
	return keys, nil
}

// path переводит ключ в путь внутри Dir и отвергает выход за каталог. Ключ относительный (его
// собирает Key), но проверка нужна: путь — единственное, что отделяет строку от файловой системы,
// а ключ с «..» записал бы объект рядом с архивом (тот потерялся бы при переносе каталога и разошёлся
// бы с реестром). Сравниваются абсолютные пути: Join чистит «.» и «..» в ключе, поэтому префиксная
// проверка по неочищенному корню отсекла бы валидные ключи при относительном Dir (и при Dir = «.»).
func (s *LocalStore) path(key string) (string, error) {
	root, err := filepath.Abs(s.Dir)
	if err != nil {
		return "", fmt.Errorf("archive local: каталог %q: %w", s.Dir, err)
	}
	p := filepath.Join(root, filepath.FromSlash(key))
	if !strings.HasPrefix(p, root+string(filepath.Separator)) {
		return "", fmt.Errorf("archive local: ключ %q вне каталога %q", key, s.Dir)
	}
	return p, nil
}
