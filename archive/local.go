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

// LocalStore обязан удовлетворять Store: бэкенды подставляются друг вместо друга (тесты util
// берут local вместо Диска), и расхождение сигнатур должно ломать сборку, а не прогон.
var _ Store = (*LocalStore)(nil)

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
	if err = os.WriteFile(p, body, 0o644); err != nil {
		return fmt.Errorf("archive local: запись %s: %w", key, err)
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
