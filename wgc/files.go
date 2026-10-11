package wgc

import (
	"os"
	"path/filepath"
	"time"
)

// defaultDataDir — папка выгрузок WGC относительно CWD (в .gitignore): страницы
// goldhub защищены Cloudflare, файлы скачиваются вручную браузером (curl/urllib
// получают 403), поэтому импортёр читает локальную папку — прецедент polyus/data.
const defaultDataDir = "wgc/data"

// findLatestFile возвращает самый свежий по mtime файл каталога dir, подходящий
// под маску Glob. Имя для выбора не годится: выгрузки названы месяцем прописью,
// а «Oct2026» лексикографически больше «Nov2026». Тай-брейк при равном mtime —
// имя по убыванию, иначе выбор зависел бы от порядка обхода каталога. Файла нет —
// ("", nil): решение «warn + 0 строк» остаётся вызывающему, чтобы dagu не краснел
// до первого скачивания выгрузки.
func findLatestFile(dir, mask string) (string, error) {
	matches, err := filepath.Glob(filepath.Join(dir, mask))
	if err != nil {
		return "", err
	}

	var (
		latest   string
		latestAt time.Time
	)

	for _, path := range matches {
		info, err := os.Stat(path)
		if err != nil {
			return "", err
		}

		mod := info.ModTime()
		if latest == "" || mod.After(latestAt) || (mod.Equal(latestAt) && path > latest) {
			latest, latestAt = path, mod
		}
	}

	return latest, nil
}
