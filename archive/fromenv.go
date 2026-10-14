package archive

import (
	"fmt"
	"net/http"
	"os"
)

// Имена переменных окружения архива вынесены в константы, потому что строку называют и загрузка
// конфигурации, и тексты ошибок: расхождение в написании («ARCHIVE_YANDEX_DIR» против
// «ARCHIVE_YANDEXDIR») стоило бы молча выключенной архивации.
const (
	EnvBackend     = "ARCHIVE_BACKEND"      // local | yandex | пусто = архивация выключена
	EnvYandexToken = "ARCHIVE_YANDEX_TOKEN" // OAuth-токен приложения (секрет: только os.Getenv)
	EnvYandexDir   = "ARCHIVE_YANDEX_DIR"   // корень архива на Диске
	EnvLocalDir    = "ARCHIVE_LOCAL_DIR"    // корень LocalStore (dev-прогон, приёмка, тесты)
)

// DefaultYandexDir — корень архива на Диске, когда ARCHIVE_YANDEX_DIR пуст: каталог настраивается
// (dev-прогон, приёмка), но у штатного конвейера он один, и подставлять его руками в каждую команду
// незачем — забытая переменная отключала бы архивацию молча.
const DefaultYandexDir = "disk:/rosstat-archive"

// FromEnv собирает хранилище по окружению. Пустой EnvBackend — архивация выключена: это (nil, nil),
// штатный режим, а не ошибка: архив — опция и включается парой «бэкенд настроен + у импортёра
// объявлена точка контроля» (спека §3.3). Любая другая неудача — ошибка конфигурации, и вызывающий
// (util) логирует её и продолжает импорт без архива: недоступный архив не должен останавливать
// данные (спека §3.1). Поэтому каждый текст ошибки называет переменную окружения: по логу дежурный
// должен понять, какую строку править, а не искать причину в API хранилища.
func FromEnv(hc *http.Client) (Store, error) {
	backend := os.Getenv(EnvBackend)
	switch backend {
	case "":
		return nil, nil
	case "local":
		dir := os.Getenv(EnvLocalDir)
		if dir == "" {
			return nil, fmt.Errorf("%s не задан (нужен при %s=local)", EnvLocalDir, EnvBackend)
		}
		return NewLocal(dir), nil
	case "yandex":
		// Токен — секрет: его значение уходит только в конструктор Диска вместе с hc, а в текст
		// ошибки и в лог не попадает — лог читает дежурный (правило 1 AGENTS.md). Хранилище
		// собирается и при недоступном Диске: проверка конфигурации не должна ходить в сеть
		// (правило 3 AGENTS.md), а первый запрос всё равно случится только на архивации версии.
		token := os.Getenv(EnvYandexToken)
		if token == "" {
			return nil, fmt.Errorf("%s не задан (нужен при %s=yandex)", EnvYandexToken, EnvBackend)
		}
		return NewYandex(token, yandexRoot(), "", hc), nil
	default:
		return nil, fmt.Errorf("неизвестный %s=%q (ожидается local или yandex)", EnvBackend, backend)
	}
}

// yandexRoot — корень архива на Диске: ARCHIVE_YANDEX_DIR, а при пустом значении DefaultYandexDir.
// Путь не секрет (в отличие от токена), поэтому его можно и показать в тексте ошибки.
func yandexRoot() string {
	if root := os.Getenv(EnvYandexDir); root != "" {
		return root
	}
	return DefaultYandexDir
}
