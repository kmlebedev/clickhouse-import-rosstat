package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/kmlebedev/clickhouse-import-rosstat/archive"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/bank"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/bea"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/bls"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/bold"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/calendar"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/cbr"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/craw"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/customs"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/dcf"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/edisclosure"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/eia"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/fao"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/financial"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/forecast"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/fred"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/gold"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/minfin"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/mlregistry"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/moex"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/news"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/polyus"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/rosstat"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/views"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/wgc"
	log "github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

var (
	ctx = context.Background()
)

// Режимы ARCHIVE_ACTION: registry обновляет git-зеркало реестра винтажного архива, list печатает
// версии и сверяет хранилище с реестром, restore не завершает процесс, а переключает обычный путь
// импортёров на восстановление из архива (util.ArchiveRestoreMode). registry и list —
// самостоятельные действия: сделали своё и вышли; реестру и листингу ClickHouse не нужен, поэтому
// выход стоит до ParseDSN (подключения к базе в этих режимах не происходит вовсе). Имя переменной
// и значения общие с util (util.EnvArchiveAction): вторая копия строки в шаблоне разошлась бы с
// разбором в main молча, и restore пошёл бы обычным путём — с сетью и полным импортом. Путь
// зеркала настраивается (ARCHIVE_REGISTRY_OUT), но штатно он один: цель Makefile и README
// называют этот же файл; листинг настраивают ARCHIVE_LIST_SOURCE (фильтр источников) и
// ARCHIVE_RECONCILE (сверка с содержимым хранилища).
const (
	envArchiveRegistryOut        = "ARCHIVE_REGISTRY_OUT"
	envArchiveRegistryAllowEmpty = "ARCHIVE_REGISTRY_ALLOW_EMPTY"
	defaultRegistryOut           = "docs/archive/registry.jsonl"
	envArchiveListSource         = "ARCHIVE_LIST_SOURCE"
	envArchiveReconcile          = "ARCHIVE_RECONCILE"
)

func main() {
	if lvl, err := log.ParseLevel(os.Getenv("LOG_LEVEL")); err == nil {
		log.SetLevel(lvl)
	}
	// Неизвестное действие — ошибка, а не «пусть прогонит импортёры»: иначе опечатка
	// (ARCHIVE_ACTION=Registry) молча запускала бы полный импорт по расписанию dagu.
	switch action := os.Getenv(util.EnvArchiveAction); action {
	case "":
		// Обычный прогон импортёров — без архива и режимов.
	case util.ArchiveActionRegistry:
		if err := runArchiveRegistry(ctx); err != nil {
			log.Fatal(err)
		}
		return
	case util.ArchiveActionList:
		if err := runArchiveList(ctx); err != nil {
			log.Fatal(err)
		}
		return
	case util.ArchiveActionRestore:
		// restore процесс не завершает: ниже продолжается обычный путь импортёров, и каждый
		// выбранный работает по архиву. Фильтр обязателен: пустой CLICKHOUSE_IMPORT_STAT означал бы
		// «восстановить всё», а это не ручное действие на один импортёр, а случайный полный прогон.
		if len(parseImportFilter(os.Getenv("CLICKHOUSE_IMPORT_STAT"))) == 0 {
			log.Fatalf("режим восстановления (%s=%s) требует непустой CLICKHOUSE_IMPORT_STAT с именем импортёра", util.EnvArchiveAction, util.ArchiveActionRestore)
		}
	default:
		log.Fatalf("неизвестное %s=%q (ожидается %s, %s или %s)", util.EnvArchiveAction, action, util.ArchiveActionRegistry, util.ArchiveActionList, util.ArchiveActionRestore)
	}
	clickhouseOptions, err := clickhouse.ParseDSN(os.Getenv("CLICKHOUSE_URL"))
	if err != nil {
		log.Fatal("invalid CLICKHOUSE_URL")
	}
	conn, err := clickhouse.Open(clickhouseOptions)
	if err != nil {
		log.Fatal(err)
	}
	if err = conn.Ping(ctx); err != nil {
		log.Fatal(err)
	}
	log.Infof("Connected to clickhouse")
	requested := parseImportFilter(os.Getenv("CLICKHOUSE_IMPORT_STAT"))
	matched := make(map[string]bool)
	failed := false
	for _, stat := range chimport.Stats {
		if len(requested) > 0 && !slices.Contains(requested, stat.Name()) {
			continue
		}
		matched[stat.Name()] = true
		var rows int64
		if rows, err = stat.Import(ctx, conn); err != nil {
			log.Errorf("%s: %+v", stat.Name(), err)
			failed = true
		}
		log.Infof("Imported %d rows of %s", rows, stat.Name())
	}
	for _, name := range requested {
		if !matched[name] {
			log.Warnf("no importer named %q", name)
		}
	}
	if failed {
		os.Exit(1)
	}
}

// runArchiveRegistry скачивает реестр версий (registry.jsonl) из хранилища винтажного архива
// и кладёт его в репозиторий — это git-зеркало реестра. Истина — файл в самом хранилище:
// зеркало читают глазами и версионируют, а восстановление версий идёт по оригиналу.
// Отсутствие файла в хранилище — не ошибка, а ответ «архив ещё не наполнялся»: пишем пустое
// зеркало, иначе первый прогон падал бы на пустом архиве, а зеркало в git задним числом
// врало бы «архива нет» вместо «пока ничего не сохранено». Пустой реестр поверх **непустого**
// зеркала — наоборот, отказ: пустой файл неотличим от «все версии потеряны», и затирание
// разрешает только явный ARCHIVE_REGISTRY_ALLOW_EMPTY=1 (с warn в логе).
func runArchiveRegistry(ctx context.Context) error {
	store, err := archive.FromEnv(util.HttpClient)
	if err != nil {
		return err
	}
	if store == nil {
		// Пустой ARCHIVE_BACKEND штатно значит «архивация выключена», но у этого действия
		// другого источника реестра нет: молча вернуть пустой файл значило бы затереть зеркало
		// (выключенный архив неотличим от очищенного). Поэтому — ошибка, и без значения переменной.
		return fmt.Errorf("%s не задан", archive.EnvBackend)
	}
	out := os.Getenv(envArchiveRegistryOut)
	if out == "" {
		out = defaultRegistryOut
	}
	var body []byte
	exists, err := store.Exists(ctx, archive.RegistryKey)
	if err != nil {
		return err
	}
	if exists {
		if body, err = store.Get(ctx, archive.RegistryKey); err != nil {
			return err
		}
	} else {
		log.Warnf("реестр архива пуст: %s", archive.RegistryKey)
	}
	// Пустой реестр в хранилище — законный ответ «архив ещё не наполнялся», но непустое зеркало
	// он затрёт молча: пустой файл неотличим от «все версии потеряны», а зеркало в git — витрина
	// реестра и вход будущего восстановления (§3.4). Поэтому запись пустого файла поверх непустого —
	// отказ, и разрешает её только явный ARCHIVE_REGISTRY_ALLOW_EMPTY=1 с warn в логе: осознанное
	// действие дежурного, у которого архив действительно пересоздан с нуля.
	if len(bytes.TrimSpace(body)) == 0 {
		mirror, readErr := os.ReadFile(out)
		switch {
		case readErr == nil && len(bytes.TrimSpace(mirror)) > 0:
			if os.Getenv(envArchiveRegistryAllowEmpty) != "1" {
				return fmt.Errorf("реестр в хранилище пуст, а %s непуст: запись затёрла бы git-зеркало (%s=1 — разрешить)", out, envArchiveRegistryAllowEmpty)
			}
			log.Warnf("реестр в хранилище пуст: %s затирается по %s=1", out, envArchiveRegistryAllowEmpty)
		case readErr != nil && !errors.Is(readErr, os.ErrNotExist):
			// Прочитать зеркало не удалось — неизвестно, непусто ли оно, а писать поверх «наугад»
			// значит потерять его молча; та же ошибка была бы и у записи.
			return fmt.Errorf("чтение %s: %w", out, readErr)
		}
	}
	if err = os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return err
	}
	if err = os.WriteFile(out, body, 0o644); err != nil {
		return err
	}
	// Строк — по переводам строки: реестр пишется построчно с \n в конце, поэтому число
	// завершённых строк и есть число записей (пустой файл — 0).
	log.Infof("реестр архива: %d строк -> %s", bytes.Count(body, []byte("\n")), out)
	return nil
}

// runArchiveList печатает версии реестра винтажного архива: «источник · имя · отпечаток(8) ·
// размер · first_seen · url». ClickHouse не нужен — реестр лежит в хранилище и читается без базы
// (как и у registry, выход стоит до ParseDSN). Строки с одинаковым sha256 помечаются дублем
// содержимого: один файл может лежать под sha- и fingerprint-ключом, и это две записи об одном
// содержимом, а не две версии (обязательный подпункт шага 1.2). ARCHIVE_LIST_SOURCE фильтрует
// печать по источнику. ARCHIVE_RECONCILE=1 добавляет сверку с фактическим содержимым хранилища:
// объекты без строк реестра — находка, и работа завершается ненулевым кодом возврата (молчаливое
// «чистое зеркало» при расхождении недопустимо); реконсиляция возможна только у бэкендов с
// листингом — у остальных это отсутствие возможности, а не находка (спека §3.2).
func runArchiveList(ctx context.Context) error {
	store, err := archive.FromEnv(util.HttpClient)
	if err != nil {
		return err
	}
	if store == nil {
		// Пустой ARCHIVE_BACKEND штатно значит «архивации нет», но у листинга другого источника
		// реестра нет: пустой вывод выглядел бы как пустой архив (как и у registry — ошибка).
		return fmt.Errorf("%s не задан", archive.EnvBackend)
	}
	entries, err := archive.LoadEntries(ctx, store)
	if err != nil {
		return err
	}
	source := os.Getenv(envArchiveListSource)
	// Дубли считаются по всему реестру, а не по печатаемой выборке: одинаковый sha256 — свойство
	// содержимого, а фильтр лишь сокращает вывод.
	sums := make(map[string]int, len(entries))
	for _, e := range entries {
		if e.SHA256 != "" {
			sums[e.SHA256]++
		}
	}
	printed := 0
	for _, e := range entries {
		if source != "" && e.Source != source {
			continue
		}
		printed++
		line := fmt.Sprintf("%s · %s · %s · %d · %s · %s",
			e.Source, e.Name, fingerprintPrefix(e.Fingerprint), e.Size, e.FirstSeen.Format(time.RFC3339), e.URL)
		if sums[e.SHA256] > 1 {
			line += " · дубль содержимого"
		}
		log.Infof("%s", line)
	}
	if source != "" {
		log.Infof("%s=%s: строк %d из %d", envArchiveListSource, source, printed, len(entries))
	} else {
		log.Infof("версий в реестре: %d", len(entries))
	}
	if os.Getenv(envArchiveReconcile) == "1" {
		return reconcileArchive(ctx, store, entries, source)
	}
	return nil
}

// fingerprintPrefix — первые 8 символов отпечатка версии: по ним дежурный сверяет строку с ключом
// объекта и записью source_publications, не читая 32 символа целиком (полный отпечаток — в
// реестре). Пустой отпечаток (HEAD при архивации был недоступен, ключ по sha256) печатается
// дефисом: пустое место в строке выглядело бы как потерянное поле.
func fingerprintPrefix(fingerprint string) string {
	switch {
	case fingerprint == "":
		return "-"
	case len(fingerprint) > 8:
		return fingerprint[:8]
	default:
		return fingerprint
	}
}

// reconcileArchive сверяет объекты хранилища со строками реестра: «есть объект — нет строки» —
// ровно то, что самолечение не лечит (no-op-путь до записи реестра не доходит), и находка должна
// быть видна, а не растворяться в выводе листинга. Фильтр источника действует и здесь: с ним
// проверяются только объекты выбранного источника, иначе чужие версии выглядели бы находками.
// Бэкенд без листинга — не находка, а отсутствие возможности: явное сообщение в лог и нулевой код
// возврата (листинг Яндекс.Диска — остаток пункта 1.2, спека §3.2).
func reconcileArchive(ctx context.Context, store archive.Store, entries []archive.Entry, source string) error {
	lister, ok := store.(archive.Lister)
	if !ok {
		log.Warnf("реконсиляция недоступна для бэкенда %s: листинг не реализован (остаток шага 1.2)", store.Name())
		return nil
	}
	keys, err := lister.List(ctx)
	if err != nil {
		return err
	}
	known := make(map[string]bool, len(entries))
	for _, e := range entries {
		if source == "" || e.Source == source {
			known[e.Key] = true
		}
	}
	prefix := ""
	if source != "" {
		prefix = source + "/"
	}
	checked, missing := 0, 0
	for _, key := range keys {
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			continue
		}
		checked++
		if !known[key] {
			missing++
			log.Warnf("объект без строки реестра: %s", key)
		}
	}
	if missing > 0 {
		return fmt.Errorf("объектов без строк реестра: %d из %d проверенных (залейте зеркало реестра или разберите объекты: потерянная строка при живом объекте сама не возвращается)", missing, checked)
	}
	log.Infof("реконсиляция: проверено объектов %d, все есть в реестре", checked)
	return nil
}

func parseImportFilter(value string) []string {
	var names []string
	for _, name := range strings.Split(value, ",") {
		if name = strings.TrimSpace(name); name != "" {
			names = append(names, name)
		}
	}
	return names
}
