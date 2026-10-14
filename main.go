package main

import (
	"bytes"
	"context"
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
	_ "github.com/kmlebedev/clickhouse-import-rosstat/eia"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/fao"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/financial"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/forecast"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/fred"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/gold"
	_ "github.com/kmlebedev/clickhouse-import-rosstat/minfin"
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
)

var (
	ctx = context.Background()
)

// Режим одного действия ARCHIVE_ACTION: значение registry обновляет git-зеркало реестра
// винтажного архива и завершает процесс — реестру ClickHouse не нужен, поэтому выход стоит
// до ParseDSN (подключения к базе в этом режиме не происходит вовсе). Путь зеркала настраивается
// (ARCHIVE_REGISTRY_OUT), но штатно он один: цель Makefile и README называют этот же файл.
const (
	archiveActionRegistry = "registry"
	envArchiveRegistryOut = "ARCHIVE_REGISTRY_OUT"
	defaultRegistryOut    = "docs/archive/registry.jsonl"
)

func main() {
	if lvl, err := log.ParseLevel(os.Getenv("LOG_LEVEL")); err == nil {
		log.SetLevel(lvl)
	}
	if action := os.Getenv("ARCHIVE_ACTION"); action != "" {
		// Неизвестное действие — ошибка, а не «пусть прогонит импортёры»: иначе опечатка
		// (ARCHIVE_ACTION=Registry) молча запускала бы полный импорт по расписанию dagu.
		if action != archiveActionRegistry {
			log.Fatalf("неизвестное ARCHIVE_ACTION (ожидается %s)", archiveActionRegistry)
		}
		if err := runArchiveRegistry(ctx); err != nil {
			log.Fatal(err)
		}
		return
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
// врало бы «архива нет» вместо «пока ничего не сохранено».
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

func parseImportFilter(value string) []string {
	var names []string
	for _, name := range strings.Split(value, ",") {
		if name = strings.TrimSpace(name); name != "" {
			names = append(names, name)
		}
	}
	return names
}
