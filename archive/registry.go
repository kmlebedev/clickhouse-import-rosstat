package archive

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
)

// RegistryKey — ключ реестра версий: файл в корне хранилища, рядом с каталогами источников. Ключ
// фиксирован (в отличие от Key): реестр один на всё хранилище, и по одному имени его находит и
// вызывающий, и режим ARCHIVE_ACTION=registry, и git-зеркало.
const RegistryKey = "registry.jsonl"

// Entry — одна сохранённая версия файла источника, то есть строка реестра. Поля — из §3.4 спеки:
// fingerprint — отпечаток HEAD (как в source_publications, чтобы архив и таблица версий называли
// версию одинаково), sha256 — фактическое содержимое (при неизменных заголовках оно могло
// смениться), key — объект в хранилище, first_seen — когда конвейер увидел версию, url — аудит.
// Идентичность строки — Key. Секретов в записи нет: source/name/url/sha256 и есть весь её состав.
type Entry struct {
	Source      string    `json:"source"`
	Name        string    `json:"name"`
	Fingerprint string    `json:"fingerprint"`
	SHA256      string    `json:"sha256"`
	Size        int64     `json:"size"`
	URL         string    `json:"url"`
	Key         string    `json:"key"`
	FirstSeen   time.Time `json:"first_seen"`
}

// Registry — реестр версий: JSONL-файл в хранилище плюс его состояние в памяти процесса. Файл
// читается лениво, при первой записи: реестр нужен архивации, а прогон без архивации не должен
// ни читать, ни заводить его. Запись — upsert по Key (та же версия с тем же sha256 не меняет ни
// строку, ни байты файла, иначе строка заменяется на месте, новый ключ дописывается в конец), а
// весь файл уходит в хранилище одним Put: объект там пишется целиком, дописывания по строке нет.
//
// Мьютекс защищает состояние внутри процесса и не разводит писателей между процессами: штатный
// писатель один (последовательная цепочка dagu), а строка, потерянная конкурентным ручным прогоном,
// самолечится префлайтом (§3.4). Внутри же процесса сериализация обязательна: файл пишется целиком,
// и два Ensure (самолечение архива в одном импортёре и запись в соседнем) в read-modify-write
// затёрли бы строку друг друга.
type Registry struct {
	store Store

	mu      sync.Mutex
	loaded  bool    // файл прочитан; пустой реестр тоже считается прочитанным
	entries []Entry // порядок — порядок добавления, он же порядок строк в файле
}

// NewRegistry — реестр поверх хранилища. В хранилище конструктор не ходит (ни Exists, ни Get) и
// файла не заводит: пустой реестр от несуществующего отличается только на записи, и решает это
// load — так прогон, который до архивации не дошёл, не создаёт пустой registry.jsonl.
func NewRegistry(store Store) *Registry {
	return &Registry{store: store}
}

// Ensure записывает версию в реестр. Ошибка — сбой архивации (§3.3): вызывающий не пишет строку
// версии в source_publications, и следующий прогон повторит архив целиком, поэтому заметить и
// разобрать сбой можно до того, как версия будет объявлена импортированной.
func (r *Registry) Ensure(ctx context.Context, e Entry) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.load(ctx); err != nil {
		return err
	}
	next, changed := upsert(r.entries, e)
	if !changed {
		return nil
	}
	body, err := marshalEntries(next)
	if err != nil {
		return err
	}
	if err = r.store.Put(ctx, RegistryKey, body); err != nil {
		return fmt.Errorf("archive registry: запись %s: %w", RegistryKey, err)
	}
	// Состояние принимается только после успешного Put: иначе неудачная запись одной версии утекла
	// бы в файл следующим Ensure — строкой на объект, которого в хранилище нет (Put объекта идёт до
	// реестра и к этому моменту мог не состояться).
	r.entries = next
	return nil
}

// Entries — копия состояния реестра: вызывающий не должен менять реестр через возвращённый слайс,
// а расхождение с файлом у источника истины дороже копии в десяток записей. До первого Ensure
// список пуст — файл читается лениво; для чтения реестра целиком (зеркало) есть Store.Get.
func (r *Registry) Entries() []Entry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Entry(nil), r.entries...)
}

// load читает реестр при первом обращении. Отсутствие файла — не ошибка: самый первый прогон
// заводит его сам, поэтому «нет файла» и «файл пуст» — пустой реестр (Review Focus №1). Битая
// строка, наоборот, ошибка: реестр объявлен источником истины, и пропустить строку молча значит
// потерять версию — следующий Put записал бы файл уже без неё. loaded выставляется лишь после
// успеха: сбой чтения оставляет реестр непрочитанным, и это видит вызывающий.
func (r *Registry) load(ctx context.Context) error {
	if r.loaded {
		return nil
	}
	exists, err := r.store.Exists(ctx, RegistryKey)
	if err != nil {
		return fmt.Errorf("archive registry: проверка %s: %w", RegistryKey, err)
	}
	if !exists {
		r.loaded = true
		return nil
	}
	body, err := r.store.Get(ctx, RegistryKey)
	if err != nil {
		return fmt.Errorf("archive registry: чтение %s: %w", RegistryKey, err)
	}
	entries, err := parseEntries(body)
	if err != nil {
		return err
	}
	r.entries, r.loaded = entries, true
	return nil
}

// parseEntries разбирает JSONL и нумерует строки с единицы — по номеру дежурный находит битую
// строку и в зеркале (docs/archive/registry.jsonl), и в файле на Диске. Пустая строка записи не
// несёт и ошибкой не считается: перевод строки отделяет записи, и лишний мог оставить ручной
// разбор или инструмент зеркала, тогда как терять при этом нечего.
func parseEntries(body []byte) ([]Entry, error) {
	var entries []Entry
	for i, line := range strings.Split(string(body), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var e Entry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			return nil, fmt.Errorf("archive registry: %s: строка %d: %w", RegistryKey, i+1, err)
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// upsert возвращает новое состояние реестра и признак изменения. Та же версия (совпали и Key, и
// sha256) — изменений нет: сравниваются ровно эти поля, потому что версию называет key, а смену
// содержимого — sha256 (§3.4). Замена идёт на месте: строка остаётся там, где была записана
// впервые, и файл читается как порядок появления источников, а не по последнему обновлению.
// Возвращается копия, а не правка слайса на месте: состояние принимается лишь после успешного Put.
func upsert(entries []Entry, e Entry) ([]Entry, bool) {
	for i, old := range entries {
		if old.Key != e.Key {
			continue
		}
		if old.SHA256 == e.SHA256 {
			return nil, false
		}
		next := slices.Clone(entries)
		next[i] = e
		return next, true
	}
	return append(slices.Clone(entries), e), true
}

// marshalEntries собирает файл: одна запись — одна строка JSON, перевод строки — разделитель
// (JSONL). Собирается весь файл целиком, потому что объект в хранилище пишется целиком.
// json.Marshal экранирует «&» в url как \u0026 (он же экранирует «<» и «>»): на разбор это не
// влияет — строка остаётся тем же JSON, — а формат строки задаёт писатель, а не читатель.
func marshalEntries(entries []Entry) ([]byte, error) {
	var buf bytes.Buffer
	for _, e := range entries {
		line, err := json.Marshal(e)
		if err != nil {
			// Entry — плоские строки, число и время: сюда попасть нельзя. Но молча потерять запись
			// хуже ошибки — её видит вызывающий и не пишет строку версии в source_publications.
			return nil, fmt.Errorf("archive registry: %s: запись %q: %w", RegistryKey, e.Key, err)
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	return buf.Bytes(), nil
}
