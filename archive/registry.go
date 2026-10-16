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
// Идентичность строки — Key. Секретов в записи нет: все восемь полей (source, name, fingerprint,
// sha256, size, url, key, first_seen) публичны — запись и есть реестр версий, он уезжает в
// git-зеркало и служит входом будущего восстановления (§3.4).
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

// registryMu — мьютекс процесса, под которым идёт весь read-modify-write файла реестра и чтение
// состояния для Entries. Пакетный, а не на экземпляр: файл читается целиком на каждой записи, и
// два Registry в одном процессе (самолечение архива в соседнем импортёре, ручной прогон рядом с
// dagu) без общего замка не видели бы строк друг друга и затёрли бы их своими состояниями.
// Писателей между процессами он не разводит — для этого штатный писатель один (последовательная
// цепочка dagu), а строка, потерянная конкурентным ручным прогоном, при живом объекте сама не
// возвращается: no-op-путь (объект есть ⇒ no-op) до записи реестра не доходит; реконсиляция
// «объект без строки» для бэкендов с листингом — `make archive-list ARCHIVE_RECONCILE=1`
// (шаг 1.2 роадмапа, спека §3.4).
var registryMu sync.Mutex

// Registry — реестр версий: JSONL-файл в хранилище плюс состояние последнего завершённого Ensure
// в памяти — только для Entries, потому что читать реестр (и тем более писать) вызывающий не должен
// уметь мимо мьютекса. Чтение — на каждой записи: файл целиком перечитывается под registryMu, а
// прогон, который до архивации не дошёл, в хранилище не ходит вовсе. Запись — upsert по Key (та же
// версия с тем же sha256 не меняет ни строку, ни байты файла, иначе строка заменяется и уезжает в
// конец, новый ключ дописывается туда же), а весь файл уходит в хранилище одним Put: объект там
// пишется целиком, дописывания по строке нет.
type Registry struct {
	store Store

	// entries — копия содержимого файла после последнего завершённого Ensure этого экземпляра.
	// До первого Ensure пусто: реестр читается на записи, а не в конструкторе. Поле пишет Ensure и
	// читает Entries — оба под registryMu.
	entries []Entry
}

// NewRegistry — реестр поверх хранилища. В хранилище конструктор не ходит (ни Exists, ни Get) и
// файла не заводит: пустой реестр от несуществующего отличается только на записи, и решает это
// read — так прогон, который до архивации не дошёл, не создаёт пустой registry.jsonl.
func NewRegistry(store Store) *Registry {
	return &Registry{store: store}
}

// Ensure записывает версию в реестр. Ошибка — сбой архивации (§3.3): вызывающий не пишет строку
// версии в source_publications, и следующий прогон повторит архив целиком, поэтому заметить и
// разобрать сбой можно до того, как версия будет объявлена импортированной.
func (r *Registry) Ensure(ctx context.Context, e Entry) error {
	registryMu.Lock()
	defer registryMu.Unlock()

	entries, err := r.read(ctx)
	if err != nil {
		return err
	}
	next, changed := upsert(entries, e)
	if !changed {
		// Изменений нет и Put не нужен: состояние — только что прочитанный файл, а не прежний слайс
		// этого экземпляра, поэтому строки, добавленные соседним Registry, видны и в Entries.
		r.entries = entries
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

// Entries — копия состояния после последнего завершённого Ensure этого экземпляра: вызывающий не
// должен менять реестр через возвращённый слайс, а расхождение с файлом у источника истины дороже
// копии в десяток записей. Замок тот же пакетный и здесь: поле пишет Ensure, а читает это чтение
// параллельно с ним (гонку на поле ловит -race). До первого Ensure список пуст — реестр читается
// на записи; прочитать файл целиком (зеркало, восстановление) можно Store.Get, реестр для того и
// лежит в хранилище.
func (r *Registry) Entries() []Entry {
	registryMu.Lock()
	defer registryMu.Unlock()
	return append([]Entry(nil), r.entries...)
}

// LoadEntries — публичное чтение реестра версий из хранилища: строки registry.jsonl для листинга
// (ARCHIVE_ACTION=list) и восстановления (util.RestoreVersion). Разбор общий с записью реестра —
// приватный read делегирует сюда: реестр обязан читаться одинаково, и вторая копия парсера
// разошлась бы с первой на первой же правке формата (молча потерянная строка — потерянная версия).
// Отсутствие файла — не ошибка (пустой список): самый первый прогон заводит реестр сам, поэтому
// «нет файла» и «файл пуст» — пустой реестр (Review Focus №1), а не сбой чтения. Битая строка,
// наоборот, ошибка с номером строки: реестр объявлен источником истины, и пропустить строку молча
// значит потерять версию — следующий Put записал бы файл уже без неё.
func LoadEntries(ctx context.Context, store Store) ([]Entry, error) {
	exists, err := store.Exists(ctx, RegistryKey)
	if err != nil {
		return nil, fmt.Errorf("archive registry: проверка %s: %w", RegistryKey, err)
	}
	if !exists {
		return nil, nil
	}
	body, err := store.Get(ctx, RegistryKey)
	if err != nil {
		return nil, fmt.Errorf("archive registry: чтение %s: %w", RegistryKey, err)
	}
	return parseEntries(body)
}

// read перечитывает реестр целиком — на каждой записи, потому что состояние реестра в процессе
// общее, а экземпляры могут быть разными (см. комментарий к registryMu). Поведение чтения —
// общее с публичным LoadEntries (в т.ч. «нет файла = пустой реестр»): реестр один, и способы его
// прочитать не могут расходиться.
func (r *Registry) read(ctx context.Context) ([]Entry, error) {
	return LoadEntries(ctx, r.store)
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
// содержимого — sha256 (§3.4). Замена — с переносом строки в конец: у изменившейся версии first_seen
// новый (её увидели заново), а файл обязан быть упорядочен по first_seen, иначе порядок строк
// перестал бы соответствовать полю, которым он объявлен. Возвращается копия, а не правка слайса
// на месте: состояние принимается лишь после успешного Put, а строки соседей могли быть прочитаны
// из файла в том же слайсе.
func upsert(entries []Entry, e Entry) ([]Entry, bool) {
	for i, old := range entries {
		if old.Key != e.Key {
			continue
		}
		if old.SHA256 == e.SHA256 {
			return nil, false
		}
		next := append(slices.Clone(entries[:i]), entries[i+1:]...)
		return append(next, e), true
	}
	return append(slices.Clone(entries), e), true
}

// marshalEntries собирает файл: одна запись — одна строка JSON, перевод строки — разделитель
// (JSONL). Собирается весь файл целиком, потому что объект в хранилище пишется целиком. Encoder, а
// не Marshal: SetEscapeHTML(false) оставляет «&» в url дословным (?id=1&lang=ru) — реестр уезжает в
// git-зеркало и читается глазами, а \u0026 там только мешает искать. HTML-экранирование для JSON не
// обязательно, на разбор строки это не влияет (Entry разбирается тем же encoding/json).
func marshalEntries(entries []Entry) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	for _, e := range entries {
		if err := enc.Encode(e); err != nil {
			// Entry — плоские строки, число и время: сюда попасть нельзя. Но молча потерять запись
			// хуже ошибки — её видит вызывающий и не пишет строку версии в source_publications.
			return nil, fmt.Errorf("archive registry: %s: запись %q: %w", RegistryKey, e.Key, err)
		}
	}
	return buf.Bytes(), nil
}
