package archive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// registryTestEntry — запись в том виде, в каком её собирает util после архивации версии: ключ из
// Key (префикс отпечатка HEAD), sha256 фактического содержимого, размер, URL аудита. Имя входит и
// в name, и в URL: выгрузки ЦБ и Росстата названы по-русски, и запись обязана доехать до зеркала
// без искажений.
func registryTestEntry(name, sum string, size int64) Entry {
	return Entry{
		Source:      "cbr_m2",
		Name:        name,
		Fingerprint: fp,
		SHA256:      sum,
		Size:        size,
		URL:         "https://www.cbr.ru/vfs/macro/" + name,
		Key:         Key("cbr_m2", name, fp, zeroSum),
		FirstSeen:   time.Date(2026, 10, 14, 9, 20, 11, 0, time.FixedZone("MSK", 3*60*60)),
	}
}

// sumHex — sha256 содержимого в hex, как его пишет util: у двух версий одного файла различается
// ровно это поле (отпечаток HEAD при неизменных заголовках остаётся прежним, §3.2).
func sumHex(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}

// registryPutCounter считает записи в хранилище. Байты файла одинаковы и при перезаписи тем же
// содержимым, поэтому «повторный Ensure не делает Put» иначе не отличить от «перезаписал то же
// самое»; хранилище при этом настоящее — LocalStore в t.TempDir().
type registryPutCounter struct {
	Store
	puts int
}

func (s *registryPutCounter) Put(ctx context.Context, key string, body []byte) error {
	s.puts++
	return s.Store.Put(ctx, key, body)
}

// registryBytes — байты registry.jsonl с диска, а не через Store: проверяется ровно тот файл,
// который уезжает в git-зеркало и станет входом будущего восстановления.
func registryBytes(t *testing.T, dir string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(dir, RegistryKey))
	if err != nil {
		t.Fatalf("чтение %s: %v", RegistryKey, err)
	}
	return body
}

// registryLines — строки файла реестра. JSONL: перевод строки отделяет записи, поэтому файл обязан
// им заканчиваться, а последний перевод строки новой записи не создаёт.
func registryLines(t *testing.T, dir string) []string {
	t.Helper()
	body := string(registryBytes(t, dir))
	if body == "" {
		return nil
	}
	if !strings.HasSuffix(body, "\n") {
		t.Fatalf("реестр не заканчивается переводом строки: %q", body)
	}
	return strings.Split(strings.TrimSuffix(body, "\n"), "\n")
}

// decodedEntries — разобранные строки файла: тесты проверяют и порядок строк, и содержимое, но
// именно по файлу, а не по памяти процесса.
func decodedEntries(t *testing.T, dir string) []Entry {
	t.Helper()
	lines := registryLines(t, dir)
	out := make([]Entry, 0, len(lines))
	for i, line := range lines {
		var e Entry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("строка %d реестра не разбирается как запись: %v (%s)", i+1, err, line)
		}
		out = append(out, e)
	}
	return out
}

// assertRegistryEntry — сравнение по полям: time.Time несравним через ==, а поле, потерянное из-за
// неверного тега, видно именно здесь.
func assertRegistryEntry(t *testing.T, got, want Entry) {
	t.Helper()
	if got.Source != want.Source || got.Name != want.Name || got.Fingerprint != want.Fingerprint ||
		got.SHA256 != want.SHA256 || got.Size != want.Size || got.URL != want.URL || got.Key != want.Key {
		t.Errorf("запись реестра = %+v, ожидалось %+v", got, want)
	}
	if !got.FirstSeen.Equal(want.FirstSeen) {
		t.Errorf("first_seen = %s, ожидалось %s", got.FirstSeen, want.FirstSeen)
	}
}

// Самый первый прогон: файла нет, и это не ошибка — Ensure заводит реестр одной строкой (Review
// Focus №1). Формат пинится по файлу: одна JSON-строка, перевод строки в конце, ровно восемь
// полей спеки §3.4 — потерянное поле лишает восстановление key/url/sha256.
func TestRegistryEnsureCreatesMissingFile(t *testing.T) {
	dir := t.TempDir()
	store := NewLocal(dir)
	ctx := context.Background()
	want := registryTestEntry("monetary_agg_SA.xlsx", sumHex("версия первая"), 583746)

	if err := NewRegistry(store).Ensure(ctx, want); err != nil {
		t.Fatalf("Ensure в пустое хранилище: %v", err)
	}
	// Имя файла — часть контракта: по нему реестр находят зеркало, ARCHIVE_ACTION=registry и будущее
	// восстановление, поэтому проверяется литералом на диске, а не сравнением с константой, из
	// которой собран путь в хелперах теста.
	if _, err := os.Stat(filepath.Join(dir, "registry.jsonl")); err != nil {
		t.Errorf("файла с ожидаемым именем в каталоге архива нет: %v", err)
	}

	lines := registryLines(t, dir)
	if len(lines) != 1 {
		t.Fatalf("строк в реестре %d, ожидалась 1:\n%s", len(lines), strings.Join(lines, "\n"))
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(lines[0]), &raw); err != nil {
		t.Fatalf("строка реестра не JSON-объект: %v (%s)", err, lines[0])
	}
	if len(raw) != 8 {
		t.Errorf("полей в строке %d, ожидалось 8: %s", len(raw), lines[0])
	}
	for _, field := range []string{"source", "name", "fingerprint", "sha256", "size", "url", "key", "first_seen"} {
		if _, ok := raw[field]; !ok {
			t.Errorf("в строке реестра нет поля %q: %s", field, lines[0])
		}
	}
	assertRegistryEntry(t, decodedEntries(t, dir)[0], want)
}

// Пустой файл — тоже пустой реестр (Review Focus №1): обнулённый или потерянный реестр не должен
// ронять архивацию, Ensure заводит в нём первую строку. Ошибка здесь означала бы, что после сбоя
// инструмента зеркала архив перестал пополняться, и версии терялись бы молча.
func TestRegistryEnsureEmptyFile(t *testing.T) {
	dir := t.TempDir()
	store := NewLocal(dir)
	ctx := context.Background()
	want := registryTestEntry("monetary_agg_SA.xlsx", sumHex("версия первая"), 583746)
	if err := os.WriteFile(filepath.Join(dir, RegistryKey), nil, 0o644); err != nil {
		t.Fatalf("подготовка пустого %s: %v", RegistryKey, err)
	}

	if err := NewRegistry(store).Ensure(ctx, want); err != nil {
		t.Fatalf("Ensure в пустой реестр: %v", err)
	}

	if lines := registryLines(t, dir); len(lines) != 1 {
		t.Fatalf("строк в реестре %d, ожидалась 1:\n%s", len(lines), strings.Join(lines, "\n"))
	}
	assertRegistryEntry(t, decodedEntries(t, dir)[0], want)
}

// Повторная архивация той же версии (тот же Key, тот же sha256) — не запись: байты файла обязаны
// остаться прежними, а Put — не состояться (перезапись тем же содержимым стоила бы запроса к
// хранилищу на каждый прогон каждого импортёра, а на Диске это ещё и лишний трафик). Первый
// повтор идёт в тот же Registry (версия уже в памяти), второй — в новый (следующий прогон: реестр
// читается с нуля, и «уже записано» видно по файлу).
func TestRegistryEnsureIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	store := &registryPutCounter{Store: NewLocal(dir)}
	ctx := context.Background()
	e := registryTestEntry("monetary_agg_SA.xlsx", sumHex("версия первая"), 583746)

	reg := NewRegistry(store)
	if err := reg.Ensure(ctx, e); err != nil {
		t.Fatalf("первый Ensure: %v", err)
	}
	if store.puts != 1 {
		t.Fatalf("после первой записи Put вызван %d раз, ожидался 1", store.puts)
	}
	before := registryBytes(t, dir)

	for _, repeat := range []*Registry{reg, NewRegistry(store)} {
		if err := repeat.Ensure(ctx, e); err != nil {
			t.Fatalf("повторный Ensure: %v", err)
		}
	}
	if after := registryBytes(t, dir); !bytes.Equal(before, after) {
		t.Errorf("повторный Ensure изменил файл реестра:\nдо:    %q\nпосле: %q", before, after)
	}
	if store.puts != 1 {
		t.Errorf("повторный Ensure сделал Put (%d вызовов против 1): содержимое не менялось", store.puts)
	}
	if lines := registryLines(t, dir); len(lines) != 1 {
		t.Errorf("строк в реестре %d, ожидалась 1", len(lines))
	}
}

// Содержимое версии под тем же Key изменилось (заголовки те же — sha256 другой): строка заменяется,
// число строк не растёт (last-wins, §3.4). Прежняя версия в реестре не остаётся: две строки на один
// key сломали бы поиск версии при восстановлении. Заменённая строка уезжает в конец с новым
// first_seen — версию увидели заново, а файл обязан быть упорядочен по этому полю (§3.4), иначе
// порядок строк перестал бы соответствовать ему. Ensure идёт из нового Registry: версия могла
// смениться между прогонами, и решение принимается по файлу, а не по памяти процесса.
func TestRegistryEnsureReplacesChangedContent(t *testing.T) {
	dir := t.TempDir()
	store := NewLocal(dir)
	ctx := context.Background()
	first := registryTestEntry("monetary_agg_SA.xlsx", sumHex("версия первая"), 583746)
	other := registryTestEntry("other.xlsx", sumHex("другой файл"), 10)
	// Время версий разное: по нему проверяется порядок строк, а не только их число.
	other.FirstSeen = other.FirstSeen.Add(time.Hour)
	second := registryTestEntry("monetary_agg_SA.xlsx", sumHex("версия вторая"), 601233)
	second.FirstSeen = other.FirstSeen.Add(time.Hour)
	if first.Key != second.Key {
		t.Fatalf("ключи тестовых записей разошлись: %q и %q", first.Key, second.Key)
	}

	reg := NewRegistry(store)
	for _, e := range []Entry{first, other} {
		if err := reg.Ensure(ctx, e); err != nil {
			t.Fatalf("Ensure %s: %v", e.Key, err)
		}
	}
	if err := NewRegistry(store).Ensure(ctx, second); err != nil {
		t.Fatalf("Ensure изменившейся версии: %v", err)
	}

	entries := decodedEntries(t, dir)
	if len(entries) != 2 {
		t.Fatalf("строк в реестре %d, ожидалось 2 (замена, а не дописывание):\n%+v", len(entries), entries)
	}
	assertRegistryEntry(t, entries[0], other)  // соседка поднялась выше
	assertRegistryEntry(t, entries[1], second) // изменившаяся версия — последняя строка
	for i := 1; i < len(entries); i++ {
		if entries[i].FirstSeen.Before(entries[i-1].FirstSeen) {
			t.Errorf("строка %d (%s) старее предыдущей (%s): файл перестал быть упорядочен по first_seen",
				i+1, entries[i].FirstSeen, entries[i-1].FirstSeen)
		}
	}
}

// Новый ключ дописывается строкой ниже: порядок строк — порядок добавления (first_seen, §3.4),
// поэтому реестр читается сверху вниз как история архива, а не как случайный набор версий.
func TestRegistryAppendsNewKeys(t *testing.T) {
	dir := t.TempDir()
	store := NewLocal(dir)
	ctx := context.Background()
	first := registryTestEntry("monetary_agg_SA.xlsx", sumHex("версия первая"), 583746)
	second := registryTestEntry("other.xlsx", sumHex("другой файл"), 10)

	if err := NewRegistry(store).Ensure(ctx, first); err != nil {
		t.Fatalf("Ensure первой версии: %v", err)
	}
	// Второй ключ — из нового Registry: так выглядит следующий прогон, реестр читается из файла.
	if err := NewRegistry(store).Ensure(ctx, second); err != nil {
		t.Fatalf("Ensure второй версии: %v", err)
	}

	entries := decodedEntries(t, dir)
	if len(entries) != 2 {
		t.Fatalf("строк в реестре %d, ожидалось 2:\n%+v", len(entries), entries)
	}
	assertRegistryEntry(t, entries[0], first)
	assertRegistryEntry(t, entries[1], second)
}

// Entries отдаёт копию: правка возвращённого слайса не должна менять ни состояние реестра, ни
// файл — иначе случайная мутация у вызывающего разошлась бы с реестром, объявленным источником
// истины. До первого Ensure список пуст: файл читается лениво и только тогда, когда нужен записи.
func TestRegistryEntriesReturnsCopy(t *testing.T) {
	dir := t.TempDir()
	store := NewLocal(dir)
	ctx := context.Background()
	e := registryTestEntry("monetary_agg_SA.xlsx", sumHex("версия первая"), 583746)

	reg := NewRegistry(store)
	if got := reg.Entries(); len(got) != 0 {
		t.Errorf("до первого Ensure Entries вернул %d записей, ожидалось 0", len(got))
	}
	if err := reg.Ensure(ctx, e); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	before := registryBytes(t, dir)

	got := reg.Entries()
	if len(got) != 1 {
		t.Fatalf("Entries вернул %d записей, ожидалась 1", len(got))
	}
	got[0].SHA256 = sumHex("подмена")
	got[0].Key = "cbr_m2/подмена.xlsx"
	got[0].FirstSeen = time.Now()

	again := reg.Entries()
	if len(again) != 1 {
		t.Fatalf("после правки копии Entries вернул %d записей, ожидалась 1", len(again))
	}
	assertRegistryEntry(t, again[0], e)
	if after := registryBytes(t, dir); !bytes.Equal(before, after) {
		t.Errorf("правка копии изменила файл реестра:\nдо:    %q\nпосле: %q", before, after)
	}
}

// Битая строка реестра — ошибка, а не пропуск: реестр объявлен источником истины, и молча
// потерянная строка означает потерянную версию — следующий Ensure записал бы файл уже без неё.
// По той же причине при ошибке разбора файл не перезаписывается: прежнее содержимое остаётся для
// разбора дежурным (номер битой строки — в тексте ошибки).
func TestRegistryBrokenLineIsError(t *testing.T) {
	dir := t.TempDir()
	store := NewLocal(dir)
	ctx := context.Background()
	good := `{"source":"cbr_m2","name":"full.xlsx","fingerprint":"` + fp + `","sha256":"b7d1","size":1,` +
		`"url":"https://www.cbr.ru/vfs/macro/full.xlsx","key":"cbr_m2/` + fp16 + `_full.xlsx",` +
		`"first_seen":"2026-10-14T09:20:11+03:00"}`
	body := []byte(good + "\n" + `{"source":"cbr_m2",` + "\n")
	if err := os.WriteFile(filepath.Join(dir, RegistryKey), body, 0o644); err != nil {
		t.Fatalf("подготовка реестра: %v", err)
	}

	err := NewRegistry(store).Ensure(ctx, registryTestEntry("other.xlsx", sumHex("версия вторая"), 10))
	if err == nil {
		t.Fatal("Ensure на реестре с битой строкой не дал ошибки")
	}
	if !strings.Contains(err.Error(), "строка 2") {
		t.Errorf("в ошибке нет номера битой строки: %v", err)
	}
	if after := registryBytes(t, dir); !bytes.Equal(body, after) {
		t.Errorf("при ошибке разбора реестр перезаписан:\nбыло:  %q\nстало: %q", body, after)
	}
}

// registryFailOnceStore — хранилище, у которого первая запись падает. Хранилище настоящее
// (LocalStore в t.TempDir()): отказывает только Put, поэтому проверяется поведение реестра, а не
// подмена хранилища.
type registryFailOnceStore struct {
	Store
	fail bool
}

func (s *registryFailOnceStore) Put(ctx context.Context, key string, body []byte) error {
	if s.fail {
		s.fail = false
		return errors.New("registry_test: запись в хранилище недоступна")
	}
	return s.Store.Put(ctx, key, body)
}

// Два экземпляра реестра на одном хранилище — это один реестр: файл перечитывается на каждой записи
// под пакетным мьютексом, поэтому строки, добавленные «соседом» (самолечение архива в другом
// импортёре, два вызывающих в одном процессе), не теряются. С кэшем на экземпляре трасса ниже
// оставляла бы в файле A и C: второй Ensure первого Registry не видел строки B и затёр бы её своим
// состоянием.
func TestRegistryInstancesShareState(t *testing.T) {
	dir := t.TempDir()
	store := NewLocal(dir)
	ctx := context.Background()
	first := registryTestEntry("first.xlsx", sumHex("версия первая"), 1)
	second := registryTestEntry("second.xlsx", sumHex("версия вторая"), 2)
	third := registryTestEntry("third.xlsx", sumHex("версия третья"), 3)

	r1, r2 := NewRegistry(store), NewRegistry(store)
	for _, step := range []struct {
		reg *Registry
		e   Entry
	}{{r1, first}, {r2, second}, {r1, third}} {
		if err := step.reg.Ensure(ctx, step.e); err != nil {
			t.Fatalf("Ensure %s: %v", step.e.Key, err)
		}
	}

	entries := decodedEntries(t, dir)
	if len(entries) != 3 {
		t.Fatalf("строк в реестре %d, ожидалось 3 (строка второго Registry потеряна):\n%+v", len(entries), entries)
	}
	for i, want := range []Entry{first, second, third} {
		assertRegistryEntry(t, entries[i], want)
	}
	if got := r1.Entries(); len(got) != 3 {
		t.Errorf("Entries первого Registry вернул %d записей, ожидалось 3: состояние берётся из файла, а не из кэша экземпляра", len(got))
	}
}

// url выгрузки почти всегда с query-параметрами, и «&» обязан лежать в файле дословно: реестр —
// ещё и git-зеркало, которое читают и ищут глазами (docs/archive/registry.jsonl), а \u0026 ищется не
// как url у источника. HTML-экранирование для JSON не обязательно, на разбор строки не влияет.
func TestRegistryURLWithoutHTMLEscaping(t *testing.T) {
	dir := t.TempDir()
	store := NewLocal(dir)
	ctx := context.Background()
	e := registryTestEntry("list.xlsx", sumHex("версия первая"), 10)
	e.URL = "https://www.cbr.ru/vfs/macro/list.xlsx?id=1&lang=ru"

	if err := NewRegistry(store).Ensure(ctx, e); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	line := registryLines(t, dir)[0]
	if !strings.Contains(line, e.URL) {
		t.Errorf("url лежит в файле не дословно: %s", line)
	}
	if strings.Contains(line, `\u0026`) {
		t.Errorf("«&» в url экранирован: %s", line)
	}
	assertRegistryEntry(t, decodedEntries(t, dir)[0], e)
}

// Сбой Put — сбой архивации (§3.3): файл остаётся ровно таким, каким был, состояние не принимается
// (Entries не показывает строку, которой в файле нет), а следующий Ensure повторяет запись, и версия
// доезжает. Без этого неудачная запись одной версии «вылечилась» бы следующим Ensure, который
// записал бы состояние в памяти — строкой на объект, которого в хранилище нет.
func TestRegistryPutFailureKeepsState(t *testing.T) {
	dir := t.TempDir()
	store := &registryFailOnceStore{Store: NewLocal(dir)}
	ctx := context.Background()
	first := registryTestEntry("monetary_agg_SA.xlsx", sumHex("версия первая"), 583746)
	second := registryTestEntry("other.xlsx", sumHex("другой файл"), 10)
	reg := NewRegistry(store)
	if err := reg.Ensure(ctx, first); err != nil {
		t.Fatalf("Ensure первой версии: %v", err)
	}
	before := registryBytes(t, dir)

	store.fail = true
	if err := reg.Ensure(ctx, second); err == nil {
		t.Fatal("Ensure при падающем Put не дал ошибки")
	}
	if after := registryBytes(t, dir); !bytes.Equal(before, after) {
		t.Errorf("после сбоя Put файл реестра изменился:\nдо:    %q\nпосле: %q", before, after)
	}
	if got := reg.Entries(); len(got) != 1 {
		t.Errorf("после сбоя Put Entries вернул %d записей, ожидалась 1 (состояние не принято):\n%+v", len(got), got)
	}

	if err := reg.Ensure(ctx, second); err != nil {
		t.Fatalf("Ensure после восстановления хранилища: %v", err)
	}
	entries := decodedEntries(t, dir)
	if len(entries) != 2 {
		t.Fatalf("строк в реестре %d, ожидалось 2 (повторная запись не состоялась):\n%+v", len(entries), entries)
	}
	assertRegistryEntry(t, entries[0], first)
	assertRegistryEntry(t, entries[1], second)
}

// Мьютекс разводит писателей внутри процесса: файл пишется целиком, поэтому два Ensure из горутин
// одного процесса (самолечение архива в соседнем импортёре, ручной прогон рядом с dagu) в
// read-modify-write затёрли бы строку друг друга. Проверяется на -race: гонки нет, и все записи
// на месте.
func TestRegistryConcurrentEnsure(t *testing.T) {
	dir := t.TempDir()
	store := NewLocal(dir)
	ctx := context.Background()
	reg := NewRegistry(store)
	const writers = 8

	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("file_%d.xlsx", i)
			if err := reg.Ensure(ctx, registryTestEntry(name, sumHex(name), int64(i))); err != nil {
				t.Errorf("Ensure %s: %v", name, err)
			}
			reg.Entries() // чтение параллельно записи: так реестр читает соседний вызывающий
		}(i)
	}
	wg.Wait()

	entries := decodedEntries(t, dir)
	if len(entries) != writers {
		t.Fatalf("строк в реестре %d, ожидалось %d (потеря записи при конкурентном Ensure):\n%+v",
			len(entries), writers, entries)
	}
	seen := make(map[string]bool, writers)
	for _, e := range entries {
		seen[e.Key] = true
	}
	if len(seen) != writers {
		t.Errorf("уникальных ключей в реестре %d, ожидалось %d", len(seen), writers)
	}
}
