package archive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Фиктивные токен и корень: настоящий токен в тесты не попадает (правило 1 AGENTS.md), а значение
// из теста работает ещё и маркером утечки — его не должно быть ни в текстах ошибок, ни в запросах
// по подписанной ссылке.
const (
	fakeToken = "SECRET_TOKEN_VALUE"
	fakeRoot  = "disk:/rosstat-archive"

	// Адреса подписанных ссылок эмулятора: их выдаёт ручка ресурсов, по ним уходит тело объекта.
	fakeUploadTarget   = "/upload-target/1"
	fakeDownloadTarget = "/download-target/1"

	// Ручки эмулятора — ключи очереди статусов и счётчика запросов.
	fakeMkdir    = "mkdir"    // PUT /resources — создание каталога
	fakeUpload   = "upload"   // GET /resources/upload — ссылка на загрузку
	fakeBody     = "body"     // PUT /upload-target/1 — тело объекта
	fakeDownload = "download" // GET /resources/download — ссылка на скачивание
	fakeData     = "data"     // GET /download-target/1 — байты объекта
	fakeExists   = "exists"   // GET /resources — наличие объекта
)

// fakeDir — что эмулятор отвечал про каталог: существует ли он и какие коды ушли на его создание
// (201 — завели, 409 — уже был).
type fakeDir struct {
	exists bool
	codes  []int
}

// fakeDisk — эмулятор REST API Диска на httptest-сервере: держит каталоги и объекты в памяти и
// ведёт журнал запросов клиента. Мок стоит на уровне http.Handler, а не на уровне методов клиента,
// поэтому тесты видят настоящие запросы — путь в query, заголовок авторизации, метод, тело — и
// ловят то, что иначе прошло бы незамеченным: токен на подписанной ссылке, потерянный overwrite,
// запрос не тем методом.
type fakeDisk struct {
	server *httptest.Server

	// Поля ниже пишутся и из хендлеров (горутина сервера), и из теста, поэтому доступ к ним —
	// только под mu: пакет гоняется с -race.
	mu      sync.Mutex
	dirs    map[string]*fakeDir
	objects map[string][]byte // путь объекта → байты; «удаление» — delete
	queue   map[string][]int  // ручка → статусы, которые она отдаст раньше штатного ответа
	calls   map[string]int    // ручка → число запросов

	apiAuth  []string // Authorization запросов к API Диска
	hrefAuth []string // Authorization запросов по подписанной ссылке

	uploadPaths   []string // path из resources/upload
	downloadPaths []string // path из resources/download
	existsPaths   []string // path из GET resources
	overwrites    []string // overwrite из resources/upload

	uploadPath   string // объект, на который выдана последняя ссылка загрузки
	downloadPath string // объект, на который выдана последняя ссылка скачивания

	// uploadHref подменяет адрес ссылки загрузки: тесту про утечку href нужна ссылка в никуда.
	uploadHref string
}

// fakeState — снимок журналов под мьютексом: хендлеры живут в горутинах сервера, и читать поля
// эмулятора из теста без блокировки нельзя.
type fakeState struct {
	dirCodes      map[string][]int
	objects       map[string][]byte
	calls         map[string]int
	apiAuth       []string
	hrefAuth      []string
	uploadPaths   []string
	downloadPaths []string
	existsPaths   []string
	overwrites    []string
}

// newFakeDisk поднимает эмулятор и закрывает его по окончании теста.
func newFakeDisk(t *testing.T) *fakeDisk {
	t.Helper()
	f := &fakeDisk{
		dirs:    map[string]*fakeDir{},
		objects: map[string][]byte{},
		queue:   map[string][]int{},
		calls:   map[string]int{},
	}
	f.server = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.server.Close)
	return f
}

// newFakeStore — клиент, направленный на эмулятор. Клиент без Timeout, как util.HttpClient в
// проде (правило 4 AGENTS.md): дедлайн операции живёт в самом бэкенде, и тест это проверяет.
func newFakeStore(f *fakeDisk) *YandexDisk {
	return NewYandex(fakeToken, fakeRoot, f.server.URL, &http.Client{})
}

// failNext ставит ручке статус-ответы раньше штатных: «первый ответ 429, второй нормальный» — ровно
// тот сценарий, ради которого шаг повторяется.
func (f *fakeDisk) failNext(endpoint string, codes ...int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queue[endpoint] = append(f.queue[endpoint], codes...)
}

// overrideUploadHref заменяет адрес, который ручка загрузки отдаёт в href: так проверяется путь
// «ссылка протухла или загрузчик недоступен».
func (f *fakeDisk) overrideUploadHref(href string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.uploadHref = href
}

// remove «удаляет» объект из эмулятора: ветка Exists=false проверяется без второго сервера.
func (f *fakeDisk) remove(path string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objects, path)
}

func (f *fakeDisk) state() fakeState {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := fakeState{
		dirCodes: map[string][]int{},
		objects:  map[string][]byte{},
		calls:    map[string]int{},
	}
	for path, d := range f.dirs {
		s.dirCodes[path] = append([]int(nil), d.codes...)
	}
	for path, body := range f.objects {
		s.objects[path] = append([]byte(nil), body...)
	}
	for endpoint, n := range f.calls {
		s.calls[endpoint] = n
	}
	s.apiAuth = append([]string(nil), f.apiAuth...)
	s.hrefAuth = append([]string(nil), f.hrefAuth...)
	s.uploadPaths = append([]string(nil), f.uploadPaths...)
	s.downloadPaths = append([]string(nil), f.downloadPaths...)
	s.existsPaths = append([]string(nil), f.existsPaths...)
	s.overwrites = append([]string(nil), f.overwrites...)
	return s
}

func (f *fakeDisk) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	switch r.URL.Path {
	case "/resources":
		if r.Method == http.MethodPut {
			f.handleMkdir(w, r)
			return
		}
		f.handleExists(w, r)
	case "/resources/upload":
		f.handleHref(w, r, fakeUpload, f.server.URL+fakeUploadTarget, http.MethodPut)
	case "/resources/download":
		f.handleHref(w, r, fakeDownload, f.server.URL+fakeDownloadTarget, http.MethodGet)
	case fakeUploadTarget:
		f.handleBody(w, r)
	case fakeDownloadTarget:
		f.handleData(w, r)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// handleMkdir отвечает на создание каталога: 201 в первый раз и 409 на повтор. 409 здесь — не
// сбой, а «уже существует»; второй Put в тот же каталог обязан пройти.
func (f *fakeDisk) handleMkdir(w http.ResponseWriter, r *http.Request) {
	f.note(r, fakeMkdir)
	path := r.URL.Query().Get("path")
	d := f.dir(path)
	if code, ok := f.take(fakeMkdir); ok {
		d.codes = append(d.codes, code)
		w.WriteHeader(code)
		return
	}
	code := http.StatusCreated
	if d.exists {
		code = http.StatusConflict
	}
	d.exists = true
	d.codes = append(d.codes, code)
	w.WriteHeader(code)
}

// handleHref выдаёт подписанную ссылку: на загрузку — только после проверки, что вызывающий
// передал overwrite=true (иначе Диск откажется перезаписывать объект), на скачивание — только на
// существующий объект (иначе 404, как у настоящего API).
func (f *fakeDisk) handleHref(w http.ResponseWriter, r *http.Request, endpoint, target, method string) {
	f.note(r, endpoint)
	path := r.URL.Query().Get("path")
	switch endpoint {
	case fakeUpload:
		f.uploadPaths = append(f.uploadPaths, path)
		f.overwrites = append(f.overwrites, r.URL.Query().Get("overwrite"))
		f.uploadPath = path
	case fakeDownload:
		f.downloadPaths = append(f.downloadPaths, path)
		f.downloadPath = path
	}
	if code, ok := f.take(endpoint); ok {
		w.WriteHeader(code)
		return
	}
	if endpoint == fakeDownload {
		if _, ok := f.objects[path]; !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
	}
	if endpoint == fakeUpload && f.uploadHref != "" {
		target = f.uploadHref
	}
	body, _ := json.Marshal(map[string]string{"href": target, "method": method})
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

// handleBody принимает тело на ссылку загрузки и кладёт его по пути, который назвала ручка ссылки:
// у самой ссылки path нет — она и есть право на запись объекта.
func (f *fakeDisk) handleBody(w http.ResponseWriter, r *http.Request) {
	f.note(r, fakeBody)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if code, ok := f.take(fakeBody); ok {
		w.WriteHeader(code)
		return
	}
	f.objects[f.uploadPath] = body
	w.WriteHeader(http.StatusCreated)
}

// handleData отдаёт байты объекта по ссылке скачивания.
func (f *fakeDisk) handleData(w http.ResponseWriter, r *http.Request) {
	f.note(r, fakeData)
	if code, ok := f.take(fakeData); ok {
		w.WriteHeader(code)
		return
	}
	body, ok := f.objects[f.downloadPath]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	_, _ = w.Write(body)
}

func (f *fakeDisk) handleExists(w http.ResponseWriter, r *http.Request) {
	f.note(r, fakeExists)
	path := r.URL.Query().Get("path")
	f.existsPaths = append(f.existsPaths, path)
	if code, ok := f.take(fakeExists); ok {
		w.WriteHeader(code)
		return
	}
	if _, ok := f.objects[path]; ok {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

// note считает запросы к ручке и раскладывает Authorization по журналам: у запросов к API он
// обязан быть, у запросов по подписанной ссылке — нет. Вызывается под f.mu.
func (f *fakeDisk) note(r *http.Request, endpoint string) {
	f.calls[endpoint]++
	if strings.HasPrefix(r.URL.Path, "/resources") {
		f.apiAuth = append(f.apiAuth, r.Header.Get("Authorization"))
		return
	}
	f.hrefAuth = append(f.hrefAuth, r.Header.Get("Authorization"))
}

// take снимает с ручки очередной подменённый статус. Вызывается под f.mu.
func (f *fakeDisk) take(endpoint string) (int, bool) {
	queue := f.queue[endpoint]
	if len(queue) == 0 {
		return 0, false
	}
	f.queue[endpoint] = queue[1:]
	return queue[0], true
}

// dir возвращает запись каталога, заводя её при первом обращении. Вызывается под f.mu.
func (f *fakeDisk) dir(path string) *fakeDir {
	d, ok := f.dirs[path]
	if !ok {
		d = &fakeDir{}
		f.dirs[path] = d
	}
	return d
}

// assertYandexAuth: к API Диска запросы идут с токеном, а по подписанной ссылке — без него. На href
// токен не нужен (ссылка уже подписана), и лишний заголовок унёс бы секрет на хост загрузчика,
// который токен не проверяет.
func assertYandexAuth(t *testing.T, s fakeState) {
	t.Helper()
	for i, got := range s.apiAuth {
		if want := "OAuth " + fakeToken; got != want {
			t.Errorf("Authorization запроса к API #%d = %q, ожидалось %q", i, got, want)
		}
	}
	for i, got := range s.hrefAuth {
		if got != "" {
			t.Errorf("Authorization запроса по подписанной ссылке #%d = %q, ожидалось пусто", i, got)
		}
	}
}

// Round-trip Put → Get → Exists на живом HTTP: эмулятор отвечает теми же ручками и кодами, что
// Диск, и пинит то, чем архив пользуется, — путь в query, создание каталогов, тело на подписанной
// ссылке. Ошибка тут означала бы не «не тот URL», а нерабочую архивацию: версия не сохранилась бы
// или не прочиталась.
func TestYandexPutGetExists(t *testing.T) {
	f := newFakeDisk(t)
	store := newFakeStore(f)
	ctx := context.Background()
	key := Key("cbr_m2", "monetary_agg_SA.xlsx", fp, zeroSum)
	body := []byte("байты выгрузки cbr_m2")
	diskPath := fakeRoot + "/" + key
	sourceDir := fakeRoot + "/cbr_m2"

	if store.Name() != "yandex" {
		t.Errorf("Name() = %q, ожидалось %q", store.Name(), "yandex")
	}
	if err := store.Put(ctx, key, body); err != nil {
		t.Fatalf("Put: %v", err)
	}

	s := f.state()
	// Каталоги: корень архива и папка источника — по одному 201 на каждый. Повтор (409) проверяет
	// TestYandexFoldersIdempotent.
	for dir, want := range map[string][]int{
		fakeRoot:  {http.StatusCreated},
		sourceDir: {http.StatusCreated},
	} {
		if got := s.dirCodes[dir]; !reflect.DeepEqual(got, want) {
			t.Errorf("коды создания каталога %s = %v, ожидались %v", dir, got, want)
		}
	}
	if got, want := s.uploadPaths, []string{diskPath}; !reflect.DeepEqual(got, want) {
		t.Errorf("path запроса ссылки на загрузку = %q, ожидался %q", got, want)
	}
	if got, want := s.overwrites, []string{"true"}; !reflect.DeepEqual(got, want) {
		t.Errorf("overwrite запроса ссылки на загрузку = %q, ожидался %q", got, want)
	}
	if got := s.objects[diskPath]; !bytes.Equal(got, body) {
		t.Errorf("объект на Диске = %q, ожидались исходные байты %q", got, body)
	}
	assertYandexAuth(t, s)

	got, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("Get() = %q, ожидались исходные байты %q", got, body)
	}
	if got, want := f.state().downloadPaths, []string{diskPath}; !reflect.DeepEqual(got, want) {
		t.Errorf("path запроса ссылки на скачивание = %q, ожидался %q", got, want)
	}

	exists, err := store.Exists(ctx, key)
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !exists {
		t.Error("Exists после Put = false, ожидалось true")
	}
	if got, want := f.state().existsPaths, []string{diskPath}; !reflect.DeepEqual(got, want) {
		t.Errorf("path запроса наличия = %q, ожидался %q", got, want)
	}

	f.remove(diskPath)
	exists, err = store.Exists(ctx, key)
	if err != nil {
		t.Fatalf("Exists удалённого объекта: %v", err)
	}
	if exists {
		t.Error("Exists удалённого объекта = true, ожидалось false")
	}
	// Get удалённого объекта — ошибка, а не пустые байты: пустая версия в архиве — молчаливая
	// потеря, ради которой архив и заводился (спека §3.1).
	if _, err = store.Get(ctx, key); err == nil {
		t.Error("Get удалённого объекта вернул байты без ошибки")
	}
	assertYandexAuth(t, f.state())
}

// Второй Put в тот же каталог источника: эмулятор отвечает 409 («уже существует»), и это не сбой.
// Без этого архив работал бы ровно один раз на источник: вторая версия файла не уехала бы на Диск,
// а Exists на no-op-прогоне говорил бы «в архиве нет» — то есть полный импорт на каждом прогоне.
func TestYandexFoldersIdempotent(t *testing.T) {
	f := newFakeDisk(t)
	store := newFakeStore(f)
	ctx := context.Background()
	first := Key("cbr_m2", "monetary_agg_SA.xlsx", fp, zeroSum)
	second := Key("cbr_m2", "monetary_agg_SA_other.xlsx", fp, zeroSum)

	for _, key := range []string{first, second} {
		if err := store.Put(ctx, key, []byte("байты "+key)); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}

	s := f.state()
	if got, want := s.dirCodes[fakeRoot+"/cbr_m2"], []int{http.StatusCreated, http.StatusConflict}; !reflect.DeepEqual(got, want) {
		t.Errorf("коды создания папки источника = %v, ожидались %v", got, want)
	}
	for _, key := range []string{first, second} {
		if _, ok := s.objects[fakeRoot+"/"+key]; !ok {
			t.Errorf("объект %s не загружен на Диск", key)
		}
	}
}

// 429 от ручки ссылки — «сбавь темп», а не отказ: шаг повторяется, и Put доходит. Пауза повтора
// подменяется миллисекундой — тест проверяет повтор, а не терпение.
func TestYandexRetriesOn429(t *testing.T) {
	f := newFakeDisk(t)
	f.failNext(fakeUpload, http.StatusTooManyRequests)
	old := retrySleeps
	retrySleeps = []time.Duration{time.Millisecond}
	t.Cleanup(func() { retrySleeps = old })

	store := newFakeStore(f)
	key := Key("cbr_m2", "monetary_agg_SA.xlsx", fp, zeroSum)
	if err := store.Put(context.Background(), key, []byte("байты")); err != nil {
		t.Fatalf("Put с повтором на 429: %v", err)
	}

	s := f.state()
	if got, want := s.calls[fakeUpload], 2; got != want {
		t.Errorf("запросов к resources/upload = %d, ожидалось %d (первый 429, второй успешный)", got, want)
	}
	if _, ok := s.objects[fakeRoot+"/"+key]; !ok {
		t.Error("после повтора объект не загружен на Диск")
	}
}

// 401 — токен не принят: ошибка обёртывает ErrAuth (дежурный обновляет токен), и в её тексте нет
// значения токена: лог читают люди, а секрет в логе — утечка (правило 1 AGENTS.md).
func TestYandexAuthError(t *testing.T) {
	f := newFakeDisk(t)
	f.failNext(fakeUpload, http.StatusUnauthorized)

	store := newFakeStore(f)
	err := store.Put(context.Background(), Key("cbr_m2", "monetary_agg_SA.xlsx", fp, zeroSum), []byte("байты"))
	if !errors.Is(err, ErrAuth) {
		t.Errorf("Put при 401: %v, ожидалась обёртка ErrAuth", err)
	}
	if err != nil && strings.Contains(err.Error(), fakeToken) {
		t.Errorf("в тексте ошибки %q есть значение токена", err)
	}
}

// Квота Диска — второй различимый случай (спека §3.2): все четыре кода сводятся к ErrQuota, потому
// что дежурному важно отличить «место» от «токена», а не разобрать код. 413 и 423 приходят от
// API-прокси Диска на слишком большой файл и на занятый ресурс.
func TestYandexQuotaError(t *testing.T) {
	for _, status := range []int{
		http.StatusForbidden,
		http.StatusRequestEntityTooLarge,
		http.StatusLocked,
		http.StatusInsufficientStorage,
	} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			f := newFakeDisk(t)
			f.failNext(fakeUpload, status)

			store := newFakeStore(f)
			err := store.Put(context.Background(), Key("cbr_m2", "monetary_agg_SA.xlsx", fp, zeroSum), []byte("байты"))
			if !errors.Is(err, ErrQuota) {
				t.Errorf("Put при %d: %v, ожидалась обёртка ErrQuota", status, err)
			}
		})
	}
}

// Прочий код ответа — обычная ошибка с номером статуса и путём на Диске. Путь, а не URL запроса:
// чинится корень архива (ARCHIVE_YANDEX_DIR), и подписанной ссылки в тексте быть не должно.
func TestYandexUnexpectedStatus(t *testing.T) {
	f := newFakeDisk(t)
	f.failNext(fakeExists, http.StatusInternalServerError)

	store := newFakeStore(f)
	key := Key("cbr_m2", "monetary_agg_SA.xlsx", fp, zeroSum)
	_, err := store.Exists(context.Background(), key)
	if err == nil {
		t.Fatal("Exists при 500 вернул ответ без ошибки")
	}
	for _, want := range []string{"статус 500", fakeRoot + "/" + key} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ошибка %q не называет %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "/resources") {
		t.Errorf("в текст ошибки попал URL запроса API: %q", err)
	}
}

// Подписанная ссылка — временный секрет: её утечка в лог равна утечке доступа к файлу. Сетевой сбой
// и разбор URL http.Client оборачивает в *url.Error, чей текст содержит адрес запроса, поэтому
// ошибка транспорта разворачивается до причины; проверяется, что в тексте нет ни ссылки, ни её
// подписи, ни токена.
func TestYandexHrefNotInError(t *testing.T) {
	const signature = "SIGNED_QUERY_SECRET"
	// Ссылка ведёт на уже закрытый сервер: адрес есть, отвечать по нему некому — ровно то, что
	// видит клиент, когда подписанная ссылка протухла или загрузчик недоступен.
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	deadURL := dead.URL
	dead.Close()

	for _, c := range []struct{ name, href string }{
		{"загрузчик недоступен", deadURL + fakeUploadTarget + "?signature=" + signature},
		{"ссылка не парсится", "://bad" + fakeUploadTarget + "?signature=" + signature},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newFakeDisk(t)
			f.overrideUploadHref(c.href)

			store := newFakeStore(f)
			err := store.Put(context.Background(), Key("cbr_m2", "x.xlsx", fp, zeroSum), []byte("байты"))
			if err == nil {
				t.Fatal("Put по нерабочей подписанной ссылке вернул успех")
			}
			if !strings.Contains(err.Error(), opUpload) {
				t.Errorf("ошибка %q не называет шаг %q", err, opUpload)
			}
			for _, leak := range []string{c.href, fakeUploadTarget, signature, fakeToken} {
				if strings.Contains(err.Error(), leak) {
					t.Errorf("в тексте ошибки %q есть %q", err, leak)
				}
			}
		})
	}
}

// Дедлайн операции: util.HttpClient создан без Timeout, поэтому без своего дедлайна зависший Диск
// держал бы прогон вечно — а архив объявлен опцией, которая не останавливает данные (спека §3.1).
// opTimeout подменяется коротким: проверяется применение дедлайна, а не две минуты терпения.
func TestYandexOperationDeadline(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		select {
		case <-release:
		case <-time.After(5 * time.Second): // предохранитель: без дедлайна тест не должен висеть
		}
	}))
	defer server.Close()
	// close раньше Close (defer'ы LIFO): сервер ждёт завершения хендлера, и освобождать его надо первым.
	defer close(release)

	old := opTimeout
	opTimeout = 50 * time.Millisecond
	t.Cleanup(func() { opTimeout = old })

	store := NewYandex(fakeToken, fakeRoot, server.URL, &http.Client{})
	started := time.Now()
	err := store.Put(context.Background(), Key("cbr_m2", "x.xlsx", fp, zeroSum), []byte("байты"))
	if err == nil {
		t.Fatal("Put к зависшему Диску вернул успех")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Errorf("Put вернулся через %v, ожидалось около %v: дедлайн операции не применён", elapsed, opTimeout)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Put по дедлайну: %v, ожидалась причина context.DeadlineExceeded", err)
	}
}

// Пустой baseURL — прод-адрес API, а не пустая строка: иначе первый же запрос ушёл бы в никуда.
// Проверяется на поле, а не запросом: сети в тестах нет.
func TestYandexDefaultBaseURL(t *testing.T) {
	store := NewYandex(fakeToken, fakeRoot, "", &http.Client{})
	if store.baseURL != defaultYandexAPI {
		t.Errorf("baseURL = %q, ожидался %q", store.baseURL, defaultYandexAPI)
	}
}

// Путь на Диске состоит из корня архива и ключа, а каталоги перебираются от корня до папки
// источника: ручка ресурсов заводит одну папку за вызов. Вложенный корень (dev и приёмка задают
// ARCHIVE_YANDEX_DIR по-разному) потому и разбирается по уровням — пропущенный уровень вернул бы
// «родитель не найден», и объект не уехал бы на Диск.
func TestYandexDirsToCreate(t *testing.T) {
	for _, c := range []struct {
		name, diskPath string
		want           []string
	}{
		{
			"корень и папка источника",
			fakeRoot + "/cbr_m2/8f3c_x.xlsx",
			[]string{"disk:/rosstat-archive", "disk:/rosstat-archive/cbr_m2"},
		},
		{
			"вложенный корень",
			"disk:/backup/archive/cbr_m2/8f3c_x.xlsx",
			[]string{"disk:/backup", "disk:/backup/archive", "disk:/backup/archive/cbr_m2"},
		},
		{
			"объект в корне",
			fakeRoot + "/x.xlsx",
			[]string{"disk:/rosstat-archive"},
		},
		{
			"путь без схемы",
			"rosstat-archive/cbr_m2/x.xlsx",
			[]string{"rosstat-archive", "rosstat-archive/cbr_m2"},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := dirsToCreate(c.diskPath); !reflect.DeepEqual(got, c.want) {
				t.Errorf("dirsToCreate(%q) = %q, ожидалось %q", c.diskPath, got, c.want)
			}
		})
	}
}
