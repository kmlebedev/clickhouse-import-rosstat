//go:build mpeval

package cbr

// Живой harness сравнения LLM-моделей разметки каналов cbr_mp_summary (спека §3.4–3.5).
// Запускается только целью `make mp-llm-eval` и не входит в make all: ходит в сеть к
// провайдерам и ЦБ, пишет в ClickHouse и стоит денег. Агрегаты и их unit-тесты — в
// mp_summary_llm_eval_test.go (без build tag), здесь только чтение/запись и отчёт.

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
)

// mpLLMEvalEnv — env-контракт harness (спека §3.4). Значения ключей провайдеров сюда не
// входят: профили читает mpLLMProfilesFromEnv (только os.Getenv), и в отчёт/логи они не
// попадают.
const (
	mpEvalFlagEnv      = "MP_LLM_EVAL"
	mpEvalDocsEnv      = "MP_LLM_EVAL_DOCS"
	mpEvalWriteEnv     = "MP_LLM_EVAL_WRITE"
	mpEvalForceEnv     = "MP_LLM_EVAL_FORCE"
	mpEvalResumeEnv    = "MP_LLM_EVAL_RESUME"
	mpEvalOutEnv       = "MP_LLM_EVAL_OUT"
	mpEvalTimeoutEnv   = "MP_LLM_EVAL_TIMEOUT"
	mpEvalMaxTokensEnv = "MP_LLM_EVAL_MAX_TOKENS"
	mpEvalDelayEnv     = "MP_LLM_EVAL_DELAY"
	// mpEvalPrefillEnv — совместимость с моделями, которые тратят весь max_tokens на
	// рассуждение в text-блоке (GLM): последнее сообщение ассистента «{» заставляет их
	// продолжать JSON. Прод-путь по умолчанию prefill не использует.
	mpEvalPrefillEnv = "MP_LLM_EVAL_PREFILL"
	// mpEvalThinkingEnv — управление Anthropic-полем thinking: default (не отправлять) или
	// disabled. GLM/MiMo по умолчанию тратят весь max_tokens на reasoning и не доходят до JSON.
	mpEvalThinkingEnv = "MP_LLM_EVAL_THINKING"
	// mpEvalTransportEnv — транспорт harness: anthropic (продовый SDK) или openai
	// (OpenAI-совместимый JSON-режим; нужен GLM/MiMo, которые на Anthropic-эндпоинте
	// уходят в reasoning и не доходят до JSON).
	mpEvalTransportEnv = "MP_LLM_EVAL_TRANSPORT"
	// mpEvalReasoningEnv — OpenAI-совместимый reasoning_effort: none|low|medium|high; пусто —
	// поле не отправляется. Для GLM none снимает длинный анализ, который съедает max_tokens
	// до начала JSON (проверено на гейтвее Joingonka).
	mpEvalReasoningEnv = "MP_LLM_EVAL_REASONING"
	// mpEvalConciseEnv — harness-режим concise: не более одной цитаты на канал и короткая
	// note; нужен для повторной разметки GLM-документов, которые не влезли в 8192 токена.
	mpEvalConciseEnv = "MP_LLM_EVAL_CONCISE"
	// mpEvalReportOnlyEnv — сгенерировать отчёт по существующему stats.jsonl без вызовов
	// моделей: нужно для сведения прогонов, сделанных с разными транспортами/лимитами.
	mpEvalReportOnlyEnv = "MP_LLM_EVAL_REPORT_ONLY"

	// mpEvalBaselineModel — модель, уже лежащая в ClickHouse: согласие с ней — одна из
	// метрик качества (спека §3.5). Имя модели не секрет: оно же колонка model витрины.
	mpEvalBaselineModel = "deepseek-chat"

	// mpEvalBackfillDocs — размер полного ряда резюме на 2026-10-17 (22 заседания): по нему
	// отчёт пересчитывает стоимость одного документа в стоимость бэкфилла.
	mpEvalBackfillDocs = 22

	// mpEvalDefaultOutDir — каталог артефактов по умолчанию относительно корня репозитория
	// (go test запускает пакет из cbr/, поэтому поднимаемся на уровень выше).
	mpEvalDefaultOutDir = "mp-llm-eval"
)

// mpEvalNotePublishedDates — даты публикации трёх резюме для ручной оценки заметок
// (спека §3.5). Это даты публикации на сайте ЦБ (имена фикстур 27022024/06112024/23092026),
// а не даты заседания: витрина ключуется meeting_date, поэтому дата публикации
// переводится в дату заседания через v_cbr_mp_summary.published.
var mpEvalNotePublishedDates = []string{"2024-02-27", "2024-11-06", "2026-09-23"}

// mpEvalDoc — разобранный документ резюме: дата заседания (ключ строк и фильтра), адрес
// страницы и результат parseMPSummary.
type mpEvalDoc struct {
	Date    time.Time
	DateKey string
	URL     string
	Parsed  mpSummaryDoc
}

// mpEvalJob — одна пара (документ, профиль): один вызов модели в прогоне.
type mpEvalJob struct {
	Doc     mpEvalDoc
	Profile mpLLMProfile
}

// mpEvalNoteKey — ключ заметки в notes-sample.md: дата заседания, канал и модель.
type mpEvalNoteKey struct {
	MeetingDate string
	Channel     string
	Model       string
}

// mpEvalInterJaccard — межмодельное согласие одной пары моделей.
type mpEvalInterJaccard struct {
	A      string
	B      string
	Result mpEvalJaccardResult
}

// mpEvalPrice — тариф профиля, USD за 1М токенов. Set=false — тариф не задан (в отчёте
// «н/д»), а не «нулевая цена».
type mpEvalPrice struct {
	In  float64
	Out float64
	Set bool
}

// mpEvalReport — всё, что нужно отчёту: условия прогона, журнал записей и посчитанные по
// витрине Jaccard/цены. Живёт отдельным типом, чтобы запись отчёта не тянула за собой
// соединение с ClickHouse.
type mpEvalReport struct {
	Started    time.Time
	Profiles   []mpLLMProfile
	Docs       []mpEvalDoc
	DocsFilter string
	Timeout    time.Duration
	MaxTokens  int64
	Delay      time.Duration
	Write      bool
	Force      bool
	Resume     bool
	Prefill    bool
	Thinking   string
	Transport  string
	Reasoning  string
	Concise    bool
	ReportOnly bool
	OutDir     string
	Records    []mpEvalRecord
	Models     []string
	Prices     map[string]mpEvalPrice
	Baseline   map[string]mpEvalJaccardResult
	Regex      map[string]mpEvalJaccardResult
	Inter      []mpEvalInterJaccard
}

// TestMPLLMEvalLive — живой прогон сравнения моделей. Без MP_LLM_EVAL=1 пропускается до
// любых чтений env/сети: CI и `go test -tags mpeval` без флага остаются зелёными.
func TestMPLLMEvalLive(t *testing.T) {
	if os.Getenv(mpEvalFlagEnv) != "1" {
		t.Skip("MP_LLM_EVAL != 1")
	}
	started := time.Now().UTC()

	// Профили — только явным списком: harness без него бессмыслен (default молча взял бы
	// продовую модель и сравнил её с собой). Пустой список — ошибка теста, не skip.
	if strings.TrimSpace(os.Getenv(mpLLMModelsEnv)) == "" {
		t.Fatalf("%s не задан: harness требует явный список профилей (например, default,glm,mimo)", mpLLMModelsEnv)
	}
	profiles := mpLLMProfilesFromEnv()
	if len(profiles) == 0 {
		t.Fatalf("%s: ни одного полного LLM-профиля (для default нужны %s/%s/%s, для остальных — <PROFILE>_LLM_*)",
			mpLLMModelsEnv, mpLLMBaseURLEnv, mpLLMAPIKeyEnv, mpLLMModelEnv)
	}

	timeout := mpEvalEnvDuration(t, mpEvalTimeoutEnv, 180*time.Second)
	maxTokens := mpEvalEnvInt64(t, mpEvalMaxTokensEnv, 8192)
	delay := mpEvalEnvDelay(t, mpEvalDelayEnv, time.Second)
	write := mpEvalEnvBool(t, mpEvalWriteEnv, true)
	force := mpEvalEnvBool(t, mpEvalForceEnv, false)
	resume := mpEvalEnvBool(t, mpEvalResumeEnv, true)
	prefill := mpEvalEnvBool(t, mpEvalPrefillEnv, false)
	thinking := mpEvalEnvThinking(t)
	transport := mpEvalEnvTransport(t)
	reasoning := mpEvalEnvReasoning(t)
	concise := mpEvalEnvBool(t, mpEvalConciseEnv, false)
	reportOnly := mpEvalEnvBool(t, mpEvalReportOnlyEnv, false)
	// Флаги относятся к разным транспортам; молчаливое игнорирование сделало бы отчёт
	// неверным (в нём флаг выглядел бы применённым).
	if transport == "openai" && (prefill || thinking == "disabled") {
		t.Fatalf("%s=openai несовместим с %s/%s: prefill и thinking относятся к Anthropic-транспорту",
			mpEvalTransportEnv, mpEvalPrefillEnv, mpEvalThinkingEnv)
	}
	if transport == "anthropic" && (reasoning != "" || concise) {
		t.Fatalf("%s=anthropic несовместим с %s/%s: reasoning и concise относятся к OpenAI-транспорту",
			mpEvalTransportEnv, mpEvalReasoningEnv, mpEvalConciseEnv)
	}
	docsFilter, docsFilterRaw := mpEvalEnvDates(t)
	prices := mpEvalProfilePrices(t, profiles)

	// mpLLMTimeout/mpLLMMaxTokens/mpLLMPrefillJSON/mpLLMThinkingDisabled/mpLLMTransport/
	// mpLLMReasoningEffort/mpLLMConcise — переменные, чтобы harness подменил лимиты и режим
	// запроса только на этот прогон. Продовый дефолт возвращается в t.Cleanup: с -tags mpeval
	// в том же процессе идут и обычные тесты пакета, которые пинят max_tokens=8192 и
	// отсутствие thinking/prefill/reasoning/concise в запросе.
	oldTimeout, oldMaxTokens := mpLLMTimeout, mpLLMMaxTokens
	oldPrefill, oldThinking, oldTransport := mpLLMPrefillJSON, mpLLMThinkingDisabled, mpLLMTransport
	oldReasoning, oldConcise := mpLLMReasoningEffort, mpLLMConcise
	mpLLMTimeout = timeout
	mpLLMMaxTokens = maxTokens
	mpLLMPrefillJSON = prefill
	mpLLMThinkingDisabled = thinking == "disabled"
	mpLLMTransport = transport
	mpLLMReasoningEffort = reasoning
	mpLLMConcise = concise
	t.Cleanup(func() {
		mpLLMTimeout = oldTimeout
		mpLLMMaxTokens = oldMaxTokens
		mpLLMPrefillJSON = oldPrefill
		mpLLMThinkingDisabled = oldThinking
		mpLLMTransport = oldTransport
		mpLLMReasoningEffort = oldReasoning
		mpLLMConcise = oldConcise
	})

	outDir := strings.TrimSpace(os.Getenv(mpEvalOutEnv))
	if outDir == "" {
		outDir = filepath.Join("..", "build", mpEvalDefaultOutDir)
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatalf("%s: каталог артефактов не создан: %v", outDir, err)
	}
	statsPath := filepath.Join(outDir, "stats.jsonl")

	ctx := context.Background()
	conn := mpEvalOpenClickHouse(t, ctx)
	defer func() { _ = conn.Close() }()

	docs, err := mpEvalLoadDocs(ctx, conn, docsFilter)
	if err != nil {
		t.Fatalf("документы: %v", err)
	}
	mpEvalFetchDocs(t, docs)
	runDates := make(map[string]bool, len(docs))
	for _, doc := range docs {
		runDates[doc.DateKey] = true
	}

	var records []mpEvalRecord
	if resume {
		records, err = mpEvalReadStats(statsPath)
		if err != nil {
			t.Fatalf("%v", err)
		}
	}
	flags := os.O_CREATE | os.O_WRONLY | os.O_APPEND
	if !resume {
		// RESUME=0 — новый прогон: старый журнал не должен подмешивать пары, которые в
		// этом прогоне пропущены по tagged/фильтру.
		flags |= os.O_TRUNC
	}
	statsFile, err := os.OpenFile(statsPath, flags, 0o644)
	if err != nil {
		t.Fatalf("%s: журнал не открыт: %v", statsPath, err)
	}
	defer func() { _ = statsFile.Close() }()

	done := make(map[mpEvalPair]bool, len(records))
	for _, rec := range records {
		// Пропускается только успешная пара: запись со сбоем повторяется (спека §3.4).
		if rec.Error == "" {
			done[mpEvalPairOf(rec)] = true
		}
	}

	// Набор «размечено» по каждому профилю: ровно та же проверка, что у импортёра
	// (model, prompt_version). Дубликаты моделей отсекает mpLLMProfilesFromEnv.
	tagged := make(map[string]map[time.Time]bool, len(profiles))
	for _, profile := range profiles {
		set, taggedErr := mpLLMTagged(ctx, conn, profile.Model, mpPromptVersion)
		if taggedErr != nil {
			t.Fatalf("профиль %s: размеченные документы не прочитаны: %v", profile.Name, taggedErr)
		}
		tagged[profile.Model] = set
	}

	var jobs []mpEvalJob
	for _, doc := range docs {
		for _, profile := range profiles {
			// RESUME отсеивает успешные пары журнала; FORCE+WRITE — уже размеченные в БД.
			// При WRITE=0 FORCE не нужен: в БД прогон не пишет, dry-run обязан пройти все
			// пары (иначе baseline после первого прогона молча пропустился бы).
			if resume && done[mpEvalPair{Model: profile.Model, MeetingDate: doc.DateKey}] {
				continue
			}
			if write && !force && tagged[profile.Model][doc.Date] {
				continue
			}
			jobs = append(jobs, mpEvalJob{Doc: doc, Profile: profile})
		}
	}
	if reportOnly {
		t.Logf("report-only: вызовы моделей пропущены, отчёт по %d записям stats.jsonl", len(records))
		jobs = nil
	}
	t.Logf("harness: документов %d, профилей %d, вызовов %d (write=%t force=%t resume=%t report_only=%t prefill=%t thinking=%s transport=%s reasoning=%s concise=%t, timeout=%s, max_tokens=%d, delay=%s)",
		len(docs), len(profiles), len(jobs), write, force, resume, reportOnly, prefill, thinking, transport, reasoning, concise, timeout, maxTokens, delay)

	for i, job := range jobs {
		rec := mpEvalRunJob(t, ctx, conn, job, write)
		line, marshalErr := json.Marshal(rec)
		if marshalErr != nil {
			t.Fatalf("stats.jsonl: запись не собрана: %v", marshalErr)
		}
		if _, writeErr := statsFile.Write(append(line, '\n')); writeErr != nil {
			t.Fatalf("%s: запись не дописана: %v", statsPath, writeErr)
		}
		records = append(records, rec)
		if rec.Error == "" {
			done[mpEvalPairOf(rec)] = true
		}
		t.Logf("LLM %s / %s: %.1f с, попыток %d, цитат %d/%d (отброшено %d), заметок %d, строк %d, ошибка %q",
			rec.Model, rec.MeetingDate, float64(rec.DurationMS)/1000, rec.Attempts,
			rec.QuotesKept, rec.QuotesTotal, rec.QuotesDropped, rec.Notes, rec.Rows, rec.Error)
		// Пауза между вызовами, но не после последнего: rate limit провайдеров и
		// бесполезное ожидание в конце прогона.
		if i < len(jobs)-1 && delay > 0 {
			time.Sleep(delay)
		}
	}

	models := mpEvalModelNames(records)
	baselineRows, err := mpEvalViewLLMQuotes(ctx, conn, mpEvalBaselineModel, runDates)
	if err != nil {
		t.Fatalf("baseline %s: %v", mpEvalBaselineModel, err)
	}
	regexRows, err := mpEvalViewRegexQuotes(ctx, conn, runDates)
	if err != nil {
		t.Fatalf("regex-слой: %v", err)
	}
	baselineSets := mpEvalQuoteSets(baselineRows)
	regexSets := mpEvalQuoteSets(regexRows)
	modelSets := make(map[string]map[mpEvalDocChannel]map[string]bool, len(models))
	baselineJaccard := make(map[string]mpEvalJaccardResult, len(models))
	regexJaccard := make(map[string]mpEvalJaccardResult, len(models))
	for _, model := range models {
		rows, rowsErr := mpEvalViewLLMQuotes(ctx, conn, model, runDates)
		if rowsErr != nil {
			t.Fatalf("модель %s: цитаты витрины не прочитаны: %v", model, rowsErr)
		}
		modelSets[model] = mpEvalQuoteSets(rows)
		baselineJaccard[model] = mpEvalJaccard(modelSets[model], baselineSets, false)
		regexJaccard[model] = mpEvalJaccard(modelSets[model], regexSets, true)
	}

	report := mpEvalReport{
		Started:    started,
		Profiles:   profiles,
		Docs:       docs,
		DocsFilter: docsFilterRaw,
		Timeout:    timeout,
		MaxTokens:  maxTokens,
		Delay:      delay,
		Write:      write,
		Force:      force,
		Resume:     resume,
		Prefill:    prefill,
		Thinking:   thinking,
		Transport:  transport,
		Reasoning:  reasoning,
		Concise:    concise,
		ReportOnly: reportOnly,
		OutDir:     outDir,
		Records:    records,
		Models:     models,
		Prices:     prices,
		Baseline:   baselineJaccard,
		Regex:      regexJaccard,
		Inter:      mpEvalInterModelJaccard(models, modelSets),
	}

	mpEvalWriteSummaryCSV(t, filepath.Join(outDir, "summary.csv"), records)
	mpEvalWriteReport(t, filepath.Join(outDir, "report.md"), report)
	profileModels := make([]string, 0, len(profiles))
	for _, profile := range profiles {
		profileModels = append(profileModels, profile.Model)
	}
	mpEvalWriteNotesSample(t, ctx, conn, filepath.Join(outDir, "notes-sample.md"), docs, profileModels)

	t.Logf("артефакты: %s (stats.jsonl, summary.csv, report.md, notes-sample.md)", outDir)
}

// mpEvalOpenClickHouse — подключение harness: DSN из CLICKHOUSE_URL, Ping и DDL таблицы
// каналов (спека §3.4 п. 1). DSN в текст ошибки не попадает: в нём может быть пароль.
func mpEvalOpenClickHouse(t *testing.T, ctx context.Context) driver.Conn {
	t.Helper()
	options, err := clickhouse.ParseDSN(os.Getenv("CLICKHOUSE_URL"))
	if err != nil {
		t.Fatalf("CLICKHOUSE_URL: DSN не разобран")
	}
	conn, err := clickhouse.Open(options)
	if err != nil {
		t.Fatalf("ClickHouse: Open: %v", err)
	}
	if err = conn.Ping(ctx); err != nil {
		t.Fatalf("ClickHouse: Ping: %v", err)
	}
	if err = conn.Exec(ctx, mpSummaryChannelDdl); err != nil {
		t.Fatalf("ClickHouse: DDL %s: %v", mpSummaryChannelTable, err)
	}
	return conn
}

// mpEvalLoadDocs — шапки резюме (meeting_date, url) и необязательный фильтр MP_LLM_EVAL_DOCS.
// Дата нормализуется mpDay — тем же приведением, что у импортёра: драйвер отдаёт Date в
// зоне сессии, и без нормализации фильтр и ключи stats не совпали бы. Запрошенная дата,
// которой нет в cbr_mp_summary, — ошибка теста, а не молчаливый пустой прогон.
func mpEvalLoadDocs(ctx context.Context, conn driver.Conn, filter map[string]bool) ([]mpEvalDoc, error) {
	rows, err := conn.Query(ctx, "SELECT meeting_date, url FROM "+mpSummaryTable+" FINAL ORDER BY meeting_date")
	if err != nil {
		return nil, fmt.Errorf("%s: документы не прочитаны: %w", mpSummaryTable, err)
	}
	defer func() { _ = rows.Close() }()

	var docs []mpEvalDoc
	known := make(map[string]bool)
	for rows.Next() {
		var (
			date time.Time
			url  string
		)
		if err = rows.Scan(&date, &url); err != nil {
			return nil, fmt.Errorf("%s: строка документа не прочитана: %w", mpSummaryTable, err)
		}
		day := mpDay(date)
		key := day.Format(mpSummaryDayLayout)
		if known[key] {
			continue
		}
		known[key] = true
		docs = append(docs, mpEvalDoc{Date: day, DateKey: key, URL: url})
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: документы не дочитаны: %w", mpSummaryTable, err)
	}

	if filter != nil {
		for key := range filter {
			if !known[key] {
				return nil, fmt.Errorf("%s: дата %s не найдена в %s", mpEvalDocsEnv, key, mpSummaryTable)
			}
		}
		filtered := docs[:0]
		for _, doc := range docs {
			if filter[doc.DateKey] {
				filtered = append(filtered, doc)
			}
		}
		docs = filtered
	}
	if len(docs) == 0 {
		return nil, fmt.Errorf("нет документов для прогона (фильтр %s)", mpEvalDocsEnv)
	}
	return docs, nil
}

// mpEvalFetchDocs — страницы ЦБ скачиваются util.GetPage и разбираются тем же
// parseMPSummary, что у продового слоя. Ошибка сети или разбора — инфраструктурный сбой:
// fail, а не запись в stats (спека §3.4 п. 5). Дата разобранного документа обязана совпасть
// с meeting_date шапки, иначе сравнение моделей шло бы по разным документам.
func mpEvalFetchDocs(t *testing.T, docs []mpEvalDoc) {
	t.Helper()
	for i := range docs {
		page, err := util.GetPage(docs[i].URL)
		if err != nil {
			t.Fatalf("документ %s (%s): страница не скачана: %v", docs[i].DateKey, docs[i].URL, err)
		}
		parsed, err := parseMPSummary(page)
		if err != nil {
			t.Fatalf("документ %s (%s): не разобран: %v", docs[i].DateKey, docs[i].URL, err)
		}
		if got := mpDay(parsed.MeetingDate).Format(mpSummaryDayLayout); got != docs[i].DateKey {
			t.Fatalf("документ %s (%s): parseMPSummary вернул дату заседания %s", docs[i].DateKey, docs[i].URL, got)
		}
		docs[i].Parsed = parsed
		t.Logf("документ %s: %d блоков (%s)", docs[i].DateKey, len(parsed.Blocks), docs[i].URL)
	}
}

// mpEvalRunJob — один вызов модели для пары (документ, профиль). WRITE=1 идёт через
// mpTagLLM — ту же функцию, что у импортёра, с батчем на документ: успех отправляется,
// ошибка LLM закрывает батч и пишется в stats. WRITE=0 вызывает tagMPChannelsLLM напрямую.
// Ошибка LLM — в stats и продолжение; ошибка базы/батча — fail теста (не LLM-сбой).
func mpEvalRunJob(t *testing.T, ctx context.Context, conn driver.Conn, job mpEvalJob, write bool) mpEvalRecord {
	t.Helper()
	doc := mpLLMDoc{Date: job.Doc.Date, Url: job.Doc.URL, Doc: job.Doc.Parsed}
	rec := mpEvalRecord{
		Profile:     job.Profile.Name,
		Model:       job.Profile.Model,
		MeetingDate: job.Doc.DateKey,
	}

	var (
		stats mpLLMDocStats
		err   error
		rows  int
	)
	if write {
		batch := &mpBatch{ctx: ctx, conn: conn, table: mpSummaryChannelTable, insert: mpSummaryChannelInsert}
		stats, err = mpTagLLM(batch, doc, job.Profile)
		if err != nil {
			batch.abort()
		} else {
			if sendErr := batch.send(); sendErr != nil {
				t.Fatalf("LLM %s / %s: запись в ClickHouse: %v", job.Profile.Model, job.Doc.DateKey, sendErr)
			}
			rows = int(batch.rows)
		}
	} else {
		var matches []mpChannelMatch
		matches, stats, err = tagMPChannelsLLM(job.Doc.Parsed, job.Profile)
		rows = len(matches)
	}
	if err != nil {
		// В WRITE=0 базы в вызове нет: любая ошибка — от модели. В WRITE=1 ошибка, не
		// помеченная mpLLMTagError, пришла от батча и обязана уронить прогон.
		if write && !mpIsLLMTagError(err) {
			t.Fatalf("LLM %s / %s: ошибка не LLM (база/батч): %v", job.Profile.Model, job.Doc.DateKey, err)
		}
		rec.Error = err.Error()
		rows = 0
	}
	rec.Attempts = stats.Attempts
	rec.DurationMS = stats.Duration.Milliseconds()
	rec.InputTokens = stats.Usage.InputTokens
	rec.OutputTokens = stats.Usage.OutputTokens
	rec.CacheReadInputTokens = stats.Usage.CacheReadInputTokens
	rec.CacheCreationInputTokens = stats.Usage.CacheCreationInputTokens
	rec.QuotesTotal = stats.QuotesTotal
	rec.QuotesKept = stats.QuotesKept
	rec.QuotesDropped = stats.QuotesDropped
	rec.Notes = stats.Notes
	rec.ChannelsWithQuotes = stats.ChannelsWithQuotes
	rec.Rows = rows
	return rec
}

// mpEvalReadStats — журнал stats.jsonl. RESUME=1 читает его и пропускает только успешные
// пары; неразобранная строка — ошибка теста (RESUME=0 даёт чистый прогон, а тихо
// проглоченная строка потеряла бы уже оплаченный вызов). Отсутствующий файл — пустой журнал.
func mpEvalReadStats(path string) ([]mpEvalRecord, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%s: журнал не прочитан: %w", path, err)
	}
	var records []mpEvalRecord
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var rec mpEvalRecord
		if err = json.Unmarshal([]byte(line), &rec); err != nil {
			return nil, fmt.Errorf("%s:%d: строка stats.jsonl не разобрана: %w", path, i+1, err)
		}
		records = append(records, rec)
	}
	return records, nil
}

// mpEvalViewLLMQuotes — цитаты model из витрины (method='llm', нужная редакция промпта,
// без note). Витрина уже FINAL — повторный FINAL не нужен. Строки фильтруются по датам
// текущего прогона: Jaccard по 22 документам не должен разбавляться парами, которых в
// прогоне не было (smoke с MP_LLM_EVAL_DOCS).
func mpEvalViewLLMQuotes(ctx context.Context, conn driver.Conn, model string, runDates map[string]bool) ([]mpEvalQuoteRow, error) {
	rows, err := conn.Query(ctx, "SELECT meeting_date, channel, text FROM "+mpSummaryChannelsView.Name+
		" WHERE method = ? AND model = ? AND prompt_version = ? AND kind != ?",
		mpMethodLLM, model, mpPromptVersion, mpKindNote)
	if err != nil {
		return nil, fmt.Errorf("%s: цитаты модели %s не прочитаны: %w", mpSummaryChannelsView.Name, model, err)
	}
	defer func() { _ = rows.Close() }()
	return mpEvalScanQuoteRows(rows, runDates)
}

// mpEvalViewRegexQuotes — цитаты regex-слоя из витрины (method='regex', без note) для
// сравнения «модель vs regex» fuzzy-Jaccard (спека §3.5).
func mpEvalViewRegexQuotes(ctx context.Context, conn driver.Conn, runDates map[string]bool) ([]mpEvalQuoteRow, error) {
	rows, err := conn.Query(ctx, "SELECT meeting_date, channel, text FROM "+mpSummaryChannelsView.Name+
		" WHERE method = ? AND kind != ?", mpMethodRegex, mpKindNote)
	if err != nil {
		return nil, fmt.Errorf("%s: цитаты regex-слоя не прочитаны: %w", mpSummaryChannelsView.Name, err)
	}
	defer func() { _ = rows.Close() }()
	return mpEvalScanQuoteRows(rows, runDates)
}

// mpEvalScanQuoteRows — общий разбор строк витрины для Jaccard: дата нормализуется mpDay,
// строки вне дат прогона пропускаются.
func mpEvalScanQuoteRows(rows driver.Rows, runDates map[string]bool) ([]mpEvalQuoteRow, error) {
	var out []mpEvalQuoteRow
	for rows.Next() {
		var (
			date    time.Time
			channel string
			text    string
		)
		if err := rows.Scan(&date, &channel, &text); err != nil {
			return nil, fmt.Errorf("строка витрины не прочитана: %w", err)
		}
		key := mpDay(date).Format(mpSummaryDayLayout)
		if runDates != nil && !runDates[key] {
			continue
		}
		out = append(out, mpEvalQuoteRow{MeetingDate: key, Channel: channel, Text: text})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("строки витрины не дочитаны: %w", err)
	}
	return out, nil
}

// mpEvalInterModelJaccard — попарное межмодельное согласие (точные цитаты) по всем моделям
// прогона; меньше двух моделей — пусто (спека §3.5: GLM↔MiMo и т.п.).
func mpEvalInterModelJaccard(models []string, sets map[string]map[mpEvalDocChannel]map[string]bool) []mpEvalInterJaccard {
	var out []mpEvalInterJaccard
	for i := 0; i < len(models); i++ {
		for j := i + 1; j < len(models); j++ {
			out = append(out, mpEvalInterJaccard{
				A:      models[i],
				B:      models[j],
				Result: mpEvalJaccard(sets[models[i]], sets[models[j]], false),
			})
		}
	}
	return out
}

// mpEvalWriteSummaryCSV — summary.csv: строка на пару (model, meeting_date), последняя
// запись журнала. Сортировка по профилю и дате — чтобы файл можно было диффать между
// прогонами.
func mpEvalWriteSummaryCSV(t *testing.T, path string, records []mpEvalRecord) {
	t.Helper()
	latest := mpEvalLatestRecords(records)
	sort.Slice(latest, func(i, j int) bool {
		if latest[i].Profile != latest[j].Profile {
			return latest[i].Profile < latest[j].Profile
		}
		return latest[i].MeetingDate < latest[j].MeetingDate
	})

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("%s: файл не создан: %v", path, err)
	}
	defer func() { _ = f.Close() }()

	w := csv.NewWriter(f)
	header := []string{
		"profile", "model", "meeting_date", "attempts", "duration_ms",
		"input_tokens", "output_tokens", "cache_read_input_tokens", "cache_creation_input_tokens",
		"quotes_total", "quotes_kept", "quotes_dropped", "notes", "channels_with_quotes", "rows", "error",
	}
	if err = w.Write(header); err != nil {
		t.Fatalf("%s: заголовок не записан: %v", path, err)
	}
	for _, r := range latest {
		row := []string{
			r.Profile, r.Model, r.MeetingDate,
			strconv.Itoa(r.Attempts), strconv.FormatInt(r.DurationMS, 10),
			strconv.FormatInt(r.InputTokens, 10), strconv.FormatInt(r.OutputTokens, 10),
			strconv.FormatInt(r.CacheReadInputTokens, 10), strconv.FormatInt(r.CacheCreationInputTokens, 10),
			strconv.Itoa(r.QuotesTotal), strconv.Itoa(r.QuotesKept), strconv.Itoa(r.QuotesDropped),
			strconv.Itoa(r.Notes), strconv.Itoa(r.ChannelsWithQuotes), strconv.Itoa(r.Rows), r.Error,
		}
		if err = w.Write(row); err != nil {
			t.Fatalf("%s: строка %s/%s не записана: %v", path, r.Profile, r.MeetingDate, err)
		}
	}
	w.Flush()
	if err = w.Error(); err != nil {
		t.Fatalf("%s: CSV не дописан: %v", path, err)
	}
}

// mpEvalWriteReport — report.md: условия прогона, скорость, качество, согласие (Jaccard) и
// цена. Агрегаты считаются по всему stats.jsonl (журнал попыток), summary.csv — по последней
// записи пары; это разные срезы, и отчёт называет их явно.
func mpEvalWriteReport(t *testing.T, path string, rep mpEvalReport) {
	t.Helper()
	var b strings.Builder
	b.WriteString("# Сравнение LLM-моделей разметки каналов `cbr_mp_summary`\n\n")

	profileNames := make([]string, 0, len(rep.Profiles))
	for _, p := range rep.Profiles {
		profileNames = append(profileNames, fmt.Sprintf("`%s` (%s)", p.Name, p.Model))
	}
	filter := "все документы таблицы"
	if rep.DocsFilter != "" {
		filter = rep.DocsFilter
	}
	mpEvalWritef(&b, "- Прогон: %s\n", rep.Started.Format(time.RFC3339))
	mpEvalWritef(&b, "- Профили: %s\n", strings.Join(profileNames, ", "))
	mpEvalWritef(&b, "- Документы: %d (%s … %s); фильтр: %s\n",
		len(rep.Docs), rep.Docs[0].DateKey, rep.Docs[len(rep.Docs)-1].DateKey, filter)
	mpEvalWritef(&b, "- Записей в stats.jsonl: %d (скорость/цена — по всем записям, качество и summary.csv — по последней записи пары)\n", len(rep.Records))
	mpEvalWritef(&b, "- timeout: %s, max_tokens: %d, delay: %s\n", rep.Timeout, rep.MaxTokens, rep.Delay)
	mpEvalWritef(&b, "- write: %t, force: %t, resume: %t, report_only: %t, prefill: %t, thinking: %s, transport: %s, reasoning: %s, concise: %t\n", rep.Write, rep.Force, rep.Resume, rep.ReportOnly, rep.Prefill, rep.Thinking, rep.Transport, rep.Reasoning, rep.Concise)
	mpEvalWritef(&b, "- Артефакты: `%s` (`stats.jsonl`, `summary.csv`, `report.md`, `notes-sample.md`)\n", rep.OutDir)
	mpEvalWritef(&b, "- Baseline для Jaccard: `%s`, prompt_version=%d\n\n", mpEvalBaselineModel, mpPromptVersion)

	b.WriteString("## Скорость\n\n")
	b.WriteString("Латентность — по успешным вызовам без повтора (attempts=1); токены — фактический расход по всем записям, включая повторы и сбои.\n\n")
	if len(rep.Models) == 0 {
		b.WriteString("Записей нет: все пары пропущены (resume/tagged) — сравнивать нечего.\n\n")
	} else {
		b.WriteString("| модель | вызовов | сбоев | повторов | замеров | total, с | mean, с | p50, с | p95, с | input | output | cache read | cache write | output tok/s |\n")
		b.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
		for _, model := range rep.Models {
			s := mpEvalSpeed(rep.Records, model)
			total, mean, p50, p95 := "н/д", "н/д", "н/д", "н/д"
			rate := "н/д"
			if s.LatencySamples > 0 {
				total = fmt.Sprintf("%.2f", float64(s.TotalMS)/1000)
				mean = fmt.Sprintf("%.2f", s.MeanMS/1000)
				p50 = fmt.Sprintf("%.2f", float64(s.P50MS)/1000)
				p95 = fmt.Sprintf("%.2f", float64(s.P95MS)/1000)
				rate = fmt.Sprintf("%.2f", s.OutputTokensPerSecond)
			}
			mpEvalWritef(&b, "| %s | %d | %d | %d | %d | %s | %s | %s | %s | %d | %d | %d | %d | %s |\n",
				model, s.Calls, s.Failures, s.Retries, s.LatencySamples,
				total, mean, p50, p95,
				s.InputTokens, s.OutputTokens, s.CacheReadInputTokens, s.CacheCreationInputTokens, rate)
		}
		b.WriteString("\n")
	}

	b.WriteString("## Качество\n\n")
	b.WriteString("Считается по последней записи пары (итоговое состояние документа); сбои в журнале — в таблице скорости. drop rate = отброшенные / все цитаты ответа; покрытие — каналы с сохранённой цитатой из 11 реестра.\n\n")
	if len(rep.Models) == 0 {
		b.WriteString("Записей нет.\n\n")
	} else {
		b.WriteString("| модель | документов | сбоев | цитат | сохранено | отброшено | drop rate | ср. каналов / 11 | 11/11 | ср. заметок |\n")
		b.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
		for _, model := range rep.Models {
			q := mpEvalQuality(mpEvalLatestRecords(rep.Records), model)
			full := "н/д"
			if q.Docs > 0 {
				full = fmt.Sprintf("%d (%.0f%%)", q.FullCoverage, 100*float64(q.FullCoverage)/float64(q.Docs))
			}
			mpEvalWritef(&b, "| %s | %d | %d | %d | %d | %d | %.2f%% | %.2f | %s | %.2f |\n",
				model, q.Docs, q.Failures, q.QuotesTotal, q.QuotesKept, q.QuotesDropped,
				100*q.DropRate, q.MeanChannelsWithQuotes, full, q.MeanNotes)
		}
		b.WriteString("\n")
	}

	b.WriteString("## Согласие с baseline и regex-слоем\n\n")
	b.WriteString("Jaccard — макро-среднее по парам (meeting_date, channel) с непустым объединением; для regex совпадение — нормализованные тексты входят друг в друга (спека §3.5).\n\n")
	if len(rep.Models) == 0 {
		b.WriteString("Записей нет.\n\n")
	} else {
		mpEvalWritef(&b, "| модель | Jaccard `%s` | пар | Jaccard regex (fuzzy) | пар |\n", mpEvalBaselineModel)
		b.WriteString("|---|---:|---:|---:|---:|\n")
		for _, model := range rep.Models {
			base := rep.Baseline[model]
			regex := rep.Regex[model]
			baseScore, basePairs := "н/д", 0
			regexScore, regexPairs := "н/д", 0
			if base.Pairs > 0 {
				baseScore, basePairs = fmt.Sprintf("%.3f", base.Score), base.Pairs
			}
			if regex.Pairs > 0 {
				regexScore, regexPairs = fmt.Sprintf("%.3f", regex.Score), regex.Pairs
			}
			mpEvalWritef(&b, "| %s | %s | %d | %s | %d |\n", model, baseScore, basePairs, regexScore, regexPairs)
		}
		b.WriteString("\n")
	}

	if len(rep.Models) >= 2 {
		b.WriteString("## Межмодельное согласие\n\n")
		b.WriteString("Jaccard точных цитат по парам (meeting_date, channel); пары с пустым объединением не учитываются.\n\n")
		b.WriteString("| модель A | модель B | Jaccard | пар |\n")
		b.WriteString("|---|---|---:|---:|\n")
		for _, inter := range rep.Inter {
			score := "н/д"
			if inter.Result.Pairs > 0 {
				score = fmt.Sprintf("%.3f", inter.Result.Score)
			}
			mpEvalWritef(&b, "| %s | %s | %s | %d |\n", inter.A, inter.B, score, inter.Result.Pairs)
		}
		b.WriteString("\n")
	}

	b.WriteString("## Цена\n\n")
	hasPrices := false
	for _, p := range rep.Profiles {
		if price, ok := rep.Prices[p.Model]; ok && price.Set {
			hasPrices = true
			break
		}
	}
	if !hasPrices {
		b.WriteString("Тарифы не заданы (`<PROFILE>_LLM_PRICE_IN`, `<PROFILE>_LLM_PRICE_OUT`) — цена н/д; токены — в таблице скорости.\n")
	} else {
		b.WriteString("Тарифы — USD за 1М токенов из env; стоимость документа — средняя по документам модели, 22 документа — полный бэкфилл текущего ряда ЦБ. Цена не учитывает скидки и кеш провайдера.\n\n")
		b.WriteString("| профиль | модель | $/1М input | $/1М output | $/документ | $/22 документа |\n")
		b.WriteString("|---|---|---:|---:|---:|---:|\n")
		latest := mpEvalLatestRecords(rep.Records)
		for _, p := range rep.Profiles {
			price, ok := rep.Prices[p.Model]
			if !ok || !price.Set {
				mpEvalWritef(&b, "| %s | %s | н/д | н/д | н/д | н/д |\n", p.Name, p.Model)
				continue
			}
			docs := 0
			for _, r := range latest {
				if r.Model == p.Model {
					docs++
				}
			}
			if docs == 0 {
				mpEvalWritef(&b, "| %s | %s | %.4f | %.4f | н/д (нет записей) | н/д |\n",
					p.Name, p.Model, price.In, price.Out)
				continue
			}
			s := mpEvalSpeed(rep.Records, p.Model)
			perDoc := mpEvalCost(s.InputTokens, s.OutputTokens, price.In, price.Out) / float64(docs)
			mpEvalWritef(&b, "| %s | %s | %.4f | %.4f | $%.6f | $%.6f |\n",
				p.Name, p.Model, price.In, price.Out, perDoc, perDoc*mpEvalBackfillDocs)
		}
	}
	b.WriteString("\n")

	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatalf("%s: отчёт не записан: %v", path, err)
	}
}

// mpEvalWritef — printf в strings.Builder отчёта: fmt.Fprintf с явно отброшенной ошибкой
// (Builder.Write не возвращает содержательной ошибки), staticcheck QF1012 требует Fprintf
// вместо WriteString(Sprintf(...)).
func mpEvalWritef(b *strings.Builder, format string, args ...any) {
	_, _ = fmt.Fprintf(b, format, args...)
}

// mpEvalWriteNotesSample — notes-sample.md: три даты образца × 11 каналов × модели, чтобы
// качество заметок можно было оценить глазами. Даты образца — даты публикации: они
// переводятся в meeting_date через v_cbr_mp_summary.published, а дата вне фильтра прогона
// пропускается (спека §3.5, §3.4).
func mpEvalWriteNotesSample(t *testing.T, ctx context.Context, conn driver.Conn, path string,
	docs []mpEvalDoc, profileModels []string) {
	t.Helper()
	runDates := make(map[string]bool, len(docs))
	for _, doc := range docs {
		runDates[doc.DateKey] = true
	}

	publishedToMeeting, err := mpEvalPublishedIndex(ctx, conn)
	if err != nil {
		t.Fatalf("даты публикации: %v", err)
	}
	sampleMeetings := make(map[string]bool, len(mpEvalNotePublishedDates))
	for _, published := range mpEvalNotePublishedDates {
		if meeting, ok := publishedToMeeting[published]; ok && runDates[meeting] {
			sampleMeetings[meeting] = true
		}
	}
	notes, err := mpEvalNoteRows(ctx, conn, sampleMeetings)
	if err != nil {
		t.Fatalf("заметки витрины: %v", err)
	}

	var b strings.Builder
	b.WriteString("# Заметки моделей по каналам (ручная оценка)\n\n")
	b.WriteString("Даты — даты публикации резюме на сайте ЦБ; в витрине `v_cbr_mp_summary_channels` ключ — дата заседания (`meeting_date`), она указана в заголовке. Заметки — `kind='note'`, `prompt_version=1`.\n\n")

	written := 0
	for _, published := range mpEvalNotePublishedDates {
		meeting, ok := publishedToMeeting[published]
		if !ok {
			t.Logf("notes-sample: дата публикации %s не найдена в v_cbr_mp_summary, пропущена", published)
			continue
		}
		if !runDates[meeting] {
			t.Logf("notes-sample: %s (заседание %s) вне фильтра прогона, пропущена", published, meeting)
			continue
		}
		written++
		mpEvalWritef(&b, "## %s (заседание %s)\n\n", published, meeting)

		models := mpEvalNoteModels(notes, meeting, profileModels)
		if len(models) == 0 {
			b.WriteString("Заметок нет.\n\n")
			continue
		}
		channels := mpEvalNoteChannels(notes, meeting)

		b.WriteString("| канал | " + strings.Join(models, " | ") + " |\n")
		b.WriteString("|" + strings.Repeat("---|", len(models)+1) + "\n")
		for _, channel := range channels {
			b.WriteString("| " + channel + " |")
			for _, model := range models {
				b.WriteString(" " + mpEvalMarkdownCell(notes[mpEvalNoteKey{MeetingDate: meeting, Channel: channel, Model: model}]) + " |")
			}
			b.WriteString("\n")
		}
		b.WriteString("\n")
	}
	if written == 0 {
		b.WriteString("Ни одна из дат образца не входит в фильтр прогона.\n")
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatalf("%s: образец заметок не записан: %v", path, err)
	}
}

// mpEvalPublishedIndex — дата публикации → дата заседания из v_cbr_mp_summary. Даты
// нормализуются mpDay: в таблице Date, драйвер отдаёт его в зоне сессии.
func mpEvalPublishedIndex(ctx context.Context, conn driver.Conn) (map[string]string, error) {
	rows, err := conn.Query(ctx, "SELECT meeting_date, published FROM "+mpSummaryView.Name)
	if err != nil {
		return nil, fmt.Errorf("%s: даты публикации не прочитаны: %w", mpSummaryView.Name, err)
	}
	defer func() { _ = rows.Close() }()

	index := make(map[string]string)
	for rows.Next() {
		var meeting, published time.Time
		if err = rows.Scan(&meeting, &published); err != nil {
			return nil, fmt.Errorf("%s: строка дат не прочитана: %w", mpSummaryView.Name, err)
		}
		index[mpDay(published).Format(mpSummaryDayLayout)] = mpDay(meeting).Format(mpSummaryDayLayout)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: даты публикации не дочитаны: %w", mpSummaryView.Name, err)
	}
	return index, nil
}

// mpEvalNoteRows — заметки модели по датам образца: (meeting_date, channel, model) → text.
// Берутся все модели витрины (включая baseline): глазами сравнивают заметки рядом, а не
// только профилей текущего прогона.
func mpEvalNoteRows(ctx context.Context, conn driver.Conn, sampleMeetings map[string]bool) (map[mpEvalNoteKey]string, error) {
	rows, err := conn.Query(ctx, "SELECT meeting_date, channel, model, text FROM "+mpSummaryChannelsView.Name+
		" WHERE method = ? AND kind = ? AND prompt_version = ?", mpMethodLLM, mpKindNote, mpPromptVersion)
	if err != nil {
		return nil, fmt.Errorf("%s: заметки не прочитаны: %w", mpSummaryChannelsView.Name, err)
	}
	defer func() { _ = rows.Close() }()

	notes := make(map[mpEvalNoteKey]string)
	for rows.Next() {
		var (
			date                 time.Time
			channel, model, text string
		)
		if err = rows.Scan(&date, &channel, &model, &text); err != nil {
			return nil, fmt.Errorf("%s: строка заметки не прочитана: %w", mpSummaryChannelsView.Name, err)
		}
		meeting := mpDay(date).Format(mpSummaryDayLayout)
		if !sampleMeetings[meeting] {
			continue
		}
		key := mpEvalNoteKey{MeetingDate: meeting, Channel: channel, Model: model}
		if text = strings.TrimSpace(text); text != "" {
			notes[key] = text
		}
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: заметки не дочитаны: %w", mpSummaryChannelsView.Name, err)
	}
	return notes, nil
}

// mpEvalNoteModels — колонки образца: модели профилей прогона в их порядке, затем
// остальные модели витрины этой даты по алфавиту (baseline и прежние прогоны).
func mpEvalNoteModels(notes map[mpEvalNoteKey]string, meeting string, profileModels []string) []string {
	present := make(map[string]bool)
	for key := range notes {
		if key.MeetingDate == meeting {
			present[key.Model] = true
		}
	}
	models := make([]string, 0, len(present))
	for _, model := range profileModels {
		if present[model] {
			models = append(models, model)
			delete(present, model)
		}
	}
	rest := make([]string, 0, len(present))
	for model := range present {
		rest = append(rest, model)
	}
	sort.Strings(rest)
	return append(models, rest...)
}

// mpEvalNoteChannels — строки образца: 11 каналов реестра в его порядке, затем каналы вне
// реестра (если появятся в витрине) по алфавиту.
func mpEvalNoteChannels(notes map[mpEvalNoteKey]string, meeting string) []string {
	channels := make([]string, 0, len(mpChannels))
	known := make(map[string]bool, len(mpChannels))
	for _, ch := range mpChannels {
		channels = append(channels, ch.Slug)
		known[ch.Slug] = true
	}
	extra := make(map[string]bool)
	for key := range notes {
		if key.MeetingDate == meeting && !known[key.Channel] {
			extra[key.Channel] = true
		}
	}
	rest := make([]string, 0, len(extra))
	for channel := range extra {
		rest = append(rest, channel)
	}
	sort.Strings(rest)
	return append(channels, rest...)
}

// mpEvalMarkdownCell — текст заметки в ячейке markdown-таблицы: перевод строки ломает
// таблицу, вертикальная черта — разделитель, пустая заметка — прочерк.
func mpEvalMarkdownCell(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "—"
	}
	s = strings.ReplaceAll(s, "|", `\|`)
	s = strings.ReplaceAll(s, "\r\n", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	return s
}

// mpEvalEnvBool — 0|1, пусто → значение по умолчанию. Иное значение — ошибка теста:
// молчаливое «не 1 значит false» скрыло бы опечатку в командной строке прогона.
func mpEvalEnvBool(t *testing.T, name string, def bool) bool {
	t.Helper()
	switch raw := strings.TrimSpace(os.Getenv(name)); raw {
	case "":
		return def
	case "0":
		return false
	case "1":
		return true
	default:
		t.Fatalf("%s=%q: ожидается 0 или 1", name, raw)
		return def
	}
}

// mpEvalEnvThinking — режим Anthropic thinking: пусто/default → поле не отправляется,
// disabled → thinking: {type: disabled}. Иное — ошибка теста.
func mpEvalEnvThinking(t *testing.T) string {
	t.Helper()
	switch raw := strings.ToLower(strings.TrimSpace(os.Getenv(mpEvalThinkingEnv))); raw {
	case "", "default":
		return "default"
	case "disabled":
		return "disabled"
	default:
		t.Fatalf("%s=%q: ожидается default или disabled", mpEvalThinkingEnv, raw)
		return "default"
	}
}

// mpEvalEnvTransport — транспорт harness: пусто/anthropic → SDK, openai → JSON-режим.
func mpEvalEnvTransport(t *testing.T) string {
	t.Helper()
	switch raw := strings.ToLower(strings.TrimSpace(os.Getenv(mpEvalTransportEnv))); raw {
	case "", "anthropic":
		return "anthropic"
	case "openai":
		return "openai"
	default:
		t.Fatalf("%s=%q: ожидается anthropic или openai", mpEvalTransportEnv, raw)
		return "anthropic"
	}
}

// mpEvalEnvReasoning — OpenAI reasoning_effort: пусто → поле не отправляется, иначе
// none|low|medium|high. Влияет только на harness-транспорт openai.
func mpEvalEnvReasoning(t *testing.T) string {
	t.Helper()
	switch raw := strings.ToLower(strings.TrimSpace(os.Getenv(mpEvalReasoningEnv))); raw {
	case "", "none", "low", "medium", "high":
		return raw
	default:
		t.Fatalf("%s=%q: ожидается none|low|medium|high", mpEvalReasoningEnv, raw)
		return ""
	}
}

// mpEvalEnvInt64 — положительное целое, пусто → значение по умолчанию.
func mpEvalEnvInt64(t *testing.T, name string, def int64) int64 {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		t.Fatalf("%s=%q: ожидается положительное целое", name, raw)
	}
	return value
}

// mpEvalEnvDuration — положительная длительность time.ParseDuration, пусто → значение по
// умолчанию (timeout одного вызова).
func mpEvalEnvDuration(t *testing.T, name string, def time.Duration) time.Duration {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		t.Fatalf("%s=%q: ожидается положительная длительность (например, 180s)", name, raw)
	}
	return value
}

// mpEvalEnvDelay — пауза между вызовами: 0s допустим (rate limit не нужен), отрицательная
// и неразобранная длительность — ошибка.
func mpEvalEnvDelay(t *testing.T, name string, def time.Duration) time.Duration {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value < 0 {
		t.Fatalf("%s=%q: ожидается неотрицательная длительность (например, 1s или 0s)", name, raw)
	}
	return value
}

// mpEvalEnvPrice — необязательный тариф профиля, USD за 1М токенов. Пусто → (0,false):
// отчёт пишет «н/д», а не выдумывает цену (спека §3.5).
func mpEvalEnvPrice(t *testing.T, name string) (float64, bool) {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return 0, false
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		t.Fatalf("%s=%q: ожидается неотрицательное число — USD за 1М токенов", name, raw)
	}
	return value, true
}

// mpEvalProfilePrices — тарифы по профилям: <PROFILE>_LLM_PRICE_IN/_OUT. Пара задаётся
// вместе: одинокая переменная — опечатка конфигурации, а не «вторая цена ноль».
func mpEvalProfilePrices(t *testing.T, profiles []mpLLMProfile) map[string]mpEvalPrice {
	t.Helper()
	prices := make(map[string]mpEvalPrice, len(profiles))
	for _, profile := range profiles {
		prefix := strings.ToUpper(profile.Name) + "_LLM_PRICE_"
		inName, outName := prefix+"IN", prefix+"OUT"
		priceIn, inSet := mpEvalEnvPrice(t, inName)
		priceOut, outSet := mpEvalEnvPrice(t, outName)
		if inSet != outSet {
			missing := inName
			if inSet {
				missing = outName
			}
			t.Fatalf("%s и %s задаются только вместе: не задана %s", inName, outName, missing)
		}
		if inSet {
			prices[profile.Model] = mpEvalPrice{In: priceIn, Out: priceOut, Set: true}
		}
	}
	return prices
}

// mpEvalEnvDates — необязательный фильтр MP_LLM_EVAL_DOCS: даты YYYY-MM-DD через запятую.
// Возвращает множество дат и исходную строку для отчёта; неразобранная дата — ошибка теста.
func mpEvalEnvDates(t *testing.T) (map[string]bool, string) {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(mpEvalDocsEnv))
	if raw == "" {
		return nil, ""
	}
	dates := make(map[string]bool)
	for _, part := range strings.Split(raw, ",") {
		value := strings.TrimSpace(part)
		if value == "" {
			continue
		}
		date, err := time.Parse(mpSummaryDayLayout, value)
		if err != nil {
			t.Fatalf("%s=%q: %q не дата YYYY-MM-DD", mpEvalDocsEnv, raw, value)
		}
		dates[date.Format(mpSummaryDayLayout)] = true
	}
	if len(dates) == 0 {
		t.Fatalf("%s=%q: пустой список дат", mpEvalDocsEnv, raw)
	}
	return dates, raw
}
