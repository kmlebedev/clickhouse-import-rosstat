package cbr

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	log "github.com/sirupsen/logrus"
)

// Импортёр сырых остатков формы 101 ЦБ (первый срез — ПАО Сбербанк, REGN=1481):
// помесячная оборотная ведомость по счетам бухгалтерского учёта. Таблица
// cbr_form101 — сырые строки «счёт → остаток», без маппинга на метрики
// кредитования: маппинг счетов на loans_corporations/loans_individuals и запись
// в company_financials — следующий шаг (спека docs/superpowers/specs/
// 2026-10-17-cbr-form101-design.md §7). Артефакт — не XLSX, а RAR с DBF, поэтому
// у импортёра свой Import() (прецедент — cbr_dip_bulletin): шаблон
// util.ClickHouseImport ведёт таблицу, DDL и объявление точки контроля, но файл
// качает и разбирает этот Import.

const (
	// cbrForm101Table — имя таблицы и импортёра (Name() промотирован из
	// util.ClickHouseImport): контракт CLICKHOUSE_IMPORT_STAT и шага dagu.
	cbrForm101Table = "cbr_form101"

	// cbrForm101PageUrl — страница отчётности кредитных организаций ЦБ: на ней
	// блок «Оборотная ведомость по счетам бухгалтерского учёта (форма 101)» со
	// ссылками /vfs/credit/forms/101-YYYYMMDD.rar (спека §2).
	cbrForm101PageUrl = "https://www.cbr.ru/banking_sector/otchetnost-kreditnykh-organizaciy/"

	// cbrForm101SberRegnum — регистрационный номер ПАО Сбербанк в справочнике
	// ЦБ. Первый срез импортирует только его: таблица остаётся маленькой, а
	// схема готова к другим regnum (спека §3).
	cbrForm101SberRegnum = 1481

	// cbrForm101Ddl — канонический DDL таблицы (спека §3) в %s-форме шаблона:
	// %s — TableName. report_date — дата из имени файла, не отчётный месяц;
	// balance_total — VITG, тыс. руб., как в источнике; loaded_at — версия
	// ReplacingMergeTree: повторная запись той же даты перезаписывает строку.
	cbrForm101Ddl = `CREATE TABLE IF NOT EXISTS %s (
    report_date Date,
    regnum UInt32,
    account LowCardinality(String),
    plan LowCardinality(String),
    active_passive LowCardinality(String),
    balance_total Float64,
    loaded_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (report_date, regnum, account, active_passive, plan);
`

	// cbrForm101CountPoints — количественный чек версии (стандарт §9.2): число
	// строк своей таблицы. FINAL обязателен — иначе count() считал бы
	// несмёрженные части. Решение о no-op принимает гейт по дате (ниже), а не
	// префлайт: артефакт — один RAR, но имя файла несёт отчётную дату, и она же
	// ключ прогона.
	cbrForm101CountPoints = "SELECT count() FROM cbr_form101 FINAL"

	// cbrForm101MaxDateQuery — гейт прогона: максимальная уже записанная отчётная
	// дата. Форма 101 публикуется раз в месяц, dagu ходит ежедневно, поэтому
	// повторный прогон обязан быть no-op без скачивания 350-КБ RAR (спека §3–4).
	cbrForm101MaxDateQuery = "SELECT max(report_date) FROM cbr_form101 FINAL"

	// cbrForm101Insert — один батч на прогон; колонки перечислены явно, чтобы
	// состав не зависел от порядка полей DDL (loaded_at заполняет now()).
	cbrForm101Insert = "INSERT INTO cbr_form101 (report_date, regnum, account, plan, active_passive, balance_total)"
)

// cbrForm101View — витрина сырых остатков формы 101: long-форма с датой отчёта,
// регнумом, счётом и остатком (спека §3). Отдельная витрина, а не ветка
// v_cbr_macro: это не ряд «дата → значение», а срез всех счетов банка за дату,
// и готовая метрика кредитования из него ещё не собрана.
var cbrForm101View = util.View{
	Name:   "v_cbr_form101",
	Tables: []string{cbrForm101Table},
	Select: `SELECT report_date, regnum, account, plan, active_passive, balance_total, loaded_at
FROM cbr_form101 FINAL
ORDER BY report_date DESC, regnum, account, active_passive`,
	Comment: "Сырые остатки формы 101 Банка России (оборотная ведомость по счетам бухгалтерского учёта), " +
		"первый срез — ПАО Сбербанк (regnum=1481), с отчётной даты 2026-09-01. account — код или группа " +
		"счёта источника (NUM_SC: 408, 45.0, ITGAP и т. п.), plan — план счетов (А/Б), active_passive — " +
		"признак активно/пассивного счёта (A_P), balance_total — итоговый остаток VITG в тыс. руб. как в " +
		"источнике (не пересчитан в рубли и не сверен с пресс-релизом Сбера). report_date — дата отчёта из " +
		"имени файла 101-YYYYMMDD.rar, а не месяц публикации. Ключ таблицы включает plan, поэтому все " +
		"194 строки Сбера (включая 8 строк группы ITGAP четырёх планов А/Б/В/Г) сохраняются. " +
		"Это не готовая метрика кредитования: " +
		"маппинг счетов (44x/45x, группы 45.0/45.2) на кредиты юрлицам и физлицам — следующий шаг, " +
		"поэтому не складывать строки витрины и не сравнивать их с loans_corporations/loans_individuals " +
		"без явного отбора счетов",
	Columns: map[string]string{
		"report_date":    "Дата отчёта из имени файла источника (101-YYYYMMDD.rar → YYYY-MM-DD), например 2026-09-01. Не месяц публикации и не период накопления: форма 101 — срез на первое число",
		"regnum":         "Регистрационный номер кредитной организации в справочнике ЦБ (REGN). Первый срез — 1481 (ПАО Сбербанк); схема готова к другим банкам",
		"account":        "Код или группа счёта источника (NUM_SC): детальный счёт (408, 501, 434), группа (45.0, 45.2) или агрегат (ITGAP). Это сырой код формы 101, а не имя ряда кредитования; пустые NUM_SC в таблицу не попадают",
		"plan":           "План счетов источника (PLAN): А — балансовые счета, Б — счета доверительного управления и прочие. Смысл и допустимые значения — по методологии ЦБ",
		"active_passive": "Признак активно/пассивного счёта (A_P): 1 — активный, 2 — пассивный (как в источнике). Знак остатка не пересчитывается: balance_total берётся из VITG",
		"balance_total":  "Итоговый остаток по счёту (VITG) в тыс. руб., как в источнике; не пересчитан в рубли. Суммировать по всем строкам нельзя: группы счетов пересекаются с детальными счетами",
		"loaded_at":      "Момент записи строки импортёром (версия ReplacingMergeTree). Повторный импорт той же report_date перезаписывает строки, поэтому читать витрину нужно через FINAL",
	},
}

// cbrForm101Stat — импортёр cbr_form101. Встроенный util.ClickHouseImport даёт
// Name(), TableName и объявление точки контроля публикации; Import переопределён
// целиком: страница → RAR → DBF — путь, которого у шаблона (XLSX/HTML) нет.
type cbrForm101Stat struct {
	util.ClickHouseImport
}

// Import — полный цикл прогона (спека §4): DDL → витрина → страница ЦБ → ссылка
// на самый свежий RAR → гейт по max(report_date) → скачивание RAR → B1.dbf →
// разбор DBF → фильтр REGN=1481 → один батч. Порядок шагов — контракт: DDL и
// витрина выполняются до сети и до гейта, поэтому прогон на чистой базе и no-op
// повтор одинаково самолечат схему и витрину (спека §4.1–4.2).
func (s *cbrForm101Stat) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	// 1. DDL — до сети: гейт обращается к таблице, а на чистой базе её ещё нет.
	for _, ddl := range s.CreateTable {
		if err = conn.Exec(ctx, fmt.Sprintf(ddl, s.TableName)); err != nil {
			return 0, fmt.Errorf("%s: DDL: %w", cbrForm101Table, err)
		}
	}

	// 2. Витрина — тоже до сети: сбой её создания в прошлом прогоне должен
	// исправиться и на no-op-пути, иначе агент остался бы без витрины до
	// следующего выпуска формы (раз в месяц).
	if _, err = util.CreateView(ctx, conn, cbrForm101View); err != nil {
		return 0, fmt.Errorf("%s: витрина %s: %w", cbrForm101Table, cbrForm101View.Name, err)
	}

	// 3. Страница ЦБ и ссылка на самый свежий архив. Ноль ссылок — ошибка
	// (смена разметки), а не «новых данных нет» (спека §4.2).
	page, err := util.GetPage(cbrForm101PageUrl)
	if err != nil {
		return 0, fmt.Errorf("%s: %s: %w", cbrForm101Table, cbrForm101PageUrl, err)
	}
	latest, link, err := latestForm101Link(page)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", cbrForm101Table, err)
	}

	// 4. Гейт: отчётная дата строго новее записанной. Форма 101 выходит раз в
	// месяц, шаг dagu ежедневный, поэтому в обычный день это единственное
	// чтение своей таблицы и ни одного сетевого обращения к RAR.
	var known time.Time
	if err = conn.QueryRow(ctx, cbrForm101MaxDateQuery).Scan(&known); err != nil {
		return 0, fmt.Errorf("%s: максимум report_date не прочитан: %w", cbrForm101Table, err)
	}
	if !form101Newer(latest, known) {
		log.Infof("%s: отчётная дата %s уже импортирована (max=%s) — скачивание не требуется",
			cbrForm101Table, latest.Format(form101DayLayout), form101Day(known).Format(form101DayLayout))
		return 0, nil
	}

	// 5. RAR → B1.dbf → строки. Файл качается целиком (≈350 КБ): rardecode
	// читает из памяти, а частичное чтение RAR v5 не поддерживает.
	reader, err := util.GetFile(link)
	if err != nil {
		return 0, fmt.Errorf("%s: %s: %w", cbrForm101Table, link, err)
	}
	body, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil {
		return 0, fmt.Errorf("%s: %s: чтение: %w", cbrForm101Table, link, readErr)
	}
	if closeErr != nil {
		return 0, fmt.Errorf("%s: %s: закрытие: %w", cbrForm101Table, link, closeErr)
	}
	dbf, err := extractForm101B1(body)
	if err != nil {
		return 0, fmt.Errorf("%s: %s: %w", cbrForm101Table, link, err)
	}
	rows, err := parseForm101DBF(dbf)
	if err != nil {
		return 0, fmt.Errorf("%s: %s: %w", cbrForm101Table, link, err)
	}

	// 6. Первый срез — только Сбер. Ноль его строк при живом источнике означает
	// смену идентификатора или формата DBF: писать пустоту молча нельзя, иначе
	// regnum 1481 перестал бы пополняться незаметно (стандарт §6.4).
	sber := make([]form101Row, 0, len(rows))
	for _, row := range rows {
		if row.Regnum == cbrForm101SberRegnum {
			sber = append(sber, row)
		}
	}
	if len(sber) == 0 {
		return 0, fmt.Errorf("%s: в %s нет строк REGN=%d (всего строк %d) — сменился regnum или формат DBF",
			cbrForm101Table, link, cbrForm101SberRegnum, len(rows))
	}

	// 7. Один батч на прогон: PrepareBatch → Append (err проверяется) → Send.
	// При ошибке Append батч закрывается Abort, иначе соединение с открытым
	// батчем не вернулось бы в пул драйвера (прецедент — util.abortBatch).
	batch, err := conn.PrepareBatch(ctx, cbrForm101Insert)
	if err != nil {
		return 0, fmt.Errorf("%s: батч: %w", cbrForm101Table, err)
	}
	for i, row := range sber {
		if err = batch.Append(latest, row.Regnum, row.Account, row.Plan, row.ActivePassive, row.BalanceTotal); err != nil {
			if abortErr := batch.Abort(); abortErr != nil {
				log.Warnf("%s: не удалось закрыть батч после ошибки: %v", cbrForm101Table, abortErr)
			}
			return 0, fmt.Errorf("%s: запись %d (REGN=%d, счёт %q): %w",
				cbrForm101Table, i, row.Regnum, row.Account, err)
		}
	}
	count = int64(batch.Rows())
	if err = batch.Send(); err != nil {
		return count, fmt.Errorf("%s: отправка батча: %w", cbrForm101Table, err)
	}
	log.Infof("Imported %d rows of cbr_form101 for %s", count, latest.Format(form101DayLayout))
	return count, nil
}

// cbrForm101Importer — регистрация импортёра: Name() = TableName = cbr_form101,
// DDL, объявление точки контроля публикации (запрос совпадает с колонкой
// countPoints реестра — это сверяет тест-энфорсер) и свой Import. Каталог
// series_catalog не публикуется: это сырые строки счетов, а не series; метрики
// появятся на следующем шаге (спека §4.8). Сетевых вызовов в init() нет.
var cbrForm101Importer = &cbrForm101Stat{ClickHouseImport: util.ClickHouseImport{
	TableName:              cbrForm101Table,
	CreateTable:            []string{cbrForm101Ddl},
	PublicationCountPoints: cbrForm101CountPoints,
}}
