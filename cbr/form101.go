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
// cbr_form101 — сырые строки «счёт → остаток»; поверх них cbr-809p-sber-1 маппинг
// (form101LoanTotals) пишет ras-метрики loans_corporations/loans_individuals
// в company_financials — с августа 2026 из текущего выпуска, июль 2026 — сид
// sql/sber_ras_form101_mapped_2026_07.sql (issue #98, спека docs/superpowers/
// specs/2026-10-17-cbr-form101-design.md §7). Артефакт — не XLSX, а RAR с DBF,
// поэтому у импортёра свой Import() (прецедент — cbr_dip_bulletin): шаблон
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

	// cbrForm101CompanyTable / cbrForm101MappedInsert — производные ras-метрики
	// (cbr-809p-sber-1) в общую company_financials. Пишем только если таблица уже есть:
	// в цепочке ЦБ (09:17) она создаётся банковским пакетом (понедельник 09:23),
	// и на чистой БД первый прогон ЦБ может её не застать — тогда пропуск с warn,
	// а следующий прогон запишет. DDL здесь не дублируется намеренно.
	cbrForm101CompanyTable = "company_financials"
	cbrForm101MappedInsert = "INSERT INTO company_financials " +
		"(company, metric, period, period_type, source_kind, value, unit, source_url, source_page)"
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
		"первый срез — ПАО Сбербанк (regnum=1481); отчётная дата — из имени файла. account — код или группа " +
		"счёта источника (NUM_SC: 408, 45.0, ITGAP и т. п.), plan — план счетов (А/Б/В/Г), active_passive — " +
		"признак активно/пассивного счёта (A_P), balance_total — итоговый остаток VITG в тыс. руб. как в " +
		"источнике (не пересчитан в рубли и не сверен с пресс-релизом Сбера). report_date — дата отчёта из " +
		"имени файла 101-YYYYMMDD.rar, а не месяц публикации. Ключ таблицы включает plan, поэтому все " +
		"строки Сбера (включая 8 строк группы ITGAP четырёх планов А/Б/В/Г) сохраняются. " +
		"Это не готовая метрика кредитования: cbr-809p-sber-1 маппинг счетов (44x/45x, группы 45.0/45.2) на " +
		"кредиты юрлицам и физлицам показывает отдельная витрина v_cbr_form101_loans, а те же значения " +
		"импортёр пишет ras в company_financials; поэтому не складывать строки этой витрины и не " +
		"сравнивать их с loans_corporations/loans_individuals без явного отбора счетов",
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

// cbrForm101LoansView — витрина маппинга формы 101 на кредиты ЮЛ/ФЛ по
// методологии ЦБ (issue #103; research docs/research/2026-10-17-cbr-form101-
// mapping-methodology.md). Набор счетов — cbr-809p-sber-1: 45.0 (452+456) +
// активные 441–451,453,454 + прочие размещённые средства 460–470,472,47.1,477;
// 45.2 (455+457) — ФЛ; 458 (просроченный основной долг) распределяется
// пропорционально портфелям, 459 (проценты) и пассивные 45.1/45.2 (резервы)
// исключены. Ряд — «до резервов», как в пресс-релизе Сбера.
var cbrForm101LoansView = util.View{
	Name:   "v_cbr_form101_loans",
	Tables: []string{cbrForm101Table, "company_financials"},
	Select: `WITH f AS (
    SELECT report_date, regnum,
           round(sumIf(balance_total, active_passive = '1' AND account IN
               ('45.0', '441', '442', '443', '444', '445', '446', '447', '448', '449', '450', '451', '453', '454',
                '460', '461', '462', '463', '464', '465', '466', '467', '468', '469', '470', '472', '47.1', '477')) / 1e6, 6)
         AS corporate_performing_bln_rub,
           round(sumIf(balance_total, active_passive = '1' AND account = '45.2') / 1e6, 6) AS retail_performing_bln_rub,
           round(sumIf(balance_total, active_passive = '1' AND account = '458') / 1e6, 6) AS overdue_bln_rub,
           round(sumIf(balance_total, active_passive = '1' AND account = '459') / 1e6, 6) AS overdue_interest_bln_rub
    FROM cbr_form101 FINAL
    WHERE regnum = 1481
    GROUP BY report_date, regnum
),
m AS (
    SELECT *,
           corporate_performing_bln_rub / (corporate_performing_bln_rub + retail_performing_bln_rub) AS corporate_share,
           round(corporate_performing_bln_rub
               + overdue_bln_rub * corporate_performing_bln_rub
                 / (corporate_performing_bln_rub + retail_performing_bln_rub), 2) AS corporate_loans_bln_rub,
           round(retail_performing_bln_rub
               + overdue_bln_rub * retail_performing_bln_rub
                 / (corporate_performing_bln_rub + retail_performing_bln_rub), 2) AS retail_loans_bln_rub
    FROM f
),
pr AS (
    SELECT toDate(concat(substring(period, 1, 4), '-', substring(period, 6, 2), '-01')) AS report_month,
           maxIf(value, metric = 'loans_corporations') AS corporate,
           maxIf(value, metric = 'loans_individuals') AS retail
    FROM company_financials FINAL
    WHERE company = 'SBER' AND period_type = 'M' AND source_kind = 'kpi'
    GROUP BY report_month
)
SELECT m.report_date, m.regnum, m.corporate_loans_bln_rub, m.retail_loans_bln_rub,
       m.overdue_bln_rub, m.overdue_interest_bln_rub,
       pr.corporate AS press_release_corporate_bln_rub,
       pr.retail AS press_release_retail_bln_rub,
       if(pr.corporate = 0, NULL, round(m.corporate_loans_bln_rub / pr.corporate - 1, 4)) AS corporate_diff_pct,
       if(pr.retail = 0, NULL, round(m.retail_loans_bln_rub / pr.retail - 1, 4)) AS retail_diff_pct,
       'cbr-809p-sber-1' AS mapping_version
FROM m
LEFT JOIN pr ON pr.report_month = subtractMonths(m.report_date, 1)
ORDER BY m.report_date DESC`,
	Comment: "Маппинг формы 101 на кредиты ЮЛ/ФЛ по методологии ЦБ (issue #103): 45.0 (452+456) + активные " +
		"441–451,453,454 + прочие размещённые средства 460–470,472,47.1,477; ФЛ — 45.2 (455+457); 458 " +
		"(просроченный основной долг) распределён пропорционально портфелям, 459 (проценты) и пассивные " +
		"45.1/45.2 (резервы) исключены. mapping_version='cbr-809p-sber-1'; калибровка: июль 31 756,3 / " +
		"19 963,3 против 31 771 / 19 942 (−0,05 % / +0,11 %), август 32 455,7 / 20 075,2 против 32 512 / " +
		"20 200 (−0,17 % / −0,62 %). Ряд — до резервов, как в пресс-релизе Сбера",
	Columns: map[string]string{
		"report_date":                     "Дата отчёта формы 101 (из имени файла), для 2026-09-01 — остатки на конец августа",
		"regnum":                          "Регистрационный номер банка (REGN); первый срез — 1481 (ПАО Сбербанк)",
		"corporate_loans_bln_rub":         "Кредиты ЮЛ, млрд ₽: 45.0 (A_P=1, = 452+456) + активные 441–451,453,454 + 460–470,472,47.1,477 + доля 458 (A_P=1) пропорционально портфелям; резервы (45.1), 459 и 474+ исключены",
		"retail_loans_bln_rub":            "Кредиты ФЛ, млрд ₽: 45.2 (A_P=1, = 455+457) + доля 458 (A_P=1) пропорционально портфелям; резервы (пассивный 45.2) исключены",
		"overdue_bln_rub":                 "Просроченный основной долг 458 (A_P=1), млрд ₽; распределён между ЮЛ/ФЛ пропорционально текущим портфелям — точного разреза в форме 101 нет",
		"overdue_interest_bln_rub":        "Просроченные проценты 459 (A_P=1), млрд ₽; в кредиты не входят",
		"press_release_corporate_bln_rub": "Эталон пресс-релиза Сбера за тот же отчётный месяц (loans_corporations, source_kind='kpi'), млрд ₽; NULL — сида за месяц нет",
		"press_release_retail_bln_rub":    "Эталон пресс-релиза Сбера за тот же отчётный месяц (loans_individuals, source_kind='kpi'), млрд ₽; NULL — сида за месяц нет",
		"corporate_diff_pct":              "Разница кредитов ЮЛ с пресс-релизом как доля (−0,0017 = −0,17 %); NULL — эталона нет",
		"retail_diff_pct":                 "Разница кредитов ФЛ с пресс-релизом как доля (−0,0062 = −0,62 %); NULL — эталона нет",
		"mapping_version":                 "Версия набора счетов: 'cbr-809p-sber-1' — 809-П и формат DBF 0409101; смена набора — новая версия, чтобы сравнение между прогонами не смешивалось",
	},
}

// cbrForm101CorporateDirect — детальные счета кредитов ЮЛ, которые не входят
// в агрегат 45.0 (45.0 = 452 + 456 по формату DBF 0409101). 452/456 поэтому
// здесь отсутствуют: иначе двойной счёт. 454 — кредиты индивидуальным
// предпринимателям; по порядку 0409115 ИП классифицируются как ЮЛ.
var cbrForm101CorporateDirect = map[string]bool{
	"441": true, "442": true, "443": true, "444": true, "445": true,
	"446": true, "447": true, "448": true, "449": true, "450": true,
	"451": true, "453": true, "454": true,
}

// cbrForm101CorporateOther — «прочие размещённые средства» (460–473) и лизинг
// (477). По 809-П это не «кредиты предоставленные», но калибровка показывает,
// что Сбер включает их во внутренний корпоративный портфель: без них ряд
// систематически ниже пресс-релиза на ~2,7 %, с ними — в пределах 0,2 %.
// 47.1 — агрегат 471 + 473 по формату DBF; 471/473 отдельно не прибавляются.
var cbrForm101CorporateOther = map[string]bool{
	"460": true, "461": true, "462": true, "463": true, "464": true,
	"465": true, "466": true, "467": true, "468": true, "469": true,
	"470": true, "472": true, "47.1": true, "477": true,
}

// form101LoanTotals — маппинг строк Сбера в кредиты ЮЛ/ФЛ по методологии ЦБ
// (809-П + формат DBF 0409101), млрд ₽. Остатки — тыс. руб., поэтому /1e6.
// Ряд «до резервов»: пассивные 45.1/45.2 (резервы), 459 (проценты), 474+
// (расчёты) и 478 (приобретённые права) не входят. 458 — просроченный основной
// долг; его разреза по ЮЛ/ФЛ в форме 101 нет, поэтому он распределяется
// пропорционально текущим портфелям (допущение, см. research-документ).
func form101LoanTotals(rows []form101Row) (corporate, retail float64) {
	var corporatePerforming, retailPerforming, overdue float64
	for _, row := range rows {
		if row.ActivePassive != "1" {
			continue
		}
		switch {
		case row.Account == "45.0",
			cbrForm101CorporateDirect[row.Account],
			cbrForm101CorporateOther[row.Account]:
			corporatePerforming += row.BalanceTotal
		case row.Account == "45.2":
			retailPerforming += row.BalanceTotal
		case row.Account == "458":
			overdue += row.BalanceTotal
		}
	}
	total := corporatePerforming + retailPerforming
	if total == 0 {
		return 0, 0
	}
	corporate = (corporatePerforming + overdue*corporatePerforming/total) / 1e6
	retail = (retailPerforming + overdue*retailPerforming/total) / 1e6
	return corporate, retail
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
	// Pilot-витрина маппинга гейтится по v_company_financials: пока её нет,
	// util.CreateView вернёт created=false без ошибки, и следующий прогон
	// (или company_views) создаст её. Это не повод ронять импорт сырья.
	if _, err = util.CreateView(ctx, conn, cbrForm101LoansView); err != nil {
		return 0, fmt.Errorf("%s: витрина %s: %w", cbrForm101Table, cbrForm101LoansView.Name, err)
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
		// Производные ras-метрики пересобираются и на no-op-пути: raw-строки уже
		// лежат в таблице, а company_financials могла появиться после первого
		// прогона (или её запись могла не удаться). Читаем сохранённые строки
		// Сбера и пишем те же cbr-809p-sber-1 значения; сеть не нужна.
		stored, storedErr := form101StoredSber(ctx, conn, latest)
		if storedErr != nil {
			return 0, storedErr
		}
		if err = writeForm101Loans(ctx, conn, latest, link, stored); err != nil {
			return 0, err
		}
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

	// 8. Производные ras-метрики cbr-809p-sber-1: отчётная дата формы — первое число
	// следующего месяца, поэтому период company_financials — предыдущий месяц
	// (2026-09-01 → 2026M08). Набор счетов — тот же, что в v_cbr_form101_loans.
	if err = writeForm101Loans(ctx, conn, latest, link, sber); err != nil {
		return count, err
	}

	return count, nil
}

// form101StoredSber читает уже записанные строки Сбера за отчётную дату —
// для no-op-пути, когда ras-метрики нужно пересобрать без скачивания RAR.
func form101StoredSber(ctx context.Context, conn driver.Conn, reportDate time.Time) ([]form101Row, error) {
	rows, err := conn.Query(ctx,
		"SELECT regnum, account, plan, active_passive, balance_total FROM cbr_form101 FINAL WHERE report_date = ? AND regnum = ?",
		reportDate, cbrForm101SberRegnum)
	if err != nil {
		return nil, fmt.Errorf("%s: чтение сохранённых строк за %s: %w", cbrForm101Table, reportDate.Format(form101DayLayout), err)
	}
	defer func() { _ = rows.Close() }()

	var stored []form101Row
	for rows.Next() {
		var row form101Row
		if err = rows.Scan(&row.Regnum, &row.Account, &row.Plan, &row.ActivePassive, &row.BalanceTotal); err != nil {
			return nil, fmt.Errorf("%s: сохранённая строка за %s: %w", cbrForm101Table, reportDate.Format(form101DayLayout), err)
		}
		stored = append(stored, row)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: сохранённые строки за %s не дочитаны: %w", cbrForm101Table, reportDate.Format(form101DayLayout), err)
	}
	return stored, nil
}

// writeForm101Loans пишет cbr-809p-sber-1 ras-метрики в company_financials. Вызывается
// и после импорта raw-строк, и на no-op-пути (пересборка производных). Если
// company_financials в БД ещё нет — warn и nil: raw-импорт не должен падать из-за
// отсутствия таблицы банковского пакета (следующий прогон после неё запишет).
func writeForm101Loans(ctx context.Context, conn driver.Conn, reportDate time.Time, link string, sber []form101Row) error {
	corporate, retail := form101LoanTotals(sber)
	if corporate <= 0 || retail <= 0 {
		return fmt.Errorf("%s: маппинг cbr-809p-sber-1 дал ЮЛ=%v, ФЛ=%v — набор счетов разошёлся",
			cbrForm101Table, corporate, retail)
	}
	var hasCompany uint64
	if err := conn.QueryRow(ctx,
		"SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = ?",
		cbrForm101CompanyTable).Scan(&hasCompany); err != nil {
		return fmt.Errorf("%s: проверка %s: %w", cbrForm101Table, cbrForm101CompanyTable, err)
	}
	if hasCompany == 0 {
		log.Warnf("%s: таблицы %s нет — ras-метрики SBER за %s не записаны (следующий прогон после bank_company_metrics)",
			cbrForm101Table, cbrForm101CompanyTable, reportDate.AddDate(0, -1, 0).Format("2006M01"))
		return nil
	}
	period := reportDate.AddDate(0, -1, 0).Format("2006M01")
	mapped, err := conn.PrepareBatch(ctx, cbrForm101MappedInsert)
	if err != nil {
		return fmt.Errorf("%s: батч %s: %w", cbrForm101Table, cbrForm101CompanyTable, err)
	}
	for _, metric := range []struct {
		name  string
		value float64
	}{
		{"loans_corporations", corporate},
		{"loans_individuals", retail},
	} {
		if err = mapped.Append("SBER", metric.name, period, "M", "ras", metric.value, "млрд ₽", link, uint16(0)); err != nil {
			if abortErr := mapped.Abort(); abortErr != nil {
				log.Warnf("%s: не удалось закрыть батч %s после ошибки: %v", cbrForm101Table, cbrForm101CompanyTable, abortErr)
			}
			return fmt.Errorf("%s: %s %s за %s: %w", cbrForm101Table, "SBER", metric.name, period, err)
		}
	}
	if err = mapped.Send(); err != nil {
		return fmt.Errorf("%s: отправка %s: %w", cbrForm101Table, cbrForm101CompanyTable, err)
	}
	log.Infof("Mapped SBER %s: loans_corporations=%.2f, loans_individuals=%.2f (ras, cbr-809p-sber-1)",
		period, corporate, retail)
	return nil
}

// cbrForm101Importer — регистрация импортёра: Name() = TableName = cbr_form101,
// DDL, объявление точки контроля публикации (запрос совпадает с колонкой
// countPoints реестра — это сверяет тест-энфорсер) и свой Import. Каталог
// series_catalog не публикуется: это сырые строки счетов, а не series; метрики
// кредитования идут ras в company_financials и описываются каталогом банковского
// пакета (bank_company_metrics). Сетевых вызовов в init() нет.
var cbrForm101Importer = &cbrForm101Stat{ClickHouseImport: util.ClickHouseImport{
	TableName:              cbrForm101Table,
	CreateTable:            []string{cbrForm101Ddl},
	PublicationCountPoints: cbrForm101CountPoints,
}}
