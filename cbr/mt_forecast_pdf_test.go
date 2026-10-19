package cbr

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

// Разбор PDF-версии «Табл. 1» (импортёр cbr_mt_forecast, спека §3.5): корпус — дословный вывод
// `pdftotext -layout -enc UTF-8 -nopgbrk` живых выпусков ЦБ (стандарт §11.1), 9 комментариев
// (страница 2; у выпуска 27.02.2024 — страницы 1–2: якорь ищется после страницы без таблицы) и
// 11 «Среднесрочных прогнозов» (страница 1). Сеть и `pdftotext` в тестах не вызываются: парсер
// получает уже извлечённый текст, поэтому CI без poppler остаётся зелёным (спека §3.3, §7).
//
// Ожидания выписаны литералами, а не собранными из констант пакета: тест, сверяющий годы с
// mtForecastYear или подписи с реестром, прошёл бы и после правки нормализации — то есть ровно
// на той правке, от которой обязан падать (стандарт §8.5). Число точек (16 показателей × годы),
// набор показателей и spot-значения key_rate_avg — контракт корпуса: 1104 точки = 496 новых
// точек комментариев + 608 точек прогнозов (спека §2).

// mtForecastPDFFixtureDir — каталог корпуса PDF-фикстур.
const mtForecastPDFFixtureDir = "testdata/mt_forecast_pdf/"

// mtForecastPDFFixture — ожидания разбора одного PDF-выпуска: файл фикстуры, годы шапки (первый
// и последний используются в spot-проверке, полный список — контракт горизонта), число точек и
// границы key_rate_avg за первый и последний прогнозный год.
type mtForecastPDFFixture struct {
	File     string
	Years    []uint16
	Points   int
	KeyFirst [2]float64
	KeyLast  [2]float64
}

// mtForecastPDFFixtures — корпус 20 выпусков (спека §2): годы, число точек и spot-значения
// key_rate_avg — из живой разведки 2026-10-19. Комментарий и прогноз одного заседания дают одни
// и те же значения (различия только типографика), поэтому spot-значения пар совпадают.
var mtForecastPDFFixtures = []mtForecastPDFFixture{
	{
		File:     "comment_27022024_p1p2.txt",
		Years:    []uint16{2024, 2025, 2026},
		Points:   48,
		KeyFirst: [2]float64{13.5, 15.5},
		KeyLast:  [2]float64{6, 7},
	},
	{
		File:     "comment_13052024.txt",
		Years:    []uint16{2024, 2025, 2026},
		Points:   48,
		KeyFirst: [2]float64{15, 16},
		KeyLast:  [2]float64{6, 7},
	},
	{
		File:     "comment_07082024.txt",
		Years:    []uint16{2024, 2025, 2026, 2027},
		Points:   64,
		KeyFirst: [2]float64{16.9, 17.4},
		KeyLast:  [2]float64{7.5, 8.5},
	},
	{
		File:     "comment_06112024.txt",
		Years:    []uint16{2024, 2025, 2026, 2027},
		Points:   64,
		KeyFirst: [2]float64{17.5, 17.5},
		KeyLast:  [2]float64{7.5, 8.5},
	},
	{
		File:     "comment_26022025.txt",
		Years:    []uint16{2025, 2026, 2027},
		Points:   48,
		KeyFirst: [2]float64{19, 22},
		KeyLast:  [2]float64{7.5, 8.5},
	},
	{
		File:     "comment_12052025.txt",
		Years:    []uint16{2025, 2026, 2027},
		Points:   48,
		KeyFirst: [2]float64{19.5, 21.5},
		KeyLast:  [2]float64{7.5, 8.5},
	},
	{
		File:     "comment_06082025.txt",
		Years:    []uint16{2025, 2026, 2027, 2028},
		Points:   64,
		KeyFirst: [2]float64{18.8, 19.6},
		KeyLast:  [2]float64{7.5, 8.5},
	},
	{
		File:     "comment_06112025.txt",
		Years:    []uint16{2025, 2026, 2027, 2028},
		Points:   64,
		KeyFirst: [2]float64{19.2, 19.2},
		KeyLast:  [2]float64{7.5, 8.5},
	},
	{
		File:     "comment_26022026.txt",
		Years:    []uint16{2026, 2027, 2028},
		Points:   48,
		KeyFirst: [2]float64{13.5, 14.5},
		KeyLast:  [2]float64{7.5, 8.5},
	},
	{
		File:     "forecast_240216.txt",
		Years:    []uint16{2024, 2025, 2026},
		Points:   48,
		KeyFirst: [2]float64{13.5, 15.5},
		KeyLast:  [2]float64{6, 7},
	},
	{
		File:     "forecast_240426.txt",
		Years:    []uint16{2024, 2025, 2026},
		Points:   48,
		KeyFirst: [2]float64{15, 16},
		KeyLast:  [2]float64{6, 7},
	},
	{
		File:     "forecast_240726.txt",
		Years:    []uint16{2024, 2025, 2026, 2027},
		Points:   64,
		KeyFirst: [2]float64{16.9, 17.4},
		KeyLast:  [2]float64{7.5, 8.5},
	},
	{
		File:     "forecast_241025.txt",
		Years:    []uint16{2024, 2025, 2026, 2027},
		Points:   64,
		KeyFirst: [2]float64{17.5, 17.5},
		KeyLast:  [2]float64{7.5, 8.5},
	},
	{
		File:     "forecast_250214.txt",
		Years:    []uint16{2025, 2026, 2027},
		Points:   48,
		KeyFirst: [2]float64{19, 22},
		KeyLast:  [2]float64{7.5, 8.5},
	},
	{
		File:     "forecast_250425.txt",
		Years:    []uint16{2025, 2026, 2027},
		Points:   48,
		KeyFirst: [2]float64{19.5, 21.5},
		KeyLast:  [2]float64{7.5, 8.5},
	},
	{
		File:     "forecast_250725.txt",
		Years:    []uint16{2025, 2026, 2027, 2028},
		Points:   64,
		KeyFirst: [2]float64{18.8, 19.6},
		KeyLast:  [2]float64{7.5, 8.5},
	},
	{
		File:     "forecast_251024.txt",
		Years:    []uint16{2025, 2026, 2027, 2028},
		Points:   64,
		KeyFirst: [2]float64{19.2, 19.2},
		KeyLast:  [2]float64{7.5, 8.5},
	},
	{
		File:     "forecast_260213.txt",
		Years:    []uint16{2026, 2027, 2028},
		Points:   48,
		KeyFirst: [2]float64{13.5, 14.5},
		KeyLast:  [2]float64{7.5, 8.5},
	},
	{
		File:     "forecast_260424.txt",
		Years:    []uint16{2026, 2027, 2028},
		Points:   48,
		KeyFirst: [2]float64{14, 14.5},
		KeyLast:  [2]float64{7.5, 8.5},
	},
	{
		File:     "forecast_260724.txt",
		Years:    []uint16{2026, 2027, 2028, 2029},
		Points:   64,
		KeyFirst: [2]float64{14.5, 14.6},
		KeyLast:  [2]float64{7.5, 8.5},
	},
}

// TestMtForecastParsePDFCorpus — корпусный контракт разбора: каждый из 20 выпусков даёт ожидаемое
// число точек, все 16 показателей реестра и spot-значения key_rate_avg за первый и последний
// прогнозный год; сумма по корпусу — 1104 точки (496 комментариев + 608 прогнозов, спека §2).
// Сумма ловит потерянный или лишний выпуск даже тогда, когда каждый файл по отдельности прошёл.
func TestMtForecastParsePDFCorpus(t *testing.T) {
	total := 0
	for _, tt := range mtForecastPDFFixtures {
		text := mtForecastFixture(t, mtForecastPDFFixtureDir+tt.File)
		points, err := mtForecastParsePDF(text)
		if err != nil {
			t.Fatalf("%s: %v", tt.File, err)
		}
		if len(points) != tt.Points {
			t.Fatalf("%s: точек %d, want %d", tt.File, len(points), tt.Points)
		}
		seen := map[string]bool{}
		for _, p := range points {
			seen[p.Indicator] = true
		}
		if len(seen) != 16 {
			t.Errorf("%s: показателей %d, want 16", tt.File, len(seen))
		}
		key := map[uint16]mtForecastPoint{}
		for _, p := range points {
			if p.Indicator == "key_rate_avg" {
				key[p.Year] = p
			}
		}
		first, last := tt.Years[0], tt.Years[len(tt.Years)-1]
		if got := key[first]; got.Lo != tt.KeyFirst[0] || got.Hi != tt.KeyFirst[1] {
			t.Errorf("%s: key_rate_avg %d = %v..%v, want %v", tt.File, first, got.Lo, got.Hi, tt.KeyFirst)
		}
		if got := key[last]; got.Lo != tt.KeyLast[0] || got.Hi != tt.KeyLast[1] {
			t.Errorf("%s: key_rate_avg %d = %v..%v, want %v", tt.File, last, got.Lo, got.Hi, tt.KeyLast)
		}
		total += len(points)
	}
	if total != 1104 {
		t.Fatalf("точек в корпусе %d, want 1104 (496 комментариев + 608 прогнозов)", total)
	}
}

// mtForecastPDFWithoutLine — копия текста без строк, содержащих подстроку: так проверяются
// fail-fast и warn-ветки на живой фикстуре, а не на синтетическом тексте (стандарт §11.1).
// Файл на диске не меняется, фильтрация идёт по копии в памяти.
func mtForecastPDFWithoutLine(t *testing.T, text []byte, contains string) []byte {
	t.Helper()
	lines := strings.Split(string(text), "\n")
	kept := make([]string, 0, len(lines))
	removed := 0
	for _, line := range lines {
		if strings.Contains(line, contains) {
			removed++
			continue
		}
		kept = append(kept, line)
	}
	if removed == 0 {
		t.Fatalf("в фикстуре нет строки %q: мутация не применена", contains)
	}
	return []byte(strings.Join(kept, "\n"))
}

// mtForecastPDFReplace — копия текста с первой заменой подстроки: подмена должна сработать ровно
// один раз, иначе тест проверял бы не тот участок таблицы (у выпуска две таблицы с одинаковым
// «Источник: Банк России»).
func mtForecastPDFReplace(t *testing.T, text []byte, old, replacement string) []byte {
	t.Helper()
	if bytes.Count(text, []byte(old)) == 0 {
		t.Fatalf("в фикстуре нет подстроки %q: мутация не применена", old)
	}
	return bytes.Replace(text, []byte(old), []byte(replacement), 1)
}

// mtForecastPDFAppendToLine — копия текста, в которой к первой строке с подстрокой дописано
// продолжение: так в строку добавляется лишняя колонка значения, не трогая остальные строки.
func mtForecastPDFAppendToLine(t *testing.T, text []byte, contains, suffix string) []byte {
	t.Helper()
	lines := strings.Split(string(text), "\n")
	for i, line := range lines {
		if strings.Contains(line, contains) {
			lines[i] = line + suffix
			return []byte(strings.Join(lines, "\n"))
		}
	}
	t.Fatalf("в фикстуре нет строки %q: мутация не применена", contains)
	return nil
}

// TestMtForecastParsePDFFailFast — ветки, где разбор обязан упасть с названием причины (стандарт
// §6.4, спека §3.5): пустой текст, нет якоря, нет строки годов, неверное число значений, пропажа
// core-показателя. Ошибка обязана называть причину так, чтобы оператор понял, что сломалось в
// вёрстке, а не искал её по «0 points».
func TestMtForecastParsePDFFailFast(t *testing.T) {
	fixture := mtForecastFixture(t, mtForecastPDFFixtureDir+"forecast_260724.txt")

	t.Run("Пустой текст", func(t *testing.T) {
		_, err := mtForecastParsePDF(nil)
		if err == nil {
			t.Fatal("пустой текст: ожидалась ошибка, а разбор вернул пустой список как успех")
		}
		if !strings.Contains(err.Error(), "якорь") {
			t.Errorf("ошибка %q не называет якорь: непонятно, что искать в PDF", err)
		}
	})

	t.Run("Нет строки годов", func(t *testing.T) {
		// Якорь и одна строка показателя без шапки годов: разбор не знает прогнозных колонок и
		// обязан сказать об этом, а не принять значения за факт.
		text := []byte("ОСНОВНЫЕ ПАРАМЕТРЫ ПРОГНОЗА БАНКА РОССИИ\n" +
			"Инфляция, в %, декабрь к декабрю предыдущего года     5,0     6,0\n")
		_, err := mtForecastParsePDF(text)
		if err == nil {
			t.Fatal("текст без строки годов: ожидалась ошибка, а разбор вернул точки")
		}
		if !strings.Contains(strings.ToLower(err.Error()), "год") {
			t.Errorf("ошибка %q не называет годы шапки: причина не видна", err)
		}
	})

	t.Run("Лишнее значение в ячейке", func(t *testing.T) {
		// Ячейка с тремя числами: mtForecastBounds обязан отказать, а ошибка — назвать строку, в
		// которой это случилось (иначе в 16 строках её не найти).
		text := mtForecastPDFReplace(t, fixture, "14,5–14,61", "13,5 – 14,5 – 15,0")
		_, err := mtForecastParsePDF(text)
		if err == nil {
			t.Fatal("ячейка с тремя числами: ожидалась ошибка, а значение разобрано")
		}
		if !strings.Contains(err.Error(), "Ключевая ставка, в среднем за год, в % годовых") {
			t.Errorf("ошибка %q не называет подпись строки: непонятно, какую ячейку править", err)
		}
	})

	t.Run("Лишняя колонка значений", func(t *testing.T) {
		// В строке 6 значений при шапке из 4 лет: ошибка числа значений (спека §3.5, п. 6) обязана
		// назвать подпись и оба числа, иначе расхождение вёрстки выглядит как сдвиг колонок.
		text := mtForecastPDFAppendToLine(t, fixture, "Ключевая ставка, в среднем за год", "     1,0")
		_, err := mtForecastParsePDF(text)
		if err == nil {
			t.Fatal("лишняя колонка значений: ожидалась ошибка, а лишнее значение молча отброшено")
		}
		if !strings.Contains(err.Error(), "Ключевая ставка, в среднем за год, в % годовых") {
			t.Errorf("ошибка %q не называет подпись строки: причина не видна", err)
		}
	})

	t.Run("Нет core-строки", func(t *testing.T) {
		// Пропажа key_rate_avg — ошибка, а не предупреждение: ради неё импортёр и заведён (спека §3.2).
		text := mtForecastPDFWithoutLine(t, fixture, "Ключевая ставка, в среднем за год")
		_, err := mtForecastParsePDF(text)
		if err == nil {
			t.Fatal("текст без core-строки: ожидалась ошибка, а разбор прошёл")
		}
		if !strings.Contains(err.Error(), "key_rate_avg") {
			t.Errorf("ошибка %q не называет код пропавшего показателя: непонятно, какой ряд потерян", err)
		}
	})
}

// TestMtForecastParsePDFWarns — «предупредить и продолжить» (стандарт §6.6, спека §3.5): незнакомая
// подпись и пропажа не-core показателя не валят разбор, но обязаны быть видны в логе. Тест
// проверяет и сообщение, и что точки остальных строк собраны: молчаливое продолжение скрыло бы
// дрейф подписей источника до следующего выпуска.
func TestMtForecastParsePDFWarns(t *testing.T) {
	fixture := mtForecastFixture(t, mtForecastPDFFixtureDir+"forecast_260724.txt")

	t.Run("Пропажа не-core строки", func(t *testing.T) {
		hook := mtForecastWarnHook(t)
		text := mtForecastPDFWithoutLine(t, fixture, "ипотечные жилищные кредиты")
		points, err := mtForecastParsePDF(text)
		if err != nil {
			t.Fatalf("пропажа не-core строки: разбор упал: %v", err)
		}
		if want := 15 * 4; len(points) != want {
			t.Errorf("точек %d, want %d (15 оставшихся показателей × 4 года)", len(points), want)
		}
		mtForecastAssertWarn(t, hook, "bank_claims_mortgage")
	})

	t.Run("Неизвестная подпись", func(t *testing.T) {
		hook := mtForecastWarnHook(t)
		const unknown = "Неизвестная строка источника"
		const stop = "Источник: Банк России"
		text := mtForecastPDFReplace(t, fixture, stop,
			unknown+"     1,0     2,0     3,0     4,0     5,0\n"+stop)
		points, err := mtForecastParsePDF(text)
		if err != nil {
			t.Fatalf("незнакомая подпись: разбор упал: %v", err)
		}
		if want := 16 * 4; len(points) != want {
			t.Errorf("точек %d, want %d: незнакомая строка не должна съедать остальные", len(points), want)
		}
		mtForecastAssertWarn(t, hook, unknown)
	})
}

// TestMtForecastNormalizeLabelPDF — PDF-форма подписи (спека §3.6): номер сноски приезжает
// надстрочной цифрой («…валюте³», он же через пробел), а подпункт «к населению» в PDF заканчивается
// двоеточием. Обе формы обязаны дать подпись реестра — иначе строка выпадет из разбора молча.
// xlsx-форма (обычная цифра сноски, без двоеточия) закреплена TestMtForecastNormalizeLabel.
func TestMtForecastNormalizeLabelPDF(t *testing.T) {
	for name, tc := range map[string]struct{ raw, want string }{
		"надстрочная сноска": {
			"Требования банковской системы к экономике в рублях и иностранной валюте³",
			"Требования банковской системы к экономике в рублях и иностранной валюте",
		},
		"надстрочная сноска через пробел": {
			"Требования банковской системы к экономике в рублях и иностранной валюте ³",
			"Требования банковской системы к экономике в рублях и иностранной валюте",
		},
		"хвостовое двоеточие": {
			"– к населению, в том числе:",
			"- к населению, в том числе",
		},
		"двоеточие и надстрочная сноска": {
			"– к населению, в том числе¹:",
			"- к населению, в том числе",
		},
	} {
		t.Run(name, func(t *testing.T) {
			if got := mtForecastNormalizeLabel(tc.raw); got != tc.want {
				t.Errorf("mtForecastNormalizeLabel(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

// TestMtForecastPDFText — извлечение текста выпуска (спека §3.3): проверка сигнатуры `%PDF-`
// стоит до `exec`, а отсутствующий бинарник назван в ошибке вместе с подсказкой `poppler-utils`.
// Настоящий `pdftotext` тесты не вызывают — имя утилиты подменено заведомо отсутствующим, поэтому
// прогон офлайн и зелёный в CI без poppler (спека §3.3, §7). Подмена возвращается в t.Cleanup:
// глобальная переменная пережила бы тест и подменила утилиту соседним тестам пакета.
func TestMtForecastPDFText(t *testing.T) {
	previous := mtForecastPDFTool
	mtForecastPDFTool = "definitely-not-pdftotext"
	t.Cleanup(func() { mtForecastPDFTool = previous })

	t.Run("HTML вместо PDF", func(t *testing.T) {
		// WAF отдаёт заглушку с кодом 200: извлекатель обязан отказать по сигнатуре, не запуская
		// pdftotext. Если проверка заголовка регрессирует, exec дойдёт до подменённого имени и в
		// ошибке появится подсказка poppler-utils — её наличие и есть доказательство запуска.
		body := []byte("<!DOCTYPE html><html><body>403 WAF</body></html>")
		_, err := mtForecastPDFText(context.Background(), body)
		if err == nil {
			t.Fatal("HTML-байты: ожидалась ошибка, а извлекатель принял их за PDF")
		}
		if !strings.Contains(err.Error(), "%PDF-") {
			t.Errorf("ошибка %q не называет ожидаемый заголовок %%PDF-: непонятно, что пришло вместо файла", err)
		}
		if strings.Contains(err.Error(), "poppler-utils") {
			t.Errorf("ошибка %q подсказывает poppler-utils: извлекатель запустил exec на не-PDF байтах", err)
		}
	})

	t.Run("Нет pdftotext", func(t *testing.T) {
		// Валидный заголовок снимает проверку сигнатуры, и следующий шаг — запуск утилиты.
		body := []byte("%PDF-1.7\n% тестовое тело\n%%EOF\n")
		_, err := mtForecastPDFText(context.Background(), body)
		if err == nil {
			t.Fatal("отсутствующий pdftotext: ожидалась ошибка, а извлечение прошло")
		}
		if !strings.Contains(err.Error(), mtForecastPDFTool) {
			t.Errorf("ошибка %q не называет утилиту %q: непонятно, какого бинарника нет", err, mtForecastPDFTool)
		}
		if !strings.Contains(err.Error(), "poppler-utils") {
			t.Errorf("ошибка %q не подсказывает пакет poppler-utils: оператор не узнает, что установить", err)
		}
	})
}
