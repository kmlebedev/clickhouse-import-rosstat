package util

// Общее извлечение текста PDF через pdftotext (poppler-utils). Вынесено из импортёров,
// потому что один и тот же вызов повторялся в cbr_infl_exp_op (`-bbox` первой страницы)
// и нужен rosstat_ipc_weeks (`-layout` всего документа). Сеть и exec остаются снаружи
// шаблона: байты приходят срезом от util.ClickHouseImport, поэтому разбор офлайн.

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

const (
	// PDFHeaderSize — сколько первых байт проверяется на сигнатуру `%PDF-`: WAF отдаёт
	// HTML-заглушку с кодом 200, и проверка стоит до exec.
	PDFHeaderSize = 1024
	// PDFTimeout — потолок одного извлечения: PDF до ~1 МБ разбирается за доли секунды,
	// 2 минуты — запас на медленный хост, а не рабочее время.
	PDFTimeout = 2 * time.Minute
)

// PDFTool — утилита извлечения текста. Переменная, а не константа, ради теста
// отсутствующего бинарника.
var PDFTool = "pdftotext"

// PDFMode — формат вывода pdftotext.
type PDFMode int

const (
	// PDFLayout — весь текст с сохранением колонок (`-layout -nopgbrk`): на нём работает
	// разбор таблицы rosstat_ipc_weeks.
	PDFLayout PDFMode = iota
	// PDFBBoxFirstPage — слова первой страницы с координатами (`-bbox -f 1 -l 1`): на нём
	// работает разбор графика cbr_infl_exp_op.
	PDFBBoxFirstPage
)

// PDFText извлекает текст PDF. Fail-fast: в первых PDFHeaderSize байтах нет `%PDF-`
// (WAF/HTML-заглушка) — ошибка до exec; пустой stdout — тоже ошибка (скан без текстового
// слоя). ctx ограничен PDFTimeout, но родительский дедлайн сохраняется.
func PDFText(ctx context.Context, body []byte, mode PDFMode) ([]byte, error) {
	head := body[:min(len(body), PDFHeaderSize)]
	if !bytes.Contains(head, []byte("%PDF-")) {
		return nil, fmt.Errorf("%s: ответ не PDF (нет сигнатуры %%PDF-) — WAF/HTML вместо файла", PDFTool)
	}
	args, err := pdfArgs(mode)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, PDFTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, PDFTool, args...)
	cmd.Stdin = bytes.NewReader(body)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	output, err := cmd.Output()
	if err != nil {
		stderrText := strings.TrimSpace(stderr.String())
		if stderrText == "" {
			stderrText = "stderr пуст"
		}
		return nil, fmt.Errorf("%s: %w: %s (нужен poppler-utils: pdftotext)", PDFTool, err, stderrText)
	}
	if len(bytes.TrimSpace(output)) == 0 {
		return nil, fmt.Errorf("%s: пустой stdout — в PDF нет текстового слоя (скан?)", PDFTool)
	}
	return output, nil
}

// pdfArgs — аргументы pdftotext по режиму. stdin/stdout (`- -`) вместо временных файлов:
// байты уже в памяти, а шаблон импорта отдаёт тот же срез в разбор и в архив.
func pdfArgs(mode PDFMode) ([]string, error) {
	switch mode {
	case PDFLayout:
		return []string{"-layout", "-enc", "UTF-8", "-nopgbrk", "-", "-"}, nil
	case PDFBBoxFirstPage:
		return []string{"-bbox", "-enc", "UTF-8", "-f", "1", "-l", "1", "-", "-"}, nil
	default:
		return nil, fmt.Errorf("%s: неизвестный режим извлечения %d", PDFTool, mode)
	}
}
