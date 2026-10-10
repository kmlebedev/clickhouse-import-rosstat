package polyus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

// downloadPDF скачивает PDF-отчёт по url в destination и проверяет,
// что получен действительно PDF (Content-Type, размер, заголовок %PDF-).
func downloadPDF(ctx context.Context, url, destination string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	request.Header.Set(
		"User-Agent",
		"Mozilla/5.0 PolyusFinancialParser/1.0",
	)
	request.Header.Set("Accept", "application/pdf,*/*")

	client := &http.Client{
		Timeout: 90 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return errors.New("too many redirects")
			}
			return nil
		},
	}

	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("HTTP request: %w", err)
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))

		return fmt.Errorf(
			"unexpected HTTP status %s: %s",
			response.Status,
			strings.TrimSpace(string(body)),
		)
	}

	contentType := response.Header.Get("Content-Type")
	if contentType != "" &&
		!strings.Contains(contentType, "application/pdf") &&
		!strings.Contains(contentType, "application/octet-stream") {
		return fmt.Errorf("unexpected Content-Type: %s", contentType)
	}

	output, err := os.Create(destination)
	if err != nil {
		return fmt.Errorf("create destination: %w", err)
	}
	defer func() { _ = output.Close() }()

	written, err := io.Copy(output, response.Body)
	if err != nil {
		return fmt.Errorf("write destination: %w", err)
	}

	if written < 1024 {
		return fmt.Errorf("downloaded file is suspiciously small: %d bytes", written)
	}

	if err := validatePDFHeader(destination); err != nil {
		return err
	}

	log.Infof("downloaded %d bytes to %s", written, destination)

	return nil
}

func validatePDFHeader(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open downloaded file: %w", err)
	}
	defer func() { _ = file.Close() }()

	header := make([]byte, 5)

	if _, err := io.ReadFull(file, header); err != nil {
		return fmt.Errorf("read PDF header: %w", err)
	}

	if string(header) != "%PDF-" {
		return fmt.Errorf(
			"downloaded file is not a PDF, header=%q",
			string(header),
		)
	}

	return nil
}

// extractPage извлекает одну страницу PDF в текстовый файл через pdftotext.
func extractPage(
	ctx context.Context,
	pdfPath string,
	textPath string,
	page int,
) error {
	if page < 1 {
		return errors.New("page must be greater than zero")
	}

	command := exec.CommandContext(
		ctx,
		"pdftotext",
		"-f", strconv.Itoa(page),
		"-l", strconv.Itoa(page),
		"-layout",
		"-nopgbrk",
		"-enc", "UTF-8",
		pdfPath,
		textPath,
	)

	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf(
			"pdftotext failed: %w: %s",
			err,
			strings.TrimSpace(string(output)),
		)
	}

	info, err := os.Stat(textPath)
	if err != nil {
		return fmt.Errorf("stat extracted text: %w", err)
	}

	if info.Size() == 0 {
		return errors.New("pdftotext produced an empty file")
	}

	return nil
}

// joinFiles склеивает текстовые файлы paths в dst без разделителей.
func joinFiles(paths []string, dst string) error {
	var buf []byte
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			return fmt.Errorf("read %s: %w", p, err)
		}
		buf = append(buf, data...)
	}
	if err := os.WriteFile(dst, buf, 0o600); err != nil {
		return fmt.Errorf("write %s: %w", dst, err)
	}
	return nil
}

// extractPDF извлекает указанные страницы PDF и склеивает их в один текст.
func extractPDF(ctx context.Context, pdfPath, textPath string, pages []int) error {
	if len(pages) == 0 {
		return errors.New("no pages to extract")
	}
	dir := filepath.Dir(textPath)
	var extracted []string
	for _, page := range pages {
		tmp := filepath.Join(dir, fmt.Sprintf("%s.p%d", filepath.Base(textPath), page))
		if err := extractPage(ctx, pdfPath, tmp, page); err != nil {
			return err
		}
		extracted = append(extracted, tmp)
		defer func() { _ = os.Remove(tmp) }()
	}
	return joinFiles(extracted, textPath)
}
