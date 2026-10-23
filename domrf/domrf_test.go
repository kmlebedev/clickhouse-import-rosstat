package domrf

import (
	"os"
	"testing"
)

// TestDomrfIsChallenge — оба ответа источника 2026-10-23: настоящая страница и JS-оболочка
// ServicePipe. Порог и маркеры проверяются на реальных снапшотах, а не на выдуманных строках.
func TestDomrfIsChallenge(t *testing.T) {
	challenge, err := os.ReadFile("testdata/domrf_challenge.html")
	if err != nil {
		t.Fatalf("read challenge fixture: %v", err)
	}
	if !domrfIsChallenge(challenge) {
		t.Fatal("JS-оболочка не распознана")
	}
	page := domrfTestPage(t)
	if domrfIsChallenge(page) {
		t.Fatal("настоящая страница распознана как оболочка")
	}
	if domrfIsChallenge(nil) {
		t.Fatal("пустой ответ распознан как оболочка")
	}
}

// TestDomrfBlocks — страница несёт оба нужных блока; оболочка блоков не даёт (ошибка, а не пустая
// карта: иначе разбор пошёл бы дальше с «нет данных»).
func TestDomrfBlocks(t *testing.T) {
	blocks, err := domrfBlocks(domrfTestPage(t))
	if err != nil {
		t.Fatalf("domrfBlocks: %v", err)
	}
	for _, name := range []string{domrfMarketBlock, domrfWeeklyBlock} {
		if _, ok := blocks[name]; !ok {
			t.Errorf("блок %q не найден", name)
		}
	}
	if len(blocks) < 10 {
		t.Errorf("блоков %d, ожидалось не меньше 10", len(blocks))
	}
	challenge, err := os.ReadFile("testdata/domrf_challenge.html")
	if err != nil {
		t.Fatalf("read challenge fixture: %v", err)
	}
	if _, err = domrfBlocks(challenge); err == nil {
		t.Error("оболочка без main#app не поймана")
	}
}

// TestDomrfMarketBlock — ссылка на книгу и дата публикации из реального блока статистики.
func TestDomrfMarketBlock(t *testing.T) {
	blocks, err := domrfBlocks(domrfTestPage(t))
	if err != nil {
		t.Fatalf("domrfBlocks: %v", err)
	}
	block, err := domrfBlock[domrfMarketBlockJSON](blocks, domrfMarketBlock)
	if err != nil {
		t.Fatalf("domrfBlock: %v", err)
	}
	if block.Props.File.Src != "/api/v2/content/download/273751/135290452/" {
		t.Errorf("ссылка на книгу %q", block.Props.File.Src)
	}
	if block.Props.File.Date != "01.10.2026" {
		t.Errorf("дата публикации %q", block.Props.File.Date)
	}
	if block.Props.SubTitle == "" {
		t.Error("подзаголовок с датой данных пуст")
	}
	if _, err = domrfBlock[domrfMarketBlockJSON](blocks, "нет-такого-блока"); err == nil {
		t.Error("отсутствующий блок не пойман")
	}
}
