package bank

import (
	"strings"
	"testing"
)

// TestTbIfrsDdlShape: DDL импортёра Т-Банка — своя %s-форма (стандарт §5.1): шаблон исполняет её
// как fmt.Sprintf(sql, TableName), поэтому имя таблицы в литерале обязано быть ровно одно и только
// в позиции подстановки, а колонка значения — Float64 (спека §3.5: таблица новая, legacy-таблицы
// пакета остаются Float32). Имя общей таблицы ВТБ (rus_vtb_group_ifrs) в литерале означало бы, что
// развязка не сделана: две непохожие отчётности писались бы в одну таблицу, а префлайт и count()
// одного импортёра считали бы точки другого.
//
// Разбор книги этим тестом не покрыт: источника-книги нет (ссылка ведёт на PDF), фикстуры нет —
// разбор переведён на батч механически и не верифицирован (спека §3.5, отступление О4).
//
// Имена таблиц — литералами, а не константами импортёра (§8.5): ссылка на константу сделала бы
// проверку тавтологичной и прошла бы ровно на той подмене имени, от которой обязана падать.
func TestTbIfrsDdlShape(t *testing.T) {
	if got := strings.Count(tbIfrsDdl, "%s"); got != 1 {
		t.Errorf("в tbIfrsDdl %d вхождений %%s, want 1 (имя таблицы подставляет шаблон):\n%s", got, tbIfrsDdl)
	}
	if !strings.Contains(tbIfrsDdl, "Float64") {
		t.Errorf("в tbIfrsDdl нет Float64: колонка значения новой таблицы обязана быть Float64 (спека §3.5):\n%s", tbIfrsDdl)
	}
	if strings.Contains(tbIfrsDdl, "rus_vtb_group_ifrs") {
		t.Errorf("tbIfrsDdl содержит имя общей таблицы ВТБ rus_vtb_group_ifrs — DDL обязан быть своим:\n%s", tbIfrsDdl)
	}
	if strings.Contains(tbIfrsDdl, "tbank_group_ifrs") {
		t.Errorf("tbIfrsDdl содержит литерал имени своей таблицы tbank_group_ifrs — имя приходит подстановкой:\n%s", tbIfrsDdl)
	}
}
