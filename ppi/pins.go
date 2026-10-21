package ppi

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// sha256Hex — отпечаток встроенного артефакта.
func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// checkEmbeddedPins сверяет встроенные скрипт и данные с пинами. Это не
// косметика: правка R-скрипта или CSV без bump spec_version/taxonomy_version
// сделала бы версию в строке ложной. Ошибка останавливает импорт до чтения БД.
func checkEmbeddedPins() error {
	checks := []struct {
		name string
		data []byte
		want string
	}{
		{"ppi/data/ppi_sa_x13.R", ppiX13Script, ppiX13ScriptSHA256},
		{"ppi/data/okpd2-kipc-2026-v2.csv", ppiMappingCSV, ppiMappingSHA256},
		{"ppi/data/kipc-weights-panel.csv", ppiWeightsCSV, ppiWeightsSHA256},
	}
	for _, check := range checks {
		if got := sha256Hex(check.data); got != check.want {
			return fmt.Errorf("%s: SHA-256 %s, ожидался %s: обновите пин и версию spec/taxonomy", check.name, got, check.want)
		}
	}
	return nil
}
