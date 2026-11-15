package util

import "fmt"

// SourceReadError identifies transport or workbook-opening failures before data
// parsing and batch writes. Callers may use it to select an alternate source;
// database, parser and publication failures must never trigger that selection.
type SourceReadError struct {
	URL string
	Err error
}

func (e *SourceReadError) Error() string { return fmt.Sprintf("read source %s: %v", e.URL, e.Err) }
func (e *SourceReadError) Unwrap() error { return e.Err }
