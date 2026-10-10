package polyus

// Report описывает PDF-отчёт Polyus (KPI-пресс-релиз или МСФО-отчётность),
// из которого извлекаются метрики.
type Report struct {
	URL     string
	Period  string // "FY2025", "2026H1"
	Kind    string // "kpi" | "ifrs"
	Lang    string // "en" | "ru"
	Pages   []int
	Enabled bool
}
