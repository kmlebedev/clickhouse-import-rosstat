# План: рефакторинг `wgc/` по стандарту

Спека: [2026-11-05-wgc-refactor-design.md](../specs/2026-11-05-wgc-refactor-design.md).

- [x] 1. Инвентарь нарушений (grep: `colly`, `fmt.Print*`, `panic`, `init()`, `conn.Exec`).
- [x] 2. `wgc/importers.go`: реестр, один `init()`; удалить персональные `init()` из `cb.go`, `etf.go`.
- [x] 3. `wgc/importers_test.go`: энфорсер §8.3 (а–д, ¬) + тест комментариев в dagu.
- [x] 4. `docs/IMPORTER_PACKAGE_STANDARD.md` §18, `ARCHITECTURE.md` §2.
- [x] 5. `make all`, `make docs-check`.
- [x] 6. Живая приёмка 2026-11-05 пройдена: протокол — комментарием в PR #528.
