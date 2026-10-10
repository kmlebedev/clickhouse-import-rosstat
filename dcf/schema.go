package dcf

// Канонические DDL из ARCHITECTURE.md §6.2 — копировать как есть, не менять.

const minePlansCreateTable = `CREATE TABLE IF NOT EXISTS mine_plans (
    company LowCardinality(String),
    asset LowCardinality(String),
    year UInt16,
    production_koz Float64,
    grade_gpt Nullable(Float64),
    tcc Float64, aisc Float64,
    capex_sustaining Float64, capex_project Float64,
    closure_costs Float64
) ENGINE = ReplacingMergeTree ORDER BY (company, asset, year);`

const priceDecksCreateTable = `CREATE TABLE IF NOT EXISTS price_decks (
    deck LowCardinality(String),
    year UInt16,
    gold_usd Float64,
    published Date
) ENGINE = ReplacingMergeTree ORDER BY (deck, year, published);`

// navByAssetCreateTable — ключ обязан включать deck и contour: без них
// ReplacingMergeTree схлопывал бы три ценовых дека и два контура ставки в одну
// строку (тихая потеря — тот же класс дефекта, что и ключ company_financials без
// source_url, см. ARCHITECTURE.md §8.1, дефект 3).
const navByAssetCreateTable = `CREATE TABLE IF NOT EXISTS nav_by_asset (
    run_id UUID,
    deck LowCardinality(String),
    asset LowCardinality(String),
    contour LowCardinality(String),
    npv_usd_mln Float64,
    discount_rate Float64,
    stage_haircut Nullable(Float64)
) ENGINE = ReplacingMergeTree ORDER BY (run_id, deck, asset, contour);`
