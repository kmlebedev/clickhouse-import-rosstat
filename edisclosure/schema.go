package edisclosure

// edisclosureTable — имя таблицы импортёра; совпадает с Name() импортёра, id шага
// dagu/edisclosure.yaml и source винтажного архива версий (спека §5.2, §5.5).
// Агент читает витрину v_edisclosure_documents; сырая таблица ему не выдаётся.
const edisclosureTable = "edisclosure_documents"

// edisclosureArchiveSource — source винтажного архива (archive.Key): имя объекта
// в хранилище начинается с edisclosure_documents/<sha16>_<name> (спека §5.5).
// Пилотный реестр под source edisclosure не импортируется — это осознанная
// граница MVP, а не миграция.
const edisclosureArchiveSource = "edisclosure_documents"

// edisclosureDdl — копия канонического DDL edisclosure_documents из спеки §5.2;
// переносить дословно, не менять. Ключ (ticker, section, fileid) — идентичность
// документа, loaded_at — версия ReplacingMergeTree: одна строка хранит последнюю
// версию, история версий живёт в реестре архива (archive.Entry на каждый sha256).
// Поэтому запросы к сырой таблице обязаны использовать FINAL; витрина делает это
// за агента. DDL выполняется до любых записей: импортёр обязан отработать и на
// чистой базе, где таблицы ещё нет.
const edisclosureDdl = `CREATE TABLE IF NOT EXISTS edisclosure_documents (
    ticker LowCardinality(String),       -- биржевой тикер эмитента: PLZL, SBER, CHMF
    company_id String,                   -- id карточки e-disclosure (7832, 3043, 30)
    section LowCardinality(String),      -- раздел карточки: IFRS, RAS, AFFILIATES, AGM
    doc_type String,                     -- тип документа из карточки источника
    period String,                       -- отчётный период как в источнике: «2025», «2026, 6 месяцев»
    basis_date Nullable(Date),           -- дата составления/основания (DD.MM.YYYY), пусто → NULL
    publication_date Nullable(Date),     -- дата публикации документа, пусто → NULL
    fileid String,                       -- Fileid из FileLoad.ashx — идентичность документа
    url String,                          -- исходная ссылка на файл
    size_bytes UInt64,                   -- фактический размер скачанного файла
    sha256 String,                       -- sha256 содержимого — версия документа
    archive_key String,                  -- ключ объекта в винтажном архиве ("" — архив выключен)
    first_seen DateTime,                 -- когда конвейер впервые увидел (ticker, section, fileid)
    loaded_at DateTime DEFAULT now()     -- время записи строки: версия ReplacingMergeTree
) ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (ticker, section, fileid);
`
