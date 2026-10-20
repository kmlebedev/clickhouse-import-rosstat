#!/usr/bin/env Rscript
# Экспорт панели CB_COMP для bottom-up parity-прогона (issue #190).
#
# Использование:
#   Rscript scripts/models/cb_comp/cb_comp_panel_export.R <archive-root> <out-dir>
#
# <archive-root> — каталог BusinessSurveyCBRCpiForecastingSourceCode из
# распакованного архива авторов. FST хранит имена и строки в CP1251, поэтому
# все character-колонки конвертируются в UTF-8 до записи CSV; Python-часть
# читает уже UTF-8 и не зависит от локали.
#
# Пишет три файла:
#   cpi_panel.csv — date,code,kind,value: Индекс потребительских цен, Россия,
#                   kind in {base_index, diff} (base_index — лог-уровень,
#                   diff — его первая разность = месячный лог-прирост);
#   mp_panel.csv  — date,code,kind,value,lag: Мониторинг предприятий, Россия,
#                   три индикатора и задержка публикации в месяцах;
#   panel.csv     — date,group_stat,code,kind,value,lag: полная панель России
#                   (CPI, МП, кредит, ставки, рыночные индексы) для режимов
#                   per_bsu/full; cpi_panel/mp_panel сохранены для обратной
#                   совместимости.
#
# Скрипт ничего не меняет в архиве; out-dir создаётся при необходимости.

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 2) {
  stop("usage: Rscript scripts/models/cb_comp/cb_comp_panel_export.R <archive-root> <out-dir>")
}

archive <- args[[1]]
out_dir <- args[[2]]

suppressPackageStartupMessages(library(fst))

panel_path <- file.path(
  archive,
  "Models/CpiComponents/StfCpi_External_data/databases/StfCpiR_db/output/data_panel_bi_sa.fst"
)
if (!file.exists(panel_path)) {
  stop(paste("panel not found:", panel_path))
}

panel <- read_fst(panel_path)
names(panel) <- c(
  "date", "value", "okato", "code", "indicator_code",
  "indicator_code_kind", "kind", "group_stat", "lag"
)
for (name in names(panel)) {
  if (is.character(panel[[name]])) {
    panel[[name]] <- iconv(panel[[name]], from = "CP1251", to = "UTF-8")
  }
}

dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

cpi <- panel[
  panel$group_stat == "Индекс потребительских цен" &
    panel$okato == "643->643004.АГ" &
    panel$kind %in% c("base_index", "diff"),
  c("date", "code", "kind", "value")
]
write.csv(
  cpi,
  file.path(out_dir, "cpi_panel.csv"),
  row.names = FALSE,
  fileEncoding = "UTF-8"
)

mp <- panel[
  panel$group_stat == "Мониторинг предприятий" &
    panel$okato == "643->643004.АГ",
  c("date", "code", "kind", "value", "lag")
]
write.csv(
  mp,
  file.path(out_dir, "mp_panel.csv"),
  row.names = FALSE,
  fileEncoding = "UTF-8"
)

# Полная панель для режимов per_bsu/full: кредит, ставки и рыночные индексы
# нужны как дополнительные признаки, trend/trend_deviation — как CPI-тренд.
full <- panel[
  panel$okato == "643->643004.АГ",
  c("date", "group_stat", "code", "kind", "value", "lag")
]
write.csv(
  full,
  file.path(out_dir, "panel.csv"),
  row.names = FALSE,
  fileEncoding = "UTF-8"
)

cat(sprintf(
  "cpi rows: %d; mp rows: %d; full rows: %d\n",
  nrow(cpi), nrow(mp), nrow(full)
))
