# X-13 по рецепту авторов CB_COMP на NSA-листах публичной книги ЦБ — разбор 2026-10-31 §6.1.
#
# Авторы (`Models/CpiComponents/R/databases_creators/base_indicies_calculate_sa.R`):
# `seas(ts, estimate.maxiter = 50000)` — X-13ARIMA-SEATS с авто-выбором модели,
# одним прогоном на полной выборке. Здесь — тот же вызов на 11 листах × 3
# индикатора, старт 2002/2009 × конец 2024-09/2026-09.
#
# Вход — `sheets_nsa_sa.csv` (`cb_comp_per_code_review.py --step sa-inputs`),
# выход — `sheets_x13.csv` (slug, indicator, variant, date, value).
# Запуск: Rscript cb_comp_per_code_review_x13.R <in.csv> <out.csv>

suppressMessages(library(seasonal))

args <- commandArgs(TRUE)
data <- read.csv(args[1], stringsAsFactors = FALSE)
data <- data[data$kind == "NSA", ]
data$date <- as.Date(data$date)

out <- list()
for (key in unique(paste(data$slug, data$indicator))) {
  series <- data[paste(data$slug, data$indicator) == key, ]
  series <- series[order(series$date), ]
  for (start in c("2002-01-01", "2009-01-01")) {
    for (end in c("2024-09-01", "2026-09-01")) {
      part <- series[series$date >= as.Date(start) & series$date <= as.Date(end), ]
      first <- part$date[1]
      ts0 <- ts(part$value, start = c(as.integer(format(first, "%Y")), as.integer(format(first, "%m"))), frequency = 12)
      fit <- tryCatch(seas(ts0, estimate.maxiter = 50000), error = function(e) NULL)
      values <- if (is.null(fit)) rep(NA, nrow(part)) else as.numeric(final(fit))
      out[[length(out) + 1]] <- data.frame(
        slug = part$slug, indicator = part$indicator,
        variant = paste0("x13_", substr(start, 1, 4), "_", substr(end, 1, 7)),
        date = part$date, value = values
      )
    }
  }
}
write.csv(do.call(rbind, out), args[2], row.names = FALSE)
