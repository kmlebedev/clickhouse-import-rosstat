# X-13 (R `seasonal`) с явной спецификацией для групп CB_COMP (#421, спека §11).
# Вход CSV `month,level` (уровень индекса), выход CSV `month,full,rt` — SA м/м, %
# (full — вся выборка; rt — as-of по данным до месяца) и рядом файл `<выход>.aicc`
# с AICc regARIMA модели на всей выборке (NA — не сошлась).
# Аргументы: вход, номер (1-based) первого месяца as-of, выход, id спецификации C0…C4.
suppressMessages(library(seasonal))
args <- commandArgs(TRUE)
d <- read.csv(args[1])
first_rt <- as.integer(args[2])
spec <- args[4]
y0 <- as.integer(substr(d$month[1], 1, 4)); m0 <- as.integer(substr(d$month[1], 6, 7))
fit <- function(s) tryCatch(suppressWarnings(switch(spec,
  C0 = seas(s, transform.function = "log"),
  C1 = seas(s, transform.function = "log", outlier = NULL),
  C2 = seas(s, transform.function = "log", outlier.types = "ao"),
  C3 = seas(s, transform.function = "log", x11 = ""),
  C4 = seas(s, transform.function = "log", x11 = "", outlier = NULL),
  stop("неизвестная спецификация ", spec)
)), error = function(e) NULL)
out <- data.frame(month = d$month, full = NA_real_, rt = NA_real_)
aicc <- NA_real_
model <- fit(ts(d$level, start = c(y0, m0), frequency = 12))
if (!is.null(model)) {
  f <- as.numeric(final(model))
  out$full <- c(NA, f[-1] / f[-length(f)] - 1) * 100
  aicc <- tryCatch(as.numeric(udg(model, "aicc")), error = function(e) NA_real_)
}
for (n in seq_len(nrow(d))[seq_len(nrow(d)) >= first_rt]) {
  model <- fit(ts(d$level[1:n], start = c(y0, m0), frequency = 12))
  if (!is.null(model)) {
    f <- as.numeric(final(model))
    out$rt[n] <- (f[n] / f[n - 1] - 1) * 100
  }
}
write.csv(out, args[3], row.names = FALSE)
writeLines(format(aicc, digits = 17), paste0(args[3], ".aicc"))
