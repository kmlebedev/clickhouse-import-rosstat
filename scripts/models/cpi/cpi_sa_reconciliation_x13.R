# X-13 (R `seasonal`) для сверки SA (#399): вход CSV `month,level` (уровень индекса),
# выход CSV `month,full,rt` — SA м/м, % (full — вся выборка; rt — as-of по данным до месяца).
# Аргументы: вход, номер (1-based) первого месяца as-of, выход. Спецификация — по умолчанию
# (log, автомодель, автовыбросы), без подбора.
suppressMessages(library(seasonal))
args <- commandArgs(TRUE)
d <- read.csv(args[1])
first_rt <- as.integer(args[2])
y0 <- as.integer(substr(d$month[1], 1, 4)); m0 <- as.integer(substr(d$month[1], 6, 7))
fit <- function(s) tryCatch(suppressWarnings(seas(s, transform.function = "log")), error = function(e) NULL)
out <- data.frame(month = d$month, full = NA_real_, rt = NA_real_)
model <- fit(ts(d$level, start = c(y0, m0), frequency = 12))
if (!is.null(model)) {
  f <- as.numeric(final(model))
  out$full <- c(NA, f[-1] / f[-length(f)] - 1) * 100
}
for (n in seq_len(nrow(d))[seq_len(nrow(d)) >= first_rt]) {
  model <- fit(ts(d$level[1:n], start = c(y0, m0), frequency = 12))
  if (!is.null(model)) {
    f <- as.numeric(final(model))
    out$rt[n] <- (f[n] / f[n - 1] - 1) * 100
  }
}
write.csv(out, args[3], row.names = FALSE)
