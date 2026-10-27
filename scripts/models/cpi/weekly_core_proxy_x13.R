#!/usr/bin/env Rscript
# X-13ARIMA-SEATS для недельного ряда 4-нед. средней (пункт #348).
#
# Gate 0 (--probe): X-13 ограничен периодами 4 и 12; недельная частота 52
# отвергается сообщением "Seasonal period too large. See Section 2.7 of the
# Reference Manual on program limits". Проба воспроизводит этот отказ и
# возвращает exit 2 — Python-скрипт фиксирует статус infeasible с текстом.
#
# --run оставлен на случай, если будущая версия X-13 поддержит недельную
# частоту: читает CSV (date,level) и список cutoff, для каждого cutoff
# строит ts(frequency=52), делает seats_log_cal и пишет последние пять
# SA-уровней (четырёх недельных приростов достаточно для 4-нед. средней).

suppressPackageStartupMessages(library(seasonal))

args <- commandArgs(trailingOnly = TRUE)
mode <- if (length(args) >= 1) args[[1]] else "--probe"

probe <- function() {
  set.seed(1)
  x <- ts(
    100 + 0.02 * (1:260) + sin(2 * pi * (1:260) / 52) + rnorm(260, 0, 0.05),
    start = c(2022, 1), frequency = 52
  )
  fit <- tryCatch(
    seas(x, transform.function = "log", regression.aictest = "td", outlier = ""),
    error = function(e) e
  )
  if (inherits(fit, "error")) {
    cat("ERROR:", conditionMessage(fit), "\n")
    quit(status = 2)
  }
  adjusted <- tryCatch(as.numeric(final(fit)), error = function(e) e)
  if (inherits(adjusted, "error")) {
    cat("ERROR:", conditionMessage(adjusted), "\n")
    quit(status = 2)
  }
  cat("OK points:", length(adjusted), "\n")
}

run <- function(input, cutoffs_path, output) {
  data <- read.csv(input, stringsAsFactors = FALSE)
  data$date <- as.Date(data$date)
  data <- data[order(data$date), ]
  cutoffs <- as.Date(readLines(cutoffs_path))
  rows <- list()
  for (cutoff in cutoffs) {
    part <- data[data$date <= cutoff, ]
    if (nrow(part) < 156) {
      next
    }
    start_year <- as.integer(format(part$date[1], "%Y"))
    start_week <- as.integer(format(part$date[1], "%j")) %/% 7 + 1
    values <- ts(part$level, start = c(start_year, start_week), frequency = 52)
    fit <- tryCatch(
      seas(values, transform.function = "log",
           regression.aictest = "td", outlier = ""),
      error = function(e) e
    )
    if (inherits(fit, "error")) {
      rows[[length(rows) + 1]] <- data.frame(
        cutoff = cutoff, date = as.Date(NA), sa_level = NA_real_,
        error = conditionMessage(fit)
      )
      next
    }
    adjusted <- tryCatch(as.numeric(final(fit)), error = function(e) e)
    if (inherits(adjusted, "error")) {
      rows[[length(rows) + 1]] <- data.frame(
        cutoff = cutoff, date = as.Date(NA), sa_level = NA_real_,
        error = conditionMessage(adjusted)
      )
      next
    }
    n <- length(adjusted)
    idx <- max(1, n - 4):n
    rows[[length(rows) + 1]] <- data.frame(
      cutoff = cutoff, date = part$date[idx], sa_level = adjusted[idx],
      error = NA_character_
    )
  }
  if (length(rows) == 0) {
    cat("no successful X-13 runs\n")
    quit(status = 2)
  }
  result <- do.call(rbind, rows)
  write.csv(result, output, row.names = FALSE, quote = FALSE, na = "")
  cat(sprintf("wrote %d rows to %s\n", nrow(result), output))
}

if (mode == "--probe") {
  probe()
} else if (mode == "--run" && length(args) >= 4) {
  run(args[[2]], args[[3]], args[[4]])
} else {
  cat("usage: weekly_core_proxy_x13.R --probe | --run <grid.csv> <cutoffs.txt> <out.csv>\n")
  quit(status = 64)
}
