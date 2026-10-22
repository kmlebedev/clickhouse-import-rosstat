#!/usr/bin/env Rscript
# X-13ARIMA-SEATS на месячной сетке 4-недельной накопленной инфляции (#348).
#
# Недельная частота 52 X-13 недоступна ("Seasonal period too large"), поэтому
# ряд строится иначе: 4-недельный накопленный индекс (100 = без изменения за
# 4 недели) берётся на конец месяца — 57 точек 2022-01…2026-09, frequency=12.
# Спецификация — seats_log_cal (log, regression.aictest="td", outlier="").
#
# --probe  — синтетическая проверка frequency=12;
# --fit    — SA-ряд на полной выборке (in-sample, для диагностики);
# --asof   — SA на каждом cutoff (расширяющееся окно, >= 36 месяцев).

suppressPackageStartupMessages(library(seasonal))

args <- commandArgs(trailingOnly = TRUE)
mode <- if (length(args) >= 1) args[[1]] else "--probe"

fit_series <- function(level, dates) {
  start_year <- as.integer(format(dates[1], "%Y"))
  start_month <- as.integer(format(dates[1], "%m"))
  values <- ts(level, start = c(start_year, start_month), frequency = 12)
  seas(values, transform.function = "log", regression.aictest = "td", outlier = "")
}

probe <- function() {
  set.seed(1)
  x <- ts(
    100 + 0.01 * (1:60) + sin(2 * pi * (1:60) / 12) + rnorm(60, 0, 0.2),
    start = c(2022, 1), frequency = 12
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

read_input <- function(path) {
  data <- read.csv(path, stringsAsFactors = FALSE)
  data$date <- as.Date(data$date)
  data[order(data$date), ]
}

run_fit <- function(input, output) {
  data <- read_input(input)
  fit <- tryCatch(fit_series(data$level, data$date), error = function(e) e)
  if (inherits(fit, "error")) {
    cat("ERROR:", conditionMessage(fit), "\n")
    quit(status = 2)
  }
  adjusted <- tryCatch(as.numeric(final(fit)), error = function(e) e)
  if (inherits(adjusted, "error")) {
    cat("ERROR:", conditionMessage(adjusted), "\n")
    quit(status = 2)
  }
  out <- data.frame(date = data$date, sa_level = adjusted)
  write.csv(out, output, row.names = FALSE, quote = FALSE, na = "")
  cat(sprintf("wrote %d rows to %s\n", nrow(out), output))
}

run_asof <- function(input, cutoffs_path, output, min_months = 36) {
  data <- read_input(input)
  cutoffs <- as.Date(readLines(cutoffs_path))
  rows <- list()
  for (cutoff in cutoffs) {
    part <- data[data$date <= cutoff, ]
    if (nrow(part) < min_months) {
      next
    }
    fit <- tryCatch(fit_series(part$level, part$date), error = function(e) e)
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
    rows[[length(rows) + 1]] <- data.frame(
      cutoff = cutoff, date = part$date[nrow(part)],
      sa_level = adjusted[length(adjusted)], error = NA_character_
    )
  }
  if (length(rows) == 0) {
    cat("no successful X-13 monthly runs\n")
    quit(status = 2)
  }
  result <- do.call(rbind, rows)
  write.csv(result, output, row.names = FALSE, quote = FALSE, na = "")
  cat(sprintf("wrote %d rows to %s\n", nrow(result), output))
}

if (mode == "--probe") {
  probe()
} else if (mode == "--fit" && length(args) >= 3) {
  run_fit(args[[2]], args[[3]])
} else if (mode == "--asof" && length(args) >= 4) {
  run_asof(args[[2]], args[[3]], args[[4]])
} else {
  cat("usage: weekly_core_proxy_x13_monthly.R --probe | --fit <in.csv> <out.csv> | --asof <in.csv> <cutoffs.txt> <out.csv>\n")
  quit(status = 64)
}
