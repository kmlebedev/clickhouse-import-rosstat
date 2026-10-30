#!/usr/bin/env Rscript
# X-13ARIMA-SEATS probe for Rosstat PPI series.
# Reads build/ppi-sa-research/out/series.csv and writes sa.csv with
# seasonally adjusted levels, m/m SA and 3-month SAAR.

suppressPackageStartupMessages(library(seasonal))

root <- normalizePath(".")
args <- commandArgs(trailingOnly = TRUE)
input <- if (length(args) >= 1) args[[1]] else file.path(root, "build", "ppi-sa-research", "out", "series.csv")
output <- if (length(args) >= 2) args[[2]] else file.path(root, "build", "ppi-sa-research", "out", "sa.csv")
dir.create(dirname(output), recursive = TRUE, showWarnings = FALSE)

data <- read.csv(input, stringsAsFactors = FALSE)
data$date <- as.Date(data$date)
series_names <- unique(data$series)

variants <- list(
  list(name = "seats_log_cal", x11 = NULL, transform = "log", calendar = "td"),
  list(name = "seats_level_cal", x11 = NULL, transform = "none", calendar = "td"),
  list(name = "x11_log_cal", x11 = "", transform = "log", calendar = "td"),
  list(name = "seats_log_nocal", x11 = NULL, transform = "log", calendar = NULL)
)

rows <- list()
errors <- list()

for (name in series_names) {
  part <- data[data$series == name, ]
  part <- part[order(part$date), ]
  if (nrow(part) < 36) {
    errors[[length(errors) + 1]] <- data.frame(series = name, variant = "all",
                                               error = "fewer than 36 points")
    next
  }
  start_year <- as.integer(format(part$date[1], "%Y"))
  start_month <- as.integer(format(part$date[1], "%m"))
  values <- ts(part$level, start = c(start_year, start_month), frequency = 12)
  for (variant in variants) {
    fit <- tryCatch({
      args <- list(x = values,
                   transform.function = variant$transform,
                   outlier = "")
      if (!is.null(variant$x11)) {
        args$x11 <- variant$x11
      }
      if (!is.null(variant$calendar)) {
        args$regression.aictest <- variant$calendar
      }
      do.call(seas, args)
    }, error = function(e) e)
    if (inherits(fit, "error")) {
      errors[[length(errors) + 1]] <- data.frame(series = name, variant = variant$name,
                                                 error = conditionMessage(fit))
      next
    }
    adjusted <- as.numeric(final(fit))
    n <- length(adjusted)
    mm <- c(NA, adjusted[-1] / adjusted[-n] - 1)
    saar <- ((1 + mm)^12 - 1) * 100
    saar3 <- rep(NA_real_, n)
    for (i in 3:n) {
      saar3[i] <- mean(saar[(i - 2):i], na.rm = TRUE)
    }
    dates <- seq(part$date[1], by = "month", length.out = n)
    rows[[length(rows) + 1]] <- data.frame(
      series = name,
      variant = variant$name,
      date = format(dates, "%Y-%m-%d"),
      sa = adjusted,
      mm_sa = mm * 100,
      saar = saar,
      saar3 = saar3
    )
  }
}

if (length(rows) > 0) {
  result <- do.call(rbind, rows)
  write.csv(result, output, row.names = FALSE, quote = FALSE)
  cat(sprintf("wrote %d rows to %s\n", nrow(result), output))
} else {
  cat("no successful SA runs\n")
}

if (length(errors) > 0) {
  err <- do.call(rbind, errors)
  write.csv(err, file.path(dirname(output), "sa_errors.csv"),
            row.names = FALSE, quote = FALSE)
  cat(sprintf("errors: %d\n", nrow(err)))
}
