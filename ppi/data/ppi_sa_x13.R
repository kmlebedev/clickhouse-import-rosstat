#!/usr/bin/env Rscript
# Production X-13ARIMA-SEATS spec `ppi-sa-2026-v1` for the `ppi_sa` layer.
#
# Baseline from research #275: SEATS, log transform, automatic outliers,
# automatic ARIMA and the trading-day calendar test. One spec only; the research
# script `scripts/ppi_sa_x13.R` keeps the four-variant comparison.
#
# Input:  CSV with columns series,date,level
# Output: CSV with columns series,date,sa
#
# Any failure aborts the whole run with a non-zero exit code: a partial output
# would let the Go importer write a truncated derived series.

suppressPackageStartupMessages(library(seasonal))

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 2) {
  stop("usage: ppi_sa_x13.R <input.csv> <output.csv>")
}
input <- args[[1]]
output <- args[[2]]

data <- read.csv(input, stringsAsFactors = FALSE, check.names = FALSE)
required <- c("series", "date", "level")
missing <- setdiff(required, names(data))
if (length(missing) > 0) {
  stop("missing columns: ", paste(missing, collapse = ", "))
}
if (nrow(data) == 0) {
  stop("empty input")
}

rows <- list()
for (name in unique(data$series)) {
  part <- data[data$series == name, ]
  part$date <- as.Date(part$date)
  part <- part[order(part$date), ]
  n <- nrow(part)
  if (n < 36) {
    stop(sprintf("%s: fewer than 36 points (%d)", name, n))
  }
  if (anyNA(part$date)) {
    stop(sprintf("%s: invalid date", name))
  }
  if (anyNA(part$level) || any(part$level <= 0)) {
    stop(sprintf("%s: non-positive or missing level", name))
  }
  # Consecutive months: compare month indices, not seq.Date (month-end dates
  # make seq(by = "month") overflow into the next month).
  idx <- as.integer(format(part$date, "%Y")) * 12 + as.integer(format(part$date, "%m"))
  if (length(unique(diff(idx))) != 1 || any(diff(idx) != 1)) {
    stop(sprintf("%s: months are not consecutive", name))
  }

  start_year <- as.integer(format(part$date[1], "%Y"))
  start_month <- as.integer(format(part$date[1], "%m"))
  x <- ts(part$level, start = c(start_year, start_month), frequency = 12)

  fit <- seas(
    x,
    transform.function = "log",
    outlier = "",
    regression.aictest = "td"
  )
  sa <- as.numeric(final(fit))
  if (length(sa) != n) {
    stop(sprintf("%s: X-13 returned %d points, want %d", name, length(sa), n))
  }
  if (anyNA(sa)) {
    stop(sprintf("%s: X-13 returned NA in the final SA series", name))
  }

  rows[[length(rows) + 1]] <- data.frame(
    series = name,
    date = format(part$date, "%Y-%m-%d"),
    sa = sa,
    stringsAsFactors = FALSE
  )
}

result <- do.call(rbind, rows)
write.csv(result, output, row.names = FALSE, quote = FALSE)
cat(sprintf("ppi_sa: %d series, %d rows\n", length(unique(result$series)), nrow(result)))
