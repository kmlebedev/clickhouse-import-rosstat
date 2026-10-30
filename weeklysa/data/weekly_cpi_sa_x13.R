#!/usr/bin/env Rscript
# Production X-13ARIMA-SEATS spec `weekly-cpi-sa-2026-v1` for `weekly_cpi_sa`.
#
# Input:  CSV with columns date,level — month-end 4-week cumulative index
#         (100 = no change over 28 days), 2009-05 … current.
# Output: CSV with columns date,sa_level.
#
# The specification repeats research #348 exactly: SEATS, log transform,
# automatic outliers, automatic ARIMA and the trading-day calendar test,
# frequency 12. Any failure aborts the whole run with a non-zero exit code:
# a partial output would let the Go importer write a truncated derived series.

suppressPackageStartupMessages(library(seasonal))

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 2) {
  stop("usage: weekly_cpi_sa_x13.R <input.csv> <output.csv>")
}
input <- args[[1]]
output <- args[[2]]

data <- read.csv(input, stringsAsFactors = FALSE, check.names = FALSE)
required <- c("date", "level")
missing <- setdiff(required, names(data))
if (length(missing) > 0) {
  stop("missing columns: ", paste(missing, collapse = ", "))
}
if (nrow(data) == 0) {
  stop("empty input")
}

data$date <- as.Date(data$date)
data <- data[order(data$date), ]
if (anyNA(data$date)) {
  stop("invalid date")
}
if (anyNA(data$level) || any(data$level <= 0)) {
  stop("non-positive or missing level")
}
n <- nrow(data)
if (n < 36) {
  stop(sprintf("fewer than 36 points (%d)", n))
}
# Consecutive months: compare month indices, not seq.Date (month-end dates
# make seq(by = "month") overflow into the next month).
idx <- as.integer(format(data$date, "%Y")) * 12 + as.integer(format(data$date, "%m"))
if (length(unique(diff(idx))) != 1 || any(diff(idx) != 1)) {
  stop("months are not consecutive")
}

start_year <- as.integer(format(data$date[1], "%Y"))
start_month <- as.integer(format(data$date[1], "%m"))
x <- ts(data$level, start = c(start_year, start_month), frequency = 12)

fit <- seas(
  x,
  transform.function = "log",
  outlier = "",
  regression.aictest = "td"
)
sa <- as.numeric(final(fit))
if (length(sa) != n) {
  stop(sprintf("X-13 returned %d points, want %d", length(sa), n))
}
if (anyNA(sa)) {
  stop("X-13 returned NA in the final SA series")
}

result <- data.frame(
  date = format(data$date, "%Y-%m-%d"),
  sa_level = sa,
  stringsAsFactors = FALSE
)
write.csv(result, output, row.names = FALSE, quote = FALSE)
cat(sprintf("weekly_cpi_sa: %d rows\n", nrow(result)))
