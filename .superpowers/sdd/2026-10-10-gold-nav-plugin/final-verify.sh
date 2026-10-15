#!/usr/bin/env bash
# Final verification of branch gold-nav-contour. Run from repo root:
#   bash .superpowers/sdd/2026-10-10-gold-nav-plugin/final-verify.sh
set -u
W=.superpowers/sdd/2026-10-10-gold-nav-plugin
exec > >(tee "$W/final-verify.log") 2>&1
git log --oneline -1
echo "== make ch-status"; make ch-status
echo "== make all (lint, vet, test -race, build, build-ingest)"; make all || { echo "MAKE ALL FAILED"; exit 1; }
echo "== reseed events_calendar"; make ch-sql FILE=sql/events_calendar_q4_2026.sql
echo "== make import STAT=gold_views"; LOG_LEVEL=debug make import STAT=gold_views
echo "== view checks"; make ch-sql FILE=$W/final-check.sql
export INGEST_TOKEN="${INGEST_TOKEN:-verify-$(date +%s)}"
./build/ingest > $W/final-ingest.log 2>&1 &
PID=$!
for i in $(seq 1 30); do curl -s -o /dev/null localhost:8081/ && break; sleep 1; done
echo "== unknown field (expect 400)"; curl -s -w ' HTTP %{http_code}\n' -X POST localhost:8081/v1/model_run -H "Authorization: Bearer $INGEST_TOKEN" -d '{"nav_bulll":1}'
echo "== negative probability (expect 400)"; curl -s -w ' HTTP %{http_code}\n' -X POST localhost:8081/v1/model_run -H "Authorization: Bearer $INGEST_TOKEN" -d '{"run_id":"7f3c2a10-5b1e-4c7d-9a2f-0e8d4b6c1a98","trigger_type":"manual","trigger_ref":"x","gold_scenario":{"horizon":"Q4-2026","bull":{"low":4600,"high":5000,"point":4800},"base":{"low":4000,"high":4600,"point":4300},"bear":{"low":3750,"high":4050,"point":3900}},"usdrub_path":{"Q4-2026":81.5},"price_deck":"own_scenario","discount_rate":0.09,"wacc":0.14,"nav_per_share":1,"nav_bull":1,"nav_base":1,"nav_bear":1,"market_price":1,"upside_pct":0,"comment":"x","probabilities":{"bull":150,"base":-40,"bear":-10}}'
kill $PID 2>/dev/null
echo "== plugin repo"
if [ -d ../gold-nav ]; then echo "../gold-nav already exists — not touched"; else git clone -q $W/gold-nav.bundle ../gold-nav && git -C ../gold-nav log --oneline; fi
