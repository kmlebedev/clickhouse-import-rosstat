#!/usr/bin/env bash
# Task 3 Step 8 verification. Run from repo root: bash .superpowers/sdd/2026-10-10-gold-nav-plugin/task3-verify.sh
set -u
W=.superpowers/sdd/2026-10-10-gold-nav-plugin
OUT=$W/task3-verify.log
exec > >(tee "$OUT") 2>&1
echo "== make ch-status"; make ch-status
echo "== make all"; make all || { echo "MAKE ALL FAILED"; exit 1; }
export INGEST_TOKEN="${INGEST_TOKEN:-verify-$(date +%s)}"
echo "== start ingest"; make run-ingest > $W/task3-ingest.log 2>&1 &
PID=$!
for i in $(seq 1 30); do curl -s -o /dev/null localhost:8081/ && break; sleep 1; done
BODY='{"run_id":"7f3c2a10-5b1e-4c7d-9a2f-0e8d4b6c1a99","trigger_type":"manual","trigger_ref":"task3 verify","gold_scenario":{"horizon":"Q4-2026","bull":{"low":4600,"high":5000,"point":4800},"base":{"low":4000,"high":4600,"point":4300},"bear":{"low":3750,"high":4050,"point":3900}},"usdrub_path":{"Q4-2026":81.5},"price_deck":"own_scenario","discount_rate":0.09,"wacc":0.14,"nav_per_share":12500,"nav_bull":16000,"nav_base":12500,"nav_bear":9500,"market_price":6800,"upside_pct":83.8,"comment":"task3 verify","probabilities":{"bull":25,"base":40,"bear":35}}'
for n in 1 2; do echo "== POST #$n"; curl -s -w ' HTTP %{http_code}\n' -X POST localhost:8081/v1/model_run -H "Authorization: Bearer $INGEST_TOKEN" -H 'Content-Type: application/json' -d "$BODY"; done
echo "== POST bad run_id (expect 400)"; curl -s -w ' HTTP %{http_code}\n' -X POST localhost:8081/v1/model_run -H "Authorization: Bearer $INGEST_TOKEN" -d "${BODY/7f3c2a10-5b1e-4c7d-9a2f-0e8d4b6c1a99/not-a-uuid}"
echo "== POST no token (expect 401)"; curl -s -w ' HTTP %{http_code}\n' -X POST localhost:8081/v1/model_run -d "$BODY"
echo "== counts (expect model_runs 1)"; make ch-sql FILE=$W/task3-count.sql
pkill -P $PID 2>/dev/null; kill $PID 2>/dev/null
echo "== ingest log tail"; tail -20 $W/task3-ingest.log
