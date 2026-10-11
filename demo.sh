#!/usr/bin/env bash
set -e

PORT_A=8081
PORT_B=8082

echo "==> Cleaning up background instances..."
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT

echo "==> Starting Instance A on :$PORT_A (Redis Enabled, Limit=5)..."
PORT=$PORT_A REDIS_ENABLED=true RATE_LIMIT=5 WINDOW=10s MAX_CONCURRENT=2 go run ./cmd/server &
PID_A=$!

echo "==> Starting Instance B on :$PORT_B (Redis Enabled, Limit=5)..."
PORT=$PORT_B REDIS_ENABLED=true RATE_LIMIT=5 WINDOW=10s MAX_CONCURRENT=2 go run ./cmd/server &
PID_B=$!

sleep 2

echo ""
echo "=== DEMO 1: Distributed Rate Limit Sharing across Instances ==="
echo "Sending 3 requests to Instance A..."
for i in {1..3}; do
  curl -s -o /dev/null -w "Inst A Req $i: %{http_code}\n" -H "X-API-Key: demo-user" http://localhost:$PORT_A/api/demo
done

echo "Sending 2 requests to Instance B..."
for i in {1..2}; do
  curl -s -o /dev/null -w "Inst B Req $i: %{http_code}\n" -H "X-API-Key: demo-user" http://localhost:$PORT_B/api/demo
done

echo "Sending 6th request to Instance A (should hit 429)..."
curl -s -o /dev/null -w "Inst A Req 6: %{http_code}\n" -H "X-API-Key: demo-user" http://localhost:$PORT_A/api/demo

echo ""
echo "=== DEMO 2: Load Shedding Fast 503 ==="
echo "Sending 5 concurrent requests against MAX_CONCURRENT=2..."
for i in {1..5}; do
  curl -s -o /dev/null -w "Work Req $i: %{http_code}\n" http://localhost:$PORT_A/api/work &
done
wait

echo ""
echo "==> Demo completed successfully."