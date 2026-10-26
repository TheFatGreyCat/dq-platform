#!/bin/sh
# Supervisor: worker anomaly (127.0.0.1:8081) + uvicorn (8000). Một process chết -> dừng cả container.
set -u
java -cp '/opt/dq-worker/dq-anomaly-worker-0.1.0.jar:/opt/dq-worker/lib/*' dqplatform.worker.AnomalyWorker --port 8081 &
WORKER=$!
uvicorn app:app_from_env --factory --host 0.0.0.0 --port 8000 &
API=$!

trap 'kill "$WORKER" "$API" 2>/dev/null; wait; exit 143' TERM INT

# POSIX sh không có `wait -n`: poll nhẹ
while kill -0 "$WORKER" 2>/dev/null && kill -0 "$API" 2>/dev/null; do
  sleep 1
done
kill "$WORKER" "$API" 2>/dev/null
wait "$WORKER" 2>/dev/null; rc_w=$?
wait "$API" 2>/dev/null; rc_a=$?
echo "start.sh: một process đã thoát (worker=$rc_w api=$rc_a)" >&2
[ "$rc_w" -ne 0 ] && exit "$rc_w"
[ "$rc_a" -ne 0 ] && exit "$rc_a"
exit 1
