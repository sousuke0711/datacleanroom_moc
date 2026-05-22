#!/usr/bin/env bash
# Flink ジョブを JobManager コンテナに投入するスクリプト。
# 実行方法: bash scripts/submit_job.sh

set -euo pipefail

CONTAINER="flink-jobmanager"
JOB_DIR="/opt/flink/job"

echo "[..] Submitting anonymization_job.py to Flink ..."

docker compose exec "$CONTAINER" flink run \
    -py "$JOB_DIR/anonymization_job.py" \
    -pyFiles "$JOB_DIR/openmetadata_client.py,$JOB_DIR/anonymizer.py" \
    --detached

echo "[OK] Job submitted. Check Flink UI → http://localhost:8081"
echo ""
echo "Iceberg sink を起動する場合:"
echo "  docker compose exec $CONTAINER /opt/flink/bin/sql-client.sh -f $JOB_DIR/iceberg_sink.sql"
