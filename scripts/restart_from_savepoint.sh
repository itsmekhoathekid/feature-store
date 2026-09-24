#!/bin/sh
set -eu

job_id=$(docker compose exec -T flink-jobmanager flink list -r \
  | awk -F ' : ' '/\(RUNNING\)$/ {print $2; exit}')

if [ -z "$job_id" ]; then
  echo "No running Flink job found" >&2
  exit 1
fi

stop_output=$(docker compose exec -T flink-jobmanager \
  flink stop --savepointPath file:///checkpoints/savepoints "$job_id")
printf '%s\n' "$stop_output"

savepoint=$(printf '%s\n' "$stop_output" \
  | sed -n 's/^Savepoint completed\. Path: \(.*\)$/\1/p' \
  | tail -n 1)

if [ -z "$savepoint" ]; then
  echo "Flink did not return a completed savepoint path" >&2
  exit 1
fi

docker compose exec -T flink-jobmanager \
  flink run -d -s "$savepoint" \
    -py /opt/flink/usrlib/customer_feature_job.py \
    -pyexec /opt/pyflink/bin/python
