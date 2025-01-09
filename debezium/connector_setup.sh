#!/usr/bin/env bash
# Register the Postgres Debezium connector after docker compose is up.
# Run from the host; expects published port 8083 (Kafka Connect).

set -e
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/connector_config.json"
CONNECTOR_NAME="postgres-cdc-connector"

CONNECT_URL="http://localhost:8083/connectors"
STATUS_URL="http://localhost:8083/connectors/${CONNECTOR_NAME}/status"

# Poll URL until HTTP succeeds or timeout. Fails if Connect never left startup
# (unhealthy compose deps, wrong port mapping, or service crash loop).
wait_for_url() {
  local url=$1
  local timeout_sec=$2
  local elapsed=0

  echo -n "Waiting for ${url} (timeout ${timeout_sec}s)"
  while [[ ${elapsed} -lt ${timeout_sec} ]]; do
    if curl -sf "${url}" >/dev/null 2>&1; then
      echo " ok"
      return 0
    fi
    printf "."
    sleep 2
    elapsed=$((elapsed + 2))
  done

  echo ""
  echo "ERROR: timed out after ${timeout_sec}s waiting for ${url}" >&2
  echo "Check: docker compose ps, debezium logs, and that port 8083 is published." >&2
  exit 1
}

# Connect REST must answer before POST; 120s allows Kafka + ZK + Connect internal topics.
wait_for_url "${CONNECT_URL}" 120

# Idempotent: remove existing connector so POST always applies current connector_config.json.
CONNECTOR_URL="${CONNECT_URL}/${CONNECTOR_NAME}"
if curl -sf "${CONNECTOR_URL}" >/dev/null 2>&1; then
  echo "Connector already exists, deleting it first..."
  curl -X DELETE "${CONNECTOR_URL}"
  sleep 2
fi

echo "Registering connector from ${CONFIG_FILE}..."
register_tmp="$(mktemp)"
http_code="$(
  curl -s -w "%{http_code}" -o "${register_tmp}" -X POST \
    -H "Content-Type: application/json" \
    --data @"${CONFIG_FILE}" \
    "${CONNECT_URL}"
)"
register_body="$(cat "${register_tmp}")"
rm -f "${register_tmp}"

if [[ "${http_code}" != "201" ]]; then
  echo "ERROR: connector registration returned HTTP ${http_code} (expected 201)" >&2
  echo "${register_body}" >&2
  echo "Common causes: invalid JSON, duplicate name, Postgres unreachable from Connect, missing replication slot permissions." >&2
  exit 1
fi
echo "Connector registered (HTTP 201)."

# Connector RUNNING only means the worker accepted config; tasks can still be FAILED (e.g. DB auth, slot).
wait_for_connector_running() {
  local timeout_sec=$1
  local elapsed=0
  local state=""

  echo -n "Waiting for connector state=RUNNING (timeout ${timeout_sec}s)"
  while [[ ${elapsed} -lt ${timeout_sec} ]]; do
    state="$(
      curl -sf "${STATUS_URL}" 2>/dev/null \
        | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('connector',{}).get('state',''))" \
        2>/dev/null || true
    )"
    if [[ "${state}" == "RUNNING" ]]; then
      echo ""
      return 0
    fi
    printf "."
    sleep 2
    elapsed=$((elapsed + 2))
  done

  echo ""
  echo "ERROR: connector not RUNNING after ${timeout_sec}s (last state: ${state:-unknown})" >&2
  echo "Inspect: curl -s ${STATUS_URL} | python3 -m json.tool" >&2
  echo "And Connect logs: docker compose logs debezium" >&2
  exit 1
}

wait_for_connector_running 60

echo "Connector is RUNNING!"
echo "Waiting for task to be created..."
sleep 5

# Task must be RUNNING too — connector-level RUNNING hides snapshot/WAL failures in tasks[0].
TASK_STATE="$(curl -s "${STATUS_URL}" | \
  python3 -c "import sys, json; data=json.load(sys.stdin); print(data['tasks'][0]['state'] if data.get('tasks') else 'NONE')")"

if [[ "${TASK_STATE}" != "RUNNING" ]]; then
  echo "ERROR: task state is ${TASK_STATE} (expected RUNNING)" >&2
  curl -s "${STATUS_URL}" | python3 -m json.tool
  exit 1
fi

# Topics are created by Connect/Debezium after snapshot begins; list via broker inside compose network.
echo ""
echo "Kafka topics:"
docker compose -f "${PROJECT_DIR}/docker-compose.yml" exec -T kafka \
  kafka-topics --bootstrap-server localhost:9092 --list

echo ""
echo "SUCCESS: ${CONNECTOR_NAME} connector and task are RUNNING."
echo "  Connect:  ${CONNECT_URL}/${CONNECTOR_NAME}"
echo "  Status:   ${STATUS_URL}"
echo "  Config:   ${CONFIG_FILE}"
