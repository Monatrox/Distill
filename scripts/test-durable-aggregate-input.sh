#!/usr/bin/env bash
set -euo pipefail

for command in docker curl; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "required command is unavailable: $command" >&2
        exit 1
    fi
done
if ! docker info >/dev/null 2>&1; then
    echo 'Docker daemon is unavailable or inaccessible' >&2
    exit 1
fi

name="distill-handoff-${RANDOM}${RANDOM}"
network="$name"
rabbit="$name-rabbit"
postgres="$name-postgres"
first="$name-first"
second="$name-second"
image="$name-image"
config="$(mktemp)"

cleanup() {
    docker rm -f "$first" "$second" "$rabbit" "$postgres" >/dev/null 2>&1 || true
    docker network rm "$network" >/dev/null 2>&1 || true
    docker image rm "$image" >/dev/null 2>&1 || true
    rm -f "$config"
}
trap cleanup EXIT

port_for() {
    docker port "$1" 8080/tcp | head -n 1 | awk -F: '{print $NF}'
}

wait_for_http() {
    local port="$1"
    for _ in $(seq 1 60); do
        if curl --silent --fail --header 'X-API-Key: ingest-key' "http://127.0.0.1:$port/health" >/dev/null; then
            return
        fi
        sleep 1
    done
    docker logs "$2"
    return 1
}

start_distill() {
    local container="$1"
    docker run --detach --name "$container" --network "$network" \
        --publish 127.0.0.1::8080 \
        --mount "type=bind,src=$config,dst=/config/channels.json,readonly" \
        --env RABBITMQ_URI='amqp://guest:guest@rabbit:5672/%2f' \
        --env DISTILL_INGEST_API_KEY=ingest-key \
        --env DISTILL_ADMIN_API_KEY=admin-key \
        --env DISTILL_BIND_ADDRESS=0.0.0.0:8080 \
        --env DISTILL_CONFIG_PATH=/config/channels.json \
        "$image" >/dev/null
}

post_event() {
    local port="$1"
    local status
    status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
        --header 'Content-Type: application/json' \
        --header 'X-API-Key: ingest-key' \
        --data "[{\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"channel_id\":\"handoff.test\",\"severity_level\":\"info\",\"payload\":{}}]" \
        "http://127.0.0.1:$port/ingest")"
    test "$status" = 202
}

docker network create "$network" >/dev/null
docker run --detach --name "$postgres" --network "$network" \
    --env POSTGRES_DB=distill \
    --env POSTGRES_USER=distill \
    --env POSTGRES_PASSWORD=password \
    postgres:16-alpine >/dev/null
docker run --detach --name "$rabbit" --network "$network" rabbitmq:3.13-alpine >/dev/null

for _ in $(seq 1 60); do
    if docker exec "$postgres" pg_isready --username distill --dbname distill >/dev/null 2>&1 \
        && docker exec "$rabbit" rabbitmq-diagnostics -q ping >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
docker exec "$postgres" pg_isready --username distill --dbname distill >/dev/null
docker exec "$rabbit" rabbitmq-diagnostics -q ping >/dev/null

cat >"$config" <<'JSON'
{
  "channels": [{
    "channel_id": "handoff.test",
    "severity_threshold": "error",
    "rabbitmq_config": {
      "connection_string": "amqp://guest:guest@rabbit:5672/%2f",
      "exchange_name": "distill.handoff.test",
      "routing_key_prefix": "handoff."
    },
    "sink_config": {
      "driver": "postgres",
      "connection_string": "postgres://distill:password@postgres:5432/distill"
    },
    "aggregator": {"function": "count", "granularity": "per_n_events", "parameter": 2},
    "buffer_bytes": 1024,
    "ttl_seconds": 3600,
    "mailbox_capacity": 8
  }]
}
JSON
docker build --tag "$image" . >/dev/null
start_distill "$first"
first_port="$(port_for "$first")"
wait_for_http "$first_port" "$first"
post_event "$first_port"
docker kill "$first" >/dev/null

start_distill "$second"
second_port="$(port_for "$second")"
wait_for_http "$second_port" "$second"
post_event "$second_port"

for _ in $(seq 1 60); do
    count="$(docker exec "$postgres" psql --username distill --dbname distill --tuples-only --no-align --command "SELECT event_count FROM distill_aggregates WHERE channel_id = 'handoff.test' ORDER BY window_end DESC LIMIT 1")"
    if test "$count" = 2; then
        echo 'durable aggregate-input handoff verified'
        exit 0
    fi
    sleep 1
done

docker logs "$second"
echo "expected recovered aggregate event_count 2, got '${count:-none}'" >&2
exit 1
