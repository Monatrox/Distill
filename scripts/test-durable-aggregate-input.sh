#!/usr/bin/env bash
set -euo pipefail

if ! command -v curl >/dev/null 2>&1; then
    echo 'required command is unavailable: curl' >&2
    exit 1
fi
for candidate in podman docker; do
    if command -v "$candidate" >/dev/null 2>&1 && "$candidate" info >/dev/null 2>&1; then
        engine=$candidate
        break
    fi
done
: "${engine:?Podman or Docker service is unavailable or inaccessible}"
fixture_uid="$(id -u)"
fixture_gid="$(id -g)"

name="distill-handoff-${RANDOM}${RANDOM}"
network="$name"
rabbit="$name-rabbit"
postgres="$name-postgres"
first="$name-first"
second="$name-second"
third="$name-third"
fourth="$name-fourth"
fifth="$name-fifth"
sixth="$name-sixth"
seventh="$name-seventh"
image="$name-image"
config="$(mktemp)"

cleanup() {
    "$engine" rm -f "$first" "$second" "$third" "$fourth" "$fifth" "$sixth" "$seventh" "$rabbit" "$postgres" >/dev/null 2>&1 || true
    "$engine" network rm "$network" >/dev/null 2>&1 || true
    "$engine" image rm "$image" >/dev/null 2>&1 || true
    rm -f "$config"
}
trap cleanup EXIT

port_for() {
    "$engine" port "$1" 8080/tcp | head -n 1 | awk -F: '{print $NF}'
}

wait_for_http() {
    local port="$1"
    for _ in $(seq 1 60); do
        if curl --silent --fail --header 'X-API-Key: ingest-key' "http://127.0.0.1:$port/health" >/dev/null; then
            return
        fi
        sleep 1
    done
    "$engine" logs "$2"
    return 1
}

start_distill() {
    local container="$1"
    local shutdown_timeout="${2:-30}"
    "$engine" run --detach --name "$container" --network "$network" \
        --user "$fixture_uid:$fixture_gid" \
        --publish 127.0.0.1::8080 \
        --mount "type=bind,src=$config,dst=/config/channels.json,readonly" \
        --env RABBITMQ_URI='amqp://guest:guest@rabbit:5672/%2f' \
        --env DISTILL_INGEST_API_KEY=ingest-key \
        --env DISTILL_ADMIN_API_KEY=admin-key \
        --env DISTILL_BIND_ADDRESS=0.0.0.0:8080 \
        --env DISTILL_CONFIG_PATH=/config/channels.json \
        --env DISTILL_SHUTDOWN_TIMEOUT_SECONDS="$shutdown_timeout" \
        "$image" >/dev/null
}

post_event() {
    local port="$1"
    local channel_id="$2"
    local severity="$3"
    local event_id="$4"
    local status
    status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
        --header 'Content-Type: application/json' \
        --header 'X-API-Key: ingest-key' \
        --data "[{\"event_id\":\"$event_id\",\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"channel_id\":\"$channel_id\",\"severity_level\":\"$severity\",\"payload\":{}}]" \
        "http://127.0.0.1:$port/ingest")"
    test "$status" = 202
}

wait_for_sql() {
    local expected="$1"
    local query="$2"
    local description="$3"
    local result
    for _ in $(seq 1 60); do
        result="$("$engine" exec "$postgres" psql --username distill --dbname distill --tuples-only --no-align --command "$query")"
        if test "$result" = "$expected"; then
            return
        fi
        sleep 1
    done
    echo "expected $description '$expected', got '${result:-none}'" >&2
    return 1
}

wait_for_unacknowledged_aggregate() {
    local channel_id="$1"
    local queues
    for _ in $(seq 1 60); do
        queues="$("$engine" exec "$rabbit" rabbitmqctl list_queues -q name messages messages_unacknowledged 2>/dev/null || true)"
        if awk -v prefix="distill.aggregate.$channel_id." '$1 ~ "^" prefix && $2 == 1 && $3 == 1 { found = 1 } END { exit !found }' <<<"$queues"; then
            return
        fi
        sleep 1
    done
    echo "$queues" >&2
    echo "aggregate input for $channel_id was not buffered before shutdown" >&2
    return 1
}

"$engine" network create "$network" >/dev/null
"$engine" run --detach --name "$postgres" --network "$network" --network-alias postgres \
    --env POSTGRES_DB=distill \
    --env POSTGRES_USER=distill \
    --env POSTGRES_PASSWORD=password \
    docker.io/library/postgres:16-alpine >/dev/null
"$engine" run --detach --name "$rabbit" --network "$network" --network-alias rabbit docker.io/library/rabbitmq:3.13-alpine >/dev/null

for _ in $(seq 1 60); do
    if "$engine" exec "$postgres" pg_isready --username distill --dbname distill >/dev/null 2>&1 \
        && "$engine" exec "$rabbit" rabbitmq-diagnostics -q ping >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
"$engine" exec "$postgres" pg_isready --username distill --dbname distill >/dev/null
"$engine" exec "$rabbit" rabbitmq-diagnostics -q ping >/dev/null

cat >"$config" <<'JSON'
{
  "channels": [
    {
      "channel_id": "handoff.aggregate",
      "severity_threshold": "error",
      "rabbitmq_config": {"connection_string": "amqp://guest:guest@rabbit:5672/%2f", "exchange_name": "distill.handoff.aggregate", "routing_key_prefix": "handoff."},
      "sink_config": {"driver": "postgres", "connection_string": "postgres://distill:password@postgres:5432/distill"},
      "aggregator": {"function": "count", "granularity": "per_n_events", "parameter": 2},
      "buffer_bytes": 1024,
      "ttl_seconds": 3600,
      "mailbox_capacity": 8
    },
    {
      "channel_id": "handoff.direct",
      "severity_threshold": "error",
      "rabbitmq_config": {"connection_string": "amqp://guest:guest@rabbit:5672/%2f", "exchange_name": "distill.handoff.direct", "routing_key_prefix": "handoff."},
      "sink_config": {"driver": "postgres", "connection_string": "postgres://distill:password@postgres:5432/distill"},
      "buffer_bytes": 1024,
      "ttl_seconds": 3600,
      "mailbox_capacity": 8
    },
    {
      "channel_id": "handoff.persist-and-aggregate",
      "severity_threshold": "error",
      "rabbitmq_config": {"connection_string": "amqp://guest:guest@rabbit:5672/%2f", "exchange_name": "distill.handoff.persist-and-aggregate", "routing_key_prefix": "handoff."},
      "sink_config": {"driver": "postgres", "connection_string": "postgres://distill:password@postgres:5432/distill"},
      "aggregator": {"function": "count", "granularity": "per_n_events", "parameter": 2},
      "buffer_bytes": 1024,
      "ttl_seconds": 3600,
      "mailbox_capacity": 8
    },
    {
      "channel_id": "handoff.shutdown",
      "severity_threshold": "error",
      "rabbitmq_config": {"connection_string": "amqp://guest:guest@rabbit:5672/%2f", "exchange_name": "distill.handoff.shutdown", "routing_key_prefix": "handoff."},
      "sink_config": {"driver": "postgres", "connection_string": "postgres://distill:password@postgres:5432/distill"},
      "aggregator": {"function": "count", "granularity": "per_n_events", "parameter": 2},
      "buffer_bytes": 1024,
      "ttl_seconds": 3600,
      "mailbox_capacity": 8
    },
    {
      "channel_id": "handoff.shutdown-timeout",
      "severity_threshold": "error",
      "rabbitmq_config": {"connection_string": "amqp://guest:guest@rabbit:5672/%2f", "exchange_name": "distill.handoff.shutdown-timeout", "routing_key_prefix": "handoff."},
      "sink_config": {"driver": "postgres", "connection_string": "postgres://distill:password@postgres:5432/distill"},
      "aggregator": {"function": "count", "granularity": "per_n_events", "parameter": 2},
      "buffer_bytes": 1024,
      "ttl_seconds": 3600,
      "mailbox_capacity": 8
    }
  ]
}
JSON
# Keep embedded test credentials owner-only and run the fixture as their non-root owner.
chmod 0600 "$config"
"$engine" build --tag "$image" . >/dev/null
aggregate_first=00000000-0000-0000-0000-000000000001
aggregate_second=00000000-0000-0000-0000-000000000002
direct_event=00000000-0000-0000-0000-000000000003
persist_and_aggregate_first=00000000-0000-0000-0000-000000000004
persist_and_aggregate_second=00000000-0000-0000-0000-000000000005
shutdown_event=00000000-0000-0000-0000-000000000006
shutdown_timeout_event=00000000-0000-0000-0000-000000000007

start_distill "$first"
first_port="$(port_for "$first")"
wait_for_http "$first_port" "$first"
post_event "$first_port" handoff.aggregate info "$aggregate_first"
"$engine" kill "$first" >/dev/null

start_distill "$second"
second_port="$(port_for "$second")"
wait_for_http "$second_port" "$second"
post_event "$second_port" handoff.aggregate info "$aggregate_second"
wait_for_sql 2 "SELECT event_count FROM distill_aggregates WHERE channel_id = 'handoff.aggregate'" 'recovered aggregate event count'

post_event "$second_port" handoff.direct info "$direct_event"
"$engine" kill "$second" >/dev/null

start_distill "$third"
third_port="$(port_for "$third")"
wait_for_http "$third_port" "$third"
wait_for_sql 1 "SELECT COUNT(*) FROM distill_events WHERE event_id = '$direct_event'" 'recovered direct event count'

post_event "$third_port" handoff.persist-and-aggregate error "$persist_and_aggregate_first"
"$engine" kill "$third" >/dev/null

start_distill "$fourth"
fourth_port="$(port_for "$fourth")"
wait_for_http "$fourth_port" "$fourth"
post_event "$fourth_port" handoff.persist-and-aggregate error "$persist_and_aggregate_second"
wait_for_sql 2 "SELECT COUNT(*) FROM distill_events WHERE event_id IN ('$persist_and_aggregate_first', '$persist_and_aggregate_second')" 'recovered persist-and-aggregate direct event count'
wait_for_sql 2 "SELECT event_count FROM distill_aggregates WHERE channel_id = 'handoff.persist-and-aggregate'" 'recovered persist-and-aggregate aggregate event count'
"$engine" kill "$fourth" >/dev/null

start_distill "$fifth"
fifth_port="$(port_for "$fifth")"
wait_for_http "$fifth_port" "$fifth"
post_event "$fifth_port" handoff.shutdown info "$shutdown_event"
wait_for_unacknowledged_aggregate handoff.shutdown
"$engine" stop --time 10 "$fifth" >/dev/null
test "$("$engine" inspect --format '{{.State.ExitCode}}' "$fifth")" = 0

start_distill "$sixth"
sixth_port="$(port_for "$sixth")"
wait_for_http "$sixth_port" "$sixth"
wait_for_sql 1 "SELECT event_count FROM distill_aggregates WHERE channel_id = 'handoff.shutdown'" 'SIGTERM-flushed aggregate event count'
"$engine" stop --time 10 "$sixth" >/dev/null

start_distill "$seventh" 1
seventh_port="$(port_for "$seventh")"
wait_for_http "$seventh_port" "$seventh"
post_event "$seventh_port" handoff.shutdown-timeout info "$shutdown_timeout_event"
wait_for_unacknowledged_aggregate handoff.shutdown-timeout
"$engine" stop --time 10 "$rabbit" >/dev/null
"$engine" stop --time 5 "$seventh" >/dev/null
test "$("$engine" inspect --format '{{.State.ExitCode}}' "$seventh")" = 1
timeout_log="$("$engine" logs "$seventh" 2>&1)"
grep -Fq 'flushing channel buffers' <<<"$timeout_log"
! grep -Eq 'guest:guest|ingest-key|admin-key|password|00000000-0000-0000-0000-000000000007' <<<"$timeout_log"

echo 'durable handoff and SIGTERM flush/timeout verified'
