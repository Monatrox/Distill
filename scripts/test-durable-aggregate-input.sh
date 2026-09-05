#!/usr/bin/env bash
set -euo pipefail

for required_command in base64 curl; do
    if ! command -v "$required_command" >/dev/null 2>&1; then
        echo "required command is unavailable: $required_command" >&2
        exit 1
    fi
done
requested_engine="${DISTILL_CONTAINER_ENGINE:-}"
if test -n "$requested_engine"; then
    candidates=("$requested_engine")
else
    candidates=(docker podman)
fi
for candidate in "${candidates[@]}"; do
    if command -v "$candidate" >/dev/null 2>&1 && "$candidate" info >/dev/null 2>&1; then
        engine=$candidate
        break
    fi
done
: "${engine:?Podman or Docker service is unavailable or inaccessible}"
fixture_uid="$(id -u)"
fixture_gid="$(id -g)"
runtime_user_args=(--user "$fixture_uid:$fixture_gid")
if test "$engine" = podman; then
    runtime_user_args=(--userns=keep-id --user "$fixture_uid:$fixture_gid")
fi

name="distill-handoff-${RANDOM}${RANDOM}"
network="$name"
rabbit="$name-rabbit"
postgres="$name-postgres"
receiver="$name-receiver"
first="$name-first"
second="$name-second"
third="$name-third"
fourth="$name-fourth"
fifth="$name-fifth"
sixth="$name-sixth"
seventh="$name-seventh"
image="$name-image"
config="$(mktemp)"
artifact_dir="${DISTILL_TEST_ARTIFACT_DIR:-}"
containers=("$first" "$second" "$third" "$fourth" "$fifth" "$sixth" "$seventh" "$rabbit" "$postgres" "$receiver")

redact_diagnostics() {
    sed \
        -e 's/guest:guest/[REDACTED]/g' \
        -e 's/ingest-key/[REDACTED]/g' \
        -e 's/admin-key/[REDACTED]/g' \
        -e 's/password/[REDACTED]/g'
}

container_running() {
    test "$("$engine" inspect --format '{{.State.Running}}' "$1" 2>/dev/null || true)" = true
}

container_exec() {
    local container="$1"
    shift
    if ! container_running "$container"; then
        echo "container '$container' exited before required command: $*" >&2
        "$engine" inspect --format 'status={{.State.Status}} exit_code={{.State.ExitCode}} error={{.State.Error}}' "$container" 2>/dev/null | redact_diagnostics >&2 || true
        return 1
    fi
    "$engine" exec "$container" "$@"
}

capture_diagnostics() {
    test -n "$artifact_dir" || return 0
    mkdir -p "$artifact_dir"
    "$engine" ps -a --filter "name=$name" >"$artifact_dir/containers.txt" 2>&1 || true
    for container in "${containers[@]}"; do
        if "$engine" inspect "$container" >/dev/null 2>&1; then
            "$engine" inspect "$container" 2>&1 | redact_diagnostics >"$artifact_dir/$container-inspect.json" || true
            "$engine" logs "$container" 2>&1 | redact_diagnostics >"$artifact_dir/$container.log" || true
        fi
    done
}

cleanup() {
    local exit_code=$?
    set +e
    if test "$exit_code" -ne 0; then
        capture_diagnostics
    fi
    "$engine" rm -f "$first" "$second" "$third" "$fourth" "$fifth" "$sixth" "$seventh" "$rabbit" "$postgres" "$receiver" >/dev/null 2>&1 || true
    "$engine" network rm "$network" >/dev/null 2>&1 || true
    "$engine" image rm "$image" >/dev/null 2>&1 || true
    rm -f "$config"
    return "$exit_code"
}
trap cleanup EXIT

port_for() {
    local target_port="${2:-8080}"
    "$engine" port "$1" "$target_port/tcp" | head -n 1 | awk -F: '{print $NF}'
}

wait_for_http() {
    local port="$1"
    for _ in $(seq 1 60); do
        if curl --silent --fail --header 'X-API-Key: ingest-key' "http://127.0.0.1:$port/health" >/dev/null; then
            return
        fi
        sleep 1
    done
    "$engine" logs "$2" 2>&1 | redact_diagnostics
    return 1
}

start_distill() {
    local container="$1"
    local shutdown_timeout="${2:-30}"
    "$engine" run --detach --name "$container" --network "$network" \
        "${runtime_user_args[@]}" \
        --publish 127.0.0.1::8080 \
        --publish 127.0.0.1::8081 \
        --mount "type=bind,src=$config,dst=/config/channels.json,readonly" \
        --env RABBITMQ_URI='amqp://guest:guest@rabbit:5672/%2f' \
        --env DISTILL_INGEST_API_KEY=ingest-key \
        --env DISTILL_ADMIN_API_KEY=admin-key \
        --env DISTILL_BIND_ADDRESS=0.0.0.0:8080 \
        --env DISTILL_ADMIN_BIND_ADDRESS=0.0.0.0:8081 \
        --env DISTILL_ALLOW_ADMIN_NETWORK=true \
        --env DISTILL_CONFIG_PATH=/config/channels.json \
        --env DISTILL_SINK_ALLOWLIST=unhealthy:5678 \
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

post_event_expect_failure() {
    local port="$1"
    local channel_id="$2"
    local event_id="$3"
    local status
    status="$(curl --silent --max-time 35 --output /dev/null --write-out '%{http_code}' \
        --header 'Content-Type: application/json' \
        --header 'X-API-Key: ingest-key' \
        --data "[{\"event_id\":\"$event_id\",\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"channel_id\":\"$channel_id\",\"severity_level\":\"info\",\"payload\":{}}]" \
        "http://127.0.0.1:$port/ingest" || true)"
    test "$status" != 202
}

post_event_eventually() {
    local port="$1"
    local channel_id="$2"
    local severity="$3"
    local event_id="$4"
    for _ in $(seq 1 60); do
        if post_event "$port" "$channel_id" "$severity" "$event_id"; then
            return
        fi
        sleep 1
    done
    echo "broker recovery did not restore admission for $channel_id" >&2
    return 1
}

assert_queue_limits() {
    local queues
    queues="$(container_exec "$rabbit" rabbitmqctl list_queues -q name arguments)"
    for queue in distill.persistence distill.dead-letter.queue; do
        local row
        row="$(grep -F "$queue" <<<"$queues")"
        grep -Fq 'x-max-length' <<<"$row"
        grep -Fq '1000000' <<<"$row"
        grep -Fq 'x-max-length-bytes' <<<"$row"
        grep -Fq '1073741824' <<<"$row"
        grep -Fq 'reject-publish' <<<"$row"
    done
    local aggregate_row
    aggregate_row="$(grep -m 1 -F 'distill.aggregate.handoff.aggregate.' <<<"$queues")"
    grep -Fq '1000000' <<<"$aggregate_row"
    grep -Fq '1073741824' <<<"$aggregate_row"
    grep -Fq 'reject-publish' <<<"$aggregate_row"
}

wait_for_retry_topology() {
    local queues
    for _ in $(seq 1 60); do
        queues="$(container_exec "$rabbit" rabbitmqctl list_queues -q name arguments)"
        if grep -Fq 'distill.retry.' <<<"$queues"; then
            local row
            row="$(grep -m 1 -F 'distill.retry.' <<<"$queues")"
            grep -Fq 'x-message-ttl' <<<"$row"
            grep -Fq 'x-queue-type' <<<"$row"
            grep -Fq 'quorum' <<<"$row"
            grep -Fq 'x-dead-letter-strategy' <<<"$row"
            grep -Fq 'at-least-once' <<<"$row"
            grep -Fq '1000000' <<<"$row"
            grep -Fq '1073741824' <<<"$row"
            grep -Fq 'reject-publish' <<<"$row"
            return
        fi
        sleep 1
    done
    echo 'retry queue topology was not declared' >&2
    return 1
}

wait_for_receiver_post() {
    local logs
    for _ in $(seq 1 90); do
        logs="$("$engine" logs "$receiver" 2>&1 || true)"
        if grep -Fq 'POST' <<<"$logs"; then
            return
        fi
        sleep 1
    done
    echo 'retry delivery did not reach the recovered HTTP receiver' >&2
    return 1
}

wait_for_dlq_count() {
    local expected="$1"
    local count
    for _ in $(seq 1 60); do
        count="$(container_exec "$rabbit" rabbitmqctl list_queues -q name messages | awk '$1 == "distill.dead-letter.queue" { print $2 }')"
        if test "$count" = "$expected"; then
            return
        fi
        sleep 1
    done
    echo "expected DLQ count $expected, got ${count:-missing}" >&2
    return 1
}

run_dlq() {
    "$engine" run --rm --network "$network" \
        --env RABBITMQ_URI='amqp://guest:guest@rabbit:5672/%2f' \
        "$image" dlq "$@"
}

replace_aggregate_channel() {
    local admin_port="$1"
    local parameter="$2"
    local status
    status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
        --request PUT \
        --header 'Content-Type: application/json' \
        --header 'X-API-Key: admin-key' \
        --data "{\"channel_id\":\"handoff.aggregate\",\"severity_threshold\":\"error\",\"rabbitmq_config\":{\"connection_string\":\"amqp://guest:guest@rabbit:5672/%2f\",\"exchange_name\":\"distill.handoff.aggregate\",\"routing_key_prefix\":\"handoff.\"},\"sink_config\":{\"driver\":\"postgres\",\"connection_string\":\"postgres://distill:password@postgres:5432/distill\"},\"aggregator\":{\"function\":\"count\",\"granularity\":\"per_n_events\",\"parameter\":$parameter},\"buffer_bytes\":1024,\"ttl_seconds\":3600,\"mailbox_capacity\":8}" \
        "http://127.0.0.1:$admin_port/channels/handoff.aggregate")"
    test "$status" = 204
}

wait_for_single_generation_queue() {
    local retired_queue="$1"
    local queues
    for _ in $(seq 1 60); do
        queues="$(container_exec "$rabbit" rabbitmqctl list_queues -q name)"
        if ! grep -Fxq "$retired_queue" <<<"$queues" \
            && test "$(grep -Fc 'distill.aggregate.handoff.aggregate.' <<<"$queues")" = 1; then
            return
        fi
        sleep 1
    done
    echo 'replaced aggregate generation was not retired' >&2
    echo "$queues" >&2
    return 1
}

wait_for_sql() {
    local expected="$1"
    local query="$2"
    local description="$3"
    local result
    for _ in $(seq 1 60); do
        result="$(container_exec "$postgres" psql --username distill --dbname distill --tuples-only --no-align --command "$query")"
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
        queues="$(container_exec "$rabbit" rabbitmqctl list_queues -q name messages messages_unacknowledged)"
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
"$engine" run --detach --name "$rabbit" --network "$network" --network-alias rabbit docker.io/library/rabbitmq:3.13-management-alpine >/dev/null

for _ in $(seq 1 60); do
    if container_exec "$postgres" pg_isready --username distill --dbname distill >/dev/null 2>&1 \
        && container_exec "$rabbit" rabbitmq-diagnostics -q ping >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
container_exec "$postgres" pg_isready --username distill --dbname distill >/dev/null
container_exec "$rabbit" rabbitmq-diagnostics -q ping >/dev/null

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
    },
    {
      "channel_id": "handoff.retry",
      "severity_threshold": "error",
      "rabbitmq_config": {"connection_string": "amqp://guest:guest@rabbit:5672/%2f", "exchange_name": "distill.handoff.retry", "routing_key_prefix": "handoff."},
      "sink_config": {"driver": "http", "connection_string": "http://unhealthy:5678"},
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
broker_unavailable_event=00000000-0000-0000-0000-000000000008
broker_recovered_direct=00000000-0000-0000-0000-000000000009
broker_recovered_aggregate_first=00000000-0000-0000-0000-00000000000a
broker_recovered_aggregate_second=00000000-0000-0000-0000-00000000000b
broker_recovered_aggregate_third=00000000-0000-0000-0000-000000000013
broker_recovered_aggregate_fourth=00000000-0000-0000-0000-000000000014
retry_event=00000000-0000-0000-0000-00000000000c
retry_isolation_event=00000000-0000-0000-0000-00000000000d
poison_followup_event=00000000-0000-0000-0000-00000000000e
replay_event=00000000-0000-0000-0000-00000000000f
overflow_event=00000000-0000-0000-0000-000000000010
retirement_first=00000000-0000-0000-0000-000000000011
retirement_second=00000000-0000-0000-0000-000000000012
replay_dlq_id=10000000-0000-0000-0000-000000000002

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
sixth_admin_port="$(port_for "$sixth" 8081)"
wait_for_http "$sixth_port" "$sixth"
wait_for_sql 1 "SELECT event_count FROM distill_aggregates WHERE channel_id = 'handoff.shutdown'" 'SIGTERM-flushed aggregate event count'
assert_queue_limits
post_event "$sixth_port" handoff.retry info "$retry_event"
post_event "$sixth_port" handoff.direct info "$retry_isolation_event"
wait_for_sql 1 "SELECT COUNT(*) FROM distill_events WHERE event_id = '$retry_isolation_event'" 'retry-isolated healthy event count'
wait_for_retry_topology
"$engine" run --detach --name "$receiver" --network "$network" --network-alias unhealthy docker.io/hashicorp/http-echo:1.0 -listen=:5678 -text=ok >/dev/null
wait_for_receiver_post
container_exec "$rabbit" rabbitmqadmin publish exchange=distill.handoff.direct routing_key=handoff.event payload=not-json >/dev/null
wait_for_dlq_count 1
post_event "$sixth_port" handoff.direct info "$poison_followup_event"
wait_for_sql 1 "SELECT COUNT(*) FROM distill_events WHERE event_id = '$poison_followup_event'" 'valid event after poison delivery'
dlq_list="$(run_dlq list --limit 10)"
grep -Fq 'invalid_envelope' <<<"$dlq_list"
! grep -Fq 'not-json' <<<"$dlq_list"
actual_poison_id="$(awk '$2 == "invalid_envelope" { print $1 }' <<<"$dlq_list")"
if run_dlq replay --id "$actual_poison_id" >/dev/null 2>&1; then
    echo 'poison DLQ record was replayed unexpectedly' >&2
    exit 1
fi
original_payload="{\"version\":1,\"payload\":{\"event_id\":\"$replay_event\",\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"channel_id\":\"handoff.direct\",\"severity_level\":\"info\",\"payload\":{},\"metadata\":{}},\"retry_count\":10}"
original_base64="$(printf '%s' "$original_payload" | base64 -w0)"
replay_wrapper="{\"metadata\":{\"id\":\"$replay_dlq_id\",\"envelope_version\":1,\"failure_category\":\"retries_exhausted\",\"failure_reason\":\"delivery failed after maximum retry attempts\",\"retry_count\":10,\"original_message_id\":\"$replay_event\",\"original_exchange\":\"distill.handoff.direct\",\"original_routing_key\":\"handoff.event\",\"failed_at\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"},\"original_data_base64\":\"$original_base64\"}"
container_exec "$rabbit" rabbitmqadmin publish exchange=distill.dead-letter routing_key=handoff.event payload="$replay_wrapper" >/dev/null
wait_for_dlq_count 2
run_dlq replay --id "$replay_dlq_id" >/dev/null
wait_for_sql 1 "SELECT COUNT(*) FROM distill_events WHERE event_id = '$replay_event'" 'confirmed PostgreSQL DLQ replay'
wait_for_dlq_count 1
container_exec "$rabbit" rabbitmqadmin declare queue name=distill.overflow.probe durable=false arguments='{"x-max-length":1,"x-overflow":"reject-publish"}' >/dev/null
container_exec "$rabbit" rabbitmqadmin publish exchange=amq.default routing_key=distill.overflow.probe payload=filler >/dev/null
container_exec "$rabbit" rabbitmqadmin declare binding source=distill.handoff.direct destination_type=queue destination=distill.overflow.probe routing_key=handoff.routed-event-v1 >/dev/null
post_event_expect_failure "$sixth_port" handoff.direct "$overflow_event"
container_exec "$rabbit" rabbitmqadmin delete queue name=distill.overflow.probe >/dev/null
aggregate_total="$(container_exec "$postgres" psql --username distill --dbname distill --tuples-only --no-align --command "SELECT COALESCE(SUM(event_count), 0) FROM distill_aggregates WHERE channel_id = 'handoff.aggregate'")"
retired_queue="$(container_exec "$rabbit" rabbitmqctl list_queues -q name | grep -m 1 -F 'distill.aggregate.handoff.aggregate.')"
post_event "$sixth_port" handoff.aggregate info "$retirement_first"
replace_aggregate_channel "$sixth_admin_port" 3
wait_for_sql "$((aggregate_total + 1))" "SELECT COALESCE(SUM(event_count), 0) FROM distill_aggregates WHERE channel_id = 'handoff.aggregate'" 'first replacement flush total'
wait_for_single_generation_queue "$retired_queue"
aggregate_total="$((aggregate_total + 1))"
retired_queue="$(container_exec "$rabbit" rabbitmqctl list_queues -q name | grep -m 1 -F 'distill.aggregate.handoff.aggregate.')"
post_event "$sixth_port" handoff.aggregate info "$retirement_second"
replace_aggregate_channel "$sixth_admin_port" 4
wait_for_sql "$((aggregate_total + 1))" "SELECT COALESCE(SUM(event_count), 0) FROM distill_aggregates WHERE channel_id = 'handoff.aggregate'" 'second replacement flush total'
wait_for_single_generation_queue "$retired_queue"
"$engine" stop --time 10 "$rabbit" >/dev/null
post_event_expect_failure "$sixth_port" handoff.direct "$broker_unavailable_event"
"$engine" start "$rabbit" >/dev/null
for _ in $(seq 1 60); do
    if container_exec "$rabbit" rabbitmq-diagnostics -q ping >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
container_exec "$rabbit" rabbitmq-diagnostics -q ping >/dev/null
post_event_eventually "$sixth_port" handoff.direct info "$broker_recovered_direct"
post_event_eventually "$sixth_port" handoff.aggregate info "$broker_recovered_aggregate_first"
post_event_eventually "$sixth_port" handoff.aggregate info "$broker_recovered_aggregate_second"
post_event_eventually "$sixth_port" handoff.aggregate info "$broker_recovered_aggregate_third"
post_event_eventually "$sixth_port" handoff.aggregate info "$broker_recovered_aggregate_fourth"
wait_for_sql 1 "SELECT COUNT(*) FROM distill_events WHERE event_id = '$broker_recovered_direct'" 'broker-recovered direct event count'
wait_for_sql 4 "SELECT event_count FROM distill_aggregates WHERE channel_id = 'handoff.aggregate' AND aggregate_id <> '00000000-0000-0000-0000-000000000000' ORDER BY window_end DESC LIMIT 1" 'broker-recovered aggregate event count'
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

echo 'durable handoff, retry isolation, DLQ replay, overflow rejection, generation retirement, broker restart, queue limits, and SIGTERM behavior verified'
