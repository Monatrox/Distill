#!/usr/bin/env bash
# Exercises the same snapshot retry/reordering contract against every supported sink.
set -euo pipefail
engine="${DISTILL_CONTAINER_ENGINE:-podman}"
name="distill-sinks-${RANDOM}${RANDOM}"
work_dir="$(mktemp -d)"
receiver_pid=""
cleanup() {
    local result=$?
    if test -n "$receiver_pid"; then kill "$receiver_pid" 2>/dev/null || true; fi
    "$engine" rm -f "$name-pg" "$name-mysql" "$name-sql" >/dev/null 2>&1 || true
    rm -rf "$work_dir"
    return "$result"
}
trap cleanup EXIT
port_for() { "$engine" port "$1" "$2/tcp" | head -n1 | awk -F: '{print $NF}'; }
"$engine" run -d --name "$name-pg" -p 127.0.0.1::5432 -e POSTGRES_USER=distill -e POSTGRES_PASSWORD=contract-password -e POSTGRES_DB=distill docker.io/library/postgres:17 >/dev/null
"$engine" run -d --name "$name-mysql" -p 127.0.0.1::3306 -e MYSQL_ROOT_PASSWORD=contract-password -e MYSQL_DATABASE=distill docker.io/library/mysql:8.4 >/dev/null
"$engine" run -d --name "$name-sql" -p 127.0.0.1::1433 -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD=Contract-password123 -e MSSQL_PID=Developer mcr.microsoft.com/mssql/server:2022-latest >/dev/null
wait_for_service() {
    local container="$1"
    shift
    for _ in $(seq 1 120); do
        if "$engine" exec "$container" "$@" >/dev/null 2>&1; then return; fi
        if test "$("$engine" inspect --format '{{.State.Running}}' "$container")" != true; then
            echo "test database $container exited before readiness" >&2
            return 1
        fi
        sleep 1
    done
    echo "test database $container did not become ready" >&2
    return 1
}
wait_for_service "$name-pg" pg_isready -h 127.0.0.1 -U distill -d distill
wait_for_service "$name-mysql" mysqladmin --host=127.0.0.1 --protocol=tcp ping --password=contract-password
wait_for_service "$name-sql" /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P Contract-password123 -C -Q 'SELECT 1'
# Start with existing tables missing all V1 revision/fingerprint/version columns.
python3 scripts/seed-legacy-sink-schema.py postgres | "$engine" exec -i "$name-pg" psql -U distill -d distill -v ON_ERROR_STOP=1 >/dev/null
python3 scripts/seed-legacy-sink-schema.py mysql | "$engine" exec -i "$name-mysql" mysql --user=root --password=contract-password distill
python3 scripts/seed-legacy-sink-schema.py sqlserver | "$engine" exec -i "$name-sql" /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P Contract-password123 -C -b >/dev/null
export DISTILL_TEST_POSTGRES_URI="postgres://distill:contract-password@127.0.0.1:$(port_for "$name-pg" 5432)/distill"
export DISTILL_TEST_MYSQL_URI="mysql://root:contract-password@127.0.0.1:$(port_for "$name-mysql" 3306)/distill"
export DISTILL_TEST_SQLSERVER_URI="server=tcp:127.0.0.1,$(port_for "$name-sql" 1433);user=sa;password=Contract-password123;database=master;TrustServerCertificate=true"
python3 - "$work_dir/sinks.json" <<'MIGRATION_MANIFEST'
import json, os, sys
sinks = [{'sink_id': driver, 'driver': driver, 'connection_string_env': variable} for driver, variable in [('postgres', 'DISTILL_TEST_POSTGRES_URI'), ('mysql', 'DISTILL_TEST_MYSQL_URI'), ('sqlserver', 'DISTILL_TEST_SQLSERVER_URI')]]
with open(sys.argv[1], 'w') as stream: json.dump({'sinks': sinks}, stream)
os.chmod(sys.argv[1], 0o600)
MIGRATION_MANIFEST
for driver in postgres mysql sqlserver; do
    cargo run --offline --quiet -- sink migrate "$driver" --file "$work_dir/sinks.json"
done
http_port="$(python3 - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(('127.0.0.1', 0))
    print(sock.getsockname()[1])
PY
)"
DISTILL_RECEIVER_TOKEN=contract-token python3 examples/http_receiver.py --database "$work_dir/receiver.sqlite3" --port "$http_port" >"$work_dir/receiver.log" 2>&1 &
receiver_pid=$!
export DISTILL_TEST_HTTP_URL="http://127.0.0.1:$http_port/records"
for _ in $(seq 1 30); do
    if curl --silent --fail "http://127.0.0.1:$http_port/health" >/dev/null; then break; fi
    sleep 1
done
cargo test --offline persistence::contract_tests -- --ignored --test-threads=1

legacy_query="SELECT COUNT(*) FROM distill_events WHERE channel_id = 'migration.legacy'"
test "$("$engine" exec "$name-pg" psql -U distill -d distill -tA -c "$legacy_query")" = 1
test "$("$engine" exec "$name-mysql" mysql --user=root --password=contract-password --batch --skip-column-names distill --execute "$legacy_query")" = 1
"$engine" exec "$name-sql" /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P Contract-password123 -C -b -Q "IF (SELECT COUNT(*) FROM distill_events WHERE channel_id = 'migration.legacy') <> 1 THROW 50003, 'Legacy data was not preserved', 1;" >/dev/null
echo 'All four sink contracts and native legacy-schema upgrades with data preservation verified'
