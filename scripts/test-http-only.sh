#!/usr/bin/env bash
# Real server, broker, and reference receiver; no external database or startup catalog.
set -euo pipefail
engine="${DISTILL_CONTAINER_ENGINE:-podman}"
name="distill-http-${RANDOM}${RANDOM}"
work_dir="$(mktemp -d)"
cleanup() {
    local result=$?
    if test "$result" -ne 0; then
        "$engine" logs "$name" 2>&1 | sed 's/http-contract/[REDACTED]/g' | tail -30 >&2 || true
    fi
    "$engine" rm -f "$name" >/dev/null 2>&1 || true
    rm -rf "$work_dir"
    return "$result"
}
trap cleanup EXIT
cargo build --offline --bin distill
"$engine" run -d --name "$name" -p 127.0.0.1::5672 \
    --tmpfs /var/lib/rabbitmq:rw,mode=1777 \
    -e RABBITMQ_DEFAULT_USER=distill -e RABBITMQ_DEFAULT_PASS=http-contract \
    -e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="+S 2:2 -setcookie distill-fixture-cookie" \
    -e RABBITMQ_CTL_ERL_ARGS="+S 2:2 -setcookie distill-fixture-cookie" \
    docker.io/library/rabbitmq:3.13-management-alpine >/dev/null
for _ in $(seq 1 60); do
    if "$engine" exec "$name" rabbitmq-diagnostics -q ping >/dev/null 2>&1; then break; fi
    test "$("$engine" inspect --format '{{.State.Running}}' "$name")" = true
    sleep 1
done
"$engine" exec "$name" rabbitmq-diagnostics -q ping >/dev/null
broker_port="$("$engine" port "$name" 5672/tcp | head -n1 | awk -F: '{print $NF}')"
python3 - "$work_dir" "$broker_port" <<'PY'
import json, os, signal, socket, sqlite3, subprocess, sys, time, urllib.error, urllib.request
from pathlib import Path
root, work = Path.cwd(), Path(sys.argv[1])

def port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]

public_port, admin_port, receiver_port = port(), port(), port()
base, admin = f'http://127.0.0.1:{public_port}', f'http://127.0.0.1:{admin_port}'
environment = {key: value for key, value in os.environ.items() if not key.startswith('DISTILL_')}
environment.update(RABBITMQ_URI=f'amqp://distill:http-contract@127.0.0.1:{sys.argv[2]}/%2f',
    DISTILL_INGEST_API_KEY='shared-contract-key', DISTILL_STATE_PATH=str(work / 'state.sqlite3'),
    DISTILL_BIND_ADDRESS=f'127.0.0.1:{public_port}', DISTILL_ADMIN_BIND_ADDRESS=f'127.0.0.1:{admin_port}',
    DISTILL_SINK_ALLOWLIST=f'127.0.0.1/32:{receiver_port}')
receiver_env = dict(os.environ, DISTILL_RECEIVER_TOKEN='receiver-contract-token')
receiver = server = None
with (work / 'server.log').open('wb') as server_log, (work / 'receiver.log').open('wb') as receiver_log:
    def start():
        return subprocess.Popen([str(root / 'target/debug/distill')], env=environment, stdout=server_log, stderr=server_log)
    def request(url, body=None, key='shared-contract-key'):
        headers = {'X-API-Key': key, 'Content-Type': 'application/json'}
        data = None if body is None else json.dumps(body).encode()
        with urllib.request.urlopen(urllib.request.Request(url, data=data, headers=headers), timeout=5) as response:
            payload = response.read()
            return response.status, json.loads(payload) if payload else None
    def wait(check):
        for _ in range(100):
            try:
                if check(): return
            except (OSError, urllib.error.URLError, sqlite3.Error): pass
            if server is not None and server.poll() is not None:
                raise AssertionError('Distill exited before the contract completed')
            time.sleep(.2)
        raise AssertionError('HTTP-only contract timed out')
    try:
        receiver = subprocess.Popen([sys.executable, str(root / 'examples/http_receiver.py'), '--database', str(work / 'receiver.sqlite3'), '--port', str(receiver_port)], env=receiver_env, stdout=receiver_log, stderr=receiver_log)
        server = start()
        wait(lambda: request(base + '/health')[0] == 200)
        try: request(base + '/ready')
        except urllib.error.HTTPError as error: assert error.code == 503
        else: raise AssertionError('idle deployment must be unready')
        request(admin + '/sinks', {'name': 'receiver', 'version': '1', 'config': {'driver': 'http', 'connection_string': f'http://127.0.0.1:{receiver_port}/records', 'headers': {'Authorization': 'Bearer receiver-contract-token'}}})
        request(admin + '/channels', {'name': 'http.only', 'version': '1', 'sink_id': 'receiver@1', 'preset': 'count', 'aggregator': {'function': 'count', 'granularity': 'per_n_events', 'parameter': 2, 'rolling': True}})
        events = [{'event_id': f'00000000-0000-0000-0000-{index:012d}', 'timestamp': '2026-11-07T12:00:00Z', 'channel_id': 'http.only', 'version': '1', 'severity_level': 'info', 'payload': {}} for index in range(1, 5)]
        def snapshot(count):
            with sqlite3.connect(work / 'receiver.sqlite3') as database:
                rows = database.execute("SELECT payload FROM records WHERE kind = 'aggregate'").fetchall()
            return len(rows) == 1 and json.loads(rows[0][0])['count'] == count
        assert request(base + '/ingest', events[:2])[0] == 202
        wait(lambda: snapshot(2))
        server.kill()
        server.wait(timeout=5)
        server = start()
        wait(lambda: request(base + '/health')[0] == 200)
        assert request(admin + '/channels/http.only/config?version=1')[1]['version'] == '1'
        assert request(base + '/ingest', [events[0], *events[2:]])[0] == 202
        wait(lambda: snapshot(4))
        with sqlite3.connect(work / 'receiver.sqlite3') as database:
            record = json.loads(database.execute("SELECT payload FROM records WHERE kind = 'aggregate'").fetchone()[0])
        assert record['update_revision'] == 2
        inspection = urllib.request.Request(f'http://127.0.0.1:{receiver_port}/records/' + record['aggregate_id'], headers={'Authorization': 'Bearer receiver-contract-token'})
        with urllib.request.urlopen(inspection, timeout=5) as response: assert json.load(response)['count'] == 4
        print('HTTP-only idle startup, shared-key runtime registration, rolling snapshots, process replacement and duplicate protection verified without PostgreSQL')
    finally:
        for process in (server, receiver):
            if process is not None and process.poll() is None:
                process.send_signal(signal.SIGTERM)
                try: process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
PY
