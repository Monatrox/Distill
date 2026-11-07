#!/usr/bin/env python3
"""Verify a running DistillTesting AppHost. Pass its private VerificationOutput JSON."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import math
from pathlib import Path
import subprocess
import time
import urllib.request
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--endpoints', type=Path, required=True)
    parser.add_argument('--engine', default='podman')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--previous-output', type=Path)
    parser.add_argument('--expected-ui-clicks', type=int)
    parser.add_argument('--verify-ui-only', action='store_true')
    args = parser.parse_args()
    deadline = time.monotonic() + 720
    while not args.endpoints.exists():
        if time.monotonic() > deadline:
            raise TimeoutError('AppHost did not report healthy application endpoints')
        time.sleep(1)
    endpoints = json.loads(args.endpoints.read_text())
    assert endpoints['first'] != endpoints['second'], 'Application versions must have distinct endpoints'

    def command(*command_args):
        return subprocess.check_output(command_args, text=True, timeout=90).strip()

    def container(resource):
        names = command(args.engine, 'ps', '--format', '{{.Names}}').splitlines()
        matches = [name for name in names if name.startswith(resource + '-')]
        # The broker also starts with the logger name, so exclude its longer prefix.
        if resource == endpoints['distillResource']:
            matches = [name for name in matches if not name.startswith(endpoints['rabbitResource'] + '-')]
        if len(matches) != 1:
            raise AssertionError(f'Expected one container for {resource}; found {len(matches)}')
        return matches[0]

    postgres = container(endpoints['postgresResource']) if endpoints.get('postgresResource') else None
    receiver = container(endpoints['receiverResource']) if endpoints.get('receiverResource') else None
    logger = container(endpoints['distillResource'])
    rabbit = container(endpoints['rabbitResource'])

    def sql(statement):
        # Credentials stay inside the container, never in arguments or output.
        return command(args.engine, 'exec', postgres, 'sh', '-c',
            'PGPASSWORD="$POSTGRES_PASSWORD" exec psql -U "$POSTGRES_USER" -d distill --tuples-only --no-align --command "$1"',
            'query', statement)

    def request(url, value=None, key=None):
        data = None if value is None else json.dumps(value).encode()
        headers = {'Content-Type': 'application/json'}
        if key is not None: headers['X-API-Key'] = key
        with urllib.request.urlopen(urllib.request.Request(url, data=data, headers=headers), timeout=120) as response:
            body = response.read()
            return response.status, json.loads(body) if response.headers.get('Content-Type', '').startswith('application/json') and body else body

    def wait(check, description):
        until = time.monotonic() + 120
        while time.monotonic() < until:
            try:
                if check(): return
            except (OSError, subprocess.SubprocessError): pass
            time.sleep(.5)
        raise AssertionError('Timed out: ' + description)

    def burst(endpoint, run_id, count=40):
        status, result = request(endpoint + '/verification/burst', {'runId': run_id, 'count': count})
        assert status == 200 and result['accepted'] == count * 6
        return result

    def receiver_records():
        payload = command(args.engine, 'exec', receiver, 'python3', '-c',
            "import json,sqlite3; d=sqlite3.connect('/data/records.sqlite3'); print(json.dumps([json.loads(r[0]) for r in d.execute('SELECT payload FROM records')]))")
        return json.loads(payload)

    def verify(result):
        run_id = result['runId'].replace('-', '')
        version, count = result['version'], result['count']
        if postgres:
            rows = sql("SELECT json_agg(t) FROM (SELECT channel_id, configuration_version, aggregated_value, event_count, update_revision FROM distill_aggregates WHERE channel_id LIKE 'verification." + run_id + ".%' AND configuration_version = '" + version + "') t")
            rows = json.loads(rows) if rows else None
        else:
            rows = [{'channel_id': row['channel_id'], 'configuration_version': row.get('version'),
                'aggregated_value': row['aggregated_value'], 'event_count': row['count'], 'update_revision': row['update_revision']}
                for row in receiver_records() if row.get('aggregate_id') and row['channel_id'].startswith('verification.' + run_id + '.') and row.get('version') == version]
        if rows is None or len(rows) != 6: return False
        expected = {'count': count, 'sum': count * (count + 1) / 2,
            'average': (count + 1) / 2, 'min': 1, 'max': count, 'averagedurationbetweenevents': 1}
        for row in rows:
            name = row['channel_id'].rsplit('.', 1)[1]
            if row['event_count'] != count or row['update_revision'] != count // 10: return False
            assert math.isclose(row['aggregated_value'], expected[name], abs_tol=1e-9), name
        return True

    def logger_count(run_id):
        if postgres:
            return int(sql("SELECT COUNT(*) FROM distill_events WHERE channel_id = 'verification.logger' AND payload->>'RunId' = '" + run_id + "'"))
        return sum(1 for row in receiver_records() if row.get('event_id') and row['channel_id'] == 'verification.logger' and row['payload'].get('RunId') == run_id)

    def verify_ui(count):
        # Explicitly finish small per-N windows after replacement. Their durable
        # checkpoints are intentionally not complete output snapshots until flush.
        entries = json.loads(command(args.engine, 'inspect', '--format', '{{json .Config.Env}}', logger))
        env = dict(entry.split('=', 1) for entry in entries if '=' in entry)
        key = env['DISTILL_ADMIN_API_KEY']
        admin = endpoints.get('admin')
        if admin is None:
            for process in Path('/proc').iterdir():
                if not process.name.isdigit(): continue
                try:
                    entries = (process / 'environ').read_bytes().split(b'\0')
                    values = dict(entry.decode().split('=', 1) for entry in entries if b'=' in entry)
                    if values.get('Distill__ApiKey') == key:
                        admin = values.get('Distill__AdminEndpoint')
                        if admin: break
                except (OSError, UnicodeError): pass
        assert admin, 'Could not locate the verification administrative endpoint'
        for version in ('1', '2'):
            for name in ('count', 'average', 'interval', 'sum', 'min', 'max'):
                request(admin + '/channels/blazor.counter.' + name + '/flush?version=' + version, {}, key)
        # Confirmed publication precedes sink visibility.
        time.sleep(1)

        if postgres:
            rows = json.loads(sql("SELECT json_agg(t) FROM (SELECT channel_id, configuration_version, aggregated_value, event_count FROM distill_aggregates WHERE channel_id LIKE 'blazor.counter.%') t"))
        else:
            rows = [{'channel_id': row['channel_id'], 'configuration_version': row.get('version'), 'aggregated_value': row['aggregated_value'], 'event_count': row['count']}
                for row in receiver_records() if row.get('aggregate_id') and row['channel_id'].startswith('blazor.counter.')]
        assert len(rows) == 12, 'Expected six browser-generated aggregates per producer version'
        expected = {'count': count, 'sum': count * (count + 1) / 2, 'average': (count + 1) / 2, 'min': 1, 'max': count}
        for row in rows:
            assert row['event_count'] == count
            name = row['channel_id'].rsplit('.', 1)[1]
            if name in expected: assert math.isclose(row['aggregated_value'], expected[name], abs_tol=1e-9)
            else: assert row['aggregated_value'] >= 0

    evidence = {'checks': [], 'app_versions': ['1', '2'], 'sink': 'postgres' if postgres else 'http'}
    if args.previous_output:
        previous_runs = json.loads(args.previous_output.read_text())['runs']
        for previous in previous_runs:
            wait(lambda: verify(previous), 'persisted data after full AppHost replacement')
        for version, endpoint in [('1', endpoints['first']), ('2', endpoints['second'])]:
            previous = next(item for item in previous_runs if item['version'] == version)
            replayed = burst(endpoint, previous['runId'], previous['count'])
            wait(lambda: verify(replayed), 'restored deduplication history absorbs old input IDs')
        evidence['checks'].append('Full AppHost replacement preserves prior results, generated credential access and source deduplication history')
    if args.verify_ui_only:
        if args.expected_ui_clicks is None: parser.error('--expected-ui-clicks is required for UI-only inspection')
        verify_ui(args.expected_ui_clicks)
        evidence['checks'].append('Real browser click aggregates survive full replacement with exact count/sum/average/min/max values')
        args.output.write_text(json.dumps(evidence, indent=2) + '\n')
        print('Browser-generated aggregate values and recovery verified')
        return
    for name in ('first', 'second'):
        status, _ = request(endpoints[name] + '/counter')
        assert status == 200
        status, _ = request(endpoints[name] + '/weather')
        assert status == 200
    evidence['checks'].append('Both real Blazor application versions serve counter and weather pages')
    run_id = str(uuid.uuid4())
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda name: burst(endpoints[name], run_id), ['first', 'second']))
    assert {result['version'] for result in results} == {'1', '2'}, 'Both producer versions must be exercised'
    for result in results:
        wait(lambda: verify(result), 'all six rolling values and revisions')
    wait(lambda: logger_count(run_id) == 80, 'ILogger burst persistence')
    evidence['checks'].append('Concurrent application versions: all six cumulative rolling values, revisions and ILogger bursts')
    evidence['checks'].append('Repeated source IDs do not add aggregate contributions')
    print('Concurrent versions, six rolling functions, duplicates and ILogger verified', flush=True)

    # Kill the logger while the applications and their reconciled clients remain alive.
    command(args.engine, 'restart', '--time', '0', logger)
    for name in ('first', 'second'):
        wait(lambda: request(endpoints[name] + '/health')[0] == 200, 'logger recovery through real application health')
    for result in results:
        wait(lambda: verify(result), 'unchanged totals after logger process replacement')
    for name in ('first', 'second'):
        repeated = burst(endpoints[name], run_id)
        wait(lambda: verify(repeated), 'duplicate inputs after logger restart')
    evidence['checks'].append('Logger forced restart restores runtime versions/checkpoints; existing applications continue without reprovisioning')
    print('Logger replacement and post-restart duplicate protection verified', flush=True)

    command(args.engine, 'restart', '--time', '5', rabbit)
    for name in ('first', 'second'):
        wait(lambda: request(endpoints[name] + '/health')[0] == 200, 'broker reconnection')
    recovery_id = str(uuid.uuid4())
    recovery = burst(endpoints['first'], recovery_id)
    wait(lambda: verify(recovery), 'new work after broker restart')
    evidence['checks'].append('Managed RabbitMQ restart recovers without application/logger restart')
    print('Managed broker restart verified', flush=True)

    started = time.monotonic()
    tasks = [('first' if index % 2 == 0 else 'second', str(uuid.uuid4())) for index in range(4)]
    with ThreadPoolExecutor(max_workers=4) as pool:
        concurrent = list(pool.map(lambda task: burst(endpoints[task[0]], task[1], 100), tasks))
    for result in concurrent:
        wait(lambda: verify(result), 'concurrent configuration and ingestion')
    evidence['concurrent_source_events'] = 2400
    evidence['concurrent_elapsed_seconds'] = round(time.monotonic() - started, 3)
    evidence['checks'].append('Concurrent new channel registrations plus 2400 source events produce exact expected aggregates')
    if args.expected_ui_clicks is not None:
        verify_ui(args.expected_ui_clicks)
        evidence['checks'].append('Real browser click aggregates survive full replacement with exact count/sum/average/min/max values')
    evidence['runs'] = results + [recovery] + concurrent
    evidence['sqlite_main_bytes'] = int(command(args.engine, 'exec', logger, 'stat', '-c', '%s', '/var/lib/distill/state.sqlite3'))
    evidence['sqlite_filesystem'] = command(args.engine, 'exec', logger, 'stat', '-f', '-c', '%T', '/var/lib/distill')
    queues = command(args.engine, 'exec', rabbit, 'rabbitmqctl', 'list_queues', '-q', 'name', 'messages').splitlines()
    assert any(line.split() == ['distill.dead-letter.queue', '0'] for line in queues), 'Verification created dead-letter work'
    evidence['checks'].append('No dead-letter work from valid supported scenarios')
    args.output.write_text(json.dumps(evidence, indent=2) + '\n')
    print('Concurrent registration/ingestion verified; evidence written to ' + str(args.output), flush=True)


if __name__ == '__main__':
    main()
