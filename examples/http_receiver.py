#!/usr/bin/env python3
"""Reference Distill HTTP receiver with atomic snapshot revision handling.

Set DISTILL_RECEIVER_TOKEN and run with --database on persistent local storage.
This example owns only receiver data; Distill owns its internal recovery catalog.
"""

import argparse
import hmac
import json
import os
import sqlite3
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from contextlib import closing
from urllib.parse import unquote

MAX_RECORD_BYTES = 2 * 1024 * 1024


class Receiver(BaseHTTPRequestHandler):
    def log_message(self, *_):
        # Request URLs, bodies, and credentials are intentionally not logged.
        pass

    def reply(self, status, value=None):
        body = b"" if value is None else json.dumps(value).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def authenticated(self):
        provided = self.headers.get("Authorization", "")
        return hmac.compare_digest(provided, "Bearer " + self.server.token)

    def do_GET(self):
        if self.path == "/health":
            return self.reply(200)
        if not self.authenticated():
            return self.reply(401)
        if not self.path.startswith("/records/"):
            return self.reply(404)
        identity = unquote(self.path.removeprefix("/records/"))
        with closing(sqlite3.connect(self.server.database)) as database:
            row = database.execute("SELECT payload FROM records WHERE record_id = ?", (identity,)).fetchone()
        return self.reply(404) if row is None else self.reply(200, json.loads(row[0]))

    def do_POST(self):
        if not self.authenticated():
            return self.reply(401)
        if self.path != "/records":
            return self.reply(404)
        if self.headers.get("X-Distill-Contract-Version") != "1":
            return self.reply(400)
        try:
            size = int(self.headers.get("Content-Length", "0"))
            if size <= 0 or size > MAX_RECORD_BYTES:
                return self.reply(413)
            payload = json.loads(self.rfile.read(size))
            kind = self.headers.get("X-Distill-Record-Type")
            identity = self.headers.get("X-Distill-Record-Id")
            if not isinstance(payload, dict) or kind not in ("event", "aggregate"):
                return self.reply(400)
            if not identity or payload.get("event_id" if kind == "event" else "aggregate_id") != identity:
                return self.reply(400)
            revision = 0 if kind == "event" else payload.get("update_revision")
            if isinstance(revision, bool) or not isinstance(revision, int) or not 0 <= revision <= 2**63 - 1:
                return self.reply(400)
            if kind == "aggregate" and self.headers.get("X-Distill-Update-Revision") != str(revision):
                return self.reply(400)
            if kind == "aggregate" and payload.get("rolling") and revision == 0:
                return self.reply(422)
            canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
            with closing(sqlite3.connect(self.server.database, timeout=10)) as database:
                database.execute("BEGIN IMMEDIATE")
                row = database.execute("SELECT revision, payload, kind FROM records WHERE record_id = ?", (identity,)).fetchone()
                if row is not None:
                    if row[2] != kind or (row[0] == revision and row[1] != canonical):
                        return self.reply(409)
                    if row[0] >= revision:
                        return self.reply(200)
                database.execute(
                    "INSERT INTO records(record_id, kind, revision, payload) VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(record_id) DO UPDATE SET revision=excluded.revision, payload=excluded.payload",
                    (identity, kind, revision, canonical),
                )
                database.commit()
            return self.reply(200)
        except (ValueError, TypeError, json.JSONDecodeError):
            return self.reply(400)
        except sqlite3.Error:
            return self.reply(503)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    token = os.environ.get("DISTILL_RECEIVER_TOKEN")
    if not token:
        parser.error("DISTILL_RECEIVER_TOKEN must be set")
    # Restrict new database/journal files. Supply a private persistent directory.
    os.umask(0o077)
    args.database.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(args.database)) as database:
        database.execute("PRAGMA journal_mode=WAL")
        database.execute("CREATE TABLE IF NOT EXISTS records(record_id TEXT PRIMARY KEY, kind TEXT NOT NULL, revision INTEGER NOT NULL, payload TEXT NOT NULL)")
    server = ThreadingHTTPServer((args.host, args.port), Receiver)
    server.database = str(args.database)
    server.token = token
    server.serve_forever()


if __name__ == "__main__":
    main()
