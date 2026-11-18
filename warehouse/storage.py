import hashlib
import json
from contextlib import contextmanager
from pathlib import Path

import duckdb


def open_store(path):
    connection = duckdb.connect(str(path))
    connection.execute('SET threads=1')
    connection.execute("SET TimeZone='UTC'")
    connection.execute(Path(__file__).with_name('schema.sql').read_text())
    _migrate(connection)
    return connection


def _migrate(connection):
    """Add session-fact columns to stores created before they existed."""
    additions = {'site_days': {'sessions': 'INTEGER', 'state': 'VARCHAR'}}
    for table, columns in additions.items():
        existing = {row[0] for row in connection.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name=?",
            [table]).fetchall()}
        for name, kind in columns.items():
            if name not in existing:
                connection.execute('ALTER TABLE "' + table + '" ADD COLUMN "' + name + '" ' + kind)


def records(connection, query, parameters=None):
    cursor = connection.execute(query, parameters or [])
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


@contextmanager
def transaction(connection):
    connection.execute('BEGIN TRANSACTION')
    try:
        yield
        connection.execute('COMMIT')
    except BaseException:
        connection.execute('ROLLBACK')
        raise


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()
