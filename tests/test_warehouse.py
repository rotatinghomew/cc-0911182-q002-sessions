
import copy
import json
import tempfile
import unittest
from pathlib import Path

from warehouse.export import export_tables, read_export
from warehouse.ingest import ingest
from warehouse.queries import overview
from warehouse.storage import open_store


class WarehouseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.connection = open_store(self.root / 'store.duckdb')
        self.payload = json.loads(Path('samples/delivery.json').read_text())

    def tearDown(self):
        self.connection.close()
        self.temp.cleanup()

    def load(self):
        return ingest(self.connection, copy.deepcopy(self.payload))

    def test_replay_keeps_source_rows(self):
        first = self.load()
        second = self.load()
        self.assertGreater(first['inserted'], 0)
        self.assertEqual(second['inserted'], 0)
        self.assertTrue(second['replayed'])
        self.assertEqual(self.connection.execute('SELECT count(*) FROM receipts').fetchone()[0], 1)

    def test_batch_collision_is_atomic(self):
        self.load()
        collision = copy.deepcopy(self.payload)
        collision['tables'] = {}
        with self.assertRaises(ValueError):
            ingest(self.connection, collision)
        self.assertEqual(self.connection.execute('SELECT count(*) FROM receipts').fetchone()[0], 1)

    def test_unknown_table_does_not_write_receipt(self):
        invalid = {'batch_id': 'bad', 'tables': {'missing_table': []}}
        with self.assertRaises(ValueError):
            ingest(self.connection, invalid)
        self.assertEqual(self.connection.execute('SELECT count(*) FROM receipts').fetchone()[0], 0)

    def test_different_batch_same_rows_is_not_duplicate(self):
        first = self.load()
        duplicate = copy.deepcopy(self.payload)
        duplicate['batch_id'] = 'retry-another-envelope'
        self.assertEqual(ingest(self.connection, duplicate)['inserted'], 0)
        self.assertEqual(self.connection.execute('SELECT count(*) FROM source_rows').fetchone()[0], first['inserted'])

    def test_export_roundtrip_and_hash(self):
        self.load()
        result = export_tables(self.connection, self.root / 'exports', 'release1')
        for table, entry in result['tables'].items():
            actual = read_export(self.connection, self.root / 'exports', 'release1', table)
            self.assertEqual(len(actual), entry['rows'])
            self.assertEqual(set(actual[0]) if actual else set(),
                             set(self.payload['tables'][table][0]) if actual else set())
        table = next(iter(result['tables']))
        output = self.root / 'exports' / 'release1' / (table + '.parquet')
        output.write_bytes(output.read_bytes() + b'broken')
        with self.assertRaises(ValueError):
            read_export(self.connection, self.root / 'exports', 'release1', table)

    def test_export_names_and_immutability(self):
        self.load()
        with self.assertRaises(ValueError):
            export_tables(self.connection, self.root, '../escape')
        export_tables(self.connection, self.root, 'stable')
        with self.assertRaises(FileExistsError):
            export_tables(self.connection, self.root, 'stable')

    def test_reopen_preserves_data(self):
        self.load()
        expected = overview(self.connection)
        self.connection.close()
        self.connection = open_store(self.root / 'store.duckdb')
        self.assertEqual(overview(self.connection), expected)

    def test_rejected_record_has_no_partial_state(self):
        invalid = copy.deepcopy(self.payload)
        populated = next(rows for rows in invalid['tables'].values() if rows)
        populated.append({})
        with self.assertRaises(ValueError):
            ingest(self.connection, invalid)
        self.assertEqual(self.connection.execute('SELECT count(*) FROM source_rows').fetchone()[0], 0)

    def test_local_day_and_tenant_identity(self):
        from warehouse.events import local_hits,timeline
        self.load()
        self.assertEqual([row['hits'] for row in local_hits(self.connection,'paris')],[1,1])
        self.assertEqual(len(timeline(self.connection,'paris','anon-1')),2)
        self.assertEqual(len(timeline(self.connection,'tokyo','anon-1')),1)

    def test_order_is_event_time(self):
        from warehouse.events import timeline
        self.load()
        earlier=dict(self.payload['tables']['events'][0],event_id='e0',occurred_at='2026-11-09T21:00:00Z')
        ingest(self.connection,{'batch_id':'later-arrival','tables':{'events':[earlier]}})
        self.assertEqual(timeline(self.connection,'paris')[0]['event_id'],'e0')

    def test_naive_timestamp_rejected(self):
        self.payload['tables']['events'][0]['occurred_at']='2026-11-09T12:00:00'
        with self.assertRaises(ValueError):
            self.load()


    def test_daily_refresh_propagates_new_observation(self):
        from warehouse.daily import refresh_days,read_days
        self.load()
        refresh_days(self.connection,'paris')
        original=read_days(self.connection,'paris')
        extra=dict(self.payload['tables']['events'][0],event_id='e3')
        ingest(self.connection,{'batch_id':'supplement','tables':{'events':[extra]}})
        refresh_days(self.connection,'paris')
        current=read_days(self.connection,'paris')
        self.assertEqual(current[0]['hits'],original[0]['hits']+1)
        self.assertNotEqual(current[0]['source_digest'],original[0]['source_digest'])
