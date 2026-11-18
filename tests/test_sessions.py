import copy
import json
import tempfile
import unittest
from pathlib import Path

from warehouse import sessions
from warehouse.daily import read_days, refresh_days
from warehouse.events import timeline
from warehouse.ingest import ingest
from warehouse.queries import facts_overview
from warehouse.storage import open_store


PARIS = {'site_id': 'paris', 'timezone': 'Europe/Paris', 'label': '法国站'}


def event(site, key, visitor, occurred, received, page='/', **overrides):
    row = {
        'site_id': site,
        'event_id': key,
        'visitor_id': visitor,
        'occurred_at': occurred,
        'received_at': received,
        'page': page,
    }
    row.update(overrides)
    return row


class SessionFactTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.connection = open_store(self.root / 'store.duckdb')
        ingest(self.connection, {'batch_id': 'sites', 'tables': {'sites': [PARIS]}})

    def tearDown(self):
        self.connection.close()
        self.temp.cleanup()

    def load(self, batch, rows):
        return ingest(self.connection, {'batch_id': batch, 'tables': {'events': rows}})

    def test_gap_of_thirty_minutes_is_same_session(self):
        self.load('gap', [
            event('paris', 'e1', 'v', '2026-11-01T10:00:00Z', '2026-11-01T10:01:00Z'),
            event('paris', 'e2', 'v', '2026-11-01T10:30:00Z', '2026-11-01T10:31:00Z'),
            event('paris', 'e3', 'v', '2026-11-01T11:00:01Z', '2026-11-01T11:01:00Z'),
        ])
        found = sessions.read_sessions(self.connection, 'paris')
        self.assertEqual(len(found), 2)
        self.assertEqual(found[0]['session_id'], 's-e1')
        self.assertEqual(found[0]['hits'], 2)
        self.assertEqual(found[0]['duration_seconds'], 1800.0)
        self.assertEqual(found[1]['session_id'], 's-e3')
        self.assertEqual(found[1]['hits'], 1)

    def test_midnight_crossing_stays_one_session(self):
        self.load('midnight', [
            event('paris', 'e1', 'v', '2026-11-09T22:50:00Z', '2026-11-09T22:51:00Z', '/a'),
            event('paris', 'e2', 'v', '2026-11-09T23:10:00Z', '2026-11-09T23:11:00Z', '/b'),
        ])
        found = sessions.read_sessions(self.connection, 'paris')
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0]['start_day'], '2026-11-09')
        self.assertEqual(found[0]['hits'], 2)
        days = {row['day']: row for row in sessions.read_day_facts(self.connection, 'paris')}
        self.assertEqual(days['2026-11-09']['sessions'], 1)
        self.assertEqual(days['2026-11-09']['visits'], 1)
        self.assertEqual(days['2026-11-10']['sessions'], 0)
        self.assertEqual(days['2026-11-10']['visits'], 1)

    def test_simultaneous_events_do_not_add_duration(self):
        self.load('same-instant', [
            event('paris', 'e1', 'v', '2026-11-01T10:00:00Z', '2026-11-01T10:01:00Z'),
            event('paris', 'e2', 'v', '2026-11-01T10:00:00Z', '2026-11-01T10:01:00Z'),
        ])
        found = sessions.read_sessions(self.connection, 'paris')
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0]['hits'], 2)
        self.assertEqual(found[0]['duration_seconds'], 0.0)
        self.assertEqual(found[0]['entry_page'], '/')

    def test_entry_page_uses_earliest_valid_event(self):
        self.load('entry', [
            # Too old to enter facts: it must not decide the entry page.
            event('paris', 'old', 'v', '2026-11-01T09:00:00Z', '2026-11-10T09:00:00Z', '/old'),
            event('paris', 'e1', 'v', '2026-11-10T09:00:00Z', '2026-11-10T09:01:00Z', '/first'),
            event('paris', 'e2', 'v', '2026-11-10T09:10:00Z', '2026-11-10T09:11:00Z', '/second'),
        ])
        found = sessions.read_sessions(self.connection, 'paris')
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0]['entry_page'], '/first')
        self.assertEqual(sessions.read_entry_pages(self.connection, 'paris'),
                         [{'entry_page': '/first', 'sessions': 1}])

    def test_late_bridge_merges_sessions_and_keeps_references(self):
        self.load('before', [
            event('paris', 'e1', 'v', '2026-11-10T10:00:00Z', '2026-11-10T10:01:00Z'),
            event('paris', 'e3', 'v', '2026-11-10T11:00:00Z', '2026-11-10T11:01:00Z'),
        ])
        before = sessions.read_sessions(self.connection, 'paris')
        self.assertEqual([row['session_id'] for row in before], ['s-e1', 's-e3'])
        self.assertEqual(sessions.read_day_facts(self.connection, 'paris')[0]['sessions'], 2)

        self.load('bridge', [
            event('paris', 'e2', 'v', '2026-11-10T10:30:00Z', '2026-11-10T10:31:00Z'),
        ])
        after = sessions.read_sessions(self.connection, 'paris')
        self.assertEqual(len(after), 1)
        self.assertEqual(after[0]['session_id'], 's-e1')
        self.assertEqual(after[0]['hits'], 3)
        # The retired reference still resolves to the surviving session.
        self.assertEqual(sessions.resolve_session(self.connection, 'paris', 's-e3'), 's-e1')
        detail = sessions.read_session(self.connection, 'paris', 's-e3')
        self.assertEqual(detail['session_id'], 's-e1')
        self.assertEqual([row['event_id'] for row in detail['members']], ['e1', 'e2', 'e3'])
        # The session that used to start on the second day is no longer counted.
        day = sessions.read_day_facts(self.connection, 'paris')[0]
        self.assertEqual(day['sessions'], 1)
        self.assertEqual(day['visits'], 3)
        self.assertEqual(sessions.read_sessions(self.connection, 'paris', visitor='v')[0]['hits'], 3)

    def test_merge_across_start_days_removes_extra_start_day(self):
        self.load('cross', [
            event('paris', 'e1', 'v', '2026-11-09T22:50:00Z', '2026-11-09T22:51:00Z'),
            event('paris', 'e3', 'v', '2026-11-09T23:50:00Z', '2026-11-09T23:51:00Z'),
        ])
        before = {row['day']: row for row in sessions.read_day_facts(self.connection, 'paris')}
        self.assertEqual(before['2026-11-09']['sessions'], 1)
        self.assertEqual(before['2026-11-10']['sessions'], 1)

        self.load('bridge-cross', [
            event('paris', 'e2', 'v', '2026-11-09T23:20:00Z', '2026-11-09T23:21:00Z'),
        ])
        after = {row['day']: row for row in sessions.read_day_facts(self.connection, 'paris')}
        self.assertEqual(after['2026-11-09']['sessions'], 1)
        self.assertEqual(after['2026-11-10']['sessions'], 0)
        self.assertEqual(after['2026-11-09']['visits'], 1)
        self.assertEqual(after['2026-11-10']['visits'], 2)

    def test_out_of_window_event_is_explained_and_detail_is_kept(self):
        self.load('window', [
            event('paris', 'recent', 'v', '2026-11-10T10:00:00Z', '2026-11-10T10:01:00Z'),
            event('paris', 'stale', 'v', '2026-11-01T10:00:00Z', '2026-11-10T10:00:00Z'),
        ])
        found = sessions.read_sessions(self.connection, 'paris')
        self.assertEqual([row['session_id'] for row in found], ['s-recent'])
        excluded = sessions.read_dispositions(self.connection, 'paris', 'excluded')
        self.assertEqual(len(excluded), 1)
        self.assertEqual(excluded[0]['event_id'], 'stale')
        self.assertEqual(excluded[0]['reason'], sessions.REASON_AGE)
        self.assertEqual(excluded[0]['window_seconds'], 48 * 60 * 60)
        # Existing detail queries still see the raw record.
        self.assertEqual([row['event_id'] for row in timeline(self.connection, 'paris')],
                         ['stale', 'recent'])

    def test_out_of_window_event_does_not_change_frozen_day(self):
        self.load('base', [
            event('paris', 'd1', 'v', '2026-11-01T10:00:00Z', '2026-11-01T10:01:00Z'),
            event('paris', 'now', 'v', '2026-11-10T10:00:00Z', '2026-11-10T10:01:00Z'),
        ])
        days = sessions.read_day_facts(self.connection, 'paris')
        self.assertEqual({row['day']: row['state'] for row in days},
                         {'2026-11-01': 'closed', '2026-11-10': 'open'})
        self.load('stale-extra', [
            event('paris', 'stale', 'v', '2026-11-01T12:00:00Z', '2026-11-10T11:00:00Z'),
        ])
        after = {row['day']: row for row in sessions.read_day_facts(self.connection, 'paris')}
        self.assertEqual(after['2026-11-01']['visits'], 1)
        self.assertEqual(after['2026-11-01']['sessions'], 1)
        self.assertEqual(after['2026-11-01']['state'], 'closed')

    def test_in_window_record_cannot_silently_rewrite_closed_day(self):
        self.load('base', [
            event('paris', 'd1', 'v', '2026-11-01T10:00:00Z', '2026-11-01T10:01:00Z'),
            event('paris', 'now', 'v', '2026-11-10T10:00:00Z', '2026-11-10T10:01:00Z'),
        ])
        with self.assertRaises(ValueError):
            self.load('late-rewrite', [
                event('paris', 'd2', 'v', '2026-11-01T11:00:00Z', '2026-11-01T11:01:00Z'),
            ])
        # The rejected batch is atomic: no raw row and no receipt survive.
        self.assertEqual([row['event_id'] for row in timeline(self.connection, 'paris')],
                         ['d1', 'now'])
        self.assertEqual(self.connection.execute('SELECT count(*) FROM receipts').fetchone()[0], 2)
        self.assertEqual(self.connection.execute(
            'SELECT count(*) FROM source_rows WHERE batch_id=?', ['late-rewrite']).fetchone()[0], 0)

    def test_explicit_restatement_is_audited(self):
        self.load('base', [
            event('paris', 'd1', 'v', '2026-11-01T10:00:00Z', '2026-11-01T10:01:00Z'),
            event('paris', 'now', 'v', '2026-11-10T10:00:00Z', '2026-11-10T10:01:00Z'),
        ])
        # Insert the row the way a corrected delivery could, then restate on purpose.
        self.connection.execute(
            'INSERT INTO events VALUES (?,?,?,?,?,?)',
            ['paris', 'd2', 'v', '2026-11-01T11:00:00Z', '2026-11-01T11:01:00Z', '/'])
        with self.assertRaises(ValueError):
            sessions.rebuild_facts(self.connection, 'paris')
        sessions.rebuild_facts(self.connection, 'paris', allow_restatement=True)
        audit = sessions.read_restatements(self.connection, 'paris')
        self.assertTrue(any(row['day'] == '2026-11-01' for row in audit))
        changed = next(row for row in audit if row['day'] == '2026-11-01')
        self.assertEqual(json.loads(changed['before_values'])['visits'], 1)
        self.assertEqual(json.loads(changed['after_values'])['visits'], 2)

    def test_rebuild_from_raw_matches_incremental(self):
        self.load('a', [
            event('paris', 'e1', 'v', '2026-11-10T10:00:00Z', '2026-11-10T10:01:00Z'),
            event('paris', 'e3', 'v', '2026-11-10T11:10:00Z', '2026-11-10T11:11:00Z'),
            event('paris', 'stale', 'v', '2026-11-01T10:00:00Z', '2026-11-10T10:00:00Z'),
        ])
        self.load('b', [
            event('paris', 'e2', 'v', '2026-11-10T10:40:00Z', '2026-11-10T10:41:00Z'),
        ])
        snapshot = {
            'sessions': sessions.read_sessions(self.connection, 'paris'),
            'days': sessions.read_day_facts(self.connection, 'paris'),
            'dispositions': sessions.read_dispositions(self.connection, 'paris'),
        }
        for table in ('sessions', 'session_events', 'site_days', 'event_dispositions'):
            self.connection.execute('DELETE FROM ' + table)
        sessions.rebuild_facts(self.connection, 'paris')
        self.assertEqual(sessions.read_sessions(self.connection, 'paris'), snapshot['sessions'])
        self.assertEqual(sessions.read_day_facts(self.connection, 'paris'), snapshot['days'])
        self.assertEqual(sessions.read_dispositions(self.connection, 'paris'),
                         snapshot['dispositions'])
        # A second rebuild of an already consistent store is stable and silent.
        before = len(sessions.read_restatements(self.connection, 'paris'))
        sessions.rebuild_facts(self.connection, 'paris')
        self.assertEqual(len(sessions.read_restatements(self.connection, 'paris')), before)

    def test_visitor_identity_is_scoped_to_site(self):
        ingest(self.connection, {'batch_id': 'tokyo-site', 'tables': {'sites': [
            {'site_id': 'tokyo', 'timezone': 'Asia/Tokyo', 'label': '日本站'}]}})
        self.load('paris', [
            event('paris', 'e1', 'shared', '2026-11-10T10:00:00Z', '2026-11-10T10:01:00Z')])
        self.load('tokyo', [
            event('tokyo', 'e1', 'shared', '2026-11-10T10:00:00Z', '2026-11-10T10:01:00Z')])
        self.assertEqual(len(sessions.read_sessions(self.connection, 'paris')), 1)
        self.assertEqual(len(sessions.read_sessions(self.connection, 'tokyo')), 1)
        self.assertEqual(sessions.read_sessions(self.connection, 'paris', visitor='shared')[0]['site_id'],
                         'paris')
        # Same exposed id, different tenant, resolves within the requested site.
        self.assertEqual(sessions.resolve_session(self.connection, 'paris', 's-e1'), 's-e1')
        self.assertEqual(sessions.resolve_session(self.connection, 'tokyo', 's-e1'), 's-e1')

    def test_unknown_reference_is_rejected(self):
        self.load('one', [
            event('paris', 'e1', 'v', '2026-11-10T10:00:00Z', '2026-11-10T10:01:00Z')])
        with self.assertRaises(ValueError):
            sessions.resolve_session(self.connection, 'paris', 's-missing')
        with self.assertRaises(ValueError):
            sessions.resolve_session(self.connection, 'paris', '')

    def test_daily_refresh_and_facts_agree(self):
        self.load('days', [
            event('paris', 'e1', 'v', '2026-11-09T22:50:00Z', '2026-11-09T22:51:00Z'),
            event('paris', 'e2', 'v', '2026-11-09T23:10:00Z', '2026-11-09T23:11:00Z'),
        ])
        refresh_days(self.connection, 'paris')
        days = read_days(self.connection, 'paris')
        self.assertEqual([row['visits'] for row in days], [1, 1])
        self.assertEqual(days[0]['sessions'], 1)
        self.assertEqual(facts_overview(self.connection)['paris']['days'], days)

    def test_earlier_late_event_renames_session_but_old_reference_resolves(self):
        self.load('first', [
            event('paris', 'e2', 'v', '2026-11-10T10:30:00Z', '2026-11-10T10:31:00Z')])
        self.assertEqual(sessions.read_sessions(self.connection, 'paris')[0]['session_id'], 's-e2')
        self.load('earlier', [
            event('paris', 'e1', 'v', '2026-11-10T10:00:00Z', '2026-11-10T10:01:00Z')])
        session = sessions.read_sessions(self.connection, 'paris')[0]
        self.assertEqual(session['session_id'], 's-e1')
        self.assertEqual(session['hits'], 2)
        self.assertEqual(sessions.resolve_session(self.connection, 'paris', 's-e2'), 's-e1')
        detail = sessions.read_session(self.connection, 'paris', 's-e2')
        self.assertEqual(detail['reference'], 's-e2')
        self.assertEqual([row['event_id'] for row in detail['members']], ['e1', 'e2'])

    def test_out_of_window_event_has_no_session(self):
        self.load('window', [
            event('paris', 'recent', 'v', '2026-11-10T10:00:00Z', '2026-11-10T10:01:00Z'),
            event('paris', 'stale', 'v', '2026-11-01T10:00:00Z', '2026-11-10T10:00:00Z'),
        ])
        stale = next(row for row in sessions.read_dispositions(self.connection, 'paris')
                     if row['event_id'] == 'stale')
        self.assertIsNone(stale['session_id'])
        self.assertEqual(stale['status'], 'excluded')
        self.assertEqual(stale['age_seconds'], 9 * 24 * 60 * 60)

    def test_reconcile_balances_detail_and_mart(self):
        self.load('reconcile', [
            event('paris', 'e1', 'v', '2026-11-10T10:00:00Z', '2026-11-10T10:01:00Z'),
            event('paris', 'e2', 'v', '2026-11-10T10:20:00Z', '2026-11-10T10:21:00Z'),
            event('paris', 'stale', 'v', '2026-11-01T10:00:00Z', '2026-11-10T10:00:00Z'),
        ])
        result = sessions.reconcile(self.connection, 'paris')
        self.assertTrue(result['consistent'])
        self.assertEqual(result['raw_events'], 3)
        self.assertEqual(result['included'], 2)
        self.assertEqual(result['excluded'], 1)
        self.assertEqual(result['excluded_by_reason'], {'outside_backfill_window': 1})
        self.assertEqual(result['mart_visits'], 2)
        self.assertEqual(result['mart_sessions'], 1)

    def test_unknown_site_is_rejected_by_readers(self):
        with self.assertRaises(ValueError):
            sessions.read_sessions(self.connection, 'nowhere')
        with self.assertRaises(ValueError):
            sessions.read_day_facts(self.connection, 'nowhere')


class LegacyStoreTests(unittest.TestCase):
    def test_legacy_daily_table_is_migrated(self):
        import duckdb
        temp = tempfile.TemporaryDirectory()
        try:
            path = Path(temp.name) / 'legacy.duckdb'
            connection = duckdb.connect(str(path))
            connection.execute('''CREATE TABLE site_days(site_id VARCHAR, day VARCHAR,
                hits INTEGER, visitors INTEGER, source_digest VARCHAR,
                PRIMARY KEY(site_id,day))''')
            connection.execute("INSERT INTO site_days VALUES ('p','2026-11-01',1,1,'old')")
            connection.close()
            from warehouse.storage import open_store
            store = open_store(path)
            row = store.execute('SELECT sessions, state FROM site_days').fetchone()
            self.assertEqual(row, (None, None))
            store.close()
        finally:
            temp.cleanup()


if __name__ == '__main__':
    unittest.main()
