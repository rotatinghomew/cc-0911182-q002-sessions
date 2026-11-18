"""Session facts built from raw event detail.

``events`` remains the system of record for detail queries.  This module derives
per site:

* which raw events belong to the same visit session,
* the daily mart combining sessions (by local start date) and visits (by local
  event date),
* an explicit destination for every raw event, including those that cannot
  enter the fact window, and
* an audit trail whenever an already published day is restated.

Every derived row is a pure function of the raw rows and the site timezone.
Deleting the fact tables and recomputing a site therefore yields exactly what
incremental batches produced.  A session id is derived from its earliest event,
so it survives merges: when a late record joins two sessions, the earlier id
stays valid and the later reference resolves through current event membership.
"""

from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from .storage import canonical, digest, records, transaction

SESSION_GAP_SECONDS = 30 * 60
BACKFILL_SECONDS = 48 * 60 * 60
SESSION_PREFIX = 's-'

_MICROS = 1_000_000
_GAP_MICROS = SESSION_GAP_SECONDS * _MICROS
_BACKFILL_MICROS = BACKFILL_SECONDS * _MICROS
_EPOCH = datetime(1970, 1, 1)

INCLUDED = 'included'
EXCLUDED = 'excluded'
REASON_INCLUDED = 'within_backfill_window'
REASON_AGE = 'outside_backfill_window'

OPEN = 'open'
CLOSED = 'closed'


def instant(value):
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if stamp.utcoffset() is None:
        raise ValueError('时间必须包含时区')
    return stamp


def epoch_micros(stamp):
    utc = stamp.astimezone(timezone.utc).replace(tzinfo=None)
    delta = utc - _EPOCH
    return (delta.days * 86400 + delta.seconds) * _MICROS + delta.microseconds


def _iso(micros):
    if micros is None:
        return None
    return datetime.fromtimestamp(micros / _MICROS, timezone.utc).isoformat()


def _day_end_micros(day, zone):
    following = datetime.combine(day + timedelta(days=1), time.min, tzinfo=zone)
    return epoch_micros(following)


def site_timezone(connection, site):
    info = connection.execute('SELECT timezone FROM sites WHERE site_id=?', [site]).fetchone()
    if info is None:
        raise ValueError('站点不存在')
    return ZoneInfo(info[0])


def _local_day(stamp, zone):
    return instant(stamp).astimezone(zone).date()


def sessionize(events, zone):
    """Group accepted events into per-visitor sessions.

    Ordering is by instant and then event id, never by calendar day, so a visit
    crossing midnight stays one session.  A gap of more than thirty minutes
    starts a new session; equal instants have a zero gap and cannot inflate a
    session's duration.
    """
    grouped = defaultdict(list)
    for event in events:
        grouped[event['visitor_id']].append(event)
    sessions = []
    for visitor in sorted(grouped):
        ordered = sorted(grouped[visitor], key=lambda row: (row['_ts'], row['event_id']))
        members = []
        for event in ordered:
            if members and event['_ts'] - members[-1]['_ts'] > _GAP_MICROS:
                sessions.append(_finish(visitor, members, zone))
                members = []
            members.append(event)
        if members:
            sessions.append(_finish(visitor, members, zone))
    return sorted(sessions, key=lambda row: (row['start_ts'], row['session_id']))


def _finish(visitor, members, zone):
    first = members[0]
    last = members[-1]
    return {
        'session_id': SESSION_PREFIX + first['event_id'],
        'visitor_id': visitor,
        'start_at': first['occurred_at'],
        'end_at': last['occurred_at'],
        'start_ts': first['_ts'],
        'end_ts': last['_ts'],
        'hits': len(members),
        'entry_page': first['page'],
        'start_day': _local_day(first['occurred_at'], zone).isoformat(),
        'members': members,
    }


def classify(events):
    """Split raw events into accepted facts and explained exclusions.

    The backfill window is a property of each event (occurrence versus receipt),
    never of the latest arrival, which keeps recomputation deterministic: adding
    an old record cannot retroactively remove an event that was already counted.
    """
    accepted = []
    dispositions = []
    for event in events:
        # The window is about how old the observation is; a receipt stamped
        # slightly before the event is simply not aged yet.
        age = event['_received_ts'] - event['_ts']
        if age > _BACKFILL_MICROS:
            status, reason = EXCLUDED, REASON_AGE
        else:
            status, reason = INCLUDED, REASON_INCLUDED
        dispositions.append({
            'event_id': event['event_id'],
            'status': status,
            'reason': reason,
            'occurred_at': event['occurred_at'],
            'received_at': event['received_at'],
            'age_seconds': age // _MICROS,
            'window_seconds': BACKFILL_SECONDS,
        })
        if status == INCLUDED:
            accepted.append(event)
    return accepted, dispositions


def _payload(connection, site):
    zone = site_timezone(connection, site)
    events = []
    for row in records(connection, 'SELECT * FROM events WHERE site_id=?', [site]):
        item = dict(row)
        item['_ts'] = epoch_micros(instant(row['occurred_at']))
        item['_received_ts'] = epoch_micros(instant(row['received_at']))
        events.append(item)
    events.sort(key=lambda row: (row['_ts'], row['event_id']))
    return zone, events


def _event_projection(event):
    return {key: event[key] for key in
            ('event_id', 'visitor_id', 'occurred_at', 'received_at', 'page')}


def _day_signature(day_events, day_sessions):
    return digest({
        'events': [_event_projection(event) for event in day_events],
        'sessions': [{'session_id': row['session_id'],
                      'members': [member['event_id'] for member in row['members']]}
                     for row in day_sessions],
    })


def _mart_rows(site, zone, accepted, sessions, frontier):
    """Combine sessions by start day and visits by event day in one mart."""
    events_by_day = defaultdict(list)
    visitors = defaultdict(set)
    for event in accepted:
        day = _local_day(event['occurred_at'], zone).isoformat()
        events_by_day[day].append(event)
        visitors[day].add(event['visitor_id'])
    sessions_by_day = defaultdict(list)
    for row in sessions:
        sessions_by_day[row['start_day']].append(row)
    # A local day is closed once the latest receipt is more than the backfill
    # window past its end.  Any later delivery carries a receipt at or beyond
    # that frontier, so an event it reports that occurred on this day would be
    # older than 48 hours and outside the window; events after the day end
    # cannot contribute visits or start days here.
    result = []
    for value in sorted(set(events_by_day) | set(sessions_by_day)):
        day = date.fromisoformat(value)
        closed = frontier is not None and frontier > _day_end_micros(day, zone) + _BACKFILL_MICROS
        result.append({
            'day': value,
            'visits': len(events_by_day.get(value, ())),
            'visitors': len(visitors.get(value, ())),
            'sessions': len(sessions_by_day.get(value, ())),
            'state': CLOSED if closed else OPEN,
            'source_digest': _day_signature(events_by_day.get(value, ()),
                                            sessions_by_day.get(value, ())),
        })
    return result


def _prior_days(connection, site):
    return {row['day']: row for row in records(
        connection, 'SELECT * FROM site_days WHERE site_id=?', [site])}


def _guard_closed(site, prior, rows, allow_restatement):
    """Refuse to silently change a closed day; callers may explicitly restate."""
    current = {row['day']: row for row in rows}
    conflicts = []
    for day, old in prior.items():
        if old['state'] != CLOSED:
            continue
        new = current.get(day)
        before = (old['sessions'], old['hits'], old['visitors'])
        after = ((new['sessions'], new['visits'], new['visitors']) if new else (0, 0, 0))
        if before != after:
            conflicts.append({'day': day, 'before': before, 'after': after})
    if conflicts and not allow_restatement:
        raise ValueError('已冻结日期事实发生变化，需要显式重述：%s %s' % (
            site, ', '.join(row['day'] for row in conflicts)))
    return conflicts


def _log_restatements(connection, site, prior, rows, cause, observed_at):
    """Record only changes to days that had already left the backfill window."""
    for row in rows:
        old = prior.get(row['day'])
        if old is None or old['state'] != CLOSED:
            continue
        before = (old['sessions'], old['hits'], old['visitors'])
        after = (row['sessions'], row['visits'], row['visitors'])
        if before == after:
            continue
        connection.execute(
            'INSERT INTO fact_restatements(site_id,day,cause,observed_at,before_values,'
            'after_values) VALUES (?,?,?,?,?,?)',
            [site, row['day'], cause, observed_at,
             canonical({'sessions': before[0], 'visits': before[1], 'visitors': before[2]}),
             canonical({'sessions': after[0], 'visits': after[1], 'visitors': after[2]})])


def build_site_facts(connection, site, cause='refresh', allow_restatement=False):
    """Recompute one site's fact state.  The caller owns the transaction."""
    zone, events = _payload(connection, site)
    frontier = max((event['_received_ts'] for event in events), default=None)
    accepted, dispositions = classify(events)
    sessions = sessionize(accepted, zone)
    rows = _mart_rows(site, zone, accepted, sessions, frontier)
    prior = _prior_days(connection, site)
    _guard_closed(site, prior, rows, allow_restatement)
    _log_restatements(connection, site, prior, rows, cause, _iso(frontier))

    membership = {}
    for row in sessions:
        for member in row['members']:
            membership[member['event_id']] = row['session_id']
    for row in dispositions:
        row['session_id'] = membership.get(row['event_id'])

    connection.execute('DELETE FROM sessions WHERE site_id=?', [site])
    connection.execute('DELETE FROM session_events WHERE site_id=?', [site])
    connection.execute('DELETE FROM site_days WHERE site_id=?', [site])
    connection.execute('DELETE FROM event_dispositions WHERE site_id=?', [site])
    for row in sessions:
        connection.execute(
            'INSERT INTO sessions(site_id,session_id,visitor_id,start_at,end_at,'
            'start_ts,end_ts,hits,entry_page,start_day) VALUES (?,?,?,?,?,?,?,?,?,?)',
            [site, row['session_id'], row['visitor_id'], row['start_at'], row['end_at'],
             row['start_ts'], row['end_ts'], row['hits'], row['entry_page'], row['start_day']])
        for member in row['members']:
            connection.execute(
                'INSERT INTO session_events(site_id,event_id,session_id) VALUES (?,?,?)',
                [site, member['event_id'], row['session_id']])
    for row in rows:
        connection.execute(
            'INSERT INTO site_days(site_id,day,hits,visitors,source_digest,sessions,state) '
            'VALUES (?,?,?,?,?,?,?)',
            [site, row['day'], row['visits'], row['visitors'], row['source_digest'],
             row['sessions'], row['state']])
    for row in dispositions:
        connection.execute(
            'INSERT INTO event_dispositions(site_id,event_id,status,reason,occurred_at,'
            'received_at,age_seconds,window_seconds,session_id) VALUES (?,?,?,?,?,?,?,?,?)',
            [site, row['event_id'], row['status'], row['reason'], row['occurred_at'],
             row['received_at'], row['age_seconds'], row['window_seconds'],
             row['session_id']])
    return {
        'site_id': site,
        'frontier': _iso(frontier),
        'sessions': len(sessions),
        'days': len(rows),
        'included': len(accepted),
        'excluded': len(dispositions) - len(accepted),
    }


def refresh_facts(connection, site, cause='refresh', allow_restatement=False):
    """Recompute one site's facts atomically."""
    with transaction(connection):
        return build_site_facts(connection, site, cause, allow_restatement)


def refresh_sites(connection, sites, cause='ingest'):
    """Refresh facts for the given sites; unknown sites are left untouched."""
    known = {row[0] for row in connection.execute('SELECT site_id FROM sites').fetchall()}
    targets = sorted({site for site in sites if site in known})
    if not targets:
        return []
    with transaction(connection):
        return [build_site_facts(connection, site, cause) for site in targets]


def rebuild_facts(connection, sites=None, allow_restatement=False):
    """Recompute facts from raw rows, optionally restricted to given sites."""
    if sites is None:
        targets = [row[0] for row in connection.execute(
            'SELECT site_id FROM sites ORDER BY site_id').fetchall()]
    elif isinstance(sites, str):
        targets = [sites]
    else:
        targets = sorted(set(sites))
    with transaction(connection):
        return [build_site_facts(connection, site, 'rebuild', allow_restatement)
                for site in targets]


def resolve_session(connection, site, reference):
    """Return the current session id for a previously exposed reference."""
    if not isinstance(reference, str) or not reference.strip():
        raise ValueError('会话引用不能为空')
    if connection.execute('SELECT 1 FROM sites WHERE site_id=?', [site]).fetchone() is None:
        raise ValueError('站点不存在')
    if connection.execute('SELECT 1 FROM sessions WHERE site_id=? AND session_id=?',
                          [site, reference]).fetchone() is not None:
        return reference
    event_id = reference[len(SESSION_PREFIX):] if reference.startswith(SESSION_PREFIX) else reference
    row = connection.execute(
        'SELECT session_id FROM session_events WHERE site_id=? AND event_id=?',
        [site, event_id]).fetchone()
    if row is None:
        raise ValueError('会话引用无法归属：' + reference)
    return row[0]


def read_sessions(connection, site, visitor=None, start_day=None):
    if connection.execute('SELECT 1 FROM sites WHERE site_id=?', [site]).fetchone() is None:
        raise ValueError('站点不存在')
    query = 'SELECT * FROM sessions WHERE site_id=?'
    parameters = [site]
    if visitor is not None:
        query += ' AND visitor_id=?'
        parameters.append(visitor)
    if start_day is not None:
        query += ' AND start_day=?'
        parameters.append(start_day)
    rows = records(connection, query + ' ORDER BY start_ts, session_id', parameters)
    for row in rows:
        row['duration_seconds'] = (row['end_ts'] - row['start_ts']) / _MICROS
    return rows


def read_session(connection, site, reference):
    session_id = resolve_session(connection, site, reference)
    row = next(row for row in read_sessions(connection, site) if row['session_id'] == session_id)
    result = dict(row)
    result['reference'] = reference
    result['members'] = session_members(connection, site, session_id)
    return result


def session_members(connection, site, reference):
    session_id = resolve_session(connection, site, reference)
    rows = records(connection, '''
        SELECT e.site_id, e.event_id, e.visitor_id, e.occurred_at, e.received_at,
               e.page, se.session_id
        FROM session_events se JOIN events e
          ON e.site_id=se.site_id AND e.event_id=se.event_id
        WHERE se.site_id=? AND se.session_id=?''', [site, session_id])
    rows.sort(key=lambda row: (epoch_micros(instant(row['occurred_at'])), row['event_id']))
    return rows


def read_day_facts(connection, site, day=None):
    if connection.execute('SELECT 1 FROM sites WHERE site_id=?', [site]).fetchone() is None:
        raise ValueError('站点不存在')
    query = ('SELECT site_id, day, sessions, hits, hits AS visits, visitors, state, '
             'source_digest FROM site_days WHERE site_id=?')
    parameters = [site]
    if day is not None:
        query += ' AND day=?'
        parameters.append(day)
    return records(connection, query + ' ORDER BY day', parameters)


def read_dispositions(connection, site, status=None):
    if connection.execute('SELECT 1 FROM sites WHERE site_id=?', [site]).fetchone() is None:
        raise ValueError('站点不存在')
    query = 'SELECT * FROM event_dispositions WHERE site_id=?'
    parameters = [site]
    if status is not None:
        query += ' AND status=?'
        parameters.append(status)
    return records(connection, query + ' ORDER BY occurred_at, event_id', parameters)


def read_restatements(connection, site):
    if connection.execute('SELECT 1 FROM sites WHERE site_id=?', [site]).fetchone() is None:
        raise ValueError('站点不存在')
    return records(connection,
        'SELECT * FROM fact_restatements WHERE site_id=? ORDER BY day, observed_at', [site])


def read_entry_pages(connection, site, day=None):
    if connection.execute('SELECT 1 FROM sites WHERE site_id=?', [site]).fetchone() is None:
        raise ValueError('站点不存在')
    query = 'SELECT entry_page, count(*) AS sessions FROM sessions WHERE site_id=?'
    parameters = [site]
    if day is not None:
        query += ' AND start_day=?'
        parameters.append(day)
    return records(connection, query + ' GROUP BY entry_page ORDER BY entry_page', parameters)


def reconcile(connection, site):
    """Show how raw detail maps onto the mart so the two cannot silently diverge.

    Every raw event is either included in the mart or listed with a reason in
    ``event_dispositions``.  ``consistent`` is true when the raw total splits
    exactly over that boundary and the mart visits equal both the included
    events and the session membership.
    """
    if connection.execute('SELECT 1 FROM sites WHERE site_id=?', [site]).fetchone() is None:
        raise ValueError('站点不存在')
    raw = connection.execute(
        'SELECT count(*) FROM events WHERE site_id=?', [site]).fetchone()[0]
    included, excluded = connection.execute(
        'SELECT sum(CASE WHEN status=? THEN 1 ELSE 0 END), '
        'sum(CASE WHEN status=? THEN 1 ELSE 0 END) FROM event_dispositions WHERE site_id=?',
        [INCLUDED, EXCLUDED, site]).fetchone()
    included = int(included or 0)
    excluded = int(excluded or 0)
    mart_visits = int(connection.execute(
        'SELECT coalesce(sum(hits),0) FROM site_days WHERE site_id=?', [site]).fetchone()[0])
    mart_sessions = int(connection.execute(
        'SELECT coalesce(sum(sessions),0) FROM site_days WHERE site_id=?', [site]).fetchone()[0])
    session_rows = connection.execute(
        'SELECT coalesce(sum(hits),0), count(*) FROM sessions WHERE site_id=?',
        [site]).fetchone()
    membership = connection.execute(
        'SELECT count(*) FROM session_events WHERE site_id=?', [site]).fetchone()[0]
    reasons = {row['reason']: row['events'] for row in records(connection,
        'SELECT reason, count(*) AS events FROM event_dispositions WHERE site_id=? '
        'AND status=? GROUP BY reason ORDER BY reason', [site, EXCLUDED])}
    consistent = (raw == included + excluded
                  and included == mart_visits == session_rows[0] == membership
                  and mart_sessions == session_rows[1])
    return {
        'site_id': site,
        'raw_events': raw,
        'included': included,
        'excluded': excluded,
        'excluded_by_reason': reasons,
        'mart_visits': mart_visits,
        'mart_sessions': mart_sessions,
        'session_hits': int(session_rows[0]),
        'members': membership,
        'consistent': consistent,
    }
