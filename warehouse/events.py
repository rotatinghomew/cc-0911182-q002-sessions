from datetime import datetime
from zoneinfo import ZoneInfo
from .storage import records


def timeline(connection, site, visitor=None):
    if connection.execute('SELECT 1 FROM sites WHERE site_id=?',[site]).fetchone() is None:
        raise ValueError('站点不存在')
    rows=records(connection,'SELECT * FROM events WHERE site_id=?',[site])
    if visitor is not None:
        rows=[row for row in rows if row['visitor_id']==visitor]
    return sorted(rows,key=lambda row:(datetime.fromisoformat(row['occurred_at']),row['event_id']))


def local_hits(connection, site):
    info=connection.execute('SELECT timezone FROM sites WHERE site_id=?',[site]).fetchone()
    if info is None:
        raise ValueError('站点不存在')
    counts={}
    visitors={}
    for row in timeline(connection,site):
        day=datetime.fromisoformat(row['occurred_at']).astimezone(ZoneInfo(info[0])).date().isoformat()
        counts[day]=counts.get(day,0)+1
        visitors.setdefault(day,set()).add(row['visitor_id'])
    return [{'day':day,'hits':counts[day],'visitors':len(visitors[day])} for day in sorted(counts)]
