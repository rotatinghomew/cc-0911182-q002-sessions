from .events import local_hits,timeline
from .storage import digest,records,transaction


def refresh_days(connection,site):
    events=timeline(connection,site)
    signature=digest(events)
    totals=local_hits(connection,site)
    with transaction(connection):
        connection.execute('DELETE FROM site_days WHERE site_id=?',[site])
        for row in totals:
            connection.execute('INSERT INTO site_days VALUES (?,?,?,?,?)',[site,row['day'],row['hits'],row['visitors'],signature])
    return totals


def read_days(connection,site):
    return records(connection,'SELECT * FROM site_days WHERE site_id=? ORDER BY day',[site])
