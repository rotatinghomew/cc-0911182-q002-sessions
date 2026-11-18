from .events import local_hits
from .sessions import read_day_facts, read_sessions


def overview(connection):
    return {site:local_hits(connection,site) for site, in connection.execute('SELECT site_id FROM sites ORDER BY site_id').fetchall()}


def facts_overview(connection):
    """Current session and daily-mart totals per site."""
    return {site: {'sessions': len(read_sessions(connection, site)),
                   'days': read_day_facts(connection, site)}
            for site, in connection.execute(
                'SELECT site_id FROM sites ORDER BY site_id').fetchall()}
