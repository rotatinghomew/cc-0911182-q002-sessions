from .events import local_hits


def overview(connection):
    return {site:local_hits(connection,site) for site, in connection.execute('SELECT site_id FROM sites ORDER BY site_id').fetchall()}
