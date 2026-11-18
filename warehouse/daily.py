from .events import timeline
from .sessions import build_site_facts, read_day_facts
from .storage import transaction


def refresh_days(connection,site):
    """Refresh the combined daily mart for one site.

    The mart is produced from accepted session facts, so visit counts here and
    the exposure detail in :mod:`warehouse.sessions` cannot disagree.
    """
    timeline(connection,site)
    with transaction(connection):
        build_site_facts(connection,site)
    return read_days(connection,site)


def read_days(connection,site):
    return read_day_facts(connection,site)
