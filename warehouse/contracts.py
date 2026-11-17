from zoneinfo import ZoneInfo
from .checks import text, moment


def normalize(table, original):
    row=dict(original)
    for value in row.values():
        text(value)
    if table=='sites':
        ZoneInfo(row['timezone'])
    else:
        row['occurred_at']=moment(row['occurred_at'])
        row['received_at']=moment(row['received_at'])
    return row
