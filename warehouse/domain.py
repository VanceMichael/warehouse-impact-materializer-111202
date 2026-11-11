from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal


def overview(connection):
    rows = [dict(record) for record in connection.execute('SELECT day, item, units FROM demand ORDER BY record')]
    totals = defaultdict(int)
    for record in rows:
        calendar = date.fromisoformat(record["day"]).isocalendar()
        totals[(calendar.year, calendar.week, record["item"])] += record["units"]
    return [{"year": key[0], "week": key[1], "item": key[2], "units": value} for key, value in sorted(totals.items())]
