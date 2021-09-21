from datetime import datetime, timedelta


def time_interval(date1, date2, interval = 1):

    date_format = "%Y-%m-%d"
    d1          = datetime.strptime(date1, date_format).date()
    d2          = datetime.strptime(date2, date_format).date()
    step        = timedelta(days = interval)


    d     = d1
    dates = []
    while d < d2:
        dates.append(d.strftime(date_format))
        d += step


    interval_collect = []
    for d1, d2 in zip(dates, dates[1:]):
        date2 = datetime.strptime(d2, date_format).date()
        interval_collect.append((d1, date2.strftime(date_format)))
        
    return interval_collect




