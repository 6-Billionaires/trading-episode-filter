import datetime

def timestamp2time(given_time):
    temp_time = datetime.datetime.fromtimestamp(given_time)
    h = int(temp_time.strftime("%H"))
    m = int(temp_time.strftime("%M"))
    s = int(temp_time.strftime("%S"))
    f = int(temp_time.strftime("%f"))
    return datetime.time(h, m, s, f)

def addSecs(tm, secs):
    fulldate = datetime.datetime(100, 1, 1, tm.hour, tm.minute, tm.second)  ## 100,1,1 are dummies
    fulldate = fulldate + datetime.timedelta(seconds=secs)
    return fulldate.time()

def timeDiff(t1, t2):
    t1fulldate = datetime.datetime(100, 1, 1, t1.hour, t1.minute, t1.second, t1.microsecond)
    t2fulldate = datetime.datetime(100, 1, 1, t2.hour, t2.minute, t2.second, t2.microsecond)
    return t1fulldate - t2fulldate