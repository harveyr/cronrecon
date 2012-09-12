import logging
import datetime
from cronrecon import CronJob


def setUp():
    logging.basicConfig(level=logging.DEBUG,
        format='%(levelname)s %(module)s (%(lineno)s): %(message)s')


def test_cron1():
    minute = '*/3'
    hour = 4
    dom = 5
    month = 6
    dow = '*'

    cron_str = '{0} {1} {2} {3} {4}'.format(
        minute, hour, dom, month, dow)
    job = CronJob(cron_str)
    next_run = job.next_run()
    logging.debug(job)
    logging.debug('next_run: %s' % next_run)
    assert next_run.hour == hour
    assert next_run.day == dom
    assert next_run.minute == 0 or next_run.minute % 3 == 0
    assert next_run.month == month


def test_dow1():
    minute = '*/10'
    hour = 4
    dom = '*'
    month = 6
    dow = 4

    cron_str = '{0} {1} {2} {3} {4}'.format(
        minute, hour, dom, month, dow)
    job = CronJob(cron_str)
    next_run = job.next_run()
    logging.debug(job)
    logging.debug('next_run: %s' % next_run)
    assert next_run.hour == hour
    assert next_run.weekday() == dow
    assert next_run.minute == 0 or next_run.minute % 10 == 0
    assert next_run.month == month


def test_dow2():
    minute = 7
    hour = '9-12'
    dom = '*'
    month = '*/4'
    dow = 3

    cron_str = '{0} {1} {2} {3} {4}'.format(
        minute, hour, dom, month, dow)
    job = CronJob(cron_str)
    next_run = job.next_run()
    logging.debug(job)
    logging.debug('next_run: %s' % next_run)
    assert next_run.minute == minute
    assert next_run.hour >= 9 and next_run.hour <= 12
    assert next_run.weekday() == dow
    assert next_run.month == 1 or next_run.month % 4 - 1 == 0


def test_month_rollover():
    minute = '*'
    hour = '*'
    dom = '3-5,9-22'
    month = '*/4'
    dow = '*'

    cron_str = '{0} {1} {2} {3} {4}'.format(
        minute, hour, dom, month, dow)
    job = CronJob(cron_str)

    start_dt = datetime.datetime(
        year=2012,
        month=2,
        day=27)

    next_run = job.next_run(start_dt)
    assert next_run.month == 5
    assert next_run.day == 3


def test_day_and_hour_rollover():
    minute = '*'
    hour = '12,19'
    dom = '3,22'
    month = '*'
    dow = '*'

    cron_str = '{} {} {} {} {}'.format(
        minute, hour, dom, month, dow)
    job = CronJob(cron_str)

    start_dt = datetime.datetime(
        year=2012,
        month=4,
        day=1)

    next_run = job.next_run(start_dt)
    assert next_run.day == 3
    assert next_run.hour == 12

    start_dt = next_run + datetime.timedelta(hours=1)
    next_run = job.next_run(start_dt)
    assert next_run.hour == 19
    assert next_run.day == 3

    start_dt = next_run + datetime.timedelta(hours=1)
    next_run = job.next_run(start_dt)
    assert next_run.day == 22
    assert next_run.hour == 12


def test_multi_rollover():
    minute = '*/20'
    hour = '5,19'
    dom = '*/13'
    month = '*/3'
    dow = 6

    cron_str = '{} {} {} {} {}'.format(
        minute, hour, dom, month, dow)
    job = CronJob(cron_str)

    # This was a Sunday (DOW == 6).
    start_dt = datetime.datetime(
        year=2012,
        month=1,
        day=1,
        hour=0,
        minute=1)

    nr = job.next_run(start_dt)
    assert nr.minute == 20
    assert nr.hour == 5
    assert nr.weekday() == dow
    assert nr.month == 1

    nr += datetime.timedelta(minutes=1)
    nr = job.next_run(nr)
    assert nr.minute == 40
    assert nr.hour == 5
    assert nr.weekday() == dow
    assert nr.month == 1

    nr += datetime.timedelta(minutes=1)
    nr = job.next_run(nr)
    assert nr.minute == 0
    assert nr.hour == 19
    assert nr.weekday() == dow
    assert nr.month == 1

    # Now should be on the 8th (the next Sunday)
    nr += datetime.timedelta(hours=1)
    nr = job.next_run(nr)
    assert nr.minute == 0
    assert nr.hour == 5
    assert nr.day == 8
    assert nr.month == 1

    # Now should be on the 14th (a Saturday)
    nr += datetime.timedelta(days=1)
    nr = job.next_run(nr)
    assert nr.minute == 0
    assert nr.hour == 5
    assert nr.day == 14
    assert nr.month == 1

    # Now should be on the 15th (the next Sunday)
    nr += datetime.timedelta(days=1)
    nr = job.next_run(nr)
    assert nr.minute == 0
    assert nr.hour == 5
    assert nr.day == 15
    assert nr.month == 1

    # Now should be on the 22nd (the next Sunday)
    nr += datetime.timedelta(days=1)
    nr = job.next_run(nr)
    assert nr.minute == 0
    assert nr.hour == 5
    assert nr.day == 22
    assert nr.month == 1

    # Now should be on the 27th (a Friday)
    nr += datetime.timedelta(days=1)
    nr = job.next_run(nr)
    assert nr.minute == 0
    assert nr.hour == 5
    assert nr.day == 27
    assert nr.month == 1

    # Now should jump three months ahead
    nr += datetime.timedelta(days=4)
    nr = job.next_run(nr)
    assert nr.minute == 0
    assert nr.hour == 5
    assert nr.day == 1
    assert nr.month == 4

