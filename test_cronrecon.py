import logging
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
