import sys
import datetime
import calendar
import logging

logging.basicConfig(level=logging.ERROR,
    format='%(levelname)s %(module)s (%(lineno)s): %(message)s')


class CronJob(object):
    """Represents a line of a crontab."""

    MAX_MINUTE = 60
    MIN_MINUTE = 0
    MAX_MONTH = 13
    MIN_MONTH = 1
    MAX_HOUR = 24
    MIN_HOUR = 0
    MAX_DOM = 32
    MIN_DOM = 1
    MAX_DOW = 7
    MIN_DOW = 0

    # Recursion counter for debugging
    counter = 0

    def __init__(self, raw_string):
        self.raw_string = raw_string
        self.parse()

    def __repr__(self):
        return ('[CronJob] Action: {} Minute: {}; Hour: {}; DOM: {}; ' +
            ' Month: {}; DOW: {}').format(self.action, self.minute,
            self.hour, self.dom, self.month, self.dow)

    def list_repr(self):
        return ('CronJob:\nMinute: {0}\n Hour: {1}\n DOM: {2}\n Month: {3}\n' +
            'DOW: {4}').format(
            self.cron_minutes, self.cron_hours, self.cron_dom,
            self.cron_months, self.cron_dow)

    def parse(self):
        """Populates this object with lists containing the months,
        days, hours, etc. when this cron job will run. Those lists are
        used by next_run()."""
        def finish_parse(cron_str, cron_list, max_value):
            # Recursive parse when there is a comma or dash.
            # (Recursion only on commma.)
            if ',' in cron_str:
                for substr in cron_str.split(','):
                    finish_parse(substr, cron_list, max_value)
            elif '-' in cron_str:
                vals = cron_str.split('-')
                for i in range(int(vals[0]), int(vals[1]) + 1):
                    cron_list.append(i)
            else:
                try:
                    cron_list.append(int(cron_str))
                    # if num != 0:
                    #     while num * 2 < max_value:
                    #         num *= 2
                    #         cron_list.append(num)
                except ValueError as e:
                    logging.error(e)

        def start_parse(field_str, min_value, max_value):
            cron_list = []
            if field_str == '*':
                cron_list = range(min_value, max_value)
            elif '*/' in field_str:
                # Evaluate frequency/period of this element.
                freq = int(field_str[2:])
                # Cron months are all the months occurring on that frequency
                cron_list = range(min_value, max_value)[::freq]
            else:
                finish_parse(field_str, cron_list, max_value)

            return sorted(cron_list)

        fields = filter(None, self.raw_string.split(" "))
        for i in range(len(fields)):
            if i == 0:
                self.minute = fields[i]
            elif i == 1:
                self.hour = fields[i]
            elif i == 2:
                self.dom = fields[i]
            elif i == 3:
                self.month = fields[i]
            elif i == 4:
                self.dow = fields[i]

        self.action = ' '.join(fields[5:]).strip()

        self.cron_months = start_parse(self.month, self.MIN_MONTH,
            self.MAX_MONTH)
        self.cron_minutes = start_parse(self.minute, self.MIN_MINUTE,
            self.MAX_MINUTE)
        self.cron_hours = start_parse(self.hour, self.MIN_HOUR,
            self.MAX_HOUR)
        self.cron_dom = start_parse(self.dom, self.MIN_DOM,
            self.MAX_DOM)
        self.cron_dow = start_parse(self.dow, self.MIN_DOW,
            self.MAX_DOW)

    def next_run(self, start_dt=None):
        """Returns a timedate object for the time when this
        job will next run. This method starts at present time and
        recursively nudges the date forward until it hits the first
        cron job time. There is probably a better way to do this."""

        # The following methods replace a certain element of a timedate.
        # This happens when we're moving forward in time. So, for example,
        # if we're bumping forward a month, we want to zero out the smaller
        # time fields.
        #
        # Note that the self.MIN_* assignments should probably be replaced
        # with the first selements in the self.cron_* lists, so we're jumping
        # straight to the first cron job instead of simply to the minimum
        # allowable time values. I won't have time to test this, though.
        def replace_year(dt, year):
            return dt.replace(year=year,
                month=self.MIN_MONTH,
                day=self.MIN_DOM,
                hour=self.MIN_HOUR,
                minute=self.MIN_MINUTE)

        def replace_month(dt, month):
            return dt.replace(month=month,
                day=self.MIN_DOM,
                hour=self.MIN_HOUR,
                minute=self.MIN_MINUTE)

        def replace_day(dt, day):
            return dt.replace(day=day,
                hour=self.MIN_HOUR,
                minute=self.MIN_MINUTE)

        def replace_hour(dt, hour):
            return dt.replace(hour=hour,
                minute=self.MIN_MINUTE)

        def replace_minute(dt, minute):
            return dt.replace(minute=minute)

        # This is the method that is recursively called.
        def create_date(start_dt):

            self.counter += 1
            if self.counter == 80:
                # Recursion halt for debugging.
                sys.exit()

            # Step 1: Find next month in which job will run.
            # (See whether any of the remaining months in the current year
            # match any of the cron job's months.)
            rem_months = range(start_dt.month, self.MAX_MONTH)
            try:
                next_month = next(i for i in rem_months if i in self.cron_months)
                if next_month != start_dt.month:
                    start_dt = replace_month(start_dt, next_month)
            except Exception:
                # If no months match, move into first month of next year,
                # and restart.
                start_dt = replace_year(start_dt, start_dt.year + 1)
                return create_date(start_dt)

            logging.debug('month set to %s' % start_dt.month)

            # Step 2. Deal with DOM versus DOW. In theory, this treats DOM
            # and DOW as cumulative when they are both set.
            # I read that this is how cron works in that scenario.
            #
            # This is currently more expensive than it should be.

            use_dom = False

            # Flags determine whether, after the comparison,
            # we need to make a recursive call.
            in_next_month = False
            in_next_week = False

            rem_dom = range(start_dt.day, self.MAX_DOM)
            test_dom = start_dt
            try:
                next_dom = next(i for i in rem_dom if i in self.cron_dom)
                if next_dom != start_dt.day:
                    test_dom = replace_day(start_dt, next_dom)
            except Exception:
                # If no days match, move into next month and restart.
                # First, find how many days left until next month's first
                # job, and then advance those days.
                mr = calendar.monthrange(start_dt.year, start_dt.month)
                add_days = mr[-1] - test_dom.day + self.cron_dom[0]
                test_dom += datetime.timedelta(days=add_days)
                test_dom = test_dom.replace(hour=self.MIN_HOUR,
                    minute=self.MIN_MINUTE)
                in_next_month = True

            rem_dow = range(start_dt.weekday(), self.MAX_DOW)
            test_dow = start_dt
            try:
                next_dow = next(i for i in rem_dow if i in self.cron_dow)
                add_days = next_dow - start_dt.weekday()
                # logging.debug('1st add_days {0} ({1} - {2})'.format(
                #     add_days, next_dow, start_dt.weekday()))
                if add_days > 0:
                    test_dow += datetime.timedelta(days=add_days)
                    test_dow = test_dow.replace(hour=self.MIN_HOUR,
                        minute=self.MIN_MINUTE)
            except Exception:
                # If no weekdays match, move into next week and restart.
                add_days = self.MAX_DOW - test_dow.weekday()
                logging.debug('2nd add_days {0} ({1}-{2}) to date {3}'.format(
                    add_days, self.MAX_DOW, start_dt.weekday(), test_dow))
                test_dow += datetime.timedelta(days=add_days)
                test_dow = replace_hour(test_dow, self.MIN_HOUR)
                logging.debug(' ... new test_dow: %s' % test_dow)
                in_next_week = True

            logging.debug('resulting dow: %s' % test_dow.weekday())

            # Determine whether to use DOM or DOW
            if self.dom != '*' and self.dow == '*':
                use_dom = True
            elif self.dom == '*' and self.dom != '*':
                use_dom = False
            elif self.dom != '*' and self.dow != '*':
                if test_dom < test_dow:
                    use_dom = True
                else:
                    use_dom = False

            if use_dom:
                logging.debug('using dom')
                start_dt = test_dom
                if in_next_month:
                    return create_date(start_dt)
            else:
                logging.debug('using dow')
                start_dt = test_dow
                if in_next_week:
                    return create_date(start_dt)

            logging.debug('day set to %s' % start_dt.day)

            # Calculate hour.

            rem_hours = range(start_dt.hour, self.MAX_HOUR)
            try:
                next_hour = next(i for i in rem_hours if i in self.cron_hours)
                if next_hour != start_dt.hour:
                    start_dt = replace_hour(start_dt, next_hour)
            except Exception:
                # If no hours match, move into next day and restart.
                start_dt += datetime.timedelta(days=1)
                start_dt = replace_hour(start_dt, self.MIN_HOUR)
                return create_date(start_dt)

            logging.debug('hour set to %s' % start_dt.hour)

            rem_mins = range(start_dt.minute, self.MAX_MINUTE)
            try:
                next_min = next(i for i in rem_mins if i in self.cron_minutes)
                if next_min != start_dt.minute:
                    start_dt = replace_minute(start_dt, next_min)
            except Exception:
                # If no minutes match, move into next hour and restart.
                start_dt += datetime.timedelta(hours=1)
                start_dt = replace_minute(start_dt, self.MIN_MINUTE)
                return create_date(start_dt)

            logging.debug('minute set to %s' % start_dt.minute)

            return start_dt
        # end of create_date()

        if start_dt is None:
            start_dt = datetime.datetime.now()
        start_dt = datetime.datetime(start_dt.year,
            start_dt.month,
            start_dt.day,
            start_dt.hour,
            start_dt.minute)

        logging.debug(self.list_repr())

        return create_date(start_dt)


class CronCrusher(object):

    def __init__(self, filename):
        self.filename = filename
        self.cronjobs = []
        logging.debug('CronTracker created with %s' % filename)
        self.parse_file()

    def parse_file(self):
        try:
            f = open(self.filename)
        except IOError as e:
            logging.error('Failed to crush cron file {0}. ({1})'.format(
                self.filename, e))
            return None

        for line in f:
            line = line.lstrip()
            if line and not line[0] == '#':
                job = CronJob(line)
                self.cronjobs.append(job)
            else:
                # ignore line
                pass

    def upcoming_jobs(self, n=None):
        # Make a list of all upcoming jobs and their dates
        l = []
        for i in range(len(self.cronjobs)):
            job = self.cronjobs[i]
            d = {'job_index': i, 'date': job.next_run()}
            l.append(d)

        # Avoid index errors.
        if not n or n >= len(self.cronjobs):
            n = len(self.cronjobs) - 1

        # Return n dates
        l.sort(key=lambda item:item['date'])
        for i in range(n):
            item = l[i]
            job = self.cronjobs[item['job_index']]
            print '{}\n   Next run date: {}'.format(
                job, item['date'])

    def next_job(self):
        return self.upcoming_jobs(1)

    def all_jobs(self):
        return self.upcoming_jobs()

    # for debugging
    def job_for_line(self, index):
        logging.debug('finding next job for %s' % self.cronjobs[index - 1])
        return self.cronjobs[index - 1].next_run()
