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

    def list_repr(self):
        return ('[CronJob Schedule Lists]\n\tMinute: {0}\n\t Hour: {1}\n\t' +
        'DOM: {2}\n\t Month: {3}\n\tDOW: {4}').format(
            self.cron_minutes, self.cron_hours, self.cron_dom,
            self.cron_months, self.cron_dow)

    def parse(self):
        """Populates this object with lists containing the months,
        days, hours, etc. when this cron job will run. Those lists are
        used by next_run()."""
        def finish_parse(cron_str, cron_list, max_value):
            # Handles commas, dashes, and integers.
            # (Recursion on commma.)
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
                except ValueError as e:
                    logging.error(e)

        def start_parse(field_str, min_value, max_value):
            cron_list = []
            if field_str == '*':
                # If asterisk, populate list with all possible values.
                cron_list = range(min_value, max_value)
            elif '*/' in field_str:
                # Evaluate frequency/period of this element.
                freq = int(field_str[2:])
                # Cron months are all the months occurring on that frequency
                cron_list = range(min_value, max_value)[::freq]
            else:
                # If here, there's a comma, dash, or integer.
                # finish_parse() handles them.
                finish_parse(field_str, cron_list, max_value)

            return sorted(cron_list)

        fields = filter(None, self.raw_string.split(' '))
        self.minute = fields[0]
        self.hour = fields[1]
        self.dom = fields[2]
        self.month = fields[3]
        self.dow = fields[4]
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
        """Returns a timedate object for the date/time when this
        job will next run. This method starts at present time and
        recursively nudges the date forward until it hits the first
        cron job time. There is probably a better way to do this."""

        # The following methods replace a certain element of a timedate.
        # This happens when we're moving forward in time. So, for example,
        # if we're bumping forward a month, we want to zero out the smaller
        # time fields so we start at the beginning of the month.
        # "Zeroing out" means starting at the first possible next job.
        # Accordingly, we set the new start time to the first element
        # in the self.cron_* lists, except for the day field, which could
        # be a DOM or DOW, so we just set that to the earliest possible day.

        def replace_year(dt, year):
            return dt.replace(year=year,
                month=self.cron_months[0],
                day=self.MIN_DOM,
                hour=self.cron_hours[0],
                minute=self.cron_minutes[0])

        def replace_month(dt, month):
            return dt.replace(month=month,
                day=self.MIN_DOM,
                hour=self.cron_hours[0],
                minute=self.cron_minutes[0])

        def replace_day(dt, day):
            return dt.replace(day=day,
                hour=self.cron_hours[0],
                minute=self.cron_minutes[0])

        def replace_hour(dt, hour):
            return dt.replace(hour=hour,
                minute=self.cron_minutes[0])

        def replace_minute(dt, minute):
            return dt.replace(minute=minute)

        def first_common_value(list1, list2):
            # Finds the first matching element in both lists
            return next(i for i in list1 if i in list2)

        def create_date(start_dt):
            # This method is recursively called until we land on a cron date.

            # Recursion halt for debugging.
            self.counter += 1
            if self.counter == 80:
                sys.exit()

            # Step 1: Find next month in which job will run.
            # (See whether any of the remaining months in the current year
            # match any of the cron job's months.)
            remaining_months = range(start_dt.month, self.MAX_MONTH)
            try:
                next_month = first_common_value(remaining_months,
                    self.cron_months)
                if next_month != start_dt.month:
                    start_dt = replace_month(start_dt, next_month)
            except Exception:
                # If no months match, move into first month of next year,
                # and restart.
                start_dt = replace_year(start_dt, start_dt.year + 1)
                return create_date(start_dt)

            logging.debug('month set to %s' % start_dt.month)

            # Step 2. Deal with DOM versus DOW. This should treat DOM
            # and DOW as cumulative when they are both set. Test days for both
            # DOM and DOW are found to determine which might be next.

            # These flags determine whether, after the comparison,
            # we need to make a recursive call.
            in_next_month = False
            in_next_week = False

            remaining_dom = range(start_dt.day, self.MAX_DOM)
            test_dom = start_dt
            try:
                next_dom = next(
                    i for i in remaining_dom if i in self.cron_dom)
                if next_dom != start_dt.day:
                    test_dom = replace_day(start_dt, next_dom)
            except Exception:
                # If no days match, move into next month by
                # determining how many days left until next month's first
                # job and then advancing those days.
                mr = calendar.monthrange(start_dt.year, start_dt.month)
                add_days = mr[-1] - test_dom.day + self.cron_dom[0]
                test_dom += datetime.timedelta(days=add_days)
                test_dom = test_dom.replace(hour=self.cron_hours[0],
                    minute=self.cron_minutes[0])
                in_next_month = True

            rem_dow = range(start_dt.weekday(), self.MAX_DOW)
            test_dow = start_dt
            try:
                next_dow = next(i for i in rem_dow if i in self.cron_dow)
                add_days = next_dow - start_dt.weekday()
                if add_days > 0:
                    test_dow += datetime.timedelta(days=add_days)
                    test_dow = test_dow.replace(hour=self.cron_hours[0],
                        minute=self.cron_minutes[0])
            except Exception:
                # If no weekdays match, move into next week and restart.
                add_days = self.MAX_DOW - test_dow.weekday()
                test_dow += datetime.timedelta(days=add_days)
                test_dow = replace_hour(test_dow, self.cron_hours[0])
                in_next_week = True

            use_dom = True

            logging.debug('dom: ({}), dow: ({})'.format(self.dom, self.dow))
            logging.debug('dom == *: {}'.format(self.dom == '*'))
            logging.debug('dow == *: {}'.format(self.dow == '*'))
            # Determine whether to use DOM or DOW
            if self.dom != '*' and self.dow == '*':
                # If dom is set and dow is not, use dom.
                use_dom = True
            elif self.dom == '*' and self.dow != '*':
                # If dow is set and dom is not, use dow.
                use_dom = False
            elif self.dom != '*' and self.dow != '*':
                # If both are set, use the earliest one.
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

            # Calculate hour.

            remaining_hours = range(start_dt.hour, self.MAX_HOUR)
            try:
                next_hour = next(
                    i for i in remaining_hours if i in self.cron_hours)
                if next_hour != start_dt.hour:
                    start_dt = replace_hour(start_dt, next_hour)
            except Exception:
                # If no hours match, move into next day and restart.
                start_dt += datetime.timedelta(days=1)
                start_dt = replace_hour(start_dt, self.cron_hours[0])
                return create_date(start_dt)

            remaining_mins = range(start_dt.minute, self.MAX_MINUTE)
            try:
                next_min = next(
                    i for i in remaining_mins if i in self.cron_minutes)
                if next_min != start_dt.minute:
                    start_dt = replace_minute(start_dt, next_min)
            except Exception:
                # If no minutes match, move into next hour and restart.
                start_dt += datetime.timedelta(hours=1)
                start_dt = replace_minute(start_dt, self.cron_minutes[0])
                return create_date(start_dt)

            return start_dt

        if start_dt is None:
            start_dt = datetime.datetime.now()
        start_dt = datetime.datetime(start_dt.year,
            start_dt.month,
            start_dt.day,
            start_dt.hour,
            start_dt.minute)

        return create_date(start_dt)

    def __repr__(self):
        return ('CronJob: {}\n\tMinute: {}\n\tHour: {}\n\t' +
            'DOM: {}\n\tMonth: {}\n\tDOW: {}\n\t' +
            'Next Run Date/Time: {}').format(self.action,
            self.minute, self.hour, self.dom, self.month, self.dow,
            self.next_run())


class CronExaminer(object):

    def __init__(self, filename):
        self.filename = filename
        self.cronjobs = []
        self.parse_file()

    def parse_file(self):
        try:
            f = open(self.filename)
        except IOError as e:
            logging.error('Failed to open cron file {}. ({})'.format(
                self.filename, e))
            return None

        for line in f:
            line = line.lstrip()
            if line and not line[0] == '#':
                job = CronJob(line)
                self.cronjobs.append(job)
                logging.debug('Created CronJob for {}'.format(job))
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
        l.sort(key=lambda item: item['date'])
        return_l = []
        for i in range(n):
            item = l[i]
            job = self.cronjobs[item['job_index']]
            return_l.append(job)
        return return_l

    def next_job(self):
        return self.upcoming_jobs(1)

    def all_jobs(self):
        return self.upcoming_jobs()

    # for debugging
    def job_for_line(self, index):
        logging.debug('finding next job for %s' % self.cronjobs[index - 1])
        return self.cronjobs[index - 1].next_run()
