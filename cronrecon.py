import sys
import datetime
import calendar
import logging

logging.basicConfig(level=logging.DEBUG,
    format='%(levelname)s %(module)s (%(lineno)s): %(message)s')
logging.disable(logging.DEBUG)


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
        self.minute = None
        self.hour = None
        self.dom = None
        self.month = None
        self.dow = None
        self.action = None

        self.cron_months = None
        self.cron_minutes = None
        self.cron_hours = None
        self.cron_dom = None
        self.cron_dow = None

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

        # Parse the text from the cron entry.
        fields = filter(None, self.raw_string.split(' '))
        self.minute = fields[0]
        self.hour = fields[1]
        self.dom = fields[2]
        self.month = fields[3]
        self.dow = fields[4]
        self.action = ' '.join(fields[5:]).strip()

        # Create the lists of months, minutes, days when job will run
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
        job will next run. This method starts by finding the next
        cron minute and moves on up from there."""

        def first_common_value(list1, list2):
            # Finds the first matching element in both lists
            try:
                return next(i for i in list1 if i in list2)
            except Exception:
                return None

        def set_next_minute(start_dt):
            remaining_mins = range(start_dt.minute, self.MAX_MINUTE)
            next_min = first_common_value(remaining_mins,
                self.cron_minutes)
            if next_min is not None and next_min != start_dt.minute:
                start_dt = start_dt.replace(minute=next_min)
            elif next_min is None:
                # If no minutes match, move into next hour.
                start_dt += datetime.timedelta(hours=1)
                start_dt = start_dt.replace(minute=self.cron_minutes[0])

            return start_dt

        def set_next_hour(start_dt):
            remaining_hours = range(start_dt.hour, self.MAX_HOUR)
            next_hour = first_common_value(remaining_hours,
                self.cron_hours)
            if next_hour is not None and next_hour != start_dt.hour:
                start_dt = start_dt.replace(hour=next_hour)
            elif next_hour is None:
                # If no hours match, move into next day and restart.
                start_dt += datetime.timedelta(days=1)
                start_dt = start_dt.replace(hour=self.cron_hours[0])

            return start_dt

        def get_next_dom(start_dt):
            remaining_dom = range(start_dt.day, self.MAX_DOM)
            return_dt = start_dt

            next_dom = first_common_value(remaining_dom,
                self.cron_dom)
            if next_dom is not None and next_dom != start_dt.day:
                return_dt = return_dt.replace(day=next_dom)
            elif next_dom is None:
                # If no days match, move into next month by
                # determining how many days left until next month's first
                # job and then advancing those days.
                mr = calendar.monthrange(start_dt.year, start_dt.month)
                add_days = mr[-1] - return_dt.day + self.cron_dom[0]
                return_dt += datetime.timedelta(days=add_days)

            return return_dt

        def get_next_dow(start_dt):
            remaining_dow = range(start_dt.weekday(), self.MAX_DOW)
            return_dt = start_dt
            next_dow = first_common_value(remaining_dow,
                self.cron_dow)
            if next_dow is not None:
                add_days = next_dow - start_dt.weekday()
                if add_days > 0:
                    return_dt += datetime.timedelta(days=add_days)
            else:
                # If no weekdays match, move into next week.
                add_days = (self.MAX_DOW - return_dt.weekday() +
                    self.cron_dow[0])
                return_dt += datetime.timedelta(days=add_days)

            return return_dt

        def set_next_day(start_dt):
            # Deal with DOM versus DOW. This should treat DOM
            # and DOW as cumulative when they are both set. Test days for both
            # DOM and DOW are found to determine which might be next.

            if self.dom != '*' and self.dow == '*':
                # If dom is set and dow is not, use dom.
                return get_next_dom(start_dt)

            if self.dom == '*' and self.dow != '*':
                # If dow is set and dom is not, use dow.
                return get_next_dow(start_dt)

            test_dom = get_next_dom(start_dt)
            test_dow = get_next_dow(start_dt)

            if self.dom != '*' and self.dow != '*':
                # If both are set, use the earliest one.
                if test_dom < test_dow:
                    return get_next_dom(start_dt)
                else:
                    return get_next_dow(start_dt)

        def set_next_month(start_dt):
            # Find next month in which job will run.
            # (See whether any of the remaining months in the current year
            # match any of the cron job's months.)
            remaining_months = range(start_dt.month, self.MAX_MONTH)
            next_month = first_common_value(remaining_months,
                self.cron_months)
            if next_month is not None and next_month != start_dt.month:
                start_dt = start_dt.replace(month=next_month)
            elif next_month is None:
                # If no months match, move into first month of next year.
                start_dt = start_dt.replace(year=start_dt.year + 1,
                    month=self.cron_months[0])

            return start_dt

        def create_date(start_dt):
            logging.debug(self)
            logging.debug('0: %s' % start_dt)
            start_dt = set_next_minute(start_dt)
            logging.debug('1: %s' % start_dt)
            start_dt = set_next_hour(start_dt)
            logging.debug('2: %s' % start_dt)
            start_dt = set_next_day(start_dt)
            logging.debug('3: %s' % start_dt)
            start_dt = set_next_month(start_dt)
            logging.debug('4: %s' % start_dt)

            return start_dt

        if start_dt is None:
            start_dt = datetime.datetime.now()
        # New date with only the relevant time fields.
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
            '[disabled while testing]')

        # return ('CronJob: {}\n\tMinute: {}\n\tHour: {}\n\t' +
        #     'DOM: {}\n\tMonth: {}\n\tDOW: {}\n\t' +
        #     'Next Run Date/Time: {}').format(self.action,
        #     self.minute, self.hour, self.dom, self.month, self.dow,
        #     self.next_run())


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

    def jobs_matching_str(self, match_str):
        li = [job for job in self.cronjobs if (
            match_str.lower() in job.action.lower())]
        return li

    def upcoming_jobs(self, n=None):
        # Make a list of n upcoming jobs and their dates
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
