import cronrecon

examiner = cronrecon.CronExaminer('crontab')

print '\n\nALL JOBS\n'
for job in examiner.all_jobs():
    print job

print '\n\nNEXT JOB\n'
for job in examiner.upcoming_jobs(1):
    print job

print '\n\nTHREE JOBS\n'
for job in examiner.upcoming_jobs(3):
    print job

print '\n\nJOBS MATCHING "memory_limit"'
for job in examiner.jobs_matching_str('memory_limit'):
    print job
