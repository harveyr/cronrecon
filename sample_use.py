import croncrush

crusher = croncrush.CronCrusher('crontab')
print '\n\nALL JOBS\n'
crusher.all_jobs()
print '\n\nNEXT JOB\n'
crusher.upcoming_jobs(1)
print '\n\nTHREE JOBS\n'
crusher.upcoming_jobs(3)

# print crusher.job_for_line(7)
