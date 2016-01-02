from aiorq.decorators import job
from rq import Connection
from rq.job import Job


def add(*args):

    return sum(args)


@job('my_queue')
def summator(*args):

    return add(*args)


def job_summator(id1, id2):

    with Connection():
        job1 = Job.fetch(id1)
        job2 = Job.fetch(id2)

    return add(job1.result, job2.result)
