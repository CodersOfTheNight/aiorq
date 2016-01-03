from aiorq.decorators import job
from rq import Connection, Queue


def add(*args):

    return sum(args)


@job('my_queue')
def summator(*args):

    return add(*args)


def job_summator(id1, id2):

    with Connection():
        job1 = Queue().fetch_job(id1)
        job2 = Queue().fetch_job(id2)

    return add(job1.result, job2.result)
