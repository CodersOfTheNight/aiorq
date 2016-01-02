from aiorq.decorators import job


def add(*args):

    return sum(args)


@job('my_queue')
def summator(*args):

    return add(*args)
