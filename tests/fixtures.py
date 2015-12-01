def say_hello(name=None):

    if name is None:
        name = 'Stranger'
    return 'Hi there, %s!' % (name,)


def some_calculation(x, y, z=1):

    return x * y / z


class Number:

    def __init__(self, value):

        self.value = value

    def div(self, y):

        return self.value / y
