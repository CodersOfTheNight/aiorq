def some_calculation(x, y, z=1):

    return x * y / z


class Number:

    def __init__(self, value):

        self.value = value

    def div(self, y):

        return self.value / y
