from ordered_pipe import OrderedPipe

def add (a, b):
    return a + b


def multiply (a, b):
    return a * b


# (d1 + d2) * d3 + d1 * d3

def test_pipeline():
    p = OrderedPipe(
        f1 = (add,      'd1', 'd2'),
        f2 = (multiply, 'f1', 'd3'),
        f3 = (multiply, 'd1', 'd3'),
        f4 = (add,      'f2', 'f3'),
    )
    result = p(
        parallel=True,
        d1 = 8,
        d2 = 9,
        d3 = 10,
    )
    print(result)
    assert result == 250


if __name__ == '__main__':
    test_pipeline()