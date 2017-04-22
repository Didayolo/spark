def infinity(start):
    yield start
    for x in infinity(start + 1):
        yield x

it = infinity(1)
print(next(it))
print(next(it))
