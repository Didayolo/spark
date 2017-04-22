def generator(start, f):
    """ Generateur recursif (RecursivelyEnumeratedSet) """
    yield start
    for x in generator(f(start), f):
        yield x

