
def map(key, value):
    """Emit one (word, 1) pair for each word in the input."""
    for word in value.lower().split():
        yield word, 1


def reduce(key, values):
    """Return the total count for each word."""
    return key, sum(values)
