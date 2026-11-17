import time


def map(key, value):
    # Simulate an unusually slow map task.
    if "SLOW" in value:
        time.sleep(5)

    for word in value.split():
        if word != "SLOW":
            yield word, 1


def reduce(key, values):
    return key, sum(values)
