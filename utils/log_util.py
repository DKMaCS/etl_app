import functools
import logging
from inspect import signature


def counter(func):
    """A decorator to affect indentation in our logging history

    param func: object
        any pre-existing function object
    returns: wrapper
        the call to the actual wrapped function
    """
    @functools.wraps(func)
    def wrapper_counter(*args, **kwargs):
        wrapper_counter.calls = 0
        return func(*args, **kwargs)
    return wrapper_counter


@counter
def log_trace(func):
    """Decorator of any function for our logging records

    param func: object
        any pre-existing function object
    returns: wrapper
        the call to the actual wrapped function
    """

    @functools.wraps(func)
    def wrapper_logging(*args, **kwargs):
        log_trace.calls += 1
        logging.info(f'{"   " * log_trace.calls}Entering {func.__name__} using {signature(func)}')
        target = func(*args, **kwargs)
        logging.info(f'{"   " * log_trace.calls}Leaving {func.__name__} using {signature(func)}')
        log_trace.calls -= 1
        return target
    return wrapper_logging
