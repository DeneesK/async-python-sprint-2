from functools import wraps


def coro_initializer(func):
    @wraps(func)
    def wraper(*args, **kwargs):
        gen_func = func(*args, **kwargs)
        next(gen_func)
        return gen_func
    return wraper
