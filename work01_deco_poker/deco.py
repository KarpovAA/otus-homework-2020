#!/usr/bin/env python
# -*- coding: utf-8 -*-

from functools import update_wrapper

_disabled = set()


def disable(func):
    """Disable a decorator by re-assigning the decorator's name
    to this function. For example, to turn off memoization:
    >>> memo = disable    """
    _disabled.add(func)
    pass


def decorator(func):
    """Decorate a decorator so that it inherits the docstrings
    and stuff from the function it's decorating."""
    def decor(decor_func):
        return update_wrapper(func(decor_func), decor_func)
    update_wrapper(decor, func)
    return decor


@decorator
def countcalls(func):
    """Decorator that counts calls made to the function decorated."""
    def wrapper(*args, **kwargs):
        wrapper.calls += 1
        return func(*args, **kwargs)
    wrapper.calls = 0
    return wrapper


@decorator
def memo(func):
    """Memoize a function so that it caches all return values for faster future lookups."""
    temp = {}

    def wrapper(*args, **kwargs):
        if memo in _disabled:
            return func(*args, *kwargs)
        if args in temp:
            return temp[args]
        else:
            result = func(*args, **kwargs)
            temp[args] = result
        return result
    return wrapper


@decorator
def n_ary(func):
    """ Given binary function f(x, y), return an n_ary function such
    that f(x, y, z) = f(x, f(y,z)), etc. Also allow f(x) = x. """
    def wrapper(*args, **kwargs):
        return func(*args, *kwargs) if len(args) <= 2 else func(args[0], wrapper(*args[1:]))
    return wrapper


def trace(arg):
    """Trace calls made to function decorated.

    @trace("____")
    def fib(n):
        ....

    >>> fib(3)
     --> fib(3)
    ____ --> fib(2)
    ________ --> fib(1)
    ________ <-- fib(1) == 1
    ________ --> fib(0)
    ________ <-- fib(0) == 1
    ____ <-- fib(2) == 2
    ____ --> fib(1)
    ____ <-- fib(1) == 1
     <-- fib(3) == 3

    """
    @decorator
    def outer(func):
        level = 0

        def wrapper(*args, **kwargs):
            nonlocal level
            if level == 0:
                print('%s\n >>> fib(%s)' % (str(arg), str(args[0])))
            level += 1
            print('  ' * level + '%s fib(%s)' % ('-->', str(args[0])))
            result = func(*args, **kwargs)
            print('  ' * level + '%s fib(%s) == %s' % ('<--', str(args[0]), str(result)))
            level -= 1
            return result
        return wrapper
    return outer


@memo
@countcalls
@n_ary
def foo(a, b):
    """ foo = a+b """
    return a + b


@countcalls
@memo
@n_ary
def bar(a, b):
    """ bar = a*b """
    return a * b


@countcalls
@trace("####")
@memo
def fib(n):
    """Функция возвращает N-ое число Фибоначчи """
    return 1 if n <= 1 else fib(n-1) + fib(n-2)


def main():
    print(foo(4, 3))
    print(foo(4, 3, 2))
    print(foo(4, 3, 2, 1))
    print(foo(5, 4, 3, 2, 1))
    print("foo was called:", foo.calls, "times")
    print("foo docstring:", foo.__doc__)

    print(bar(4, 3))
    print(bar(4, 3, 2))
    print(bar(4, 3, 2, 1))
    print(bar(5, 4, 3, 2, 1))
    print("bar was called", bar.calls, "times")
    print("bar docstring:", bar.__doc__)

    print(fib(10))
    print("fib docstring:", fib.__doc__)
    print("fib was called:", fib.calls, 'times')

    disable(memo)
    print(fib(10))
    print("fib docstring:", fib.__doc__)
    print("fib was called:", fib.calls, 'times')


if __name__ == '__main__':
    main()
