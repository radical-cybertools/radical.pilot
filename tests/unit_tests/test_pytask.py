# pylint: disable=unused-argument, no-value-for-parameter

from unittest import TestCase

from radical.pilot import PythonTask


# ------------------------------------------------------------------------------
#
class TestPytask(TestCase):

    # --------------------------------------------------------------------------
    #
    def test_class_call(self):

        from typing import Callable
        from functools import partial

        def AA(func: Callable, x):
            eq = func(x)
            print (eq * 4)

        def AB(z):
            return 2 * z

        wrapped_function = partial(AA, AB)
        print(type(wrapped_function))
        print(callable(wrapped_function))
        pytask_class_obj = PythonTask(wrapped_function)

        self.assertIsInstance(pytask_class_obj, str)


    # --------------------------------------------------------------------------
    #
    def test_callable_decor(self):

        @PythonTask.pythontask
        def hello_test(y):
            import time
            return time.time() * y

        decor_task = hello_test(2)

        self.assertIsInstance(decor_task, str)


# ------------------------------------------------------------------------------
