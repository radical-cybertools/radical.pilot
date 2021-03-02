
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import functools


class PythonTask(object):

    TASK = {} 

    def __new__(cls, func, *args, **kwargs):
        """
        We handle wrapped functions here with no args or kwargs.
        Example:
        import PythonTask
        wrapped_func   = partial(func_A, func_AB)      
        cud.EXECUTABLE = PythonTask(wrapped_func)
        """

        TASK = {'func'  :func,
                'args'  :args,
                'kwargs':kwargs}

        return TASK

    def pythontask(f):
        """
        We handle all other functions here.
        Example:
        from PythonTask import pythonfunc as pythonfunc
        @pythontask
        def func_C(x):
            return (x)
        cud.EXECUTABLE = func_C(2)
        """
        @functools.wraps(f)
        def decor(*args, **kwargs): 
            TASK = {'func'  :f,
                    'args'  :args,
                    'kwargs':kwargs}
            return TASK 
        return decor

