
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import pickle
import codecs
import functools
from radical.pilot.serialize import serializer as serialize

TASK = dict 

class PythonTask(object):

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

        if not callable(f):
            raise ValueError('Task function not callable')

        @functools.wraps(f)
        def decor(*args, **kwargs): 
            TASK = {'func'  :f,
                    'args'  :args,
                    'kwargs':kwargs}
            return TASK 
        return decor

    def mpirun(f):

        if not callable(f):
            raise ValueError('Task function not callable')
        
        @functools.wraps(f)
        def decor(*args, **kwargs):
            ser_func = serialize.FuncSerializer.serialize_obj(f)
            TASK = {'func'  :ser_func,
                    'args'  :args,
                    'kwargs':kwargs}
            try:

                SER_TASK = codecs.encode(pickle.dumps(TASK), "base64").decode()
                return SER_TASK
            except Exception as e:
                raise ValueError(e)
        return decor
