
__copyright__ = 'Copyright 2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import functools

from typing import Callable

from .utils import serialize_obj, serialize_bson
from .utils import deserialize_obj, deserialize_bson


# ------------------------------------------------------------------------------
#
class PythonTask(object):

    # --------------------------------------------------------------------------
    #
    def __new__(cls, func, *args, **kwargs):
        '''
        We handle wrapped functions here with no args or kwargs.

        Example:
            import PythonTask
            wrapped_func = partial(func_A, func_AB)
            td.function  = pythonTask(wrapped_func)
        '''

        if not callable(func):
            raise ValueError('task function not callable')

        task = {'func'  : serialize_obj(func),
                'args'  : args,
                'kwargs': kwargs}

        return serialize_bson(task)



    # --------------------------------------------------------------------------
    #
    @staticmethod
    def get_func_attr(bson_obj):
        '''
        Deserialize function call from BSON string.

        :param bson_obj: serialized PythonTask
        :return: callable, args, and kwargs

        Raises
        ------
        ValueError
            argument is not a `str`
        TypeError
            serialized object does not appear to be a PythonTask
        Exception
            error raised when attempting to deserialize *bson_obj*
        '''

        if not isinstance(bson_obj, str):
            raise ValueError('bson object should be string')

        pytask = deserialize_bson(bson_obj)

        for key in ['func', 'args', 'kwargs']:
            if key not in pytask:
                raise TypeError('Encoded object has unexpected schema.')

        args   = pytask['args']
        kwargs = pytask['kwargs']
        func   = deserialize_obj(pytask['func'])

        return func, list(args), kwargs


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def pythontask(f: Callable):
        '''
        We handle all other functions here.

        Example:
            from PythonTask import pythonfunc as pythonfunc
            @pythontask
            def func_C(x):
                return (x)
            cud.EXECUTABLE = func_C(2)
        '''

        if not callable(f):
            raise ValueError('task function not callable')

        # ----------------------------------------------------------------------
        @functools.wraps(f)
        def decor(*args, **kwargs):

            task = {'func'  : serialize_obj(f),
                    'args'  : args,
                    'kwargs': kwargs}

            return serialize_bson(task)

        return decor
        # ----------------------------------------------------------------------


# ------------------------------------------------------------------------------

