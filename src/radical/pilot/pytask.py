
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
    def __new__(cls, func, args=(), kwargs=None):
        '''
        We handle wrapped functions here with no args or kwargs.

        Example:
            import PythonTask
            wrapped_func = partial(func_A, func_AB)
            td = rp.TaskDescription()
            td.function  = PythonTask(wrapped_func)
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
        """Deserialize function call from BSON string.

        Args:
            bson_obj (str): serialized PythonTask

        Returns:
            tuple: callable, args, and kwargs

        Raises:
            ValueError: argument is not a `str`
            TypeError: serialized object does not appear to be a PythonTask
            Exception: error raised when attempting to deserialize *bson_obj*

        """

        if not isinstance(bson_obj, str):
            raise ValueError('bson object should be string')

        pytask = deserialize_bson(bson_obj)
        if any(key not in pytask for key in ('args', 'func', 'kwargs')):
            raise TypeError('Encoded object does not have the expected schema.')
        args   = list(pytask['args'])
        kwargs = pytask['kwargs']
        func   = deserialize_obj(pytask['func'])

        return func, args, kwargs


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def pythontask(f: Callable):
        """We handle all other functions here.

        Example:            
            import radical.pilot as rp

            @rp.pythontask
            def func_C(x):
                return (x)
            td = rp.TaskDescription()
            td.function = func_C(2)

        """

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
