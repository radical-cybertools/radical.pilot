
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import pathlib

import radical.utils as ru

from dill import dump  as DUMPFILE
from dill import load  as LOADFILE 
from dill import dumps as DUMPOBJ
from dill import loads as LOADOBJ

FUNC_DIR       = pathlib.Path.home()
FUNC_FILE_NAME = 'func.pkl'
FUNC_FILE_PATH = os.path.join(FUNC_DIR, FUNC_FILE_NAME)

log = ru.Logger(name='serializer', level='DEBUG')


class FuncSerializer(object):

    """
    The serializer should be able to serialize information that we want
    to send over the wire as a CU from the client side to the agent side:

        * Function body (func)
        1- Byte code
        2- string

        * Function dependencies:
          We want to chase down all of the dependencies of any serialized function
          using dill.

        * Args  + Kwargs

    """


    def __init__(self):
        """ Instantiate the class
        """


    def serialize_obj(func):
        """
        serialize funcion body and their dependcies to object
        """
        serialized_func = None

        if callable(func):
            try:
                serialized_func = DUMPOBJ(func)

            except Exception:
                log.error("Failed to serialize function %s to object", (func))

        else:
            try:
                serialized_func = DUMPOBJ(func, recurse=True)
            except Exception:
                log.error("Failed to serialize function %s", (func))


        if serialized_func is None:
            raise Exception("Function %s is not serializable", (func))
        return  serialized_func


    def serialize_file(func):
        """
        serialize funcion body and their dependcies to file
        """
        try:

            if callable(func):
                try:
                    with open(FUNC_FILE_PATH, 'wb') as f:
                        DUMPFILE(func, f)

                except Exception:
                    log.error("Failed to serialize function %s to file", (func))

            else:
                try:
                    with open(FUNC_FILE_PATH, 'wb') as f:
                        DUMPFILE(func, f, recurse=True)
                except Exception:
                    log.error("Failed to serialize function %s to file", (func))


        except Exception:
            raise Exception("Function %s is not serializable with dill to file", (func))
        return  FUNC_FILE_PATH


    def deserialize_file(obj):
        """
        Deserialize function body from file
        """
        result = None

        if os.path.isfile(obj):
            try:
                with open(obj, 'rb') as f:
                    result = LOADFILE(f)
                    log.debug(result)

            except Exception as e:
                log.error("Failed to deserialize function from file due to %s:", e)
        else:
            log.error("Failed to deserialize function: not a valid file or does not exist %s:", e)

        if result is None:
            raise Exception("Deserilization from file failed")
        return result

    def deserialize_obj(obj):
        """
        Deserialize function body from object
        """
        result = None

        try:
            result = LOADOBJ(obj)
            log.debug(result)

        except Exception as e:
            log.error("Failed to deserialize function from object due to %s:", e)

        if result is None:
            raise Exception("Deserilization from object failed")
        return result

