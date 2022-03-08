
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import pickle
import codecs
import pathlib

import radical.utils as ru

from dill import dump  as _DUMPFILE
from dill import load  as _LOADFILE
from dill import dumps as _DUMPOBJ
from dill import loads as _LOADOBJ


class Serializer(object):

    """
    The serializer should be able to (de)serialize information that we want
    to send over the wire from the client side to the agent side via
    1- ZMQ
    2- MongoDB
    """


    def __init__(self):
        """ Instantiate the class
        """
        self._log           = ru.Logger(name='serializer', level='DEBUG')
        self._obj_dir       = pathlib.Path.home()
        self._obj_file_name = 'rp_obj.pkl'
        self._obj_file_path = os.path.join(self._obj_dir, self._obj_file_name)


    def serialize_obj(self, obj):
        """
        serialize object
        """
        result = None
        exception = None

        if callable(obj):
            try:
                result = _DUMPOBJ(obj)
            except Exception as e:
                exception = e
                self._log.error("failed to serialize object: %s", e)

            if not result:
                # see issue: https://github.com/uqfoundation/dill/issues/128
                try:
                    result = _DUMPOBJ(obj, byref = True)
                except Exception as e:
                    exception = e
                    self._log.error("failed to serialize object (by ref): %s", e)

        else:
            try:
                result = _DUMPOBJ(obj, recurse = True)
            except Exception as e:
                exception = e
                self._log.error("failed to serialize object: %s", e)

            if not result:
                # see issue: https://github.com/uqfoundation/dill/issues/128
                try:
                    result = _DUMPOBJ(obj, byref = True)
                except Exception as e:
                    exception = e
                    self._log.error("failed to serialize object (by ref): %s", e)

        if result is None:
            raise Exception("object %s is not serializable", exception)

        return  result


    def serialize_file(self, obj):
        """
        serialize object to file
        """
        result = None
        exception = None

        if callable(obj):
            try:
                with open(self._obj_file_path, 'wb') as f:
                    _DUMPFILE(obj, f)
                    result = self._obj_file_path

            except Exception as e:
                exception = e
                self._log.error("failed to serialize object to file: %s", e)

        else:
            try:
                with open(self._obj_file_path, 'wb') as f:
                    _DUMPFILE(obj, f, recurse = True)
                    result = self._obj_file_path
            except Exception as e:
                exception = e
                self._log.error("failed to serialize object to file: %s", e)

        if result is None:
            raise Exception("object is not serializable: %s", exception)

        return self._obj_file_path



    def deserialize_file(self, file_obj):
        """
        Deserialize object from file
        """
        result = None
        exception = None

        if os.path.isfile(file_obj):
            try:
                with open(file_obj, 'rb') as f:
                    result = _LOADFILE(f)
                    self._log.debug(result)

            except Exception as e:
                exception = e
                self._log.error("failed to deserialize object from file: %s", e)
        else:
            self._log.error("failed to deserialize object: %s", exception)

        if result is None:
            raise Exception("deserilization from file failed: %s", exception)
        return result


    def deserialize_obj(self, obj):
        """
        Deserialize object from str
        """
        result = None
        exception = None

        try:
            result = _LOADOBJ(obj)
            self._log.debug(result)

        except Exception as e:
            exception = e
            self._log.error("failed to deserialize from object: %s", e)

        if result is None:
            raise Exception("deserilization from object failed: %s", exception)
        return result


    def serialize_bson(self, obj):

        try:
            result = codecs.encode(pickle.dumps(obj), "base64").decode()
        except Exception as e:
            raise Exception("serilization to mongo_obj failed: %s", e)

        return result


    def deserialize_bson(self, obj):

        try:
            result = pickle.loads(codecs.decode(obj.encode(), "base64"))
        except Exception as e:
            raise Exception("deserilization from mongo_obj failed: %s", e)

        return result
