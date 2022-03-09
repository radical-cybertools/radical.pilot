
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import dill
import pickle
import codecs
import tempfile

import radical.utils as ru


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
        self._obj_dir       = tempfile.gettempdir()
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
                result = dill.dumps(obj)
            except Exception as e:
                exception = e
                self._log.exception("failed to serialize object: %s", e)

            if not result:
                # see issue: https://github.com/uqfoundation/dill/issues/128
                try:
                    result = dill.dumps(obj, byref = True)
                except Exception as e:
                    exception = e
                    self._log.exception("failed to serialize object (by ref): %s", e)

        else:
            try:
                result = dill.dumps(obj, recurse = True)
            except Exception as e:
                exception = e
                self._log.exception("failed to serialize object: %s", e)

            if not result:
                # see issue: https://github.com/uqfoundation/dill/issues/128
                try:
                    result = dill.dumps(obj, byref = True)
                except Exception as e:
                    exception = e
                    self._log.exception("failed to serialize object (by ref): %s", e)

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
                    dill.dump(obj, f)
                    result = self._obj_file_path

            except Exception as e:
                exception = e
                self._log.exception("failed to serialize object to file: %s", e)

        else:
            try:
                with open(self._obj_file_path, 'wb') as f:
                    dill.dump(obj, f, recurse = True)
                    result = self._obj_file_path
            except Exception as e:
                exception = e
                self._log.exception("failed to serialize object to file: %s", e)

        if result is None:
            raise Exception("object is not serializable: %s", exception)

        return self._obj_file_path



    def deserialize_file(self, file_obj):
        """
        Deserialize object from file
        """
        result = None

        if not os.path.isfile(file_obj):
            self._log.error("%s is not a file", file_obj)
            return None

        try:
            with open(file_obj, 'rb') as f:
                result = dill.load(f)
                self._log.debug(result)
                if not result:
                    raise RuntimeError('failed to deserialize')
                return result

        except Exception as e:
            self._log.exception("failed to deserialize object from file: %s", e)


    def deserialize_obj(self, obj):
        """
        Deserialize object from str
        """
        result = None

        try:
            result = dill.loads(obj)
            self._log.debug(result)
            if not result:
                raise RuntimeError('failed to deserialize')
            return result

        except Exception as e:
            self._log.exception("failed to deserialize from object: %s", e)


    def serialize_bson(self, obj):

        result = codecs.encode(pickle.dumps(obj), "base64").decode()

        return result


    def deserialize_bson(self, obj):

        result = pickle.loads(codecs.decode(obj.encode(), "base64"))

        return result
