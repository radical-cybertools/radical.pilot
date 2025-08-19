__copyright__ = 'Copyright 2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os
import dill
import pickle
import codecs
import tempfile


_obj_dir       = tempfile.gettempdir()
_obj_file_name = 'rp_obj.pkl'
_obj_file_path = os.path.join(_obj_dir, _obj_file_name)


# ------------------------------------------------------------------------------
# Custom Exceptions
# ------------------------------------------------------------------------------

class SerializationError(Exception):
    def __init__(self, message: str, original_exception: Exception):
        super().__init__(f"{message}: {str(original_exception)}")
        self.original_exception = original_exception


class DeserializationError(Exception):
    def __init__(self, message: str, original_exception: Exception):
        super().__init__(f"{message}: {str(original_exception)}")
        self.original_exception = original_exception


# ------------------------------------------------------------------------------
# Serialization
# ------------------------------------------------------------------------------

def serialize_obj(obj):
    '''
    Serialize object using dill.
    '''
    try:
        if callable(obj):
            return dill.dumps(obj)
        else:
            return dill.dumps(obj, recurse=True)
    except Exception as e:
        try:
            return dill.dumps(obj, byref=True)
        except Exception as e2:
            raise SerializationError("Failed to serialize object", e2) from e2


def serialize_file(obj, fname=None):
    '''
    Serialize object to file.
    '''
    if not fname:
        fname = _obj_file_path

    try:
        with open(fname, 'wb') as f:
            f.write(serialize_obj(obj))
        return fname
    except Exception as e:
        raise SerializationError("Failed to serialize object to file", e) from e


def serialize_bson(obj):
    '''
    Serialize object to base64-encoded BSON.
    '''
    try:
        return codecs.encode(pickle.dumps(obj), "base64").decode()
    except Exception as e:
        raise SerializationError("Failed to serialize object to BSON", e) from e


# ------------------------------------------------------------------------------
# Deserialization
# ------------------------------------------------------------------------------

def deserialize_file(fname):
    '''
    Deserialize object from file.
    '''
    try:
        with open(fname, 'rb') as f:
            return dill.load(f)
    except Exception as e:
        raise DeserializationError("Failed to deserialize object from file", e) from e


def deserialize_obj(data):
    '''
    Deserialize object from bytes.
    '''
    try:
        return dill.loads(data)
    except Exception as e:
        raise DeserializationError("Failed to deserialize object from data", e) from e


def deserialize_bson(obj):
    '''
    Deserialize object from base64-encoded BSON.
    '''
    try:
        return pickle.loads(codecs.decode(obj.encode(), "base64"))
    except Exception as e:
        raise DeserializationError("Failed to deserialize object from BSON", e) from e
