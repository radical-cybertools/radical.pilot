
'''
The serializer should be able to (de)serialize information that we want
to send over the wire from the client side to the agent side via
1- ZMQ
2- MongoDB

we except:
    1- Callables with and without dependecies.
    2- Non-callables like classes and other python objects
'''
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
#
def serialize_obj(obj):
    '''
    serialize object
    '''

    if callable(obj):
        try:
            return dill.dumps(obj)
        except:
            # if we fail, then pickle it by reference
            # see issue: https://github.com/uqfoundation/dill/issues/128
            return dill.dumps(obj, byref = True)

    else:
        try:
            return dill.dumps(obj, recurse = True)
        except:
            # if we fail, then pickle it by reference
            # see issue: https://github.com/uqfoundation/dill/issues/128
            return dill.dumps(obj, byref = True)


# ------------------------------------------------------------------------------
#
def serialize_file(obj, fname=None):
    '''
    serialize object to file
    '''

    if not fname:
        fname = _obj_file_path

    # FIXME: assign unique path and id for the pickled file
    #        to avoid overwriting to the same file
    with open(fname, 'wb') as f:
        f.write(serialize_obj(obj))

    return _obj_file_path


# ------------------------------------------------------------------------------
#
def deserialize_file(fname):
    '''
    Deserialize object from file
    '''

    with open(fname, 'rb') as f:
        return dill.load(f)


# ------------------------------------------------------------------------------
#
def deserialize_obj(data):
    '''
    Deserialize object from str.
    '''

    return dill.loads(data)


# ------------------------------------------------------------------------------
#
def serialize_bson(obj):

    return codecs.encode(pickle.dumps(obj), "base64").decode()


# ------------------------------------------------------------------------------
#
def deserialize_bson(obj):

    return pickle.loads(codecs.decode(obj.encode(), "base64"))


# ------------------------------------------------------------------------------

