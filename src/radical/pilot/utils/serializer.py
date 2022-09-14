
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
    result    = None
    exception = None

    if callable(obj):
        try:
            result = dill.dumps(obj)
        except Exception as e:
            exception = e

        # if we fail, then pikle it by reference
        if result is None:
            # see issue: https://github.com/uqfoundation/dill/issues/128
            try:
                result = dill.dumps(obj, byref = True)
            except Exception as e:
                exception = e

    else:
        try:
            result = dill.dumps(obj, recurse = True)
        except Exception as e:
            exception = e

        if result is None:
            # see issue: https://github.com/uqfoundation/dill/issues/128
            try:
                result = dill.dumps(obj, byref = True)
            except Exception as e:
                exception = e

    if result is None:
        raise Exception("object %s is not serializable") from exception

    return  result


# ------------------------------------------------------------------------------
#
def serialize_file(obj):
    '''
    serialize object to file
    '''

    # FIXME: assign unique path and id for the pickled file
    #        to avoid overwriting to the same file

    result    = None
    exception = None

    kwargs = {}
    if not callable(obj):
        kwargs['recurse'] = True

    try:
        with open(_obj_file_path, 'wb') as f:
            dill.dump(obj, f, **kwargs)

    except Exception as e:
        exception = e

    else:
        result = _obj_file_path

    if result is None:
        raise Exception('object is not serializable') from exception

    return _obj_file_path


# ------------------------------------------------------------------------------
#
def deserialize_file(fname):
    '''
    Deserialize object from file
    '''
    result = None

    # check if we have a valid file
    if not os.path.isfile(fname):
        return None

    try:
        with open(fname, 'rb') as f:
            result = dill.load(f)
    except Exception as e:
        raise Exception ("failed to deserialize object from file") from e

    if result is None:
        raise RuntimeError('failed to deserialize')
    return result


# ------------------------------------------------------------------------------
#
def deserialize_obj(obj):
    '''
    Deserialize object from str.

    Raises
    ------
    TypeError
        if object cannot be deserialized.
    ValueError
        if no result is found.

    '''
    result = None

    try:
        result = dill.loads(obj)
    except Exception as e:
        raise TypeError("failed to deserialize from object") from e

    # Ref https://github.com/radical-cybertools/radical.pilot/pull/ \
    #                        2615#discussion_r870356482
    assert result is not None
    return result


# ------------------------------------------------------------------------------
#
def serialize_bson(obj):

    result = codecs.encode(pickle.dumps(obj), "base64").decode()

    return result


# ------------------------------------------------------------------------------
#
def deserialize_bson(obj):

    result = pickle.loads(codecs.decode(obj.encode(), "base64"))

    return result


# ------------------------------------------------------------------------------

