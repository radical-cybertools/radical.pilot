

from   constants import *


# ------------------------------------------------------------------------------
#
class SinonException(Exception) :
    """
    :param msg: Error message, indicating the cause for the exception
                being raised.
    :type  msg: string
    :param obj: Sinon object on whose activity the exception was raised.
    :type  obj: object
    :raises:    --

    The base class for all  Sinon Exception classes -- this exception type is
    never raised directly, but can be used to catch all Sinon exceptions within
    a single `except` clause.

    The exception message and originating object are also accessable as class
    attributes (:func:`e.object` and :func:`e.message`).  The :func:`__str__`
    operator redirects to :func:`get_message()`.  
    """


    # --------------------------------------------------------------------------
    #
    def __init__ (self, msg, obj=None) :
        """Le constructeur. Creates a new exception object. 
        """
        Exception.__init__(self, msg)
        self._obj = obj


    # --------------------------------------------------------------------------
    #
    def get_object (self) :
        """
        Return the object instance on whose activity the exception was raised.
        """
        return self._obj


    # --------------------------------------------------------------------------
    #
    def get_message (self) :
        """
        Return the error message associated with the exception
        """
        return self.message


    # --------------------------------------------------------------------------
    #
    def __str__ (self) :
        """
        an alias for `get_message()`
        """
        return self.get_message()


# ------------------------------------------------------------------------------
#
class IncorrectURL (SinonException) :
    """
    :param msg: Error message, indicating the cause for the exception
                being raised.
    :type  msg: string
    :raises:    --

    The given URL could not be interpreted, for example due to an incorrect
    / unknown schema. 
    """
    def __init__ (self, msg, obj=None) :
        SinonException.__init__(self, obj)


# ------------------------------------------------------------------------------
#
class BadParameter (SinonException) :
    """
    :param msg: Error message, indicating the cause for the exception
                being raised.
    :type  msg: string
    :raises:    --

    A given parameter is out of bound or ill formatted.
    """
    def __init__ (self, msg, obj=None) :
        SinonException.__init__(self, obj)


# ------------------------------------------------------------------------------
#
class DoesNotExist (SinonException) :
    """
    :param msg: Error message, indicating the cause for the exception
                being raised.
    :type  msg: string
    :raises:    --

    An operation tried to access a non-existing entity.
    """
    def __init__ (self, msg, obj=None) :
        SinonException.__init__(self, obj)


# ------------------------------------------------------------------------------
#
class IncorrectState (SinonException) :
    """
    :param msg: Error message, indicating the cause for the exception
                being raised.
    :type  msg: string
    :raises:    --

    The operation is not allowed on the entity in its current state.
    """
    def __init__ (self, msg, obj=None) :
        SinonException.__init__(self, obj)


# ------------------------------------------------------------------------------
#
class PermissionDenied (SinonException) :
    """
    :param msg: Error message, indicating the cause for the exception
                being raised.
    :type  msg: string
    :raises:    --

    The used identity is not permitted to perform the requested operation.
    """
    def __init__ (self, msg, obj=None) :
        SinonException.__init__(self, obj)


# ------------------------------------------------------------------------------
#
class AuthorizationFailed (SinonException) :
    """
    :param msg: Error message, indicating the cause for the exception
                being raised.
    :type  msg: string
    :raises:    --

    The backend could not accept the given identity.
    """
    def __init__ (self, msg, obj=None) :
        SinonException.__init__(self, obj)


# ------------------------------------------------------------------------------
#
class AuthenticationFailed (SinonException) :
    """
    :param msg: Error message, indicating the cause for the exception
                being raised.
    :type  msg: string
    :raises:    --

    The backend could not establish a valid identity.
    """
    def __init__ (self, msg, obj=None) :
        SinonException.__init__(self, obj)


# ------------------------------------------------------------------------------
#
class Timeout (SinonException) :
    """
    :param msg: Error message, indicating the cause for the exception
                being raised.
    :type  msg: string
    :raises:    --

    The interaction with the backend timed out.
    """
    def __init__ (self, msg, obj=None) :
        SinonException.__init__(self, obj)


# ------------------------------------------------------------------------------
#
class NoSuccess (SinonException) :
    """
    :param msg: Error message, indicating the cause for the exception
                being raised.
    :type  msg: string
    :raises:    --

    Some unsopecified error occurred.
    """
    def __init__ (self, msg, obj=None) :
        SinonException.__init__(self, obj)
