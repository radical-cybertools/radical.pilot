

from   constants import *


# ------------------------------------------------------------------------------
#
class SinonException (Exception) :
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
        """
        some more info
        """
        raise Exception ("%s is not implemented" % self.__class__.__name__)


    # --------------------------------------------------------------------------
    #
    def get_object (self) :
        """
        Return the object instance on whose activity the exception was raised.
        """
        raise Exception ("%s.get_object() is not implemented" % self.__class__.__name__)


    # --------------------------------------------------------------------------
    #
    def get_message (self) :
        """
        Return the error message associated with the exception
        """
        raise Exception ("%s.get_message() is not implemented" % self.__class__.__name__)


    # --------------------------------------------------------------------------
    #
    def __str__ (self) :
        """
        an alias for `get_message()`
        """
        raise Exception ("%s.__str__() is not implemented" % self.__class__.__name__)


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
        raise Exception ("%s is not implemented" % self.__class__.__name__)


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
        raise Exception ("%s is not implemented" % self.__class__.__name__)


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
        raise Exception ("%s is not implemented" % self.__class__.__name__)


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
        raise Exception ("%s is not implemented" % self.__class__.__name__)


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
        raise Exception ("%s is not implemented" % self.__class__.__name__)


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
        raise Exception ("%s is not implemented" % self.__class__.__name__)


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
        raise Exception ("%s is not implemented" % self.__class__.__name__)


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
        raise Exception ("%s is not implemented" % self.__class__.__name__)


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
        raise Exception ("%s is not implemented" % self.__class__.__name__)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

