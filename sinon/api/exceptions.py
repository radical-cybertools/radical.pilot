

from   constants import *


# ------------------------------------------------------------------------------
#
class SinonException (Exception) :
    """
    The given URL could not be interpreted, for example due to an incorrect
    / unknown schema. 
    """
    def __init__ (self) :
        raise Exception ("%s is not implemented" % self.__class__.__name__)


# ------------------------------------------------------------------------------
#
class IncorrectURL (Exception) :
    """
    The given URL could not be interpreted, for example due to an incorrect
    / unknown schema. 
    """
    def __init__ (self) :
        raise Exception ("%s is not implemented" % self.__class__.__name__)


# ------------------------------------------------------------------------------
#
class BadParameter (Exception) :
    """
    A given parameter is out of bound or ill formatted.
    """
    def __init__ (self) :
        raise Exception ("%s is not implemented" % self.__class__.__name__)


# ------------------------------------------------------------------------------
#
class DoesNotExist (Exception) :
    """
    An operation tried to access a non-existing entity.
    """
    def __init__ (self) :
        raise Exception ("%s is not implemented" % self.__class__.__name__)


# ------------------------------------------------------------------------------
#
class IncorrectState (Exception) :
    """
    The operation is not allowed on the entity in its current state.
    """
    def __init__ (self) :
        raise Exception ("%s is not implemented" % self.__class__.__name__)


# ------------------------------------------------------------------------------
#
class PermissionDenied (Exception) :
    """
    The used identity is not permitted to perform the requested operation.
    """
    def __init__ (self) :
        raise Exception ("%s is not implemented" % self.__class__.__name__)


# ------------------------------------------------------------------------------
#
class AuthorizationFailed (Exception) :
    """
    The backend could not establish a valid identity.
    """
    def __init__ (self) :
        raise Exception ("%s is not implemented" % self.__class__.__name__)


# ------------------------------------------------------------------------------
#
class AuthenticationFailed (Exception) :
    """
    The backend could not establish a valid identity.
    """
    def __init__ (self) :
        raise Exception ("%s is not implemented" % self.__class__.__name__)


# ------------------------------------------------------------------------------
#
class Timeout (Exception) :
    """
    The interaction with the backend times out.
    """
    def __init__ (self) :
        raise Exception ("%s is not implemented" % self.__class__.__name__)


# ------------------------------------------------------------------------------
#
class NoSuccess (Exception) :
    """
    Some other error occurred.
    """
    def __init__ (self) :
        raise Exception ("%s is not implemented" % self.__class__.__name__)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

