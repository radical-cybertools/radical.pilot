

import saga.exceptions as se
from   constants import *


# ------------------------------------------------------------------------------
#
class SinonException (se.SagaException) :
    """
    The given URL could not be interpreted, for example due to an incorrect
    / unknown schema. 
    """
    pass


# ------------------------------------------------------------------------------
#
class IncorrectURL (SinonException, se.IncorrectURL) :
    """
    The given URL could not be interpreted, for example due to an incorrect
    / unknown schema. 
    """
    pass


# ------------------------------------------------------------------------------
#
class BadParameter (SinonException, se.BadParameter) :
    """
    A given parameter is out of bound or ill formatted.
    """
    pass


# ------------------------------------------------------------------------------
#
class DoesNotExist (SinonException, se.DoesNotExist) :
    """
    An operation tried to access a non-existing entity.
    """
    pass


# ------------------------------------------------------------------------------
#
class IncorrectState (SinonException, se.IncorrectState) :
    """
    The operation is not allowed on the entity in its current state.
    """
    pass


# ------------------------------------------------------------------------------
#
class PermissionDenied (SinonException, se.PermissionDenied) :
    """
    The used identity is not permitted to perform the requested operation.
    """
    pass


# ------------------------------------------------------------------------------
#
class AuthorizationFailed (SinonException, se.AuthorizationFailed) :
    """
    The backend could not establish a valid identity.
    """
    pass


# ------------------------------------------------------------------------------
#
class AuthenticationFailed (SinonException, se.AuthenticationFailed) :
    """
    The backend could not establish a valid identity.
    """
    pass


# ------------------------------------------------------------------------------
#
class Timeout (SinonException, se.Timeout) :
    """
    The interaction with the backend times out.
    """
    pass


# ------------------------------------------------------------------------------
#
class NoSuccess (SinonException, se.NoSuccess) :
    """
    Some other error occurred.
    """
    pass


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

