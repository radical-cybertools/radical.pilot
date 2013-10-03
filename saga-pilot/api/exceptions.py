

import saga.exceptions as se


# ------------------------------------------------------------------------------
#
class SAGAPilotException (se.SAGAException) :
    """
    The given URL could not be interpreted, for example due to an incorrect
    / unknown schema. 
    """
    pass


# ------------------------------------------------------------------------------
#
class IncorrectURL (SAGAPilotException, se.IncorrectUrl) :
    """
    The given URL could not be interpreted, for example due to an incorrect
    / unknown schema. 
    """
    pass


# ------------------------------------------------------------------------------
#
class BadParameter (SAGAPilotException, se.BadParameter) :
    """
    A given parameter is out of bound or ill formatted.
    """
    pass


# ------------------------------------------------------------------------------
#
class DoesNotExist (SAGAPilotException, se.DoesNotExist) :
    """
    An operation tried to access a non-existing entity.
    """
    pass


# ------------------------------------------------------------------------------
#
class IncorrectState (SAGAPilotException, se.IncorrectState) :
    """
    The operation is not allowed on the entity in its current state.
    """
    pass


# ------------------------------------------------------------------------------
#
class PermissionDenied (SAGAPilotException, se.PermissionDenied) :
    """
    The used identity is not permitted to perform the requested operation.
    """
    pass


# ------------------------------------------------------------------------------
#
class AuthorizationFailed (SAGAPilotException, se.AuthorizationFailed) :
    """
    The backend could not establish a valid identity.
    """
    pass


# ------------------------------------------------------------------------------
#
class AuthenticationFailed (SAGAPilotException, se.AuthenticationFailed) :
    """
    The backend could not establish a valid identity.
    """
    pass


# ------------------------------------------------------------------------------
#
class Timeout (SAGAPilotException, se.Timeout) :
    """
    The interaction with the backend times out.
    """
    pass


# ------------------------------------------------------------------------------
#
class NoSuccess (SAGAPilotException, se.NoSuccess) :
    """
    Some other error occurred.
    """
    pass


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

