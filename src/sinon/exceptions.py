

import saga.exceptions as se
import sinon._api      as sa


# ------------------------------------------------------------------------------
#
class SinonException (se.SagaException, sa.SinonException) :

    def __init__ (self, msg, obj=None) :

        se.SagaException.__init__ (self, msg, obj)



# ------------------------------------------------------------------------------
#
class IncorrectURL (SinonException, se.IncorrectURL) :

    def __init__ (self, msg, obj=None) :

        SinonException.__init__ (self, msg, obj)


# ------------------------------------------------------------------------------
#
class BadParameter (SinonException, se.BadParameter) :

    def __init__ (self, msg, obj=None) :

        SinonException.__init__ (self, msg, obj)


# ------------------------------------------------------------------------------
#
class DoesNotExist (SinonException, se.DoesNotExist) :

    def __init__ (self, msg, obj=None) :

        SinonException.__init__ (self, msg, obj)


# ------------------------------------------------------------------------------
#
class IncorrectState (SinonException, se.IncorrectState) :

    def __init__ (self, msg, obj=None) :

        SinonException.__init__ (self, msg, obj)


# ------------------------------------------------------------------------------
#
class PermissionDenied (SinonException, se.PermissionDenied) :

    def __init__ (self, msg, obj=None) :

        SinonException.__init__ (self, msg, obj)


# ------------------------------------------------------------------------------
#
class AuthorizationFailed (SinonException, se.AuthorizationFailed) :

    def __init__ (self, msg, obj=None) :

        SinonException.__init__ (self, msg, obj)


# ------------------------------------------------------------------------------
#
class AuthenticationFailed (SinonException, se.AuthenticationFailed) :

    def __init__ (self, msg, obj=None) :

        SinonException.__init__ (self, msg, obj)


# ------------------------------------------------------------------------------
#
class Timeout (SinonException, se.Timeout) :

    def __init__ (self, msg, obj=None) :

        SinonException.__init__ (self, msg, obj)


# ------------------------------------------------------------------------------
#
class NoSuccess (SinonException, se.NoSuccess) :

    def __init__ (self, msg, obj=None) :

        SinonException.__init__ (self, msg, obj)


# ------------------------------------------------------------------------------
#


