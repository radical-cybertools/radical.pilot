

import sinon.api       as sa
import saga.exceptions as se


# ------------------------------------------------------------------------------
#
class SinonException (se.SagaException, sa.SinonException) :

    def __init__ (self, msg, api_object=None) :

        se.SagaException.__init__ (self, msg, api_object)



# ------------------------------------------------------------------------------
#
class IncorrectURL (SinonException, se.IncorrectURL) :

    def __init__ (self, msg, api_object=None) :

        SinonException.__init__ (self, msg, api_object)


# ------------------------------------------------------------------------------
#
class BadParameter (SinonException, se.BadParameter) :

    def __init__ (self, msg, api_object=None) :

        SinonException.__init__ (self, msg, api_object)


# ------------------------------------------------------------------------------
#
class DoesNotExist (SinonException, se.DoesNotExist) :

    def __init__ (self, msg, api_object=None) :

        SinonException.__init__ (self, msg, api_object)


# ------------------------------------------------------------------------------
#
class IncorrectState (SinonException, se.IncorrectState) :

    def __init__ (self, msg, api_object=None) :

        SinonException.__init__ (self, msg, api_object)


# ------------------------------------------------------------------------------
#
class PermissionDenied (SinonException, se.PermissionDenied) :

    def __init__ (self, msg, api_object=None) :

        SinonException.__init__ (self, msg, api_object)


# ------------------------------------------------------------------------------
#
class AuthorizationFailed (SinonException, se.AuthorizationFailed) :

    def __init__ (self, msg, api_object=None) :

        SinonException.__init__ (self, msg, api_object)


# ------------------------------------------------------------------------------
#
class AuthenticationFailed (SinonException, se.AuthenticationFailed) :

    def __init__ (self, msg, api_object=None) :

        SinonException.__init__ (self, msg, api_object)


# ------------------------------------------------------------------------------
#
class Timeout (SinonException, se.Timeout) :

    def __init__ (self, msg, api_object=None) :

        SinonException.__init__ (self, msg, api_object)


# ------------------------------------------------------------------------------
#
class NoSuccess (SinonException, se.NoSuccess) :

    def __init__ (self, msg, api_object=None) :

        SinonException.__init__ (self, msg, api_object)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

