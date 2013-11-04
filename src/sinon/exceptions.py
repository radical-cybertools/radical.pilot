

import saga.exceptions as se


# ------------------------------------------------------------------------------
#
class SinonException (se.SagaException) :

    def __init__ (self, msg, obj=None) :

        se.SagaException.__init__ (self, msg, obj)

