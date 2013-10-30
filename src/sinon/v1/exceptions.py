

import saga.exceptions as se
import sinon._api      as sa


# ------------------------------------------------------------------------------
#
class SinonException (se.SagaException, sa.SinonException) :

    def __init__ (self, msg, obj=None) :

        se.SagaException.__init__ (self, msg, obj)

