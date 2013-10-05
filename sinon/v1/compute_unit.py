

import sinon.api     as sa
import unit          as u
from   constants import *


# ------------------------------------------------------------------------------
#
class ComputeUnit (u.Unit, sa.ComputeUnit) :
    """ 
    Base class for DataUnit and ComputeUnit.
    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self, uid, _description=None, _manager=None) : 

        u.Unit.__init__ (self, uid, _description, _manager)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

