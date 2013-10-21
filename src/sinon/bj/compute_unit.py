

import unit
import sinon.api  as sa


# ------------------------------------------------------------------------------
#
class ComputeUnit (unit.Unit, sa.ComputeUnit) :
    """ 
    Base class for DataUnit and ComputeUnit.
    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self, uid, _description=None, _manager=None, _pid=None) : 

        unit.Unit.__init__ (self, uid, _description, _manager, _pid)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

