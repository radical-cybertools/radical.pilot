

import radical.utils  as ru

import unit        as u
import sinon._api    as sa


# ------------------------------------------------------------------------------
#
class ComputeUnit (u.Unit, sa.ComputeUnit) :
    """ 
    Base class for DataUnit and ComputeUnit.
    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self, uid, _description=None, _manager=None, _pid=None) : 

        u.Unit.__init__ (self, uid, _description, _manager, _pid)

  
    # --------------------------------------------------------------------------
    #
    @classmethod
    def _register (cls, description, manager) :

        uid = ru.generate_id ('u.')

        return cls (uid, _description=description, _manager=manager)


    # --------------------------------------------------------------------------
    #
    def _submit (self, pilot) :

        # FIXME
        pass


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

