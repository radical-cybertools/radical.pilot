

import radical.utils  as ru

import unit        as u
import sinon._api  as sa


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

        self._bj_unit = None


  
    # --------------------------------------------------------------------------
    #
    @classmethod
    def _register (cls, description, manager) :

        uid = ru.generate_id ('u.')

        return cls (uid, _description=description, _manager=manager)


    # --------------------------------------------------------------------------
    #
    def _submit (self, pilot) :

        print " @@@@@@@@@@@@@@@@@@ "
        print pilot._bj_pilot
        print type(pilot._bj_pilot)
        print " @@@@@@@@@@@@@@@@@@ "

        self._bj_unit = pilot._bj_pilot.submit_compute_unit (self.description.as_dict ())

        print " $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ "
        print type(self._bj_unit)
        print self._bj_unit
        print " $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ "



# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

