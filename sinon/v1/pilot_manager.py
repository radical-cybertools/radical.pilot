

import sinon.api       as sa
from   attributes import *
from   constants  import *


# ------------------------------------------------------------------------------
#
class PilotManager (Attributes, sa.PilotManager) :

    # --------------------------------------------------------------------------
    #
    def __init__ (self, url=None, session=None) : 

        Attributes.__init__ (self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # deep inspection
        self._attributes_register  (PILOTS, [], STRING, VECTOR, READONLY)
        # ...



    # --------------------------------------------------------------------------
    #
    def submit_pilot (self, description, ttype=SYNC) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def list_pilots (self, ttype=SYNC) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def get_pilot (self, pids, ttype=SYNC) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def wait_pilot (self, pids, state=[DONE, FAILED, CANCELED], timeout=-1.0, ttype=SYNC) :

        if  not isinstance (state, list) :
            state = [state]

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def cancel_pilot (self, pids, ttype=SYNC) :

        # FIXME
        pass



# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

