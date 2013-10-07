

import saga
import datetime

import sinon.api       as sa
import sinon.utils     as su
import sinon
from   attributes import *
from   constants  import *


# ------------------------------------------------------------------------------
#
class PilotManager (Attributes, sa.PilotManager) :

    # --------------------------------------------------------------------------
    #
    def __init__ (self, pmid=None, session=None) : 

        # initialize session
        self._sid, self._root = sinon.initialize ()

        # get a unique ID if none was given -- otherwise we reconnect
        if  not pmid :
            self.pmid = su.generate_pilot_manager_id ()
        else :
            self.pmid = str(pmid)

        # initialize attributes
        Attributes.__init__ (self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # deep inspection
        self._attributes_register  ('pmid', self.pmid, STRING, SCALAR, READONLY)
        self._attributes_register  (PILOTS,   [], STRING, VECTOR, READONLY)
        # ...


        # register state
        self._base         = self._root.open_dir (self.pmid, flags=saga.advert.CREATE_PARENTS)
        self._base.set_attribute ('pilots', [])



    # --------------------------------------------------------------------------
    #
    def submit_pilot (self, description, async=False) :

        # FIXME: bulk, async

        pilot = sinon.Pilot.create (description, self)
        
        pilots = self._base.get_attribute ('pilots')
        print 'pilots: %s (%s)' % (pilots, type (pilots))

        self._base.set_attribute ('pilots', [pilot.pid])

        pass


    # --------------------------------------------------------------------------
    #
    def list_pilots (self, async=False) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def get_pilot (self, pids, async=False) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def wait_pilot (self, pids, state=[DONE, FAILED, CANCELED], timeout=-1.0, async=False) :

        if  not isinstance (state, list) :
            state = [state]

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def cancel_pilot (self, pids, async=False) :

        # FIXME
        pass



# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

