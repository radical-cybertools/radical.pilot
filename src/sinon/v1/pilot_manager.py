

import saga

import radical.utils   as ru

import sinon.api       as sa
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
        self._sid = sinon.initialize ()

        # get a unique ID if none was given -- otherwise we reconnect
        if  not pmid :
            self.pmid = ru.generate_id ('pm.')
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


    # --------------------------------------------------------------------------
    #
    def submit_pilot (self, description) :

        # FIXME: bulk

        pilot  = sinon.Pilot._create (description, self)

        pilots = self._base.get_attribute ('pilots')
        print 'pilots: %s (%s)' % (pilots, type (pilots))

        self._base.set_attribute ('pilots', [pilot.pid])

        return pilot


    # --------------------------------------------------------------------------
    #
    def list_pilots (self) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def get_pilot (self, pids) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def wait_pilot (self, pids, state=[DONE, FAILED, CANCELED], timeout=-1.0) :

        if  not isinstance (state, list) :
            state = [state]

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def cancel_pilot (self, pids) :

        # FIXME
        pass



# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

