

import saga

import radical.utils   as ru

import sinon._api      as sa
import attributes      as att
import pilot           as p


# ------------------------------------------------------------------------------
#
class PilotManager (att.Attributes, sa.PilotManager) :

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
        att.Attributes.__init__ (self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # deep inspection
        self._attributes_register  ('pmid',    self.pmid, att.STRING, att.SCALAR, att.READONLY)
        self._attributes_register  (sa.PILOTS, [],        att.STRING, att.VECTOR, att.READONLY)
        # ...


    # --------------------------------------------------------------------------
    #
    def submit_pilot (self, description) :

        # FIXME: bulk

        pilot  = p.Pilot._create (description, self)

        pilots = self.pilots
        print 'pilots: %s (%s)' % (pilots, type (pilots))

        # FIXME: append
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
    def wait_pilot (self, pids, state=[sa.DONE, sa.FAILED, sa.CANCELED], timeout=-1.0) :

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

