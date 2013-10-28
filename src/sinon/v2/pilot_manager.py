

import os

import sinon.v1.pilot           as p
import sinon.v1.session         as s
import sinon.v1.attributes      as att
import sinon._api      as sa

import uuid
import sinon._api as sa
from sinon.db import Session as dbSession



# ------------------------------------------------------------------------------
#
class PilotManager (sa.PilotManager) :

    # --------------------------------------------------------------------------
    #
    def __init__ (self, pmid=None, session=None) : 

        if pmid is None:
            # Create a new pilot manager.
            self._pmid = str(uuid.uuid4())
            session._dbs.insert_pilot_manager(self._pmid)
        else:
            # reconnect to an existing PM
            self._pmid = pmid

    #---------------------------------------------------------------------------
    #
    @property
    def pmid(self):
        """ Returns the pilot manager id.
        """
        return self._pmid


    # --------------------------------------------------------------------------
    #
    def submit_pilot (self, description) :

        # FIXME: bulk

        if  not sa.RESOURCE in description :
            raise ValueError ("no RESOURCE specified in pilot description")

        # replace resource with more complete spec, if so configured 
        if  description[sa.RESOURCE] in self._resource_cfg :
            description[sa.RESOURCE] = self._resource_cfg[description[sa.RESOURCE]]

        print description


        # hand off pilot creation to the pilot class
        pilot  = p.Pilot._create (description, self)

        # keep pilot around for inspection
        self.pilots.append (pilot.pid)

        return pilot


    # --------------------------------------------------------------------------
    #
    def list_pilots (self) :

        return self.pilots


    # --------------------------------------------------------------------------
    #
    def get_pilot (self, pids) :

        if  not isinstance (pids, list) :
            if  not pids in self.pilots :
                raise LookupError ("pilot '%s' not found" % pids)

            # FIXME: switch by type
            return troy.ComputePilot (pids)

        # handle bulk
        else :
            ret = []

            for pid in pids :
                if  not pid in self.pilots :
                    raise LookupError ("pilot '%s' not found" % pid)

                # FIXME: switch by type
                ret.append (troy.ComputePilot (pid))

            return ret



    # --------------------------------------------------------------------------
    #
    def wait_pilot (self, pids, state=[sa.DONE, sa.FAILED, sa.CANCELED], timeout=-1.0) :

        if  not isinstance (state, list) :
            state = [state]


        if  not isinstance (pids, list) :
            if  not pids in self.pilots :
                raise LookupError ("pilot '%s' not found" % pids)

            troy.Pilot (pids).wait (state, timeout)

        # handle bulk
        else :
            # FIXME: better timeout handling
            for pid in pids :
                if  not pid in self.pilots :
                    raise LookupError ("pilot '%s' not found" % pid)

                troy.Pilot (pid).wait (state, timeout)


    # --------------------------------------------------------------------------
    #
    def cancel_pilot (self, pids) :

        if  not isinstance (pids, list) :
            if  not pids in self.pilots :
                raise LookupError ("pilot '%s' not found" % pids)

            troy.Pilot (pids).cancel ()

        # handle bulk
        else :
            for pid in pids :
                if  not pid in self.pilots :
                    raise LookupError ("pilot '%s' not found" % pid)

                troy.Pilot (pid).cancel ()


# ------------------------------------------------------------------------------
#


