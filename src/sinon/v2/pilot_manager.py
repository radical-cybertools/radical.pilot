

import os

#import sinon.v2.pilot           as p
#import sinon.v2.session         as s
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
    def __init__ (self, pilot_manager_id=None, session=None) : 

        if pilot_manager_id is None:
            # Create a new pilot manager.
            self._pmid = session._dbs.insert_pilot_manager(pilot_manager_data={})
        else:
            # reconnect to an existing PM
            if pilot_manager_id not in session._dbs.list_pilot_manager_ids():
                raise LookupError ("Pilot Manager '%s' not in database." % pilot_manager_id)
            self._pmid = pilot_manager_id

        # The session hold the DB handle.
        self._session = session

    #---------------------------------------------------------------------------
    #
    @property
    def pmid(self):
        """ Returns the pilot manager id.
        """
        return self._pmid

    # --------------------------------------------------------------------------
    #
    def submit_pilot (self, pilot_description) :

 
        # hand off pilot creation to the pilot class
        #pilot = p.Pilot._create (description, self)

        # keep pilot around for inspection
        #self.pilots.append (pilot.pid)
        self._session._dbs.insert_pilot(self._pmid, pilot_description)

        #return pilot

    # --------------------------------------------------------------------------
    #
    def list_pilots (self) :
        return self._session._dbs.list_pilot_ids(self._pmid)

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


