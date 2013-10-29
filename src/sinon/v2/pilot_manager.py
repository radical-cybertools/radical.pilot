

import sinon._api as interface

from session      import Session
from pilot        import Pilot

import uuid
from sinon.db import Session as dbSession



# ------------------------------------------------------------------------------
#
class PilotManager (interface.PilotManager) :

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
    def submit_pilot(self, pilot_description):
        """ Implementation of interface.PilotManager.submit_pilot().
        """
        # hand off pilot creation to the pilot class
        pilot = Pilot._create(pilot_description=pilot_description, 
                              pilot_manager_obj=self)
        return pilot

    # --------------------------------------------------------------------------
    #
    def list_pilots(self):
        """ Implementation of interface.PilotManager.list_pilots().
        """
        return self._session._dbs.list_pilot_ids(self._pmid)

    # --------------------------------------------------------------------------
    #
    def get_pilot(self, pilot_id):
        """ Implementation of interface.PilotManager.get_pilot().
        """
        pilot = Pilot._get(pilot_id=pilot_id,
                           pilot_manager_obj=self)
        return pilot

    # --------------------------------------------------------------------------
    #
    def wait_pilot (self, pids, state=[interface.DONE, interface.FAILED, interface.CANCELED], timeout=-1.0) :

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


