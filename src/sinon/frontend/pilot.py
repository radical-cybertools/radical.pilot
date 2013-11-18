"""
.. module:: sinon.pilot
   :platform: Unix
   :synopsis: Implementation of the Pilot class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013, radical.rutgers.edu"
__license__   = "MIT"

from sinon.constants  import *
from sinon.exceptions import SinonException

from sinon.frontend.attributes import *

import time
import threading

# ------------------------------------------------------------------------------
#
class Pilot (object) :

    # --------------------------------------------------------------------------
    #
    def __init__ (self):
        """ Le constructeur. Not meant to be called directly.
        """
        # non-changing members
        self._uid = None
        self._description = None
        self._manager = None

        # database handle
        self._db = None

    # --------------------------------------------------------------------------
    #
    @staticmethod 
    def _create (pilot_manager_obj, pilot_uid, pilot_description) :
        """ PRIVATE: Create a new pilot.
        """
        # create and return pilot object
        pilot = Pilot()

        pilot._uid = pilot_uid
        pilot._description = pilot_description
        pilot._manager     = pilot_manager_obj

        pilot._db = pilot._manager._session._dbs

        return pilot

    # --------------------------------------------------------------------------
    #
    @staticmethod 
    def _get (pilot_manager_obj, pilot_uids) :
        """ PRIVATE: Get one or more pilot via their UIDs.
        """
        # create database entry
        pilots_json = pilot_manager_obj._session._dbs.get_pilots(pilot_manager_uid=pilot_manager_obj.uid, 
                                                                 pilot_uids=pilot_uids)
        # create and return pilot objects
        pilots = []

        for p in pilots_json:
            pilot = Pilot()
            pilot._uid = str(p['_id'])
            pilot._description = p['description']
            pilot._manager = pilot_manager_obj

            pilot._db = pilot._manager._session._dbs
        
            pilots.append(pilot)

        return pilots

    # --------------------------------------------------------------------------
    #
    @property 
    def uid(self):
        """Returns the Pilot's unique identifier.

        The uid identifies the Pilot within the :class:`PilotManager` and 
        can be used to retrieve an existing Pilot.

        **Returns:**
            * A unique identifier (string).
        """
        # Check if this instance is valid
        if not self._uid:
            raise SinonException("Invalid Pilot instance.")

        # uid is static and doesn't change over the lifetime 
        # of a pilot, hence it can be stored in a member var.
        return self._uid

    # --------------------------------------------------------------------------
    #
    @property 
    def description(self):
        """ Returns the pilot description the pilot was started with.
        """
        # Check if this instance is valid
        if not self._uid:
            raise SinonException("Invalid Pilot instance.")

        # description is static and doesn't change over the lifetime 
        # of a pilot, hence it can be stored in a member var.
        return self._description

    # --------------------------------------------------------------------------
    #
    @property 
    def state(self):
        """ Returns the current state of the pilot.
        """
        # Check if this instance is valid
        if not self._uid:
            raise SinonException("Invalid Pilot instance.")

        # state is oviously dynamic and changes over the 
        # lifetime of a pilot, hence we need to make a call to the 
        # database layer (db layer might cache this call).
        pilots_json = self._db.get_pilots(pilot_manager_uid=self._manager.uid, 
                                          pilot_uids=[self.uid])
        return pilots_json[0]['info']['state']

    # --------------------------------------------------------------------------
    #
    def wait (self, state=[DONE, FAILED, CANCELED], timeout=None):
        """
        """
        # Check if this instance is valid
        if not self._uid:
            raise SinonException("Invalid Pilot instance.")

        if  not isinstance (state, list) :
            state = [state]

        start_wait = time.time ()
        while self.state not in state :
            print "%s waiting for %s (%s)" % (self.uid, state, self.state)
            time.sleep (1)

            if  (None != timeout) and (timeout <= (time.time () - start_wait)) :
                print "wait timeout"
                break

        # done waiting
        return


    # --------------------------------------------------------------------------
    #
    def cancel (self, drain=False) :
        if not self._uid:
            raise sinon.SinonException("Invalid Pilot instance.")

        with self._rlock :

            # FIXME drain

            if  self.state in [DONE, FAILED, CANCELED] :
                # nothing to do
                return

            if  self.state in [UNKNOWN] :
                raise e.IncorrectState ("Pilot state is UNKNOWN, cannot cancel")

            # FIXME
    

