__copyright__ = "Copyright 2013, RADICAL Group @ Rutgers"
__license__   = "MIT"

import threading
import time
import radical.utils   as ru

import session         as s
import exceptions      as e
import sinon._api      as sa


# ------------------------------------------------------------------------------
#
class Pilot (sa.Pilot) :

    # --------------------------------------------------------------------------
    #
    def __init__ (self):
        """ Le constructeur. Not meant to be called directly.
        """
        self._uid = None

        self._description = None
        self._manager     = None

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
        if not self._uid:
            raise sinon.SinonException("Invalid Pilot instance.")

        return self._uid

    # --------------------------------------------------------------------------
    #
    @property 
    def description(self):
        """ Implements the interface.Pilot.description property.
        """
        if not self._uid:
            raise sinon.SinonException("Invalid Pilot instance.")

        return self._description

    # --------------------------------------------------------------------------
    #
    @staticmethod 
    def _create (pilot_manager_obj, pilot_uid, pilot_description) :
        """ Create a new pilot.
        """
        # create and return pilot object
        pilot = Pilot()
        pilot._uid = pilot_uid

        pilot._description = pilot_description
        pilot._manager     = pilot_manager_obj

        return pilot

    # --------------------------------------------------------------------------
    #
    @staticmethod 
    def _get (pilot_manager_obj, pilot_uids) :
        """ Get a pilot via its ID.
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
            pilots.append(pilot)

        return pilots

    # --------------------------------------------------------------------------
    #
    @classmethod
    def _connect (cls, pid, manager) :
        """
        """
        with self._rlock :

            pid = ru.generate_id ('p.')

            return cls (pid, _description=description, _manager=manager)


    # --------------------------------------------------------------------------
    #
    def wait (self, state=[sa.DONE, sa.FAILED, sa.CANCELED], timeout=None):
        """
        """
        if not self._uid:
            raise sinon.SinonException("Invalid Pilot instance.")

        with self._rlock :

            if  not isinstance (state, list) :
                state = [state]

                start_wait = time.time ()
                while self.state not in state :
                    print "%s waiting for %s (%s)" % (self.pid, state, self.state)
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

            if  self.state in [sa.DONE, sa.FAILED, sa.CANCELED] :
                # nothing to do
                return

            if  self.state in [sa.UNKNOWN] :
                raise e.IncorrectState ("Pilot state is UNKNOWN, cannot cancel")

            # FIXME
    

    # --------------------------------------------------------------------------
    #
    def _get_state (self) :

        return sa.UNKNOWN


# ------------------------------------------------------------------------------
#


