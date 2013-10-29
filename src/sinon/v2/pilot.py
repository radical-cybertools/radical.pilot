

import threading
import time
import radical.utils   as ru

import session         as s
import exceptions      as e
#import attributes      as att
#import pilot_manager   as pm
import sinon._api      as sa


# ------------------------------------------------------------------------------
#
class Pilot (sa.Pilot) :

    # --------------------------------------------------------------------------
    #
    def __init__ (self):
        """ Le constructeur. Not meant to be called directly.
        """
        self._pid = None

        self._description = None
        self._manager     = None

    # --------------------------------------------------------------------------
    #
    @property 
    def id(self):
        """ Implements the interface.Pilot.id property.
        """
        if not self._pid:
            raise Exception("ARGH")
        return self._pid

    # --------------------------------------------------------------------------
    #
    @property 
    def description(self):
        """ Implements the interface.Pilot.description property.
        """
        if not self._pid:
            raise Exception("ARGH")
        return self._description

    # --------------------------------------------------------------------------
    #
    @staticmethod 
    def _create (pilot_manager_obj, pilot_description) :
        """ Create a new pilot.
        """
        # create database entry
        pilot_id = pilot_manager_obj._session._dbs.insert_pilot(pilot_manager_id=pilot_manager_obj.pmid, 
                                                                pilot_description=pilot_description)
        # create and return pilot object
        pilot = Pilot()
        pilot._pid = pilot_id

        pilot._description = pilot_description
        pilot._manager     = pilot_manager_obj

        return pilot

    # --------------------------------------------------------------------------
    #
    @staticmethod 
    def _get (pilot_manager_obj, pilot_id) :
        """ Get a pilot via its ID.
        """
        # create database entry
        pilot_json = pilot_manager_obj._session._dbs.get_pilot(pilot_manager_id=pilot_manager_obj.pmid, 
                                                               pilot_id=pilot_id)
        # check if we have a result
        if len(pilot_json) < 1:
            raise Exception("Pilot with ID %s doesn't exist: %s" % (pilot_id, pilot_json))
        pilot_json = pilot_json[0]

        # create and return pilot object
        pilot = Pilot()
        pilot._pid = pilot_id

        pilot._description = pilot_json['description']
        pilot._manager     = pilot_manager_obj

        return pilot

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
        if not self._pid:
            raise Exception("ARGH")


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


