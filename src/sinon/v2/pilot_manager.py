__copyright__ = "Copyright 2013, RADICAL Group @ Rutgers"
__license__   = "MIT"

import sinon._api as interface

from session      import Session
from pilot        import Pilot

from sinon.db import Session as dbSession


# ------------------------------------------------------------------------------
#
class PilotManager(object):
    """ Docstring
    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self, pilot_manager_id=None, session=None): 
        """ Le constructeur.
        """
        self._DB = session._dbs
        self._session = session

        if pilot_manager_id is None:
            # Create a new pilot manager.
            self._uid = self._DB.insert_pilot_manager(pilot_manager_data={})
        else:
            # reconnect to an existing PM
            if pilot_manager_id not in self._DB.list_pilot_manager_ids():
                raise LookupError ("Pilot Manager '%s' not in database." \
                    % pilot_manager_id)
            self._uid = pilot_manager_id

    #---------------------------------------------------------------------------
    #
    @property
    def uid(self):
        """ Returns the pilot manager id.
        """
        return self._uid

    # --------------------------------------------------------------------------
    #
    def submit_pilots(self, pilot_descriptions):
        """ Implementation of interface.PilotManager.submit_pilots().
        """
        # implicit list conversion
        if type(pilot_descriptions) is not list:
            pilot_description_list = []
            pilot_description_list.append(pilot_descriptions)
        else:
            pilot_description_list = pilot_descriptions

        # create database entries
        pilot_ids = self._session._dbs.insert_pilots(pilot_manager_id=self.uid, 
                                                     pilot_descriptions=pilot_description_list)

        assert len(pilot_ids) == len(pilot_description_list)

        # create the pilot objects
        pilots = []
        for i in range(0, len(pilot_description_list)):
            pilot = Pilot._create(pilot_id=pilot_ids[i],
                                  pilot_description=pilot_description_list[i], 
                                  pilot_manager_obj=self)
            pilots.append(pilot)

        # implicit return value conversion
        if len(pilots) == 1:
            return pilots[0]
        else:
            return pilots

    # --------------------------------------------------------------------------
    #
    def list_pilots(self):
        """ Implementation of interface.PilotManager.list_pilots().
        """
        return self._session._dbs.list_pilot_ids(self._uid)

    # --------------------------------------------------------------------------
    #
    def get_pilots(self, pilot_ids=None):
        """ Implementation of interface.PilotManager.get_pilot().
        """
        # implicit list conversion
        if (pilot_ids is not None) and (type(pilot_ids) is not list):
            pilot_id_list = []
            pilot_id_list.append(pilot_ids)
        else:
            pilot_id_list = pilot_ids

        pilots = Pilot._get(pilot_ids=pilot_id_list, pilot_manager_obj=self)
        return pilots

    # --------------------------------------------------------------------------
    #
    def wait_pilots(self, pids, state=[interface.DONE, interface.FAILED, interface.CANCELED], timeout=-1.0) :
        """ Docstring 
        """
        pass

    # --------------------------------------------------------------------------
    #
    def cancel_pilots(self, pids):
        """ Docstring
        """
        pass


