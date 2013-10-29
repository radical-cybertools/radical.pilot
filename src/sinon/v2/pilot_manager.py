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
            self._pmid = self._DB.insert_pilot_manager(pilot_manager_data={})
        else:
            # reconnect to an existing PM
            if pilot_manager_id not in self._DB.list_pilot_manager_ids():
                raise LookupError ("Pilot Manager '%s' not in database." \
                    % pilot_manager_id)
            self._pmid = pilot_manager_id

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
    def get_pilots(self, pilot_ids=None):
        """ Implementation of interface.PilotManager.get_pilot().
        """
        # implicit list conversion
        if (pilot_ids is not None) and (type(pilot_ids) is not list):
            pilot_id_list = list()
            pilot_id_list.append(pilot_ids)
        else:
            pilot_id_list = pilot_ids

        pilots = Pilot._get(pilot_ids=pilot_id_list, pilot_manager_obj=self)
        return pilots

    # --------------------------------------------------------------------------
    #
    def wait_pilot (self, pids, state=[interface.DONE, interface.FAILED, interface.CANCELED], timeout=-1.0) :
        """ Docstring 
        """
        pass

    # --------------------------------------------------------------------------
    #
    def cancel_pilot (self, pids) :
        """ Docstring
        """
        pass


