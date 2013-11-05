"""
.. module:: sinon.pilot_manager
   :platform: Unix
   :synopsis: Implementation of the PilotManager class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013, RADICAL Group at Rutgers University"
__license__   = "MIT"

import _api as interface

from session      import Session
from exceptions   import SinonException
from pilot        import Pilot

from sinon.db import Session as dbSession


# ------------------------------------------------------------------------------
#
class PilotManager(object):
    """A PilotManager holds :class:`sinon.Pilot` instances that are 
    submitted via the :meth:`sinon.PilotManager.submit_pilots` method.
    
    It is possible to attach one or more :ref:`chapter_machconf` 
    to a PilotManager to outsource machine specific configuration 
    parameters to an external configuration file. 

    Each PilotManager has a unique identifier :data:`sinon.PilotManager.uid`
    that can be used to re-connect to previoulsy created PilotManager in a
    given :class:`sinon.Session`.

    **Example**::

        s = sinon.Session(database_url=DBURL)
        
        pm1 = sinon.PilotManager(session=s, resource_configurations=RESCONF)
        # Re-connect via the 'get()' method.
        pm2 = sinon.PilotManager.get(session=s, pilot_manager_uid=pm1.uid)

        # pm1 and pm2 are pointing to the same PilotManager
        assert pm1.uid == pm2.uid
    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self, session, resource_configurations): 
        """Creates a new PilotManager and attaches is to the session. 

        .. note:: The `resource_configurations` (see :ref:`chapter_machconf`)
                  parameter is currently mandatory for creating a new PilotManager
                  instance. 

        **Arguments:**

            * **session** (:class:`sinon.Session`): The session instance to use.

            * **resource_configurations** (`string` or `list` of `strings`): 
              A list of URLs pointing to :ref:`chapter_machconf`. Currently 
              `file://`, `http://` and `https://` URLs are supported.
              
              If one or more resource_configurations are provided, Pilots submitted 
              via this PilotManager can access the configuration entries in the 
              files via the :class:`ComputePilotDescription`. For example::

                  pm = sinon.PilotManager(session=s, resource_configurations="https://raw.github.com/saga-project/saga-pilot/master/configs/futuregrid.json")

                  pd = sinon.ComputePilotDescription()
                  pd.resource = "futuregrid.INDIA"  # Key defined in futuregrid.json
                  pd.cores = 16

                  pilot_india = pm.submit_pilots(pd)

        **Raises:**
            * :class:`sinon.SinonException`
        """
        self._DB = session._dbs
        self._session = session

        if resource_configurations == "~RECON~":
            # When we get the "~RECON~" keyword as resource_configurations, we were called 
            # from the 'get()' class method
            pass
        else:
            # Create a new pilot manager. We need at least one configuration file for that:
            self._uid = self._DB.insert_pilot_manager(pilot_manager_data={})
            self._mcfgs = resource_configurations

    # --------------------------------------------------------------------------
    #
    @classmethod 
    def get(cls, session, pilot_manager_uid) :
        """ Re-connects to an existing PilotManager via its uid.

        **Arguments:**

            * **session** (:class:`sinon.Session`): The session instance to use.

            * **pilot_manager_uid** (`string`): The unique identifier of the 
              PilotManager we want to re-connect to.

        **Raises:**
            * :class:`sinon.SinonException` if a PilotManager with 
              `pilot_manager_uid` doesn't exist in the database.
        """
        if pilot_manager_uid not in session._dbs.list_pilot_manager_uids():
            raise LookupError ("PilotManager '%s' not in database." \
                % pilot_manager_uid)

        obj = cls(session=session, resource_configurations="~RECON~")
        obj._uid = pilot_manager_uid

        return obj

    #---------------------------------------------------------------------------
    #
    @property
    def uid(self):
        """Returns the PilotManagers's unique identifier.

        The uid identifies the PilotManager within the :class:`sinon.Session` and 
        can be used to retrieve an existing PilotManager. 

        **Returns:**
            * A unique identifier (`string`).
        """
        return self._uid

    # --------------------------------------------------------------------------
    #
    def submit_pilots(self, pilot_descriptions):
        """Submits a new :class:`sinon.Pilot` to a resource. 

        **Returns:**
            * One or more :class:`sinon.Pilot` instances (`list` of :class:`sinon.Pilot`).

        **Raises:**
            * :class:`sinon.SinonException`
        """
        # implicit list conversion
        if type(pilot_descriptions) is not list:
            pilot_description_list = []
            pilot_description_list.append(pilot_descriptions)
        else:
            pilot_description_list = pilot_descriptions

        # create database entries
        pilot_uids = self._session._dbs.insert_pilots(pilot_manager_uid=self.uid, 
                                                     pilot_descriptions=pilot_description_list)

        assert len(pilot_uids) == len(pilot_description_list)

        # create the pilot objects
        pilots = []
        for i in range(0, len(pilot_description_list)):
            pilot = Pilot._create(pilot_uid=pilot_uids[i],
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
        """Lists the unique identifiers of all :class:`sinon.Pilot` instances 
        associated with this PilotManager

        **Returns:**
            * A list of :class:`sinon.Pilot` uids (`string`).

        **Raises:**
            * :class:`sinon.SinonException`
        """
        return self._session._dbs.list_pilot_uids(self._uid)

    # --------------------------------------------------------------------------
    #
    def get_pilots(self, pilot_uids=None):
        """Returns one or more :class:`sinon.Pilot` instances.

        **Arguments:**

            **pilot_uids** (list of strings): If pilot_uids is set, only the
            Pilots with  the specified uids are returned. If pilot_uids is
            None, all Pilots are returned.

        **Returns:**
            * A list of :class:`sinon.Pilot` objects (`list` of :class:`sinon.Pilot`).

        **Raises:**
            * :class:`sinon.SinonException`
        """
        # implicit list conversion
        if (pilot_uids is not None) and (type(pilot_uids) is not list):
            pilot_uid_list = []
            pilot_uid_list.append(pilot_uids)
        else:
            pilot_uid_list = pilot_uids

        pilots = Pilot._get(pilot_uids=pilot_uid_list, pilot_manager_obj=self)
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


