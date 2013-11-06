"""
.. module:: sinon.pilot_manager
   :platform: Unix
   :synopsis: Implementation of the PilotManager class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013, RADICAL Group at Rutgers University"
__license__   = "MIT"

from session      import Session
from exceptions   import SinonException
from pilot        import Pilot

import constants

from sinon.db import Session as dbSession
from utils import as_list


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
                  parameter is currently mandatory for creating a new 
                  PilotManager instance. 

        **Arguments:**

            * **session** [:class:`sinon.Session`]: 
              The session instance to use.

            * **resource_configurations** [`string` or `list of strings`]: 
              A list of URLs pointing to :ref:`chapter_machconf`. Currently 
              `file://`, `http://` and `https://` URLs are supported.
              
              If one or more resource_configurations are provided, Pilots
              submitted  via this PilotManager can access the configuration
              entries in the  files via the :class:`ComputePilotDescription`.
              For example::

                  pm = sinon.PilotManager(session=s, resource_configurations="https://raw.github.com/saga-project/saga-pilot/master/configs/futuregrid.json")

                  pd = sinon.ComputePilotDescription()
                  pd.resource = "futuregrid.INDIA"  # defined in futuregrid.json
                  pd.cores = 16

                  pilot_india = pm.submit_pilots(pd)

        **Returns:**

            * A new `PilotManager` object [:class:`sinon.PilotManager`].

        **Raises:**
            * :class:`sinon.SinonException`
        """
        self._DB = session._dbs
        self._session = session

        if resource_configurations == "~=RECON=~":
            # When we get the "~=RECON=~" keyword as resource_configurations, we
            # were called  from the 'get()' class method
            pass
        else:
            # Create a new pilot manager. First we parse the configuration file(s)


            self._uid = self._DB.insert_pilot_manager(pilot_manager_data={})
            self._mcfgs = resource_configurations

    # --------------------------------------------------------------------------
    #
    @classmethod 
    def get(cls, session, pilot_manager_uid) :
        """ Re-connects to an existing PilotManager via its uid.

        **Arguments:**

            * **session** [:class:`sinon.Session`]: 
              The session instance to use.

            * **pilot_manager_uid** [`string`]: 
              The unique identifier of the PilotManager we want 
              to re-connect to.

        **Returns:**

            * A new `PilotManager` object [:class:`sinon.PilotManager`].

        **Raises:**

            * :class:`sinon.SinonException` if a PilotManager with 
              `pilot_manager_uid` doesn't exist in the database.
        """
        if pilot_manager_uid not in session._dbs.list_pilot_manager_uids():
            raise LookupError ("PilotManager '%s' not in database." \
                % pilot_manager_uid)

        obj = cls(session=session, resource_configurations="~=RECON=~")
        obj._uid = pilot_manager_uid

        return obj

    #---------------------------------------------------------------------------
    #
    @property
    def uid(self):
        """Returns the PilotManagers's unique identifier.

        The uid identifies the PilotManager within the :class:`sinon.Session`
        and can be used to retrieve an existing PilotManager.

        **Returns:**

            * A unique identifier [`string`].
        """
        return self._uid

    # --------------------------------------------------------------------------
    #
    def submit_pilots(self, pilot_descriptions):
        """Submits a new :class:`sinon.Pilot` to a resource. 

        **Returns:**

            * One or more :class:`sinon.Pilot` instances 
              [`list of :class:`sinon.Pilot`].

        **Raises:**

            * :class:`sinon.SinonException`
        """
        # implicit list conversion
        pilot_description_list = as_list(pilot_descriptions)

        # create database entries
        pilot_uids = self._session._dbs.insert_pilots(pilot_manager_uid=self.uid, 
            pilot_descriptions=pilot_description_list)

        # create the pilot objects
        pilots = []
        for i in range(0, len(pilot_description_list)):
            # check wether pilot description defines the mandatory 
            # fields 
            if pilot_description_list[i].resource is not None:
                raise SinonException("ComputePilotDescription.resource not defined")


            pilot = Pilot._create(pilot_uid=pilot_uids[i],
                                  pilot_description=pilot_description_list[i], 
                                  pilot_manager_obj=self)
            pilots.append(pilot)

        

        # basic sanity check
        assert len(pilot_uids) == len(pilot_description_list)

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

            * A list of :class:`sinon.Pilot` uids [`string`].

        **Raises:**

            * :class:`sinon.SinonException`
        """
        return self._session._dbs.list_pilot_uids(self._uid)

    # --------------------------------------------------------------------------
    #
    def get_pilots(self, pilot_uids=None):
        """Returns one or more :class:`sinon.Pilot` instances.

        **Arguments:**

            * **pilot_uids** [`list of strings`]: If pilot_uids is set, 
              only the Pilots with  the specified uids are returned. If 
              pilot_uids is `None`, all Pilots are returned.

        **Returns:**

            * A list of :class:`sinon.Pilot` objects 
              [`list of :class:`sinon.Pilot`].

        **Raises:**

            * :class:`sinon.SinonException`
        """
        # implicit list conversion
        pilot_uid_list = as_list(pilot_uids)

        pilots = Pilot._get(pilot_uids=pilot_uid_list, pilot_manager_obj=self)
        return pilots

    # --------------------------------------------------------------------------
    #
    def wait_pilots(self, pilot_uids=None, state=[constants.DONE, constants.FAILED, constants.CANCELED], timeout=-1.0):
        """Returns when one or more :class:`sinon.Pilots` reach a 
        specific state. 

        If `pilot_uids` is `None`, `wait_pilots` returns when **all** Pilots
        reach the state defined in `state`. 

        **Arguments:**

            * **pilot_uids** [`string` or `list of strings`] 
              If pilot_uids is set, only the Pilots with the specified uids are
              considered. If pilot_uids is `None` (default), all Pilots are 
              considered.

            * **state** [`string`]
              The state that Pilots have to reach in order for the call
              to return. 

              By default `wait_pilots` waits for the Pilots to reach 
              a terminal state, which can be one of the following:

              * :data:`sinon.DONE`
              * :data:`sinon.FAILED`
              * :data:`sinon.CANCELED`

            * **timeout** [`float`]
              Timeout in seconds before the call returns regardless of Pilot
              state changes. The default value **-1.0** waits forever.

        **Raises:**

            * :class:`sinon.SinonException`
        """
        pass

    # --------------------------------------------------------------------------
    #
    def cancel_pilots(self, pilot_uids=None):
        """Cancels one or more Pilots. 

        **Arguments:**

            * **pilot_uids** [`string` or `list of strings`] 
              If pilot_uids is set, only the Pilots with the specified uids are
              canceled. If pilot_uids is `None`, all Pilots are canceled.

        **Raises:**

            * :class:`sinon.SinonException`
        """
        pass


