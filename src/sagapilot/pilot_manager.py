"""
.. module:: sagapilot.pilot_manager
   :platform: Unix
   :synopsis: Implementation of the PilotManager class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

#pylint: disable=C0301, C0103
#pylint: disable=W0212

from sagapilot.compute_pilot import ComputePilot
from sagapilot.utils.logger  import logger

from sagapilot.mpworker import PilotManagerWorker

import sagapilot.states     as states
import sagapilot.attributes as attributes
import sagapilot.exceptions as exceptions

from bson.objectid import ObjectId
from radical.utils import which

import os
import saga
import time
import json
import urllib2
import datetime


# ------------------------------------------------------------------------------
# Attribute keys
UID  = 'UID'

# ------------------------------------------------------------------------------
#
class PilotManager(attributes.Attributes):
    """A PilotManager holds :class:`sagapilot.ComputePilot` instances that are 
    submitted via the :func:`sagapilot.PilotManager.submit_pilots` method.
    
    It is possible to attach one or more :ref:`chapter_machconf` 
    to a PilotManager to outsource machine specific configuration 
    parameters to an external configuration file. 

    Each PilotManager has a unique identifier :data:`sagapilot.PilotManager.uid`
    that can be used to re-connect to previoulsy created PilotManager in a
    given :class:`sagapilot.Session`.

    **Example**::

        s = sagapilot.Session(database_url=DBURL)
        
        pm1 = sagapilot.PilotManager(session=s, resource_configurations=RESCONF)
        # Re-connect via the 'get()' method.
        pm2 = sagapilot.PilotManager.get(session=s, pilot_manager_id=pm1.uid)

        # pm1 and pm2 are pointing to the same PilotManager
        assert pm1.uid == pm2.uid
    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self, session, resource_configurations=None): 
        """Creates a new PilotManager and attaches is to the session. 

        .. note:: The `resource_configurations` (see :ref:`chapter_machconf`)
                  parameter is currently mandatory for creating a new 
                  PilotManager instance. 

        **Arguments:**

            * **session** [:class:`sagapilot.Session`]: 
              The session instance to use.

            * **resource_configurations** [`string` or `list of strings`]: 
              A list of URLs pointing to :ref:`chapter_machconf`. Currently 
              `file://`, `http://` and `https://` URLs are supported.
              
              If one or more resource_configurations are provided, Pilots
              submitted  via this PilotManager can access the configuration
              entries in the  files via the :class:`ComputePilotDescription`.
              For example::

                  pm = sagapilot.PilotManager(session=s, resource_configurations="https://raw.github.com/saga-project/saga-pilot/master/configs/futuregrid.json")

                  pd = sagapilot.ComputePilotDescription()
                  pd.resource = "futuregrid.INDIA"  # defined in futuregrid.json
                  pd.cores = 16

                  pilot_india = pm.submit_pilots(pd)

        **Returns:**

            * A new `PilotManager` object [:class:`sagapilot.PilotManager`].

        **Raises:**
            * :class:`sagapilot.SagapilotException`
        """
        # Each pilot manager has a worker thread associated with it. The task of the 
        # worker thread is to check and update the state of pilots, fire callbacks
        # and so on. 

        self._DB = session._dbs
        self._session = session

        # initialize attributes
        attributes.Attributes.__init__(self)

        # set attribute interface properties
        self._attributes_extensible(False)
        self._attributes_camelcasing(True)

        # The UID attributes
        self._attributes_register(UID, None, attributes.STRING, attributes.SCALAR, attributes.READONLY)
        self._attributes_set_getter(UID, self._get_uid_priv)

        if resource_configurations == "~=RECON=~":
            # When we get the "~=RECON=~" keyword as resource_configurations, we
            # were called  from the 'get()' class method. In this case object 
            # instantiation happens there... 
            return

        ###############################
        # Create a new pilot manager. #
        ###############################

        # Donwload and parse the configuration file(s) and the content to 
        # our resource dictionary.
        self._resource_cfgs = {}

        # Add 'localhost' as a built-in resource configuration
        self._resource_cfgs["localhost"] = {
                "URL"                : "fork://localhost",
                "filesystem"         : "file://localhost",
                "pre_bootstrap"      : ["hostname", "date"],
                "task_launch_mode"   : "LOCAL"
        }
        
        if resource_configurations is not None:

            # implicit list conversion
            if not isinstance(resource_configurations, list):
                resource_configurations = [resource_configurations]
        
            for rcf in resource_configurations:
                try:
                    # download resource configuration file
                    response = urllib2.urlopen(rcf)
                    rcf_content = response.read()
                except urllib2.URLError, err:
                    msg = "Couln't open/download resource configuration file '%s': %s." % (rcf, str(err))
                    raise exceptions.BadParameter(msg=msg)

                try:
                    # convert JSON string to dictionary and append
                    rcf_dict = json.loads(rcf_content)
                    for key, val in rcf_dict.iteritems():
                        if key in self._resource_cfgs:
                            raise exceptions.BadParameter("Resource configuration entry for '%s' defined in %s is already defined." % (key, rcf))
                        self._resource_cfgs[key] = val
                except ValueError, err:
                    raise exceptions.BadParameter("Couldn't parse resource configuration file '%s': %s." % (rcf, str(err)))

        self._uid = self._DB.insert_pilot_manager(pilot_manager_data={})

        # Start a worker process fo this PilotManager instance. The worker 
        # process encapsulates database access et al.
        self._worker = PilotManagerWorker(logger=logger, pilotmanager_id=self._uid, db_connection=session._dbs)
        self._worker.start()

        # Register the worker with the session-wide process registry.
        self._session._process_registry.register(self._uid, self._worker)

    #---------------------------------------------------------------------------
    #
    def __del__(self):
        print "calling destructor"
        self._worker.stop()
        self._worker.join()

    #---------------------------------------------------------------------------
    #
    @classmethod
    def _reconnect(cls, session, pilot_manager_id):
        """PRIVATE: reconnect to an existing pilot manager.
        """

        if pilot_manager_id not in session._dbs.list_pilot_manager_uids():
            raise exceptions.BadParameter ("PilotManager with id '%s' not in database." % pilot_manager_id)

        obj = cls(session=session, resource_configurations="~=RECON=~")
        obj._uid           = pilot_manager_id
        obj._resource_cfgs = None # TODO: reconnect

        # Retrieve or start a worker process fo this PilotManager instance.
        worker = session._process_registry.retrieve(pilot_manager_id)
        if worker is not None:
            obj._worker = worker
        else:
            obj._worker = PilotManagerWorker(logger=logger, pilotmanager_id=pilot_manager_id, db_connection=session._dbs)
            session._process_registry.register(pilot_manager_id, obj._worker)

        # start the worker if it's not already running
        if worker.is_alive() is False:
            obj._worker.start()

        return obj

    #---------------------------------------------------------------------------
    #
    def _get_uid_priv(self):
        """Returns the PilotManagers's unique identifier.

        The uid identifies the PilotManager within the :class:`sagapilot.Session`
        and can be used to retrieve an existing PilotManager.

        **Returns:**

            * A unique identifier [`string`].
        """
        return self._uid

    # --------------------------------------------------------------------------
    #
    def as_dict(self):
        """Returns information about the pilot manager as a Python dictionary.
        """
        info_dict = {
            'uid'   : self._get_uid_priv()
        }
        return info_dict

    # --------------------------------------------------------------------------
    #
    def __str__(self):
        """Returns a string representation of the pilot manager.
        """
        return str(self.as_dict())

    # --------------------------------------------------------------------------
    #
    def submit_pilots(self, pilot_descriptions):
        """Submits a new :class:`sagapilot.ComputePilot` to a resource. 

        **Returns:**

            * One or more :class:`sagapilot.ComputePilot` instances 
              [`list of :class:`sagapilot.ComputePilot`].

        **Raises:**

            * :class:`sagapilot.SagapilotException`
        """
        # Check if the object instance is still valid.
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid object instance.")
        
        # Implicit list conversion.
        if not isinstance(pilot_descriptions, list):
            pilot_descriptions = [pilot_descriptions]

        # Create a dictionary representing the database entry. If any 
        # mandatory attributes are mussing we throw an exception.
        pilot_description_dict = {}

        for pilot_description in pilot_descriptions:

            if pilot_description.resource is None:
                error_msg = "ComputePilotDescription does not define mandatory attribute 'resource'."
                raise exceptions.BadParameter(error_msg)

            elif pilot_description.sandbox is None:
                error_msg = "ComputePilotDescription does not define mandatory attribute 'sandbox'."
                raise exceptions.BadParameter(error_msg)

            elif pilot_description.cores is None:
                error_msg = "ComputePilotDescription does not define mandatory attribute 'runtime'."
                raise exceptions.BadParameter(error_msg)

            elif pilot_description.cores is None:
                error_msg = "ComputePilotDescription does not define mandatory attribute 'cores'."
                raise exceptions.BadParameter(error_msg)

            # Make sure resource key is known.
            if pilot_description.resource not in self._resource_cfgs:
                error_msg = "ComputePilotDescription.resource key '%s' is not known by this PilotManager." % pilot_description.resource
            else:
                resource_cfg = self._resource_cfgs[pilot_description.resource]

            # Make sure sandbox is a valid path.
            if "valid_roots" in resource_cfg:
                is_valid = False
                for vr in resource_cfg["valid_roots"]:
                    if pilot_description.sandbox.startswith(vr):
                        is_valid = True
                if is_valid is False:
                    raise exceptions.BadParameter("Working directory for resource '%s' defined as '%s' but needs to be rooted in %s " % (pilot_description.resource, pilot_description.sandbox, resource_cfg["valid_roots"]))

            # Create the dictionary object.
            pilot_description_dict[ObjectId()] = {
                'description' : pilot_description.as_dict(),
                'resourcecfg' : resource_cfg,
                'session'     : self._session.as_dict(),
                'command'     : None, 
                'info'        : {'state'     : None, 
                                 'submitted' : None,
                                 'started'   : None,
                                 'finished'  : None,
                                 'log'       : []
                                }
            }

        # Register the startup request with the worker.
        self._worker.register_startup_pilots_request(pilot_descriptions=pilot_description_dict)

        # Create the pilot objects, launch the actual pilots via saga
        # and update the status accordingly.
        pilots = []
        for pilot_id, pilot_description in pilot_description_dict.iteritems():

            pilot = ComputePilot._create(
                pilot_id=str(pilot_id),
                pilot_description=pilot_description['description'], 
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
        """Lists the unique identifiers of all :class:`sagapilot.ComputePilot` 
        instances associated with this PilotManager

        **Returns:**

            * A list of :class:`sagapilot.ComputePilot` uids [`string`].

        **Raises:**

            * :class:`sagapilot.SagapilotException`
        """
        # Check if the object instance is still valid.
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid object instance.")

        # Get the pilot list from the worker
        return self._worker.list_pilots()

    # --------------------------------------------------------------------------
    #
    def get_pilots(self, pilot_ids=None):
        """Returns one or more :class:`sagapilot.ComputePilot` instances.

        **Arguments:**

            * **pilot_uids** [`list of strings`]: If pilot_uids is set, 
              only the Pilots with  the specified uids are returned. If 
              pilot_uids is `None`, all Pilots are returned.

        **Returns:**

            * A list of :class:`sagapilot.ComputePilot` objects 
              [`list of :class:`sagapilot.ComputePilot`].

        **Raises:**

            * :class:`sagapilot.SagapilotException`
        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid object instance.")

        if (not isinstance(pilot_ids, list)) and (pilot_ids is not None):
            pilot_ids = [pilot_ids]

        pilots = ComputePilot._get(pilot_ids=pilot_ids, pilot_manager_obj=self)
        return pilots

    # --------------------------------------------------------------------------
    #
    def wait_pilots(self, pilot_ids=None, 
        state=[states.DONE, states.FAILED, states.CANCELED], timeout=-1.0):
        """Returns when one or more :class:`sagapilot.ComputePilots` reach a 
        specific state or when an optional timeout is reached.

        If `pilot_uids` is `None`, `wait_pilots` returns when **all** Pilots
        reach the state defined in `state`. 

        **Arguments:**

            * **pilot_uids** [`string` or `list of strings`] 
              If pilot_uids is set, only the Pilots with the specified uids are
              considered. If pilot_uids is `None` (default), all Pilots are 
              considered.

            * **state** [`list of strings`]
              The state(s) that Pilots have to reach in order for the call
              to return. 

              By default `wait_pilots` waits for the Pilots to reach 
              a **terminal** state, which can be one of the following:

              * :data:`sagapilot.DONE`
              * :data:`sagapilot.FAILED`
              * :data:`sagapilot.CANCELED`

            * **timeout** [`float`]
              Optional timeout in seconds before the call returns regardless 
              whether the Pilots have reached the desired state or not. 
              The default value **-1.0** never times out.

        **Raises:**

            * :class:`sagapilot.SagapilotException`
        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid object instance.")

        if not isinstance (state, list):
            state = [state]

        if (not isinstance(pilot_ids, list)) and (pilot_ids is not None):
            pilot_ids = [pilot_ids]

        start_wait = time.time()
        all_done   = False
        return_states = []

        while all_done is False:

            all_done = True

            pilots_json = self._DB.get_pilots(pilot_manager_id=self.uid)
            for pilot in pilots_json:
                if pilot['info']['state'] not in state:
                    all_done = False
                    break # leave for loop
                else:
                    return_states.append(pilot['info']['state'])

            # check timeout
            if (None != timeout) and (timeout <= (time.time () - start_wait)):
                break

            # wait a bit
            time.sleep(1)

        # done waiting
        return return_states

    # --------------------------------------------------------------------------
    #
    def cancel_pilots(self, pilot_ids=None):
        """Cancels one or more ComputePilots. 

        **Arguments:**

            * **pilot_uids** [`string` or `list of strings`] 
              If pilot_uids is set, only the Pilots with the specified uids are
              canceled. If pilot_uids is `None`, all Pilots are canceled.

        **Raises:**

            * :class:`sagapilot.SagapilotException`
        """
        # Check if the object instance is still valid.
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid object instance.")

        # Implicit list conversion.
        if (not isinstance(pilot_ids, list)) and (pilot_ids is not None):
            pilot_ids = [pilot_ids]

        # Register the cancelation request with the worker.
        self._worker.register_cancel_pilots_request(pilot_ids=pilot_ids)
