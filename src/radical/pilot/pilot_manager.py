#pylint: disable=C0301, C0103, W0212

"""
.. module:: radical.pilot.pilot_manager
   :platform: Unix
   :synopsis: Provides the interface for the PilotManager class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import os
import time
import glob
import copy

from radical.pilot.states import *
from radical.pilot.exceptions import *

from radical.pilot.object import Object
from radical.pilot.controller import PilotManagerController
from radical.pilot.compute_pilot import ComputePilot
from radical.pilot.utils.logger import logger
from radical.pilot.exceptions import PilotException, BadParameter
from radical.pilot.resource_config import ResourceConfig

# -----------------------------------------------------------------------------
#
class PilotManager(Object):
    """A PilotManager holds :class:`radical.pilot.ComputePilot` instances that are
    submitted via the :func:`radical.pilot.PilotManager.submit_pilots` method.

    It is possible to attach one or more :ref:`chapter_machconf`
    to a PilotManager to outsource machine specific configuration
    parameters to an external configuration file.

    Each PilotManager has a unique identifier :data:`radical.pilot.PilotManager.uid`
    that can be used to re-connect to previoulsy created PilotManager in a
    given :class:`radical.pilot.Session`.

    **Example**::

        s = radical.pilot.Session(database_url=dbURL)

        pm1 = radical.pilot.PilotManager(session=s, resource_configurations=RESCONF)
        # Re-connect via the 'get()' method.
        pm2 = radical.pilot.PilotManager.get(session=s, pilot_manager_id=pm1.uid)

        # pm1 and pm2 are pointing to the same PilotManager
        assert pm1.uid == pm2.uid
    """

    # -------------------------------------------------------------------------
    #
    def __init__(self, session, pilot_launcher_workers=1, _reconnect=False):
        """Creates a new PilotManager and attaches is to the session.

        .. note:: The `resource_configurations` (see :ref:`chapter_machconf`)
                  parameter is currently mandatory for creating a new
                  PilotManager instance.

        **Arguments:**

            * **session** [:class:`radical.pilot.Session`]:
              The session instance to use.

            * **resource_configurations** [`string` or `list of strings`]:
              A list of URLs pointing to :ref:`chapter_machconf`. Currently
              `file://`, `http://` and `https://` URLs are supported.

              If one or more resource_configurations are provided, Pilots
              submitted  via this PilotManager can access the configuration
              entries in the  files via the :class:`ComputePilotDescription`.
              For example::

                  pm = radical.pilot.PilotManager(session=s)

                  pd = radical.pilot.ComputePilotDescription()
                  pd.resource = "futuregrid.india"  # defined in futuregrid.json
                  pd.cores    = 16
                  pd.runtime  = 5 # minutes

                  pilot = pm.submit_pilots(pd)

            * pilot_launcher_workers (`int`): The number of pilot launcher 
              worker processes to start in the background. 

        .. note:: `pilot_launcher_workers` can be used to tune RADICAL-Pilot's 
                  performance. However, you should only change the default values 
                  if you know what you are doing.

        **Returns:**

            * A new `PilotManager` object [:class:`radical.pilot.PilotManager`].

        **Raises:**
            * :class:`radical.pilot.PilotException`
        """
        self._session = session
        self._worker = None
        self._uid = None

        if _reconnect == True:
            return

        ###############################
        # Create a new pilot manager. #
        ###############################

        # Start a worker process fo this PilotManager instance. The worker
        # process encapsulates database access, persitency et al.
        self._worker = PilotManagerController(
            pilot_manager_uid=None,
            pilot_manager_data={},
            pilot_launcher_workers=pilot_launcher_workers, 
            session=self._session,
            db_connection=session._dbs,
            db_connection_info=session._connection_info)
        self._worker.start()

        self._uid = self._worker.pilot_manager_uid

        # Each pilot manager has a worker thread associated with it. The task
        # of the worker thread is to check and update the state of pilots, fire
        # callbacks and so on.
        self._session._pilot_manager_objects.append(self)
        self._session._process_registry.register(self._uid, self._worker)

    #--------------------------------------------------------------------------
    #
    def close(self, terminate=True):
        """Shuts down the PilotManager and its background workers in a 
        coordinated fashion.

        **Arguments:**

            * **terminate** [`bool`]: If set to True, all active pilots will 
              get canceled (default: False).

        """

        logger.debug("pmgr    %s closing" % (str(self._uid)))

        # Spit out a warning in case the object was already closed.
        if not self._uid:
            logger.error("PilotManager object already closed.")
            return

        # before we terminate pilots, we have to kill the pilot launcher threads
        # -- otherwise we'll run into continous race conditions due to the
        # ongoing state checks...
        if self._worker is not None:
            # Stop the worker process
            logger.debug("pmgr    %s cancel   worker %s" % (str(self._uid), self._worker.name))
            self._worker.cancel_launcher()
            logger.debug("pmgr    %s canceled worker %s" % (str(self._uid), self._worker.name))



        # If terminate is set, we cancel all pilots. 
        if  terminate :
            # cancel all pilots, make sure they are gone, and close the pilot
            # managers.
            for pilot in self.get_pilots () :
                logger.debug("pmgr    %s cancels  pilot  %s" % (str(self._uid), pilot._uid))
            self.cancel_pilots ()

          # FIXME:
          #
          # wait_pilots() will wait until all pilots picked up the sent cancel
          # signal and died.  However, that can take a loooong time.  For
          # example, if a pilot is in 'PENDING_ACTIVE' state, this will have to
          # wait until the pilot is bootstrapped, started, connected to the DB,
          # and shut down again.  Or, for a pilot which just got a shitload of
          # units, it will have to wait until the pilot started all those units
          # and then checks its command queue again.  Or, if the pilot job
          # already died, wait will block until the state checker kicks in and
          # declares the pilot as dead, which takes a couple of minutes.
          #
          # Solution would be to add a CANCELING state and to wait for that one,
          # too, which basically means to wait until the cancel signal has been
          # sent.  There is not much more to do at this point anyway.  This is at
          # the moment faked in the manager controler, which sets that state
          # after sending the cancel command.  This should be converted into
          # a proper state -- that would, btw, remove the need for a cancel
          # command in the first place, as the pilot can just pull its own state
          # instead, and cancel on CANCELING...
          #
          # self.wait_pilots ()
            wait_for_cancel = True
            all_pilots = self.get_pilots ()
            while wait_for_cancel :
                wait_for_cancel = False
                for pilot in all_pilots :
                    logger.debug("pmgr    %s wait for pilot  %s (%s)" % (str(self._uid), pilot._uid, pilot.state))
                    if  pilot.state not in [DONE, FAILED, CANCELED, CANCELING] :
                        time.sleep (1)
                        wait_for_cancel = True
                        break
            for pilot in self.get_pilots () :
                logger.debug("pmgr    %s canceled pilot  %s" % (str(self._uid), pilot._uid))


        logger.debug("pmgr    %s stops    worker %s" % (str(self._uid), self._worker.name))
        self._worker.stop()
        self._worker.join()
        logger.debug("pmgr    %s stopped  worker %s" % (str(self._uid), self._worker.name))

        # Remove worker from registry
        self._session._process_registry.remove(self._uid)


        logger.debug("pmgr    %s closed" % (str(self._uid)))
        self._uid = None

    #--------------------------------------------------------------------------
    #
    @classmethod
    def _reconnect(cls, session, pilot_manager_id):
        """PRIVATE: reconnect to an existing pilot manager.
        """
        uid_exists = PilotManagerController.uid_exists(
            db_connection=session._dbs,
            pilot_manager_uid=pilot_manager_id
        )

        if not uid_exists:
            raise BadParameter(
                "PilotManager with id '%s' not in database." % pilot_manager_id)

        obj = cls(session=session, _reconnect=True)
        obj._uid = pilot_manager_id

        # Retrieve or start a worker process fo this PilotManager instance.
        worker = session._process_registry.retrieve(pilot_manager_id)
        if worker is not None:
            obj._worker = worker
        else:
            obj._worker = PilotManagerController(
                pilot_manager_uid=pilot_manager_id,
                pilot_manager_data={},
                session=session,
                db_connection=session._dbs,
                db_connection_info=session._connection_info)
            session._process_registry.register(pilot_manager_id, obj._worker)

        # start the worker if it's not already running
        if obj._worker.is_alive() is False:
            obj._worker.start()

        return obj

    # -------------------------------------------------------------------------
    #
    def as_dict(self):
        """Returns a Python dictionary representation of the object.
        """
        self._assert_obj_is_valid()

        object_dict = {
            'uid': self.uid
        }
        return object_dict

    # -------------------------------------------------------------------------
    #
    def __str__(self):
        """Returns a string representation of the object.
        """
        return str(self.as_dict())

    # -------------------------------------------------------------------------
    #
    def submit_pilots(self, pilot_descriptions):
        """Submits a new :class:`radical.pilot.ComputePilot` to a resource.

        **Returns:**

            * One or more :class:`radical.pilot.ComputePilot` instances
              [`list of :class:`radical.pilot.ComputePilot`].

        **Raises:**

            * :class:`radical.pilot.PilotException`
        """
        # Check if the object instance is still valid.
        self._assert_obj_is_valid()

        # Implicit list conversion.
        return_list_type = True
        if  not isinstance(pilot_descriptions, list):
            return_list_type   = False
            pilot_descriptions = [pilot_descriptions]

        # Itereate over the pilot descriptions, try to create a pilot for
        # each one and append it to 'pilot_obj_list'.
        pilot_obj_list = list()

        for pd in pilot_descriptions:

            if pd.resource is None:
                error_msg = "ComputePilotDescription does not define mandatory attribute 'resource'."
                raise BadParameter(error_msg)

            elif pd.runtime is None:
                error_msg = "ComputePilotDescription does not define mandatory attribute 'runtime'."
                raise BadParameter(error_msg)

            elif pd.cores is None:
                error_msg = "ComputePilotDescription does not define mandatory attribute 'cores'."
                raise BadParameter(error_msg)

            resource_key = pd.resource
            resource_cfg = self._session.get_resource_config(resource_key)

            # Check resource-specific mandatory attributes
            if "mandatory_args" in resource_cfg:
                for ma in resource_cfg["mandatory_args"]:
                    if getattr(pd, ma) is None:
                        error_msg = "ComputePilotDescription does not define attribute '{0}' which is required for '{1}'.".format(ma, resource_key)
                        raise BadParameter(error_msg)


            # we expand and exchange keys in the resource config, depending on
            # the selected schema so better use a deep copy...
            import copy
            resource_cfg  = copy.deepcopy (resource_cfg)
            schema        = pd['access_schema']

            if  not schema :
                if 'schemas' in resource_cfg :
                    schema = resource_cfg['schemas'][0]
              # import pprint
              # print "no schema, using %s" % schema
              # pprint.pprint (pd)

            if  not schema in resource_cfg :
              # import pprint
              # pprint.pprint (resource_cfg)
                logger.warning ("schema %s unknown for resource %s -- continue with defaults" \
                             % (schema, resource_key))

            else :
                for key in resource_cfg[schema] :
                    # merge schema specific resource keys into the
                    # resource config
                    resource_cfg[key] = resource_cfg[schema][key]

            # If 'default_sandbox' is defined, set it.
            if pd.sandbox is not None:
                if "valid_roots" in resource_cfg and resource_cfg["valid_roots"] is not None:
                    is_valid = False
                    for vr in resource_cfg["valid_roots"]:
                        if pd.sandbox.startswith(vr):
                            is_valid = True
                    if is_valid is False:
                        raise BadParameter("Working directory for resource '%s'" \
                               " defined as '%s' but needs to be rooted in %s " \
                                % (resource_key, pd.sandbox, resource_cfg["valid_roots"]))

            # After the sanity checks have passed, we can register a pilot
            # startup request with the worker process and create a facade
            # object.

            pilot = ComputePilot.create(
                pilot_description=pd,
                pilot_manager_obj=self)

            pilot_uid = self._worker.register_start_pilot_request(
                pilot=pilot,
                resource_config=resource_cfg)

            pilot._uid = pilot_uid

            pilot_obj_list.append(pilot)

            if self._session._rec:
                import radical.utils as ru
                ru.write_json(pd.as_dict(), "%s/%s.json" 
                        % (self._session._rec, pilot_uid))


        # Implicit return value conversion
        if  return_list_type :
            return pilot_obj_list
        else:
            return pilot_obj_list[0]

    # -------------------------------------------------------------------------
    #
    def list_pilots(self):
        """Lists the unique identifiers of all :class:`radical.pilot.ComputePilot`
        instances associated with this PilotManager

        **Returns:**

            * A list of :class:`radical.pilot.ComputePilot` uids [`string`].

        **Raises:**

            * :class:`radical.pilot.PilotException`
        """
        # Check if the object instance is still valid.
        self._assert_obj_is_valid()

        # Get the pilot list from the worker
        return self._worker.list_pilots()

    # -------------------------------------------------------------------------
    #
    def get_pilots(self, pilot_ids=None):
        """Returns one or more :class:`radical.pilot.ComputePilot` instances.

        **Arguments:**

            * **pilot_uids** [`list of strings`]: If pilot_uids is set,
              only the Pilots with  the specified uids are returned. If
              pilot_uids is `None`, all Pilots are returned.

        **Returns:**

            * A list of :class:`radical.pilot.ComputePilot` objects
              [`list of :class:`radical.pilot.ComputePilot`].

        **Raises:**

            * :class:`radical.pilot.PilotException`
        """
        self._assert_obj_is_valid()

        
        return_list_type = True
        if (not isinstance(pilot_ids, list)) and (pilot_ids is not None):
            return_list_type = False
            pilot_ids = [pilot_ids]

        pilots = ComputePilot._get(pilot_ids=pilot_ids, pilot_manager_obj=self)

        if  return_list_type :
            return pilots
        else :
            return pilots[0]

    # -------------------------------------------------------------------------
    #
    def wait_pilots(self, pilot_ids=None,
                    state=[DONE, FAILED, CANCELED],
                    timeout=None):
        """Returns when one or more :class:`radical.pilot.ComputePilots` reach a
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

              * :data:`radical.pilot.DONE`
              * :data:`radical.pilot.FAILED`
              * :data:`radical.pilot.CANCELED`

            * **timeout** [`float`]
              Optional timeout in seconds before the call returns regardless
              whether the Pilots have reached the desired state or not.
              The default value **-1.0** never times out.

        **Raises:**

            * :class:`radical.pilot.PilotException`
        """
        self._assert_obj_is_valid()

        if not isinstance(state, list):
            state = [state]

        return_list_type = True
        if (not isinstance(pilot_ids, list)) and (pilot_ids is not None):
            return_list_type = False
            pilot_ids = [pilot_ids]


        start  = time.time()
        all_ok = False
        states = list()

        while not all_ok :

            pilots = self._worker.get_compute_pilot_data(pilot_ids=pilot_ids)
            all_ok = True
            states = list()

            for pilot in pilots :
                if  pilot['state'] not in state :
                    all_ok = False

                states.append (pilot['state'])

            # check timeout
            if  (None != timeout) and (timeout <= (time.time() - start)):
                if  not all_ok :
                    logger.debug ("wait timed out: %s" % states)
                break

            # sleep a little if this cycle was idle
            if  not all_ok :
                time.sleep (0.1)

        # done waiting
        if  return_list_type :
            return states
        else :
            return states[0]


    # -------------------------------------------------------------------------
    #
    def cancel_pilots(self, pilot_ids=None):
        """Cancels one or more ComputePilots.

        **Arguments:**

            * **pilot_uids** [`string` or `list of strings`]
              If pilot_uids is set, only the Pilots with the specified uids are
              canceled. If pilot_uids is `None`, all Pilots are canceled.

        **Raises:**

            * :class:`radical.pilot.PilotException`
        """
        # Check if the object instance is still valid.
        self._assert_obj_is_valid()

        # Implicit list conversion.
        if (not isinstance(pilot_ids, list)) and (pilot_ids is not None):
            pilot_ids = [pilot_ids]

        # Register the cancelation request with the worker.
        self._worker.register_cancel_pilots_request(pilot_ids=pilot_ids)


    # -------------------------------------------------------------------------
    #
    def register_callback(self, callback_function, callback_data=None):
        """Registers a new callback function with the PilotManager.
        Manager-level callbacks get called if any of the ComputePilots managed
        by the PilotManager change their state.

        All callback functions need to have the same signature::

            def callback_func(obj, state, data)

        where ``object`` is a handle to the object that triggered the callback,
        ``state`` is the new state of that object, and ``data`` are the data
        passed on callback registration.
        """
        self._assert_obj_is_valid()

        self._worker.register_manager_callback(callback_function, callback_data)

