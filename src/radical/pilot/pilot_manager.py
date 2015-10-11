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

import radical.utils as ru

from .states          import *
from .exceptions      import *
from .utils           import logger
from .controller      import PilotManagerController
from .compute_pilot   import ComputePilot
from .exceptions      import PilotException, BadParameter
from .resource_config import ResourceConfig

# -----------------------------------------------------------------------------
#
class PilotManager(object):
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
    def __init__(self, session, pilot_launcher_workers=1):
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
        logger.demo('info', 'create pilot manager')

        self._session = session
        self._worker = None

        self.uid = ru.generate_id ('pmgr')

        # ----------------------------------------------------------------------
        # Create a new pilot manager

        # Start a worker process fo this PilotManager instance. The worker
        # process encapsulates database access, persitency et al.
        self._worker = PilotManagerController(
            pmgr_uid=self.uid,
            pilot_manager_data={},
            pilot_launcher_workers=pilot_launcher_workers, 
            session=self._session)
        self._worker.start()


        # Each pilot manager has a worker thread associated with it. The task
        # of the worker thread is to check and update the state of pilots, fire
        # callbacks and so on.
        self._session._pilot_manager_objects[self.uid] = self

        self._valid = True

        logger.demo('ok', '\\ok\n')


    #---------------------------------------------------------------------------
    #
    def _is_valid(self):
        if not self._valid:
            raise RuntimeError("instance was closed")


    #--------------------------------------------------------------------------
    #
    def close(self, terminate=True):
        """Shuts down the PilotManager and its background workers in a 
        coordinated fashion.

        **Arguments:**

            * **terminate** [`bool`]: If set to True, all active pilots will 
              get canceled (default: False).

        """

        logger.debug("pmgr    %s closing" % (str(self.uid)))

        # Spit out a warning in case the object was already closed.
        if not self.uid:
            logger.error("PilotManager object already closed.")
            return

        # before we terminate pilots, we have to disable the pilot launcher, so
        # that no new pilots are getting started.  threads.  This will not stop
        # the state monitor, as we'll need that to have a functional pilot.wait.
        if self._worker is not None:
            logger.debug("pmgr    %s cancel   launcher %s" % (str(self.uid), self._worker.name))
            self._worker.disable_launcher()
            logger.debug("pmgr    %s canceled launcher %s" % (str(self.uid), self._worker.name))

        # If terminate is set, we cancel all pilots. 
        if  terminate :
            # cancel all pilots, make sure they are gone, and close the pilot
            # managers.
            for pilot in self.get_pilots () :
                logger.debug("pmgr    %s cancels  pilot  %s" % (str(self.uid), pilot.uid))
            self.cancel_pilots()
            self.wait_pilots()
            # we leave it to the worker shutdown below to ensure that pilots are
            # final before joining

        # not that all pilots are dead, we can terminate the launcher altogether
        # (incl. state checker)
        if self._worker is not None:
            logger.debug("pmgr    %s cancel   worker %s" % (str(self.uid), self._worker.name))
            self._worker.cancel_launcher()
            logger.debug("pmgr    %s canceled worker %s" % (str(self.uid), self._worker.name))

        logger.debug("pmgr    %s stops    worker %s" % (str(self.uid), self._worker.name))
        self._worker.stop()
        self._worker.join()
        logger.debug("pmgr    %s stopped  worker %s" % (str(self.uid), self._worker.name))
        logger.debug("pmgr    %s closed" % (str(self.uid)))

        self._valid = False


    # -------------------------------------------------------------------------
    #
    def as_dict(self):
        """Returns a Python dictionary representation of the object.
        """
        self._is_valid()

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
        self._is_valid()

        # Implicit list conversion.
        return_list_type = True
        if  not isinstance(pilot_descriptions, list):
            return_list_type   = False
            pilot_descriptions = [pilot_descriptions]

        logger.demo('info', 'submit %d pilot(s)' % len(pilot_descriptions))

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
            logger.demo('progress')

        logger.demo('ok', '\\ok\n')

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
        self._is_valid()

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
        self._is_valid()

        
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
        self._is_valid()

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
                else:
                    logger.demo('progress')

                states.append (pilot['state'])

            # check timeout
            if  (None != timeout) and (timeout <= (time.time() - start)):
                if  not all_ok :
                    logger.debug ("wait timed out: %s" % states)
                break

            # sleep a little if this cycle was idle
            if  not all_ok :
                time.sleep (0.5)

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
        self._is_valid()

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
        self._is_valid()

        self._worker.register_manager_callback(callback_function, callback_data)

