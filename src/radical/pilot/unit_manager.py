    #pylint: disable=C0301, C0103, W0212

"""
.. module:: radical.pilot.unit_manager
   :platform: Unix
   :synopsis: Implementation of the UnitManager class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import os
import time
import weakref

from radical.pilot.compute_unit import ComputeUnit
from radical.pilot.utils.logger import logger

from radical.pilot.controller import UnitManagerController
from radical.pilot.scheduler import get_scheduler

from radical.pilot.states import *
from radical.pilot.exceptions import PilotException


# -----------------------------------------------------------------------------
#
class UnitManager(object):
    """A UnitManager manages :class:`radical.pilot.ComputeUnit` instances which
    represent the **executable** workload in RADICAL-Pilot. A UnitManager connects
    the ComputeUnits with one or more :class:`Pilot` instances (which represent
    the workload **executors** in RADICAL-Pilot) and a **scheduler** which
    determines which :class:`ComputeUnit` gets executed on which
    :class:`Pilot`.

    Each UnitManager has a unique identifier :data:`radical.pilot.UnitManager.uid`
    that can be used to re-connect to previoulsy created UnitManager in a
    given :class:`radical.pilot.Session`.

    **Example**::

        s = radical.pilot.Session(database_url=DBURL)

        pm = radical.pilot.PilotManager(session=s)

        pd = radical.pilot.ComputePilotDescription()
        pd.resource = "futuregrid.alamo"
        pd.cores = 16

        p1 = pm.submit_pilots(pd) # create first pilot with 16 cores
        p2 = pm.submit_pilots(pd) # create second pilot with 16 cores

        # Create a workload of 128 '/bin/sleep' compute units
        compute_units = []
        for unit_count in range(0, 128):
            cu = radical.pilot.ComputeUnitDescription()
            cu.executable = "/bin/sleep"
            cu.arguments = ['60']
            compute_units.append(cu)

        # Combine the two pilots, the workload and a scheduler via
        # a UnitManager.
        um = radical.pilot.UnitManager(session=session,
                                   scheduler=radical.pilot.SCHED_ROUND_ROBIN)
        um.add_pilot(p1)
        um.submit_units(compute_units)
    """

    # -------------------------------------------------------------------------
    #
    def __init__(self, session, scheduler=None, input_transfer_workers=2,
                 output_transfer_workers=2, _reconnect=False):
        """Creates a new UnitManager and attaches it to the session.

        **Args:**

            * session (`string`): The session instance to use.

            * scheduler (`string`): The name of the scheduler plug-in to use.

            * input_transfer_workers (`int`): The number of input file transfer 
              worker processes to launch in the background. 

            * output_transfer_workers (`int`): The number of output file transfer 
              worker processes to launch in the background. 

        .. note:: `input_transfer_workers` and `output_transfer_workers` can be
                  used to tune RADICAL-Pilot's file transfer performance. 
                  However, you should only change the default values if you 
                  know what you are doing.

        **Raises:**
            * :class:`radical.pilot.PilotException`
        """
        self._session = session
        self._worker = None 
        self._pilots = []

        # queues for managing the CUs.
        self.wait_queue = list() # incoming CU requesus
        self.work_queue = list() # active CUs
        self.done_queue = list() # finished CUs

        # also keep a assignment map from pilot to CUs.  This will contain all
        # CUs -- CUs not assigned to a pilot are mapped to None.  New pilots
        # will get a new entry in this map.
        self.pilot_cu_map = {None: list()}



        if _reconnect is False:
            # Start a worker process fo this UnitManager instance. The worker
            # process encapsulates database access et al.
            self._worker = UnitManagerController(
                unit_manager_uid=None, 
                scheduler=scheduler,
                input_transfer_workers=input_transfer_workers,
                output_transfer_workers=output_transfer_workers, 
                session=self._session,
                db_connection=session._dbs,
                db_connection_info=session._connection_info)
            self._worker.start()

            self._uid = self._worker.unit_manager_uid
            self._scheduler = get_scheduler(name=scheduler)

            # Each pilot manager has a worker thread associated with it.
            # The task of the worker thread is to check and update the state
            # of pilots, fire callbacks and so on.
            self._session._unit_manager_objects.append(self)
            self._session._process_registry.register(self._uid, self._worker)

        else:
            # re-connect. do nothing
            pass

      # # make sure to register unit state callbacks, to trigger re-scheduling
      # # as needed...
      # self.register_callback (self._unit_state_callback)


    #--------------------------------------------------------------------------
    #
    def close(self):
        """Shuts down the UnitManager and its background workers in a 
        coordinated fashion.
        """
        if not self._uid:
            logger.warning("UnitManager object already closed.")
            return

        if self._worker is not None:
            self._worker.stop()
            # Remove worker from registry
            self._session._process_registry.remove(self._uid)

        logger.info("Closed UnitManager %s." % str(self._uid))
        self._uid = None

    #--------------------------------------------------------------------------
    #
    @classmethod
    def _reconnect(cls, session, unit_manager_id):
        """PRIVATE: Reconnect to an existing UnitManager.
        """
        uid_exists = UnitManagerController.uid_exists(
            db_connection=session._dbs,
            unit_manager_uid=unit_manager_id)

        if not uid_exists:
            raise exceptions.BadParameter(
                "UnitManager with id '%s' not in database." % unit_manager_id)

        # The UnitManager object
        obj = cls(session=session, scheduler=None, _reconnect=True)

        # Retrieve or start a worker process fo this PilotManager instance.
        worker = session._process_registry.retrieve(unit_manager_id)
        if worker is not None:
            obj._worker = worker
        else:
            obj._worker = UnitManagerController(
                unit_manager_uid=unit_manager_id,
                session=session,
                db_connection=session._dbs,
                db_connection_info=session._connection_info)
            session._process_registry.register(unit_manager_id, obj._worker)

        # start the worker if it's not already running
        if obj._worker.is_alive() is False:
            obj._worker.start()

        # Now that the worker is running (again), we can get more information
        # about the UnitManager
        um_data = obj._worker.get_unit_manager_data()

        obj._scheduler = get_scheduler(name=um_data['scheduler'])
        obj._uid = unit_manager_id

        logger.info("Reconnected to existing UnitManager %s." % str(obj))
        return obj

    # -------------------------------------------------------------------------
    #
    def as_dict(self):
        """Returns a Python dictionary representation of the UnitManager
        object.
        """
        obj_dict = {
            'uid':               self.uid,
            'scheduler':         self.scheduler,
            'scheduler_details': self.scheduler_details
        }
        return obj_dict

    # -------------------------------------------------------------------------
    #
    def __str__(self):
        """Returns a string representation of the UnitManager object.
        """
        return str(self.as_dict())

    #--------------------------------------------------------------------------
    #
    @property
    def uid(self):
        """Returns the unique id.
        """
        return self._uid

    #--------------------------------------------------------------------------
    #
    @property
    def scheduler(self):
        """Returns the scheduler name.
        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid object instance.")

        return self._scheduler.name

    #--------------------------------------------------------------------------
    #
    @property
    def scheduler_details(self):
        """Returns the scheduler logs.
        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid object instance.")

        return "NO SCHEDULER DETAILS (Not Implemented)"

    # -------------------------------------------------------------------------
    #
    def add_pilots(self, pilots):
        """Associates one or more pilots with the unit manager.

        **Arguments:**

            * **pilots** [:class:`radical.pilot.ComputePilot` or list of
              :class:`radical.pilot.ComputePilot`]: The pilot objects that will be
              added to the unit manager.

        **Raises:**

            * :class:`radical.pilot.PilotException`
        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid object instance.")

        if not isinstance(pilots, list):
            pilots = [pilots]

        self._worker.add_pilots(pilots)

        for pilot in pilots:

            self._pilots.append(pilot.uid)
            self.pilot_cu_map[pilot.uid] = list()


    # -------------------------------------------------------------------------
    #
    def list_pilots(self):
        """Lists the UIDs of the pilots currently associated with
        the unit manager.

        **Returns:**

              * A list of :class:`radical.pilot.ComputePilot` UIDs [`string`].

        **Raises:**

            * :class:`radical.pilot.PilotException`
        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid object instance.")

        return self._worker.get_pilot_uids()

    # -------------------------------------------------------------------------
    #
    def remove_pilots(self, pilot_ids, drain=True):
        """Disassociates one or more pilots from the unit manager.

        TODO: Implement 'drain'.

        After a pilot has been removed from a unit manager, it won't process
        any of the unit manager's units anymore. Calling `remove_pilots`
        doesn't stop the pilot itself.

        **Arguments:**

            * **drain** [`boolean`]: Drain determines what happens to the units
              which are managed by the removed pilot(s). If `True`, all units
              currently assigned to the pilot are allowed to finish execution.
              If `False` (the default), then `ACTIVE` units will be canceled.

        **Raises:**

            * :class:`radical.pilot.PilotException`
        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid object instance.")

        if not isinstance(pilot_ids, list):
            pilot_ids = [pilot_ids]

        self._worker.remove_pilots(pilot_ids)


        # if a pilot gets removed, we need to re-assign all its units to other
        # pilots.   We thus move them all into the wait queue, and call the
        # global rescheduler.  Some of the CUs might already be in final state
        # -- those are ignored (they'll be in the done_queue anyways).  We leave
        # it to the scheduling policy what happens to non-NEW CUs in the
        # wait_queue, i.e. if they get rescheduled, or if they'll raise an
        # error.

        for pilot_id in pilot_ids :
            for cu in self.pilot_cu_map[pilot_id] :
                if  cu.state not in [DONE, FAILED, CANCELED] :
                    self.wait_queue.append (cu)

            self.pilot_cu_map[pilot_id] = list()

        # FIXME: call global reschedule...


    # -------------------------------------------------------------------------
    #
    def list_units(self):
        """Returns the UIDs of the :class:`radical.pilot.ComputeUnit` managed by
        this unit manager.

        **Returns:**

              * A list of :class:`radical.pilot.ComputeUnit` UIDs [`string`].

        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid object instance.")

        return self._worker.get_compute_unit_uids()

    # -------------------------------------------------------------------------
    #
    def _unit_state_callback (self, cu, state) :
        
        logger.debug ("unit %s changed to %s" % (cu.uid, state))

        if  state in [DONE, FAILED, CANCELED] :
            # the pilot which owned this CU should now have free slots available
            self._re_schedule (finished_cu=cu)


    # -------------------------------------------------------------------------
    #
    def _pilot_state_callback (self, pilot, state) :
        
        logger.debug ("pilot %s changed to %s" % (pilot.uid, state))

        if  state in [ACTIVE] :
            # the pilot which owned this CU should now have free slots available
            self._re_schedule (active_pilot=pilot)

        if  state in [DONE, FAILED, CANCELED] :
            # the CUs owned by this pilot need to be re-scheduled (unless they
            # are in a final state, then they'll be in the done_queue anyways).
            #
            self._re_schedule (active_pilot=pilot)


    # -------------------------------------------------------------------------
    #
    def _re_schedule (self, finished_cu=None, active_pilot=None) :
        """
        On certain events, we attempot to re-schedule CUs which previously have
        not been assigned to a pilot, usually due to insufficient capabilities.
        This methid is called when a previsouly submitted CU finishes (which
        frees resources from the pilot); 
        """

        if  finished_cu :

            # check if we knew the finished CU -- if not, we have a consistency
            # problem
            if  finished_cu not in self.work_queue :
                raise RuntimeError ('Unit scheduler is in inconsistent state')

            # move the CU to done list
            self.done_queue.append (finished_cu)
            self.work_queue.remove (finished_cu)

            # look through the wait queue, and try to schedule the first CU which
            # has the same or smaller size as the finished CU -- g=that should fit.
            # We repeat that until we find no more CUs which can (in total) occupy
            # the freed space.
            free_slots = finished_cu.cores

            # at this point, we would *love* to know which pilot owned the
            # finished CU -- then we could simply assign the matching CUs below
            # to that pilot... :(



        if  active_pilot :
            # if a new pilot became active, then the number of free slots is
            # exactly the size of that pilot
            free_slots = active_pilot.cores


        # we now know how many slots are *at least* free, so we attempt to
        # schedule CUs into that space
        for (cud, manager) in self.wait_queue :

            if  free_slots < 0 :
                raise RuntimeError ('Unit scheduler is in inconsistent state')

            if  free_slots == 0 :
                # wait for the next CU do finish...
                break

            if  cud.cores <= free_slots :
                try :
                    cu_id       = self.schedule (manager, cud)
                    free_slots -= cud.cores
                    logger.info ("re-scheduled CU %s" % cu_id)

                except Exception as e :
                    logger.debug ("Failed to reschedule CU %s" % cud)


    # -------------------------------------------------------------------------
    #
    def submit_units(self, unit_descriptions):
        """Submits on or more :class:`radical.pilot.ComputeUnit` instances to the
        unit manager.

        **Arguments:**

            * **unit_descriptions** [:class:`radical.pilot.ComputeUnitDescription`
              or list of :class:`radical.pilot.ComputeUnitDescription`]: The
              description of the compute unit instance(s) to create.

        **Returns:**

              * A list of :class:`radical.pilot.ComputeUnit` objects.

        **Raises:**

            * :class:`radical.pilot.PilotException`
        """

        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid object instance.")

        if not isinstance(unit_descriptions, list):
            unit_descriptions = [unit_descriptions]

        # we return a list of compute units
        ret = list()

        # the scheduler will return a dictionary of the form:
        #   {
        #     ud_1 : pilot_id_a,
        #     ud_2 : pilot_id_b
        #     ...
        #   }
        #
        # The scheduler may not be able to schedule some units - those will
        # have 'None' as pilot ID.

        try:
            schedule = self._scheduler.schedule(
                manager=self, 
                unit_descriptions=unit_descriptions)

        except Exception as e:
            raise PilotException(
                "Internal error - unit scheduler failed: %s" % e)

        if  len(schedule.keys()) != len(unit_descriptions) :
            raise PilotException(
                "Internal error - invalid unit count")


        # we want to use bulk submission to the pilots, so we collect all units
        # assigned to the same set of pilots.  At the same time, we select
        # unscheduled units for later insertion into the wait queue.

        pilot_cu_map = dict()
        unscheduled  = list()

        for ud in schedule.keys() :

            pilot_id = schedule[ud]

            if  None == pilot_id :
                unscheduled.append (ud)
                continue

            if  pilot_id not in pilot_cu_map :
                pilot_cu_map[pilot_id] = list()

            pilot_cu_map[pilot_id].append (ud)


        # submit to all pilots which got something submitted to
        for pilot_id in pilot_cu_map.keys():

            pilot_units = list()

            # sanity check on scheduler provided information
            if not pilot_id in self.list_pilots():
                raise PilotException(
                    "Internal error - invalid scheduler reply, "
                    "no such pilot %s" % pilot_id)

            # submit each unit description scheduled here, all in one bulk
            for ud in pilot_cu_map[pilot_id] :

                # create a new ComputeUnit object
                cu = ComputeUnit._create(
                    unit_description=ud,
                    unit_manager_obj=self, 
                    local_state=STATE_X
                )

                pilot_units.append(cu)

            self._worker.schedule_compute_units(
                pilot_uid=pilot_id,
                units=pilot_units
            )

            ret += pilot_units

        # The above submission will have pushed all units into the SCHEDULED
        # state, and will have addded entries into the CUs state history.
        # For all unscheduled units we will also create a CU object, but will
        # set the state to 'NEW' and rely on later rescheduling to pick them
        # up.
        if len(unscheduled) :

           new_units = list()

           for ud in unscheduled :
               # create a new ComputeUnit object
               cu = ComputeUnit._create(
                   unit_description=ud,
                   unit_manager_obj=self, 
                   local_state=NEW
               )
               logger.warning ("delayed scheduling of unit %s" % str(cu.uid))

               new_units.append(cu)

           
           self._worker.publish_compute_units(units=new_units)

           ret += new_units

        # we are done -- return the little buggers...
        if len(ret) == 1:
            return ret[0]
        else:
            return ret

    # -------------------------------------------------------------------------
    #
    def get_units(self, unit_ids=None):
        """Returns one or more compute units identified by their IDs.

        **Arguments:**

            * **unit_ids** [`string` or `list of strings`]: The IDs of the
              compute unit objects to return.

        **Returns:**

              * A list of :class:`radical.pilot.ComputeUnit` objects.

        **Raises:**

            * :class:`radical.pilot.PilotException`
        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid object instance.")

        if (not isinstance(unit_ids, list)) and (unit_ids is not None):
            unit_ids = [unit_ids]

        units = ComputeUnit._get(unit_ids=unit_ids, unit_manager_obj=self)
        return units

    # -------------------------------------------------------------------------
    #
    def wait_units(self, unit_ids=None,
                   state=[DONE, FAILED, CANCELED],
                   timeout=None):
        """Returns when one or more :class:`radical.pilot.ComputeUnits` reach a
        specific state.

        If `unit_uids` is `None`, `wait_units` returns when **all**
        ComputeUnits reach the state defined in `state`.

        **Example**::

            # TODO -- add example

        **Arguments:**

            * **unit_uids** [`string` or `list of strings`]
              If unit_uids is set, only the ComputeUnits with the specified
              uids are considered. If unit_uids is `None` (default), all
              ComputeUnits are considered.

            * **state** [`string`]
              The state that ComputeUnits have to reach in order for the call
              to return.

              By default `wait_units` waits for the ComputeUnits to
              reach a terminal state, which can be one of the following:

              * :data:`radical.pilot.DONE`
              * :data:`radical.pilot.FAILED`
              * :data:`radical.pilot.CANCELED`

            * **timeout** [`float`]
              Timeout in seconds before the call returns regardless of Pilot
              state changes. The default value **None** waits forever.

        **Raises:**

            * :class:`radical.pilot.PilotException`
        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid object instance.")

        if (not isinstance(unit_ids, list)) and (unit_ids is not None):
            unit_ids = [unit_ids]

        start_wait = time.time()
        all_done = False

        while all_done is False:

            all_done = True

            for wu_state in self._worker.get_compute_unit_states():
                if wu_state not in state:
                    all_done = False
                    break  # leave for loop

            # check timeout
            if (None != timeout) and (timeout <= (time.time() - start_wait)):
                break

            # wait a bit
            time.sleep(0.1)

        # done waiting
        return

    # -------------------------------------------------------------------------
    #
    def cancel_units(self, unit_ids=None):
        """Cancel one or more :class:`radical.pilot.ComputeUnits`.

        **Arguments:**

            * **unit_ids** [`string` or `list of strings`]: The IDs of the
              compute unit objects to cancel.

        **Raises:**

            * :class:`radical.pilot.PilotException`
        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid object instance.")

        if (not isinstance(unit_ids, list)) and (unit_ids is not None):
            unit_ids = [unit_ids]

        raise Exception("Not implemented")

    # -------------------------------------------------------------------------
    #
    def register_callback(self, callback_function):
        """Registers a new callback function with the UnitManager.
        Manager-level callbacks get called if any of the ComputeUnits managed
        by the PilotManager change their state.

        All callback functions need to have the same signature::

            def callback_func(obj, state)

        where ``object`` is a handle to the object that triggered the callback
        and ``state`` is the new state of that object.
        """
        self._worker.register_manager_callback(callback_function)
