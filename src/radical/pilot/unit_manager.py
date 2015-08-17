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

from radical.pilot.controller   import UnitManagerController
from radical.pilot.scheduler    import get_scheduler, SCHED_DEFAULT

from radical.pilot.types        import *
from radical.pilot.states       import *
from radical.pilot.exceptions   import *

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
        self._worker  = None 
        self._pilots  = list()
        self._rec_id  = 0

        if not scheduler:
            scheduler = SCHED_DEFAULT

        # keep track of some changing metrics
        self.wait_queue_size = 0

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
            self._scheduler = get_scheduler(name=scheduler, 
                                            manager=self, 
                                            session=self._session)

            # Each unit manager has a worker thread associated with it.
            # The task of the worker thread is to check and update the state
            # of units, fire callbacks and so on.
            self._session._unit_manager_objects.append(self)
            self._session._process_registry.register(self._uid, self._worker)

        else:
            # re-connect. do nothing
            pass


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
            raise BadParameter(
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

        obj._scheduler = get_scheduler(name=um_data['scheduler'], 
                                       manager=obj,
                                       session=obj._session)
        # FIXME: we need to tell the scheduler about all the pilots...

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
            raise IncorrectState(msg="Invalid object instance.")

        return self._scheduler.name

    #--------------------------------------------------------------------------
    #
    @property
    def scheduler_details(self):
        """Returns the scheduler logs.
        """
        if not self._uid:
            raise IncorrectState(msg="Invalid object instance.")

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
            raise IncorrectState(msg="Invalid object instance.")

        if not isinstance(pilots, list):
            pilots = [pilots]

        pilot_ids = self.list_pilots()

        for pilot in pilots :
            if  pilot.uid in pilot_ids :
                logger.warning ('adding the same pilot twice (%s)' % pilot.uid)

        self._worker.add_pilots(pilots)

        # let the scheduler know...
        for pilot in pilots :
            self._scheduler.pilot_added (pilot)

        # also keep the instances around
        for pilot in pilots :
            self._pilots.append (pilot)


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
            raise IncorrectState(msg="Invalid object instance.")

        return self._worker.get_pilot_uids()


    # -------------------------------------------------------------------------
    #
    def get_pilots(self):
        """get the pilots instances currently associated with
        the unit manager.

        **Returns:**

              * A list of :class:`radical.pilot.ComputePilot` instances.

        **Raises:**

            * :class:`radical.pilot.PilotException`
        """
        if not self._uid:
            raise IncorrectState(msg="Invalid object instance.")

        return self._pilots

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
            raise IncorrectState(msg="Invalid object instance.")

        if not isinstance(pilot_ids, list):
            pilot_ids = [pilot_ids]

        self._worker.remove_pilots(pilot_ids)


        # FIXME:
        # if a pilot gets removed, we need to re-assign all its units to other
        # pilots.   We thus move them all into the wait queue, and call the
        # global rescheduler.  Some of the CUs might already be in final state
        # -- those are ignored (they'll be in the done_queue anyways).  We leave
        # it to the scheduling policy what happens to non-NEW CUs in the
        # wait_queue, i.e. if they get rescheduled, or if they'll raise an
        # error.

        # let the scheduler know...
        for pilot_id in pilot_ids :
            self._scheduler.pilot_removed (pilot_id)

        # update instance list
        for pilot_id in pilot_ids :
            for pilot in self._pilots[:] :
                if  pilot_id == pilot.uid :
                    self._pilots.remove (pilot)

    # -------------------------------------------------------------------------
    #
    def list_units(self):
        """Returns the UIDs of the :class:`radical.pilot.ComputeUnit` managed by
        this unit manager.

        **Returns:**

              * A list of :class:`radical.pilot.ComputeUnit` UIDs [`string`].

        """
        if not self._uid:
            raise IncorrectState(msg="Invalid object instance.")

        return self._worker.get_compute_unit_uids()


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
            raise IncorrectState(msg="Invalid object instance.")

        return_list_type = True
        if not isinstance(unit_descriptions, list):
            return_list_type  = False
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

        units = list()
        for ud in unit_descriptions :

            u = ComputeUnit.create (unit_description=ud,
                                    unit_manager_obj=self, 
                                    local_state=SCHEDULING)
            units.append(u)

            if self._session._rec:
                import radical.utils as ru
                ru.write_json(ud.as_dict(), "%s/%s.batch.%03d.json" \
                        % (self._session._rec, u.uid, self._rec_id))

        if self._session._rec:
            self._rec_id += 1

        self._worker.publish_compute_units (units=units)

        schedule = None
        try:
            schedule = self._scheduler.schedule (units=units)
       
        except Exception as e:
            logger.exception ("Internal error - unit scheduler failed")
            raise 

        self.handle_schedule (schedule)

        if  return_list_type :
            return units
        else :
            return units[0]


    # -------------------------------------------------------------------------
    #
    def handle_schedule (self, schedule) :

        # we want to use bulk submission to the pilots, so we collect all units
        # assigned to the same set of pilots.  At the same time, we select
        # unscheduled units for later insertion into the wait queue.
        
        if  not schedule :
            logger.debug ('skipping empty unit schedule')
            return

      # print 'handle schedule:'
      # import pprint
      # pprint.pprint (schedule)
      #
        pilot_cu_map = dict()
        unscheduled  = list()

        pilot_ids = self.list_pilots ()

        for unit in schedule['units'].keys() :

            pid = schedule['units'][unit]

            if  None == pid :
                unscheduled.append (unit)
                continue

            else :

                if  pid not in pilot_ids :
                    raise RuntimeError ("schedule points to unknown pilot %s" % pid)

                if  pid not in pilot_cu_map :
                    pilot_cu_map[pid] = list()

                pilot_cu_map[pid].append (unit)


        # submit to all pilots which got something submitted to
        for pid in pilot_cu_map.keys():

            units_to_schedule = list()

            # if a kernel name is in the cu descriptions set, do kernel expansion
            for unit in pilot_cu_map[pid] :

                if  not pid in schedule['pilots'] :
                    # lost pilot, do not schedule unit
                    logger.warn ("unschedule unit %s, lost pilot %s" % (unit.uid, pid))
                    continue

                unit.sandbox = schedule['pilots'][pid]['sandbox'] + "/" + str(unit.uid)

                ud = unit.description

                if  'kernel' in ud and ud['kernel'] :

                    try :
                        from radical.ensemblemd.mdkernels import MDTaskDescription
                    except Exception as ex :
                        logger.error ("Kernels are not supported in" \
                              "compute unit descriptions -- install " \
                              "radical.ensemblemd.mdkernels!")
                        # FIXME: unit needs a '_set_state() method or something!
                        self._session._dbs.set_compute_unit_state (unit._uid, FAILED, 
                                ["kernel expansion failed"])
                        continue

                    pilot_resource = schedule['pilots'][pid]['resource']

                    mdtd           = MDTaskDescription ()
                    mdtd.kernel    = ud.kernel
                    mdtd_bound     = mdtd.bind (resource=pilot_resource)
                    ud.environment = mdtd_bound.environment
                    ud.pre_exec    = mdtd_bound.pre_exec
                    ud.executable  = mdtd_bound.executable
                    ud.mpi         = mdtd_bound.mpi


                units_to_schedule.append (unit)

            if  len(units_to_schedule) :
                self._worker.schedule_compute_units (pilot_uid=pid,
                                                     units=units_to_schedule)


        # report any change in wait_queue_size
        old_wait_queue_size = self.wait_queue_size

        self.wait_queue_size = len(unscheduled)
        if  old_wait_queue_size != self.wait_queue_size :
            self._worker.fire_manager_callback (WAIT_QUEUE_SIZE, self,
                                                self.wait_queue_size)

        if  len(unscheduled) :
            self._worker.unschedule_compute_units (units=unscheduled)

        logger.info ('%s units remain unscheduled' % len(unscheduled))


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
            raise IncorrectState(msg="Invalid object instance.")

        return_list_type = True
        if (not isinstance(unit_ids, list)) and (unit_ids is not None):
            return_list_type = False
            unit_ids = [unit_ids]

        units = ComputeUnit._get(unit_ids=unit_ids, unit_manager_obj=self)

        if  return_list_type :
            return units
        else :
            return units[0]

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
        if  not self._uid:
            raise IncorrectState(msg="Invalid object instance.")

        if not isinstance(state, list):
            state = [state]

        return_list_type = True
        if (not isinstance(unit_ids, list)) and (unit_ids is not None):
            return_list_type = False
            unit_ids = [unit_ids]

        units  = self.get_units (unit_ids)
        start  = time.time()
        all_ok = False
        states = list()

        while not all_ok :

            all_ok = True
            states = list()

            for unit in units :
                if  unit.state not in state :
                    all_ok = False

                states.append (unit.state)

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
    def cancel_units(self, unit_ids=None):
        """Cancel one or more :class:`radical.pilot.ComputeUnits`.

        **Arguments:**

            * **unit_ids** [`string` or `list of strings`]: The IDs of the
              compute unit objects to cancel.

        **Raises:**

            * :class:`radical.pilot.PilotException`
        """
        if not self._uid:
            raise IncorrectState(msg="Invalid object instance.")

        if (not isinstance(unit_ids, list)) and (unit_ids is not None):
            unit_ids = [unit_ids]

        cus = self.get_units(unit_ids)
        for cu in cus:
            cu.cancel()


    # -------------------------------------------------------------------------
    #
    def register_callback(self, callback_function, metric=UNIT_STATE, callback_data=None):

        """
        Registers a new callback function with the UnitManager.  Manager-level
        callbacks get called if the specified metric changes.  The default
        metric `UNIT_STATE` fires the callback if any of the ComputeUnits
        managed by the PilotManager change their state.

        All callback functions need to have the same signature::

            def callback_func(obj, value, data)

        where ``object`` is a handle to the object that triggered the callback,
        ``value`` is the metric, and ``data`` is the data provided on
        callback registration..  In the example of `UNIT_STATE` above, the
        object would be the unit in question, and the value would be the new
        state of the unit.

        Available metrics are:

          * `UNIT_STATE`: fires when the state of any of the units which are
            managed by this unit manager instance is changing.  It communicates
            the unit object instance and the units new state.

          * `WAIT_QUEUE_SIZE`: fires when the number of unscheduled units (i.e.
            of units which have not been assigned to a pilot for execution)
            changes.
        """

        if  metric not in UNIT_MANAGER_METRICS :
            raise ValueError ("Metric '%s' is not available on the unit manager" % metric)

        self._worker.register_manager_callback(callback_function, metric, callback_data)

