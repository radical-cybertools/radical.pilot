
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import copy
import time
import threading

import radical.utils as ru

from .  import utils     as rpu
from .  import states    as rps
from .  import constants as rpc
from .  import types     as rpt

from .umgr import scheduler as rpus


# ------------------------------------------------------------------------------
#
class UnitManager(rpu.Component):
    """
    A UnitManager manages :class:`radical.pilot.ComputeUnit` instances which
    represent the **executable** workload in RADICAL-Pilot. A UnitManager connects
    the ComputeUnits with one or more :class:`Pilot` instances (which represent
    the workload **executors** in RADICAL-Pilot) and a **scheduler** which
    determines which :class:`ComputeUnit` gets executed on which
    :class:`Pilot`.

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
                                       scheduler=radical.pilot.SCHEDULER_ROUND_ROBIN)
        um.add_pilot(p1)
        um.submit_units(compute_units)


    The unit manager can issue notification on unit state changes.  Whenever
    state notification arrives, any callback registered for that notification is
    fired.  
    
    NOTE: State notifications can arrive out of order wrt the unit state model!
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, session, scheduler=None):
        """
        Creates a new UnitManager and attaches it to the session.

        **Arguments:**
            * session [:class:`radical.pilot.Session`]:
              The session instance to use.
            * scheduler (`string`): 
              The name of the scheduler plug-in to use.

        **Returns:**
            * A new `UnitManager` object [:class:`radical.pilot.UnitManager`].
        """

        self._components  = None
        self._pilots      = dict()
        self._pilots_lock = threading.RLock()
        self._units       = dict()
        self._units_lock  = threading.RLock()
        self._callbacks   = dict()
        self._cb_lock     = threading.RLock()
        self._closed      = False
        self._rec_id      = 0       # used for session recording

        for m in rpt.UMGR_METRICS:
            self._callbacks[m] = list()

        # get an ID and initialize logging and profiling
        # FIXME: log and prof are already provided by the base class -- but it
        #        will take a while until we can initialize that, and meanwhile
        #        we use these...
        self._uid  = ru.generate_id('umgr')
        self._log  = ru.get_logger(self.uid, "%s.%s.log" % (session.uid, self._uid))
        self._prof = rpu.Profiler("%s.%s" % (session.uid, self._uid))

        session.prof.prof('create umgr', uid=self._uid)

        cfg = ru.read_json("%s/configs/umgr_%s.json" \
                % (os.path.dirname(__file__),
                    os.environ.get('RADICAL_PILOT_UMGR_CONFIG', 'default')))

        cfg['session_id']  = session.uid
        cfg['owner']       = self.uid
        cfg['mongodb_url'] = str(session.dburl)

        if scheduler:
            # overwrite the scheduler from the config file
            cfg['scheduler'] = scheduler

        if not cfg.get('scheduler'):
            # set default scheduler if needed
            cfg['scheduler'] = rpus.SCHEDULER_DEFAULT

        components = cfg.get('components', [])

        from .. import pilot as rp
        
        # we also need a map from component names to class types
        typemap = {
            rpc.UMGR_STAGING_INPUT_COMPONENT  : rp.umgr.Input,
            rpc.UMGR_SCHEDULING_COMPONENT     : rp.umgr.Scheduler,
            rpc.UMGR_STAGING_OUTPUT_COMPONENT : rp.umgr.Output
            }

        # get addresses from the bridges, and append them to the
        # config, so that we can pass those addresses to the components
        cfg['bridge_addresses'] = copy.deepcopy(session._bridge_addresses)

        # the bridges are known, we can start to connect the components to them
        self._components = rpu.Component.start_components(components, 
                typemap, cfg, session)

        # initialize the base class
        # FIXME: unique ID
        cfg['owner'] = session.uid
        rpu.Component.__init__(self, self.uid, cfg, session)

        # only now we have a logger... :/
        self._log.report.info('<<create unit manager')

        # The output queue is used to forward submitted units to the
        # scheduling component.
        self.declare_output(rps.UMGR_SCHEDULING_PENDING, rpc.UMGR_SCHEDULING_QUEUE)

        # register the state notification pull cb
        # FIXME: we may want to have the frequency configurable
        # FIXME: this should be a tailing cursor in the update worker
        self.declare_idle_cb(self._state_pull_cb, timeout=1.0)

        # also listen to the state pubsub for unit state changes
        self.declare_subscriber('state', 'state_pubsub', self._state_sub_cb)


        self._session._dbs.insert_unit_manager(self.as_dict())

        self._prof.prof('UMGR setup done', logger=self._log.debug)
        self._log.report.ok('>>ok\n')


    # --------------------------------------------------------------------------
    #
    def close(self):
        """
        Shuts down the UnitManager.  This will cancel all units.
        """
        if self._closed:
            raise RuntimeError("instance is already closed")

        self._log.debug("closing %s", self.uid)
        self._log.report.info('<<close unit manager')


     ## if self._worker:
     ##     self._worker.stop()
     ## TODO: kill components

        # kill child process, threads
        self.stop()

        self._session.prof.prof('closed umgr', uid=self._uid)
        self._log.info("Closed UnitManager %s." % str(self._uid))

        self._closed = True
        self._log.report.ok('>>ok\n')


    # --------------------------------------------------------------------------
    #
    def as_dict(self):
        """
        Returns a dictionary representation of the UnitManager object.
        """

        ret = {
            'uid': self.uid,
            'cfg': self.cfg
        }

        return ret


    # --------------------------------------------------------------------------
    #
    def __str__(self):

        """
        Returns a string representation of the UnitManager object.
        """

        return str(self.as_dict())


    #---------------------------------------------------------------------------
    #
    def _state_pull_cb(self):

        # pull all unit states from the DB, and compare to the states we know
        # about.  If any state changed, update the unit instance and issue
        # notification callbacks as needed
        # FIXME: we also pull for dead units.  That is not efficient...
        # FIXME: this needs to be converted into a tailed cursor in the update
        #        worker
        units  = self._session._dbs.get_units(umgr_uid=self.uid)
        action = False

        for unit in units:
            if self._update_unit(unit['uid'], unit):
                action = True

        return action


    # --------------------------------------------------------------------------
    #
    def _state_sub_cb(self, topic, msg):

        if 'type' in msg and msg['type'] == 'unit':

            uid   = msg["uid"]
            state = msg["state"]

            self._update_unit(uid, {'state' : state})


    # --------------------------------------------------------------------------
    #
    def _update_unit(self, uid, unit):

        # we don't care about units we don't know
        # otherwise get old state
        with self._units_lock:

            if uid not in self._units:
                return False

            # only update on state changes
            if self._units[uid].state != unit['state']:
                return self._units[uid]._update(unit)
            else:
                return False


    # --------------------------------------------------------------------------
    #
    def _call_unit_callbacks(self, unit, state):

        for cb_func, cb_data in self._callbacks[rpt.UNIT_STATE]:

            if cb_data: cb_func(unit, state, cb_data)
            else      : cb_func(unit, state)


    # --------------------------------------------------------------------------
    #
    # FIXME: this needs to go to the scheduler
    def _default_wait_queue_size_cb(self, umgr, wait_queue_size):
        # FIXME: this needs to come from the scheduler?

        self._log.info("[Callback]: wait_queue_size: %s.", wait_queue_size)


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):
        """
        Returns the unique id.
        """
        return self._uid


    # --------------------------------------------------------------------------
    #
    @property
    def scheduler(self):
        """
        Returns the scheduler name.
        """

        return self._cfg.get('scheduler')



    # --------------------------------------------------------------------------
    #
    def add_pilots(self, pilots):
        """
        Associates one or more pilots with the unit manager.

        **Arguments:**

            * **pilots** [:class:`radical.pilot.ComputePilot` or list of
              :class:`radical.pilot.ComputePilot`]: The pilot objects that will be
              added to the unit manager.
        """

        if self._closed:
            raise RuntimeError("instance is already closed")

        if not isinstance(pilots, list):
            pilots = [pilots]

        if len(pilots) == 0:
            raise ValueError('cannot add no pilots')

        self._log.report.info('<<add %d pilot(s)' % len(pilots))

        with self._pilots_lock:

            for pilot in pilot:

                pid = pilot.uid

                # sanity check
                if pid in self._pilots:
                    raise ValueError('pilot %s already added' % pid)

                # publish to the command channel for the scheduler to pick up
                self.publish('command', {'cmd' : 'add_pilot', 
                                         'arg' : {'pid'  : pid, 
                                                  'umgr' : self.uid}})

                # also keep pilots around for inspection
                self._pilots[pid] = pilot

        self._log.report.ok('>>ok\n')


    # --------------------------------------------------------------------------
    #
    def list_pilots(self):
        """
        Lists the UIDs of the pilots currently associated with the unit manager.

        **Returns:**
              * A list of :class:`radical.pilot.ComputePilot` UIDs [`string`].
        """

        if self._closed:
            raise RuntimeError("instance is already closed")

        with self._pilots_lock:
            return self._pilots.keys()


    # --------------------------------------------------------------------------
    #
    def get_pilots(self):
        """
        Get the pilots instances currently associated with the unit manager.

        **Returns:**
              * A list of :class:`radical.pilot.ComputePilot` instances.
        """
        if self._closed:
            raise RuntimeError("instance is already closed")

        with self._pilots_lock:
            return self._pilots.values()


    # --------------------------------------------------------------------------
    #
    def remove_pilots(self, pilot_ids, drain=False):
        """
        Disassociates one or more pilots from the unit manager.

        After a pilot has been removed from a unit manager, it won't process
        any of the unit manager's units anymore. Calling `remove_pilots`
        doesn't stop the pilot itself.

        **Arguments:**

            * **drain** [`boolean`]: Drain determines what happens to the units
              which are managed by the removed pilot(s). If `True`, all units
              currently assigned to the pilot are allowed to finish execution.
              If `False` (the default), then `ACTIVE` units will be canceled.
        """

        # TODO: Implement 'drain'.

        if self._closed:
            raise RuntimeError("instance is already closed")

        if drain:
            raise RuntimeError("'drain' is not yet implemented")

        if not isinstance(pilot_ids, list):
            pilot_ids = [pilot_ids]

        with self._pilots_lock:
            for pid in pilot_ids:

                if pid not in self._pilots:
                    raise ValueError('pilot %s not added' % pid)

                del(self._pilots[pid])

                # publish to the command channel for the scheduler to pick up
                self.publish('command', {'cmd' : 'remove_pilot', 
                                         'arg' : pid})


    # --------------------------------------------------------------------------
    #
    def list_units(self):
        """
        Returns the UIDs of the :class:`radical.pilot.ComputeUnit` managed by
        this unit manager.

        **Returns:**
              * A list of :class:`radical.pilot.ComputeUnit` UIDs [`string`].
        """

        if self._closed:
            raise RuntimeError("instance is already closed")

        with self._pilots_lock:
            return self._units.keys()


    # --------------------------------------------------------------------------
    #
    def submit_units(self, descriptions):
        """
        Submits on or more :class:`radical.pilot.ComputeUnit` instances to the
        unit manager.

        **Arguments:**
            * **descriptions** [:class:`radical.pilot.ComputeUnitDescription`
              or list of :class:`radical.pilot.ComputeUnitDescription`]: The
              description of the compute unit instance(s) to create.

        **Returns:**
              * A list of :class:`radical.pilot.ComputeUnit` objects.
        """

        from .compute_unit import ComputeUnit

        if self._closed:
            raise RuntimeError("instance is already closed")

        ret_list = True
        if not isinstance(descriptions, list):
            ret_list     = False
            descriptions = [descriptions]

        if len(descriptions) == 0:
            raise ValueError('cannot submit no unit descriptions')


        self._log.report.info('<<submit %d unit(s)\n\t' % len(descriptions))

        # we return a list of compute units
        units     = list()
        unit_docs = list()
        for descr in descriptions :
            unit = ComputeUnit.create(umgr=self, descr=descr)
            units.append(unit)
            unit_docs.append(unit.as_dict())

            # keep units around
            with self._units_lock:
                self._units[unit.uid] = unit

            if self._session._rec:
                import radical.utils as ru
                ru.write_json(descr.as_dict(), "%s/%s.batch.%03d.json" \
                        % (self._session._rec, unit.uid, self._rec_id))
            self._log.report.progress()

        if self._session._rec:
            self._rec_id += 1

        # insert units into the database, as a bulk.
        self._session._dbs.insert_units(unit_docs)

        # Only after the insert can we hand the units over to the next
        # components (ie. advance state).
        # FIXME: advance as bulk
        for doc in unit_docs:
            self.advance(doc, rps.UMGR_SCHEDULING_PENDING, publish=True, push=True)

        self._log.report.ok('>>ok\n')

        if ret_list: return units
        else       : return units[0]


    # --------------------------------------------------------------------------
    #
    def get_units(self, uids=None):
        """Returns one or more compute units identified by their IDs.

        **Arguments:**
            * **uids** [`string` or `list of strings`]: The IDs of the
              compute unit objects to return.

        **Returns:**
              * A list of :class:`radical.pilot.ComputeUnit` objects.
        """
        
        if self._closed:
            raise RuntimeError("instance is already closed")

        if not uids:
            with self._units_lock:
                ret = self._units.values()
            return ret


        ret_list = True
        if (not isinstance(uids, list)) and (uids is not None):
            ret_list = False
            uids = [uids]

        ret = list()
        with self._units_lock:
            for uid in uids:
                if uid not in self._units:
                    raise ValueError('unit %s not known' % uid)
                ret.append(self._units[uid])

        if ret_list: return ret
        else       : return ret[0]


    # --------------------------------------------------------------------------
    #
    def wait_units(self, uids=None, state=None, timeout=None):
        """
        Returns when one or more :class:`radical.pilot.ComputeUnits` reach a
        specific state.

        If `unit_uids` is `None`, `wait_units` returns when **all**
        ComputeUnits reach the state defined in `state`.  This may include
        units which have previously terminated or waited upon.

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

              * :data:`radical.pilot.rps.DONE`
              * :data:`radical.pilot.rps.FAILED`
              * :data:`radical.pilot.rps.CANCELED`

            * **timeout** [`float`]
              Timeout in seconds before the call returns regardless of Pilot
              state changes. The default value **None** waits forever.
        """

        if self._closed:
            raise RuntimeError("instance is already closed")

        if not uids:
            with self._units_lock:
                uids = list()
                for uid,unit in self._units.iteritems():
                    if unit.state not in rps.FINAL:
                        uids.append(uid)

        if not state:
            states = rps.FINAL
        elif isinstance(state, list):
            states = state
        else:
            states = [state]

        ret_list = True
        if not isinstance(uids, list):
            ret_list = False
            uids = [uids]

        self._log.report.info('<<wait for %d unit(s)\n\t' % len(uids))

        start    = time.time()
        to_check = None

        with self._units_lock:
            to_check = [self._units[uid] for uid in uids]

        # We don't want to iterate over all units again and again, as that would
        # duplicate checks on units which were found in matching states.  So we
        # create a list from which we drop the units as we find them in
        # a matching state
        self._log.report.idle(mode='start')
        while to_check and not self._session._terminate.is_set():

            self._log.report.idle()

            to_check = [unit for unit in to_check \
                              if unit.state not in states and \
                                 unit.state not in rps.FINAL]
            # check timeout
            if to_check:
                if timeout and (timeout <= (time.time() - start)):
                    self._log.debug ("wait timed out")
                    break

                time.sleep (0.5)

        self._log.report.idle(mode='stop')

        if to_check: self._log.report.warn('>>timeout\n')
        else       : self._log.report.ok(  '>>ok\n')

        # grab the current states to return
        state = None
        with self._units_lock:
            states = [self._units[uid].state for uid in uids]

        # done waiting
        if ret_list: return states
        else       : return states[0]


    # --------------------------------------------------------------------------
    #
    def cancel_units(self, uids=None):
        """
        Cancel one or more :class:`radical.pilot.ComputeUnits`.

        **Arguments:**
            * **uids** [`string` or `list of strings`]: The IDs of the
              compute unit objects to cancel.
        """
        if self._closed:
            raise RuntimeError("instance is already closed")

        if not uids:
            with self._units_lock:
                uids = self._units.keys()

        if not isinstance(uids, list):
            uids = [uids]

        units = self.get_units(uids)
        for unit in units:
            unit.cancel()


    # --------------------------------------------------------------------------
    #
    def register_callback(self, cb_func, metric=rpt.UNIT_STATE, cb_data=None):
        """
        Registers a new callback function with the UnitManager.  Manager-level
        callbacks get called if the specified metric changes.  The default
        metric `UNIT_STATE` fires the callback if any of the ComputeUnits
        managed by the PilotManager change their state.

        All callback functions need to have the same signature::

            def cb_func(obj, value, cb_data)

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

        # FIXME: the signature should be (self, metrics, cb, cb_data)

        if  metric not in rpt.UMGR_METRICS :
            raise ValueError ("Metric '%s' is not available on the unit manager" % metric)

        with self._cb_lock:
            self._callbacks['umgr'][metric].append([cb_func, cb_data])


# ------------------------------------------------------------------------------

