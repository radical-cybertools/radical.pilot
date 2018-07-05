
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import time
import threading

import radical.utils as ru

from . import utils     as rpu
from . import states    as rps
from . import constants as rpc
from . import types     as rpt

from . import compute_unit_description as rpcud

from .umgr import scheduler as rpus


# ------------------------------------------------------------------------------
#
class UnitManager(rpu.Component):
    """
    A UnitManager manages :class:`radical.pilot.ComputeUnit` instances which
    represent the **executable** workload in RADICAL-Pilot. A UnitManager
    connects the ComputeUnits with one or more :class:`Pilot` instances (which
    represent the workload **executors** in RADICAL-Pilot) and a **scheduler**
    which determines which :class:`ComputeUnit` gets executed on which
    :class:`Pilot`.

    **Example**::

        s = rp.Session(database_url=DBURL)

        pm = rp.PilotManager(session=s)

        pd = rp.ComputePilotDescription()
        pd.resource = "futuregrid.alamo"
        pd.cores = 16

        p1 = pm.submit_pilots(pd) # create first pilot with 16 cores
        p2 = pm.submit_pilots(pd) # create second pilot with 16 cores

        # Create a workload of 128 '/bin/sleep' compute units
        compute_units = []
        for unit_count in range(0, 128):
            cu = rp.ComputeUnitDescription()
            cu.executable = "/bin/sleep"
            cu.arguments = ['60']
            compute_units.append(cu)

        # Combine the two pilots, the workload and a scheduler via
        # a UnitManager.
        um = rp.UnitManager(session=session,
                            scheduler=rp.SCHEDULER_ROUND_ROBIN)
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

        self._bridges     = dict()
        self._components  = dict()
        self._pilots      = dict()
        self._pilots_lock = threading.RLock()
        self._units       = dict()
        self._units_lock  = threading.RLock()
        self._callbacks   = dict()
        self._cb_lock     = threading.RLock()
        self._terminate   = threading.Event()
        self._closed      = False
        self._rec_id      = 0       # used for session recording

        for m in rpt.UMGR_METRICS:
            self._callbacks[m] = dict()

        cfg = ru.read_json("%s/configs/umgr_%s.json" \
                % (os.path.dirname(__file__),
                   os.environ.get('RADICAL_PILOT_UMGR_CFG', 'default')))

        if scheduler:
            # overwrite the scheduler from the config file
            cfg['scheduler'] = scheduler

        if not cfg.get('scheduler'):
            # set default scheduler if needed
            cfg['scheduler'] = rpus.SCHEDULER_DEFAULT

        assert(cfg['db_poll_sleeptime']), 'db_poll_sleeptime not configured'

        # initialize the base class (with no intent to fork)
        self._uid    = ru.generate_id('umgr')
        cfg['owner'] = self.uid
        rpu.Component.__init__(self, cfg, session)
        self.start(spawn=False)
        self._log.info('started umgr %s', self._uid)

        # only now we have a logger... :/
        self._rep.info('<<create unit manager')

        # The output queue is used to forward submitted units to the
        # scheduling component.
        self.register_output(rps.UMGR_SCHEDULING_PENDING, 
                             rpc.UMGR_SCHEDULING_QUEUE)

        # the umgr will also collect units from the agent again, for output
        # staging and finalization
        self.register_output(rps.UMGR_STAGING_OUTPUT_PENDING, 
                             rpc.UMGR_STAGING_OUTPUT_QUEUE)

        # register the state notification pull cb
        # FIXME: this should be a tailing cursor in the update worker
        self.register_timed_cb(self._state_pull_cb,
                               timer=self._cfg['db_poll_sleeptime'])

        # register callback which pulls units back from agent
        # FIXME: this should be a tailing cursor in the update worker
        self.register_timed_cb(self._unit_pull_cb,
                               timer=self._cfg['db_poll_sleeptime'])

        # also listen to the state pubsub for unit state changes
        self.register_subscriber(rpc.STATE_PUBSUB, self._state_sub_cb)

        # let session know we exist
        self._session._register_umgr(self)

        self._prof.prof('setup_done', uid=self._uid)
        self._rep.ok('>>ok\n')


    # --------------------------------------------------------------------------
    # 
    def initialize_common(self):

        # the manager must not carry bridge and component handles across forks
        ru.atfork(self._atfork_prepare, self._atfork_parent, self._atfork_child)


    # --------------------------------------------------------------------------
    #
    def _atfork_prepare(self): pass
    def _atfork_parent(self) : pass
    def _atfork_child(self)  : 
        self._bridges    = dict()
        self._components = dict()


    # --------------------------------------------------------------------------
    # 
    def finalize_parent(self):

        # terminate umgr components
        for c in self._components:
            c.stop()
            c.join()

        # terminate umgr bridges
        for b in self._bridges:
            b.stop()
            b.join()


    # --------------------------------------------------------------------------
    #
    def close(self):
        """
        Shut down the UnitManager, and all umgr components.
        """

        # we do not cancel units at this point, in case any component or pilot
        # wants to continue to progress unit states, which should indeed be
        # independent from the umgr life cycle.

        if self._closed:
            return

        self._terminate.set()
        self.stop()

        self._rep.info('<<close unit manager')

        # we don't want any callback invokations during shutdown
        # FIXME: really?
        with self._cb_lock:
            self._callbacks = dict()
            for m in rpt.UMGR_METRICS:
                self._callbacks[m] = dict()

        self._log.info("Closed UnitManager %s." % self._uid)

        self._closed = True
        self._rep.ok('>>ok\n')


    # --------------------------------------------------------------------------
    #
    def is_valid(self, term=True):

        # don't check during termination
        if self._closed:
            return True

        return super(UnitManager, self).is_valid(term)


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
    def _pilot_state_cb(self, pilot, state):

        if self._terminate.is_set():
            return False

        # we register this callback for pilots added to this umgr.  It will
        # specifically look out for pilots which complete, and will make sure
        # that all units are pulled back into umgr control if that happens
        # prematurely.
        #
        # If we find units which have not completed the agent part of the unit
        # state model, we declare them FAILED.  If they can be restarted, we
        # resubmit an identical unit, which then will get a new unit ID.  This
        # avoids state model confusion (the state model is right now expected to
        # be linear), but is not intuitive for the application (FIXME).
        #
        # FIXME: there is a race with the umgr scheduler which may, just now,
        #        and before being notified about the pilot's demise, send new
        #        units to the pilot.

        # we only look into pilot states when the umgr is still active
        # FIXME: note that there is a race in that the umgr can be closed while
        #        we are in the cb.
        # FIXME: should is_valid be used?  Either way, `self._closed` is not an
        #        `mt.Event`!
        if self._closed:
            self._log.debug('umgr closed, ignore pilot state (%s: %s)', 
                            pilot.uid, pilot.state)
            return True


        if state in rps.FINAL:

            self._log.debug('pilot %s is final - pull units', pilot.uid)

            unit_cursor = self.session._dbs._c.find({
                'type'    : 'unit',
                'pilot'   : pilot.uid,
                'umgr'    : self.uid,
                'control' : {'$in' : ['agent_pending', 'agent']}})

            if not unit_cursor.count():
                units = list()
            else:
                units = list(unit_cursor)

            self._log.debug("units pulled: %3d (pilot dead)", len(units))

            if not units:
                return True

            # update the units to avoid pulling them again next time.
            # NOTE:  this needs not locking with the unit pulling in the
            #        _unit_pull_cb, as that will only pull umgr_pending 
            #        units.
            uids = [unit['uid'] for unit in units]

            self._session._dbs._c.update({'type'  : 'unit',
                                          'uid'   : {'$in'     : uids}},
                                         {'$set'  : {'control' : 'umgr'}},
                                         multi=True)
            to_restart = list()
            for unit in units:

                unit['state'] = rps.FAILED
                if not unit['description'].get('restartable'):
                    self._log.debug('unit %s not restartable', unit['uid'])
                    continue

                self._log.debug('unit %s is  restartable', unit['uid'])
                unit['restarted'] = True
                ud = rpcud.ComputeUnitDescription(unit['description'])
                to_restart.append(ud)
                # FIXME: increment some restart counter in the description?
                # FIXME: reference the resulting new uid in the old unit.

            if to_restart and not self._closed:
                self._log.debug('restart %s units', len(to_restart))
                restarted = self.submit_units(to_restart)
                for u in restarted:
                    self._log.debug('restart unit %s', u.uid)

            # final units are not pushed
            self.advance(units, publish=True, push=False)

            return True


    # --------------------------------------------------------------------------
    #
    def _state_pull_cb(self):

        if self._terminate.is_set():
            return False

        # pull all unit states from the DB, and compare to the states we know
        # about.  If any state changed, update the unit instance and issue
        # notification callbacks as needed.  Do not advance the state (again).
        # FIXME: we also pull for dead units.  That is not efficient...
        # FIXME: this needs to be converted into a tailed cursor in the update
        #        worker
        units  = self._session._dbs.get_units(umgr_uid=self.uid)

        for unit in units:
            if not self._update_unit(unit, publish=True, advance=False):
                return False

        return True


    # --------------------------------------------------------------------------
    #
    def _unit_pull_cb(self):

        if self._terminate.is_set():
            return False

        # pull units from the agent which are about to get back
        # under umgr control, and push them into the respective queues
        # FIXME: this should also be based on a tailed cursor
        # FIXME: Unfortunately, 'find_and_modify' is not bulkable, so we have
        #        to use 'find'.  To avoid finding the same units over and over 
        #        again, we update the 'control' field *before* running the next
        #        find -- so we do it right here.
        unit_cursor = self.session._dbs._c.find({'type'    : 'unit',
                                                 'umgr'    : self.uid,
                                                 'control' : 'umgr_pending'})

        if not unit_cursor.count():
            # no units whatsoever...
            self._log.info("units pulled:    0")
            return True  # this is not an error

        # update the units to avoid pulling them again next time.
        units = list(unit_cursor)
        uids  = [unit['uid'] for unit in units]

        self._session._dbs._c.update({'type'  : 'unit',
                                      'uid'   : {'$in'     : uids}},
                                     {'$set'  : {'control' : 'umgr'}},
                                     multi=True)

        self._log.info("units pulled: %4d", len(units))
        self._prof.prof('get', msg="bulk size: %d" % len(units), uid=self.uid)
        for unit in units:

            # we need to make sure to have the correct state:
            uid = unit['uid']
            self._prof.prof('get', uid=uid)

            old = unit['state']
            new = rps._unit_state_collapse(unit['states'])

            if old != new:
                self._log.debug("unit  pulled %s: %s / %s", uid, old, new)

            unit['state']   = new
            unit['control'] = 'umgr'

        # now we really own the CUs, and can start working on them (ie. push
        # them into the pipeline).  We don't record state transition profile
        # events though - the transition has already happened.
        self.advance(units, publish=True, push=True, prof=False)

        return True


    # --------------------------------------------------------------------------
    #
    def _state_sub_cb(self, topic, msg):

        if self._terminate.is_set():
            return False

        cmd = msg.get('cmd')
        arg = msg.get('arg')

        if cmd != 'update':
            self._log.debug('ignore state cb msg with cmd %s', cmd)
            return True

        if isinstance(arg, list): things =  arg
        else                    : things = [arg]

        for thing in things:

            if thing.get('type') == 'unit':

                self._log.debug('umgr state cb for unit: %s', thing['uid'])

                # we got the state update from the state callback - don't
                # publish it again
                self._update_unit(thing, publish=False, advance=False)

            else:

                self._log.debug('umgr state cb ignores %s/%s', thing.get('uid'),
                        thing.get('state'))

        return True


    # --------------------------------------------------------------------------
    #
    def _update_unit(self, unit_dict, publish=False, advance=False):

        # FIXME: this is breaking the bulk!

        uid = unit_dict['uid']

        with self._units_lock:

            # we don't care about units we don't know
            if uid not in self._units:
                return True

            # only update on state changes
            current = self._units[uid].state
            target  = unit_dict['state']
            if current == target:
                return True

            target, passed = rps._unit_state_progress(uid, current, target)

            if target in [rps.CANCELED, rps.FAILED]:
                # don't replay intermediate states
                passed = passed[-1:]

            for s in passed:
                unit_dict['state'] = s
                self._units[uid]._update(unit_dict)

                if advance:
                    self.advance(unit_dict, s, publish=publish, push=False,
                                 prof=False)

            return True


    # --------------------------------------------------------------------------
    #
    def _call_unit_callbacks(self, unit_obj, state):

        with self._cb_lock:
            for cb_name, cb_val in self._callbacks[rpt.UNIT_STATE].iteritems():

                self._log.debug('%s calls state cb %s for %s', 
                                self.uid, cb_name, unit_obj.uid)

                cb      = cb_val['cb']
                cb_data = cb_val['cb_data']

                if cb_data: cb(unit_obj, state, cb_data)
                else      : cb(unit_obj, state)


    # --------------------------------------------------------------------------
    #
    # FIXME: this needs to go to the scheduler
    def _default_wait_queue_size_cb(self, umgr, wait_queue_size):
        # FIXME: this needs to come from the scheduler?

        if self._terminate.is_set():
            return False

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

        self.is_valid()

        if not isinstance(pilots, list):
            pilots = [pilots]

        if len(pilots) == 0:
            raise ValueError('cannot add no pilots')

        self._rep.info('<<add %d pilot(s)' % len(pilots))

        with self._pilots_lock:

            # sanity check, and keep pilots around for inspection
            for pilot in pilots:
                pid = pilot.uid
                if pid in self._pilots:
                    raise ValueError('pilot %s already added' % pid)
                self._pilots[pid] = pilot

                # sinscribe for state updates
                pilot.register_callback(self._pilot_state_cb)

        pilot_docs = [pilot.as_dict() for pilot in pilots]

        # publish to the command channel for the scheduler to pick up
        self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'add_pilots',
                                          'arg' : {'pilots': pilot_docs,
                                                   'umgr'  : self.uid}})
        self._rep.ok('>>ok\n')


    # --------------------------------------------------------------------------
    #
    def list_pilots(self):
        """
        Lists the UIDs of the pilots currently associated with the unit manager.

        **Returns:**
              * A list of :class:`radical.pilot.ComputePilot` UIDs [`string`].
        """

        self.is_valid()

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

        self.is_valid()

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
              If `False` (the default), then non-final units will be canceled.
        """

        # TODO: Implement 'drain'.
        # NOTE: the actual removal of pilots from the scheduler is asynchron!

        if drain:
            raise RuntimeError("'drain' is not yet implemented")

        self.is_valid()

        if not isinstance(pilot_ids, list):
            pilot_ids = [pilot_ids]

        if len(pilot_ids) == 0:
            raise ValueError('cannot remove no pilots')

        self._rep.info('<<add %d pilot(s)' % len(pilot_ids))

        with self._pilots_lock:

            # sanity check, and keep pilots around for inspection
            for pid in pilot_ids:
                if pid not in self._pilots:
                    raise ValueError('pilot %s not added' % pid)
                del(self._pilots[pid])

        # publish to the command channel for the scheduler to pick up
        self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'remove_pilots',
                                          'arg' : {'pids'  : pilot_ids, 
                                                   'umgr'  : self.uid}})
        self._rep.ok('>>ok\n')


    # --------------------------------------------------------------------------
    #
    def list_units(self):
        """
        Returns the UIDs of the :class:`radical.pilot.ComputeUnit` managed by
        this unit manager.

        **Returns:**
              * A list of :class:`radical.pilot.ComputeUnit` UIDs [`string`].
        """

        self.is_valid()

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

        self.is_valid()

        ret_list = True
        if not isinstance(descriptions, list):
            ret_list     = False
            descriptions = [descriptions]

        if len(descriptions) == 0:
            raise ValueError('cannot submit no unit descriptions')

        self._rep.info('<<submit %d unit(s)\n\t' % len(descriptions))

        # we return a list of compute units
        units = list()
        for ud in descriptions:

            if not ud.executable:
                raise ValueError('compute unit executable must be defined')

            unit = ComputeUnit.create(umgr=self, descr=ud)
            units.append(unit)

            # keep units around
            with self._units_lock:
                self._units[unit.uid] = unit

            if self._session._rec:
                ru.write_json(ud.as_dict(), "%s/%s.batch.%03d.json"
                        % (self._session._rec, unit.uid, self._rec_id))

            self._rep.progress()

        if self._session._rec:
            self._rec_id += 1

        # insert units into the database, as a bulk.
        unit_docs = [u.as_dict() for u in units]
        self._session._dbs.insert_units(unit_docs)

        # Only after the insert can we hand the units over to the next
        # components (ie. advance state).
        self.advance(unit_docs, rps.UMGR_SCHEDULING_PENDING, 
                     publish=True, push=True)
        self._rep.ok('>>ok\n')

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

        self.is_valid()

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

        If `uids` is `None`, `wait_units` returns when **all**
        ComputeUnits reach the state defined in `state`.  This may include
        units which have previously terminated or waited upon.

        **Example**::

            # TODO -- add example

        **Arguments:**

            * **uids** [`string` or `list of strings`]
              If uids is set, only the ComputeUnits with the specified
              uids are considered. If uids is `None` (default), all
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

        self.is_valid()

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

        # we simplify state check by waiting for the *earliest* of the given
        # states - if the unit happens to be in any later state, we are sure the
        # earliest has passed as well.
        check_state_val = rps._unit_state_values[rps.FINAL[-1]]
        for state in states:
            check_state_val = min(check_state_val, rps._unit_state_values[state])

        ret_list = True
        if not isinstance(uids, list):
            ret_list = False
            uids = [uids]

        self._rep.info('<<wait for %d unit(s)\n\t' % len(uids))

        start    = time.time()
        to_check = None

        with self._units_lock:
            to_check = [self._units[uid] for uid in uids]

        # We don't want to iterate over all units again and again, as that would
        # duplicate checks on units which were found in matching states.  So we
        # create a list from which we drop the units as we find them in
        # a matching state
        self._rep.idle(mode='start')
        while to_check and not self._terminate.is_set():

            # check timeout
            if timeout and (timeout <= (time.time() - start)):
                self._log.debug ("wait timed out")
                break

            time.sleep (0.1)

            # FIXME: print percentage...
            self._rep.idle()
          # print 'wait units: %s' % [[u.uid, u.state] for u in to_check]

            check_again = list()
            for unit in to_check:

                # we actually don't check if a unit is in a specific (set of)
                # state(s), but rather check if it ever *has been* in any of
                # those states
                if unit.state not in rps.FINAL and \
                    rps._unit_state_values[unit.state] <= check_state_val:
                    # this unit does not match the wait criteria
                    check_again.append(unit)

                else:
                    # stop watching this unit
                    if unit.state in [rps.FAILED]:
                        self._rep.idle(color='error', c='-')
                    elif unit.state in [rps.CANCELED]:
                        self._rep.idle(color='warn', c='*')
                    else:
                        self._rep.idle(color='ok', c='+')

            to_check = check_again

            self.is_valid()

        self._rep.idle(mode='stop')

        if to_check: self._rep.warn('>>timeout\n')
        else       : self._rep.ok(  '>>ok\n')

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

        Note that cancellation of units is *immediate*, i.e. their state is
        immediately set to `CANCELED`, even if some RP component may still
        operate on the units.  Specifically, other state transitions, including
        other final states (`DONE`, `FAILED`) can occur *after* cancellation.
        This is a side effect of an optimization: we consider this 
        acceptable tradeoff in the sense "Oh, that unit was DONE at point of
        cancellation -- ok, we can use the results, sure!".

        If that behavior is not wanted, set the environment variable:

            export RADICAL_PILOT_STRICT_CANCEL=True

        **Arguments:**
            * **uids** [`string` or `list of strings`]: The IDs of the
              compute units objects to cancel.
        """

        self.is_valid()

        if not uids:
            with self._units_lock:
                uids  = self._units.keys()
        else:
            if not isinstance(uids, list):
                uids = [uids]

        # NOTE: We advance all units to cancelled, and send a cancellation
        #       control command.  If that command is picked up *after* some
        #       state progression, we'll see state transitions after cancel.
        #       For non-final states that is not a problem, as it is equivalent
        #       with a state update message race, which our state collapse
        #       mechanism accounts for.  For an eventual non-canceled final
        #       state, we do get an invalid state transition.  That is also
        #       corrected eventually in the state collapse, but the point
        #       remains, that the state model is temporarily violated.  We
        #       consider this a side effect of the fast-cancel optimization.
        #
        #       The env variable 'RADICAL_PILOT_STRICT_CANCEL == True' will
        #       disable this optimization.
        #
        # FIXME: the effect of the env var is not well tested
        if 'RADICAL_PILOT_STRICT_CANCEL' not in os.environ:
            with self._units_lock:
                units = [self._units[uid] for uid  in uids ]
            unit_docs = [unit.as_dict()   for unit in units]
            self.advance(unit_docs, state=rps.CANCELED, publish=True, push=True)

        # we *always* issue the cancellation command to the local components
        self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'cancel_units', 
                                          'arg' : {'uids' : uids,
                                                   'umgr' : self.uid}})

        # we also inform all pilots about the cancelation request
        self._session._dbs.pilot_command(cmd='cancel_units', arg={'uids':uids})

        # In the default case of calling 'advance' above, we just set the state,
        # so we *know* units are canceled.  But we nevertheless wait until that
        # state progression trickled through, so that the application will see
        # the same state on unit inspection.
        self.wait_units(uids=uids)


    # --------------------------------------------------------------------------
    #
    def register_callback(self, cb, metric=rpt.UNIT_STATE, cb_data=None):
        """
        Registers a new callback function with the UnitManager.  Manager-level
        callbacks get called if the specified metric changes.  The default
        metric `UNIT_STATE` fires the callback if any of the ComputeUnits
        managed by the PilotManager change their state.

        All callback functions need to have the same signature::

            def cb(obj, value, cb_data)

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
            raise ValueError ("Metric '%s' not available on the umgr" % metric)

        with self._cb_lock:
            cb_name = cb.__name__
            self._callbacks[metric][cb_name] = {'cb'      : cb, 
                                                'cb_data' : cb_data}


    # --------------------------------------------------------------------------
    #
    def unregister_callback(self, cb=None, metric=None):


        if metric and metric not in rpt.UMGR_METRICS :
            raise ValueError ("Metric '%s' not available on the umgr" % metric)

        if not metric:
            metrics = rpt.UMGR_METRICS
        elif isinstance(metric, list):
            metrics = metric
        else:
            metrics = [metric]

        with self._cb_lock:

            for metric in metrics:

                if cb:
                    to_delete = [cb.__name__]
                else:
                    to_delete = self._callbacks[metric].keys()

                for cb_name in to_delete:

                    if cb_name not in self._callbacks[metric]:
                        raise ValueError("Callback %s not registered" % cb_name)

                    del(self._callbacks[metric][cb_name])


# ------------------------------------------------------------------------------

