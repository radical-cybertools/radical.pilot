
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import time
import threading as mt

import radical.utils as ru

from . import utils     as rpu
from . import states    as rps
from . import constants as rpc

from . import compute_unit_description as rpcud


# bulk callbacks are implemented, but are currently not used nor exposed.
_USE_BULK_CB = False
if os.environ.get('RADICAL_PILOT_BULK_CB', '').lower() in ['true', 'yes', '1']:
    _USE_BULK_CB = True


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
        um = rp.UnitManager(session=session, scheduler=rp.SCHEDULER_ROUND_ROBIN)
        um.add_pilot(p1)
        um.submit_units(compute_units)


    The unit manager can issue notification on unit state changes.  Whenever
    state notification arrives, any callback registered for that notification is
    fired.

    NOTE: State notifications can arrive out of order wrt the unit state model!
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, session, cfg='default', scheduler=None):
        """
        Creates a new UnitManager and attaches it to the session.

        **Arguments:**
            * session [:class:`radical.pilot.Session`]:
              The session instance to use.
            * cfg (`dict` or `string`):
              The configuration or name of configuration to use.
            * scheduler (`string`):
              The name of the scheduler plug-in to use.

        **Returns:**
            * A new `UnitManager` object [:class:`radical.pilot.UnitManager`].
        """

        self._pilots      = dict()
        self._pilots_lock = ru.RLock('umgr.pilots_lock')
        self._uids        = list()   # known UIDs
        self._units       = dict()
        self._units_lock  = ru.RLock('umgr.units_lock')
        self._callbacks   = dict()
        self._cb_lock     = ru.RLock('umgr.cb_lock')
        self._terminate   = mt.Event()
        self._closed      = False
        self._rec_id      = 0        # used for session recording
        self._uid         = ru.generate_id('umgr.%(item_counter)04d',
                                           ru.ID_CUSTOM, ns=session.uid)

        for m in rpc.UMGR_METRICS:
            self._callbacks[m] = dict()

        # NOTE: `name` and `cfg` are overloaded, the user cannot point to
        #       a predefined config and amed it at the same time.  This might
        #       be ok for the session, but introduces a minor API inconsistency.
        #
        name = None
        if isinstance(cfg, str):
            name = cfg
            cfg  = None

        cfg           = ru.Config('radical.pilot.umgr', name=name, cfg=cfg)
        cfg.uid       = self._uid
        cfg.owner     = self._uid
        cfg.sid       = session.uid
        cfg.base      = session.base
        cfg.path      = session.path
        cfg.dburl     = session.dburl
        cfg.heartbeat = session.cfg.heartbeat

        if scheduler:
            # overwrite the scheduler from the config file
            cfg.scheduler = scheduler


        rpu.Component.__init__(self, cfg, session=session)
        self.start()

        self._log.info('started umgr %s', self._uid)
        self._rep.info('<<create unit manager')

        # create pmgr bridges and components, use session cmgr for that
        self._cmgr = rpu.ComponentManager(self._cfg)
        self._cmgr.start_bridges()
        self._cmgr.start_components()

        # The output queue is used to forward submitted units to the
        # scheduling component.
        self.register_output(rps.UMGR_SCHEDULING_PENDING,
                             rpc.UMGR_SCHEDULING_QUEUE)

        # the umgr will also collect units from the agent again, for output
        # staging and finalization
        if self._cfg.bridges.umgr_staging_output_queue:
            self._has_sout = True
            self.register_output(rps.UMGR_STAGING_OUTPUT_PENDING,
                                 rpc.UMGR_STAGING_OUTPUT_QUEUE)
        else:
            self._has_sout = False

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
    def initialize(self):

        # the manager must not carry bridge and component handles across forks
        ru.atfork(self._atfork_prepare, self._atfork_parent, self._atfork_child)


    # --------------------------------------------------------------------------
    #
    # EnTK forks, make sure we don't carry traces of children across the fork
    #
    def _atfork_prepare(self): pass
    def _atfork_parent(self) : pass
    def _atfork_child(self)  :
        self._bridges    = dict()
        self._components = dict()


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        self._cmgr.close()


    # --------------------------------------------------------------------------
    #
    def close(self):
        """
        Shut down the UnitManager and all its components.
        """

        # we do not cancel units at this point, in case any component or pilot
        # wants to continue to progress unit states, which should indeed be
        # independent from the umgr life cycle.

        if self._closed:
            return

        self._terminate.set()
        self._rep.info('<<close unit manager')

        # disable callbacks during shutdown
        with self._cb_lock:
            self._callbacks = dict()
            for m in rpc.UMGR_METRICS:
                self._callbacks[m] = dict()

        self._cmgr.close()

        self._log.info("Closed UnitManager %s." % self._uid)

        self._closed = True
        self._rep.ok('>>ok\n')


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


    # --------------------------------------------------------------------------
    #
    def _pilot_state_cb(self, pilots, state=None):

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
        # FIXME: `self._closed` is not an `mt.Event`!
        if self._closed:
            self._log.debug('umgr closed, ignore pilot cb %s',
                            ['%s:%s' % (p.uid, p.state) for p in pilots])
            return True

        if not isinstance(pilots, list):
            pilots = [pilots]

        for pilot in pilots:

            state = pilot.state

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
                    continue

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


        # keep cb registered
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
        units = self._session._dbs.get_units(umgr_uid=self.uid)

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
          # self._log.info("units pulled:    0")
            return True  # this is not an error

        # update the units to avoid pulling them again next time.
        units = list(unit_cursor)
        uids  = [unit['uid'] for unit in units]

        self._log.info("units pulled:    %d", len(uids))

        for unit in units:
            unit['control'] = 'umgr'

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

            unit['state'] = new

        # now we really own the CUs, and can start working on them (ie. push
        # them into the pipeline).

        to_stage    = list()
        to_finalize = list()

        for unit in units:
            # only advance units to data stager if we need data staging
            # = otherwise finalize them right away
            if unit['description'].get('output_staging'):
                to_stage.append(unit)
            else:
                to_finalize.append(unit)

        # don't profile state transitions - those happened in the past
        if to_stage:
            if self._has_sout:
                # normal route: needs data stager
                self.advance(to_stage, publish=True, push=True, prof=False)
            else:
                self._log.error('output staging needed but not available!')
                for unit in to_stage:
                    unit['target_state'] = rps.FAILED
                    to_finalize.append(unit)

        if to_finalize:
            # shortcut, skip the data stager, but fake state transition
            self.advance(to_finalize, state=rps.UMGR_STAGING_OUTPUT,
                                      publish=True, push=False)

            # move to final stata
            for unit in to_finalize:
                unit['state'] = unit['target_state']
            self.advance(to_finalize, publish=True, push=False)

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

        cb_requests = list()

        for thing in things:

            if thing.get('type') == 'unit':

                # we got the state update from the state callback - don't
                # publish it again
                to_notify = self._update_unit(thing, publish=False,
                                              advance=False)
                if to_notify:
                    cb_requests += to_notify
            else:
                self._log.debug('umgr state cb ignores %s/%s', thing.get('uid'),
                        thing.get('state'))

        if cb_requests:
            if _USE_BULK_CB:
                self._bulk_cbs(set([unit for unit,state in cb_requests]))
            else:
                for unit,state in cb_requests:
                    self._unit_cb(unit, state)

        return True


    # --------------------------------------------------------------------------
    #
    def _update_unit(self, unit_dict, publish=False, advance=False):

        uid = unit_dict['uid']

        # return information about needed callback and advance activities, so
        # that we don't break bulks here.
        # note however that individual unit callbacks are still being called on
        # each unit (if any are registered), which can lead to arbitrary,
        # application defined delays.
        to_notify = list()

        with self._units_lock:

            # we don't care about units we don't know
            if uid not in self._units:
                self._log.debug('umgr: unknown: %s', uid)
                return None

            unit = self._units[uid]

            # only update on state changes
            current = unit.state
            target  = unit_dict['state']
            if current == target:
                self._log.debug('umgr: static: %s', uid)
                return None

            target, passed = rps._unit_state_progress(uid, current, target)

            if target in [rps.CANCELED, rps.FAILED]:
                # don't replay intermediate states
                passed = passed[-1:]

            for s in passed:
                unit_dict['state'] = s
                self._units[uid]._update(unit_dict)
                to_notify.append([unit, s])

                # we don't usually advance state at this point, but just keep up
                # with state changes reported from elsewhere
                if advance:
                    self.advance(unit_dict, s, publish=publish, push=False,
                                 prof=False)

            self._log.debug('umgr: notify: %s %s %s', len(to_notify), unit_dict,
                    unit_dict['state'])
            return to_notify


    # --------------------------------------------------------------------------
    #
    def _unit_cb(self, unit, state):

        with self._cb_lock:

            uid      = unit.uid
            cb_dicts = list()
            metric   = rpc.UNIT_STATE

            # get wildcard callbacks
            cb_dicts += self._callbacks[metric].get('*', {}).values()
            cb_dicts += self._callbacks[metric].get(uid, {}).values()

            for cb_dict in cb_dicts:

                cb           = cb_dict['cb']
                cb_data      = cb_dict['cb_data']

                try:
                    if cb_data: cb(unit, state, cb_data)
                    else      : cb(unit, state)
                except:
                    self._log.exception('cb error (%s)', cb.__name__)


    # --------------------------------------------------------------------------
    #
    def _bulk_cbs(self, units,  metrics=None):

        if not metrics: metrics = [rpc.UNIT_STATE]
        else          : metrics = ru.as_list(metrics)

        cbs = dict()  # bulked callbacks to call

        with self._cb_lock:

            for metric in metrics:

                # get wildcard callbacks
                cb_dicts = self._callbacks[metric].get('*')
                for cb_name in cb_dicts:
                    cbs[cb_name] = {'cb'     : cb_dicts[cb_name]['cb'],
                                    'cb_data': cb_dicts[cb_name]['cb_data'],
                                    'units'  : set(units)}

                # add unit specific callbacks if needed
                for unit in units:

                    uid = unit.uid
                    if uid not in self._callbacks[metric]:
                        continue

                    cb_dicts = self._callbacks[metric].get(uid, {})
                    for cb_name in cb_dicts:

                        if cb_name in cbs:
                            cbs[cb_name]['units'].add(unit)
                        else:
                            cbs[cb_name] = {'cb'     : cb_dicts[cb_name]['cb'],
                                            'cb_data': cb_dicts[cb_name]['cb_data'],
                                            'units'  : set([unit])}

            for cb_name in cbs:

                cb      = cbs[cb_name]['cb']
                cb_data = cbs[cb_name]['cb_data']
                objs    = cbs[cb_name]['units']

                if cb_data: cb(list(objs), cb_data)
                else      : cb(list(objs))


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

        if not isinstance(pilots, list):
            pilots = [pilots]

        if len(pilots) == 0:
            raise ValueError('cannot add no pilots')

        with self._pilots_lock:

            # sanity check, and keep pilots around for inspection
            for pilot in pilots:
                pid = pilot.uid
                if pid in self._pilots:
                    raise ValueError('pilot %s already added' % pid)
                self._pilots[pid] = pilot

                # subscribe for state updates
                pilot.register_callback(self._pilot_state_cb)

        pilot_docs = [pilot.as_dict() for pilot in pilots]

        # publish to the command channel for the scheduler to pick up
        self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'add_pilots',
                                          'arg' : {'pilots': pilot_docs,
                                                   'umgr'  : self.uid}})


    # --------------------------------------------------------------------------
    #
    def list_pilots(self):
        """
        Lists the UIDs of the pilots currently associated with the unit manager.

        **Returns:**
              * A list of :class:`radical.pilot.ComputePilot` UIDs [`string`].
        """

        with self._pilots_lock:
            return list(self._pilots.keys())


    # --------------------------------------------------------------------------
    #
    def get_pilots(self):
        """
        Get the pilots instances currently associated with the unit manager.

        **Returns:**
              * A list of :class:`radical.pilot.ComputePilot` instances.
        """

        with self._pilots_lock:
            return list(self._pilots.values())


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

        if not isinstance(pilot_ids, list):
            pilot_ids = [pilot_ids]

        if len(pilot_ids) == 0:
            raise ValueError('cannot remove no pilots')

        with self._pilots_lock:

            # sanity check, and keep pilots around for inspection
            for pid in pilot_ids:
                if pid not in self._pilots:
                    raise ValueError('pilot %s not removed' % pid)
                del(self._pilots[pid])

        # publish to the command channel for the scheduler to pick up
        self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'remove_pilots',
                                          'arg' : {'pids'  : pilot_ids,
                                                   'umgr'  : self.uid}})


    # --------------------------------------------------------------------------
    #
    def list_units(self):
        """
        Returns the UIDs of the :class:`radical.pilot.ComputeUnit` managed by
        this unit manager.

        **Returns:**
              * A list of :class:`radical.pilot.ComputeUnit` UIDs [`string`].
        """

        with self._pilots_lock:
            return list(self._units.keys())


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

        ret_list = True
        if not isinstance(descriptions, list):
            ret_list     = False
            descriptions = [descriptions]

        if len(descriptions) == 0:
            raise ValueError('cannot submit no unit descriptions')

        # we return a list of compute units
        self._rep.progress_tgt(len(descriptions), label='submit')
        units = list()
        for ud in descriptions:

            if not ud.executable:
                raise ValueError('compute unit executable must be defined')

            unit = ComputeUnit(umgr=self, descr=ud)
            units.append(unit)

            # keep units around
            with self._units_lock:
                self._units[unit.uid] = unit

            if self._session._rec:
                ru.write_json(ud.as_dict(), "%s/%s.batch.%03d.json"
                        % (self._session._rec, unit.uid, self._rec_id))

            self._rep.progress()

        self._rep.progress_done()

        if self._session._rec:
            self._rec_id += 1

        # insert units into the database, as a bulk.
        unit_docs = [u.as_dict() for u in units]
        self._session._dbs.insert_units(unit_docs)

        # Only after the insert can we hand the units over to the next
        # components (ie. advance state).
        self.advance(unit_docs, rps.UMGR_SCHEDULING_PENDING,
                     publish=True, push=True)

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

        if not uids:
            with self._units_lock:
                ret = list(self._units.values())
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

        if not uids:
            with self._units_lock:
                uids = list()
                for uid,unit in self._units.items():
                    if unit.state not in rps.FINAL:
                        uids.append(uid)

        if   not state                  : states = rps.FINAL
        elif not isinstance(state, list): states = [state]
        else                            : states =  state

        # we simplify state check by waiting for the *earliest* of the given
        # states - if the unit happens to be in any later state, we are sure the
        # earliest has passed as well.
        check_state_val = rps._unit_state_values[rps.FINAL[-1]]
        for state in states:
            check_state_val = min(check_state_val,
                                  rps._unit_state_values[state])

        ret_list = True
        if not isinstance(uids, list):
            ret_list = False
            uids = [uids]

        start    = time.time()
        to_check = None

        with self._units_lock:
            to_check = [self._units[uid] for uid in uids]

        # We don't want to iterate over all units again and again, as that would
        # duplicate checks on units which were found in matching states.  So we
        # create a list from which we drop the units as we find them in
        # a matching state
        self._rep.progress_tgt(len(to_check), label='wait')
        while to_check and not self._terminate.is_set():

            # check timeout
            if timeout and (timeout <= (time.time() - start)):
                self._log.debug ("wait timed out")
                break

            time.sleep (0.1)

            # FIXME: print percentage...
          # print 'wait units: %s' % [[u.uid, u.state] for u in to_check]

            check_again = list()
            for unit in to_check:

                # we actually don't check if a unit is in a specific (set of)
                # state(s), but rather check if it ever *has been* in any of
                # those states
                if unit.state not in rps.FINAL and \
                    rps._unit_state_values[unit.state] < check_state_val:
                    # this unit does not match the wait criteria
                    check_again.append(unit)

                else:
                    # stop watching this unit
                    if unit.state in [rps.FAILED]:
                        self._rep.progress()  # (color='error', c='-')
                    elif unit.state in [rps.CANCELED]:
                        self._rep.progress()  # (color='warn', c='*')
                    else:
                        self._rep.progress()  # (color='ok', c='+')

            to_check = check_again

        self._rep.progress_done()


        # grab the current states to return
        state = None
        with self._units_lock:
            states = [self._units[uid].state for uid in uids]

        sdict = {state: states.count(state) for state in set(states)}
        for state in sorted(set(states)):
            self._rep.info('\t%-10s: %5d\n' % (state, sdict[state]))

        if to_check: self._rep.warn('>>timeout\n')
        else       : self._rep.ok  ('>>ok\n')

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

        if not uids:
            with self._units_lock:
                uids  = list(self._units.keys())
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
    def register_callback(self, cb, cb_data=None, metric=None, uid=None):
        """
        Registers a new callback function with the UnitManager.  Manager-level
        callbacks get called if the specified metric changes.  The default
        metric `UNIT_STATE` fires the callback if any of the ComputeUnits
        managed by the PilotManager change their state.

        All callback functions need to have the same signature::

            def cb(obj, value)

        where ``object`` is a handle to the object that triggered the callback,
        ``value`` is the metric, and ``data`` is the data provided on
        callback registration..  In the example of `UNIT_STATE` above, the
        object would be the unit in question, and the value would be the new
        state of the unit.

        If 'cb_data' is given, then the 'cb' signature changes to

            def cb(obj, state, cb_data)

        and 'cb_data' are passed unchanged.

        If 'uid' is given, the callback will invoked only for the specified
        unit.


        Available metrics are:

          * `UNIT_STATE`: fires when the state of any of the units which are
            managed by this unit manager instance is changing.  It communicates
            the unit object instance and the units new state.

          * `WAIT_QUEUE_SIZE`: fires when the number of unscheduled units (i.e.
            of units which have not been assigned to a pilot for execution)
            changes.
        """

        # FIXME: the signature should be (self, metrics, cb, cb_data)

        if not metric:
            metric = rpc.UNIT_STATE

        if  metric not in rpc.UMGR_METRICS:
            raise ValueError ("Metric '%s' not available on the umgr" % metric)

        if not uid:
            uid = '*'

        elif uid not in self._units:
            raise ValueError('no such unit %s' % uid)


        with self._cb_lock:
            cb_name = cb.__name__

            if metric not in self._callbacks:
                self._callbacks[metric] = dict()

            if uid not in self._callbacks[metric]:
                self._callbacks[metric][uid] = dict()

            self._callbacks[metric][uid][cb_name] = {'cb'      : cb,
                                                     'cb_data' : cb_data}


    # --------------------------------------------------------------------------
    #
    def unregister_callback(self, cb=None, metrics=None, uid=None):

        if not metrics: metrics = [rpc.UMGR_METRICS]
        else          : metrics = ru.as_list(metrics)

        if not uid:
            uid = '*'

        elif uid not in self._units:
            raise ValueError('no such unit %s' % uid)

        for metric in metrics:
            if metric not in rpc.UMGR_METRICS :
                raise ValueError ("invalid umgr metric '%s'" % metric)

        with self._cb_lock:

            for metric in metrics:

                if metric not in rpc.UMGR_METRICS :
                    raise ValueError("cb metric '%s' unknown" % metric)

                if metric not in self._callbacks:
                    raise ValueError("cb metric '%s' invalid" % metric)

                if uid not in self._callbacks[metric]:
                    raise ValueError("cb target '%s' invalid" % uid)

                if cb:
                    to_delete = [cb.__name__]
                else:
                    to_delete = list(self._callbacks[metric][uid].keys())

                for cb_name in to_delete:

                    if cb_name not in self._callbacks[uid][metric]:
                        raise ValueError("cb %s not registered" % cb_name)

                    del(self._callbacks[uid][metric][cb_name])


    # --------------------------------------------------------------------------
    #
    def check_uid(self, uid):

        # ensure that uid is not yet known
        if uid in self._uids:
            return False
        else:
            self._uids.append(uid)
            return True


# ------------------------------------------------------------------------------

