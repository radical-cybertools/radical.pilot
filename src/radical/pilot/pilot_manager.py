
# pylint: disable=protected-access

__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import time
import threading as mt

import radical.utils as ru

from .  import utils     as rpu
from .  import states    as rps
from .  import constants as rpc

from .staging_directives import expand_staging_directives


# bulk callbacks are implemented, but are currently not used nor exposed.
_USE_BULK_CB = False
if os.environ.get('RADICAL_PILOT_BULK_CB', '').lower() in ['true', 'yes', '1']:
    _USE_BULK_CB = True


# ------------------------------------------------------------------------------
#
class PilotManager(rpu.ClientComponent):
    """Manage Pilot instances.

    A PilotManager manages :class:`rp.Pilot` instances that are
    submitted via the :func:`radical.pilot.PilotManager.submit_pilots` method.
    It is possible to attach one or more
    :ref:`HPC resources </tutorials/configuration.ipynb#Platform-description>`
    to a PilotManager to outsource machine specific configuration parameters
    to an external configuration file.

    Example::

        s = rp.Session()

        pm = rp.PilotManager(session=s)

        pd = rp.PilotDescription()
        pd.resource = "futuregrid.alamo"
        pd.cpus = 16

        p1 = pm.submit_pilots(pd)  # create first  pilot with 16 cores
        p2 = pm.submit_pilots(pd)  # create second pilot with 16 cores

        # Create a workload of 128 '/bin/sleep' tasks
        tasks = []
        for task_count in range(0, 128):
            t = rp.TaskDescription()
            t.executable = "/bin/sleep"
            t.arguments = ['60']
            tasks.append(t)

        # Combine the two pilots, the workload and a scheduler via
        # a TaskManager.
        tm = rp.TaskManager(session=session, scheduler=rp.SCHEDULER_ROUND_ROBIN)
        tm.add_pilot(p1)
        tm.submit_tasks(tasks)


    The pilot manager can issue notification on pilot state changes.  Whenever
    state notification arrives, any callback registered for that notification is
    fired.

    Note:
        State notifications can arrive out of order wrt the pilot state model!

    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, session, cfg='default'):
        """Creates a new PilotManager and attaches is to the session.

        Arguments:
            session (rp.Session): The session instance to use.
            uid (str): ID for pilot manager, to be used for reconnect
            cfg (dict, str): The configuration or name of configuration to use.

        Returns:
            rp.PilotManager: A new `PilotManager` object.

        """

        assert session._role == session._PRIMARY, 'pmgr needs primary session'

        # initialize the base class (with no intent to fork)
        self._uid         = ru.generate_id('pmgr.%(item_counter)04d',
                                            ru.ID_CUSTOM, ns=session.uid)

        self._uids        = list()   # known UIDs
        self._pilots      = dict()
        self._pilots_lock = mt.RLock()
        self._callbacks   = dict()
        self._pcb_lock    = mt.RLock()
        self._terminate   = mt.Event()
        self._closed      = False

        for m in rpc.PMGR_METRICS:
            self._callbacks[m] = dict()

        # NOTE: `name` and `cfg` are overloaded, the user cannot point to
        #       a predefined config and amed it at the same time.  This might
        #       be ok for the session, but introduces a minor API inconsistency.
        #
        name = None
        if isinstance(cfg, str):
            name = cfg
            cfg  = None

        cfg                = ru.Config('radical.pilot.pmgr', name=name, cfg=cfg)
        cfg.uid            = self._uid
        cfg.owner          = self._uid
        cfg.sid            = session.uid
        cfg.path           = session.path
        cfg.reg_addr       = session.reg_addr
        cfg.heartbeat      = session.cfg.heartbeat
        cfg.client_sandbox = session._get_client_sandbox()

        super().__init__(cfg, session=session)
        self.start()

        self._log.info('started pmgr %s', self._uid)

        self._rep = self._session._get_reporter(name=self._uid)
        self._rep.info('<<create pilot manager')

        # create pmgr bridges and components, use session cmgr for that
        self._cmgr = rpu.ComponentManager(cfg.sid, cfg.reg_addr, self._uid)
        self._cmgr.start_bridges(self._cfg.bridges)
        self._cmgr.start_components(self._cfg.components)

        self._session._register_pmgr(self)

        # The output queue is used to forward submitted pilots to the
        # launching component.
        self.register_output(rps.PMGR_LAUNCHING_PENDING,
                             rpc.PMGR_LAUNCHING_QUEUE)

        self._stager = rpu.StagingHelper(self._log)

        # register the state notification pull cb and hb pull cb
        # FIXME: we may want to have the frequency configurable
        # FIXME: this should be a tailing cursor in the update worker
        self.register_timed_cb(self._pilot_heartbeat_cb,
                               timer=self._cfg['db_poll_sleeptime'])

        # also listen to the state pubsub for pilot state changes
        self.register_subscriber(rpc.STATE_PUBSUB, self._state_sub_cb)

        # let session know we exist
        self._session._register_pmgr(self)

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
        self._fail_missing_pilots()


    # --------------------------------------------------------------------------
    #
    def close(self, terminate=True):
        """Shut down the PilotManager and all its components.

        Arguments:
            terminate (bool): cancel non-final pilots if True (default)

                Note:
                    Pilots cannot be reconnected to after termination.

        """

        if self._closed:
            return

        self._rep.info('<<close pilot manager')

        # disable callbacks during shutdown
        # FIXME: really?
        with self._pcb_lock:
            for m in rpc.PMGR_METRICS:
                self._callbacks[m] = dict()

        # If terminate is set, kill all pilots.
        if terminate:

            # skip reporting for `wait_pilots`
            is_rep_enabled = self._rep._enabled
            self._rep._enabled = False
            self.cancel_pilots(_timeout=1)
            self._rep._enabled = is_rep_enabled

            self.kill_pilots(_timeout=10)

        self._terminate.set()
        self._cmgr.close()

        self._log.info("Closed PilotManager %s." % self._uid)

        self._closed = True
        self._rep.ok('>>ok\n')

        self.dump()

        super().close()


    # --------------------------------------------------------------------------
    #
    def dump(self, name=None):

        # dump json
        json = self.as_dict()
        json['type']   = 'pmgr'
        json['uid']    = self.uid
        json['pilots'] = [pilot.as_dict() for pilot in self._pilots.values()]

        if name:
            tgt = '%s/%s.%s.json' % (self._session.path, self.uid, name)
        else:
            tgt = '%s/%s.json' % (self._session.path, self.uid)

        ru.write_json(json, tgt)

    # --------------------------------------------------------------------------
    #
    def as_dict(self):
        """Returns a dictionary representation of the PilotManager object."""

        ret = {
            'uid': self.uid,
            'cfg': self.cfg
        }

        return ret


    # --------------------------------------------------------------------------
    #
    def __str__(self):
        """Returns a string representation of the PilotManager object."""

        return str(self.as_dict())


    # --------------------------------------------------------------------------
    #
    def _pilot_heartbeat_cb(self):

        if self._terminate.is_set():
            return False

        # send heartbeat
        self._pilot_send_hb()
        return True


    # --------------------------------------------------------------------------
    #
    def _state_sub_cb(self, topic, msg):

        if self._terminate.is_set():
            return False

        self._log.debug('state event: %s', msg)

        cmd = msg.get('cmd')
        arg = msg.get('arg')

        if cmd != 'update':
            self._log.debug('ignore state cb msg with cmd %s', cmd)
            return True

        if isinstance(arg, list): things =  arg
        else                    : things = [arg]

        for thing in things:

            if 'type' in thing and thing['type'] == 'pilot':

                self._log.debug('state push: %s: %s', thing['uid'],
                                thing['state'])

                # we got the state update from the state callback - don't
                # publish it again
                if not self._update_pilot(thing, publish=False):
                    return False

        return True


    # --------------------------------------------------------------------------
    #
    def control_cb(self, topic, msg):

        if self._terminate.is_set():
            return False

        cmd = msg.get('cmd')
        arg = msg.get('arg')

        self._log.debug_9('got control cmd %s: %s', cmd, arg)

        if cmd == 'pilot_activate':
            pilot = arg['pilot']
            self._update_pilot(pilot, publish=True)

            # store resource json for RA
            fname = '%s/%s.resources.json' % (self._cfg.path, pilot['uid'])
            ru.write_json(fname, pilot['resources'])


    # --------------------------------------------------------------------------
    #
    def _update_pilot(self, pilot_dict, publish=False, advance=True):

        # FIXME: this is breaking the bulk!

        pid   = pilot_dict['uid']
      # state = pilot_dict['state']

        with self._pilots_lock:

            # we don't care about pilots we don't know
            if pid not in self._pilots:
                return   # this is not an error

            # only update on state changes
            current = self._pilots[pid].state
            target  = pilot_dict['state']

            # always update the pilot instance, even if state didn't change
            if current == target:
                self._pilots[pid]._update(pilot_dict)
                return

            target, passed = rps._pilot_state_progress(pid, current, target)
          # self._log.debug('%s current: %s', pid, current)
          # self._log.debug('%s target : %s', pid, target )
          # self._log.debug('%s passed : %s', pid, passed )

            if target in [rps.CANCELED, rps.FAILED]:
                # don't replay intermediate states
                passed = passed[-1:]

            for s in passed:
                self._log.debug('%s advance: %s', pid, s )
                # we got state from either pubsub or DB, so don't publish again.
                # we also don't need to maintain bulks for that reason.
                pilot_dict['state'] = s
                self._pilots[pid]._update(pilot_dict)

                if advance:
                    self.advance(pilot_dict, s, publish=publish, push=False)

                if s in [rps.PMGR_ACTIVE]:
                    self._log.info('pilot %s is %s: %s [%s]', pid, s,
                                    pilot_dict.get('lm_info'),
                                    pilot_dict.get('lm_detail'))


    # --------------------------------------------------------------------------
    #
    def _call_pilot_callbacks(self, pilot):

        state = pilot.state

        with self._pcb_lock:

            for cb_dict in self._callbacks[rpc.PILOT_STATE].values():

                cb      = cb_dict['cb']
                cb_data = cb_dict['cb_data']

              # print ' ~~~ call PCBS: %s -> %s : %s [%s]' \
              #         % (self.uid, pilot.state, cb_name, cb_data)
                self._log.debug('pmgr calls cb %s for %s', pilot.uid, cb)

                if _USE_BULK_CB:
                    if cb_data: cb([pilot], cb_data)
                    else      : cb([pilot])
                else:
                    if cb_data: cb(pilot, state, cb_data)
                    else      : cb(pilot, state)


    # --------------------------------------------------------------------------
    #
    def _pilot_send_hb(self, pid=None):

        self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'pmgr_heartbeat',
                                          'arg' : {'pmgr' : self.uid}})


    # --------------------------------------------------------------------------
    #
    def _pilot_staging_input(self, pid, sds):
        """Run some staging directives for a pilot."""

        for sd in expand_staging_directives(sds):
            self._prof.prof('staging_in_start', uid=pid, msg=sd['uid'])
            self._stager.handle_staging_directive(sd)
            self._prof.prof('staging_in_stop', uid=pid, msg=sd['uid'])


    # --------------------------------------------------------------------------
    #
    def _pilot_staging_output(self, pid, sds):
        """Run some staging directives for a pilot."""

        for sd in expand_staging_directives(sds):
            self._prof.prof('staging_out_start', uid=self.uid, msg=sd['uid'])
            self._stager.handle_staging_directive(sd)
            self._prof.prof('staging_out_stop', uid=self.uid, msg=sd['uid'])


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):
        """str: The unique id."""
        return self._uid


    # --------------------------------------------------------------------------
    #
    def list_pilots(self):
        """Get the UIDs of the managed :class:`rp.Pilots`.

        Returns:
            list[str]: A list of :class:`rp.Pilot` UIDs.

        """

        with self._pilots_lock:
            ret = list(self._pilots.keys())

        return ret


    # --------------------------------------------------------------------------
    #
    def submit_pilots(self, descriptions):
        """Submit one or more `rp.Pilot` instances to the pilot manager.

        Arguments:
            descriptions (rp.PilotDescription | list[rp.PilotDescription]):
                The description of the pilot instance(s) to create.

        Returns:
            list[rp.Pilot]: A list of :class:`rp.Pilot` objects.

        """

        from .pilot import Pilot

        ret_list = True
        if not isinstance(descriptions, list):
            ret_list     = False
            descriptions = [descriptions]

        if len(descriptions) == 0:
            raise ValueError('cannot submit no pilot descriptions')


        self._rep.info('<<submit %d pilot(s)' % len(descriptions))

        # create the pilot instance
        pilots     = list()
        pilot_docs = list()
        for pd in descriptions :

            pilot = Pilot(pmgr=self, descr=pd)
            pilots.append(pilot)
            pilot_doc = pilot.as_dict()
            pilot_docs.append(pilot_doc)

            # keep pilots around
            with self._pilots_lock:
                self._pilots[pilot.uid] = pilot

            if pd.get('nodes'):
                bn = pd.get('backup_nodes', 0)
                if bn:
                    self._rep.plain('\n\t%s   %-20s %6d+%d nodes' %
                                    (pilot.uid, pd['resource'], pd['nodes'], bn))
                else:
                    self._rep.plain('\n\t%s   %-20s %6d nodes' %
                                    (pilot.uid, pd['resource'], pd['nodes']))

            else:
                self._rep.plain('\n\t%s   %-20s %6d cores  %6d gpus' %
                                (pilot.uid, pd['resource'],
                                 pd.get('cores', 0), pd.get('gpus', 0)))

        # initial state advance to 'NEW'
        # FIXME: we should use update_pilot(), but that will not trigger an
        #        advance, since the state did not change.  We would then miss
        #        the profile entry for the advance to NEW.  So we here basically
        #        only trigger the profile entry for NEW.
        self.advance(pilot_docs, state=rps.NEW, publish=False, push=False)

        # immediately send first heartbeat
        for pilot_doc in pilot_docs:
            pid = pilot_doc['uid']
            self._pilot_send_hb(pid)

        # Only after the insert/update can we hand the pilots over to the next
        # components (ie. advance state).
        for pd in pilot_docs:
            pd['state'] = rps.PMGR_LAUNCHING_PENDING
            self._update_pilot(pd, advance=False)
        self.advance(pilot_docs, publish=True, push=True)


        self._rep.ok('>>ok\n')

        if ret_list: return pilots
        else       : return pilots[0]


    # --------------------------------------------------------------------------
    #
    def _reconnect_pilots(self):
        """Restore Pilot info from database.

        When reconnecting, we need to dig information about all pilots from the
        DB for which this pmgr is responsible.
        """

        from .pilot             import Pilot
        from .pilot_description import PilotDescription

      # self.is_valid()

        # FIXME MONGODB
        # pilot_docs = self._session._dbs.get_pilots(pmgr_uid=self.uid)

        # with self._pilots_lock:
        #     for ud in pilot_docs:

        #         descr = PilotDescription(ud['description'])
        #         pilot = Pilot(pmgr=self, descr=descr)

        #         self._pilots[pilot.uid] = pilot


    # --------------------------------------------------------------------------
    #
    def get_pilots(self, uids=None):
        """Returns one or more pilots identified by their IDs.

        Arguments:
            uids (str | list[str]): The IDs of the pilot objects to return.

        Returns:
            list: A list of :class:`rp.Pilot` objects.

        """

        if not uids:
            with self._pilots_lock:
                ret = list(self._pilots.values())
            return ret


        ret_list = True
        if (not isinstance(uids, list)) and (uids is not None):
            ret_list = False
            uids = [uids]

        ret = list()
        with self._pilots_lock:
            for uid in uids:
                if uid not in self._pilots:
                    raise ValueError('pilot %s not known' % uid)
                ret.append(self._pilots[uid])

        if ret_list: return ret
        else       : return ret[0]


    # --------------------------------------------------------------------------
    #
    def wait_pilots(self, uids=None, state=None, timeout=None):
        """Block for state transition.

        Returns when one or more :class:`rp.Pilots` reach a
        specific state.

        If `pilot_uids` is `None`, `wait_pilots` returns when **all**
        Pilots reach the state defined in `state`.  This may include
        pilots which have previously terminated or waited upon.

        Example:
            # TODO -- add example

        Arguments:
            uids (str | list[str], optional): If set, only the Pilots with the
                specified uids are considered. If `None` (default), all
                Pilots are considered.
            state (str | list[str]): The state that Pilots have to reach in order
                for the call to return.

                By default `wait_pilots` waits for the Pilots to
                reach a terminal state, which can be one of the following:
                * :data:`rp.rps.DONE`
                * :data:`rp.rps.FAILED`
                * :data:`rp.rps.CANCELED`
            timeout (float, optional): Timeout in seconds before the call returns
                regardless of Pilot
                state changes. The default value **None** waits forever.

        """

        if not uids:
            with self._pilots_lock:
                uids = list()
                for uid,pilot in self._pilots.items():
                    if pilot.state not in rps.FINAL:
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
            uids     = [uids]

        self._rep.info('<<wait for %d pilot(s)\n\t' % len(uids))

        start    = time.time()
        to_check = None

        with self._pilots_lock:

            for uid in uids:
                if uid not in self._pilots:
                    raise ValueError('pilot %s not known' % uid)

            to_check = [self._pilots[uid] for uid in uids]

        # We don't want to iterate over all pilots again and again, as that
        # would duplicate checks on pilots which were found in matching states.
        # So we create a list from which we drop the pilots as we find them in
        # a matching state
        self._rep.idle(mode='start')
        while to_check and not self._terminate.is_set():

            self._rep.idle()

            to_check = [pilot for pilot in to_check
                               if pilot.state not in states and
                                  pilot.state not in rps.FINAL]

            if to_check:

                if timeout and (timeout <= (time.time() - start)):
                    self._log.debug ("wait timed out")
                    break

            time.sleep (0.1)

        self._rep.idle(mode='stop')

        if to_check: self._rep.warn('>>timeout\n')
        else       : self._rep.ok  ('>>ok\n')

        # grab the current states to return
        state = None
        with self._pilots_lock:
            states = [self._pilots[uid].state for uid in uids]

        # done waiting
        if ret_list: return states
        else       : return states[0]


    # --------------------------------------------------------------------------
    #
    def _fail_missing_pilots(self):
        """Advance remaining pilots to failed state.

        During termination, fail all pilots for which we did not manage to
        obtain a final state - we trust that they'll follow up on their
        cancellation command in due time, if they can
        """

        pass

      # with self._pilots_lock:
      #     for pid in self._pilots:
      #         pilot = self._pilots[pid]
      #         if pilot.state not in rps.FINAL:
      #             self.advance(pilot.as_dict(), rps.FAILED,
      #                          publish=True, push=False)


    # --------------------------------------------------------------------------
    #
    def cancel_pilots(self, uids=None, _timeout=None):
        """Cancel one or more :class:`rp.Pilots`.

        Arguments:
            uids (str | list[str], optional): The IDs of the pilots to cancel.

        """

        if not uids:
            with self._pilots_lock:
                uids = list(self._pilots.keys())

        if not isinstance(uids, list):
            uids = [uids]

        self._log.debug('pilot(s).need(s) cancellation %s', uids)

        # send the cancellation request to the pilots
        self._log.debug('issue cancel_pilots for %s', uids)
        self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'cancel_pilots',
                                          'arg' : {'pmgr' : self.uid,
                                                   'uids' : uids},
                                          'fwd' : True})
        # wait for the cancel to be enacted
        self.wait_pilots(uids=uids, timeout=_timeout)

        # FIXME: only finalize pilots which actually terminated
        with self._pilots_lock:
            for uid in uids:
                if uid not in self._pilots:
                    raise ValueError('pilot %s not known' % uid)


    # --------------------------------------------------------------------------
    #
    def kill_pilots(self, uids=None, _timeout=None):
        """Kill one or more :class:`rp.Pilots`

        Arguments:
            uids (str | list[str], optional): The IDs of the pilots to cancel.

        """

        if not uids:
            with self._pilots_lock:
                uids = list(self._pilots.keys())

        if not isinstance(uids, list):
            uids = [uids]

        with self._pilots_lock:
            for uid in uids:
                if uid not in self._pilots:
                    raise ValueError('pilot %s not known' % uid)

        self._log.debug('pilot(s).need(s) killing %s', uids)

        # inform pmgr.launcher - it will force-kill the pilots
        self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'kill_pilots',
                                          'arg' : {'pmgr' : self.uid,
                                                   'uids' : uids}})

        # wait for the kill to be enacted
        self.wait_pilots(uids=uids, timeout=_timeout)


    # --------------------------------------------------------------------------
    #
    def register_callback(self, cb, cb_data=None, metric=rpc.PILOT_STATE):
        """Registers a new callback function with the PilotManager.

        Manager-level callbacks get called if the specified metric changes.
        The default metric `PILOT_STATE` fires the callback if any of the Pilots
        managed by the PilotManager change their state.

        All callback functions need to have the same signature::

            def cb(obj, value, cb_data)

        where ``object`` is a handle to the object that triggered the callback,
        ``value`` is the metric, and ``data`` is the data provided on
        callback registration..  In the example of `PILOT_STATE` above, the
        object would be the pilot in question, and the value would be the new
        state of the pilot.

        Available metrics are

        * `PILOT_STATE`: fires when the state of any of the pilots which are
            managed by this pilot manager instance is changing.  It communicates
            the pilot object instance and the pilots new state.

        """

        # FIXME: the signature should be (self, metrics, cb, cb_data)

        if metric not in rpc.PMGR_METRICS :
            raise ValueError ("invalid pmgr metric '%s'" % metric)

        with self._pcb_lock:
            cb_id = id(cb)
            self._callbacks[metric][cb_id] = {'cb'      : cb,
                                              'cb_data' : cb_data}


    # --------------------------------------------------------------------------
    #
    def unregister_callback(self, cb, metric=rpc.PILOT_STATE):

        if metric and metric not in rpc.PMGR_METRICS :
            raise ValueError ("invalid pmgr metric '%s'" % metric)

        if not metric:
            metrics = rpc.PMGR_METRICS
        elif isinstance(metric, list):
            metrics =  metric
        else:
            metrics = [metric]

        with self._pcb_lock:

            for metric in metrics:

                if cb:
                    to_delete = [id(cb)]
                else:
                    to_delete = list(self._callbacks[metric].keys())

                for cb_id in to_delete:

                    if cb_id not in self._callbacks[metric]:
                        raise ValueError("unknown callback '%s'" % cb_id)

                    del self._callbacks[metric][cb_id]


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
