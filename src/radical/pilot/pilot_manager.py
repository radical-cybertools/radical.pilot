
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
class PilotManager(rpu.Component):
    '''

    A PilotManager manages :class:`rp.Pilot` instances that are
    submitted via the :func:`radical.pilot.PilotManager.submit_pilots` method.

    It is possible to attach one or more :ref:`chapter_machconf` to a
    PilotManager to outsource machine specific configuration parameters
    to an external configuration file.

    **Example**::

        s = rp.Session(database_url=DBURL)

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

    NOTE: State notifications can arrive out of order wrt the pilot state model!
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, session, uid=None, cfg='default'):
        '''
        Creates a new PilotManager and attaches is to the session.

        **Arguments:**
            * session [:class:`rp.Session`]:
              The session instance to use.
            * uid (`string`):
              ID for pilot manager, to be used for reconnect
            * cfg (`dict` or `string`):
              The configuration or name of configuration to use.

        **Returns:**
            * A new `PilotManager` object [:class:`rp.PilotManager`].
        '''

        assert session.primary, 'pmgr needs primary session'

        # initialize the base class (with no intent to fork)
        if uid:
            self._reconnect = True
            self._uid       = uid
        else:
            self._reconnect = False
            self._uid       = ru.generate_id('pmgr.%(item_counter)04d',
                                             ru.ID_CUSTOM, ns=session.uid)

        self._uids        = list()   # known UIDs
        self._pilots      = dict()
        self._pilots_lock = mt.RLock()
        self._callbacks   = dict()
        self._pcb_lock    = mt.RLock()
        self._terminate   = mt.Event()
        self._closed      = False
        self._rec_id      = 0       # used for session recording

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
        cfg.base           = session.base
        cfg.path           = session.path
        cfg.dburl          = session.dburl
        cfg.heartbeat      = session.cfg.heartbeat
        cfg.client_sandbox = session._get_client_sandbox()

        rpu.Component.__init__(self, cfg, session=session)
        self.start()

        self._log.info('started pmgr %s', self._uid)
        self._rep.info('<<create pilot manager')

        # create pmgr bridges and components, use session cmgr for that
        self._cmgr = rpu.ComponentManager(self._cfg)
        self._cmgr.start_bridges()
        self._cmgr.start_components()

        if self._reconnect:
            self._session._reconnect_pmgr(self)
            self._reconnect_pilots()
        else:
            self._session._register_pmgr(self)

        # The output queue is used to forward submitted pilots to the
        # launching component.
        self.register_output(rps.PMGR_LAUNCHING_PENDING,
                             rpc.PMGR_LAUNCHING_QUEUE)

        # get queue for staging requests
        self._stager_queue = self.get_output_ep(rpc.STAGER_REQUEST_QUEUE)

        # we also listen for completed staging directives
        self.register_subscriber(rpc.STAGER_RESPONSE_PUBSUB, self._staging_ack_cb)
        self._active_sds = dict()
        self._sds_lock   = mt.Lock()

        # register the state notification pull cb and hb pull cb
        # FIXME: we may want to have the frequency configurable
        # FIXME: this should be a tailing cursor in the update worker
        self.register_timed_cb(self._state_pull_cb,
                               timer=self._cfg['db_poll_sleeptime'])
        self.register_timed_cb(self._pilot_heartbeat_cb,
                               timer=self._cfg['db_poll_sleeptime'])

        # also listen to the state pubsub for pilot state changes
        self.register_subscriber(rpc.STATE_PUBSUB, self._state_sub_cb)

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
        """
        Shut down the PilotManager and all its components.

        **Arguments:**
            * **terminate** [`bool`]: cancel non-final pilots if True (default)
              NOTE: pilots cannot be reconnected to after termination
        """

        if self._closed:
            return

        self._rep.info('<<close pilot manager')

        # disable callbacks during shutdown
        # FIXME: really?
        with self._pcb_lock:
            for m in rpc.PMGR_METRICS:
                self._callbacks[m] = dict()

        # If terminate is set, we cancel all pilots.
        if terminate:
            self.cancel_pilots(_timeout=20)
            # if this cancel op fails and the pilots are s till alive after
            # timeout, the pmgr.launcher termination will kill them

        self._terminate.set()
        self._cmgr.close()

        self._log.info("Closed PilotManager %s." % self._uid)

        self._closed = True
        self._rep.ok('>>ok\n')


    # --------------------------------------------------------------------------
    #
    def as_dict(self):
        """
        Returns a dictionary representation of the PilotManager object.
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
        Returns a string representation of the PilotManager object.
        """

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
    def _state_pull_cb(self):

        if self._terminate.is_set():
            return False

        # pull all pilot states from the DB, and compare to the states we know
        # about.  If any state changed, update the known pilot instances and
        # push an update message to the state pubsub.
        # pubsub.
        # FIXME: we also pull for dead pilots.  That is not efficient...
        # FIXME: this needs to be converted into a tailed cursor in the update
        #        worker
        # FIXME: this is a big and frequently invoked lock
        pilot_dicts = self._session._dbs.get_pilots(pmgr_uid=self.uid)


        for pilot_dict in pilot_dicts:
            self._log.debug('state pulled: %s: %s', pilot_dict['uid'],
                                                    pilot_dict['state'])
            if not self._update_pilot(pilot_dict, publish=True):
                return False

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
    def _update_pilot(self, pilot_dict, publish=False, advance=True):

        # FIXME: this is breaking the bulk!

        pid   = pilot_dict['uid']
      # state = pilot_dict['state']

        with self._pilots_lock:

            # we don't care about pilots we don't know
            if pid not in self._pilots:
                return True  # this is not an error

            # only update on state changes
            current = self._pilots[pid].state
            target  = pilot_dict['state']
            if current == target:
                return True

            target, passed = rps._pilot_state_progress(pid, current, target)
          # print '%s current: %s' % (pid, current)
          # print '%s target : %s' % (pid, target )
          # print '%s passed : %s' % (pid, passed )

            if target in [rps.CANCELED, rps.FAILED]:
                # don't replay intermediate states
                passed = passed[-1:]

            for s in passed:
              # print '%s advance: %s' % (pid, s )
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

            return True


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

        self._session._dbs.pilot_command('heartbeat', {'pmgr': self._uid}, pid)


    # --------------------------------------------------------------------------
    #
    def _pilot_staging_input(self, sds):
        '''
        Run some staging directives for a pilot.
        '''

        # add uid, ensure its a list, general cleanup
        sds  = expand_staging_directives(sds)
        uids = [sd['uid'] for sd in sds]

        # prepare to wait for completion
        with self._sds_lock:

            self._active_sds = dict()
            for sd in sds:
                sd['state'] = rps.NEW
                self._active_sds[sd['uid']] = sd

            sd_states = [sd['state'] for sd
                                     in  self._active_sds.values()
                                     if  sd['uid'] in uids]
        # push them out
        self._stager_queue.put(sds)

        while rps.NEW in sd_states:
            time.sleep(1.0)
            with self._sds_lock:
                sd_states = [sd['state'] for sd
                                         in  self._active_sds.values()
                                         if  sd['uid'] in uids]

        if rps.FAILED in sd_states:
            errs = list()
            for uid in self._active_sds:
                if self._active_sds[uid].get('error'):
                    errs.append(self._active_sds[uid]['error'])

            if errs:
                raise RuntimeError('pilot staging failed: %s' % errs)
            else:
                raise RuntimeError('pilot staging failed')


    # --------------------------------------------------------------------------
    #
    def _pilot_staging_output(self, sds):
        '''
        Run some staging directives for a pilot.
        '''

        # add uid, ensure its a list, general cleanup
        sds  = expand_staging_directives(sds)
        uids = [sd['uid'] for sd in sds]

        # prepare to wait for completion
        with self._sds_lock:

            self._active_sds = dict()
            for sd in sds:
                sd['state'] = rps.NEW
                self._active_sds[sd['uid']] = sd

        # push them out
        self._stager_queue.put(sds)

        # wait for their completion
        with self._sds_lock:
            sd_states = [sd['state'] for sd in self._active_sds.values()
                                            if sd['uid'] in uids]
        while rps.NEW in sd_states:
            time.sleep(1.0)
            with self._sds_lock:
                sd_states = [sd['state'] for sd in self._active_sds.values()
                                                if sd['uid'] in uids]

        if rps.FAILED in sd_states:
            errs = list()
            for uid in self._active_sds:
                if self._active_sds[uid].get('error'):
                    errs.append(self._active_sds[uid]['error'])

            if errs:
                raise RuntimeError('pilot staging failed: %s' % errs)
            else:
                raise RuntimeError('pilot staging failed')


    # --------------------------------------------------------------------------
    #
    def _staging_ack_cb(self, topic, msg):
        '''
        update staging directive state information
        '''

        cmd = msg.get('cmd')
        arg = msg.get('arg')

        if cmd == 'staging_result':

            with self._sds_lock:
                for sd in arg['sds']:
                    uid = sd['uid']
                    if uid in self._active_sds:
                        self._active_sds[uid]['state'] = sd['state']
                        self._active_sds[uid]['error'] = sd['error']

        return True


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
    def list_pilots(self):
        """
        Returns the UIDs of the :class:`rp.Pilots` managed by
        this pilot manager.

        **Returns:**
              * A list of :class:`rp.Pilot` UIDs [`string`].
        """

        with self._pilots_lock:
            ret = list(self._pilots.keys())

        return ret


    # --------------------------------------------------------------------------
    #
    def submit_pilots(self, descriptions):
        """
        Submits on or more :class:`rp.Pilot` instances to the
        pilot manager.

        **Arguments:**
            * **descriptions** [:class:`rp.PilotDescription`
              or list of :class:`rp.PilotDescription`]: The
              description of the pilot instance(s) to create.

        **Returns:**
              * A list of :class:`rp.Pilot` objects.
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

            if self._session._rec:
                ru.write_json(pd.as_dict(), "%s/%s.batch.%03d.json"
                        % (self._session._rec, pilot.uid, self._rec_id))

            self._rep.plain('\n\t%s   %-20s %6d cores  %6d gpus' %
                      (pilot.uid, pd['resource'],
                       pd.get('cores', 0), pd.get('gpus', 0)))


        # initial state advance to 'NEW'
        # FIXME: we should use update_pilot(), but that will not trigger an
        #        advance, since the state did not change.  We would then miss
        #        the profile entry for the advance to NEW.  So we here basically
        #        only trigger the profile entry for NEW.
        self.advance(pilot_docs, state=rps.NEW, publish=False, push=False)

        if self._session._rec:
            self._rec_id += 1

        # insert pilots into the database, as a bulk.
        self._session._dbs.insert_pilots(pilot_docs)

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
        '''
        When reconnecting, we need to dig information about all pilots from the
        DB for which this pmgr is responsible.
        '''

        from .pilot             import Pilot
        from .pilot_description import PilotDescription

      # self.is_valid()

        pilot_docs = self._session._dbs.get_pilots(pmgr_uid=self.uid)

        with self._pilots_lock:
            for ud in pilot_docs:

                descr = PilotDescription(ud['description'])
                pilot = Pilot(pmgr=self, descr=descr)

                self._pilots[pilot.uid] = pilot


    # --------------------------------------------------------------------------
    #
    def get_pilots(self, uids=None):
        """Returns one or more pilots identified by their IDs.

        **Arguments:**
            * **uids** [`string` or `list of strings`]: The IDs of the
              pilot objects to return.

        **Returns:**
              * A list of :class:`rp.Pilot` objects.
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
        """
        Returns when one or more :class:`rp.Pilots` reach a
        specific state.

        If `pilot_uids` is `None`, `wait_pilots` returns when **all**
        Pilots reach the state defined in `state`.  This may include
        pilots which have previously terminated or waited upon.

        **Example**::

            # TODO -- add example

        **Arguments:**

            * **pilot_uids** [`string` or `list of strings`]
              If pilot_uids is set, only the Pilots with the specified
              uids are considered. If pilot_uids is `None` (default), all
              Pilots are considered.

            * **state** [`string`]
              The state that Pilots have to reach in order for the call
              to return.

              By default `wait_pilots` waits for the Pilots to
              reach a terminal state, which can be one of the following:

              * :data:`rp.rps.DONE`
              * :data:`rp.rps.FAILED`
              * :data:`rp.rps.CANCELED`

            * **timeout** [`float`]
              Timeout in seconds before the call returns regardless of Pilot
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
        '''
        During termination, fail all pilots for which we did not manage to
        obtain a final state - we trust that they'll follow up on their
        cancellation command in due time, if they can
        '''

        with self._pilots_lock:
            for pid in self._pilots:
                pilot = self._pilots[pid]
                if pilot.state not in rps.FINAL:
                    self.advance(pilot.as_dict(), rps.FAILED,
                                 publish=True, push=False)


    # --------------------------------------------------------------------------
    #
    def cancel_pilots(self, uids=None, _timeout=None):
        """
        Cancel one or more :class:`rp.Pilots`.

        **Arguments:**
            * **uids** [`string` or `list of strings`]: The IDs of the
              pilot objects to cancel.
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

        self._log.debug('pilot(s).need(s) cancellation %s', uids)

        # send the cancelation request to the pilots
        # FIXME: the cancellation request should not go directly to the DB, but
        #        through the DB abstraction layer...
        self._session._dbs.pilot_command('cancel_pilot', [], uids)

        # inform pmgr.launcher - it will force-kill the pilot after some delay
        self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'kill_pilots',
                                          'arg' : {'pmgr' : self.uid,
                                                   'uids' : uids}})

        self.wait_pilots(uids=uids, timeout=_timeout)


    # --------------------------------------------------------------------------
    #
    def register_callback(self, cb, cb_data=None, metric=rpc.PILOT_STATE):
        """
        Registers a new callback function with the PilotManager.  Manager-level
        callbacks get called if the specified metric changes.  The default
        metric `PILOT_STATE` fires the callback if any of the Pilots
        managed by the PilotManager change their state.

        All callback functions need to have the same signature::

            def cb(obj, value, cb_data)

        where ``object`` is a handle to the object that triggered the callback,
        ``value`` is the metric, and ``data`` is the data provided on
        callback registration..  In the example of `PILOT_STATE` above, the
        object would be the pilot in question, and the value would be the new
        state of the pilot.

        Available metrics are:

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

