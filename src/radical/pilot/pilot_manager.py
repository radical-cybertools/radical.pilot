
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import copy
import time
import pprint
import threading

import radical.utils as ru

from .  import utils     as rpu
from .  import states    as rps
from .  import constants as rpc
from .  import types     as rpt


# ------------------------------------------------------------------------------
#
class PilotManager(rpu.Component):
    """
    A PilotManager manages :class:`radical.pilot.ComputePilot` instances that are
    submitted via the :func:`radical.pilot.PilotManager.submit_pilots` method.

    It is possible to attach one or more :ref:`chapter_machconf`
    to a PilotManager to outsource machine specific configuration
    parameters to an external configuration file.

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


    The pilot manager can issue notification on pilot state changes.  Whenever
    state notification arrives, any callback registered for that notification is
    fired.  
    
    NOTE: State notifications can arrive out of order wrt the pilot state model!
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, session):
        """
        Creates a new PilotManager and attaches is to the session.

        **Arguments:**
            * session [:class:`radical.pilot.Session`]:
              The session instance to use.

        **Returns:**
            * A new `PilotManager` object [:class:`radical.pilot.PilotManager`].
        """

        self._components  = None
        self._pilots      = dict()
        self._pilots_lock = threading.RLock()
        self._callbacks   = dict()
        self._pcb_lock    = threading.RLock()
        self._terminate   = threading.Event()
        self._closed      = False
        self._rec_id      = 0       # used for session recording

        for m in rpt.PMGR_METRICS:
            self._callbacks[m] = dict()

        cfg = ru.read_json("%s/configs/pmgr_%s.json" \
                % (os.path.dirname(__file__),
                   os.environ.get('RADICAL_PILOT_PMGR_CFG', 'default')))

        assert(cfg['db_poll_sleeptime'])

        # before we do any further setup, we get the session's ctrl config with
        # bridge addresses, dburl and stuff.
        ru.dict_merge(cfg, session.ctrl_cfg, ru.PRESERVE)

        # initialize the base class (with no intent to fork)
        self._uid    = ru.generate_id('pmgr')
        cfg['owner'] = self.uid
        rpu.Component.__init__(self, cfg, session)
        self.start(spawn=False)

        # only now we have a logger... :/
        self._log.report.info('<<create pilot manager')
        self._prof.prof('create pmgr', uid=self._uid)

        # we can start bridges and components, as needed
        self._controller = rpu.Controller(cfg=self._cfg, session=self.session)

        # merge controller config back into our own config
        ru.dict_merge(self._cfg, self._controller.ctrl_cfg, ru.OVERWRITE)

        # The output queue is used to forward submitted pilots to the
        # launching component.
        self.register_output(rps.PMGR_LAUNCHING_PENDING,
                             rpc.PMGR_LAUNCHING_QUEUE)

        # register the state notification pull cb
        # FIXME: we may want to have the frequency configurable
        # FIXME: this should be a tailing cursor in the update worker
        self.register_timed_cb(self._state_pull_cb, 
                               timer=self._cfg['db_poll_sleeptime'])

        # also listen to the state pubsub for pilot state changes
        self.register_subscriber(rpc.STATE_PUBSUB, self._state_sub_cb)

        # let session know we exist
        self._session._register_pmgr(self)

        self._prof.prof('PMGR setup done', logger=self._log.debug)
        self._log.report.ok('>>ok\n')


    #--------------------------------------------------------------------------
    #
    def close(self, terminate=True):
        """
        Shuts down the PilotManager.

        **Arguments:**
            * **terminate** [`bool`]: cancel non-final pilots if True (default)
        """

        if self._closed:
            return

        self._log.debug("closing %s", self.uid)
        self._log.report.info('<<close pilot manager')

        # we don't want any callback invokations during shutdown
        # FIXME: really?
        with self._pcb_lock:
            for m in rpt.PMGR_METRICS:
                self._callbacks[m] = dict()

        # If terminate is set, we cancel all pilots. 
        if terminate:
            self.cancel_pilots()

        self._terminate.set()
        self._controller.stop()
        self.stop()

        self._session.prof.prof('closed pmgr', uid=self._uid)
        self._log.info("Closed PilotManager %s." % self._uid)

        self._closed = True
        self._log.report.ok('>>ok\n')


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


    #---------------------------------------------------------------------------
    #
    def _state_pull_cb(self):

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
            self._update_pilot(pilot_dict, publish=True)


    # --------------------------------------------------------------------------
    #
    def _state_sub_cb(self, topic, msg):

        cmd = msg.get('cmd')
        arg = msg.get('arg')

        if cmd != 'update':
            self._log.debug('ignore state cb msg with cmd %s', cmd)
            return

        if isinstance(arg, list): things =  arg
        else                    : things = [arg]

        for thing in things:

            if 'type' in thing and thing['type'] == 'pilot':

                # we got the state update from the state callback - don't
                # publish it again
                self._update_pilot(thing, publish=False)


    # --------------------------------------------------------------------------
    #
    def _update_pilot(self, pilot_dict, publish=False, advance=True):

        # FIXME: this is breaking the bulk!

        pid   = pilot_dict['uid']
        state = pilot_dict['state']

        with self._pilots_lock:

            # we don't care about pilots we don't know
            if pid not in self._pilots:
                return False

            # only update on state changes
            current = self._pilots[pid].state
            target  = pilot_dict['state']
            if current == target:
                return

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

                if s in [ACTIVE]:
                    logger.info('pilot %s is %s: %s [%s]', \
                            pid, s, pilot.get('lm_info'), pilot.get('lm_detail')) 



    # --------------------------------------------------------------------------
    #
    def _call_pilot_callbacks(self, pilot_obj, state):

        with self._pcb_lock:
            for cb_name, cb_val in self._callbacks[rpt.PILOT_STATE].iteritems():

                cb      = cb_val['cb']
                cb_data = cb_val['cb_data']
                
              # print ' ~~~ call PCBS: %s -> %s : %s' % (self.uid, self.state, cb_name)

                if cb_data: cb(pilot_obj, state, cb_data)
                else      : cb(pilot_obj, state)
          # print ' ~~~~ done PCBS'


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
        Returns the UIDs of the :class:`radical.pilot.ComputePilots` managed by
        this pilot manager.

        **Returns:**
              * A list of :class:`radical.pilot.ComputePilot` UIDs [`string`].
        """

        if self._closed:
            raise RuntimeError("instance is already closed")

        with self._pilots_lock:
            ret = self._pilots.keys()

        return ret


    # --------------------------------------------------------------------------
    #
    def submit_pilots(self, descriptions):
        """
        Submits on or more :class:`radical.pilot.ComputePilot` instances to the
        pilot manager.

        **Arguments:**
            * **descriptions** [:class:`radical.pilot.ComputePilotDescription`
              or list of :class:`radical.pilot.ComputePilotDescription`]: The
              description of the compute pilot instance(s) to create.

        **Returns:**
              * A list of :class:`radical.pilot.ComputePilot` objects.
        """

        from .compute_pilot import ComputePilot

        if self._closed:
            raise RuntimeError("instance is already closed")

        ret_list = True
        if not isinstance(descriptions, list):
            ret_list     = False
            descriptions = [descriptions]

        if len(descriptions) == 0:
            raise ValueError('cannot submit no pilot descriptions')


        self._log.report.info('<<submit %d pilot(s)\n\t' % len(descriptions))

        # create the pilot instance
        pilots     = list()
        pilot_docs = list()
        for pd in descriptions :

            if not pd.runtime:
                raise ValueError('pilot runtime must be defined')

            if pd.runtime <= 0:
                raise ValueError('pilot runtime must be positive')

            if not pd.cores:
                raise ValueError('pilot core size must be defined')

            if not pd.resource:
                raise ValueError('pilot target resource must be defined')

            pilot = ComputePilot(pmgr=self, descr=pd)
            pilots.append(pilot)
            pilot_doc = pilot.as_dict()
            pilot_docs.append(pilot_doc)

            # keep pilots around
            with self._pilots_lock:
                self._pilots[pilot.uid] = pilot

            if self._session._rec:
                ru.write_json(pd.as_dict(), "%s/%s.batch.%03d.json" \
                        % (self._session._rec, pilot.uid, self._rec_id))
            self._log.report.progress()

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

        # Only after the insert can we hand the pilots over to the next
        # components (ie. advance state).
        for pd in pilot_docs:
            pd['state'] = rps.PMGR_LAUNCHING_PENDING
            self._update_pilot(pd, advance=False)
        self.advance(pilot_docs, publish=True, push=True)

        self._log.report.ok('>>ok\n')

        if ret_list: return pilots
        else       : return pilots[0]


    # --------------------------------------------------------------------------
    #
    def get_pilots(self, uids=None):
        """Returns one or more compute pilots identified by their IDs.

        **Arguments:**
            * **uids** [`string` or `list of strings`]: The IDs of the
              compute pilot objects to return.

        **Returns:**
              * A list of :class:`radical.pilot.ComputePilot` objects.
        """
        
        if self._closed:
            raise RuntimeError("instance is already closed")

        if not uids:
            with self._pilots_lock:
                ret = self._pilots.values()
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
        Returns when one or more :class:`radical.pilot.ComputePilots` reach a
        specific state.

        If `pilot_uids` is `None`, `wait_pilots` returns when **all**
        ComputePilots reach the state defined in `state`.  This may include
        pilots which have previously terminated or waited upon.

        **Example**::

            # TODO -- add example

        **Arguments:**

            * **pilot_uids** [`string` or `list of strings`]
              If pilot_uids is set, only the ComputePilots with the specified
              uids are considered. If pilot_uids is `None` (default), all
              ComputePilots are considered.

            * **state** [`string`]
              The state that ComputePilots have to reach in order for the call
              to return.

              By default `wait_pilots` waits for the ComputePilots to
              reach a terminal state, which can be one of the following:

              * :data:`radical.pilot.rps.DONE`
              * :data:`radical.pilot.rps.FAILED`
              * :data:`radical.pilot.rps.CANCELED`

            * **timeout** [`float`]
              Timeout in seconds before the call returns regardless of Pilot
              state changes. The default value **None** waits forever.
        """

        if self._closed:
            raise RuntimeError("instance is already losed")

        if not uids:
            with self._pilots_lock:
                uids = list()
                for uid,pilot in self._pilots.iteritems():
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

        self._log.report.info('<<wait for %d pilot(s)\n\t' % len(uids))

        start    = time.time()
        to_check = None

        with self._pilots_lock:

            for uid in uids:
                if uid not in self._pilots:
                    raise ValueError('pilot %s not known' % uid)

            to_check = [self._pilots[uid] for uid in uids]

        # We don't want to iterate over all pilots again and again, as that would
        # duplicate checks on pilots which were found in matching states.  So we
        # create a list from which we drop the pilots as we find them in
        # a matching state
        self._log.report.idle(mode='start')
        while to_check and not self._terminate.is_set():

            self._log.report.idle()

            to_check = [pilot for pilot in to_check \
                               if pilot.state not in states and \
                                  pilot.state not in rps.FINAL]

            if to_check:

                if timeout and (timeout <= (time.time() - start)):
                    self._log.debug ("wait timed out")
                    break

                time.sleep (0.1)


        self._log.report.idle(mode='stop')

        if to_check: self._log.report.warn('>>timeout\n')
        else       : self._log.report.ok(  '>>ok\n')

        # grab the current states to return
        state = None
        with self._pilots_lock:
            states = [self._pilots[uid].state for uid in uids]

        # done waiting
        if ret_list: return states
        else       : return states[0]


    # --------------------------------------------------------------------------
    #
    def cancel_pilots(self, uids=None):
        """
        Cancel one or more :class:`radical.pilot.ComputePilots`.

        **Arguments:**
            * **uids** [`string` or `list of strings`]: The IDs of the
              compute pilot objects to cancel.
        """
        if self._closed:
            raise RuntimeError("instance is already closed")

        if not uids:
            with self._pilots_lock:
                uids = self._pilots.keys()

        if not isinstance(uids, list):
            uids = [uids]

        with self._pilots_lock:
            for uid in uids:
                if uid not in self._pilots:
                    raise ValueError('pilot %s not known' % uid)


        self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'cancel_pilots', 
                                          'arg' : {'pmgr' : self.uid,
                                                   'uids' : uids}})
        self.wait_pilots(uids=uids)


    # --------------------------------------------------------------------------
    #
    def register_callback(self, cb, metric=rpt.PILOT_STATE, cb_data=None):
        """
        Registers a new callback function with the PilotManager.  Manager-level
        callbacks get called if the specified metric changes.  The default
        metric `PILOT_STATE` fires the callback if any of the ComputePilots
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

        if metric not in rpt.PMGR_METRICS :
            raise ValueError ("Metric '%s' is not available on the pilot manager" % metric)

        with self._pcb_lock:
            cb_name = cb.__name__
            self._callbacks[metric][cb_name] = {'cb'      : cb, 
                                                'cb_data' : cb_data}


    # --------------------------------------------------------------------------
    #
    def unregister_callback(self, cb, metric=rpt.PILOT_STATE):

        if metric and metric not in rpt.UMGR_METRICS :
            raise ValueError ("Metric '%s' is not available on the pilot manager" % metric)

        if not metric:
            metrics = rpt.PMGR_METRICS
        elif isinstance(metric, list):
            metrics =  metric
        else:
            metrics = [metric]

        with self._pcb_lock:

            for metric in metrics:

                if cb:
                    to_delete = [cb.__name__]
                else:
                    to_delete = self._callbacks[metric].keys()

                for cb_name in to_delete:

                    if cb_name not in self._callbacks[metric]:
                        raise ValueError("Callback '%s' is not registered" % cb_name)

                    del(self._callbacks[metric][cb_name])


# ------------------------------------------------------------------------------

