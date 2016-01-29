
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import sys
import time
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

        from .. import pilot as rp
        
        self._session    = session
        self._cfg        = None
        self._components = None
        self._pilots     = dict()
        self._units      = dict()
        self._callbacks  = dict()
        self._cb_lock    = threading.RLock()
        self._closed     = False
        self._rec_id     = 0       # used for session recording

        # get an ID and initialize logging and profiling
        # FIXME: log and prof are already provided by the base class -- but it
        #        will take a while until we can initialize that, and meanwhile
        #        we use these...
        self._uid  = ru.generate_id('pmgr')
        self._log  = ru.get_logger(self.uid, "%s.%s.log" % (session.uid, self._uid))
        self._prof = rpu.Profiler("%s.%s" % (session.uid, self._uid))

        self._session.prof.prof('create pmgr', uid=self._uid)

        self._log.report.info('<<create unit manager')

        try:
            self._cfg = ru.read_json("%s/configs/pmgr_%s.json" \
                    % (os.path.dirname(__file__),
                       os.environ.get('RADICAL_PILOT_PMGR_CONFIG', 'default')))

            self._cfg['session_id']  = self._session.uid
            self._cfg['mongodb_url'] = self._session._dburl
            self._cfg['owner']       = self._uid

            components = self._cfg.get('components', [])

            # we also need a map from component names to class types
            typemap = {
                rpc.PMGR_LAUNCHING_COMPONENT : rp.pmgr.Launching
                }

            # get addresses from the bridges, and append them to the
            # config, so that we can pass those addresses to the components
            self._cfg['bridge_addresses'] = copy.deepcopy(self._session._bridge_addresses)

            # the bridges are known, we can start to connect the components to them
            self._components = rpu.Component.start_components(components,
                    typemap, self._cfg)

            # initialize the base class
            # FIXME: unique ID
            rpu.Component.__init__(self, 'PilotManager', self._cfg)

            # The command pubsub is always used
            self.declare_publisher('command', rpc.COMMAND_PUBSUB)

            # The output queue is used to forward submitted pilots to the
            # launching component.
            self.declare_output(rps.PMGR_LAUNCHING_PENDING, rpc.PMGR_LAUNCHING_QUEUE)


        except Exception as e:
            self._log.exception("PMGR setup error: %s" % e)
            raise

        self._prof.prof('PMGR setup done', logger=self._log.debug)

        self._log.report.ok('>>ok\n')


    #--------------------------------------------------------------------------
    #
    def close(self, terminate=True):
        """
        Shuts down the PilotManager.

        **Arguments:**
            * **terminate** [`bool`]: If set to True, all active pilots will 
              get canceled (default: False).

        """
        if self._closed:
            raise RuntimeError("instance is already closed")

        self._log.debug("closing %s", self.uid)
        self._log.report.info('<<close pilot manager')

        # If terminate is set, we cancel all pilots. 
        if  terminate :
            self.cancel_pilots()
            self.wait_pilots()

     ## if self._worker:
     ##     self._worker.stop()
     ## TODO: kill components

        self._session.prof.prof('closed pmgr', uid=self._uid)
        self._log.info("Closed PilotManager %s." % str(self._uid))

        self._closed = True
        self._log.report.ok('>>ok\n')


    # --------------------------------------------------------------------------
    #
    def as_dict(self):
        """
        Returns a dictionary representation of the PilotManager object.
        """

        ret = {
            'uid': self.uid
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
    def _default_pilot_state_cb(self, pilot, state):

        self._log.info("[Callback]: pilot %s state: %s.", pilot.uid, state)


    # --------------------------------------------------------------------------
    #
    def _default_pilot_error_cb(self, pilot, state):

        # FIXME: use when 'exit_on_error' is set
        if state == rps.FAILED:
            self._log.error("[Callback]: pilot '%s' failed -- calling exit", pilot.uid)
            sys.exit(1)


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
    def session(self):
        """
        Returns the session object
        """
        return self._session




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

        return self._pilots.keys()


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

        # we return a list of compute pilots
        ret = list()
        for descr in descriptions :

            pilot = ComputePilot.create(pmgr=self, descr=descr)
            ret.append(pilot)

            # keep pilots around
            self._pilots[pilot.uid] = pilot

            if self._session._rec:
                import radical.utils as ru
                ru.write_json(descr.as_dict(), "%s/%s.batch.%03d.json" \
                        % (self._session._rec, pilot.uid, self._rec_id))
            self._log.report.progress()

            self.advance(pilot.as_dict(), rps.PMGR_LAUNCHING_PENDING, 
                         publish=True, push=True)

        if self._session._rec:
            self._rec_id += 1

        self._log.report.ok('>>ok\n')

        if ret_list: return ret
        else               : return ret[0]


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
            return self._pilots.values()


        ret_list = True
        if (not isinstance(uids, list)) and (uids is not None):
            ret_list = False
            uids = [uids]

        ret = list()
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
            raise RuntimeError("instance is already closed")

        if not uids:
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
            uids = [uids]

        self._log.report.info('<<wait for %d pilot(s)\n\t' % len(uids))

        start    = time.time()
        to_check = [self._pilots[uid] for uid in uids]

        # We don't want to iterate over all pilots again and again, as that would
        # duplicate checks on pilots which were found in matching states.  So we
        # create a list from which we drop the pilots as we find them in
        # a matching state
        self._log.report.idle(mode='start')
        while to_check and not self._session._terminate.is_set():

            self._log.report.idle()

            to_check = [pilot for pilot in to_check \
                               if pilot.state not in states and \
                                  pilot.state not in rps.FINAL]
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
            uids = self._pilots.keys()

        if not isinstance(uids, list):
            uids = [uids]

        pilots = self.get_pilots(uids)
        for pilot in pilots:
            pilot.cancel()


    # --------------------------------------------------------------------------
    #
    def register_callback(self, cb_func, metric=rpt.PILOT_STATE, cb_data=None):
        """
        Registers a new callback function with the PilotManager.  Manager-level
        callbacks get called if the specified metric changes.  The default
        metric `PILOT_STATE` fires the callback if any of the ComputePilots
        managed by the PilotManager change their state.

        All callback functions need to have the same signature::

            def cb_func(obj, value, cb_data)

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

        if  metric not in rpt.PMGR_METRICS :
            raise ValueError ("Metric '%s' is not available on the unit manager" % metric)

        with self._cb_lock:
            self._callbacks['umgr'][metric].append([cb_func, cb_data])


# ------------------------------------------------------------------------------

