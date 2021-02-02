
# pylint: disable=unused-argument    # W0613 Unused argument 'timeout' & 'input'
# pylint: disable=redefined-builtin  # W0622 Redefining built-in 'input'

import os
import sys
import copy
import time

import threading       as mt
import radical.utils   as ru

from ..          import constants      as rpc
from ..          import states         as rps


def out(msg):
    sys.stdout.write('%s\n' % msg)
    sys.stdout.flush()


# ------------------------------------------------------------------------------
#
class ComponentManager(object):
    '''
    RP spans a hierarchy of component instances: the application has a pmgr and
    tmgr, and the tmgr has a staging component and a scheduling component, and
    the pmgr has a launching component, and components also can have bridges,
    etc. etc.  This ComponentManager centralises the code needed to spawn,
    manage and terminate such components - any code which needs to create
    component should create a ComponentManager instance and pass the required
    component and bridge layout and configuration.  Callng `stop()` on the cmgr
    will terminate the components and brisged.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):

        self._cfg  = ru.Config('radical.pilot.cmgr', cfg=cfg)
        self._sid  = self._cfg.sid
        self._uid  = ru.generate_id('cmgr', ns=self._sid)
        self._uids = [self._uid]  # uids to track hartbeats for (incl. own)

        self._prof = ru.Profiler(self._uid, ns='radical.pilot',
                               path=self._cfg.path)
        self._log  = ru.Logger(self._uid, ns='radical.pilot',
                               path=self._cfg.path)

        self._prof.prof('init2', uid=self._uid, msg=self._cfg.path)

        # Every ComponentManager runs a HB pubsub bridge in a separate thread.
        # That HB channel should be used by all components and bridges created
        # under this CMGR.
        bcfg = ru.Config(cfg={'channel'    : 'heartbeat',
                              'type'       : 'pubsub',
                              'uid'        : self._uid + '.hb',
                              'stall_hwm'  : 1,
                              'bulk_size'  : 0,
                              'path'       : self._cfg.path})
        self._hb_bridge = ru.zmq.PubSub(bcfg)
        self._hb_bridge.start()

        self._cfg.heartbeat.addr_pub = str(self._hb_bridge.addr_pub)
        self._cfg.heartbeat.addr_sub = str(self._hb_bridge.addr_sub)

        # runs a HB monitor on that channel
        self._hb = ru.Heartbeat(uid=self.uid,
                                timeout=self._cfg.heartbeat.timeout,
                                interval=self._cfg.heartbeat.interval,
                                beat_cb=self._hb_beat_cb,  # on every heartbeat
                                term_cb=self._hb_term_cb,  # on termination
                                log=self._log)

        self._hb_pub = ru.zmq.Publisher('heartbeat',
                                        self._cfg.heartbeat.addr_pub,
                                        log=self._log, prof=self._prof)
        self._hb_sub = ru.zmq.Subscriber('heartbeat',
                                         self._cfg.heartbeat.addr_sub,
                                         topic='heartbeat', cb=self._hb_sub_cb,
                                         log=self._log, prof=self._prof)

        # confirm the bridge being usable by listening to our own heartbeat
        self._hb.start()
        self._hb.wait_startup(self._uid, self._cfg.heartbeat.timeout)
        self._log.info('heartbeat system up')


    # --------------------------------------------------------------------------
    #
    def _hb_sub_cb(self, topic, msg):
        '''
        keep track of heartbeats for all bridges/components we know
        '''

      # self._log.debug('hb_sub %s: get %s check', self.uid, msg['uid'])
        if msg['uid'] in self._uids:
          # self._log.debug('hb_sub %s: get %s used', self.uid, msg['uid'])
            self._hb.beat(uid=msg['uid'])


    # --------------------------------------------------------------------------
    #
    def _hb_beat_cb(self):
        '''
        publish own heartbeat on the hb channel
        '''

        self._hb_pub.put('heartbeat', msg={'uid' : self.uid})
      # self._log.debug('hb_cb %s: put %s', self.uid, self.uid)


    # --------------------------------------------------------------------------
    #
    def _hb_term_cb(self, uid=None):

        self._log.debug('hb_term %s: %s died', self.uid, uid)
        self._prof.prof('term', uid=self._uid)

        # FIXME: restart goes here

        # NOTE: returning `False` indicates failure to recover.  The HB will
        #       terminate and suicidally kill the very process it is living in.
        #       Make sure all required cleanup is done at this point!

        return None


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):
        return self._uid


    # --------------------------------------------------------------------------
    #
    @property
    def cfg(self):
        return self._cfg


    # --------------------------------------------------------------------------
    #
    def start_bridges(self, cfg=None):
        '''
        check if any bridges are defined under `cfg['bridges']` and start them
        '''

        self._prof.prof('start_bridges_start', uid=self._uid)

        timeout = self._cfg.heartbeat.timeout

        if cfg is None:
            cfg = self._cfg

        for bname, bcfg in cfg.get('bridges', {}).items():

            bcfg.uid         = bname
            bcfg.channel     = bname
            bcfg.cmgr        = self.uid
            bcfg.sid         = cfg.sid
            bcfg.path        = cfg.path
            bcfg.heartbeat   = cfg.heartbeat

            fname = '%s/%s.json' % (cfg.path, bcfg.uid)
            bcfg.write(fname)

            self._log.info('create  bridge %s [%s]', bname, bcfg.uid)

            out, err, ret = ru.sh_callout('radical-pilot-bridge %s' % fname)
            self._log.debug('bridge startup out: %s', out)
            self._log.debug('bridge startup err: %s', err)
            if ret:
                raise RuntimeError('bridge startup failed')

            self._uids.append(bcfg.uid)
            self._log.info('created bridge %s [%s]', bname, bcfg.uid)

        # all bridges should start now, for their heartbeats
        # to appear.
      # self._log.debug('wait   for %s', self._uids)
        failed = self._hb.wait_startup(self._uids, timeout=timeout)
      # self._log.debug('waited for %s: %s', self._uids, failed)
        if failed:
            raise RuntimeError('could not start all bridges %s' % failed)

        self._prof.prof('start_bridges_stop', uid=self._uid)


    # --------------------------------------------------------------------------
    #
    def start_components(self, cfg=None):
        '''
        check if any components are defined under `cfg['components']`
        and start them
        '''

        self._prof.prof('start_components_start', uid=self._uid)

        timeout = self._cfg.heartbeat.timeout

        if cfg is None:
            cfg = self._cfg

        # we pass a copy of the complete session config to all components, but
        # merge it into the component specific config settings (no overwrite),
        # and then remove the `bridges` and `components` sections
        #
        scfg = ru.Config(cfg=cfg)
        if 'bridges'    in scfg: del(scfg['bridges'])
        if 'components' in scfg: del(scfg['components'])

        for cname, ccfg in cfg.get('components', {}).items():

            for _ in range(ccfg.get('count', 1)):

                ccfg.uid         = ru.generate_id(cname, ns=self._sid)
                ccfg.cmgr        = self.uid
                ccfg.kind        = cname
                ccfg.sid         = cfg.sid
                ccfg.base        = cfg.base
                ccfg.path        = cfg.path
                ccfg.heartbeat   = cfg.heartbeat

                ccfg.merge(scfg, policy=ru.PRESERVE, log=self._log)

                fname = '%s/%s.json' % (cfg.path, ccfg.uid)
                ccfg.write(fname)

                self._log.info('create  component %s [%s]', cname, ccfg.uid)

                out, err, ret = ru.sh_callout('radical-pilot-component %s' % fname)
                self._log.debug('out: %s' , out)
                self._log.debug('err: %s' , err)
                if ret:
                    raise RuntimeError('bridge startup failed')

                self._uids.append(ccfg.uid)
                self._log.info('created component %s [%s]', cname, ccfg.uid)

        # all components should start now, for their heartbeats
        # to appear.
        failed = self._hb.wait_startup(self._uids, timeout=timeout * 10)
        if failed:
            raise RuntimeError('could not start all components %s' % failed)

        self._prof.prof('start_components_stop', uid=self._uid)


    # --------------------------------------------------------------------------
    #
    def close(self):

        self._prof.prof('close', uid=self._uid)

        self._hb_bridge.stop()
        self._hb.stop()


# ------------------------------------------------------------------------------
#
class Component(object):
    '''
    This class provides the basic structure for any RP component which operates
    on stateful things.  It provides means to:

      - define input channels on which to receive new things in certain states
      - define work methods which operate on the things to advance their state
      - define output channels to which to send the things after working on them
      - define notification channels over which messages with other components
        can be exchanged (publish/subscriber channels)

    All low level communication is handled by the base class -- deriving classes
    will register the respective channels, valid state transitions, and work
    methods.  When a 'thing' is received, the component is assumed to have full
    ownership over it, and that no other component will change the 'thing's
    state during that time.

    The main event loop of the component -- `work()` -- is executed on `run()`
    and will not terminate on its own, unless it encounters a fatal error.

    Components inheriting this class should attempt not to use shared
    resources.  That will ensure that multiple instances of the component can
    coexist for higher overall system throughput.  Should access to shared
    resources be necessary, it will require some locking mechanism across
    process boundaries.

    This approach should ensure that

      - 'thing's are always in a well defined state;
      - components are simple and focus on the semantics of 'thing' state
        progression;
      - no state races can occur on 'thing' state progression;
      - only valid state transitions can be enacted (given correct declaration
        of the component's semantics);
      - the overall system is performant and scalable.

    Inheriting classes SHOULD overload the following methods:

      - `initialize()`:
        - set up the component state for operation
        - register input/output/notification channels
        - register work methods
        - register callbacks to be invoked on state notification
        - the component will terminate if this method raises an exception.

      - `work()`
        - called in the main loop of the component process, on all entities
          arriving on input channels.  The component will *not* terminate if
          this method raises an exception.  For termination, `terminate()` must
          be called.

      - `finalize()`
        - tear down the component (close threads, unregister resources, etc).

    Inheriting classes MUST call the constructor:

        class StagingComponent(rpu.Component):
            def __init__(self, cfg, session):
                rpu.Component.__init__(self, cfg, session)


    A component thus must be passed a configuration (either as a path pointing
    to a file name to be opened as `ru.Config`, or as a pre-populated
    `ru.Config` instance).  That config MUST contain a session ID (`sid`) for
    the session under which to run this component, and a uid for the component
    itself which MUST be unique within the scope of the given session.  It MUST
    further contain information about the session's heartbeat ZMQ pubsub channel
    (`hb_pub`, `hb_sub`) on which heartbeats are sent and received for lifetime
    management.  All components and the session will continuously sent
    heartbeat messages on that channel - missing heartbeats will by default lead
    to session termination.

    The config MAY contain `bridges` and `component` sections.  If those exist,
    the component will start the communication bridges and the components
    specified therein, and is then considered an owner of those components and
    bridges.  As such, it much watch the HB channel for heartbeats from those
    components, and must terminate itself if those go AWOL.

    Further, the class must implement the registered work methods, with
    a signature of:

        work(self, things)

    The method is expected to change the state of the 'thing's given.  'Thing's
    will not be pushed to outgoing channels automatically -- to do so, the work
    method has to call (see call documentation for other options):

        self.advance(thing)

    Until that method is called, the component is considered the sole owner of
    the 'thing's.  After that method is called, the 'thing's are considered
    disowned by the component.  If, however, components return from the work
    methods without calling advance on the given 'thing's, then the component
    keeps ownership of the 'thing's to advance it asynchronously at a later
    point in time.  That implies that a component can collect ownership over an
    arbitrary number of 'thing's over time, and they can be advanced at the
    component's discretion.

    The component process is a stand-alone daemon process which runs outside of
    Python's multiprocessing domain.  As such, it can freely use Python's
    multithreading (and it extensively does so by default) - but developers
    should be aware that spawning additional *processes* in this component is
    discouraged, as Python's process management is not playing well with it's
    multithreading implementation.
    '''


    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):
        '''
        This constructor MUST be called by inheriting classes, as it specifies
        the operation mode of the component: components can spawn a child
        process, or not.

        If a child will be spawned later, then the child process state can be
        initialized by overloading the`initialize()` method.

        Note that this policy should be strictly followed by all derived
        classes, as we will otherwise carry state over the process fork.  That
        can become nasty if the state included any form of locking (like, for
        profiling or locking).

        The symmetric teardown methods are called `finalize()`.

        Constructors of inheriting components *may* call start() in their
        constructor.
        '''

        # NOTE: a fork will not duplicate any threads of the parent process --
        #       but it will duplicate any locks which are shared between the
        #       parent process and its threads -- and those locks might be in
        #       any state at this point.  As such, each child has to make
        #       sure to never, ever, use any of the inherited locks, but instead
        #       to create it's own set of locks in self.initialize.

        self._cfg     = cfg
        self._uid     = cfg.uid
        self._session = session

        # we always need an UID
        assert(self._uid), 'Component needs a uid (%s)' % type(self)

        # state we carry over the fork
        self._debug      = cfg.get('debug')
        self._owner      = cfg.get('owner', self.uid)
        self._ctype      = "%s.%s" % (self.__class__.__module__,
                                      self.__class__.__name__)
        self._number     = cfg.get('number', 0)
        self._name       = cfg.get('name.%s' %  self._number,
                                   '%s.%s'   % (self._ctype, self._number))

        self._bridges    = list()       # communication bridges
        self._components = list()       # sub-components
        self._inputs     = dict()       # queues to get things from
        self._outputs    = dict()       # queues to send things to
        self._workers    = dict()       # methods to work on things
        self._publishers = dict()       # channels to send notifications to
        self._threads    = dict()       # subscriber and idler threads
        self._cb_lock    = ru.RLock('comp.cb_lock.%s' % self._name)
                                        # guard threaded callback invokations
        self._work_lock  = ru.RLock('comp.work_lock.%s' % self._name)
                                        # guard threaded callback invokations

        self._subscribers = dict()      # ZMQ Subscriber classes

        if self._owner == self.uid:
            self._owner = 'root'

        self._prof = self._session._get_profiler(name=self.uid)
        self._rep  = self._session._get_reporter(name=self.uid)
        self._log  = self._session._get_logger  (name=self.uid,
                                                 level=self._debug)
      # self._prof.register_timing(name='component_lifetime',
      #                            scope='uid=%s' % self.uid,
      #                            start='component_start',
      #                            stop='component_stop')
      # self._prof.register_timing(name='entity_runtime',
      #                            scope='entity',
      #                            start='get',
      #                            stop=['put', 'drop'])
        self._prof.prof('init1', uid=self._uid, msg=self._prof.path)

        self._q    = None
        self._in   = None
        self._out  = None
        self._poll = None
        self._ctx  = None

        self._thread = None
        self._term   = mt.Event()


    # --------------------------------------------------------------------------
    #
    def start(self):

        sync = mt.Event()
        self._thread = mt.Thread(target=self._worker_thread, args=[sync])
        self._thread.daemon = True
        self._thread.start()

        while not sync.is_set():

            if not self._thread.is_alive():
                raise RuntimeError('worker thread died during initialization')

            time.sleep(0.1)

        assert(self._thread.is_alive())


    # --------------------------------------------------------------------------
    #
    def _worker_thread(self, sync):

        try:
            self._initialize()

        except Exception:
            self._log.exception('worker thread initialization failed')
            return

        sync.set()

        while not self._term.is_set():
            try:
                ret = self.work_cb()
                if not ret:
                    break
            except:
                self._log.exception('work cb error [ignored]')

        try:
            self._finalize()
        except Exception:
            self._log.exception('worker thread finalialization failed')


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def create(cfg, session):

        # TODO:  We keep this static typemap for component startup. The map
        #        should really be derived from rp module inspection via an
        #        `ru.PluginManager`.
        #
        from radical.pilot import worker as rpw
        from radical.pilot import pmgr   as rppm
        from radical.pilot import tmgr   as rptm
        from radical.pilot import agent  as rpa
        from radical.pilot import raptor as rpt
      # from radical.pilot import constants as rpc

        comp = {
                rpc.WORKER                         : rpt.Worker,
                rpc.UPDATE_WORKER                  : rpw.Update,
                rpc.STAGER_WORKER                  : rpw.Stager,

                rpc.PMGR_LAUNCHING_COMPONENT       : rppm.Launching,

                rpc.TMGR_STAGING_INPUT_COMPONENT   : rptm.Input,
                rpc.TMGR_SCHEDULING_COMPONENT      : rptm.Scheduler,
                rpc.TMGR_STAGING_OUTPUT_COMPONENT  : rptm.Output,

                rpc.AGENT_STAGING_INPUT_COMPONENT  : rpa.Input,
                rpc.AGENT_SCHEDULING_COMPONENT     : rpa.Scheduler,
                rpc.AGENT_EXECUTING_COMPONENT      : rpa.Executing,
                rpc.AGENT_STAGING_OUTPUT_COMPONENT : rpa.Output

               }

        assert(cfg.kind in comp), '%s not in %s' % (cfg.kind, list(comp.keys()))

        return comp[cfg.kind].create(cfg, session)


    # --------------------------------------------------------------------------
    #
    def __str__(self):
        return "%s <%s> [%s]" % (self.uid, self.__class__.__name__, self._owner)


    # --------------------------------------------------------------------------
    #
    def _cancel_monitor_cb(self, topic, msg):
        '''
        We listen on the control channel for cancel requests, and append any
        found UIDs to our cancel list.
        '''

        # FIXME: We do not check for types of things to cancel - the UIDs are
        #        supposed to be unique.  That abstraction however breaks as we
        #        currently have no abstract 'cancel' command, but instead use
        #        'cancel_tasks'.

        self._log.debug('command incoming: %s', msg)

        cmd = msg['cmd']
        arg = msg['arg']

        if cmd == 'cancel_tasks':

            uids = arg['uids']

            if not isinstance(uids, list):
                uids = [uids]

            self._log.debug('register for cancellation: %s', uids)

            with self._cancel_lock:
                self._cancel_list += uids

        if cmd == 'terminate':
            self._log.info('got termination command')
            self.stop()

        else:
            self._log.debug('command ignored: %s', cmd)

        return True


    # --------------------------------------------------------------------------
    #
    @property
    def cfg(self):
        return copy.deepcopy(self._cfg)

    @property
    def session(self):
        return self._session

    @property
    def uid(self):
        return self._uid

    @property
    def ctype(self):
        return self._ctype


    # --------------------------------------------------------------------------
    #
    def _initialize(self):
        '''
        initialization of component base class goes here
        '''
        # components can always publish logs, state updates and control messages
     #  self.register_publisher(rpc.LOG_PUBSUB)
        self.register_publisher(rpc.STATE_PUBSUB)
        self.register_publisher(rpc.CONTROL_PUBSUB)

        # set controller callback to handle cancellation requests
        self._cancel_list = list()
        self._cancel_lock = ru.RLock('comp.cancel_lock.%s' % self._uid)
        self.register_subscriber(rpc.CONTROL_PUBSUB, self._cancel_monitor_cb)

        # call component level initialize
        self.initialize()
        self._prof.prof('component_init')


    def initialize(self):
        pass  # can be overloaded


    # --------------------------------------------------------------------------
    #
    def _finalize(self):

        self._log.debug('_finalize()')

        # call component level finalize, before we tear down channels
        self.finalize()

        for thread in self._threads.values():
            thread.stop()

        self._log.debug('%s close prof', self.uid)
        try:
            self._prof.prof('component_final')
            self._prof.flush()
            self._prof.close()
        except Exception:
            pass


    def finalize(self):
        pass  # can be overloaded


    # --------------------------------------------------------------------------
    #
    def stop(self, timeout=None):                                         # noqa
        '''
        We need to terminate and join all threads, close all communication
        channels, etc.  But we trust on the correct invocation of the finalizers
        to do all this, and thus here only forward the stop request to the base
        class.
        '''

        #  FIXME: implement timeout, or remove parameter
        #   (pylint W0613 should be removed if changes to timeout are applied)

        self._log.info('stop %s (%s : %s) [%s]', self.uid, os.getpid(),
                       ru.get_thread_name(), ru.get_caller_name())

        self._term.set()


    # --------------------------------------------------------------------------
    #
    def register_input(self, states, input, worker=None):
        '''
        Using this method, the component can be connected to a queue on which
        things are received to be worked upon.  The given set of states (which
        can be a single state or a list of states) will trigger an assert check
        upon thing arrival.

        This method will further associate a thing state with a specific worker.
        Upon thing arrival, the thing state will be used to lookup the
        respective worker, and the thing will be handed over.  Workers should
        call self.advance(thing), in order to push the thing toward the next
        component.  If, for some reason, that is not possible before the worker
        returns, the component will retain ownership of the thing, and should
        call advance() asynchronously at a later point in time.

        Worker invocation is synchronous, ie. the main event loop will only
        check for the next thing once the worker method returns.
        '''

        states = ru.as_list(states)
        if not states:
            states = [None]  # worker handles stateless entities

        name = '%s.%s.%s' % (self.uid, worker.__name__,
                             '_'.join([str(s) for s in states]))

        if name in self._inputs:
            raise ValueError('input %s already registered' % name)

        self._inputs[name] = {'queue'  : self.get_input_ep(input),
                              'states' : states}

        self._log.debug('registered input %s', name)

        # we want exactly one worker associated with a state -- but a worker
        # can be responsible for multiple states

        for state in states:

            self._log.debug('%s register input %s: %s', self.uid, state, name)

            if state in self._workers:
                self._log.warn("%s replaces worker %s (%s)"
                        % (self.uid, self._workers[state], state))
            self._workers[state] = worker

            self._log.debug('registered worker %s [%s]', worker.__name__, state)


    # --------------------------------------------------------------------------
    #
    def unregister_input(self, states, input, worker):
        '''
        This methods is the inverse to the 'register_input()' method.
        '''

        states = ru.as_list(states)
        if not states:
            states = [None]  # worker handles statless entities


        name = '%s.%s.%s' % (self.uid, worker.__name__,
                             '_'.join([str(s) for s in states]))

        if name not in self._inputs:
            self._log.warn('input %s not registered', name)
            return

        self._inputs[name]['queue'].stop()
        del(self._inputs[name])
        self._log.debug('unregistered input %s', name)

        for state in states:

            self._log.debug('%s unregister input %s (%s)', self.uid, name, state)

            if state not in self._workers:
                self._log.warn('%s input %s unknown', worker.__name__, state)
                continue

            del(self._workers[state])


    # --------------------------------------------------------------------------
    #
    def register_output(self, states, output):
        '''
        Using this method, the component can be connected to a queue to which
        things are sent after being worked upon.  The given set of states (which
        can be a single state or a list of states) will trigger an assert check
        upon thing departure.

        If a state but no output is specified, we assume that the state is
        final, and the thing is then considered 'dropped' on calling advance() on
        it.  The advance() will trigger a state notification though, and then
        mark the drop in the log.  No other component should ever again work on
        such a final thing.  It is the responsibility of the component to make
        sure that the thing is in fact in a final state.
        '''

        states = ru.as_list(states)
        if not states:
            states = [None]  # worker handles stateless entities

        for state in states:

            self._log.debug('%s register output %s:%s', self.uid, state, output)

            # we want a *unique* output queue for each state.
            if state in self._outputs:
                self._log.warn("%s replaces output for %s : %s -> %s"
                        % (self.uid, state, self._outputs[state], output))

            if not output:
                # this indicates a final state
                self._log.debug('%s register output to None %s', self.uid, state)
                self._outputs[state] = None

            else:
                # non-final state, ie. we want a queue to push to:
                self._outputs[state] = self.get_output_ep(output)


    # --------------------------------------------------------------------------
    #
    def get_input_ep(self, input):
        '''
        return an input endpoint
        '''

        # dig the addresses from the bridge's config file
        fname = '%s/%s.cfg' % (self._cfg.path, input)
        cfg   = ru.read_json(fname)

        return ru.zmq.Getter(input, url=cfg['get'])


    # --------------------------------------------------------------------------
    #
    def get_output_ep(self, output):
        '''
        return an output endpoint
        '''

        # dig the addresses from the bridge's config file
        fname = '%s/%s.cfg' % (self._cfg.path, output)
        cfg   = ru.read_json(fname)

        return ru.zmq.Putter(output, url=cfg['put'])


    # --------------------------------------------------------------------------
    #
    def unregister_output(self, states):
        '''
        this removes any outputs registerd for the given states.
        '''

        states = ru.as_list(states)
        if not states:
            states = [None]  # worker handles stateless entities

        for state in states:

            self._log.debug('TERM : %s unregister output %s', self.uid, state)

            if state not in self._outputs:
                self._log.warn('state %s has no output registered',  state)
              # raise ValueError('state %s has no output registered' % state)
                continue

            del(self._outputs[state])
            self._log.debug('unregistered output for %s', state)


    # --------------------------------------------------------------------------
    #
    def output(self, things, state=None):
        '''
        this pushes the given things to the output queue register for the given
        state
        '''

        # NOTE: we do not check if things are actually in the given state
        things = ru.as_list(things)
        if not things:
            # nothing to do
            return

        if state not in self._outputs:
            raise ValueError('state %s has no output registered' % state)

        if self._outputs[state]:
            # the bridge will sort things into bulks, wit bulk size dependig on
            # bridge configuration
            self._outputs[state].put(things)


    # --------------------------------------------------------------------------
    #
    def register_timed_cb(self, cb, cb_data=None, timer=None):
        '''
        Idle callbacks are invoked at regular intervals -- they are guaranteed
        to *not* be called more frequently than 'timer' seconds, no promise is
        made on a minimal call frequency.  The intent for these callbacks is to
        run lightweight work in semi-regular intervals.
        '''

        name = "%s.idler.%s" % (self.uid, cb.__name__)
        self._log.debug('START: %s register idler %s', self.uid, name)

        with self._cb_lock:
            if name in self._threads:
                raise ValueError('cb %s already registered' % cb.__name__)

            if timer is None: timer = 0.0  # NOTE: busy idle loop
            else            : timer = float(timer)

            # create a separate thread per idle cb, and let it be watched by the
            # ru.Process base class
            #
            # ------------------------------------------------------------------
            # NOTE: idle timing is a tricky beast: if we sleep for too long,
            #       then we have to wait that long on stop() for the thread to
            #       get active again and terminate/join.  So we always sleep
            #       just a little, and explicitly check if sufficient time has
            #       passed to activate the callback.
            class Idler(mt.Thread):

                # --------------------------------------------------------------
                def __init__(self, name, log, timer, cb, cb_data, cb_lock):
                    self._name    = name
                    self._log     = log
                    self._timeout = timer
                    self._cb      = cb
                    self._cb_data = cb_data
                    self._cb_lock = cb_lock
                    self._last    = 0.0
                    self._term    = mt.Event()

                    super(Idler, self).__init__()
                    self.daemon = True
                    self.start()

                def stop(self):
                    self._term.set()

                def run(self):
                    try:
                        self._log.debug('start idle thread: %s', self._cb)
                        ret = True
                        while ret and not self._term.is_set():
                            if self._timeout and \
                               self._timeout > (time.time() - self._last):
                                # not yet
                                time.sleep(0.1)  # FIXME: make configurable
                                continue

                            with self._cb_lock:
                                if self._cb_data is not None:
                                    ret = self._cb(cb_data=self._cb_data)
                                else:
                                    ret = self._cb()
                            if self._timeout:
                                self._last = time.time()
                    except:
                        self._log.exception('idle thread failed: %s', self._cb)
            # ------------------------------------------------------------------

            idler = Idler(name=name, timer=timer, log=self._log,
                          cb=cb, cb_data=cb_data, cb_lock=self._cb_lock)
            self._threads[name] = idler

        self._log.debug('%s registered idler %s', self.uid, name)


    # --------------------------------------------------------------------------
    #
    def unregister_timed_cb(self, cb):
        '''
        This method is reverts the register_timed_cb() above: it
        removes an idler from the component, and will terminate the
        respective thread.
        '''

        name = "%s.idler.%s" % (self.uid, cb.__name__)
        self._log.debug('TERM : %s unregister idler %s', self.uid, name)

        with self._cb_lock:

            if name not in self._threads:
                self._log.warn('timed cb %s is not registered', name)
              # raise ValueError('%s is not registered' % name)
                return

            self._threads[name].stop()  # implies join
            del(self._threads[name])

        self._log.debug("TERM : %s unregistered idler %s", self.uid, name)


    # --------------------------------------------------------------------------
    #
    def register_publisher(self, pubsub):
        '''
        Using this method, the component can registered itself to be a publisher
        of notifications on the given pubsub channel.
        '''

        assert(pubsub not in self._publishers)

        # dig the addresses from the bridge's config file
        fname = '%s/%s.cfg' % (self._cfg.path, pubsub)
        cfg   = ru.read_json(fname)

        self._publishers[pubsub] = ru.zmq.Publisher(channel=pubsub,
                                                    url=cfg['pub'],
                                                    log=self._log,
                                                    prof=self._prof)

        self._log.debug('registered publisher for %s', pubsub)


    # --------------------------------------------------------------------------
    #
    def register_subscriber(self, pubsub, cb):
        '''
        This method is complementary to the register_publisher() above: it
        registers a subscription to a pubsub channel.  If a notification
        is received on thag channel, the registered callback will be
        invoked.  The callback MUST have one of the signatures:

          callback(topic, msg)

        where 'topic' is set to the name of the pubsub channel.

        The subscription will be handled in a separate thread, which implies
        that the callback invocation will also happen in that thread.  It is the
        caller's responsibility to ensure thread safety during callback
        invocation.
        '''

        # dig the addresses from the bridge's config file
        fname = '%s/%s.cfg' % (self._cfg.path, pubsub)
        cfg   = ru.read_json(fname)

        if pubsub not in self._subscribers:
            self._subscribers[pubsub] = ru.zmq.Subscriber(channel=pubsub,
                                                          url=cfg['sub'],
                                                          log=self._log,
                                                          prof=self._prof)

        self._subscribers[pubsub].subscribe(topic=pubsub, cb=cb,
                                            lock=self._cb_lock)


    # --------------------------------------------------------------------------
    #
    def work_cb(self):
        '''
        This is the main routine of the component, as it runs in the component
        process.  It will first initialize the component in the process context.
        Then it will attempt to get new things from all input queues
        (round-robin).  For each thing received, it will route that thing to the
        respective worker method.  Once the thing is worked upon, the next
        attempt on getting a thing is up.
        '''

        # if no action occurs in this iteration, idle
        if not self._inputs:
            time.sleep(0.1)
            return True

        for name in self._inputs:

            input  = self._inputs[name]['queue']
            states = self._inputs[name]['states']

            # FIXME: a simple, 1-thing caching mechanism would likely
            #        remove the req/res overhead completely (for any
            #        non-trivial worker).
            things = input.get_nowait(500)  # in microseconds
            things = ru.as_list(things)

            if not things:
                return True

            # the worker target depends on the state of things, so we
            # need to sort the things into buckets by state before
            # pushing them
            buckets = dict()
            for thing in things:
                state = thing.get('state')  # can be stateless
                uid   = thing.get('uid')    # and not have uids
                self._prof.prof('get', uid=uid, state=state)

                if state not in buckets:
                    buckets[state] = list()
                buckets[state].append(thing)

            # We now can push bulks of things to the workers

            for state,things in buckets.items():

                assert(state in states), 'cannot handle state %s' % state
                assert(state in self._workers), 'no worker for state %s' % state

                try:
                    to_cancel = list()

                    for thing in things:

                        uid = thing.get('uid')

                        # FIXME: this can become expensive over time
                        #        if the cancel list is never cleaned
                        if uid and uid in self._cancel_list:
                            with self._cancel_lock:
                                self._cancel_list.remove(uid)
                            to_cancel.append(thing)

                        self._log.debug('got %s (%s)', uid, state)

                    if to_cancel:
                        # only advance stateful entities, otherwise just drop
                        if state:
                            self.advance(to_cancel, rps.CANCELED, publish=True,
                                                                  push=False)
                    with self._work_lock:
                        self._workers[state](things)

                except Exception:

                    # this is not fatal -- only the 'things' fail, not
                    # the component
                    self._log.exception("work %s failed", self._workers[state])

                    if state:
                        self.advance(things, rps.FAILED, publish=True,
                                                         push=False)

        # keep work_cb registered
        return True


    # --------------------------------------------------------------------------
    #
    def advance(self, things, state=None, publish=True, push=False, ts=None,
                      prof=True):
        '''
        Things which have been operated upon are pushed down into the queues
        again, only to be picked up by the next component, according to their
        state model.  This method will update the thing state, and push it into
        the output queue registered as target for that state.

        things:  list of things to advance
        state:   new state to set for the things
        publish: determine if state update notifications should be issued
        push:    determine if things should be pushed to outputs
        prof:    determine if state advance creates a profile event
                 (publish, and push are always profiled)

        'Things' are expected to be a dictionary, and to have 'state', 'uid' and
        optionally 'type' set.

        If 'thing' contains an '$all' key, the complete dict is published;
        otherwise, *only the state* is published.

        This is evaluated in self.publish.
        '''

        if not things:
            return

        if not ts:
            ts = time.time()

        things = ru.as_list(things)

        self._log.debug('advance bulk: %s [%s, %s]', len(things), push, publish)

        # assign state, sort things by state
        buckets = dict()
        for thing in things:

            uid = thing['uid']

          # if thing['type'] not in ['task', 'pilot']:
          #     raise TypeError("thing has unknown type (%s)" % uid)

            if state:
                # state advance done here
                thing['state'] = state

            _state = thing['state']

            if prof:
                self._prof.prof('advance', uid=uid, state=_state, ts=ts)

            if _state not in buckets:
                buckets[_state] = list()
            buckets[_state].append(thing)

        # should we publish state information on the state pubsub?
        if publish:

            to_publish = list()

            # If '$all' is set, we update the complete thing_dict.
            # Things in final state are also published in full.
            # If '$set' is set, we also publish all keys listed in there.
            # In all other cases, we only send 'uid', 'type' and 'state'.
            for thing in things:
                if '$all' in thing:
                    del(thing['$all'])
                    if '$set' in thing:
                        del(thing['$set'])
                    to_publish.append(thing)

                elif thing['state'] in rps.FINAL:
                    to_publish.append(thing)

                else:
                    tmp = {'uid'   : thing['uid'],
                           'type'  : thing['type'],
                           'state' : thing['state']}
                    if '$set' in thing:
                        for key in thing['$set']:
                            tmp[key] = thing[key]
                        del(thing['$set'])
                    to_publish.append(tmp)

            self.publish(rpc.STATE_PUBSUB, {'cmd': 'update', 'arg': to_publish})

          # ts = time.time()
          # for thing in things:
          #     self._prof.prof('publish', uid=thing['uid'],
          #                     state=thing['state'], ts=ts)

        # never carry $all and across component boundaries!
        for thing in things:
            if '$all' in thing:
                del(thing['$all'])

        # should we push things downstream, to the next component
        if push:

            # the push target depends on the state of things, so we need to sort
            # the things into buckets by state before pushing them
            # now we can push the buckets as bulks
            for _state,_things in buckets.items():

              # ts = time.time()
                if _state in rps.FINAL:
                    # things in final state are dropped
                    for thing in _things:
                        self._log.debug('final %s [%s]', thing['uid'], _state)
                        self._prof.prof('drop', uid=thing['uid'], state=_state,
                                        ts=ts)
                    continue

                if _state not in self._outputs:
                    # unknown target state -- error
                    for thing in _things:
                        self._log.debug("lost  %s [%s]", thing['uid'], _state)
                        self._prof.prof('lost', uid=thing['uid'], state=_state,
                                        ts=ts)
                    continue

                if not self._outputs[_state]:
                    # empty output -- drop thing
                    for thing in _things:
                        self._log.debug('drop  %s [%s]', thing['uid'], _state)
                        self._prof.prof('drop', uid=thing['uid'], state=_state,
                                        ts=ts)
                    continue

                output = self._outputs[_state]

                # push the thing down the drain
                self._log.debug('put bulk %s: %s', _state, len(_things))
                output.put(_things)

                ts = time.time()
                for thing in _things:
                    self._prof.prof('put', uid=thing['uid'], state=_state,
                                    msg=output.name, ts=ts)


    # --------------------------------------------------------------------------
    #
    def publish(self, pubsub, msg):
        '''
        push information into a publication channel
        '''

        if not self._publishers.get(pubsub):
            raise RuntimeError("no msg route for '%s': %s" % (pubsub, msg))

        self._publishers[pubsub].put(pubsub, msg)


# ------------------------------------------------------------------------------
#
class Worker(Component):
    '''
    A Worker is a Component which cannot change the state of the thing it
    handles.  Workers are employed as helper classes to mediate between
    components, between components and database, and between components and
    notification channels.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        Component.__init__(self, cfg=cfg, session=session)


# ------------------------------------------------------------------------------

