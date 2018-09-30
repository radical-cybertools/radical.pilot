
import os
import sys
import copy
import time
import pprint
import signal

import setproctitle    as spt
import threading       as mt
import multiprocessing as mp
import radical.utils   as ru

from ..          import constants      as rpc
from ..          import states         as rps

from .queue      import Queue          as rpu_Queue
from .queue      import QUEUE_OUTPUT   as rpu_QUEUE_OUTPUT
from .queue      import QUEUE_INPUT    as rpu_QUEUE_INPUT
from .queue      import QUEUE_BRIDGE   as rpu_QUEUE_BRIDGE

from .pubsub     import Pubsub         as rpu_Pubsub
from .pubsub     import PUBSUB_PUB     as rpu_PUBSUB_PUB
from .pubsub     import PUBSUB_SUB     as rpu_PUBSUB_SUB
from .pubsub     import PUBSUB_BRIDGE  as rpu_PUBSUB_BRIDGE


# ==============================================================================
#
class Component(ru.Process):
    """
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

    The main event loop of the component -- work_cb() -- is executed as a separate
    process.  Components inheriting this class should be fully self sufficient,
    and should specifically attempt not to use shared resources.  That will
    ensure that multiple instances of the component can coexist for higher
    overall system throughput.  Should access to shared resources be necessary,
    it will require some locking mechanism across process boundaries.

    This approach should ensure that

      - 'thing's are always in a well defined state;
      - components are simple and focus on the semantics of 'thing' state
        progression;
      - no state races can occur on 'thing' state progression;
      - only valid state transitions can be enacted (given correct declaration
        of the component's semantics);
      - the overall system is performant and scalable.

    Inheriting classes may overload the methods:

        initialize
        initialize_child
        finalize
        finalize_child

    These method should be used to

      - set up the component state for operation
      - register input/output/notification channels
      - register work methods
      - register callbacks to be invoked on state notification
      - tear down the same on closing

    Inheriting classes MUST call the constructor:

        class StagingComponent(rpu.Component):
            def __init__(self, cfg, session):
                rpu.Component.__init__(self, cfg, session)

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
    component's descretion.
    """

    # FIXME:
    #  - make state transitions more formal
   

    # --------------------------------------------------------------------------
    #
    @staticmethod
    def start_bridges(cfg, session, log):
        '''
        Check the given config, and specifically check if any bridges are
        defined under `cfg['bridges']`.  If that is the case, we check
        if those bridges have endpoints documented.  If so, we assume they are
        running, and that's fine.  If not, we start them and add the respective
        enspoint information to the config.  
        
        This method will return a list of created bridge instances.  It is up to
        the callee to watch those bridges for health and to terminate them as
        needed.
        '''

        bspec = cfg.get('bridges', {})
        log.debug('start bridges   : %s', pprint.pformat(bspec))

        if not bspec:
            # nothing to do
            return list()

        # start all bridges which don't yet have an address
        bridges = list()
        for bname,bcfg in bspec.iteritems():

            addr_in  = bcfg.get('addr_in')
            addr_out = bcfg.get('addr_out')

            if addr_in:
                # bridge is running
                assert(addr_out), 'addr_out not set, invalid bridge'
                continue

            # bridge needs starting
            log.info('create bridge %s', bname)
            
            bcfg_clone = copy.deepcopy(bcfg)

            # The type of bridge (queue or pubsub) is derived from the name.
            if bname.endswith('queue'):
                bridge = rpu_Queue(session, bname, rpu_QUEUE_BRIDGE, bcfg_clone)

            elif bname.endswith('pubsub'):
                bridge = rpu_Pubsub(session, bname, rpu_PUBSUB_BRIDGE, bcfg_clone)

            else:
                raise ValueError('unknown bridge type for %s' % bname)

            # bridge addresses are URLs
            bcfg['addr_in']  = str(bridge.addr_in)
            bcfg['addr_out'] = str(bridge.addr_out)

            # we keep a handle to the bridge for later shutdown
            bridges.append(bridge)

            # make bridge address part of the
            # session config
            log.info('created bridge %s (%s)', bname, bridge.name)

        return bridges


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def start_components(cfg, session, log):
        '''
        `start_components()` is very similar to `start_bridges()`, in that it
        interprets a given configuration and creates all listed component
        instances.  Components are, however,  *always* created, independent of
        any existing instances.

        This method will return a list of created component instances.  It is up
        to the callee to watch those components for health and to terminate them
        as needed.  
        '''

        # ----------------------------------------------------------------------
        # NOTE:  I'd rather have this as class data than as stack data, but
        #        python stumbles over circular imports at that point :/
        from .. import worker as rpw
        from .. import pmgr   as rppm
        from .. import umgr   as rpum
        from .. import agent  as rpa

        _ctypemap = {rpc.UPDATE_WORKER                  : rpw.Update,

                     rpc.PMGR_LAUNCHING_COMPONENT       : rppm.Launching,

                     rpc.UMGR_STAGING_INPUT_COMPONENT   : rpum.Input,
                     rpc.UMGR_SCHEDULING_COMPONENT      : rpum.Scheduler,
                     rpc.UMGR_STAGING_OUTPUT_COMPONENT  : rpum.Output,

                     rpc.AGENT_STAGING_INPUT_COMPONENT  : rpa.Input,
                     rpc.AGENT_SCHEDULING_COMPONENT     : rpa.Scheduler,
                     rpc.AGENT_EXECUTING_COMPONENT      : rpa.Executing,
                     rpc.AGENT_STAGING_OUTPUT_COMPONENT : rpa.Output
                    }
        # ----------------------------------------------------------------------

        cspec  = cfg.get('components', {})
        log.debug('start components: %s', pprint.pformat(cspec))

        if not cspec:
            # nothing to do
            return list()

        # merge the session's bridge information (preserve) into the given
        # config
        ru.dict_merge(cfg['bridges'], session._cfg.get('bridges', {}), ru.PRESERVE)

        # start components
        components = list()
        for cname,ccfg in cspec.iteritems():

            cnum = ccfg.get('count', 1)

            log.debug('start %s component(s) %s', cnum, cname)

            if cname not in _ctypemap:
                raise ValueError('unknown component type (%s)' % cname)

            ctype = _ctypemap[cname]
            for i in range(cnum):

                # for components, we pass on the original cfg (or rather a copy
                # of that), and merge the component's config section into it
                tmp_cfg = copy.deepcopy(cfg)
                tmp_cfg['cname']      = cname
                tmp_cfg['number']     = i
                tmp_cfg['owner']      = cfg.get('uid', session.uid)

                # avoid recursion - but keep bridge information around.  
                tmp_cfg['agents']     = dict()
                tmp_cfg['components'] = dict()

                # merge the component config section (overwrite)
                ru.dict_merge(tmp_cfg, ccfg, ru.OVERWRITE)

                comp = ctype.create(tmp_cfg, session)
                comp.start()

                log.info('%-30s starts %s',  tmp_cfg['owner'], comp.uid)
                components.append(comp)

        # components are started -- we return the handles to the callee for
        # lifetime management
        return components


    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):
        """
        This constructor MUST be called by inheriting classes, as it specifies
        the operation mode of the component: components can spawn a child
        process, or not.

        If a child will be spawned later, then the child process state can be
        initialized by overloading the`initialize_child()` method.
        Initialization for component the parent process is similarly done via
        `initializale_parent()`, which will be called no matter if the component
        spawns a child or not.

        Note that this policy should be strictly followed by all derived
        classes, as we will otherwise carry state over the process fork.  That
        can become nasty if the state included any form of locking (like, for
        profiling or locking).

        The symmetric teardown methods are called `finalize_child()` and
        `finalize_parent()`, for child and parent process, repsectively.

        Constructors of inheriting components *may* call start() in their
        constructor.
        """

        # NOTE: a fork will not duplicate any threads of the parent process --
        #       but it will duplicate any locks which are shared between the
        #       parent process and its threads -- and those locks might be in
        #       any state at this point.  As such, each child has to make
        #       sure to never, ever, use any of the inherited locks, but instead
        #       to create it's own set of locks in self.initialize_child
        #       / self.initialize_parent!

        self._cfg     = copy.deepcopy(cfg)
        self._session = session

        # we always need an UID
        if not hasattr(self, '_uid'):
            raise ValueError('Component needs a uid (%s)' % type(self))

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
        self._cb_lock    = mt.RLock()   # guard threaded callback invokations

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

        self._q    = None
        self._in   = None
        self._out  = None
        self._poll = None
        self._ctx  = None

        # initialize the Process base class for later fork.
        super(Component, self).__init__(name=self._uid, log=self._log)

        # make sure we bootstrapped ok
        self.is_valid()
        self._session._to_stop.append(self)


    # --------------------------------------------------------------------------
    #
    def __str__(self):
        return "%s <%s> [%s]" % (self.uid, self.__class__.__name__, self._owner)


    # --------------------------------------------------------------------------
    #
    def is_valid(self, term=True):
        '''
        Just as the Process' `is_valid()` call, we make sure that the component
        is still viable, and will raise an exception if not.  Additionally to
        the health of the component's child process, we also check health of any
        sub-components and communication bridges.
        '''

        # TODO: add a time check to avoid checking validity too frequently.
        #       make frequency configurable.

        if self._ru_terminating:
            # don't go any further.  Specifically, don't call stop.  Calling
            # that is up to the thing who inioated termination.
            return False

        valid = True

        if valid:
            if not super(Component, self).is_valid():
                self._log.warn("super %s is invalid" % self.uid)
                valid = False

        if valid:
            if not self._session.is_valid(term):
                self._log.warn("session %s is invalid" % self._session.uid)
                valid = False

        if valid:
            for bridge in self._bridges:
                if not bridge.is_valid(term):
                    self._log.warn("bridge %s is invalid" % bridge.uid)
                    valid = False
                    break

        if valid:
            for component in self._components:
                if not component.is_valid(term):
                    self._log.warn("sub component %s is invalid" % component.uid)
                    valid = False
                    break

        if not valid:
            self._log.warn("component %s is invalid" % self.uid)
            self.stop()
          # raise RuntimeError("component %s is invalid" % self.uid)

        return valid


    # --------------------------------------------------------------------------
    #
    def _cancel_monitor_cb(self, topic, msg):
        """
        We listen on the control channel for cancel requests, and append any
        found UIDs to our cancel list.
        """

        self.is_valid()
        
        # FIXME: We do not check for types of things to cancel - the UIDs are
        #        supposed to be unique.  That abstraction however breaks as we
        #        currently have no abstract 'cancel' command, but instead use
        #        'cancel_units'.

      # self._log.debug('command incoming: %s', msg)

        cmd = msg['cmd']
        arg = msg['arg']

        if cmd == 'cancel_units':

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
    def owner(self):
        return self._owner

    @property
    def name(self):
        return self._name

    @property
    def ctype(self):
        return self._ctype

    @property
    def is_parent(self):
        return self._ru_is_parent

    @property
    def is_child(self):
        return self._ru_is_child

    @property
    def has_child(self):
        return self.is_parent and self.pid


    # --------------------------------------------------------------------------
    #
    def ru_initialize_common(self):
        """
        This private method contains initialization for both parent a child
        process, which gets the component into a proper functional state.

        This method must be called *after* fork (this is asserted).
        """

        # NOTE: this method somewhat breaks the initialize_child vs.
        #       initialize_parent paradigm, in that it will behave differently
        #       for parent and child.  We do this to ensure existence of
        #       bridges and sub-components for the initializers of the
        #       inheriting classes

        # make sure we have a unique logfile etc for the child
        if self.is_child:
            self._uid = self.ru_childname  # derived from parent name

            # get debugging, logging, profiling set up
          # self._dh   = ru.DebugHelper(name=self.uid)
            self._prof = self._session._get_profiler(name=self.uid)
            self._log  = self._session._get_logger  (name=self.uid,
                                                     level=self._debug)

            # make sure that the Process base class uses the same logger
            # FIXME: do same for profiler?
            super(Component, self)._ru_set_logger(self._log)

        self._log.info('initialize %s',   self.uid)
        self._log.info('cfg: %s', pprint.pformat(self._cfg))

        try:
            # make sure our config records the uid
            self._cfg['uid'] = self.uid

            # all components need at least be able to talk to a control, log and
            # state pubsub.  We expect those channels to be provided by the
            # session, and the respective addresses to be available in the
            # session config's `bridges` section.  We merge that information into
            # our own config, and then check for completeness.
            #
            # Before doing so we start our own bridges though -- this way we can
            # potentially attach those basic pubsubs to a root component
            # (although this is not done at the moment).
            #
            # bridges can *only* be started by non-spawning components --
            # otherwise we would not be able to communicate bridge addresses to
            # the parent or child process (remember, this is *after* fork, the
            # cfg is already passed on).
            if self._ru_is_parent and not self._ru_spawned:
                self._bridges = Component.start_bridges(self._cfg, 
                                                        self._session, 
                                                        self._log)

            # only one side will start sub-components: either the child, if it
            # exists, and only otherwise the parent
            if self._ru_is_parent and not self._ru_spawned:
                self._components = Component.start_components(self._cfg, 
                                                              self._session, 
                                                              self._log)
            
            elif self._ru_is_child:
                self._components = Component.start_components(self._cfg, 
                                                              self._session, 
                                                              self._log)

            # bridges should now be available and known - assert!
            assert('bridges' in self._cfg),                              'missing bridges'
            assert(rpc.LOG_PUBSUB     in self._cfg['bridges']),          'missing log pubsub'
            assert(rpc.STATE_PUBSUB   in self._cfg['bridges']),          'missing state pubsub'
            assert(rpc.CONTROL_PUBSUB in self._cfg['bridges']),          'missing control pubsub'
            assert(self._cfg['bridges'][rpc.LOG_PUBSUB    ]['addr_in']), 'log pubsub invalid'
            assert(self._cfg['bridges'][rpc.STATE_PUBSUB  ]['addr_in']), 'state pubsub invalid'
            assert(self._cfg['bridges'][rpc.CONTROL_PUBSUB]['addr_in']), 'control pubsub invalid'

        except Exception as e:
            self._log.exception('bridge / component startup incomplete:\n%s' \
                    % pprint.pformat(self._cfg))
            raise


        # components can always publish logs, state updates and control messages
        self.register_publisher(rpc.LOG_PUBSUB)
        self.register_publisher(rpc.STATE_PUBSUB)
        self.register_publisher(rpc.CONTROL_PUBSUB)

        # call component level initialize
        self.initialize_common()


    # --------------------------------------------------------------------------
    #
    def initialize_common(self):
        pass # can be overloaded


    # --------------------------------------------------------------------------
    #
    def ru_initialize_parent(self):

        # call component level initialize
        self.initialize_parent()
        self._prof.prof('component_init')


    def initialize_parent(self):
        pass # can be overloaded


    # --------------------------------------------------------------------------
    #
    def ru_initialize_child(self):
        """
        child initialization of component base class goes here
        """

        spt.setproctitle('rp.%s' % self.uid)

        if os.path.isdir(self._session.uid):
            sys.stdout = open("%s/%s.out" % (self._session.uid, self.uid), "w")
            sys.stderr = open("%s/%s.err" % (self._session.uid, self.uid), "w")
        else:
            sys.stdout = open("%s.out" % self.uid, "w")
            sys.stderr = open("%s.err" % self.uid, "w")


        # set controller callback to handle cancellation requests
        self._cancel_list = list()
        self._cancel_lock = mt.RLock()
        self.register_subscriber(rpc.CONTROL_PUBSUB, self._cancel_monitor_cb)

        # call component level initialize
        self.initialize_child()
        self._prof.prof('component_init')

    def initialize_child(self):
        pass # can be overloaded


    # --------------------------------------------------------------------------
    #
    def ru_finalize_common(self):

        self._log.debug('ru_finalize_common()')

        # call component level finalize, before we tear down channels
        self.finalize_common()

        # reverse order from initialize_common
        self.unregister_publisher(rpc.LOG_PUBSUB)
        self.unregister_publisher(rpc.STATE_PUBSUB)
        self.unregister_publisher(rpc.CONTROL_PUBSUB)

        self._log.debug('%s close prof', self.uid)
        try:
            self._prof.prof('component_final')
            self._prof.close()
        except Exception:
            pass

        with self._cb_lock:

            for bridge in self._bridges:
                bridge.stop()
            self._bridges = list()

            for comp in self._components:
                comp.stop()
            self._components = list()

          # #  FIXME: the stuff below caters to unsuccessful or buggy termination
          # #         routines - but for now all those should be served by the
          # #         respective unregister routines.
          #
          # for name in self._inputs:
          #     self._inputs[name]['queue'].stop()
          # self._inputs = dict()
          #
          # for name in self._workers.keys()[:]:
          #     del(self._workers[name])
          #
          # for name in self._outputs:
          #     if self._outputs[name]:
          #         self._outputs[name].stop()
          # self._outputs = dict()
          #
          # for name in self._publishers:
          #     self._publishers[name].stop()
          # self._publishers = dict()
          #
          # for name in self._threads:
          #     self._threads[name].stop()
          # self._threads = dict()

    def finalize_common(self):
        pass # can be overloaded


    # --------------------------------------------------------------------------
    #
    def ru_finalize_parent(self):

        # call component level finalize
        self.finalize_parent()

    def finalize_parent(self):
        pass # can be overloaded


    # --------------------------------------------------------------------------
    #
    def ru_finalize_child(self):

        # call component level finalize
        self.finalize_child()

    def finalize_child(self):
        pass # can be overloaded


    # --------------------------------------------------------------------------
    #
    def stop(self, timeout=None):
        '''
        We need to terminate and join all threads, close all comunication
        channels, etc.  But we trust on the correct invocation of the finalizers
        to do all this, and thus here only forward the stop request to the base
        class.
        '''

        self._log.info('stop %s (%s : %s : %s) [%s]', self.uid, os.getpid(),
                       self.pid, ru.get_thread_name(), ru.get_caller_name())

        # FIXME: well, we don't completely trust termination just yet...
        self._prof.flush()

        super(Component, self).stop(timeout)


    # --------------------------------------------------------------------------
    #
    def register_input(self, states, input, worker=None):
        """
        Using this method, the component can be connected to a queue on which
        things are received to be worked upon.  The given set of states (which
        can be a single state or a list of states) will trigger an assert check
        upon thing arrival.

        This method will further associate a thing state with a specific worker.
        Upon thing arrival, the thing state will be used to lookup the respective
        worker, and the thing will be handed over.  Workers should call
        self.advance(thing), in order to push the thing toward the next component.
        If, for some reason, that is not possible before the worker returns, the
        component will retain ownership of the thing, and should call advance()
        asynchronously at a later point in time.

        Worker invocation is synchronous, ie. the main event loop will only
        check for the next thing once the worker method returns.
        """

        self.is_valid()

        if not isinstance(states, list):
            states = [states]

        name = '%s.%s.%s' % (self.uid, worker.__name__, '_'.join(states))

        if name in self._inputs:
            raise ValueError('input %s already registered' % name)

        # get address for the queue
        addr = self._cfg['bridges'][input]['addr_out']
        self._log.debug("using addr %s for input %s", addr, input)

        q = rpu_Queue(self._session, input, rpu_QUEUE_OUTPUT, self._cfg, addr=addr)
        self._inputs[name] = {'queue'  : q,
                              'states' : states}

        self._log.debug('registered input %s', name)

        # we want exactly one worker associated with a state -- but a worker can
        # be responsible for multiple states
        for state in states:

            self._log.debug('START: %s register input %s: %s', self.uid, state, name)

            if state in self._workers:
                self._log.warn("%s replaces worker for %s (%s)" \
                        % (self.uid, state, self._workers[state]))
            self._workers[state] = worker

            self._log.debug('registered worker %s [%s]', worker.__name__, state)


    # --------------------------------------------------------------------------
    #
    def unregister_input(self, states, input, worker):
        """
        This methods is the inverse to the 'register_input()' method.
        """

        self.is_valid()

        if not isinstance(states, list):
            states = [states]

        name = '%s.%s.%s' % (self.uid, worker.__name__, '_'.join(states))

        if name not in self._inputs:
            self._log.warn('input %s not registered', name)
          # raise ValueError('input %s not registered' % name)
            return

        self._inputs[name]['queue'].stop()
        del(self._inputs[name])
        self._log.debug('unregistered input %s', name)

        for state in states:
            self._log.debug('TERM : %s unregister input %s: %s', self.uid, state, name)
            if state not in self._workers:
                raise ValueError('worker %s not registered for %s' % worker.__name__, state)
            del(self._workers[state])
            self._log.debug('unregistered worker %s [%s]', worker.__name__, state)


    # --------------------------------------------------------------------------
    #
    def register_output(self, states, output=None):
        """
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
        """

        self.is_valid()

        if not isinstance(states, list):
            states = [states]

        for state in states:

            self._log.debug('START: %s register output %s', self.uid, state)

            # we want a *unique* output queue for each state.
            if state in self._outputs:
                self._log.warn("%s replaces output for %s : %s -> %s" \
                        % (self.uid, state, self._outputs[state], output))

            if not output:
                # this indicates a final state
                self._outputs[state] = None
            else:
                # get address for the queue
                addr = self._cfg['bridges'][output]['addr_in']
                self._log.debug("using addr %s for output %s", addr, output)

                # non-final state, ie. we want a queue to push to
                q = rpu_Queue(self._session, output, rpu_QUEUE_INPUT, self._cfg, addr=addr)
                self._outputs[state] = q

                self._log.debug('registered output    : %s : %s : %s' \
                     % (state, output, q.name))


    # --------------------------------------------------------------------------
    #
    def unregister_output(self, states):
        """
        this removes any outputs registerd for the given states.
        """

        self.is_valid()

        if not isinstance(states, list):
            states = [states]

        for state in states:
            self._log.debug('TERM : %s unregister output %s', self.uid, state)

            if state not in self._inputs:

                self._log.warn('input %s is not registered', state)
              # raise ValueError('input %s is not registered' % state)
                continue

            if not state in self._outputs:
                self._log.warn('state %s has no output registered',  state)
              # raise ValueError('state %s has no output registered' % state)
                continue

            del(self._outputs[state])
            self._log.debug('unregistered output for %s', state)


    # --------------------------------------------------------------------------
    #
    def register_timed_cb(self, cb, cb_data=None, timer=None):
        """
        Idle callbacks are invoked at regular intervals -- they are guaranteed
        to *not* be called more frequently than 'timer' seconds, no promise is
        made on a minimal call frequency.  The intent for these callbacks is to
        run lightweight work in semi-regular intervals.  
        """

        self.is_valid()

        name = "%s.idler.%s" % (self.uid, cb.__name__)
        self._log.debug('START: %s register idler %s', self.uid, name)

        with self._cb_lock:
            if name in self._threads:
                raise ValueError('cb %s already registered' % cb.__name__)

            if timer == None: timer = 0.0  # NOTE: busy idle loop
            else            : timer = float(timer)

            # create a separate thread per idle cb, and let it be watched by the
            # ru.Process base class
            #
            # ----------------------------------------------------------------------
            # NOTE: idle timing is a tricky beast: if we sleep for too long, then we
            #       have to wait that long on stop() for the thread to get active
            #       again and terminate/join.  So we always sleep just a little, and
            #       explicitly check if sufficient time has passed to activate the
            #       callback.
            class Idler(ru.Thread):

                # ------------------------------------------------------------------
                def __init__(self, name, log, timer, cb, cb_data, cb_lock):
                    self._name    = name
                    self._log     = log
                    self._timeout = timer
                    self._cb      = cb
                    self._cb_data = cb_data
                    self._cb_lock = cb_lock
                    self._last    = 0.0

                    super(Idler, self).__init__(name=self._name, log=self._log)

                    # immediately start the thread upon construction
                    self.start()

                # ------------------------------------------------------------------
                def work_cb(self):
                    self.is_valid()
                    if self._timeout and (time.time()-self._last) < self._timeout:
                        # not yet
                        time.sleep(0.1) # FIXME: make configurable
                        return True

                    with self._cb_lock:
                        if self._cb_data != None:
                            ret = self._cb(cb_data=self._cb_data)
                        else:
                            ret = self._cb()
                    if self._timeout:
                        self._last = time.time()
                    return ret
            # ----------------------------------------------------------------------

            idler = Idler(name=name, timer=timer, log=self._log,
                          cb=cb, cb_data=cb_data, cb_lock=self._cb_lock)
            self._threads[name] = idler

        self.register_watchable(idler)
        self._session._to_stop.append(idler)
        self._log.debug('%s registered idler %s', self.uid, name)


    # --------------------------------------------------------------------------
    #
    def unregister_timed_cb(self, cb):
        """
        This method is reverts the register_timed_cb() above: it
        removes an idler from the component, and will terminate the
        respective thread.
        """

        self.is_valid()

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
        """
        Using this method, the component can registered itself to be a publisher
        of notifications on the given pubsub channel.
        """

        self.is_valid()

        if pubsub in self._publishers:
            raise ValueError('publisher for %s already registered' % pubsub)

        # get address for pubsub
        if not pubsub in self._cfg['bridges']:
            self._log.error('no addr: %s' % pprint.pformat(self._cfg['bridges']))
            raise ValueError('no bridge known for pubsub channel %s' % pubsub)

        self._log.debug('START: %s register publisher %s', self.uid, pubsub)

        addr = self._cfg['bridges'][pubsub]['addr_in']
        self._log.debug("using addr %s for pubsub %s", addr, pubsub)

        q = rpu_Pubsub(self._session, pubsub, rpu_PUBSUB_PUB, self._cfg, addr=addr)
        self._publishers[pubsub] = q

        self._log.debug('registered publisher : %s : %s', pubsub, q.name)


    # --------------------------------------------------------------------------
    #
    def unregister_publisher(self, pubsub):
        """
        This removes the registration of a pubsub channel for publishing.
        """

        self.is_valid()

        if pubsub not in self._publishers:
            self._log.warn('publisher %s is not registered', pubsub)
          # raise ValueError('publisher for %s is not registered' % pubsub)
            return

        self._log.debug('TERM : %s unregister publisher %s', self.uid, pubsub)

        self._publishers[pubsub].stop()
        del(self._publishers[pubsub])
        self._log.debug('unregistered publisher %s', pubsub)


    # --------------------------------------------------------------------------
    #
    def register_subscriber(self, pubsub, cb, cb_data=None):
        """
        This method is complementary to the register_publisher() above: it
        registers a subscription to a pubsub channel.  If a notification
        is received on thag channel, the registered callback will be
        invoked.  The callback MUST have one of the signatures:

          callback(topic, msg)
          callback(topic, msg, cb_data)

        where 'topic' is set to the name of the pubsub channel.

        The subscription will be handled in a separate thread, which implies
        that the callback invocation will also happen in that thread.  It is the
        caller's responsibility to ensure thread safety during callback
        invocation.
        """

        self.is_valid()

        name = "%s.subscriber.%s" % (self.uid, cb.__name__)
        self._log.debug('START: %s register subscriber %s', self.uid, name)

        # get address for pubsub
        if not pubsub in self._cfg['bridges']:
            raise ValueError('no bridge known for pubsub channel %s' % pubsub)

        addr = self._cfg['bridges'][pubsub]['addr_out']
        self._log.debug("using addr %s for pubsub %s", addr, pubsub)

        # subscription is racey for the *first* subscriber: the bridge gets the
        # subscription request, and forwards it to the publishers -- and only
        # then can we expect the publisher to send any messages *at all* on that
        # channel.  Since we do not know if we are the first subscriber, we'll
        # have to wait a little to let the above happen, before we go further
        # and create any publishers.
        # FIXME: this barrier should only apply if we in fact expect a publisher
        #        to be created right after -- but it fits here better logically.
      # time.sleep(0.1)

        # ----------------------------------------------------------------------
        class Subscriber(ru.Thread):

            # ------------------------------------------------------------------
            def __init__(self, name, l, q, cb, cb_data, cb_lock):
                self._name     = name
                self._log      = l
                self._q        = q
                self._cb       = cb
                self._cb_data  = cb_data
                self._cb_lock  = cb_lock

                super(Subscriber, self).__init__(name=self._name, log=self._log)

                # immediately start the thread upon construction
                self.start()

            # ------------------------------------------------------------------
            def work_cb(self):
                self.is_valid()
                topic, msg = None, None
                try:
                    topic, msg = self._q.get_nowait(500)  # timout in ms
                except Exception as e:
                    if not self._ru_term.is_set():
                        # abort during termination
                        return False

                if topic and msg:
                    if not isinstance(msg,list):
                        msg = [msg]
                    for m in msg:
                        with self._cb_lock:
                            if self._cb_data != None:
                                ret = cb(topic=topic, msg=m, cb_data=self._cb_data)
                            else:
                                ret = self._cb(topic=topic, msg=m)
                        # we abort whenever a callback indicates thus
                        if not ret:
                            return False
                return True
            def ru_finalize_common(self):
                self._q.stop()
        # ----------------------------------------------------------------------
        # create a pubsub subscriber (the pubsub name doubles as topic)
        # FIXME: this should be moved into the thread child_init
        q = rpu_Pubsub(self._session, pubsub, rpu_PUBSUB_SUB, self._cfg, addr=addr)
        q.subscribe(pubsub)

        subscriber = Subscriber(name=name, l=self._log, q=q, 
                                cb=cb, cb_data=cb_data, cb_lock=self._cb_lock)

        with self._cb_lock:
            self._threads[name] = subscriber

        self.register_watchable(subscriber)
        self._session._to_stop.append(subscriber)
        self._log.debug('%s registered %s subscriber %s', self.uid, pubsub, name)


    # --------------------------------------------------------------------------
    #
    def unregister_subscriber(self, pubsub, cb):
        """
        This method is reverts the register_subscriber() above: it
        removes a subscription from a pubsub channel, and will terminate the
        respective thread.
        """

        self.is_valid()

        name = "%s.subscriber.%s" % (self.uid, cb.__name__)
        self._log.debug('TERM : %s unregister subscriber %s', self.uid, name)

        with self._cb_lock:
            if name not in self._threads:
                self._log.warn('subscriber %s is not registered', cb.__name__)
              # raise ValueError('%s is not subscribed to %s' % (cb.__name__, pubsub))
                return

            self._threads[name]  # implies join
            del(self._threads[name])

        self._log.debug("unregistered subscriber %s", name)


    # --------------------------------------------------------------------------
    #
    def watch_common(self):
        # FIXME: this is not used at the moment
        '''
        This method is called repeatedly in the ru.Process watcher loop.  We use
        it to watch all our threads, and will raise an exception if any of them
        disappears.  This will initiate the ru.Process termination sequence.
        '''

        self.is_valid()

        with self._cb_lock:
            for tname in self._threads:
                if not self._threads[tname].is_alive():
                    raise RuntimeError('%s thread %s died', self.uid, tname)


    # --------------------------------------------------------------------------
    #
    def work_cb(self):
        """
        This is the main routine of the component, as it runs in the component
        process.  It will first initialize the component in the process context.
        Then it will attempt to get new things from all input queues
        (round-robin).  For each thing received, it will route that thing to the
        respective worker method.  Once the thing is worked upon, the next
        attempt on getting a thing is up.
        """

        self.is_valid()

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
            things = input.get_nowait(1000) # timeout in microseconds

            if not things:
                return True

            if not isinstance(things, list):
                things = [things]

            # the worker target depends on the state of things, so we 
            # need to sort the things into buckets by state before 
            # pushing them
            buckets = dict()
            for thing in things:
                
                state = thing['state']
                uid   = thing['uid']
                self._prof.prof('get', uid=uid, state=state)

                if not state in buckets:
                    buckets[state] = list()
                buckets[state].append(thing)

            # We now can push bulks of things to the workers

            for state,things in buckets.iteritems():

                assert(state in states), 'inconsistent state'
                assert(state in self._workers), 'no worker for state %s' % state

                try:
                    to_cancel = list()
                    for thing in things:
                        uid   = thing['uid']
                        ttype = thing['type']
                        state = thing['state']

                        # FIXME: this can become expensive over time
                        #        if the cancel list is never cleaned
                        if uid in self._cancel_list:
                            with self._cancel_lock:
                                self._cancel_list.remove(uid)
                            to_cancel.append(thing)

                        self._log.debug('got %s (%s)', ttype, uid)

                    if to_cancel:
                        self.advance(to_cancel, rps.CANCELED, publish=True, push=False)

                    with self._cb_lock:
                        self._workers[state](things)

                except Exception as e:

                    # this is not fatal -- only the 'things' fail, not
                    # the component
                    self._log.exception("worker %s failed", self._workers[state])
                    self.advance(things, rps.FAILED, publish=True, push=False)


        # keep work_cb registered
        return True


    # --------------------------------------------------------------------------
    #
    def advance(self, things, state=None, publish=True, push=False,
                timestamp=None, prof=True):
        """
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
        """

        self.is_valid()

        if not timestamp:
            timestamp = time.time()

        if not isinstance(things, list):
            things = [things]

        self._log.debug('advance bulk size: %s [%s, %s]', len(things), push, publish)

        # assign state, sort things by state
        buckets = dict()
        for thing in things:

            uid   = thing['uid']
            ttype = thing['type']

            if ttype not in ['unit', 'pilot']:
                raise TypeError("thing has unknown type (%s)" % uid)

            if state:
                # state advance done here
                thing['state'] = state
            _state = thing['state']

            if prof:
                self._prof.prof('advance', uid=uid, state=_state,
                                timestamp=timestamp)

            if not _state in buckets:
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
                    to_publish.append(thing)

                elif thing['state'] in rps.FINAL:
                    to_publish.append(thing)

                else:
                    tmp = {'uid'   : thing['uid'],
                           'type'  : thing['type'],
                           'state' : thing['state']}
                    for key in thing.get('$set', []):
                        tmp[key] = thing[key]
                    to_publish.append(tmp)

            self.publish(rpc.STATE_PUBSUB, {'cmd': 'update', 'arg': to_publish})
            ts = time.time()
            for thing in things:
                self._prof.prof('publish', uid=thing['uid'], 
                                state=thing['state'], timestamp=ts)

        # never carry $all across component boundaries!
        else:
            for thing in things:
                if '$all' in thing:
                    del(thing['$all'])

        # should we push things downstream, to the next component
        if push:

            # the push target depends on the state of things, so we need to sort
            # the things into buckets by state before pushing them
            # now we can push the buckets as bulks
            for _state,_things in buckets.iteritems():

                ts = time.time()
                if _state in rps.FINAL:
                    # things in final state are dropped
                    for thing in _things:
                        self._log.debug('final %s [%s]', thing['uid'], _state)
                        self._prof.prof('drop', uid=thing['uid'], state=_state,
                                        timestamp=ts)
                    continue

                if _state not in self._outputs:
                    # unknown target state -- error
                    for thing in _things:
                        self._log.debug("lost  %s [%s]", thing['uid'], _state)
                        self._prof.prof('lost', uid=thing['uid'], state=_state,
                                        timestamp=ts)
                    continue

                if not self._outputs[_state]:
                    # empty output -- drop thing
                    for thing in _things:
                        self._log.debug('drop  %s [%s]', thing['uid'], _state)
                        self._prof.prof('drop', uid=thing['uid'], state=_state,
                                        timestamp=ts)
                    continue

                output = self._outputs[_state]

                # push the thing down the drain
                self._log.debug('put bulk %s: %s', _state, len(_things))
                output.put(_things)

                ts = time.time()
                for thing in _things:
                    self._prof.prof('put', uid=thing['uid'], state=_state,
                                    msg=output.name, timestamp=ts)


    # --------------------------------------------------------------------------
    #
    def publish(self, pubsub, msg):
        """
        push information into a publication channel
        """

        self.is_valid()

        if pubsub not in self._publishers:
            raise RuntimeError("can't route '%s' notification: %s" % (pubsub,
                self._publishers.keys()))

        if not self._publishers[pubsub]:
            raise RuntimeError("no route for '%s' notification: %s" % (pubsub, msg))

        self._publishers[pubsub].put(pubsub, msg)



# ==============================================================================
#
class Worker(Component):
    """
    A Worker is a Component which cannot change the state of the thing it
    handles.  Workers are emplyed as helper classes to mediate between
    components, between components and database, and between components and
    notification channels.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session=None):

        Component.__init__(self, cfg=cfg, session=session)


# ------------------------------------------------------------------------------

