
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


# ------------------------------------------------------------------------------
#
import cProfile
class ProfiledThread(mt.Thread):
    # Overrides threading.Thread.run()
    def run(self):
        profiler = cProfile.Profile()
        try:
            return profiler.runcall(mt.Thread.run, self)
        finally:
            self_thread = mt.current_thread()
            profiler.dump_stats('python-%s.profile' % (self_thread.name))


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

    The main event loop of the component -- run() -- is executed as a separate
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
        if not hasattr(self, 'uid'):
            raise ValueError('class which inherits Component needs a uid')

        # state we carry over the fork
        self._started   = False
        self._debug     = cfg.get('debug', 'DEBUG')
        self._owner     = cfg.get('owner', self.uid)
        self._ctype     = "%s.%s" % (self.__class__.__module__,
                                     self.__class__.__name__)
        self._number    = cfg.get('number', 0)
        self._name      = cfg.get('name.%s' %  self._number,
                                  '%s.%s'   % (self._ctype, self._number))

        if self._owner == self.uid:
            self._owner = 'root'

        self._log  = self._session._get_logger(self.uid, level=self._debug)

        # initialize the Process base class for later fork.
        ru.Process.__init__(self, name=self.uid, log=self._log)


    # --------------------------------------------------------------------------
    #
    def __str__(self):
        return "%s <%s> [%s]" % (self.uid, self.__class__.__name__, self._owner)


    # --------------------------------------------------------------------------
    #
    def _cancel_monitor_cb(self, topic, msg):
        """
        We listen on the control channel for cancel requests, and append any
        found UIDs to our cancel list.
        """
        
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
        else:
            pass
          # self._log.debug('command ignored: %s', cmd)


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
        return self.uid

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
        return self._rup_is_parent

    @property
    def is_child(self):
        return self._rup_is_child

    @property
    def has_child(self):
        return self.is_parent and self.pid


    # --------------------------------------------------------------------------
    #
    def initialize_common(self):
        """
        This private method contains initialization for both parent a child
        process, which gets the component into a proper functional state.

        This method must be called *after* fork (this is asserted).
        """

        assert(not self._started)

        self._inputs        = dict()       # queues to get things from
        self._outputs       = dict()       # queues to send things to
        self._workers       = dict()       # methods to work on things
        self._publishers    = dict()       # channels to send notifications to
        self._threads       = dict()       # subscriber and idler threads

        self._cb_lock       = mt.RLock()   # guard threaded callback invokations

        # get debugging, logging, profiling set up
        self._dh   = ru.DebugHelper(name=self.uid)
        self._log  = self._session._get_logger(self.uid, level=self._debug)
        self._prof = self._session._get_profiler(self.uid)

        self._prof.prof('initialize', uid=self.uid)
        self._log.info('initialize %s',   self.uid)
        self._log.info('cfg: %s', pprint.pformat(self._cfg))

        # all components need at least be able to talk to a control pubsub
        assert('bridges' in self._cfg)
        assert(rpc.LOG_PUBSUB     in self._cfg['bridges'])
        assert(rpc.STATE_PUBSUB   in self._cfg['bridges'])
        assert(rpc.CONTROL_PUBSUB in self._cfg['bridges'])
        assert(self._cfg['bridges'][rpc.LOG_PUBSUB    ]['addr_in'])
        assert(self._cfg['bridges'][rpc.STATE_PUBSUB  ]['addr_in'])
        assert(self._cfg['bridges'][rpc.CONTROL_PUBSUB]['addr_in'])

        # components can always publish logs, state updates and send control messages
        self.register_publisher(rpc.LOG_PUBSUB)
        self.register_publisher(rpc.STATE_PUBSUB)
        self.register_publisher(rpc.CONTROL_PUBSUB)


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):
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


    # --------------------------------------------------------------------------
    #
    def finalize_common(self):

        # reverse order from initialize_common
        self.unregister_publisher(rpc.LOG_PUBSUB)
        self.unregister_publisher(rpc.STATE_PUBSUB)
        self.unregister_publisher(rpc.CONTROL_PUBSUB)

        self._log.debug('%s close prof', self.uid)
        try:
            self._prof.prof("stopped", uid=self.name)
            self._prof.close()
        except Exception:
            pass


    # --------------------------------------------------------------------------
    #
    def stop(self, timeout=None):
        '''
        Before calling the ru.Process stop, we need to terminate and join all
        threads, as those might otherwise invoke callback which can interfere
        with termination.
        '''

        self._log.debug('stop %s (%s : %s : %s) [%s]', self.uid, os.getpid(),
                        self.pid, mt.current_thread().name,
                        ru.get_caller_name())

        for _,t in self._threads.iteritems(): t['term'  ].set()
        for _,t in self._threads.iteritems(): t['thread'].join()

        return ru.Process.stop(timeout)


    # --------------------------------------------------------------------------
    #
    def register_input(self, states, input, worker):
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

        if not isinstance(states, list):
            states = [states]


        name = '%s.%s.%s' % (self.uid, worker.__name__, '_'.join(states))

        if name in self._inputs:
            raise ValueError('input %s already registered' % name)

        # get address for the queue
        addr = self._cfg['bridges'][input]['addr_out']
        self._log.debug("using addr %s for input %s" % (addr, input))

        q = rpu_Queue(self._session, input, rpu_QUEUE_OUTPUT, self._cfg, addr=addr)
        self._inputs['name'] = {'queue'  : q,
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

        if not isinstance(states, list):
            states = [states]

        name = '%s.%s.%s' % (self.uid, worker.__name__, '_'.join(states))

        if name not in self._inputs:
            raise ValueError('input %s not registered' % name)
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
                self._log.debug("using addr %s for output %s" % (addr, output))

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

        if not isinstance(states, list):
            states = [states]

        for state in states:
            self._log.debug('TERM : %s unregister output %s', self.uid, state)
            if not state in self._outputs:
                raise ValueError('state %s hasno output to unregister' % state)
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

        name = "%s.idler.%s" % (self.uid, cb.__name__)
        self._log.debug('START: %s register idler %s', self.uid, name)

        with self._cb_lock:
            if name in self._threads:
                raise ValueError('cb %s already registered' % cb.__name__)

        if None != timer:
            timer = float(timer)

        # create a separate thread per idle cb
        #
        # NOTE: idle timing is a tricky beast: if we sleep for too long, then we
        #       have to wait that long on stop() for the thread to get active
        #       again and terminate/join.  So we always sleep just a little, and
        #       explicitly check if sufficient time has passed to activate the
        #       callback.
        # ----------------------------------------------------------------------
        def _idler(term):
            try:
                last = 0.0  # never been called
                while not term.is_set():
                    now = time.time()
                    if timer == None or (now-last) > timer:
                        with self._cb_lock:
                            if cb_data != None:
                                cb(cb_data=cb_data)
                            else:
                                cb()
                        last = now
                    if timer:
                        time.sleep(0.1)
            except Exception as e:
                self._log.exception("TERM : %s idler failed %s", self.uid, mt.current_thread().name)
            finally:
                self._log.debug("TERM : %s idler final %s", self.uid, mt.current_thread().name)
        # ----------------------------------------------------------------------

        # create a idler thread
        e = mt.Event()
        t = mt.Thread(target=_idler, args=[e], name=name)
        t.daemon = True
        t.start()

        with self._cb_lock:
            self._threads[name] = {'term'   : e,  # termination signal
                                   'thread' : t}  # thread handle

        self._log.debug('%s registered idler %s' % (self.uid, t.name))


    # --------------------------------------------------------------------------
    #
    def unregister_timed_cb(self, cb):
        """
        This method is reverts the register_timed_cb() above: it
        removes an idler from the component, and will terminate the
        respective thread.
        """

        name = "%s.idler.%s" % (self.uid, cb.__name__)
        self._log.debug('TERM : %s unregister idler %s', self.uid, name)

        with self._cb_lock:
            if name not in self._threads:
                raise ValueError('%s is not registered' % name)

            entry = self._threads[name]
            entry['term'].set()
            entry['thread'].join()
            del(self._threads[name])

        self._log.debug("TERM : %s unregistered idler %s", self.uid, name)


    # --------------------------------------------------------------------------
    #
    def register_publisher(self, pubsub):
        """
        Using this method, the component can registered itself to be a publisher
        of notifications on the given pubsub channel.
        """

        if pubsub in self._publishers:
            raise ValueError('publisher for %s already registered' % pubsub)

        # get address for pubsub
        if not pubsub in self._cfg['bridges']:
            self._log.error('no addr: %s' % pprint.pformat(self._cfg['bridges']))
            raise ValueError('no bridge known for pubsub channel %s' % pubsub)

        self._log.debug('START: %s register publisher %s', self.uid, pubsub)

        addr = self._cfg['bridges'][pubsub]['addr_in']
        self._log.debug("using addr %s for pubsub %s" % (addr, pubsub))

        q = rpu_Pubsub(self._session, pubsub, rpu_PUBSUB_PUB, self._cfg, addr=addr)
        self._publishers[pubsub] = q

        self._log.debug('registered publisher : %s : %s' % (pubsub, q.name))


    # --------------------------------------------------------------------------
    #
    def unregister_publisher(self, pubsub):
        """
        This removes the registration of a pubsub channel for publishing.
        """

        if pubsub not in self._publishers:
            raise ValueError('publisher for %s is not registered' % pubsub)

        self._log.debug('TERM : %s unregister publisher %s', self.uid, pubsub)

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

        name = "%s.subscriber.%s" % (self.uid, cb.__name__)
        self._log.debug('START: %s unregister subscriber %s', self.uid, name)

        # get address for pubsub
        if not pubsub in self._cfg['bridges']:
            raise ValueError('no bridge known for pubsub channel %s' % pubsub)

        addr = self._cfg['bridges'][pubsub]['addr_out']
        self._log.debug("using addr %s for pubsub %s" % (addr, pubsub))

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
        def _subscriber(q, term):

            try:
                while not term.is_set():
                    topic, msg = q.get_nowait(1000) # timout in ms
                    if topic and msg:
                        if not isinstance(msg,list):
                            msg = [msg]
                        for m in msg:
                            with self._cb_lock:
                                if cb_data != None:
                                    cb(topic=topic, msg=m, cb_data=cb_data)
                                else:
                                    cb(topic=topic, msg=m)
                self._log.debug("x< %s:%s: %s", self.uid, cb.__name__, topic)
            except Exception as e:
                self._log.exception("subscriber failed %s", mt.current_thread().name)
        # ----------------------------------------------------------------------

        with self._cb_lock:
            if name in self._threads:
                raise ValueError('cb %s already registered for %s' % (cb.__name__, pubsub))

            if pubsub in os.getenv("RADICAL_PILOT_CPROFILE_SUBSCRIBERS", "").split():
                ttype = ProfiledThread
                tname = name="%s-%s.subscriber" % (self.uid, pubsub)
            else:
                ttype = mt.Thread
                tname = name="%s.subscriber" % self.uid

            # create a pubsub subscriber (the pubsub name doubles as topic)
            q = rpu_Pubsub(self._session, pubsub, rpu_PUBSUB_SUB, self._cfg, addr=addr)
            q.subscribe(pubsub)

            e = mt.Event()
            t = ttype(target=_subscriber, args=[q,e], name=tname)
            t.start()

            self._threads[name] = {'term'   : e,  # termination signal
                                   'thread' : t}  # thread handle

        self._log.debug('%s registered %s subscriber %s' % (self.uid, pubsub, t.name))


    # --------------------------------------------------------------------------
    #
    def unregister_subscriber(self, pubsub, cb):
        """
        This method is reverts the register_subscriber() above: it
        removes a subscription from a pubsub channel, and will terminate the
        respective thread.
        """

        name = "%s.subscriber.%s" % (self.uid, cb.__name__)
        self._log.debug('TERM : %s unregister subscriber %s', self.uid, name)

        with self._cb_lock:
            if name not in self._threads:
                raise ValueError('%s is not subscribed to %s' % (cb.__name__, pubsub))

            entry = self._threads[name]
            entry['term'].set()
            entry['thread'].join()
            del(self._threads[name])

        self._log.debug("unregistered %s", name)


    # --------------------------------------------------------------------------
    #
    def watch_common(self):
        '''
        This method is called repeatedly in the ru.Process watcher loop.  We use
        it to watch all our threads, and will raise an exception if any of them
        disappears.  This will initiate the ru.Process termination sequence.
        '''

        for n,t in self._threads.iteritems():
            if not t['thread'].is_alive():
                raise RuntimeError('%s idler %s died', self.uid, n)


    # --------------------------------------------------------------------------
    #
    def work(self):
        """
        This is the main routine of the component, as it runs in the component
        process.  It will first initialize the component in the process context.
        Then it will attempt to get new things from all input queues
        (round-robin).  For each thing received, it will route that thing to the
        respective worker method.  Once the thing is worked upon, the next
        attempt on getting a thing is up.
        """

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

          # self._log.debug(' === input bulk %s things on %s' % (len(things), name))

            # the worker target depends on the state of things, so we 
            # need to sort the things into buckets by state before 
            # pushing them
            buckets = dict()
            for thing in things:
                state = thing['state']
                if not state in buckets:
                    buckets[state] = list()
                buckets[state].append(thing)

            # We now can push bulks of things to the workers

            for state,things in buckets.iteritems():

                assert(state in states)
                assert(state in self._workers)

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
                        self._prof.prof(event='get', state=state, uid=uid, msg=input.name)
                        self._prof.prof(event='work start', state=state, uid=uid)

                    if to_cancel:
                        self.advance(to_cancel, rps.CANCELED, publish=True, push=False)

                    with self._cb_lock:
                      # self._log.debug(' === work on bulk [%s]', len(things))
                        self._workers[state](things)

                    for thing in things:
                        self._prof.prof(event='work done ', state=state, uid=uid)

                except Exception as e:
                    # this is not fatal -- only the 'things' fail, not
                    # the component

                    self._log.exception("worker %s failed", self._workers[state])
                    self.advance(things, rps.FAILED, publish=True, push=False)

                    for thing in things:
                        self._prof.prof(event='failed', msg=str(e), 
                                        uid=thing['uid'], state=state)


    # --------------------------------------------------------------------------
    #
    def advance(self, things, state=None, publish=True, push=False,
                timestamp=None):
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

        if not timestamp:
            timestamp = time.time()

        if not isinstance(things, list):
            things = [things]

        self._log.debug(' === advance bulk size: %s [%s, %s]', len(things), push, publish)

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
            else:
                # state advance was done by caller
                state = thing['state']

            self._log.debug(' === advance bulk: %s [%s]', uid, len(things))
            self._prof.prof('advance', uid=uid, state=state, timestamp=timestamp)

            if not state in buckets:
                buckets[state] = list()
            buckets[state].append(thing)

        # should we publish state information on the state pubsub?
        if publish:

            to_publish = list()

            # If '$all' is set, we update the complete thing_dict.  
            # Things in final state are also published in full.
            # In all other cases, we only send 'uid', 'type' and 'state'.
            for thing in things:
                if '$all' in thing:
                    del(thing['$all'])
                    to_publish.append(thing)

                elif state in rps.FINAL:
                    to_publish.append(thing)

                else:
                    to_publish.append({'uid'   : thing['uid'],
                                       'type'  : thing['type'],
                                       'state' : state})

            self.publish(rpc.STATE_PUBSUB, {'cmd': 'update', 'arg': to_publish})
            ts = time.time()
            for thing in things:
                self._prof.prof('publish', uid=thing['uid'], state=thing['state'], timestamp=ts)

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
            for state,things in buckets.iteritems():

                self._log.debug(" === bucket: %s : %s", state, [t['uid'] for t in things])

                if state in rps.FINAL:
                    # things in final state are dropped
                    for thing in things:
                        self._log.debug('push %s ===| %s', thing['uid'], thing['state'])
                    continue

                if state not in self._outputs:
                    # unknown target state -- error
                    self._log.error("%s", ru.get_stacktrace())
                    self._log.error("%s can't route state for %s: %s (%s)" \
                            % (self.uid, things[0]['uid'], state, self._outputs.keys()))

                    continue

                if not self._outputs[state]:
                    # empty output -- drop thing
                    for thing in things:
                        self._log.debug('%s %s ~~~| %s' % ('push', thing['uid'], thing['state']))
                    continue

                output = self._outputs[state]

                # push the thing down the drain
                # FIXME: we should assert that the things are in a PENDING state.
                #        Better yet, enact the *_PENDING transition right here...
                self._log.debug(' === put bulk %s: %s', state, len(things))
                output.put(things)

                ts = time.time()
                for thing in things:
                    
                    # never carry $all across component boundaries!
                    if '$all' in thing:
                        del(thing['$all'])

                    uid   = thing['uid']
                    state = thing['state']

                    self._log.debug('push %s ---> %s', uid, state)
                    self._prof.prof('put', uid=uid, state=state,
                            msg=output.name, timestamp=ts)


    # --------------------------------------------------------------------------
    #
    def publish(self, pubsub, msg):
        """
        push information into a publication channel
        """

        if pubsub not in self._publishers:
            raise RuntimeError("can't route '%s' notification: %s" % (pubsub, msg))

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


    # --------------------------------------------------------------------------
    #
    # we overload state changing methods from component and assert neutrality
    # FIXME: we should insert hooks around callback invocations, too
    #
    def advance(self, things, state=None, publish=True, push=False, prof=True):

        if state:
            raise RuntimeError("worker %s cannot advance state (%s)"
                    % (self.uid, state))

        Component.advance(self, things, state, publish, push, prof)


# ------------------------------------------------------------------------------

