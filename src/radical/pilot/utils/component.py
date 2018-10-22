
import copy
import time

import threading       as mt
import radical.utils   as ru

from ..      import constants  as rpc
from ..      import states     as rps

from .queue  import Putter     as rpu_Putter
from .queue  import Getter     as rpu_Getter

from .pubsub import Publisher  as rpu_Publisher
from .pubsub import Subscriber as rpu_Subscriber


# ==============================================================================
#
class Component(object):
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
        finalize

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
    def create(cfg):
        '''
        '''

        # ----------------------------------------------------------------------
        # NOTE:  I'd rather have this as class data than as stack data, but
        #        python stumbles over circular imports at that point :/
        #        Another option though is to discover and dynamically load
        #        components.
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

        sid  = cfg['sid']
        uid  = cfg['uid']
        kind = cfg['kind']

        from .. import session as rp_session
        session = rp_session.Session(uid=sid, _cfg=cfg)
        log     = session.get_logger(uid)
        log.debug('start component %s [%s]', uid, kind)

        if kind not in _ctypemap:
            raise ValueError('unknown component type (%s)' % kind)

        ctype = _ctypemap[kind]
        comp = ctype.create(cfg, session)

        return comp


    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        # NOTE: a fork will not duplicate any threads of the parent process --
        #       but it will duplicate any locks which are shared between the
        #       parent process and its threads -- and those locks might be in
        #       any state at this point.  As such, each child has to make
        #       sure to never, ever, use any of the inherited locks, but instead
        #       to create it's own set of locks in self.initialize

        self._cfg     = copy.deepcopy(cfg)
        self._session = session

        # state we carry over the fork
        ## FIXME
        self._uid         = self._cfg['uid']
        self._debug       = cfg.get('debug')

        self._inputs      = dict()       # queues to get things from
        self._outputs     = dict()       # queues to send things to
        self._workers     = dict()       # methods to work on things
        self._publishers  = dict()       # channels to send notifications to
        self._cb_lock     = mt.RLock()   # guard threaded callback invokations
        self._terminate   = mt.Event()   # signal work termination

        self._cancel_list = list()       # list of units to cancel
        self._cancel_lock = mt.RLock()   # lock for above list

        self._prof = self._session.get_profiler(name=self.uid)
        self._rep  = self._session.get_reporter(name=self.uid)
        self._log  = self._session.get_logger  (name=self.uid,
                                                level=self._debug)
      # self._prof.register_timing(name='component_lifetime',
      #                            scope='uid=%s' % self.uid,
      #                            start='component_start',
      #                            stop='component_stop')
      # self._prof.register_timing(name='entity_runtime',
      #                            scope='entity',
      #                            start='get',
      #                            stop=['put', 'drop'])


        # components can always publish logs, state updates and control messages
        self.register_publisher(rpc.LOG_PUBSUB)
        self.register_publisher(rpc.STATE_PUBSUB)
        self.register_publisher(rpc.CONTROL_PUBSUB)

        # always subscribe to control messages
        self.register_subscriber(rpc.CONTROL_PUBSUB, self._control_cb)

        # call component level initialize
        self.initialize()

        # signal completion of startup
        if 'fchk' in self._cfg:
            with open(self._cfg['fchk'], 'w') as fout:
                fout.write('ok\n')



    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):     return self._uid 
    @property
    def session(self): return self._session


    # --------------------------------------------------------------------------
    #
    def initialize(self):
        pass  # can be overloaded


    # --------------------------------------------------------------------------
    #
    def close(self):

        self._log.debug('close')

        self.finalize()

        self._prof.prof('component_final')
        self._prof.close()


    # --------------------------------------------------------------------------
    #
    def finalize(self):
        pass  # can be overloaded


    # --------------------------------------------------------------------------
    #
    def is_valid(self, term=True):
        '''
        Just as the Process' `is_valid()` call, we make sure that the component
        is still viable, and will raise an exception if not.  Additionally to
        the health of the component's child process, we also check health of any
        sub-components and communication bridges.
        '''

        ## FIXME: check thread liveliness

        return True


    # --------------------------------------------------------------------------
    #
    def _control_cb(self, topic, msg):
        """
        We listen on the control channel for cancel requests, and append any
        found UIDs to our cancel list.
        """

        self.is_valid()

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
    def register_input(self, states, input, worker):
        """
        Using this method, the component can be connected to a queue on which
        things are received to be worked upon.  The given set of states (which
        can be a single state or a list of states) will trigger an assert check
        upon thing arrivals.

        This method will further associate a thing state with a specific worker.
        Upon thing arrival, the thing state will be used to lookup the
        respective worker, and the thing will be handed over.  Workers should
        call self.advance(thing), in order to push the thing toward the next
        component.  If, for some reason, that is not possible before the worker
        returns, the component will retain ownership of the thing, and should
        call advance() asynchronously at a later point in time.

        Worker invocation is synchronous, ie. the main event loop will only
        check for the next thing once the worker method returns.
        """

        self.is_valid()

        if not isinstance(states, list):
            states = [states]

        name = '%s.%s.%s' % (self.uid, worker.__name__, '_'.join(states))

        if name in self._inputs:
            raise ValueError('input %s already registered' % name)

        q = rpu_Getter(input, self._session)
        self._inputs[name] = {'queue'  : q,
                              'states' : states}

        self._log.debug('registered input %s', name)

        # we want exactly one worker associated with a state -- but a worker can
        # be responsible for multiple states
        for state in states:

            self._log.debug('%s register input %s: %s', self.uid, state, name)

            if state in self._workers:
                self._log.warn("%s replaces worker for %s (%s)"
                        % (self.uid, state, self._workers[state]))
            self._workers[state] = worker

            self._log.debug('registered worker %s [%s]', worker.__name__, state)


    # --------------------------------------------------------------------------
    #
    def register_output(self, states, output=None):
        """
        Using this method, the component can be connected to a queue to which
        things are sent after being worked upon.  The given set of states (which
        can be a single state or a list of states) will trigger an assert check
        upon thing departure.

        If a state but no output is specified, we assume that the state is
        final, and the thing is then considered 'dropped' on calling advance()
        on it.  The advance() will trigger a state notification though, and then
        mark the drop in the log.  No other component should ever again work on
        such a final thing.  It is the responsibility of the component to make
        sure that the thing is in fact in a final state.
        """

        self.is_valid()

        if not isinstance(states, list):
            states = [states]

        for state in states:

            self._log.debug('%s register output %s', self.uid, state)

            # we want a *unique* output queue for each state.
            if state in self._outputs:
                self._log.warn('%s replaces output for %s : %s -> %s'
                        % (self.uid, state, self._outputs[state], output))

            if not output:
                # this indicates a final state
                self._outputs[state] = None
            else:
                # non-final state, ie. we want a queue to push to
                q = rpu_Putter(output, self._session)
                self._outputs[state] = q

                self._log.debug('registered output    : %s : %s : %s'
                     % (state, output, q.name))


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
        self._log.debug('%s register idler %s', self.uid, name)

        if timer is None: timer = 0.0  # NOTE: busy idle loop
        else            : timer = float(timer)

        # create a separate daemon thread per idle cb
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

            # ------------------------------------------------------------------
            def work_cb(self):
                self.is_valid()
                if self._timeout and (time.time()-self._last) < self._timeout:
                    # not yet
                    time.sleep(0.1)  # FIXME: make configurable
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

        # daemonize and start the thread upon construction
        idler = Idler(name=name, timer=timer, log=self._log,
                      cb=cb, cb_data=cb_data, cb_lock=self._cb_lock)
        idler.dameon = True
        idler.start()

        self._log.debug('%s registered idler %s', self.uid, name)


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

        self._log.debug('%s register publisher %s', self.uid, pubsub)

        q = rpu_Publisher(pubsub, self._session)
        self._publishers[pubsub] = q

        self._log.debug('registered publisher : %s : %s', pubsub, q.name)


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
        self._log.debug('%s register subscriber %s', self.uid, name)

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

                self._x = False
                super(Subscriber, self).__init__(name=self._name, log=self._log)

            # ------------------------------------------------------------------
            def work_cb(self):

                self.is_valid()

                try:
                    topic, msg = self._q.get_nowait(500)  # timout in ms
                except Exception:
                    if self._ru_term.is_set():
                        # abort during termination
                        return False
                    else:
                        self._log.exception('get interrupted in %s', self._name)
                        return True

                if topic and msg:
                    if not isinstance(msg,list):
                        msg = [msg]
                    for m in msg:
                        with self._cb_lock:
                            if self._cb_data is not None:
                                ret = cb(topic=topic, msg=m, cb_data=self._cb_data)
                            else:
                                ret = self._cb(topic=topic, msg=m)
                        # we abort whenever a callback indicates thus
                        if not ret:
                            self._log.debug('cb failed on %s: %s', self._name, str(self._cb))
                            return False
                return True

            def ru_finalize_common(self):
                self._log.debug('finalize subscriber %s', self._name)
              # self._q.stop()
        # ----------------------------------------------------------------------
        # create a pubsub subscriber (the pubsub name doubles as topic)
        # FIXME: this should be moved into the thread child_init
        q = rpu_Subscriber(pubsub, self._session)
        q.subscribe(pubsub)

        subscriber = Subscriber(name=name, l=self._log, q=q, 
                                cb=cb, cb_data=cb_data, cb_lock=self._cb_lock)
        # daemonize and start the thread upon construction
        subscriber.daemon = True
        subscriber.start()

        self._log.debug('%s registered %s subscriber %s', self.uid, pubsub, name)


    # --------------------------------------------------------------------------
    #
    def start(self):

        def _work():
            while not self._terminate.is_set():
                if not self.work_cb():
                    return

        self._work_thread = mt.Thread(target=_work)
        self._work_thread.daemon = True
        self._work_thread.start()


    # --------------------------------------------------------------------------
    #
    def stop(self):

        self._terminate.set()
        self.wait()


    # --------------------------------------------------------------------------
    #
    def wait(self):

        # we don't use join, as that negates the daemon setting
        while self._work_thread.is_alive():
            time.sleep(0.1)


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
            things = input.get_nowait(1000)  # timeout in microseconds

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

                if state not in buckets:
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

                except Exception:
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

        self._log.debug('=== advance bulk size: %s [%s, %s]', len(things), push, publish)

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

            self._log.debug('=== adv 1 %s [%s]', uid, state)

            if prof:
                self._prof.prof('advance', uid=uid, state=_state,
                                timestamp=timestamp)

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

                self._log.debug('=== adv 2 %s [%s]', _state, len(things))

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
                self._log.debug('=== put bulk %s: %s', _state, len(_things))
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

        if not self._publishers.get(pubsub):
            raise RuntimeError("can't route notification '%s'" % pubsub)

        self._log.debug('pub %s', msg)
        self._log.debug('====== x4 %s', [pubsub, msg])
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

