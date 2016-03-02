
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import copy
import threading

import radical.utils as ru

from ... import utils     as rpu
from ... import states    as rps
from ... import constants as rpc


# ------------------------------------------------------------------------------
# 'enum' for RPs's umgr scheduler types
SCHEDULER_DIRECT       = "direct"
SCHEDULER_ROUND_ROBIN  = "round_robin"
SCHEDULER_BACKFILLING  = "backfilling"

# default:
SCHEDULER_DEFAULT      = SCHEDULER_ROUND_ROBIN

# internally used enums for pilot roles
ROLE    = '_scheduler_role'
ADDED   = 'added'
REMOVED = 'removed'
FAILED  = 'failed'



# ==============================================================================
#
class UMGRSchedulingComponent(rpu.Component):

    # FIXME: clarify what can be overloaded by Scheduler classes

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self._uid = ru.generate_id('umgr.scheduling.%(counter)s', ru.ID_CUSTOM)

        rpu.Component.__init__(self, cfg, session)

        self._umgr = self._owner


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):

        self._pilots      = dict()             # set of pilots to schedule over
        self._pilots_lock = threading.RLock()  # lock on the above set

        # configure the scheduler instance
        self._configure()

        self.declare_input(rps.UMGR_SCHEDULING_PENDING,
                           rpc.UMGR_SCHEDULING_QUEUE, self.work)

        self.declare_output(rps.UMGR_STAGING_INPUT_PENDING, rpc.UMGR_STAGING_INPUT_QUEUE)

        # Some schedulers care about states (of pilots and/or units), some
        # don't.  Either way, we here subscribe to state updates.
        self.declare_subscriber('state', rpc.STATE_PUBSUB, self.base_state_cb)

        # Schedulers use that command channel to get information about
        # pilots being added or removed.
        self.declare_subscriber('control', rpc.CONTROL_PUBSUB, self.base_command_cb)


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Scheduler.
    #
    @classmethod
    def create(cls, cfg, session):

        # Make sure that we are the base-class!
        if cls != UMGRSchedulingComponent:
            raise TypeError("Scheduler Factory only available to base class!")

        name = cfg['scheduler']

        from .direct       import Direct
        from .round_robin  import RoundRobin
        from .backfilling  import Backfilling

        try:
            impl = {
                SCHEDULER_DIRECT      : Direct,
                SCHEDULER_ROUND_ROBIN : RoundRobin,
                SCHEDULER_BACKFILLING : Backfilling
            }[name]

            impl = impl(cfg, session)
            return impl

        except KeyError:
            raise ValueError("Scheduler '%s' unknown or defunct" % name)


    # --------------------------------------------------------------------------
    #
    def base_state_cb(self, topic, msg):

        # the base class will keep track of pilot state changes and updates
        # self._pilots accordingly.  Unit state changes will be ignored -- if
        # a scheduler needs to keep track of those, it will need to add its own
        # callback.
        
        cmd   = msg['cmd']
        arg   = msg['arg']
        ttype = arg.get('type')

        self._log.info('scheduler state_cb: %s: %s' % (cmd, arg))


        # FIXME: get cmd string consistent throughout the code
        if cmd in ['update', 'state_update'] and ttype == 'pilot':

            with self._pilots_lock:
        
                pilot = arg
                state = pilot['state']
                pid   = pilot['uid']

                if pid not in self._pilots:

                    self._pilots[pid] = {
                            ROLE    : None,
                            'state' : None,
                            'thing' : None
                            }

                # FIXME: enforce state model order!
                self._pilots[pid]['state'] = state
                self._pilots[pid]['thing'] = copy.deepcopy(arg)

                self._log.debug('update pilot: %s' % self._pilots[pid])



    # --------------------------------------------------------------------------
    #
    def base_command_cb(self, topic, msg):

        # we'll wait for commands from the agent, to learn about pilots we can
        # use or we should stop using.
        #
        # make sure command is for *this* scheduler, and from *that* umgr

        cmd = msg['cmd']

        if cmd not in ['add_pilot', 'remove_pilot']:
            print 'scheduler base: %s' % cmd
            return

        arg   = msg['arg']
        pid   = arg.get('pid')
        thing = arg.get('thing')
        umgr  = arg.get('umgr')

        print 'scheduler command: %s: %s\n' % (cmd, arg)
        self._log.info('scheduler command: %s: %s' % (cmd, arg))

        if umgr != self._umgr:
            print 'umgr %s / %s' % (self._umgr, umgr)
            # this is not the command we are looking for
            return


        if cmd == 'add_pilot':

            print 'here: %s, %s' % (cmd, pid)
            with self._pilots_lock:

                if pid not in self._pilots:

                    self._pilots[pid] = {
                            ROLE    : None,
                            'state' : None,
                            'thing' : thing
                            }

                if self._pilots[pid][ROLE] == ADDED:
                    raise ValueError('pilot already added (%s)' % pid)

                self._pilots[pid][ROLE] = ADDED
                self._log.debug('added pilot: %s' % self._pilots[pid])

            # let the scheduler know
            self.add_pilot(pid)


        elif cmd == 'remove_pilot':

            with self._pilots_lock:

                if pid not in self._pilots:
                    raise ValueError('pilot not added (%s)' % pid)

                if self._pilots[pid][ROLE] != ADDED:
                    raise ValueError('pilot not added (%s)' % pid)

                self._pilots[pid][ROLE] = REMOVED
                self._log.debug('removed pilot: %s' % self._pilots[pid])

            # let the scheduler know
            self.remove_pilot(pid)



    # --------------------------------------------------------------------------
    #
    def _configure(self):

        raise NotImplementedError("_configure() missing for '%s'" % self.uid)


    # --------------------------------------------------------------------------
    #
    def add_pilot(self, pid):
        raise NotImplementedError("add_pilot() missing for '%s'" % self.uid)


    # --------------------------------------------------------------------------
    #
    def remove_pilot(self, pid):
        raise NotImplementedError("remove_pilot() missing for '%s'" % self.uid)


    # --------------------------------------------------------------------------
    #
    def work(self):
        raise NotImplementedError("work() missing for '%s'" % self.uid)


# ------------------------------------------------------------------------------

