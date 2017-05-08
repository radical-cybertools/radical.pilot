
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

        self.register_input(rps.UMGR_SCHEDULING_PENDING,
                            rpc.UMGR_SCHEDULING_QUEUE, self.work)

        self.register_output(rps.UMGR_STAGING_INPUT_PENDING,
                             rpc.UMGR_STAGING_INPUT_QUEUE)

        # Some schedulers care about states (of pilots and/or units), some
        # don't.  Either way, we here subscribe to state updates.
        self.register_subscriber(rpc.STATE_PUBSUB, self._base_state_cb)

        # Schedulers use that command channel to get information about
        # pilots being added or removed.
        self.register_subscriber(rpc.CONTROL_PUBSUB, self._base_command_cb)


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

        from .round_robin  import RoundRobin
        from .backfilling  import Backfilling

        try:
            impl = {
                SCHEDULER_ROUND_ROBIN : RoundRobin,
                SCHEDULER_BACKFILLING : Backfilling
            }[name]

            impl = impl(cfg, session)
            return impl

        except KeyError:
            raise ValueError("Scheduler '%s' unknown or defunct" % name)


    # --------------------------------------------------------------------------
    #
    def _base_state_cb(self, topic, msg):

        # the base class will keep track of pilot state changes and updates
        # self._pilots accordingly.  Unit state changes will be ignored -- if
        # a scheduler needs to keep track of those, it will need to add its own
        # callback.
        
        cmd = msg.get('cmd')
        arg = msg.get('arg')

        self._log.info('scheduler state_cb: %s', cmd)
      # self._log.debug(' === base state cb: %s' % cmd)

        # FIXME: get cmd string consistent throughout the code
        if cmd not in ['update', 'state_update']:
          # self._log.debug(' === base state cb: ignore %s' % cmd)
            self._log.debug('ignore cmd %s', cmd)
            return True

        if not isinstance(arg, list): things = [arg]
        else                        : things =  arg

      # self._log.debug(' === base state cb: things %s' % things)

        pilots = [t for t in things if t['type'] == 'pilot']
        units  = [t for t in things if t['type'] == 'unit' ]

        self._log.debug('update pilots %s', [p['uid'] for p in pilots])
        self._log.debug('update units  %s', [u['uid'] for u in units])

      # self._log.debug(' === base state cb: update  pilots: %s' % [p['uid'] for p in pilots])
        self._update_pilot_states(pilots)
      # self._log.debug(' === base state cb: updated pilots: %s' % [p['uid'] for p in pilots])
      # self._log.debug(' === base state cb: update  units:  %s' % [u['uid'] for u in units])
        self._update_unit_states(units)
      # self._log.debug(' === base state cb: updated units:  %s' % [u['uid'] for u in units])

        return True


    # --------------------------------------------------------------------------
    #
    def _update_pilot_states(self, pilots):

        self._log.debug('update pilot states for %s', [p['uid'] for p in pilots])

      # self._log.debug(' === update pilot states for %s' % ([p['uid'] for p in pilots]))

        if not pilots:
            return

        to_update = list()

        with self._pilots_lock:

            for pilot in pilots:

                pid = pilot['uid']

                if pid not in self._pilots:
                    self._pilots[pid] = {'role'  : None,
                                         'state' : None,
                                         'pilot' : None, 
                                         'info'  : dict()  # scheduler private info
                                         }

                target  = pilot['state']
                current = self._pilots[pid]['state']

                # enforce state model order
                target, passed = rps._pilot_state_progress(pid, current, target) 

                if current != target:
                  # self._log.debug(' === %s: %s -> %s' % (pid,  current, target))
                    to_update.append(pid)
                    self._pilots[pid]['state'] = target
                    self._log.debug('update pilot state: %s -> %s', current, passed)

      # self._log.debug(' === to update: %s' % to_update)
        if to_update:
            self.update_pilots(to_update)
      # self._log.debug(' === updated  : %s' % to_update)


    # --------------------------------------------------------------------------
    #
    def _update_unit_states(self, units):

      # if not units:
      #     print ' === no units to update'

        self.update_units(units)


    # --------------------------------------------------------------------------
    #
    def _base_command_cb(self, topic, msg):

        # we'll wait for commands from the umgr, to learn about pilots we can
        # use or we should stop using.
        #
        # make sure command is for *this* scheduler, and from *that* umgr

        cmd = msg['cmd']

        if cmd not in ['add_pilots', 'remove_pilots']:
            return True

        arg   = msg['arg']
        umgr  = arg['umgr']

        self._log.info('scheduler command: %s: %s' % (cmd, arg))

        if umgr != self._umgr:
            # this is not the command we are looking for
            return True


        if cmd == 'add_pilots':

            pilots = arg['pilots']
        
            with self._pilots_lock:

                for pilot in pilots:

                    pid = pilot['uid']

                    if pid in self._pilots:
                        if self._pilots[pid]['role'] == ADDED:
                            raise ValueError('pilot already added (%s)' % pid)
                    else:
                        self._pilots[pid] = {'role'  : None,
                                             'state' : None,
                                             'pilot' : None,
                                             'info'  : dict()
                                            }

                    self._pilots[pid]['role']  = ADDED
                    self._pilots[pid]['pilot'] = pilot
                    self._log.debug('added pilot: %s', self._pilots[pid])

                self._update_pilot_states(pilots)

            # let the scheduler know
            self.add_pilots([pilot['uid'] for pilot in pilots])


        elif cmd == 'remove_pilots':

            pids = arg['pids']

            with self._pilots_lock:

                for pid in pids:

                    if pid not in self._pilots:
                        raise ValueError('pilot not added (%s)' % pid)

                    if self._pilots[pid]['role'] != ADDED:
                        raise ValueError('pilot not added (%s)' % pid)

                    self._pilots[pid]['role'] = REMOVED
                    self._log.debug('removed pilot: %s' % self._pilots[pid])

            # let the scheduler know
            self.remove_pilots(pids)

        return True


    # --------------------------------------------------------------------------
    #
    def _configure(self):
        raise NotImplementedError("_configure() missing for '%s'" % self.uid)


    # --------------------------------------------------------------------------
    #
    def add_pilots(self, pids):
        raise NotImplementedError("add_pilots() missing for '%s'" % self.uid)


    # --------------------------------------------------------------------------
    #
    def remove_pilots(self, pids):
        raise NotImplementedError("remove_pilots() missing for '%s'" % self.uid)


    # --------------------------------------------------------------------------
    #
    def update_pilots(self, pids):
        raise NotImplementedError("update_pilots() missing for '%s'" % self.uid)


    # --------------------------------------------------------------------------
    #
    def update_units(self, uids):
        raise NotImplementedError("update_units() missing for '%s'" % self.uid)


    # --------------------------------------------------------------------------
    #
    def work(self):
        raise NotImplementedError("work() missing for '%s'" % self.uid)


# ------------------------------------------------------------------------------

