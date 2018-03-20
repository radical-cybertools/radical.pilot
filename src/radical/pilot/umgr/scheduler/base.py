
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
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

        self._uid = ru.generate_id(cfg['owner'] + '.scheduling.%(counter)s',
                                   ru.ID_CUSTOM)

        rpu.Component.__init__(self, cfg, session)

        self._umgr = self._owner


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):

        self._early       = dict()            # early-bound units, sorted by pid
        self._pilots      = dict()            # dict of pilots to schedule over
        self._pilots_lock = threading.RLock() # lock on the above set

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

        # cache the local client sandbox to avoid repeated os calls
        self._client_sandbox = os.getcwd()


    # --------------------------------------------------------------------------
    #
    def finalize_child(self):

        self._log.info('finalize_child')

        self.unregister_subscriber(rpc.CONTROL_PUBSUB, self._base_command_cb)
        self.unregister_subscriber(rpc.STATE_PUBSUB,   self._base_state_cb)
        self.unregister_output(rps.UMGR_STAGING_INPUT_PENDING)
        self.unregister_input (rps.UMGR_SCHEDULING_PENDING,
                               rpc.UMGR_SCHEDULING_QUEUE, self.work)


        self._log.info('finalize_child done')


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
      # self._log.debug('base state cb: %s', cmd)

        # FIXME: get cmd string consistent throughout the code
        if cmd not in ['update', 'state_update']:
          # self._log.debug('base state cb: ignore %s', cmd)
            self._log.debug('ignore cmd %s', cmd)
            return True

        if not isinstance(arg, list): things = [arg]
        else                        : things =  arg

      # self._log.debug('base state cb: things %s', things)

        pilots = [t for t in things if t['type'] == 'pilot']
        units  = [t for t in things if t['type'] == 'unit' ]

        self._log.debug('update pilots %s', [p['uid'] for p in pilots])
        self._log.debug('update units  %s', [u['uid'] for u in units])

        self._update_pilot_states(pilots)
        self._update_unit_states(units)

        return True


    # --------------------------------------------------------------------------
    #
    def _update_pilot_states(self, pilots):

        self._log.debug('update pilot states for %s', [p['uid'] for p in pilots])

      # self._log.debug('update pilot states for %s', [p['uid'] for p in pilots])

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
                  # self._log.debug('%s: %s -> %s', pid,  current, target)
                    to_update.append(pid)
                    self._pilots[pid]['state'] = target
                    self._log.debug('update pilot state: %s -> %s', current, passed)

      # self._log.debug('to update: %s', to_update)
        if to_update:
            self.update_pilots(to_update)
      # self._log.debug('updated  : %s', to_update)


    # --------------------------------------------------------------------------
    #
    def _update_unit_states(self, units):

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

                for pilot in pilots:

                    pid = pilot['uid']

                    # if we have any early_bound units waiting for this pilots,
                    # advance them now
                    early_units = self._early.get(pid)
                    if early_units:
                        for unit in early_units:
                            if not unit.get('sandbox'):
                                unit['sandbox'] = self._session._get_unit_sandbox(unit, pilot)

                        self.advance(early_units, rps.UMGR_STAGING_INPUT_PENDING, 
                                     publish=True, push=True)

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
                    self._log.debug('removed pilot: %s', self._pilots[pid])

            # let the scheduler know
            self.remove_pilots(pids)

        return True


    # --------------------------------------------------------------------------
    #
    def _configure(self):
        raise NotImplementedError("_configure() missing for '%s'" % self.uid)


    # --------------------------------------------------------------------------
    #
    def _assign_pilot(self, unit, pilot):
        '''
        assign a unit to a pilot.
        This is also a good opportunity to determine the unit sandbox(es).
        '''

        unit['pilot'           ] = pilot['uid']
        unit['client_sandbox'  ] = str(self._session._get_client_sandbox())
        unit['resource_sandbox'] = str(self._session._get_resource_sandbox(pilot))
        unit['pilot_sandbox'   ] = str(self._session._get_pilot_sandbox(pilot))
        unit['unit_sandbox'    ] = str(self._session._get_unit_sandbox(unit, pilot))


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
    def work(self, units):
        '''
        We get a number of units, and filter out those which are already bound
        to a pilot.  Those will get adavnced to UMGR_STAGING_INPUT_PENDING
        straight away.  All other units are passed on to `self._work()`, which
        is the scheduling routine as implemented by the deriving scheduler
        classes.

        Note that the filter needs to know any pilot the units are early-bound
        to, as it needs to obtain the unit's sandbox information to prepare for
        the follow-up staging ops.  If the respective pilot is not yet known,
        the units are put into a early-bound-wait list, which is checked and
        emptied as pilots get registered.
        '''

        if not isinstance(units, list):
            units = [units]

        self.advance(units, rps.UMGR_SCHEDULING, publish=True, push=False)

        to_schedule = list()

        with self._pilots_lock:

            for unit in units:

                uid = unit['uid']
                pid = unit.get('pilot')

                if pid:
                    # this unit is bound already (it is early-bound), so we don't
                    # need to pass it to the actual schedulng algorithymus

                    # check if we know about the pilot, so that we can advance
                    # the unit to data staging
                    pilot = self._pilots.get(pid, {}).get('pilot')
                    if pilot:
                        # make sure we have a sandbox defined, too
                        if not unit.get('sandbox'):
                            pilot = self._pilots[pid]['pilot']
                            unit['sandbox'] = self._session._get_unit_sandbox(unit, pilot)

                        self.advance(unit, rps.UMGR_STAGING_INPUT_PENDING, 
                                     publish=True, push=True)
                    else:
                        # otherwise keep in `self._early` until we learn about
                        # the pilot
                        self._log.warn('got unit %s for unknown pilot %s', uid, pid)
                        if pid not in self._early: 
                            self._early[pid] = list()
                        self._early[pid].append(unit)

                else:
                    to_schedule.append(unit)

        self._work(to_schedule)


    # --------------------------------------------------------------------------
    #
    def _work(self, units=None):
        raise NotImplementedError("work() missing for '%s'" % self.uid)


# ------------------------------------------------------------------------------

