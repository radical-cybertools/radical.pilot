
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import pprint
import threading

import radical.utils as ru

from ... import utils     as rpu
from ... import states    as rps
from ... import constants as rpc

from .base import UMGRSchedulingComponent, ROLE, ADDED


# the high water mark determines the percentage of unit oversubscription for the
# pilots, in terms of numbers of cores
_HWM = int(os.environ.get('RADICAL_PILOT_BACKFILLING_HWM', 200))

# we consider pilots eligible for unit scheduling beyond a certain start state,
# which defaults to 'PMGR_ACTIVE'.
_BF_START = os.environ.get('RADICAL_PILOT_BACKFILLING_START', rps.PMGR_ACTIVE)
_BF_STOP  = os.environ.get('RADICAL_PILOT_BACKFILLING_STOP',  rps.PMGR_ACTIVE)

_BF_START_VAL = rps._pilot_state_value(_BF_START)
_BF_STOP_VAL  = rps._pilot_state_value(_BF_STOP)

# ==============================================================================
#
class Backfilling(UMGRSchedulingComponent):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        UMGRSchedulingComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        self._wait_pool = dict()             # set of unscheduled units
        self._wait_lock = threading.RLock()  # look on the above set

        self._pids = list()
        self._idx  = 0


    # --------------------------------------------------------------------------
    #
    def add_pilots(self, pids):

      # self._log.debug('add pilots %s', pids)

        # pilots just got added.  If we did not have any pilot before, we might
        # have units in the wait queue waiting -- now is a good time to take
        # care of those!
        with self._wait_lock:

            # initialize custom data for the pilot
            for pid in pids:
                pilot = self._pilots[pid]['pilot']
                cores = pilot['description']['cores']
                hwm   = int(cores * _HWM/100)
                self._pilots[pid]['info'] = {
                        'cores' : cores,
                        'hwm'   : hwm,
                        'used'  : 0, 
                        'units' : list(), # list of assigned unit IDs
                        'done'  : list(), # list of executed unit IDs
                        }

            # now we can use the pilot
            self._pids += pids
            self._schedule_units()


    # --------------------------------------------------------------------------
    #
    def remove_pilots(self, pids):

      # self._log.debug('rem pilots %s', pids)

        with self._pilots_lock:

            for pid in pids:

                if not pid in self._pids:
                    raise ValueError('no such pilot %s' % pid)

                self._pids.remove(pid)
                # FIXME: cancel units


    # --------------------------------------------------------------------------
    #
    def update_pilots(self, pids):

      # self._log.debug('update pilots for %s', pids)

        # FIXME: if PMGR_ACTIVE: schedule
        # FIXME: if FINAL:  un/re-schedule
        action = False
        with self._pilots_lock:

            for pid in pids:

                state = self._pilots[pid]['state']

              # self._log.debug('=== update pilot: %s %s', pid, state)

                if  rps._pilot_state_value(state) < _BF_START_VAL:
                  # self._log.debug('=== early')
                    # not eligible, yet
                    continue

                if  rps._pilot_state_value(state) > _BF_STOP_VAL:
                  # self._log.debug('=== late')
                    # not eligible anymore
                    continue

                # this pilot is eligible.  Stop checking the others, and attempt
                # reschedule
                action = True
              # self._log.debug('break')
                break

      # self._log.debug('action: %s', action)

        if action:
          # self._log.debug('upd pilot  -> schedule')
            self._schedule_units()


    # --------------------------------------------------------------------------
    #
    def update_units(self, units):

      # self._log.debug('update  units: %s', [u['uid'] for u in units])

        reschedule = False

        with self._pilots_lock, self._wait_lock:

            for unit in units:
        
                uid   = unit['uid']
                state = unit['state']
                pid   = unit.get('pilot', '')

                self._log.debug('update  unit: %s [%s] [%s]',  uid, pid, state)

                if not pid:
                    # we are not interested in state updates for unscheduled
                    # units
                    self._log.debug('upd unit  %s no pilot', uid)
                    continue

                if not pid in self._pilots:
                    # we don't handle the pilot of this unit
                    self._log.debug('upd unit  %s not handled', uid)
                    continue

                info = self._pilots[pid]['info']

                if uid in info['done']:
                    # we don't need further state udates
                    self._log.debug('upd unit  %s in done', uid)
                    continue

                if  rps._unit_state_value(state) <= \
                    rps._unit_state_value(rps.AGENT_EXECUTING):
                    self._log.debug('upd unit  %s too early', uid)
                    continue

                if not uid in info['units']:
                    # this contradicts the unit's assignment
                    self._log.debug('upd unit  %s not in units', uid)
                    self._log.error('bf: unit %s on %s inconsistent', uid, pid)
                    raise RuntimeError('inconsistent scheduler state')

                # this unit is now considered done
                info['done'].append(uid)
                info['used'] -= unit['description']['cpu_processes'] \
                              * unit['description']['cpu_threads']
                reschedule = True
                self._log.debug('upd unit  %s -  schedule (used: %s)', uid, info['used'])

                if info['used'] < 0:
                    self._log.error('bf: pilot %s inconsistent', pid)
                    raise RuntimeError('inconsistent scheduler state')


        # if any pilot state was changed, consider new units for scheduling
        if reschedule:
            self._log.debug('upd units -> schedule')
            self._schedule_units()


    # --------------------------------------------------------------------------
    #
    def _work(self, units):

        with self._pilots_lock, self._wait_lock:

            for unit in units:

                uid = unit['uid']
                    
                # not yet scheduled - put in wait pool
                self._wait_pool[uid] = unit
                        
        self._schedule_units()


    # --------------------------------------------------------------------------
    #
    def _schedule_units(self):
        """
        We have a set of units which we can place over a set of pilots.  
        
        The overall objective is to keep pilots busy while load balancing across
        all pilots, even those which might yet to get added.  We achieve that
        via the following algorithm:

          - for each pilot which is being added, no matter the state:
            - assign sufficient units to the pilot that it can run 'n'
              generations of them, 'n' being a tunable parameter called
              'RADICAL_PILOT_BACKFILLING_HWM'.  

          - for each unit being completed (goes out of AGENT_EXECUTING state)
            - determine the pilot which executed it
            - backfill units from the wait queue until the backfilling HWM is
              reached again.

        The HWM is interpreted as percent of pilot size.  For example, a pilot
        of size 10 cores and a HWM of 200 can get units with a total of 20 cores
        assigned.  It can get assigned more than that, if the last unit
        assigned to it surpasses the HWM.  We will not schedule any unit larger
        than pilot size however.
        """

        with self._pilots_lock, self._wait_lock:

            # units to advance beyond scheduling
            to_advance = list()

            # check if we have pilots to schedule over
            if not self._pids:
                return

            # we ignore pilots which are not yet added, are not yet in
            # BF_START_STATE, and are beyond PMGR_ACTIVE state
            pids = list()
            for pid in self._pids:

                info  = self._pilots[pid]['info']
                state = self._pilots[pid]['state']
                role  = self._pilots[pid]['role']

                if role != ADDED:
                    continue

                if  rps._pilot_state_value(state) < _BF_START_VAL:
                    # not eligible, yet
                    continue

                if  rps._pilot_state_value(state) > _BF_STOP_VAL:
                    # not ligible anymore
                    continue

                if info['used'] >= info['hwm']:
                    # pilot is full
                    continue

                pids.append(pid)

            # check if we have any eligible pilots to schedule over
            if not pids:
                return

            # cycle over available pids and add units until we either ran
            # out of units to schedule, or out of pids to schedule over

          # self._log.debug('schedule %s units over %s pilots',
          #         len(self._wait_pool), len(pids))

            scheduled   = list()   # units we want to advance
            unscheduled = dict()   # this will be the new wait pool
            for uid, unit in self._wait_pool.iteritems():

                if not pids:
                    # no more useful pilots -- move remaining units into
                    # unscheduled pool
                    self._log.debug(' =!= sch unit  %s', uid)
                    unscheduled[uid] = unit
                    continue

                cores   = unit['description']['cpu_processes'] \
                        * unit['description']['cpu_threads']
                success = False
                for pid in pids:

                    info = self._pilots[pid]['info']

                    if info['used'] <= info['hwm']:

                      # self._log.debug('sch unit  %s -> %s', uid, pid)
                        self._log.info('schedule %s -> %s', uid, pid)

                        pilot = self._pilots[pid]['pilot']
                        info['units'].append(unit['uid'])
                        info['used']   += cores

                        self._assign_pilot(unit, pilot)
                        scheduled.append(unit)
                        success = True

                        # this pilot might now be full.  If so, remove it from
                        # list of eligible pids
                        if info['used'] >= info['hwm']:
                            pids.remove(pid)

                        break  # stop looking through pilot list

                if not success:
                    # we did not find a useable pilot for this unit -- keep it
                    self._log.debug(' ==! sch unit  %s', uid)
                    unscheduled[uid] = unit


          # self._log.debug('retain   %s units and  %s pilots',
          #         len(unscheduled), len(pids))

            # all unscheduled units *are* the new wait pool
            self._log.debug(' 1 > waits: %s', self._wait_pool.keys())
            self._wait_pool = unscheduled
            self._log.debug(' 2 > waits: %s', self._wait_pool.keys())

        # advance scheduled units
        if scheduled:
            self.advance(scheduled, rps.UMGR_STAGING_INPUT_PENDING, 
                         publish=True, push=True)


        self._log.debug('\nafter schedule:')
        self._log.debug('waits:    %s', self._wait_pool.keys())
      # for pid in self._pilots:
      #     print 'pilot %s' % pid
      #     pprint.pprint(self._pilots[pid]['info'])
      # self._log.debug()
        

# ------------------------------------------------------------------------------

