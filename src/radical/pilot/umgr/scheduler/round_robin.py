
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import radical.utils    as ru

from ... import states as rps

from .base import UMGRSchedulingComponent


# ------------------------------------------------------------------------------
#
class RoundRobin(UMGRSchedulingComponent):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        UMGRSchedulingComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        self._wait_pool = list()      # set of unscheduled units
        self._wait_lock = ru.RLock()  # look on the above set

        self._pids = list()
        self._idx  = 0

        self._log.debug('RoundRobin umgr scheduler configured')


    # --------------------------------------------------------------------------
    #
    def add_pilots(self, pids):

        # pilots just got added.  If we did not have any pilot before, we might
        # have units in the wait queue waiting -- now is a good time to take
        # care of those!
        with self._wait_lock:

            self._log.debug('add pilot - waitpool %d', len(self._wait_pool))

            self._pids += pids

            if self._wait_pool:
                units = self._wait_pool[:]   # deep copy to avoid data recursion
                self._wait_pool = list()
                self._schedule_units(units)


    # --------------------------------------------------------------------------
    #
    def remove_pilots(self, pids):

        with self._pilots_lock:

            for pid in pids:

                if pid not in self._pids:
                    raise ValueError('no such pilot %s' % pid)

                self._pids.remove(pid)
                # FIXME: cancel units


    # --------------------------------------------------------------------------
    #
    def update_pilots(self, pids):

        # FIXME: we don't react on pilot state changes right now
        pass


    # --------------------------------------------------------------------------
    #
    def update_units(self, units):

        # RR scheduling is not concerned about unit states
        pass


    # --------------------------------------------------------------------------
    #
    def _work(self, units):

        unscheduled = list()
        scheduled   = list()
        failed      = list()

        with self._pilots_lock:

            for unit in units:

                # check if units are already scheduled, ie. by the application
                uid = unit.get('uid')
                pid = unit.get('pilot')

                self._log.debug('attempt %s', uid)

                if pid:
                    # make sure we know this pilot
                    if pid not in self._pilots:
                        self._log.error('unknown pilot %s (unit %s)', uid, pid)
                        failed.append(unit)
                        continue

                    pilot = self._pilots[pid]['pilot']

                    self._assign_pilot(unit, pilot)
                    scheduled.append(unit)

                else:
                    # not yet scheduled - put in wait pool
                    unscheduled.append(unit)

        self._log.debug('failed %d / scheduled %d / unscheduled %d',
                        len(failed), len(scheduled), len(unscheduled))

        if failed     : self.advance(failed, rps.FAILED,
                                     publish=True, push=True)
        if scheduled  : self.advance(scheduled, rps.UMGR_STAGING_INPUT_PENDING,
                                     publish=True, push=True)
        if unscheduled: self._schedule_units(unscheduled)


    # --------------------------------------------------------------------------
    #
    def _schedule_units(self, units):

        self._log.debug('schedule %d units', len(units))

        with self._pilots_lock:

            if not self._pids:

                self._log.debug('no pilots')

                # no pilots, no schedule...
                with self._wait_lock:
                    self._wait_pool += units
                    return

            units_ok = list()
            units_fail = list()

            for unit in units:

                try:
                    # determine target pilot for unit
                    if self._idx >= len(self._pids):
                        self._idx = 0

                    pid   = self._pids[self._idx]
                    pilot = self._pilots[pid]['pilot']

                    self._idx += 1

                    # we assign the unit to the pilot.
                    self._assign_pilot(unit, pilot)

                    units_ok.append(unit)

                except Exception:
                    self._log.exception('unit schedule preparation failed')
                    units_fail.append(unit)

            # make sure that all scheduled units have sandboxes known

            # advance all units
            self._log.debug('failed: %d, ok: %d', len(units_fail), len(units_ok))
            self.advance(units_fail, rps.FAILED, publish=True, push=False)
            self.advance(units_ok,   rps.UMGR_STAGING_INPUT_PENDING,
                         publish=True, push=True)


# ------------------------------------------------------------------------------

