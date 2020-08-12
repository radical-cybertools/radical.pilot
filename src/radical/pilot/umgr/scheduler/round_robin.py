
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

        units_wait  = list()
        units_early = dict()
        units_late  = list()

        with self._pilots_lock:
            pilot_ids = list(self._pilots.keys())


        for unit in units:

            # check if units are already scheduled, ie. by the application
            pid = unit.get('pilot')

            if not pid:
                # not yet assigned - put in wait pool for later scheduling
                units_late.append(unit)

            else:
                # make sure we know this pilot
                if pid not in pilot_ids:

                    # pilot is not yet known, let unit wait
                    units_wait.append(unit)
                    continue

                if pid not in units_early:
                    units_early[pid] = list()

                units_early[pid].append(unit)


        # assign early bound tasks for which a pilot is known
        with self._pilots_lock:

            for pid in units_early:

                self._assign_pilot(units_early[pid], self._pilots[pid]['pilot'])
                self.advance(units_early[pid], rps.UMGR_STAGING_INPUT_PENDING,
                             publish=True, push=True)

        # delay units which have to wait for assigned pilots to appear
        # (delayed early binding)
        if units_wait:
            self._wait_pool += units_wait

        # for all unssigned tasks, attempt to schedule them (later binding)
        if units_late:
            self._schedule_units(units_late)


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

            units_fail = list()
            units_done = dict()

            for unit in units:

                try:
                    # determine target pilot for unit
                    if self._idx >= len(self._pids):
                        self._idx = 0

                    pid   = self._pids[self._idx]
                    pilot = self._pilots[pid]['pilot']

                    self._idx += 1

                    # we assign the unit to the pilot.
                    if pid not in units_done:
                        units_done[pid] = list()
                    units_done[pid].append(unit)

                except Exception:
                    self._log.exception('unit schedule preparation failed')
                    units_fail.append(unit)

            # TODO: make sure that all scheduled units have sandboxes known

        # advance all scheuled units
        with self._pilots_lock:
            for pid in units_done:
                self._assign_pilot(units_done[pid], self._pilots[pid]['pilot'])
                self.advance(units_done[pid], rps.UMGR_STAGING_INPUT_PENDING,
                             publish=True, push=True)

        # advance failed units
        if units_fail:
            self.advance(units_fail, rps.FAILED,
                         publish=True, push=False)


# ------------------------------------------------------------------------------

