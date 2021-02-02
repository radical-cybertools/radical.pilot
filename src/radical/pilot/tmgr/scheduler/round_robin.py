
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import radical.utils    as ru

from ... import states as rps

from .base import TMGRSchedulingComponent


# ------------------------------------------------------------------------------
#
class RoundRobin(TMGRSchedulingComponent):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        TMGRSchedulingComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        self._wait_pool = list()      # set of unscheduled tasks
        self._wait_lock = ru.RLock()  # look on the above set

        self._pids = list()
        self._idx  = 0

        self._log.debug('RoundRobin tmgr scheduler configured')


    # --------------------------------------------------------------------------
    #
    def add_pilots(self, pids):

        # pilots just got added.  If we did not have any pilot before, we might
        # have tasks in the wait queue waiting -- now is a good time to take
        # care of those!
        with self._wait_lock:

            self._log.debug('add pilot - waitpool %d', len(self._wait_pool))

            self._pids += pids

            if self._wait_pool:
                tasks = self._wait_pool[:]   # deep copy to avoid data recursion
                self._wait_pool = list()
                self._schedule_tasks(tasks)


    # --------------------------------------------------------------------------
    #
    def remove_pilots(self, pids):

        with self._pilots_lock:

            for pid in pids:

                if pid not in self._pids:
                    raise ValueError('no such pilot %s' % pid)

                self._pids.remove(pid)
                # FIXME: cancel tasks


    # --------------------------------------------------------------------------
    #
    def update_pilots(self, pids):

        # FIXME: we don't react on pilot state changes right now
        pass


    # --------------------------------------------------------------------------
    #
    def update_tasks(self, tasks):

        # RR scheduling is not concerned about task states
        pass


    # --------------------------------------------------------------------------
    #
    def _work(self, tasks):

        tasks_wait  = list()
        tasks_early = dict()
        tasks_late  = list()

        scheduled   = list()

        with self._pilots_lock:
            pilot_ids = list(self._pilots.keys())

        for task in tasks:

            # check if tasks are already scheduled, ie. by the application
            pid = task.get('pilot')

            if not pid:
                # not yet assigned - put in wait pool for later scheduling
                tasks_late.append(task)
                continue

            # make sure we know this pilot
            if pid not in pilot_ids:

                # pilot is not yet known, let task wait
                tasks_wait.append(task)
                continue

            if pid not in tasks_early:
                tasks_early[pid] = list()

            tasks_early[pid].append(task)


        # assign early bound tasks for which a pilot is known
        with self._pilots_lock:

            for pid in tasks_early:

                self._assign_pilot(tasks_early[pid], self._pilots[pid]['pilot'])
                self.advance(tasks_early[pid], rps.UMGR_STAGING_INPUT_PENDING,
                             publish=True, push=True)
                scheduled += tasks_early[pid]


        # delay tasks which have to wait for assigned pilots to appear
        # (delayed early binding)
        if tasks_wait:
            self._wait_pool += tasks_wait

        # for all unssigned tasks, attempt to schedule them (later binding)
        if tasks_late:
            self._schedule_tasks(tasks_late)


    # --------------------------------------------------------------------------
    #
    def _schedule_tasks(self, tasks):

        self._log.debug('schedule %d tasks', len(tasks))

        with self._pilots_lock:

            if not self._pids:

                self._log.debug('no pilots')

                # no pilots, no schedule...
                with self._wait_lock:
                    self._wait_pool += tasks
                    return

            tasks_fail = list()
            tasks_ok   = dict()

            for task in tasks:

                try:
                    # determine target pilot for task
                    if self._idx >= len(self._pids):
                        self._idx = 0

                    pid   = self._pids[self._idx]
                    pilot = self._pilots[pid]['pilot']

                    self._idx += 1

                    # we assign the task to the pilot.
                    if pid not in tasks_ok:
                        tasks_ok[pid] = list()
                    tasks_ok[pid].append(task)

                except Exception:
                    self._log.exception('task schedule preparation failed')
                    tasks_fail.append(task)

            # TODO: make sure that all scheduled tasks have sandboxes known

        # advance all scheduled tasks
        with self._pilots_lock:
            for pid in tasks_ok:
                self._assign_pilot(tasks_ok[pid], self._pilots[pid]['pilot'])
                self.advance(tasks_ok[pid], rps.UMGR_STAGING_INPUT_PENDING,
                             publish=True, push=True)

        # advance failed tasks
        if tasks_fail:
            self._log.debug('failed: %d, ok: %d', len(tasks_fail), len(tasks_ok))
            self.advance(tasks_fail, rps.FAILED, publish=True, push=False)


# ------------------------------------------------------------------------------

