
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

        unscheduled = list()
        scheduled   = list()
        failed      = list()

        with self._pilots_lock:

            for task in tasks:

                # check if tasks are already scheduled, ie. by the application
                uid = task.get('uid')
                pid = task.get('pilot')

                self._log.debug('attempt %s', uid)

                if pid:
                    # make sure we know this pilot
                    if pid not in self._pilots:
                        self._log.error('unknown pilot %s (task %s)', uid, pid)
                        failed.append(task)
                        continue

                    pilot = self._pilots[pid]['pilot']

                    self._assign_pilot(task, pilot)
                    scheduled.append(task)

                else:
                    # not yet scheduled - put in wait pool
                    unscheduled.append(task)

        self._log.debug('failed %d / scheduled %d / unscheduled %d',
                        len(failed), len(scheduled), len(unscheduled))

        if failed     : self.advance(failed, rps.FAILED,
                                     publish=True, push=True)
        if scheduled  : self.advance(scheduled, rps.TMGR_STAGING_INPUT_PENDING,
                                     publish=True, push=True)
        if unscheduled: self._schedule_tasks(unscheduled)


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

            tasks_ok = list()
            tasks_fail = list()

            for task in tasks:

                try:
                    # determine target pilot for task
                    if self._idx >= len(self._pids):
                        self._idx = 0

                    pid   = self._pids[self._idx]
                    pilot = self._pilots[pid]['pilot']

                    self._idx += 1

                    # we assign the task to the pilot.
                    self._assign_pilot(task, pilot)

                    tasks_ok.append(task)

                except Exception:
                    self._log.exception('task schedule preparation failed')
                    tasks_fail.append(task)

            # make sure that all scheduled tasks have sandboxes known

            # advance all tasks
            self._log.debug('failed: %d, ok: %d', len(tasks_fail), len(tasks_ok))
            self.advance(tasks_fail, rps.FAILED, publish=True, push=False)
            self.advance(tasks_ok,   rps.TMGR_STAGING_INPUT_PENDING,
                         publish=True, push=True)


# ------------------------------------------------------------------------------

