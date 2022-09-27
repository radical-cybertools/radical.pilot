
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import os

import radical.utils   as ru

from ... import states as rps

from .base import TMGRSchedulingComponent, ADDED


# the high water mark determines the percentage of task oversubscription for the
# pilots, in terms of numbers of cores
_HWM = int(os.environ.get('RADICAL_PILOT_BACKFILLING_HWM', 200))

# we consider pilots eligible for task scheduling beyond a certain start state,
# which defaults to 'PMGR_ACTIVE'.
_BF_START = os.environ.get('RADICAL_PILOT_BACKFILLING_START', rps.PMGR_ACTIVE)
_BF_STOP  = os.environ.get('RADICAL_PILOT_BACKFILLING_STOP',  rps.PMGR_ACTIVE)

_BF_START_VAL = rps._pilot_state_value(_BF_START)
_BF_STOP_VAL  = rps._pilot_state_value(_BF_STOP)


# ------------------------------------------------------------------------------
#
class Backfilling(TMGRSchedulingComponent):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        TMGRSchedulingComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        self._wait_pool = dict()      # set of unscheduled tasks
        self._wait_lock = ru.RLock()  # look on the above set

        self._pids = list()
        self._idx  = 0


    # --------------------------------------------------------------------------
    #
    def add_pilots(self, pids):

      # self._log.debug('add pilots %s', pids)

        # pilots just got added.  If we did not have any pilot before, we might
        # have tasks in the wait queue waiting -- now is a good time to take
        # care of those!
        with self._wait_lock:

            # initialize custom data for the pilot
            for pid in pids:
                pilot = self._pilots[pid]['pilot']
                cores = pilot['description']['cores']
                hwm   = int(cores * _HWM / 100)
                self._pilots[pid]['info'] = {
                        'cores' : cores,
                        'hwm'   : hwm,
                        'used'  : 0,
                        'tasks' : list(),  # list of assigned task IDs
                        'done'  : list(),  # list of executed task IDs
                }

            # now we can use the pilot
            self._pids += pids
            self._schedule_tasks()


    # --------------------------------------------------------------------------
    #
    def remove_pilots(self, pids):

      # self._log.debug('rem pilots %s', pids)

        with self._pilots_lock:

            for pid in pids:

                if pid not in self._pids:
                    raise ValueError('no such pilot %s' % pid)

                self._pids.remove(pid)
                # FIXME: cancel tasks


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

              # self._log.debug('update pilot: %s %s', pid, state)

                if  rps._pilot_state_value(state) < _BF_START_VAL:
                  # self._log.debug('early')
                    # not eligible, yet
                    continue

                if  rps._pilot_state_value(state) > _BF_STOP_VAL:
                  # self._log.debug('late')
                    # not eligible anymore
                    continue

                # this pilot is eligible.  Stop checking the others, and attempt
                # reschedule
                action = True
                break

      # self._log.debug('action: %s', action)

        if action:
          # self._log.debug('upd pilot  -> schedule')
            self._schedule_tasks()


    # --------------------------------------------------------------------------
    #
    def update_tasks(self, tasks):

        self._log.debug('update tasks: %s', [u['uid'] for u in tasks])

        reschedule = False

        with self._pilots_lock, self._wait_lock:

            for task in tasks:

                uid = task['uid']

              # if uid not in self._wait_pool:
              #     continue

                state = task['state']
                pid   = task.get('pilot', '')

                self._log.debug('update  task: %s [%s] [%s]',  uid, pid, state)

                if not pid:
                    # we are not interested in state updates for unscheduled
                    # tasks
                    self._log.debug('upd task %s no pilot', uid)
                    continue

                if pid not in self._pilots:
                    # we don't handle the pilot of this task
                    self._log.debug('upd task %s not handled', uid)
                    continue

                info = self._pilots[pid]['info']

                if uid in info['done']:
                    # we don't need further state udates
                    self._log.debug('upd task %s in done', uid)
                    continue

                if  rps._task_state_value(state) <= \
                    rps._task_state_value(rps.AGENT_EXECUTING):
                    self._log.debug('upd task %s too early', uid)
                    continue

                if uid not in info['tasks']:
                    # this contradicts the task's assignment
                    self._log.debug('upd task  %s not in tasks', uid)
                    self._log.error('bf: task %s on %s inconsistent', uid, pid)
                    raise RuntimeError('inconsistent scheduler state')

                # this task is now considered done
                info['done'].append(uid)
                info['used'] -= task['description']['ranks'] \
                              * task['description']['cores_per_rank']
                reschedule = True
                self._log.debug('upd task %s - schedule (used: %s)',
                                uid, info['used'])

                if info['used'] < 0:
                    self._log.error('bf: pilot %s inconsistent', pid)
                    raise RuntimeError('inconsistent scheduler state')


        # if any pilot state was changed, consider new tasks for scheduling
        if reschedule:
            self._log.debug('upd tasks -> schedule')
            self._schedule_tasks()


    # --------------------------------------------------------------------------
    #
    def _work(self, tasks):

        with self._pilots_lock, self._wait_lock:

            for task in tasks:

                uid = task['uid']

                # not yet scheduled - put in wait pool
                self._wait_pool[uid] = task

        self._schedule_tasks()


    # --------------------------------------------------------------------------
    #
    def _schedule_tasks(self):
        """
        We have a set of tasks which we can place over a set of pilots.

        The overall objective is to keep pilots busy while load balancing across
        all pilots, even those which might yet to get added.  We achieve that
        via the following algorithm:

          - for each pilot which is being added, no matter the state:
            - assign sufficient tasks to the pilot that it can run 'n'
              generations of them, 'n' being a tunable parameter called
              'RADICAL_PILOT_BACKFILLING_HWM'.

          - for each task being completed (goes out of AGENT_EXECUTING state)
            - determine the pilot which executed it
            - backfill tasks from the wait queue until the backfilling HWM is
              reached again.

        The HWM is interpreted as percent of pilot size.  For example, a pilot
        of size 10 cores and a HWM of 200 can get tasks with a total of 20 cores
        assigned.  It can get assigned more than that, if the last task
        assigned to it surpasses the HWM.  We will not schedule any task larger
        than pilot size however.
        """

        with self._pilots_lock, self._wait_lock:

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

            # cycle over available pids and add tasks until we either ran
            # out of tasks to schedule, or out of pids to schedule over

          # self._log.debug('schedule %s tasks over %s pilots',
          #         len(self._wait_pool), len(pids))

            scheduled   = list()   # tasks we want to advance
            unscheduled = dict()   # this will be the new wait pool
            for uid, task in self._wait_pool.items():

                if not pids:
                    # no more useful pilots -- move remaining tasks into
                    # unscheduled pool
                    self._log.debug(' =!= sch task  %s', uid)
                    unscheduled[uid] = task
                    continue

                cores   = task['description']['ranks'] \
                        * task['description']['cores_per_rank']
                success = False
                for pid in list(pids):

                    info = self._pilots[pid]['info']

                    if info['used'] <= info['hwm']:

                      # self._log.debug('sch task  %s -> %s', uid, pid)
                        self._log.info('schedule %s -> %s', uid, pid)

                        pilot = self._pilots[pid]['pilot']
                        info['tasks'].append(task['uid'])
                        info['used']   += cores

                        self._assign_pilot(task, pilot)
                        scheduled.append(task)
                        success = True

                        # this pilot might now be full.  If so, remove it from
                        # list of eligible pids
                        if info['used'] >= info['hwm']:
                            pids.remove(pid)

                        break  # stop looking through pilot list

                if not success:
                    # we did not find a useable pilot for this task -- keep it
                    self._log.debug(' ==! sch task  %s', uid)
                    unscheduled[uid] = task


          # self._log.debug('retain   %s tasks and  %s pilots',
          #         len(unscheduled), len(pids))

            # all unscheduled tasks *are* the new wait pool
            self._log.debug(' 1 > waits: %s', list(self._wait_pool.keys()))
            self._wait_pool = unscheduled
            self._log.debug(' 2 > waits: %s', list(self._wait_pool.keys()))

        # advance scheduled tasks
        if scheduled:
            self.advance(scheduled, rps.TMGR_STAGING_INPUT_PENDING,
                         publish=True, push=True)


        self._log.debug('\nafter schedule:')
        self._log.debug('waits:    %s', list(self._wait_pool.keys()))
      # for pid in self._pilots:
      #     print 'pilot %s' % pid
      #     pprint.pprint(self._pilots[pid]['info'])
      # self._log.debug()


# ------------------------------------------------------------------------------

