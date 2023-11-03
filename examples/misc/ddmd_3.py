#!/usr/bin/env python3


import os
import time
import random
import signal

from   collections import defaultdict

import threading     as mt

import radical.pilot as rp
import radical.utils as ru

rp.Task.__repr__ = lambda x: '-'


# - initial ML force field exists
# - while iteration < 1000 (configurable):
#   - start model training task (ModelTrain) e.g. CVAE
#   - start force field training task (FFTrain)
#   - start MD simulation tasks, use all the available resources (MDSim)
#   - for any MD that completes
#     - start UncertaintyCheck test for it (UCCheck)
#     - if uncertainty > threshold:
#       - ADAPTIVITY GOES HERE
#       - run DFT task
#       - DFT task output -> input to FFTrain task
#         - FFTrain task has some conversion criteria.
#           If met, FFTrain task goes away
#           - kill MDSim tasks from previous iteration
#           -> CONTINUE WHILE (with new force field)
#     - else (uncertainty <= threshold):
#       - MD output -> input to ModelTrain task
#       - run new MD task / run multiple MD tasks for each structure (configurable)

# lower / upper bound on active num of simulations
# ddmd.get_last_n_sims ...

# ------------------------------------------------------------------------------
#
class DDMD(object):

    # define task types (used as prefix on task-uid)
    TASK_TRAIN_MODEL = 'task_train_model'
    TASK_TRAIN_FF    = 'task_train_ff'
    TASK_MD_SIM      = 'task_md_sim'
    TASK_MD_CHECK    = 'task_md_check'
    TASK_DFT         = 'task_dft'

    TASK_TYPES       = [TASK_TRAIN_MODEL,
                        TASK_TRAIN_FF,
                        TASK_MD_SIM,
                        TASK_MD_CHECK,
                        TASK_DFT]

    # keep track of core usage
    cores_used     = 0

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        # control flow table
        self._protocol = {self.TASK_TRAIN_MODEL: self._control_train_model,
                          self.TASK_TRAIN_FF   : self._control_train_ff   ,
                          self.TASK_MD_SIM     : self._control_md_sim     ,
                          self.TASK_MD_CHECK   : self._control_md_check   ,
                          self.TASK_DFT        : self._control_dft        }

        self._glyphs   = {self.TASK_TRAIN_MODEL: 'T',
                          self.TASK_TRAIN_FF   : 't',
                          self.TASK_MD_SIM     : 's',
                          self.TASK_MD_CHECK   : 'c',
                          self.TASK_DFT        : 'd'}

        # bookkeeping
        self._cores          = 4
        self._cores_used     = 0
        self._iter           = 0
        self._threshold      = 1
        self._lock           = mt.RLock()
        self._tasks          = {ttype: dict() for ttype in self.TASK_TYPES}
        self._final_tasks    = list()

        # silence RP reporter, use own
        os.environ['RADICAL_REPORT'] = 'false'
        self._rep = ru.Reporter('ddmd')
        self._rep.title('DDMD')

        # RP setup
        self._session = rp.Session()
        self._pmgr    = rp.PilotManager(session=self._session)
        self._tmgr    = rp.TaskManager(session=self._session)

        pdesc = rp.PilotDescription({'resource': 'local.localhost',
                                     'runtime' : 30,
                                     'cores'   : self._cores})
        self._pilot = self._pmgr.submit_pilots(pdesc)

        self._tmgr.add_pilots(self._pilot)
        self._tmgr.register_callback(self._state_cb)


    # --------------------------------------------------------------------------
    #
    def __del__(self):

        self.close()


    # --------------------------------------------------------------------------
    #
    def close(self):

        if self._session is not None:
            self._session.close()
            self._session = None


    # --------------------------------------------------------------------------
    #
    def dump(self, task=None, msg=''):
        '''
        dump a representation of current task set to stdout
        '''

        # this assumes one core per task

        self._rep.plain('<<|')

        idle = self._cores

        for ttype in self.TASK_TYPES:

            n     = len(self._tasks[ttype])
            idle -= n
            self._rep.ok('%s' % self._glyphs[ttype] * n)

        self._rep.plain('%s' % '-' * idle +
                        '| %4d [%4d]' % (self._cores_used, self._cores))

        if task and msg:
            self._rep.plain(' %-15s: %s\n' % (task.uid, msg))
        else:
            if task:
                msg = task
            self._rep.plain(' %-15s: %s\n' % (' ', msg))


    # --------------------------------------------------------------------------
    #
    def start(self):
        '''
        submit initial set of MD similation tasks
        '''

        self.dump('submit MD simulations')

        # start first iteration
        self._next_iteration()


    # --------------------------------------------------------------------------
    #
    def _next_iteration(self):

        if self._iter > 3:
            self.stop()

        self._iter += 1

        self.dump('new iter: requested')

        # cancel all tasks from previous iteration
        if self._iter > 1:

            uids = list()
            for ttype in self._tasks:
                uids.extend(self._tasks[ttype].keys())

            self._cancel_tasks(uids)

        # always (re)start a training tasks
        self._submit_task(self.TASK_TRAIN_FF   , n=1)
        self._submit_task(self.TASK_TRAIN_MODEL, n=1)

        # run initial batch of MD_SIM tasks (assume one core per task)
        self._submit_task(self.TASK_MD_SIM, n=self._cores - 2)

        self.dump('new iter: started %s md sims' % (self._cores - 2))


    # --------------------------------------------------------------------------
    #
    def stop(self):

        os.kill(os.getpid(), signal.SIGKILL)
        os.kill(os.getpid(), signal.SIGTERM)


    # --------------------------------------------------------------------------
    #
    def _get_ttype(self, uid):
        '''
        get task type from task uid
        '''

        ttype = uid.split('.')[0]

        assert ttype in self.TASK_TYPES, 'unknown task type: %s' % uid
        return ttype


    # --------------------------------------------------------------------------
    #
    def _submit_task(self, ttype, args=None, n=1):
        '''
        submit 'n' new tasks of specified type

        NOTE: all tasks are uniform for now: they use a single core and sleep
              for a random number (0..3) of seconds.
        '''

        with self._lock:

            tds   = list()
            for _ in range(n):
                tds.append(rp.TaskDescription({
                         'uid'          : ru.generate_id(ttype),
                         'cpu_processes': 1,
                         'executable'   : '/bin/sh',
                         'arguments'    : ['-c', 'sleep %s; echo %s %s' %
                             (int(random.randint(0,30) / 10),
                              int(random.randint(0,10) /  1), args)]}))

            tasks  = self._tmgr.submit_tasks(tds)

            for task in tasks:
                self._register_task(task)


    # --------------------------------------------------------------------------
    #
    def _cancel_tasks(self, uids):
        '''
        cancel tasks with the given uids, and unregister them
        '''

        uids = ru.as_list(uids)

        tasks = list()
        for uid in uids:
            ttype = self._get_ttype(uid)
            task  = self._tasks[ttype][uid]
            tasks.append(task)
            task.cancel()

        # FIXME: does not work
        print('=== cancel %s' % uids)
        print([t.state for t in tasks])

      # self._tmgr.cancel_tasks(uids)

      # print('wait')
      # while True:
      #     time.sleep(1)
      #     print([t.state for t in tasks])
      # self._tmgr.wait_tasks(uids)
      # print('waited')
        self.dump(msg='==== canceled')


    # --------------------------------------------------------------------------
    #
    def _register_task(self, task):
        '''
        add task to bookkeeping
        '''

        with self._lock:
            ttype = self._get_ttype(task.uid)
            self._tasks[ttype][task.uid] = task

            cores = task.description['cpu_processes'] \
                  * task.description['cpu_threads']
            self._cores_used += cores


    # --------------------------------------------------------------------------
    #
    def _unregister_task(self, task):
        '''
        remove completed task from bookkeeping
        '''

        with self._lock:

            ttype = self._get_ttype(task.uid)
            self.dump(task, '==== unregister %s / %s' % (task.uid, ttype))

            if task.uid not in self._tasks[ttype]:
                return

            # remove task from bookkeeping
            self._final_tasks.append(task.uid)
            del self._tasks[ttype][task.uid]
            self.dump(task, '==== unregister %s / %s ok' % (task.uid, ttype))

        print('= ureg', self._tasks)


    # --------------------------------------------------------------------------
    #
    def _state_cb(self, task, state):
        '''
        act on task state changes according to our protocol
        '''

        try:
            return self._checked_state_cb(task, state)
        except Exception as e:
            self._rep.exception('\n\n---------\nexception caught: %s\n\n' % repr(e))
            ru.print_exception_trace()
            self.stop()


    # --------------------------------------------------------------------------
    #
    def _checked_state_cb(self, task, state):

        # this cb will react on task state changes.  Specifically it will watch
        # out for task completion notification and react on them, depending on
        # the task type.

        if state in [rp.TMGR_SCHEDULING] + rp.FINAL:
            self.dump(task, ' -> %s' % task.state)

        # ignore all non-final state transitions
        if state not in rp.FINAL:
            return

        # ignore tasks which were already completed
        if task.uid in self._final_tasks:
            return

        # lock bookkeeping
        with self._lock:

            # raise alarm on failing tasks (but continue anyway)
            if state == rp.FAILED:
                self._rep.error('task %s failed: %s' % (task.uid, task.stderr))
                self.stop()

            # control flow depends on ttype
            ttype  = self._get_ttype(task.uid)
            action = self._protocol[ttype]
            if not action:
                self._rep.exit('no action found for task %s' % task.uid)
            action(task)

            # remove final task from bookkeeping
            self._unregister_task(task)


    # --------------------------------------------------------------------------
    #
    def _control_train_model(self, task):
        '''
        react on completed MD simulation task
        '''

        self.dump(task, 'completed model train, next iteration')

        # FIXME: is that the right response?
        self._next_iteration()


    # --------------------------------------------------------------------------
    #
    def _control_train_ff(self, task):
        '''
        react on completed ff training task
        '''

        #       - When FFTrain task goes away, FFTrain met conversion criteria
        #         - kill MDSim tasks from previous iteration (in _next_iteration)
        #         -> CONTINUE WHILE (with new force field)

        self.dump(task, 'completed ff train, next iteration')
        self._next_iteration


    # --------------------------------------------------------------------------
    #
    def _control_md_sim(self, task):
        '''
        react on completed MD sim task
        '''

        # FIXME

        # - for any MD that completes
        #   - start UncertaintyCheck test for it (UCCheck)

        tid = task.uid
        self.dump(task, 'completed md, start check ')
        self._submit_task(self.TASK_MD_CHECK, tid)


    # --------------------------------------------------------------------------
    #
    def _control_md_check(self, task):
        '''
        react on completed MD check task
        '''

        uncertainty = int(task.stdout)

        #   - if uncertainty > threshold:
        #     - ADAPTIVITY GOES HERE
        #     - run DFT task
        #   - else (uncertainty <= threshold):
        #     - MD output -> input to TASK_TRAIN_MODEL
        #     - run new MD task / run multiple MD tasks for each structure
        #       (configurable)

        if uncertainty > self._threshold:
          # self._adaptivity_cb()
            self._submit_task(self.TASK_DFT)

        else:
            # FIXME: output to TASK_TRAIN_MODEL
            self._submit_task(self.TASK_MD_SIM)


    # --------------------------------------------------------------------------
    #
    def _control_dft(self, task):
        '''
        react on completed DFT task
        '''

        # - DFT task output -> input to FFTrain task

        # FIXME: output to TASK_TRAIN_MODEL
        # FIXME: what else us supposed to happen here?
        pass


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    ddmd = DDMD()

    try:
        ddmd.start()

        while True:
          # ddmd.dump()
            time.sleep(1)

    finally:
        ddmd.close()


# ------------------------------------------------------------------------------

