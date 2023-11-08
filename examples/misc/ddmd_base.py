#!/usr/bin/env python3

# ------------------------------------------------------------------------------
#

import os
import time
import random
import signal
import threading as mt

from collections import defaultdict

import radical.pilot as rp
import radical.utils as ru



# ------------------------------------------------------------------------------
#
class DDMD_Base(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        self._task_types     = dict()

        self._cores          = 16  # available resources
        self._cores_used     =  0

        self._seed           = list()
        self._lock           = mt.RLock()
        self._tasks          = defaultdict(dict)
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
    def register_task_type(self, ttype, on_final, glyph):

        self._task_types[ttype] = {'on_final': on_final,
                                   'glyph'   : glyph}


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

        for ttype in self._task_types:

            n     = len(self._tasks[ttype])
            idle -= n
            self._rep.ok('%s' % self._get_glyph(ttype) * n)

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
    def seed(self, ttype, n=1):

        for _ in range(n):
            self._seed.append(ttype)


    # --------------------------------------------------------------------------
    #
    def start(self):
        '''
        submit initial set of MD similation tasks
        '''

        self.dump('submit seed')
        assert self._seed

        # start first iteration
        self._submit_tasks(self._seed)


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

        assert ttype in self._task_types, 'unknown task type: %s' % uid
        return ttype


    # --------------------------------------------------------------------------
    #
    def _get_action(self, ttype):
        '''
        get action from protocol
        '''

        assert ttype in self._task_types, 'unknown task type: %s' % ttype
        return self._task_types[ttype]['on_final']


    # --------------------------------------------------------------------------
    #
    def _get_glyph(self, ttype):
        '''
        get task glyph from task type
        '''

        assert ttype in self._task_types, 'unknown task type: %s' % ttype
        return self._task_types[ttype]['glyph']


    # --------------------------------------------------------------------------
    #
    def _submit_tasks(self, ttypes, n=1):
        '''
        submit 'n' new tasks of specified type

        n == -1: fill remaining cores

        NOTE: all tasks are uniform for now: they use a single core and sleep
              for a random number (0..3) of seconds.
        '''

        with self._lock:

            tds = list()
            for ttype in ru.as_list(ttypes):
                for _ in range(n):

                    t_sleep = int(random.randint(0,30) / 10) + 3
                    result  = int(random.randint(0,10) /  1)

                    uid = ru.generate_id('%s' % ttype)
                    tds.append(rp.TaskDescription({
                               'uid'          : uid,
                               'cpu_processes': 1,
                               'executable'   : '/bin/sh',
                               'arguments'    : ['-c', 'sleep %s; echo %s' %
                                                       (t_sleep, result)]}))

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

        # FIXME: does not work
        self._tmgr.cancel_tasks(uids)

        for uid in uids:
            ttype = self._get_ttype(uid)
            task  = self._tasks[ttype][uid]
            self.dump(task, 'cancel [%s]' % task.state)

            self._unregister_task(task)

        self.dump('cancelled')


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

            if task.uid not in self._tasks[ttype]:
                return

            # remove task from bookkeeping
            self._final_tasks.append(task.uid)
            del self._tasks[ttype][task.uid]
            self.dump(task, 'unregister %s' % task.uid)


    # --------------------------------------------------------------------------
    #
    def _state_cb(self, task, state):
        '''
        act on task state changes according to our protocol
        '''

        try:
            return self._checked_state_cb(task, state)
        except Exception as e:
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
            action = self._get_action(ttype)
            if not action:
                self._rep.exit('no action found for task %s' % task.uid)
            action(task)

            # remove final task from bookkeeping
            self._unregister_task(task)


# ------------------------------------------------------------------------------
#
class AsyncDDMD(DDMD_Base):

    TASK_TRAIN_MODEL = 'task_train_model'
    TASK_TRAIN_FF    = 'task_train_ff'
    TASK_MD_SIM      = 'task_md_sim'
    TASK_MD_CHECK    = 'task_md_check'
    TASK_DFT         = 'task_dft'

    def __init__(self):

        self._threshold = 1

        super().__init__()

        self.register_task_type(self.TASK_TRAIN_MODEL, self.control_train_model, 'T')
        self.register_task_type(self.TASK_TRAIN_FF,    self.control_train_ff,    't')
        self.register_task_type(self.TASK_MD_SIM,      self.control_md_sim,      's')
        self.register_task_type(self.TASK_MD_CHECK,    self.control_md_check,    'c')
        self.register_task_type(self.TASK_DFT,         self.control_dft,         'd')



    # --------------------------------------------------------------------------
    #
    def control_train_model(self, task):
        '''
        react on completed MD simulation task
        '''

        self.dump(task, 'completed model train, next iteration')

        # FIXME: is that the right response?
        self.next_iteration()


    # --------------------------------------------------------------------------
    #
    def control_train_ff(self, task):
        '''
        react on completed ff training task
        '''

        #       - When FFTrain task goes away, FFTrain met conversion criteria
        #         - kill MDSim tasks from previous iteration (in next_iteration)
        #         -> CONTINUE WHILE (with new force field)

        self.dump(task, 'completed ff train, next iteration')
        self.next_iteration()


    # --------------------------------------------------------------------------
    #
    def control_md_sim(self, task):
        '''
        react on completed MD sim task
        '''

        # FIXME

        # - for any MD that completes
        #   - start UncertaintyCheck test for it (UCCheck)

        tid = task.uid
        self.dump(task, 'completed md, start check ')
        self._submit_tasks(self.TASK_MD_CHECK)


    # --------------------------------------------------------------------------
    #
    def control_md_check(self, task):
        '''
        react on completed MD check task
        '''

        try:
            uncertainty = int(task.stdout.split()[0])
        except:
            uncertainty = 1

        #   - if uncertainty > threshold:
        #     - ADAPTIVITY GOES HERE
        #     - run DFT task
        #   - else (uncertainty <= threshold):
        #     - MD output -> input to TASK_TRAIN_MODEL
        #     - run new MD task / run multiple MD tasks for each structure
        #       (configurable)

        if uncertainty > self._threshold:
          # self._adaptivity_cb()
            self._submit_tasks(self.TASK_DFT)

        else:
            # FIXME: output to TASK_TRAIN_MODEL
            self._submit_tasks(self.TASK_MD_SIM)


    # --------------------------------------------------------------------------
    #
    def control_dft(self, task):
        '''
        react on completed DFT task
        '''

        # - DFT task output -> input to FFTrain task

        # FIXME: output to TASK_TRAIN_MODEL
        # FIXME: what else us supposed to happen here?
        pass


    # --------------------------------------------------------------------------
    #
    def next_iteration(self):

        self.dump('-----------------------------------------------------------')
        self.dump('next iteration')

        uids = list()
        for ttype in self._tasks:
            uids.extend(self._tasks[ttype].keys())

        if uids:
            self._cancel_tasks(uids)

        # always (re)start a training tasks
        self._submit_tasks(self.TASK_TRAIN_FF   , n=1)
        self._submit_tasks(self.TASK_TRAIN_MODEL, n=1)

        # run initial batch of MD_SIM tasks (assume one core per task)
        self._submit_tasks(self.TASK_MD_SIM, n=self._cores - 2)

        self.dump('next iter: started %s md sims' % (self._cores - 2))


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    ddmd = AsyncDDMD()

    try:
        ddmd.seed(ddmd.TASK_TRAIN_MODEL, 1)
        ddmd.seed(ddmd.TASK_TRAIN_FF,    1)
        ddmd.seed(ddmd.TASK_MD_SIM,     10)

        ddmd.start()

        while True:
          # ddmd.dump()
            time.sleep(1)

    finally:
        ddmd.close()


# ------------------------------------------------------------------------------

