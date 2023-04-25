#!/usr/bin/env python3

# Changing the ratio among types of tasks at runtime.
# Algorithmically:
# - Start MD simulation tasks, use all the available resources
# - upon termination of an MD sim task:
#   - if the aggregation threshold is reached, kill a sim task and
#     launch an Aggregation task
#   - else, launch a new sim task
# - upon termination of an Aggregation task, launch a ML training task (possibly
#   killing some of the sim tasks if it requires more resource)
# - upon termination of an ML training task:
#   - if learning threshold is reached, launch an Agent task;
#   - else, launch a sim task
# - Upon termination of an Agent task, kill all the tasks and goto i.


import os
import time
import random
import signal
import threading as mt

import radical.pilot as rp
import radical.utils as ru


# ------------------------------------------------------------------------------
#
class DDMD(object):

    # define task types (used as prefix on task-uid)
    TASK_MD_SIM    = 'md_sim'
    TASK_AGGREGATE = 'aggregate'
    TASK_ML_TRAIN  = 'ml_train'
    TASK_AGENT     = 'agent'

    TASK_TYPES     = [TASK_MD_SIM, TASK_AGGREGATE, TASK_ML_TRAIN, TASK_AGENT]

    # keep track of core usage
    cores_used     = 0

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        # control flow table
        self._protocol = {self.TASK_MD_SIM   : self._control_md_sim,
                          self.TASK_AGGREGATE: self._control_aggregate,
                          self.TASK_ML_TRAIN : self._control_ml_train,
                          self.TASK_AGENT    : self._control_agent}

        self._glyphs   = {self.TASK_MD_SIM   : '#',
                          self.TASK_AGGREGATE: '+',
                          self.TASK_ML_TRAIN : '=',
                          self.TASK_AGENT    : '*'}

        # bookkeeping
        self._aggregated     =  0
        self._aggregated_max = 64  # aggregation threshold

        self._trained        =  0
        self._trained_max    = 16  # training threshold

        self._cores          = 16  # available resources
        self._cores_used     =  0

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
        self._tmgr.register_callback(self._checked_state_cb)


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

        n     = len(self._tasks[self.TASK_MD_SIM])
        idle -= n
        self._rep.ok('%s' % self._glyphs[self.TASK_MD_SIM] * n)

        n     = len(self._tasks[self.TASK_AGGREGATE])
        idle -= n
        self._rep.warn('%s' % self._glyphs[self.TASK_AGGREGATE] * n)

        n     = len(self._tasks[self.TASK_ML_TRAIN])
        idle -= n
        self._rep.error('%s' % self._glyphs[self.TASK_ML_TRAIN] * n)

        n     = len(self._tasks[self.TASK_AGENT])
        idle -= n
        self._rep.info('%s' % self._glyphs[self.TASK_AGENT] * n)

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

        # reset bookkeeping
        self._aggregated = 0
        self._trained    = 0
        self._cores_used = 0
        self._tasks      = {ttype: dict() for ttype in self._protocol}

        # run initial batch of MD_SIM tasks (assume one core per task)
        self._submit_task(self.TASK_MD_SIM, n=self._cores)

        self.dump('started %s md sims' % self._cores)



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
    def _submit_task(self, ttype, n=1):
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
                         'arguments'    : ['-c', 'sleep %s; echo %s' %
                             (int(random.randint(0,30) / 10),
                              int(random.randint(0,10) /  1))]}))

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

            # removed tasks dont consume cores
            cores = task.description['cpu_processes'] \
                  * task.description['cpu_threads']
            self._cores_used -= cores

            # remove task from bookkeeping
            self._final_tasks.append(task.uid)
            del self._tasks[ttype][task.uid]


    # --------------------------------------------------------------------------
    #
    def _state_cb(self, task, state):
        '''
        act on task state changes according to our protocol
        '''

        try:
            return self._checked_state_cb(task, state)
        except Exception as e:
            self._rep.error('\n\n---------\nexception caught: %s\n\n' % repr(e))
            self.stop()


    # --------------------------------------------------------------------------
    #
    def _checked_state_cb(self, task, state):

        # this cb will react on task state changes.  Specifically it will watch
        # out for task completion notification and react on them, depending on
        # the task type.

      # if state in [rp.TMGR_SCHEDULING] + rp.FINAL:
      #     self.dump(task, ' -> %s' % task.state)

        # ignore all non-final state transitions
        if state not in rp.FINAL:
            return

        # ignore tasks which were already
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
    def _control_md_sim(self, task):
        '''
        react on completed MD simulation task
        '''

        # - upon termination of an MD sim task:
        #   - if the aggregation threshold is reached,
        #     - launch an Aggregation task
        #   - else
        #     - launch a new sim task
        try:
            self._aggregated += int(task.stdout)
        except:
            pass

        if self._aggregated >= self._aggregated_max:
            self._aggregated = 0
            self.dump(task, 'completed, aggregation full - start aggregate')
            self._submit_task(self.TASK_AGGREGATE)
        else:
            self.dump(task, 'completed, aggregation low  - start md sim')
            self._submit_task(self.TASK_MD_SIM)


    # --------------------------------------------------------------------------
    #
    def _control_aggregate(self, task):
        '''
        react on completed aggregation task
        '''

        # - upon termination of an Aggregation task, launch a ML training task
        #   possibly killing some of the sim tasks if it requires more resources

        sim_uid = None
        while self._cores_used >= self._cores:

            if not self._tasks[self.TASK_MD_SIM]:
                # we can't free any resources - continue to submit aggregator
                break

            # kill a sim_task
            # FIXME: I think this is wrong: *this* aggregate task just finished
            #        anyway and thus there should be space to start a new sim
            sim_uid = random.choice(list(self._tasks[self.TASK_MD_SIM].keys()))
            self._cancel_tasks(sim_uid)
            break

        # submit training task
        self.dump(task, 'completed, cancel sim %s - start ml train ' % sim_uid)
        self._submit_task(self.TASK_ML_TRAIN)


    # --------------------------------------------------------------------------
    #
    def _control_ml_train(self, task):
        '''
        react on completed ML training task
        '''
        # - upon termination of an ML training task:
        #   - if learning threshold is reached
        #     - launch an Agent task;
        #   - else
        #     - launch a sim task

        self._trained += int(task.stdout)
        if self._trained >= self._trained_max:
            self.dump(task, 'completed, training complete - start agent ')
            self._submit_task(self.TASK_AGENT)
        else:
            self.dump(task, 'completed, training incomplete - start md sim ')
            self._submit_task(self.TASK_MD_SIM)


    # --------------------------------------------------------------------------
    #
    def _control_agent(self, task):
        '''
        react on completed agent task
        '''
        # - Upon termination of an Agent task, kill all the tasks and goto i.

        to_cancel = list()
        for ttype in self._tasks:
            to_cancel += list(self._tasks[ttype].keys())

        self.dump(task, 'completed, cancel all & restart')
        self._cancel_tasks(to_cancel)

        # restart execution
        self.start()


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

