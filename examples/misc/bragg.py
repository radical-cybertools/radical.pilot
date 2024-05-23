#!/usr/bin/env python3

import os
import copy
import time
import random

from collections import defaultdict

import threading as mt

import radical.pilot as rp
import radical.utils as ru


# ------------------------------------------------------------------------------
#
class Pipeline(object):

    # the pipeline renders this structure:
    #
    #  Simulation_1
    #       |
    #       |
    #       |
    #  Policies_1
    #       |    \
    #       |     \
    #       |      \
    #  Prelim_1   Training_1
    #       |       |
    #       |       |
    #       |       |
    #  Simulation_2 |
    #       |       |
    #       |       |
    #       |       |
    #  Policies_2   |
    #       |    \  |
    #       |     \ |
    #       |      \|
    #  Prelim_2   Training_2
    #       |       |
    #       |       |
    #       |       |
    #  Simulation_3 |
    #       |       |
    #       |       |
    #       |       |
    #  Policies_3   |
    #       |    \  |
    #       |     \ |
    #       |      \|
    #  Prelim_3   Training_3
    #       |       |
    #      ...     ...

    # define task types
    TASK_SIM     = 'simulation'
    TASK_POLICY  = 'policy'
    TASK_PRELIM  = 'preliminary'
    TASK_TRAIN   = 'training'

    TASK_TYPES   = [TASK_SIM, TASK_POLICY, TASK_PRELIM, TASK_TRAIN]

    task_sim     = rp.TaskDescription({'executable': 'sleep',
                                       'metadata'  : {'type': TASK_SIM,
                                                      'iter': None}})
    task_policy  = rp.TaskDescription({'executable': 'sleep',
                                       'metadata'  : {'type': TASK_POLICY,
                                                      'iter': None}})
    task_prelim  = rp.TaskDescription({'executable': 'sleep',
                                       'metadata'  : {'type': TASK_PRELIM,
                                                      'iter': None}})
    task_train   = rp.TaskDescription({'executable': 'sleep',
                                       'metadata'  : {'type': TASK_TRAIN,
                                                      'iter': None}})

    # condition indexes -- see `self._cond_train`
    COND_PRELIM = 0  # a dependent preliminary task has completed
    COND_TRAIN  = 1  # a dependent training task has completed
    COND_ACTED  = 2  # the condition has been acted upon


    # --------------------------------------------------------------------------
    #
    def __init__(self, tmgr):

        self._tmgr = tmgr

        self._uid       = ru.generate_id('p.%(item_counter)1d', ru.ID_CUSTOM)
        self._iteration = None  # count generation of TASK_SIM instances

        # control flow table
        self._protocol = {self.TASK_SIM   : self._control_sim,
                          self.TASK_POLICY: self._control_policy,
                          self.TASK_PRELIM: self._control_prelim,
                          self.TASK_TRAIN : self._control_train}


        # TASK_TRAIN's have two dependencies: the completed TASK_PRELIM of the
        # same iteration, and the completed TASK_TRAIN of the *previous*
        # iteration (for any iteration >1).  We start the TASK_TRAIN on
        # whichever of those two preconditions is met last.  self._cond_train
        # will keep track of the preconditions and also of actions on met
        # conditions
        self._cond_lock  = mt.Lock()
        self._cond_train = defaultdict(lambda: [False, False, False])

        # for iteration 0, we mark the second precondition as resolved: there is
        # no previous iteration to wait on
        self._cond_train[0][self.COND_TRAIN] = True

        # bookkeeping: what task type in what iteration is active right now?
        self._stats = {self.TASK_SIM   : None,
                       self.TASK_POLICY: None,
                       self.TASK_PRELIM: None,
                       self.TASK_TRAIN : None}


    # --------------------------------------------------------------------------
    #
    def dump(self, title=False, header=False):
        '''
        dump a representation of current task set in a string
        '''

        if title:
            out = ' | %-33s | ' % self._uid
            return out

        if header:
            out = ' | '
            for ttype in ['SIM', 'PRELIM', 'POLICY', 'TRAIN']:

                out += '% 6s | ' % ttype

            return out

        out = ' | '
        for ttype in [self.TASK_SIM,    self.TASK_PRELIM,
                      self.TASK_POLICY, self.TASK_TRAIN]:

            iteration = self._stats[ttype]
            if iteration is None:
                out += '%6s | ' % ''
            else:
                out += '%6d | ' % self._stats[ttype]

        return out


    # --------------------------------------------------------------------------
    #
    def run(self):
        '''
        submit initial set of MD similation tasks
        '''

        self._iteration = 0

        # run initial MD_SIM task
        self._submit_task(self.TASK_SIM, self._iteration)


    # --------------------------------------------------------------------------
    #
    def stop(self):

        raise RuntimeError('stop %s' % self._uid)


    # --------------------------------------------------------------------------
    #
    def _get_tinfo(self, task):
        '''
        get task type and iteration from task metadata
        '''

        ttype =     task.metadata['type']
        titer = int(task.metadata['iter'])

        assert ttype in self.TASK_TYPES, 'unknown task type: %s' % ttype
        return ttype, titer


    # --------------------------------------------------------------------------
    #
    def _submit_task(self, ttype, iteration):
        '''
        submit new task of specified type in given iteration
        '''

        if   ttype == self.TASK_SIM   : td = copy.deepcopy(self.task_sim)
        elif ttype == self.TASK_POLICY: td = copy.deepcopy(self.task_policy)
        elif ttype == self.TASK_PRELIM: td = copy.deepcopy(self.task_prelim)
        elif ttype == self.TASK_TRAIN : td = copy.deepcopy(self.task_train)
        else: raise ValueError('no such task type %s' % ttype)

        td['uid']              = '%s.%s.%03d' % (self._uid, ttype, iteration)
        td['arguments']        = random.randint(0, 10)
        td['metadata']['iter'] = iteration

        task = self._tmgr.submit_tasks([td])[0]
        task.register_callback(self._state_cb)

        self._stats[ttype] = iteration
        self.dump()


    # --------------------------------------------------------------------------
    #
    def _state_cb(self, task, state):
        '''
        act on task state changes according to our protocol
        '''

        try:
            # this cb will react on task state changes.  Specifically it will watch
            # out for task completion notification and react on them, depending on
            # the task type.

            # we only handle final states
            if state not in rp.FINAL:
                return

            if state == rp.CANCELED:
                print('task %s cancelled: stop' % task.uid)
                self.stop()

            if state == rp.FAILED:
                print('task %s failed: %s' % (task.uid, task.stderr))
                self.stop()

            assert state == rp.DONE


            # control flow depends on ttype
            ttype, titer = self._get_tinfo(task)
            self._stats[ttype]  = None
            self.dump()

            action = self._protocol[ttype]
            action(task, titer)

        except Exception as e:
            print('\n\n---------\nexception caught: %s\n\n' % repr(e))
            self.stop()


    # --------------------------------------------------------------------------
    #
    def _control_sim(self, task, iteration):
        '''
        when a simulation task completes, a preliminary training task is started
        in the same iteration.
        '''

        self._submit_task(self.TASK_PRELIM, iteration)


    # --------------------------------------------------------------------------
    #
    def _control_prelim(self, task, iteration):
        '''
        when a preliminary training task completes, two things happen:
          - a learning policy task is started in the same iteration
          - a training task is started in the same iteration

        However, the training task will *only* be started if the training task
        of the *previous* iteration has also completed.
        '''

        # always start the learning policy task
        self._submit_task(self.TASK_POLICY, iteration)

        with self._cond_lock:

            # mark condition as resolved
            self._cond_train[iteration][self.COND_PRELIM] = True

            # if all conditions are met, start training task and mark as started
            if self._cond_train[iteration] == [True, True, False]:
                self._submit_task(self.TASK_TRAIN, iteration)
                self._cond_train[iteration][self.COND_ACTED] = True


    # --------------------------------------------------------------------------
    #
    def _control_policy(self, task, iteration):
        '''
        when a learning policy task completes, start a new simulation task of
        the *next* iteration
        '''

        self._submit_task(self.TASK_SIM, iteration + 1)


    # --------------------------------------------------------------------------
    #
    def _control_train(self, task, iteration):
        '''
        when a training task completes, mark the training condition of the
        *next* iteration as fullfilled.  If the next iteration has both
        conditions fullfilled for it's training task, then start it also.
        '''

        with self._cond_lock:

            # mark condition as resolved
            self._cond_train[iteration + 1][self.COND_TRAIN] = True

            # if all conditions are met, start training task and mark as started
            if self._cond_train[iteration + 1] == [True, True, False]:
                self._submit_task(self.TASK_TRAIN, iteration + 1)
                self._cond_train[iteration + 1][self.COND_ACTED] = True


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    # silence RP reporter
    os.environ['RADICAL_REPORT'] = 'false'

    with rp.Session() as session:

        # RP setup
        pmgr    = rp.PilotManager(session=session)
        tmgr    = rp.TaskManager(session=session)

        pdesc = rp.PilotDescription({'resource': 'local.localhost',
                                     'runtime' : 30,
                                     'cores'   : 10})
        pilot = pmgr.submit_pilots(pdesc)

        tmgr.add_pilots(pilot)

        # create pipelines
        pipeline_1 = Pipeline(tmgr)
        pipeline_2 = Pipeline(tmgr)

        out  = pipeline_1.dump(title=True)
        out += pipeline_2.dump(title=True)
        print(out)

        # start pipelines
        pipeline_1.run()
        pipeline_2.run()


        # run them forever (well, until the pilot dies...
        i = 0
        while True:

            if not i % 30:
                out  = pipeline_1.dump(header=True)
                out += pipeline_2.dump(header=True)
                print(out)

            out  = pipeline_1.dump()
            out += pipeline_2.dump()
            print(out)

            time.sleep(1)
            i += 1


# ------------------------------------------------------------------------------

