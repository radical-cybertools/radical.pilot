#!/usr/bin/env python3

# ------------------------------------------------------------------------------
#
import asyncio
import os
from collections import defaultdict
import radical.utils as ru
from radical.asyncflow import WorkflowEngine


# ------------------------------------------------------------------------------
#
class DDMD_manager(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, execution_backend):

        self.flow = WorkflowEngine(backend=execution_backend)
        self._tasks         = defaultdict(dict)
        self._iter           = 0           # number of current iteration
        self._stop_run       = False      
        self._all_tasks = {} 

        # silence RP reporter, use own
        os.environ['RADICAL_REPORT'] = 'false'
        self._rep = ru.Reporter('ddmd')
        self._rep.title('DDMD - asyncflow')
        self._rep.plain('|TASKS' + ' ' * (self.total_tasks-5) + '|   CORES\n')

        self._TASKS = {'task_train_model':'T',
                        'task_train_ff': 't',
                        'task_md_sim': 's',
                        'task_md_check': 'c',
                        'task_dft': 'd'}

    # --------------------------------------------------------------------------
    #
    def __del__(self):
        asyncio.run(self.close())

    # --------------------------------------------------------------------------
    #
    async def close(self):
        try:
            await self.flow.shutdown()
        except:
            pass

    # --------------------------------------------------------------------------
    #
    async def wait(self):

        await asyncio.sleep(1)

    # --------------------------------------------------------------------------
    #
    async def stop(self):

        try:
            await self.flow.shutdown()
        except:
            pass

    # --------------------------------------------------------------------------
    #
    async def dump(self, msg=''):
        '''
        dump a representation of current task set to stdout
        '''
        self._rep.plain('<<|' )
        idle = self.total_tasks

        new_tasks = self._tasks[self._iter]

        cores_used = 0
        for ttype, task in new_tasks.items():
            n = sum(task)
            self._rep.ok('%s' % self._get_glyph(ttype) * n)
            idle -= n
            cores_used += n
        
        self._rep.plain('%s' %  ' ' * idle + '|' )
        self._rep.plain(' %4d [%4d]    %s\n' % (cores_used, self._cores, msg))


    # --------------------------------------------------------------------------
    #
    # async def start(self):
    #     '''
    #     submit initial set of MD similation tasks
    #     '''
    #     await self._run_all_blocks()


    # --------------------------------------------------------------------------
    #
    def _register_task(self):
        '''
        add tasks for given type to bookkeeping
        '''
        self._tasks[self._iter] = {}
        for ttype in self.workflow_loads.keys():
            num_tasks = self._get_tasks_num(ttype)
            cores_per_task = self._get_cores_num(ttype)
            self._tasks[self._iter][ttype]  = [cores_per_task] * num_tasks

    # --------------------------------------------------------------------------
    #
    async def _unregister_task(self, ttype, ind):
        '''
        remove completed task from bookkeeping
        '''
        try:
            if ind >= 0:
                #self._rep.plain(f"\nunregister {ttype}# {ind} for iteration {self._iter} finished\n")
                self._tasks[self._iter][ttype][ind] = 0
            else:
                #self._rep.plain(f"\nunregister ALL {ttype} for iteration {self._iter} finished\n")
                self._tasks[self._iter][ttype] = [0] * len(self._tasks[ttype])
        except:
            pass

    # --------------------------------------------------------------------------
    #
    def _get_glyph(self, ttype):
        '''
        get task glyph from task type
        '''

        assert ttype in self._TASKS.keys(), 'unknown task type: %s' % ttype
        return self._TASKS[ttype]

    # --------------------------------------------------------------------------
    #
    def _get_tasks_num(self, ttype):
        '''
        get number of tasks for given type
        '''
                
        assert ttype in self._TASKS.keys(), 'unknown task type: %s' % ttype
        return self.workflow_loads[ttype]['task_num']

    # --------------------------------------------------------------------------
    #    
    def _get_cores_num(self, ttype):
        '''
        get number of cores for given type
        '''
                
        assert ttype in self._TASKS.keys(), 'unknown task type: %s' % ttype
        return self.workflow_loads[ttype]['cores_per_task']
    

    # --------------------------------------------------------------------------
    #
    async def run_task(self, task):
        try:
            await task
        except asyncio.CancelledError:
            pass
            #self._rep.plain("block failed due to cancellation during previous step\n")

    async def run_sim(self):
        """Main execution method - must be implemented by subclasses"""
        pass

    # async def get_train_tasks(self):
    #     """Main execution method - must be implemented by subclasses"""
    #     pass

    async def get_all_func(self):
        """Main execution method - must be implemented by subclasses"""
        pass

    def get_tasks(self, types, *args):
        tasks = {}
        
        for t in types:
            func = self._all_tasks[t]
            n = self._get_tasks_num(t)
            cpus = self._get_cores_num(t)
            task_description={'cpu_processes': cpus}
            for i in range(n):
                tasks[f"{t}_{i}"] = asyncio.create_task(self.run_task(func(i, task_description)), name=f"{t}_{i}")
        return tasks
    
    # --------------------------------------------------------------------------
    #
    async def start(self):

        self._all_tasks = self._get_all_func()

        sim_types = []
        train_types = []

        for ttype in self._all_tasks.keys():
            if 'train' in ttype:
                train_types.append(ttype)
            else:
                sim_types.append(ttype)
        while not self._stop_run:
            self._iter += 1
            self._register_task()

            train_tasks = self.get_tasks(train_types)
            sim_tasks = self.get_tasks(sim_types)

            while train_tasks:
                done, _ = await asyncio.wait(train_tasks.values(), return_when=asyncio.FIRST_COMPLETED)
                #done, _ = await asyncio.wait(tasks2.values(), return_when=asyncio.FIRST_COMPLETED)

                for t in done:
                    name = t.get_name()
                    self._rep.plain(f"{name} finished. Cancelling all other blocks.\n")
                    for other_name, other_task in train_tasks.items():
                        if other_name != name:
                            self._rep.plain(f"{other_name} will be canceled\n")
                            other_task.cancel()
                            try:
                                await other_task
                            except asyncio.CancelledError:
                                self._rep.plain(f"{other_name} cancelled\n")

                    for other_name, other_task in sim_tasks.items():
                        self._rep.plain(f"{other_name} will be canceled\n")
                        other_task.cancel()
                        try:
                            await other_task
                        except asyncio.CancelledError:
                            self._rep.plain(f"{other_name} cancelled\n")

                    train_tasks = None
                    break  # Done — iteration completed

            await asyncio.sleep(5)
            self._rep.plain(f"\nIteration# {self._iter} finished")
        self._rep.plain('\nAll iterations completed\n')