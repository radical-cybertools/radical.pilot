
from typing import Dict, List, Any, Optional

import radical.utils as ru

from .task             import Task
from .task_description import TaskDescription


# ------------------------------------------------------------------------------
#
class Raptor(Task):
    '''
    RAPTOR ('RAPid Task executOR') is a task executor which, other than other
    RADICAL-Pilot executors can handle function tasks.

    A `Raptor` must be submitted to a pilot.  It will be associated with
    `RaptorWorker` instances on that pilot and use those workers to rapidly
    execute tasks.  Raptors excel at high throughput execution for large numbers
    of short running tasks.  However, they have limited capabilities with
    respect to managing task data dependencies, multinode tasks, MPI
    executables, and tasks with heterogeneous resource requirements.
    '''

    # --------------------------------------------------------------------------
    #
    def submit_workers(self, descriptions: List[TaskDescription]) -> List[Task]:
        '''
        Submit a set of workers for this `Raptor` instance.

        Args:
            descriptions (List[TaskDescription]): ;aunch a raptor worker for
                each provided description.

        Returns:
            List[Tasks]: a list of `rp.Task` instances, one for each created
                worker task

        The method will return immediately without waiting for actual task
        instantiation.  The submitted tasks will operate solely on behalf of the
        `Raptor` master this method is being called on.
        '''

        descriptions = ru.as_list(descriptions)

        for td in descriptions:
            td.raptor_id = self.uid

        return self._tmgr.submit_workers(descriptions)


    # --------------------------------------------------------------------------
    #
    def submit_tasks(self, descriptions: List[TaskDescription]) -> List[Task]:
        '''
        Submit a set of tasks to this `Raptor` instance.

        Args:
            descriptions (List[TaskDescription]): ;aunch a raptor worker for
                each provided description.

        Returns:
            List[Tasks]: a list of `rp.Task` instances, one for each task.

        The tasks might not get executed until a worker is available for this
        Raptor instance.
        '''



        descriptions = ru.as_list(descriptions)

        for td in descriptions:
            td.raptor_id = self.uid

        return self._tmgr.submit_tasks(descriptions)


    # --------------------------------------------------------------------------
    #
    def rpc(self, cmd, *args, **kwargs):
        '''
        Send a raptor command, wait for the response, and return the result.

        Args:
            rpc (str): name of the rpc call to invoke
            *args (*List[Any]): unnamed arguments
            **kwargs (**Dict[str, Any])): named arguments

        Returns:
            Dict[str, Any]: the returned dictionary has the following fields:
              - out: captured standard output
              - err: captured standard error
              - ret: return value of the call (can be any serializable type)
              - exc: tuple of exception type (str) and error message (str)
        '''

        if not self._pilot:
            raise RuntimeError('not assigned to a pilot yet, cannot run rpc')

        kwargs['raptor_cmd'] = cmd

        return self._tmgr.pilot_rpc(self._pilot, 'raptor_rpc', *args,
                                    rpc_addr=self.uid, **kwargs)


# ------------------------------------------------------------------------------
#
RaptorMaster = Raptor
'''
The `RaptorMaster` task is an alias for the `Raptor` class and is provided
for naming symmetry with `RaptorWorker`.
'''


# ------------------------------------------------------------------------------
#
class RaptorWorker(Task):
    '''
    A `RaptorWorker` task is a thin wrapper around a normal `rp.Task` instance
    for the purpose of representing a Raptor worker task.

    NOTE: this class is not yet used.
    '''

    pass


# ------------------------------------------------------------------------------

