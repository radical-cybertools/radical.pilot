
from typing import Dict, List, Any, Optional

import radical.utils as ru

from .task             import Task
from .task_description import TaskDescription


# ------------------------------------------------------------------------------
#
class Raptor(Task):
    '''
    RAPTOR ('RAPid Task executOR') is a task executor which, other than other
    RADICAL-pilot executors can handle function tasks.

    A `Raptor` can be submitted to a pilot.  It will be associated with
    `RaptorWorker` instances on that pilot and use those workers to rapidly
    execute tasks.  Raptors excel at high throughput execution for large numbers
    of short running tasks.  However, they have limited capabilities with
    respect to managing task data dependencies, multinode tasks, MPI
    executables, and tasks with heterogeneous resource requirements.
    '''

  # # --------------------------------------------------------------------------
  # #
  # def __init__(self, tmgr: object,
  #                    descr: Dict[str, Any],
  #                    origin: str) -> None:
  #
  #     super().__init__(tmgr, descr, origin)
  #
  #
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

        tasks = self._tmgr.submit_tasks(descriptions)


    # --------------------------------------------------------------------------
    #
    def rpc(self, rpc: str,
                  args: Optional[Dict[str, Any]] = None
           ) -> Dict[str, Any]:
        '''
        Send a raptor command, wait for the response, and return the result.
        '''

        if not self._pilot:
            raise RuntimeError('not assoigned to a pilot yet, cannot run rpc')

        reply = self._session._dbs.pilot_rpc(self._pilot, self.uid, rpc, args)

        return reply


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

