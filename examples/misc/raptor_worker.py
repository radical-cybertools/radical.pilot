
import time
import random

import radical.pilot as rp


# ------------------------------------------------------------------------------
#
class MyWorker(rp.raptor.MPIWorker):
    '''
    This class provides the required functionality to execute work requests.
    In this simple example, the worker only implements a single call: `hello`.
    '''


    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):

        rp.raptor.MPIWorker.__init__(self, cfg)


    # --------------------------------------------------------------------------
    #
    def my_hello(self, count, uid):
        '''
        important work
        '''

        self._prof.prof('app_start', uid=uid)

        out = 'hello %5d @ %.2f [%s]' % (count, time.time(), self._uid)
        time.sleep(random.randint(1, 5))

        self._log.debug(out)

        self._prof.prof('app_stop', uid=uid)
      # self._prof.flush()

      # td = rp.TaskDescription({
      #         'mode'            : rp.TASK_EXECUTABLE,
      #         'scheduler'       : None,
      #         'cpu_processes'   : 2,
      #         'cpu_process_type': rp.MPI,
      #         'executable'      : '/bin/sh',
      #         'arguments'       : ['-c',
      #                              'echo "hello $RP_RANK/$RP_RANKS: $RP_TASK_ID"']})
        td = rp.TaskDescription({
                'uid'             : 'task.call.w.000000',
              # 'timeout'         : 10,
                'mode'            : rp.TASK_FUNCTION,
                'cpu_processes'   : 2,
                'cpu_process_type': rp.MPI,
                'function'        : 'test_mpi',
                'kwargs'          : {'msg': 'task.call.w.000000'}})

        master = self.get_master()
        task   = master.run_task(td)

        print('task %s: [%s] [%s] [%s]' % (task['uid'],    task['state'],
                                           task['stdout'], task['stderr']))

        return out


# ------------------------------------------------------------------------------

