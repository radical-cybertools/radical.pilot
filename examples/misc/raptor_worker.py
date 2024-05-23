
import time
import random

import radical.pilot as rp

RANKS = 2


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

        super().__init__(cfg)

        self.register_mode('foo', self._dispatch_foo)


    # --------------------------------------------------------------------------
    #
    def _dispatch_foo(self, task):

        import pprint
        self._log.debug('running foo\n%s',
                pprint.pformat(task['description']))

        return 'out', 'err', 0, None, None


    # --------------------------------------------------------------------------
    #
    def my_hello(self, uid, count=0):
        '''
        important work
        '''

        self._prof.prof('app_start', uid=uid)

        out = 'hello %5d @ %.2f [%s]' % (count, time.time(), self._uid)
        time.sleep(random.randint(1, 5))

        self._log.debug(out)

        self._prof.prof('app_stop', uid=uid)
        self._prof.flush()

        td = rp.TaskDescription({
                'mode'            : rp.TASK_EXECUTABLE,
                'raptor_id'       : None,
                'ranks'           : RANKS,
                'executable'      : '/bin/sh',
                'arguments'       : ['-c',
                            'echo "hello $RP_RANK/$RP_RANKS: $RP_TASK_ID"']})

        td = rp.TaskDescription({
              # 'uid'             : 'task.call.w.000000',
              # 'timeout'         : 10,
                'mode'            : rp.TASK_EXECUTABLE,
                'ranks'           : RANKS,
                'executable'      : 'radical-pilot-hello.sh',
                'arguments'       : ['1', 'task.call.w.000000']})

        master = self.get_master()
        task   = master.run_task(td)

        print(task['stdout'])

        return 'my_hello retval'


# ------------------------------------------------------------------------------

