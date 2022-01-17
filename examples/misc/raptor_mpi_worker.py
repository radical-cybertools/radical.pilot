
import time
import random

import radical.pilot as rp


# ------------------------------------------------------------------------------
#
class MyWorker(rp.raptor.MPIWorkerAM2):
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
    def hello(self, count, uid):
        '''
        important work
        '''

        self._prof.prof('app_start', uid=uid)

        out = 'hello %5d @ %.2f [%s]' % (count, time.time(), self._uid)
        time.sleep(random.randint(1, 5))

        self._log.debug(out)

        self._prof.prof('app_stop', uid=uid)

        return out


# ------------------------------------------------------------------------------

