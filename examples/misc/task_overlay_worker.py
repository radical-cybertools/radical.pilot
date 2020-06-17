#!/usr/bin/env python3

import os
import sys
import time

import radical.pilot as rp


# ------------------------------------------------------------------------------
#
class MyWorker(rp.task_overlay.Worker):
    '''
    This class provides the required functionality to execute work requests.
    In this simple example, the worker only implements a single call: `hello`.
    '''


    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):

        rp.task_overlay.Worker.__init__(self, cfg)

        self.register_call('hello', self.hello)


    # --------------------------------------------------------------------------
    #
    def hello(self, count, uid):
        '''
        important work
        '''

        self._prof.prof('dock_start', uid=uid)

        out = 'hello %5d @ %.2f [%6d]' % (count, time.time(), os.getpid())

        self._prof.prof('dock_io_start', uid=uid)
        self._log.debug(out)
        self._prof.prof('dock_io_stop', uid=uid)

        self._prof.prof('dock_stop', uid=uid)
        return out


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    # the `info` dict is passed to the worker as config file.
    # Create the worker class and run it's work loop.
    worker = MyWorker(sys.argv[1])
    worker.run()


# ------------------------------------------------------------------------------

