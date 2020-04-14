#!/usr/bin/env python3

import sys
import time

import radical.utils as ru
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

        self.register_method('hello', self.hello)


    # --------------------------------------------------------------------------
    #
    def hello(self, world):
        '''
        important work
        '''

        time.sleep(0.1)

        return 'hello %s @ %s' % (world, time.time())


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    # the `info` dict is passed to the worker as config file.
    # Create the worker class and run it's work loop.
    worker = MyWorker(sys.argv[1])
    worker.run()


# ------------------------------------------------------------------------------

