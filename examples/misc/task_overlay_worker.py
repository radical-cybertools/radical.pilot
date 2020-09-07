#!/usr/bin/env python3

import os
import sys
import time

import radical.pilot as rp
import radical.utils as ru


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

        # FIXME: this should be delegated to `generate_id`
        if isinstance(cfg, str): cfg = ru.Config(cfg=ru.read_json(cfg))
        else                   : cfg = ru.Config(cfg=cfg)

        rank = None

        import os

        if rank is None: rank = os.environ.get('PMIX_RANK')
        if rank is None: rank = os.environ.get('PMI_RANK')
        if rank is None: rank = os.environ.get('OMPI_COMM_WORLD_RANK')

        if rank is not None:
           cfg['uid'] = '%s.%03d' % (cfg['uid'], int(rank))

        rp.task_overlay.Worker.__init__(self, cfg)

        self.register_call('hello', self.hello)


    # --------------------------------------------------------------------------
    #
    def hello(self, count, uid):
        '''
        important work
        '''

        self._prof.prof('dock_start', uid=uid)

        out = 'hello %5d @ %.2f [%s]' % (count, time.time(), self._uid)
        time.sleep(0.1)

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

