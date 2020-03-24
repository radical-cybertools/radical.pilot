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

        # ensure that communication to the pilot agent is up and running, so
        # that the worker can respond to management commands (termination).
        # This will also read the passed config file and make it available as
        # `self._cfg`.
        rp.task_overlay.Worker.__init__(self, cfg)


    # --------------------------------------------------------------------------
    #
    def initialize(self):
        '''
        This method is called during base class construction.  All agent
        communication channels are available at this point.

        We use this point to connect to the request / response ZMQ queues.  Note
        that incoming requests will trigger an async callback `self.request_cb`.
        '''

        self._req_get = ru.zmq.Getter('to_req', self._info.req_addr_get,
                                                cb=self.request_cb)
        self._res_put = ru.zmq.Putter('to_res', self._info.res_addr_put)

        # the worker can return custom information which will be made available
        # to the master.  This can be used to communicate, for example, worker
        # specific communication endpoints.
        return {'foo': 'bar'}


    # --------------------------------------------------------------------------
    #
    def request_cb(self, msg):
        '''
        This implementation only understands a single request type: 'hello'.
        It will run that request and immediately return a respone message.
        All other requests will immediately trigger an error response.
        '''

        if msg['call'] == 'hello':
            ret = self.hello(*msg['args'], **msg['kwargs'])
            res = {'req': msg['uid'],
                   'res': ret,
                   'err': None}
            self._res_put.put(res)

        else:
            res = {'req': msg['uid'],
                   'res': None,
                   'err': 'no such call %s' % msg['call']}
            self._res_put.put(res)


    # --------------------------------------------------------------------------
    #
    def hello(self, world):
        '''
        important work
        '''

        return 'hello %s @ %s' % (world, time.time())


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    # the `info` dict is passed to the worker as config file.
    # Create the worker class and run it's work loop.
    worker = MyWorker(sys.argv[1])
    worker.run()


# ------------------------------------------------------------------------------

