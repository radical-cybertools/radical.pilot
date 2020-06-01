#!/usr/bin/env python3
# pylint: disable=redefined-outer-name

import os
import sys
import time
import signal

import threading     as mt

import radical.pilot as rp
import radical.pilot.task_overlay as rpt
import radical.utils as ru


# This script has to run as a task within an pilot allocation, and is
# a demonstration of a task overlay within the RCT framework.
# It will:
#
#   - create a master which bootstraps a specific communication layer
#   - insert n workers into the pilot (again as a task)
#   - perform RPC handshake with those workers
#   - send RPC requests to the workers
#   - terminate the worker
#
# The worker itself is an external program which is not covered in this code.


# ------------------------------------------------------------------------------
#
class Request(object):

    # poor man's future
    # TODO: use proper future implementation


    # --------------------------------------------------------------------------
    #
    def __init__(self, work):

        self._uid    = ru.generate_id('request')
        self._work   = work
        self._state  = 'NEW'
        self._result = None


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):
        return self._uid


    @property
    def state(self):
        return self._state


    @property
    def result(self):
        return self._result


    # --------------------------------------------------------------------------
    #
    def as_dict(self):
        '''
        produce the request message to be sent over the wire to the workers
        '''

        return {'uid'   : self._uid,
                'state' : self._state,
                'result': self._result,
                'call'  : self._work['call'],
                'args'  : self._work['args'],
                'kwargs': self._work['kwargs'],
               }


    # --------------------------------------------------------------------------
    #
    def set_result(self, result, error):
        '''
        This is called by the master to fulfill the future
        '''

        self._result = result
        self._error  = error

        if error: self._state = 'FAILED'
        else    : self._state = 'DONE'


    # --------------------------------------------------------------------------
    #
    def wait(self):

        while self.state not in ['DONE', 'FAILED']:
            time.sleep(0.1)

        return self._result


# ------------------------------------------------------------------------------
#
class MyMaster(rpt.Master):
    '''
    This class provides the communication setup for the task overlay: it will
    set up the request / response communication queues and provide the endpoint
    information to be forwarded to the workers.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        self._requests = dict()     # bookkeeping of submitted requests
        self._lock     = mt.Lock()  # lock the request dist on updates

        # initialize the task overlay base class.  That base class will ensure
        # proper communication channels to the pilot agent.
        rpt.Master.__init__(self)

        # set up RU ZMQ Queues for request distribution and result collection
        req_cfg = ru.Config(cfg={'channel'    : 'to_req',
                                 'type'       : 'queue',
                                 'uid'        : self._uid + '.req',
                                 'path'       : os.getcwd(),
                                 'stall_hwm'  : 0,
                                 'bulk_size'  : 0})

        res_cfg = ru.Config(cfg={'channel'    : 'to_res',
                                 'type'       : 'queue',
                                 'uid'        : self._uid + '.res',
                                 'path'       : os.getcwd(),
                                 'stall_hwm'  : 0,
                                 'bulk_size'  : 0})

        self._req_queue = ru.zmq.Queue(req_cfg)
        self._res_queue = ru.zmq.Queue(res_cfg)

        self._req_queue.start()
        self._res_queue.start()

        self._req_addr_put = str(self._req_queue.addr_put)
        self._req_addr_get = str(self._req_queue.addr_get)

        self._res_addr_put = str(self._res_queue.addr_put)
        self._res_addr_get = str(self._res_queue.addr_get)

        # this master will put requests onto the request queue, and will get
        # responses from the response queue.  Note that the responses will be
        # delivered via an async callback (`self.result_cb`).
        self._req_put = ru.zmq.Putter('to_req', self._req_addr_put)
        self._res_get = ru.zmq.Getter('to_res', self._res_addr_get,
                                                cb=self.result_cb)

        # for the workers it is the opposite: they will get requests from the
        # request queue, and will send responses to the response queue.
        self._info = {'req_addr_get': self._req_addr_get,
                      'res_addr_put': self._res_addr_put}


        # make sure the channels are up before allowing to submit requests
        time.sleep(1)


    # --------------------------------------------------------------------------
    #
    def submit(self, worker, count=1):
        '''
        submit n workers, and pass the queue info as configuration file
        '''

        descr = {'executable': worker}
        return rpt.Master.submit(self, self._info, descr, count)


    # --------------------------------------------------------------------------
    #
    def request(self, call, *args, **kwargs):
        '''
        submit a work request (function call spec) to the request queue
        '''

        # create request and add to bookkeeping dict.  That response object will
        # be updated once a response for the respective request UID arrives.
        req = Request(work={'call'  : call,
                            'args'  : args,
                            'kwargs': kwargs})
        with self._lock:
            self._requests[req.uid] = req

        # push the request message (here and dictionary) onto the request queue
        self._req_put.put(req.as_dict())

        # return the request to the master script for inspection etc.
        return req


    # --------------------------------------------------------------------------
    #
    def result_cb(self, msg):

        # update result and error information for the corresponding request UID
        uid = msg['req']
        res = msg['res']
        err = msg['err']

        self._requests[uid].set_result(res, err)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    # This master script currently runs as a task within a pilot allocation.
    # The purpose of this master is to (a) spawn a set or workers within the
    # same allocation, (b) to distribute work items (`hello` function calls) to
    # those workers, and (c) to collect the responses again.

    worker = sys.argv[1]

    # create a master class instance - this will establish communication to the
    # pilot agent
    master = MyMaster()

    # insert `n` worker tasks into the agent.  The agent will schedule (place)
    # those workers and execute them.
    master.submit(count=2, worker=worker)

    # wait until `m` of those workers are up
    # This is optional, work requests can be submitted before and will wait in
    # a work queue.
    master.wait(count=2)

    # submit work requests.  The returned request objects behave like Futures
    # (they will be implemented as proper Futures in the future - ha!)
    req = list()
    for n in range(32):
        req.append(master.request('hello', n))

    # wait for request completion and print the results
    for r in req:
        r.wait()
        print(r.result)

    # simply terminate
    # FIXME: this needs to be cleaned up
    sys.stdout.flush()
    os.kill(os.getpid(), signal.SIGKILL)
    os.kill(os.getpid(), signal.SIGTERM)


# ------------------------------------------------------------------------------

