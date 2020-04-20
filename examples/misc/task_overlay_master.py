#!/usr/bin/env python3

import sys

import radical.pilot as rp


# This script has to run as a task within an pilot allocation, and is
# a demonstration of a task overlay within the RCT framework.
# It will:
#
#   - create a master which bootstrappes a specific communication layer
#   - insert n workers into the pilot (again as a task)
#   - perform RPC handshake with those workers
#   - send RPC requests to the workers
#   - terminate the worker
#
# The worker itself is an external program which is not covered in this code.


# ------------------------------------------------------------------------------
#
class MyMaster(rp.task_overlay.Master):
    '''
    This class provides the communication setup for the task overlay: it will
    set up the request / response communication queus and provide the endpoint
    information to be forwarded to the workers.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        # initialized the task overlay base class.  That base class will ensure
        # proper communication channels to the pilot agent.
        rp.task_overlay.Master.__init__(self)


    # --------------------------------------------------------------------------
    #
    def create_work_items(self):

        # create an initial list of work items to be distributed to the workers.
        # Work items MUST be serializable dictionaries.
        items = list()
        for n in range(1024 * 32):
            items.append({'mode':  'call',
                          'data': {'method': 'hello',
                                   'kwargs': {'count': n}}})

        return items


    # --------------------------------------------------------------------------
    #
    def result_cb(self, requests):

        # result callbacks can return new work items
        new_requests = list()
        for r in requests:
            print('result_cb %s: %s [%s]' % (r.uid, r.state, r.result))
          # print('work: %s' % r.work)

            count = r.work['data']['kwargs']['count']
            if count < 10:
                new_requests.append({'mode': 'call',
                                     'data': {'method': 'hello',
                                              'kwargs': {'count': count + 100}}})

        return new_requests


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    # This master script runs as a task within a pilot allocation.  The purpose
    # of this master is to (a) spawn a set or workers within the same
    # allocation, (b) to distribute work items (`hello` function calls) to those
    # workers, and (c) to collect the responses again.
    worker   = str(sys.argv[1])
    n_nodes  = int(sys.argv[2])
    cpn      = int(sys.argv[3])
    gpn      = int(sys.argv[4])

    # one node is used by master.  Alternatively (and probably better), we could
    # reduce one of the worker sizes by one core.  But it somewhat depends on
    # the worker type and application workload to judge if that makes sense, so
    # we leave it for now.
    n_workers = n_nodes - 1

    # create a master class instance - this will establish communitation to the
    # pilot agent
    master = MyMaster()

    # insert `n` worker tasks into the agent.  The agent will schedule (place)
    # those workers and execute them.  Insert one smaller worker (see above)
    master.submit(worker=worker, count=n_workers, cores=cpn,     gpus=gpn)
    master.submit(worker=worker, count=1,         cores=cpn - 1, gpus=gpn)

    # wait until `m` of those workers are up
    # This is optional, work requests can be submitted before and will wait in
    # a work queue.
  # master.wait(count=nworkers)

    master.run()

    # simply terminate
    # FIXME: clean up workers


# ------------------------------------------------------------------------------

