#!/usr/bin/env python3


import os
import pprint

import radical.utils as ru

# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    n     = 1024
    sbox  = os.environ['SBOX']

    c_in  = ru.Config(path='%s/funcs_req_queue.cfg' % sbox)
    q_in  = ru.zmq.Putter('funcs_req_queue', c_in['put'])

    c_out = ru.Config(path='%s/funcs_res_queue.cfg' % sbox)
    q_out = ru.zmq.Getter('funcs_res_queue', c_out['get'])

    q_in.put([{'state'  : 'NEW',
               'uid'    : 'request.%06d' % i,
               'mode'   : 'eval',
               'timeout': 10,
               'data'   : {
                   'code'  : '%d * %d' % (i, i),
                   'kwargs': {} }
              } for i in range(n)
             ])

    for i in range(n):
        for res in q_out.get():
            print('%s: %s' % (res['req'], res['out']))


# ------------------------------------------------------------------------------

