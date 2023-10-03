#!/usr/bin/env python3

import sys
import time

from unittest import TestCase

import radical.utils as ru
import radical.pilot as rp


# ------------------------------------------------------------------------------
#
class TestUtils(TestCase):

    def __init__(self):

        self._log  = ru.Logger('rp', targets=['0'])
        self._prof = ru.Profiler('rp')

        self._prof.disable()

        self._ctrl = ru.zmq.PubSub(rp.CONTROL_PUBSUB)
        self._ctrl.start()

        super().__init__()

        time.sleep(1)


    # --------------------------------------------------------------------------
    #
    def rpc_handler(self):

        rpc_1 = rp.utils.RPCHelper(self._ctrl.addr_pub, self._ctrl.addr_sub,
                                   self._log, self._prof)

        time.sleep(1)

        check = False
        def rpc_check(val_1, val_2, val_3=3):
            self.assertEqual(val_1, 1)
            self.assertEqual(val_2, 2)
            self.assertEqual(val_3, 3)
            check = True
            sys.stdout.write('stdout')
            sys.stderr.write('stderr')
            return 4

        rpc_1.add_handler('check', rpc_check)
        res = rpc_1.request('check', 1, val_2=2)

        self.assertIsInstance(res.out, str)
        self.assertIsInstance(res.err, str)
        self.assertIsInstance(res.val, int)

        self.assertEqual(str(res.out), 'stdout')
        self.assertEqual(str(res.err), 'stderr')
        self.assertEqual(res.val, 4)
        self.assertEqual(res.exc, None)


        with self.assertRaises(RuntimeError):
            val = rpc_1.request('check', 1)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestUtils()
    tc.rpc_handler()


# ------------------------------------------------------------------------------

