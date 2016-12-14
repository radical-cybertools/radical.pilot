#!/usr/bin/env python

# https://bugs.python.org/issue24862

from subprocess      import Popen
from threading       import Thread  as T
from multiprocessing import Process as P
import multiprocessing as mp

class A(P):

    def __init__(self):

        P.__init__(self)

        self.q = mp.Queue()
        def b(q):
            C = q.get()
            exit_code = C.poll()
            assert(exit_code == 1), exit_code

        B = T(target = b, args=[self.q])
        B.start ()

    def run(self):
        C = Popen(args='/bin/false')
        self.q.put(C)

a = A()
a.start()
a.join()

