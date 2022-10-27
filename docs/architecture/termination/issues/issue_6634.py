#!/usr/bin/env python

# https://bugs.python.org/issue6634

import os
import sys
import time
import threading as mt

def work():

    print 'exit now'
    # NOTE: This should call the python interpreter to exit.
    #       This is not the case, only the thread is terminated.
    sys.exit()


def test():

    child = mt.Thread(target=work)
    child.daemon = True
    child.start()

    time.sleep(100)

if __name__ == '__main__':
    test()
