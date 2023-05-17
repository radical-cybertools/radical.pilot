#!/usr/bin/env python

# https://bugs.python.org/issue1856

import os
import sys
import time
import threading as mt

def work(parent):

    i = 0
    while True:
      print i,
      i += 1
      time.sleep(0.1)

def test():

    parent = None
    for th in mt.enumerate():
        if th.name == 'MainThread':
            parent = th
            break

    child = mt.Thread(target=work, args=[parent])
    child.daemon = True
    child.start()

    time.sleep(0.3)

    # NOTE: We expect sys.exit() to exit and terminate/abandon daemon threads.
    #       That does not always happen
    sys.exit()

if __name__ == '__main__':
    test()
