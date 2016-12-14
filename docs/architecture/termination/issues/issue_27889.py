#!/usr/bin/env python

# https://bugs.python.org/issue27889

#!/usr/bin/env python

import multiprocessing as mp
import threading       as mt
import signal
import time
import os

# from uuid.py line 400
import ctypes, ctypes.util
lib = ctypes.CDLL(ctypes.util.find_library('uuid'))

def sigusr2_handler(signum, frame):
    raise RuntimeError('caught sigusr2')
signal.signal(signal.SIGUSR2, sigusr2_handler)

def sub(pid):
    time.sleep(1)
    os.kill(pid, signal.SIGUSR2)

try:
  # p = mp.Process(target=sub, args=[os.getpid()])
  # p.start()
    t = mt.Thread(target=sub, args=[os.getpid()])
    t.start()
    time.sleep(3)
except Exception as e:
    # we should see this message, but should never see a stack trace.  We do.
    print 'except: %s' % e
else:
    print 'unexcepted'
finally:
  # p.join()
    t.join()


