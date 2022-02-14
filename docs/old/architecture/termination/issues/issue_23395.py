#!/usr/bin/env python

# https://bugs.python.org/issue23395

import signal, threading, thread, time
signal.signal(signal.SIGINT, signal.SIG_DFL) # or SIG_IGN

def thread_run():
    # NOTE: This should interrupt the main thread w/o an error, but 
    #       We see an error (int not callable)
    thread.interrupt_main()

t = threading.Thread(target=thread_run)
t.start()
time.sleep(1)

