#!/usr/bin/env python3

# This is an example multithreaded program that is used
# by different examples and tests.

import sys
import time
import socket
import threading


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    host = socket.gethostname().split('.')[0]

    def _thread(size, rank):
        time.sleep(1)
        print("%d/%d/%s"  % (rank + 1, size, host))

    threads = list()
    size    = int(sys.argv[1])
    for n in range(size):
        threads.append(threading.Thread(target=_thread, args=[size, n]))

    for thread in threads:
        time.sleep(0.01)  # ensure ordered output
        thread.start()

    for thread in threads:
        thread.join()

# ------------------------------------------------------------------------------

