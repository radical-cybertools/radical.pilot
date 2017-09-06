#!/usr/bin/env python

# https://bugs.python.org/file35815/signal_pause_doesnt_wake_up.py

import os
import signal
import subprocess
import sys
import time
import threading


def sigchld_handler(signum, frame):
    print('caught SIGCHLD in process {0}'.format(os.getpid()))
signal.signal(signal.SIGCHLD, sigchld_handler)


def main():
    subprocess.Popen(['sleep', '1'])
    time.sleep(1)
    print('exit now')

    # NOTE: This should exit the main thread - it does not.
    sys.exit()

if __name__ == '__main__':
    t = threading.Thread(target=main)
    t.start()
    signal.pause()

