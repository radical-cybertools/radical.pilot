#!/usr/bin/env python

# Daemon processes can't fork child processes in Python, because...
# Well, they just can't.  We want to use daemons though to avoid hanging
# processes if, for some reason, communication of termination conditions
# fails.
#
# Patchy McPatchface to the rescue (no, I am not kidding): we remove
# that useless assert (of all things!) on the fly.
#
# NOTE: while this works, we seem to have the socketpair-based detection
#       stable enough to not need the monkeypatch.
#
#
# _daemon_fork_patch = '''\
#     *** process_orig.py  Sun Nov 20 20:02:23 2016
#     --- process_fixed.py Sun Nov 20 20:03:33 2016
#     ***************
#     *** 5,12 ****
#           assert self._popen is None, 'cannot start a process twice'
#           assert self._parent_pid == os.getpid(), \\
#                  'can only start a process object created by current process'
#     -     assert not _current_process._daemonic, \\
#     -            'daemonic processes are not allowed to have children'
#           _cleanup()
#           if self._Popen is not None:
#               Popen = self._Popen
#     --- 5,10 ----
#     '''
#
# import patchy
# patchy.mc_patchface(mp.Process.start, _daemon_fork_patch)
 

import os
import sys
import time
import multiprocessing as mp

def work_2():
    print 'foo'

def work_1():

    proc = mp.Process(target=work_2)
    proc.start()
    proc.join()


def test():

    child = mp.Process(target=work_1)
    child.daemon = True

    # NOTE: we expect this to work out of the box.  
    #       It does not.
    child.start()  
    child.join()


if __name__ == '__main__':
    test()

