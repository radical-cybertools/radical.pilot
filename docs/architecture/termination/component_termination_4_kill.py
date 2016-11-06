#!/usr/bin/env python

################################################################################
#
# RP makes heavy use of processes and threads.  Communication is normally
# established via ZMQ channels -- but specifically in the case of abnormal
# conditions, an orderly termination sequence must be controled via exceptions
# and signals.
#
# Alas, a number of core python errors makes this difficult to achieve.  Amongst
# them are:
#
#   - https://bugs.python.org/issue24862 (08/2015)
#     processes can only be waited for by the parent process, which makes it
#     difficult to control process termination in a process tree if the process
#     chain gets interrupted, aven if the leaf PIDs are known.
#
#   - https://bugs.python.org/issue23395 (02/2015)
#     `SIGINT` signal handlers conflict with the *only* documented inter-thread
#     termination procedure `thread.interrupt_main()`.  This requires us to
#     either not handle `SIGINT`, or to find an alternative approach to thread
#     termination handling.
#
#   - https://bugs.python.org/issue21895 (07/2014)
#     several methods in python are not signal-interruptible, including
#     thread.join() and socket.select().  The reason is that those calls map to
#     libc level calls, but the CPython C-level signal handler is *not* directly
#     invoking the Python level signal handlers, but only sets a flag for later
#     handling.  That handling is supposed to happen at bytecode boundaries, ie.
#     after the any libc-call returns.
#
#     That could be circumvented by always using libc call equivalents with
#     a timeout.  Alas, that is not always possible -- for example, join() does
#     not have a timeout parameter.
#
#   - https://bugs.python.org/issue1856  (01/2008)
#     sys.exit can segfault Python if daemon threads are active.  This is fixed 
#     in python3, but will not be backported to 2.x, because...
#
#   - https://bugs.python.org/issue21963 (07/2014)
#     ... it would hang up for daemon threads which don't ever re-acquire the
#     GIL.  That is not an issue for us - but alas, no backport thus.  So, we
#     need to make sure our watcher threads (which are daemons) terminate 
#     on their own.
#
#   - https://bugs.python.org/issue27889 (08/2016)
#     signals can not reliably be translated into exceptions, as the delayed
#     signal handling implies races on the exception handling.  A nested
#     exception loop seems to avoid that problem -- but that cannot be enforced
#     in application code or 3rd party modules (and is very cumbersome to
#     consistently apply throughout the code stack).
#
#   - https://bugs.python.org/issue6634  (08/2009)
#     When called in a sub-thread, `sys.exit()` will, other than documented,
#     not exit the Python interpreter, nor will it (other than documented) print
#     any error to `stderr`.  The MainThread will not be notified of the exit
#     request.
#
#   - https://bugs.python.org/issue6642 (08/2009)
#     We can never fork in a sub-thread, as the fork will not clean out the
#     Python interpreter.
#
#
# Not errors, but expected behavior which makes life difficult:
#
#   - https://joeshaw.org/python-daemon-threads-considered-harmful/
#     Python's daemon threads can still be alive while the interpreter shuts
#     down.  The shutdown will remove all loaded modules -- which will lead to
#     the dreaded 
#       'AttributeError during shutdown -- can likely be ignored'
#     exceptions.  There seems no clean solution for that, but we can try to
#     catch & discard the exception in the watchers main loop (which possibly
#     masks real errors though).
#
#   - cpython's delayed signal handling can lead to signals being ignored when
#     they are translated into exceptions.
#     Assume this pseudo-code loop in a low-level 3rd party module:
# 
#     data = None
#     while not data:
#         try:
#             if fd.select(timeout):
#                 data = read(size)
#         except:
#             # select timed out - retry
#             pass
#
#     Due to the verly generous except clauses, a signal interrupting the select
#     would be interpreted as select timeout.  Those clauses *do* exist in
#     modules and core libs.
#     (This race is different from https://bugs.python.org/issue27889)
#
#   - a cpython-level API call exists to inject exceptions into other threads:
#
#       import ctypes
#       ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(thread_id),
#                                                  ctypes.py_object(e))
#
#     Alas, almost the same problems apply as for signal handling: exceptions
#     thus inserted are interpreted delayed, and are thus prone to the same
#     races as signal handlers.  Further, they can easily get lost in
#     too-generous except clauses in low level modules and core libraries.
#     Further, unlike signals, they will not interrupt any libc calls.
#     That method is thus inferior to signal handling.
#
#   - mp.join() can miss starting child processes:
#     When start() is called, join can be called immediately after.  At that
#     point, the child may, however, not yet be alive, and join would *silently*
#     return immediately.  If between that failed join and process termination
#     the child process *actually* comes up, the process termination will hang,
#     as the child has not been waited upon.
#
#   - we actually can't really use fork() either, unless it is *immediately* (as
#     in *first call*) followed by an exec, because core python modules don't
#     free locks on fork.  We monkeypatch the logging module though and also
#     ensure unlock at-fork for our own stack, but the problem remains (zmq
#     comes to mind).
#     This problem could be addressed - but this is useless unless the other
#     problems are addressed, too (the problem applies to process-bootstrapping
#     only, and is quite easy to distinguish from other bugs / races).
#
# NOTE: For some GIL details, see http://www.dabeaz.com/python/GIL.pdf
#     This focuses on performance, but contains some details relevant to
#     signal handling.  Note this is from 2009, mentions that  GIL management
#     has not changed for the past 10 years.  It has not changed by now
#     either, FWIW.
#
#
# Bottom Line: We Can't Use:
# --------------------------
#
#   - signal handlers which raise exceptions
#   - exception injects into other threads
#   - thread.interrupt_main() in combination with SIGINT(CTRL-C) handlers
#   - daemon threads
#   - multiprocessing with a method target
#
#
# Chosen Approach
# ---------------
#
# We distinguish two termination 'directions' for each component: 
#
# i)  external, where a component's termination gets triggered by calling
#     `component.stop()` on the original component object instance.
#
# ii) internal, where a component's termination gets triggered from its main
#     thread, one of its sub-threads, or one of its child processes;
#
# Any external termination will ultimately trigger an internal one.
#
# Internal termination conditions need to be communicated to the component's
# MainThread.  This happens in one of two ways:
#
# sub-threads and child processes will terminate themself if they meet a 
# termination condition, and the MainThread will be notified by a thread
# and process watcherm (which itself is a sub-thread of the MainThread).
# Upon such a notification, the component's MainThread will raise an
# exception.
#
# TODO: clarify how termination causes are communicated to the main
#       thread, to include that information in the exception.
#
# Before the MainThread raises its exception, it communicates a termination
# command to (a) its remaining sub-threads, and (b) its remaining child 
# processes.
#
# a) a `mt.Event()` (`self._thread_term`) is set, and all threads are
#    `join()`ed
#    On timeout (default: 60s), a `SYSTEM_EXIT` exception is injected into
#    the sub-thread, and it is `join()`ed again.
#
# b) a `mp.Event()` (`self._proc_term`) is set, and all child processes are
#    `join()`ed.  
#    On timeout (default: 60s), a `SIGTERM` is sent to the child process,
#    and it is `join()`ed again.
#
# Despite the mentioned problems, we *must* install a `SIGTERM` signal handler,
# as SIGTERM will be used by the OS or middleware to communicate termination to
# the pilot agents.  Those signal handlers though cannot use exceptions, and
# will thus only set the termination events.  If the event is already set, the
# signal handler will invoke `os.exit()`
#
# NOTE: The timeout fallback mechanisms are error prone per discussion above,
#       and thus should only get triggered in exceptional circumstances.  No
#       guarantees can be made on overall clean termination in those cases!
#
#       This is complicated by the fact that we cannot (reliably) interrupt any
#       operation in sub-threads and child processes, so the wait for timeout
#       will also include the time until the terminee will discover the
#       termination request, which can be an arbitrary amount of time (consider
#       the child thread is transferring a large file).  It will be up to the
#       individual component implementations to try to mitigate this effect, and
#       to check for termination signals as frequently as possible.  
#
# NOTE: The timeout approach has a hierarchy problem: a child of a child of
#       a process is waited on for 60 seconds -- but at that point the
#       original process' `join()` will already have timed out, triggering
#       a hard termination of the intermediate child, thus skipping the
#       intermediate `join()`.  The timeout should thus take the hierarchy
#       depth into account.  
#       This is ignored for now, mainly because the depth of the hierarchy
#       is not communicated / known in all places.  The above mechanism will
#       thus only be able to handle at most one hierarchy layer of unclean
#       process or thread termination, and also only if thread and process
#       termination are triggered concurrently.
# 
################################################################################
# 
#
# This code demonstrates our approach to termination, and serves as a test for
# the general problem space.
#
# We create the followin process/thread hirarchy:
#
#   - main:        'child   1'   test  1:  process 0, MainThread
#                  'thread  2'   test  2:  process 0, WatcherThread
#                  'thread  3'   test  3:  process 0, WorkerThread 1
#                  'thread  4'   test  4:  process 0, WorkerThread 2
#     - child 1:   'child   5'   test  5:  process 1, MainThread
#                  'thread  6'   test  6:  process 1, WatcherThread
#                  'thread  7'   test  7:  process 1, WorkerThread
#                  'thread  8'   test  8:  process 1, WorkerThread
#       - child 2: 'child   9'   test  9:  process 2, MainThread
#                  'thread 10'   test 10:  process 2, WatcherThread
#                  'thread 11'   test 11:  process 2, WorkerThread
#                  'thread 12'   test 12:  process 2, WorkerThread
#       - child 3: 'child  13'   test  -:  process 3, MainThread
#                  'thread 14'   test  -:  process 3, WatcherThread
#                  'thread 15'   test  -:  process 3, WorkerThread
#                  'thread 16'   test  -:  process 3, WorkerThread
#     - child 4:   'child  17'   test  -:  process 4, MainThread
#                  'thread 18'   test  -:  process 4, WatcherThread
#                  'thread 19'   test  -:  process 4, WorkerThread
#                  'thread 20'   test  -:  process 4, WorkerThread
#       - child 5: 'child  21'   test  -:  process 5, MainThread
#                  'thread 22'   test  -:  process 5, WatcherThread
#                  'thread 23'   test  -:  process 5, WorkerThread
#                  'thread 24'   test  -:  process 5, WorkerThread
#       - child 6: 'child  25'   test  -:  process 6, MainThread
#                  'thread 26'   test  -:  process 6, WatcherThread
#                  'thread 27'   test  -:  process 6, WorkerThread
#                  'thread 28'   test  -:  process 6, WorkerThread
#
# Worker threads will work on random items, consuming between 1 and 90 seconds
# of time each.  The enumerated entities will raise exceptions after 2 minutes,
# if the environment variable `RU_RAISE_ON_<N>` is set to `1`, where `<N>` is
# the respecitve enumeration value.  An additional test is defined by pressing
# `CONTROL-C`.
#
# A test is considered successful when all of the following conditions apply:
#
#   - the resulting log files contain termination notifications for all entities
#   - the application catches a `RuntimeError('terminated')` exception or a
#     `KeyboardInterrupt` exception
#   - no processes or threads remain (also, no zombies)
#
# ==============================================================================


import os
import sys
import time
import random
import signal
import setproctitle

import threading       as mt
import multiprocessing as mp

import radical.utils   as ru

TIMEOUT  =  3
WORK_MIN =  1
WORK_MAX = 10
WORK_N   =  5

# ------------------------------------------------------------------------------
#
config = {
        'watcher  0' : None, 
        'child    1' : {
            'watcher  2' : None, 
         #  'worker   3' : None, 
            'worker   4' : None, 
         #  'child    5' : {
         #      'watcher  6' : None, 
         #      'worker   7' : None, 
         #      'worker   8' : None, 
         #      'child    9' : {
         #          'watcher 10' : None, 
         #          'worker  11' : None, 
         #          'worker  12' : None, 
         #      },
         #      'child  13' : {
         #          'watcher 14' : None, 
         #          'worker  15' : None, 
         #          'worker  16' : None, 
         #      }
#           },
#           'child   17' : {
#               'watcher 18' : None, 
#               'worker  19' : None, 
#               'worker  20' : None, 
#               'child   21' : {
#                   'watcher 22' : None, 
#                   'worker  23' : None, 
#                   'worker  24' : None, 
#               },
#               'child   25' : {
#                   'watcher 26' : None, 
#                   'worker  27' : None, 
#                   'worker  28' : None, 
#               }
#           }
        }
    }


# ------------------------------------------------------------------------------
#
def work(worker):

    # a simple worker routine which sleeps repeatedly for a random number of
    # seconds, until a term signal is set.  The given 'worker' can be a thread
    # or process, or in fact anything which has a self.uid and self.term.

    try:
        worker.log.info('%-10s : work start' % worker.uid)

        while not worker.term.is_set():

            item = random.randint(WORK_MIN,WORK_MAX)
            worker.log.info('%-10s : %ds sleep start' % (worker.uid, item))
            time.sleep(item)
            worker.log.info('%-10s : %ds sleep stop'  % (worker.uid, item))

        worker.log.info('%-10s : work term requested' % worker.uid)

    except Exception as e:
        worker.log.info('%-10s : work fail [%s]' % (worker.uid, e))



# ------------------------------------------------------------------------------
#
class Child(mp.Process):
    
    # --------------------------------------------------------------------------
    #
    def __init__(self, name, cfg, term, verbose):

        mp.Process.__init__(self)

        self.uid       = name
        self.verbose   = verbose
        self.log       = ru.get_logger('radical.' + self.uid, level=verbose)
        self.is_parent = True
        self.cfg       = cfg
        self.wterm     = term             # term sig shared with parent watcher
        self.term      = mp.Event()       # private term signal
        self.killed    = False

        # start watcher for own children and threads
        self.watcher   = Watcher(cfg, verbose='error') 
        self.watcher.start()


    # --------------------------------------------------------------------------
    #
    def stop(self):

        assert(self.pid)              # child was spanwed
        assert(self.is_parent)        # is parent process
        assert(ru.is_main_thread())   # is main thread

        if not self.is_alive():
            self.log.info('%-10s : child not alive' % self.uid)
            return

        self.term.set()

        self.log.info('%-10s : stop child' % self.uid)
        self.watcher.stop()
        if None == ru.watch_condition(cond=self.watcher.is_alive,
                                      target=False,
                                      timeout=TIMEOUT):
            self.log.info('%-10s : could not stop child - kill' % self.uid)
            self.watcher.kill()
        self.watcher.join()
        self.log.info('%-10s : child stopped' % self.uid)


    # --------------------------------------------------------------------------
    #
    def kill(self):

        assert(ru.is_main_thread())
        assert(self.is_parent)

        if not self.is_alive():
            self.log.info('%-10s : child not alive' % self.uid)
            return

        signal.kill(self.child, signal.SIGUSR2)
        self.log.info('%-10s : child killed' % self.uid)


    # --------------------------------------------------------------------------
    #
    def run(self):

        self.is_parent = False
        self.log       = ru.get_logger('radical.' + self.uid + '.child',
                                       level=self.verbose)
        self.dh        = ru.DebugHelper()
        setproctitle.setproctitle('rp.%s.child' % self.uid)

        def handler(signum, sigframe):
            self.log.info('%-10s : signal handled' % self.uid)
            self.term.set()
        signal.signal(signal.SIGUSR2, handler)

        work(self)


# ------------------------------------------------------------------------------
#
class Worker(mt.Thread):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, cfg, term, verbose):

        mt.Thread.__init__(self)
        self.uid     = name
        self.verbose = verbose
        self.log     = ru.get_logger('radical.' + self.uid, level=verbose)
        self.cfg     = cfg
        self.term    = term

        # we don't allow subsubthreads
        if not ru.is_main_thread():
            raise RuntimeError('threads must be spawned by MainThread [%s]' % \
                    ru.get_thread_name())


    # --------------------------------------------------------------------------
    #
    def stop(self):

        if not self.is_alive():
            self.log.info('%-10s : child not alive' % self.uid)
            return
        
        self.term.set()


    # --------------------------------------------------------------------------
    #
    def kill(self):

        # this can only be called from the thread owner, ie. the main thread
        assert(ru.is_main_thread())
        
        if not self.is_alive():
            self.log.info('%-10s : child not alive' % self.uid)
            return

        # inject exit request (ru.ThreadExit) into child thread
        ru.raise_in_thread(self.ident)
        self.log.info('%-10s : child killed' % self.uid)


    # --------------------------------------------------------------------------
    #
    def run(self):

        try:
            self.log = ru.get_logger('radical.' + self.uid + '.child', 
                                     level=self.verbose)
            work(self)
        except ru.ThreadExit:
            self.log.info('%-10s : thread exit requested' % self.uid)



# ------------------------------------------------------------------------------
#
class Watcher(mt.Thread):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, verbose):

        mt.Thread.__init__(self)

        self.uid = None
        for name,_ in cfg.iteritems():
            if 'watcher' in name:
                if self.uid:
                    raise ValueError('only one watcher supported')
                self.uid = name

        self.log     = ru.get_logger('radical.' + self.uid + '.child', 
                                     level=verbose)
        self.cfg     = cfg
        self.term    = mt.Event()
        self.things  = list()

        self._thread_term = mt.Event()
        self._proc_term   = mp.Event()

        # first create threads and procs to be watched
        for name,_cfg in cfg.iteritems():
            self.log.info('child %s: ', name)
            if 'child' in name:
                child = Child(name=name, 
                              cfg=_cfg, 
                              term=self._proc_term,
                              verbose=verbose)
                child.start()
                self.things.append(child)
            elif 'worker' in name:
                worker = Worker(name=name, 
                                cfg=_cfg, 
                                term=self._thread_term, 
                                verbose=verbose)
                worker.start()
                self.things.append(worker)

      # if not self.things:
      #     raise ValueError('nothing to watch')

      # if not self.uid:
      #     raise ValueError('no watcher in config')
        

    # --------------------------------------------------------------------------
    #
    def stop(self):

        # NOTE: this can be called from the watcher subthread
        
        if not self.is_alive():
            self.log.info('%-10s : child not alive' % self.uid)
            return

        # make sure the watcher loop is gone
        self.term.set()

        # tell children whats up
        self._proc_term.set()
        self._thread_term.set()

        for t in self.things:
            self.log.info('%-10s : join    %s' % (self.uid, t.uid))
            t.stop()
            t.join(timeout=TIMEOUT)

            if t.is_alive():
                self.log.info('%-10s : kill    %s' % (self.uid, t.uid))
                # FIXME: differentiate between procs and threads
                ru.raise_in_thread(tident=t.ident)
                t.join(timeout=TIMEOUT)

            if t.is_alive():
                self.log.info('%-10s : zombied %s' % (self.uid, t.uid))
            else:
                self.log.info('%-10s : joined  %s' % (self.uid, t.uid))

        self.log.info('%-10s : stopped' % self.uid)


    # --------------------------------------------------------------------------
    #
    def check(self):
        return bool(self.term.is_set())


    # --------------------------------------------------------------------------
    #
    def kill(self):

        assert(ru.is_main_thread())

        if not self.is_alive():
            self.log.info('%-10s : child not alive' % self.uid)
            return

        # inject exit request (ru.ThreadExit) into child thread
        ru.raise_in_thread(tident=self.ident)
        self.log.info('%-10s : %s killed' % (self.uid, self.ident))


    # --------------------------------------------------------------------------
    #
    def run(self):

        try:
            self.log.info('%-10s : start' % self.uid)
            while not self.term.is_set():
                time.sleep(2)
                for t in self.things:
                    if not t.is_alive():
                        self.log.info('%-10s : %s died' % (self.uid, t.uid))
                        # a child died.  We kill the other children and
                        # terminate.
                      # self.stop()
                        return
                    self.log.info('%-10s : %s ok' % (self.uid, t.uid))

        except ThreadExit:
            raise RuntimeError('%-10s : watcher exit requested [%s]' % \
                    (self.uid, self.ident))

        except Exception as e:
            raise RuntimeError('%-10s : watcher error' % self.uid)

        finally:
            self.log.info('%-10s : stop' % self.uid)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    dh = ru.DebugHelper()
    setproctitle.setproctitle('rp.main')

    watcher = Watcher(config, verbose='debug')
    watcher.start()
    time.sleep(5)
    print '===================================='
    watcher.stop()
    print 'after stop'
    watcher.join()
    print 'after join 2'

# ------------------------------------------------------------------------------

