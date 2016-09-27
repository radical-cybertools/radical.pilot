#!/usr/bin/env python

# ------------------------------------------------------------------------------
#
# RP makes heavy use of processes and threads.  Communication is normally
# established via ZMQ channels -- but specifically in the case of abnormal
# conditions, an orderly termination sequence must be controled via exceptions
# and signals.
#
# Alas, a number of core python errors makes this difficult to achieve.  Amongst
# them are:
#
#   - http://bugs.python.org/issue24862
#     processes can only be waited for by the parent process, which makes it
#     difficult to control process termination in a process tree if the process
#     chain gets interrupted, aven if the leaf PIDs are known.
#
#   - http://bugs.python.org/issue23395
#     `SIGINT` signal handlers conflict with the *only* documented inter-thread
#     termination procedure `thread.interrupt_main()`.  This requires us to
#     either not handle `SIGINT`, or to find an alternative approach to thread
#     termination handling.
#
#   - https://bugs.python.org/issue21895
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
#   - http://bugs.python.org/issue1856
#     sys.exit can segfault Python if daemon threads are active.  This is fixed 
#     in python3, but will not be backported to 2.x, because...
#
#   - http://bugs.python.org/issue21963
#     ... it would hang up for daemon threads which don't ever re-acquire the
#     GIL.  That is not an issue for us - but alas, no backport thus.  So, we
#     need to make sure our watcher threads (which are daemons) terminate 
#     on their own.
#
#   - https://bugs.python.org/issue27889
#     signals can not reliably be translated into exceptions, as the delayed
#     signal handling implies races on the exception handling.  A nested
#     exception loop seems to avoid that problem -- but that cannot be enforced
#     in application code or 3rd party modules (and is very cumbersome to
#     consistently apply throughout the code stack).
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
#
# Bottom line:
#
#   - we can't use
#     - signal handlers which raise exceptions
#     - exception injects into other threads
#     - thread.interrupt_main() in combination with SIGINT(CTRL-C)
#     - daemon threads
#     - multiprocessing with a method target
#
#
# NOTE: The code below does *not* demonstrate a workaround for all issues, just
#       for some of them.  At this point, I am not aware of a clean (ie.
#       reliable) approach to thread and process termination with cpython.
#       I leave the code below as-is though, for the time being, to remind 
#       myself of better times, when there was hope... :P
#
# ------------------------------------------------------------------------------
#
# This code demonstrates a workaround for those issues, and serves as a test for
# the general problem space.
#
#   - main() creates 3 processes
#   - each of the processes creates 3 threads: 2 workers and a watcher 
#     (watching the worker)
#   - main() additionally creates a thread which watches process 2
#
# The resulting hierarchy is:
#
#   - main:      1: process 0, MainThread
#                2: process 0, ProcessWatcherThread
#     - child 1: 3: process 1, MainThread
#                4: process 1, WorkerThread
#                -: process 1, WorkerThread
#                6: process 1, ThreadWatcherThread
#     - child 2: -: process 2, MainThread
#                -: process 2, WorkerThread
#                -: process 2, WorkerThread
#                -: process 2, ThreadWatcherThread
#     - child 3: -: process 3, MainThread
#                -: process 3, WorkerThread
#                -: process 3, WorkerThread
#                -: process 3, ThreadWatcherThread
#
# We create 6 test cases, for each of the resulting entities, where for each
# case the respective entity will raise a RuntimeError.  The expected result is
# that the Exception is caught, and shutdown progresses up the hierarchy,
# leading to a clean shutdown of main, with no left over prcesses or threads,
# and which each process/thread logging its clean termination.
#
# The used approach is as follows:
#
#  - any entity which creates other entities must watch those, by pulling state 
#    of the child entities, in a separate watcher thread.
#  - that separate watcher thread will inform the parent via
#    `ru.cancel_main_thread()` about failing children, which will raise
#    a `KeyboardInterrupt` exception in the main thread.
#  - that exception must be caught, leading to termination of the parent
#    component
#  - the watcher thread must not be waited upon, and thus will be a daemonized
#    thread.  The watcher will thus also watch the main thread:
#        for i in mt.enumerate():
#            if i.name == "MainThread":
#                if not i.is_alive():
#                    self._log.info('main thread gone - terminate')
#                    sys.exit(-1)
#    The watcher will further shut down when
#      - self._term is set
#      - it has nothing to watch anymore
#    The latter is somewhat cumbersome to handle, as the component needs to keep
#    track of the number of watchables and needs to possibly restart the watcher,
#    but whatever.
#  - all termination is downstream and process-local.  In other words,
#    any component which encounters an error will 
#    - terminate its child threads   (if any), excluding the watcher thread
#    - terminate its child processes (if any), by sending a `TERM` signal (15)
#    - terminate itself, by calling `sys.exit(1)`
#    - *not* attempt to otherwise communicate its termination to the entity
#      which created it.
#
#  - SIGCHILD *can not* be used to signal child process termination, because of 
#    the Python bugs listed above.
#  - signals *can not* be used in general to actively communicate error 
#    conditions to parents, for the same reaons.
#  - SIGTERM is used to communicate termination to *child* processes -- python
#    promises to *eventually* invoke signal handlers in the child's main thread.
#    Thus terminated children must be waited for, to avoid zombies.  A wait
#    timeout and subsequend `SIGKILL` (9) is appropriate.
#  - daemon threads *can not* be expected to die on sys.exit()
#
# ------------------------------------------------------------------------------


import os
import sys
import time
import signal

import threading       as mt
import multiprocessing as mp

import radical.utils   as ru

# ------------------------------------------------------------------------------
#
dh = ru.DebugHelper()


# ------------------------------------------------------------------------------
#
SLEEP    = 1
RAISE_ON = 3


# ------------------------------------------------------------------------------
#
class WorkerThread(mt.Thread):

    def __init__(self, num, pnum, tnum):

        self.num  = num
        self.pnum = pnum
        self.tnum = tnum
        self.pid  = os.getpid() 
        self.tid  = mt.currentThread().ident 
        self.uid  = "t.%d.%s %8d.%s" % (self.pnum, self.tnum, self.pid, self.tid)
        self.term = mt.Event()
        
        mt.Thread.__init__(self, name=self.uid)

      # print '%s create' % self.uid


    def stop(self):
        self.term.set()


    def run(self):

        try:
          # print '%s start' % self.uid

            while not self.term.is_set():

              # print '%s run' % self.uid
                time.sleep(SLEEP)

                if self.num == 4 and self.pnum == 1:
                    print "4"
                    ru.raise_on(self.uid, RAISE_ON)
    
          # print '%s stop' % self.uid
    
        except Exception as e:
            print '%s error %s [%s]' % (self.uid, e, type(e))
    
        except SystemExit:
            print '%s exit' % (self.uid)
    
        except KeyboardInterrupt:
            print '%s intr' % (self.uid)
    
        finally:
            print '%s final' % (self.uid)


# ------------------------------------------------------------------------------
#
class WatcherThread(mt.Thread):

    # All entities which use a watcher thread MUST except KeyboardInterrupt, 
    # as we'll use that signal to communicate error conditions to the
    # main thread.
    #
    # The watcher thread is a daemon thread: it must not be joined.  We thus
    # overload join() and disable it.
    #
    # To avoid races and locks during termination, we frequently check if the
    # MainThread is still alive, and terminate otherwise
    
    def __init__(self, to_watch, num, pnum, tnum):

        self.to_watch = to_watch
        self.num      = num
        self.pnum     = pnum
        self.tnum     = tnum
        self.pid      = os.getpid() 
        self.tid      = mt.currentThread().ident 
        self.uid      = "w.%d.%s %8d.%s" % (self.pnum, self.tnum, self.pid, self.tid)
        self.term     = mt.Event()

        self.main     = None
        for t in mt.enumerate():
             if t.name == "MainThread":
                 self.main = t

        if not self.main:
            raise RuntimeError('%s could not find main thread' % self.uid)
        
        mt.Thread.__init__(self, name=self.uid)
        self.daemon = True  # this is a daemon thread

        print '%s create' % self.uid


    # --------------------------------------------------------------------------
    #
    def stop(self):

        self.term.set()


    # --------------------------------------------------------------------------
    #
    def join(self):

        print '%s: join ignored' % self.uid


    # --------------------------------------------------------------------------
    #
    def run(self):

        try:
            print '%s start' % self.uid

            while not self.term.is_set():

              # print '%s run' % self.uid
                time.sleep(SLEEP)

                if self.num == 2 and self.pnum == 0:
                    print "2"
                    ru.raise_on(self.uid, RAISE_ON)

                if self.num == 5 and self.pnum == 1:
                    print "5"
                    ru.raise_on(self.uid, RAISE_ON)

                # check watchables
                for thing in self.to_watch:
                    if thing.is_alive():
                        print '%s event: thing %s is alive' % (self.uid, thing.uid)
                    else:
                        print '%s event: thing %s has died' % (self.uid, thing.uid)
                        ru.cancel_main_thread()
                        assert(False) # we should never get here

                # check MainThread
                if not self.main.is_alive():
                    print '%s: main thread gone - terminate' % self.uid
                    self.stop()

            print '%s stop' % self.uid


        except Exception as e:
            print '%s error %s [%s]' % (self.uid, e, type(e))
            ru.cancel_main_thread()
       
        except SystemExit:
            print '%s exit' % (self.uid)
            # do *not* cancel main thread here!  We get here after the cancel
            # signal has been sent in the main loop above
       
        finally:
            print '%s final' % (self.uid)


# ------------------------------------------------------------------------------
#
class ProcessWorker(mp.Process):
    
    def __init__(self, num, pnum):

        self.num   = num
        self.pnum  = pnum
        self.ospid = os.getpid() 
        self.tid   = mt.currentThread().ident 
        self.uid   = "p.%d.%s %8s.%s" % (self.pnum, 0, self.ospid, self.tid)

        print '%s create' % (self.uid)

        mp.Process.__init__(self, name=self.uid)

        self.worker  = None
        self.watcher = None


    # --------------------------------------------------------------------------
    #
    def join(self):

        # Due to the overloaded stop, we may see situations where the child
        # process pid is not known anymore, and an assertion in the mp layer
        # gets triggered.  We except that assertion and assume the join
        # completed.
        #
        # NOTE: the except can mask actual errors

        try:
            # when start() is called, join can be called immediately after.  At
            # that point, the child may, however, not yet be alive, and join
            # would *silently* return immediately.  If between that failed join
            # and process termination the child process *actually* comes up, the
            # process termination will hang, as the child has not been waited
            # upon.
            #
            # We thus use a timeout on join, and, when the child did not appear
            # then, attempt to terminate it again.
            #
            # TODO: choose a sensible timeout.  Hahahaha...

            print '%s join: child join %s' % (self.uid, self.pid)
            mp.Process.join(self, timeout=1)

            # give child some time to come up in case the join
            # was racing with creation
            time.sleep(1)  

            if self.is_alive(): 
                # we still (or suddenly) have a living child - kill/join again
                self.stop()
                mp.Process.join(self, timeout=1)

            if self.is_alive():
                raise RuntimeError('Cannot kill child %s' % self.pid)
                
            print '%s join: child joined' % (self.uid)

        except AssertionError as e:
            print '%s join: ignore race' % (self.uid)


    # --------------------------------------------------------------------------
    #
    def stop(self):

        # we terminate all threads and processes here.

        # The mp stop can race with internal process termination.  We catch the
        # respective OSError here.

        # In some cases, the popen module seems finalized before the stop is
        # gone through.  I suspect that this is a race between the process
        # object finalization and internal process termination.  We catch the
        # respective AttributeError, caused by `self._popen` being unavailable.
        #
        # NOTE: both excepts can mask actual errors

        try:
            # only terminate child if it exists -- but there is a race between
            # check and signalling anyway...
            if self.is_alive():
                self.terminate()  # this sends SIGTERM to the child process
                print '%s stop: child terminated' % (self.uid)

        except OSError as e:
            print '%s stop: child already gone' % (self.uid)

        except AttributeError as e:
            print '%s stop: popen module is gone' % (self.uid)


    # --------------------------------------------------------------------------
    #
    def run(self):

        # We can't catch signals from child processes and threads, so we only
        # look out for SIGTERM signals from the parent process.  Upon receiving
        # such, we'll stop.
        #
        # We also start a watcher (WatcherThread) which babysits all spawned
        # threads and processes, an which will also call stop() on any problems.
        # This should then trickle up to the parent, who will also have
        # a watcher checking on us.

        self.ospid = os.getpid() 
        self.tid   = mt.currentThread().ident 
        self.uid   = "p.%d.0 %8d.%s" % (self.pnum, self.ospid, self.tid)

        try:
            # ------------------------------------------------------------------
            def sigterm_handler(signum, frame):
                # on sigterm, we invoke stop(), which will exit.
                # Python should (tm) give that signal to the main thread.  
                # If not, we lost.
                assert(mt.currentThread().name == 'MainThread')
                self.stop()
            # ------------------------------------------------------------------
            signal.signal(signal.SIGTERM, sigterm_handler)

            print '%s start' % self.uid

            # create worker thread
            self.worker1 = WorkerThread(self.num, self.pnum, 0)
            self.worker1.start()
     
            self.worker2 = WorkerThread(self.num, self.pnum, 0)
            self.worker2.start()
     
            self.watcher = WatcherThread([self.worker1, self.worker2], 
                                          self.num, self.pnum, 1)
            self.watcher.start()

            while True:
                print '%s run' % self.uid
                time.sleep(SLEEP)
                if self.num == 3 and self.pnum == 1:
                    print "3"
                    ru.raise_on(self.uid, RAISE_ON)

            print '%s stop' % self.uid

        except Exception as e:
            print '%s error %s [%s]' % (self.uid, e, type(e))
       
        except SystemExit:
            print '%s exit' % (self.uid)
       
        except KeyboardInterrupt:
            print '%s intr' % (self.uid)
       
        finally:
            # we came here either due to errors in run(), KeyboardInterrupt from
            # the WatcherThread, or clean exit.  Either way, we stop all
            # children.
            self.stop()


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        # the finally clause of run() can again be interrupted!  We thus move
        # the complete finalization into a separate method which shields the
        # finalization from that.  It will though abort any finalization on
        # interruption, as we have no means to distinguich finalization errors
        # from external interruptions.  This can, however lead to incomplete
        # finalization.
        #
        # This problem is mitigated when `ru.cancel_main_thread(once=True)` is
        # used to initiate finalization, as that method will make sure that any
        # signal is sent at most once to the process, thus avoiding any further
        # interruption.

        try:
            print '%s final' % (self.uid)
            if self.watcher:
                print '%s final -> twatcher' % (self.uid)
                self.watcher.stop()
            if self.worker:
                print '%s final -> tworker' % (self.uid)
                self.worker.stop()

            if self.watcher:
                print '%s final => twatcher' % (self.uid)
                self.watcher.join
                print '%s final |> twatcher' % (self.uid)
            if self.worker:
                print '%s final => tworker' % (self.uid)
                self.worker.join
                print '%s final |> tworker' % (self.uid)

            print '%s final' % (self.uid)

        except Exception as e:
            print '%s error %s [%s]' % (self.uid, e, type(e))
       
        except SystemExit:
            print '%s exit' % (self.uid)
       
        except KeyboardInterrupt:
            print '%s intr' % (self.uid)
       
        finally:
            print 'worker finalized'



# ------------------------------------------------------------------------------
#
def main(num):

    # *always* install SIGTERM and SIGINT handlers, which will translate those
    # signals into exceptable exceptions.

    watcher = None
    p1      = None
    p2      = None

    try:
        pid = os.getpid() 
        tid = mt.currentThread().ident 
        uid = "m.0.0 %8d.%s" % (pid, tid)

        print '%s start' % uid
        p1 = ProcessWorker(num, 1)
        p2 = ProcessWorker(num, 2)
        
        p1.start()
        p2.start()

        watcher = WatcherThread([p1, p2], num, 0, 1)
        watcher.start()

        while True:
            print '%s run' % uid
            time.sleep(SLEEP)
            if num == 1:
                print "1"
                ru.raise_on(uid, RAISE_ON)

        print '%s stop' % uid

    except RuntimeError as e:
        print '%s error %s [%s]' % (uid, e, type(e))
    
    except SystemExit:
        print '%s exit' % (uid)
    
    except KeyboardInterrupt:
        print '%s intr' % (uid)
    
    finally:
        finalize(p1, p2, watcher)


def finalize(p1, p2, watcher):

    try:
        if p1:
            print '%s final -> p.1' % (uid)
            p1.stop()
            print '%s final => p.1' % (uid)
            p1.join()
            print '%s final |> p.1' % (uid)
        else:
            print '%s final |? p.1' % (uid)

        if p2:
            print '%s final -> p.2' % (uid)
            p2.stop()
            print '%s final => p.2' % (uid)
            p2.join()
            print '%s final |> p.2' % (uid)
        else:
            print '%s final |? p.2' % (uid)

        if watcher:
            print '%s final -> pwatcher' % (uid)
            watcher.stop()
            print '%s final => pwatcher' % (uid)
            watcher.join()
            print '%s final |> pwatcher' % (uid)
        else:
            print '%s final |? pwatcher' % (uid)
        print '%s final' % (uid)

    except RuntimeError as e:
        print '%s finalize error %s [%s]' % (uid, e, type(e))
    
    except SystemExit:
        print '%s finalize exit' % (uid)
    
    except KeyboardInterrupt:
        print '%s finalize intr' % (uid)
    
    finally:
        print 'finalized'


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    uid = 'm.0.0 %8s.%15s' % (0, 0)

    if len(sys.argv) > 1:
        num = int(sys.argv[1])
    else:
        num = 1

    try:
        print '-------------------------------------------'
        main(num)

    except RuntimeError as e:
        print '%s error %s [%s]' % (uid, e, type(e))
    
    except SystemExit:
        print '%s exit' % (uid)
    
    except KeyboardInterrupt:
        print '%s intr' % (uid)
    
    finally:
        print 'success %d\n\n' % num

    print '-------------------------------------------'


# ------------------------------------------------------------------------------

