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
# This code demonstrates a workaround for those issues, and serves as a test for
# the general problem space.
#
#   - main() creates 2 processes
#   - each of the processes creates 2 threads: a worker and a watcher 
#     (watching the worker)
#   - main() additionally creates a thread which watches process 2
#
# The resulting hierarchy is:
#
#   - main:      1: process 0, MainThread
#                2: process 0, ProcessWatcherThread
#     - child 1: 3: process 1, MainThread
#                4: process 1, WorkerThread
#                5: process 1, ThreadWatcherThread
#     - child 2: -: process 2, MainThread
#                -: process 2, WorkerThread
#                -: process 2, ThreadWatcherThread
#
# We create 8 test cases, for each of the resulting entities, where for each
# case the respective entity will raise a RuntimeError.  The expected result is
# that the Exception is caught, and shutdown progresses up the hierarchy,
# leading to a clean shutdown of main, with no left over prcesses or threads,
# and which each process/thread logging its clean termination.
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
def sigterm_handler(signum, frame):
    print 'sigterm handler %s' % os.getpid()
    raise RuntimeError('sigterm')


# ------------------------------------------------------------------------------
#
SLEEP    = 0.1
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
    
    def __init__(self, to_watch, num, pnum, tnum):

        self.to_watch = to_watch
        self.num      = num
        self.pnum     = pnum
        self.tnum     = tnum
        self.pid      = os.getpid() 
        self.tid      = mt.currentThread().ident 
        self.uid      = "w.%d.%s %8d.%s" % (self.pnum, self.tnum, self.pid, self.tid)
        self.term     = mt.Event()
        
        mt.Thread.__init__(self, name=self.uid)

        print '%s create' % self.uid


    def stop(self):

        self.term.set()


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

                for thing in self.to_watch:
                    if not thing.is_alive():
                        print '%s event: something %s died' % (self.uid, thing.uid)
                        ru.cancel_main_thread()
                        raise RuntimeError('something %s died - assert' % thing.uid)

            print '%s stop' % self.uid


        except Exception as e:
            print '%s error %s [%s]' % (self.uid, e, type(e))
            ru.cancel_main_thread()
       
        except SystemExit:
            print '%s exit' % (self.uid)
            # do *not* cancel main thread here!  We get here 
           #ru.cancel_main_thread()
       
        except KeyboardInterrupt:
            print '%s intr' % (self.uid)
            ru.cancel_main_thread()
       
        finally:
            print '%s final' % (self.uid)


# ------------------------------------------------------------------------------
#
class ProcessWorker(mp.Process):
    
    def __init__(self, num, pnum):

        self.num  = num
        self.pnum = pnum
        self.ospid= os.getpid() 
        self.tid  = mt.currentThread().ident 
        self.uid  = "p.%d.%s %8s.%s" % (self.pnum, 0, self.ospid, self.tid)

        print '%s create' % (self.uid)

        mp.Process.__init__(self, name=self.uid)

        self.worker  = None
        self.watcher = None


    def stop(self):

        return self.terminate()


    def run(self):

        signal.signal(signal.SIGTERM, sigterm_handler)

        self.ospid  = os.getpid() 
        self.tid  = mt.currentThread().ident 
        self.uid  = "p.%d.0 %8d.%s" % (self.pnum, self.ospid, self.tid)

        try:
            print '%s start' % self.uid

            # create worker thread
            self.worker = WorkerThread(self.num, self.pnum, 0)
            self.worker.start()
     
            self.watcher = WatcherThread([self.worker], self.num, self.pnum, 1)
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
            if self.watcher:
                print '%s final -> twatcher' % (self.uid)
                self.watcher.stop()
                print '%s final => twatcher' % (self.uid)
                self.watcher.join
                print '%s final |> twatcher' % (self.uid)
            if self.worker:
                print '%s final -> tworker' % (self.uid)
                self.worker.stop()
                print '%s final => tworker' % (self.uid)
                self.worker.join
                print '%s final |> tworker' % (self.uid)
            print '%s final' % (self.uid)

        print 'worker done'



# ------------------------------------------------------------------------------
#
def main(num):

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
        if p1:
            print '%s final -> p1' % (uid)
            p1.stop()
            print '%s final => p1' % (uid)
            p1.join()
            print '%s final |> p1' % (uid)

        if p2:
            print '%s final -> p2' % (uid)
            p2.stop()
            print '%s final => p2' % (uid)
            p2.join()
            print '%s final |> p2' % (uid)

        if watcher:
            print '%s final -> pwatcher' % (uid)
            watcher.stop()
            print '%s final => pwatcher' % (uid)
            watcher.join()
            print '%s final |> pwatcher' % (uid)
        print '%s final' % (uid)



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

