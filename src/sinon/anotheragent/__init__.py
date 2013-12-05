#!/usr/bin/env python

"""An agent for saga-pilot (sinon).
"""

__author__    = "Ole Weidner"
__email__     = "ole.weidner@rutgers.edu"
__copyright__ = "Copyright 2013, The RADICAL Project at Rutgers"
__license__   = "MIT"

import os
import time
import Queue
import datetime
import threading 
import multiprocessing

# ----------------------------------------------------------------------------
# CONSTANTS
FREE = None
MAX_EXEC_WORKERS = 8

# ----------------------------------------------------------------------------
#
class Task(object):

    # ------------------------------------------------------------------------
    #
    def __init__(self, uid, executable, arguments, workdir, stdout, stderr):

        self._log         = None
        self._description = None

        # static task properties
        self._uid        = uid
        self._executable = executable
        self._arguments  = arguments
        self._workdir    = workdir
        self._stdout     = stdout
        self._stderr     = stdout


        # dynamic task properties
        self._start_time = None
        self._end_time   = None
        self._run_time   = None

        self._state      = None
        self._exit_code  = None
        self._exec_loc   = None

        self._log        = []



    # ------------------------------------------------------------------------
    #
    @property
    def uid(self):
        return self._uid

    # ------------------------------------------------------------------------
    #
    @property
    def executable(self):
        return self._executable

    # ------------------------------------------------------------------------
    #
    @property
    def arguments(self):
        return self._arguments

    # ------------------------------------------------------------------------
    #
    @property
    def workdir(self):
        return self._workdir

    # ------------------------------------------------------------------------
    #
    @property
    def stdout(self):
        return self._stdout

    # ------------------------------------------------------------------------
    #
    @property
    def stderr(self):
        return self._stderr

    # ------------------------------------------------------------------------
    #
    def update_state(self, start_time=None, end_time=None, state=None, 
                     exit_code=None, exec_loc=None):
        """Updates one or more of the task's dynamic properties
        """
        if start_time is None:
            start_time = self._start_time
        else:
            self._start_time = start_time

        if end_time is None:
            end_time = self._end_time
        else:
            self._end_time = end_time

        if state is None:
            state = self._state
        else:
            self._state = state

        if exit_code is None:
            exit_code = self._exit_code
        else:
            self._exit_code = exit_code

        if exec_loc is None:
            exec_loc = self._exec_loc
        else:
            self._exec_loc = self._exec_loc

    # ------------------------------------------------------------------------
    #
    def update_log(self, log):
        """Updates the task logs
        """
        if not isinstance(log, list):
            log = [log]
        self._log.extend(log)


# ----------------------------------------------------------------------------
#
class ExecWorker(multiprocessing.Process):
    """An ExecWorker competes for the execution of tasks in a task queue
    and writes the results back to MongoDB.
    """

    # ------------------------------------------------------------------------
    #
    def __init__(self, mongodb_credentials, task_queue, 
                 hosts, cores_per_host, launch_method):
        """Le Constructeur creates a new ExecWorker instance.
        """
        multiprocessing.Process.__init__(self)
        self.daemon      = True
        self._terminate  = False

        self._mongodb_credentials = mongodb_credentials
        self._task_queue          = task_queue
        
        self._launch_method       = launch_method

        # Slots represents the internal process management structure. The 
        # structure is as follows:
        # {
        #    'host1': [p_1, p_2, p_3, ... , p_cores_per_host],
        #    'host2': [p_1, p_2, p_3. ... , p_cores_per_host]
        # }
        # 
        self._slots = {}
        for host in hosts:
            self._slots[host] = []
            for _ in range(0, cores_per_host):
                self._slots[host].append(FREE)

    # ------------------------------------------------------------------------
    #
    def stop(self):
        """Terminates the process' main loop.
        """
        self._terminate = True

    # ------------------------------------------------------------------------
    #
    def run(self):
        """Starts the process when Process.start() is called.
        """
        while self._terminate is False:
            while True:

                for host, slots in self._slots.iteritems():
                    # we iterate over all slots. if slots are emtpy, we try
                    # to run a new process. if they are occupied, we try to 
                    # update the state 
                    for slot in range(len(slots)):
                        # check if slot is free. if so, launch a new task
                        if self._slots[slot] is FREE:

                            try:
                                task = self._task_queue.get_nowait()

                                # create working directory
                                task_dir_name = "%s/task-%s" % \
                                    (task.working_directory, task.uid)
                                # create working directory in case it
                                # doesn't exist
                                os.makedirs(task_dir_name)

                                # RUN THE TASK
                                self._slots[host][slot] = self._exec(task=task)
                                task.update_state(
                                    start_time=datetime.datetime.now(),
                                    exec_loc=host,
                                    state='RUNNING'
                                )


                            except Queue.Empty:
                                # do nothing if we don't have any queued tasks
                                self._slots[slot] = None

                        else:
                            rc = self._slots[host][slot].poll()
                            if rc is None:
                                # subprocess is still running
                                pass
                            else:
                                self._slots[host][slot].close_and_flush_filehandles()

                                # update database, set task state and notify.
                                if rc != 0:
                                    state = 'FAILED'
                                else:
                                    state = 'DONE'

                                task.update_state(
                                    end_time=datetime.datetime.now(),
                                    exit_code=rc,
                                    state=state
                                )

                                # mark slot as available
                                self._slots[host][slot] = FREE

            time.sleep(1)

    # ------------------------------------------------------------------------
    #
    def _exec(self, task):

        # Assemble command line
        cmdline = str()

        # Based on the launch method we use different, well, launch methods
        # to launch the task. just on the shell, via mpirun, ssh or aprun
        if self._launch_method == "LOCAL":
            pass

        # task executable and arguments
        cmdline += " %s " % task.executable
        if task.arguments is not None:
            for arg in task.arguments:
                cmdline += " %s " % arg

        proc = _Process(args=cmdline,
                        cwd=task.workdir,
                        stdout_file=task.stdout,
                        stderr_file=task.stderr)

        return proc

# ----------------------------------------------------------------------------
#
class Agent(threading.Thread):

    # ------------------------------------------------------------------------
    #
    def __init__(self):
        """Le Constructeur creates a new Agent instance.
        """
        threading.Thread.__init__(self)
        self.daemon      = True
        self.lock        = threading.Lock()
        self._terminate  = threading.Event()

        self._mongodb_credentials = None

        # the task queue holds the tasks that are pulled from the MongoDB 
        # server. The ExecWorkers compete for the tasks in the queue. 
        self._task_queue = multiprocessing.Queue()

        # the exec workers compete for tasks in the task queue. we start up
        # to MAX_EXEC_WORKERS tasks. at some point, this should be come a 
        # configurabale paramter.
        self._exec_workers = []
        for x in range(0, constants.MAX_EXEC_WORKERS):
            exec_worker = ExecWorker(
                task_queue          = self._task_queue,
                mongodb_credentials = self._mongodb_credentials,
                launch_method       = "LOCAL"
            )
            exec_worker.start()
            self._exec_workers.append(exec_worker)

    # ------------------------------------------------------------------------
    #
    def __del__(self):
        """Le destructeur. Deletes the agent instance.
        """

    # ------------------------------------------------------------------------
    #
    def run(self):
        """Starts the thread when Thread.start() is called.
        """
        while not self._terminate.isSet():
            time.sleep(1)













#-----------------------------------------------------------------------------
#
class _Process(subprocess.Popen):

    #-------------------------------------------------------------------------
    #
    def __init__(self, args, stdout_file, stderr_file, bufsize=0, 
                 executable=None, stdin=None, stdout=None, stderr=None, 
                 close_fds=False, shell=False, cwd=None, env=None):

        preexec_fn = None
        universal_newlines = False
        startupinfo = None  # Only relevant on MS Windows
        creationflags = 0   # Only relevant on MS Windows
        shell = True

        self.stdout_filename = stdout_file
        self._stdout_file_h = open(stdout_file, "w")

        self.stderr_filename = stderr_file
        self._stderr_file_h = open(stderr_file, "w")

        super(_Process, self).__init__(args=args,
                                       bufsize=bufsize,
                                       executable=executable,
                                       stdin=stdin,
                                       stdout=self._stdout_file_h,
                                       stderr=self._stderr_file_h,
                                       preexec_fn=preexec_fn,
                                       close_fds=close_fds,
                                       shell=shell,
                                       cwd=cwd,
                                       env=env,
                                       universal_newlines=universal_newlines,
                                       startupinfo=startupinfo,
                                       creationflags=creationflags)

    #-------------------------------------------------------------------------
    #
    def close_and_flush_filehandles(self):
        self._stdout_file_h.flush()
        self._stderr_file_h.flush()
        self._stdout_file_h.close()
        self._stderr_file_h.close()