"""
.. module:: sinon.agent.agent
   :platform: Unix
   :synopsis: Provides the task execution mechanism for sinon agent.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import time
import datetime
import Queue
import subprocess
import multiprocessing

from sinon.constants import * 
from sinon.agent.constants import *
from sinon.agent import Task

#-----------------------------------------------------------------------------
#
class _Process(subprocess.Popen):

    #-------------------------------------------------------------------------
    #
    def __init__(self, args, stdout_file, stderr_file, bufsize=0, executable=None, stdin=None, stdout=None,
                 stderr=None, close_fds=False, shell=False, cwd=None, env=None):

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

#-----------------------------------------------------------------------------
#
class _ProcessLauncher(object):
    ''' factory class that launches processes via different methods, e.g.,
        mpirun, ssh, aprun an so on...
    '''
    @classmethod
    def local(cls, nodename, event_callback, logger):

        pl = cls()
        pl.nodename = nodename
        pl.launch_method = LAUNCH_METHOD_LOCAL
        pl.event_callback = event_callback
        pl.logger = logger
        return pl

    @classmethod
    def ssh(cls, nodename, ssh, event_callback, logger):

        pl = cls()
        pl.ssh_executable = ssh
        pl.nodename = nodename
        pl.launch_method = LAUNCH_METHOD_SSH
        pl.event_callback = event_callback
        pl.logger = logger
        return pl

    @classmethod
    def mpirun(cls, nodename, mpirun, event_callback, logger):

        pl = cls()
        pl.mpirun_executable = mpirun
        pl.nodename = nodename
        pl.launch_method = LAUNCH_METHOD_MPIRUN
        pl.event_callback = event_callback
        pl.logger = logger
        return pl

    @classmethod
    def aprun(cls, nodename, aprun, event_callback, logger):

        pl = cls()
        pl.aprun_executable = aprun
        pl.nodename = nodename
        pl.launch_method = LAUNCH_METHOD_APRUN
        pl.event_callback = event_callback
        pl.logger = logger
        return pl

    def __init__(self):
        pass

    #-------------------------------------------------------------------------
    #
    def launch(self, task, workdir, stdout_file, stderr_file):

        cmdline = str()

        if self.launch_method == LAUNCH_METHOD_MPIRUN:
            cmdline =  self.mpirun_executable
            cmdline += " -np %s -host %s" % (str(task.numcores), self.nodename)

        if self.launch_method == LAUNCH_METHOD_APRUN:
            cmdline =  self.aprun_executable
            cmdline += " -n %s " % str(task.numcores)
            
        if self.launch_method == LAUNCH_METHOD_SSH:
            cmdline = self.ssh_executable
            cmdline += " %s " % self.nodename

        elif self.launch_method == LAUNCH_METHOD_LOCAL:
            pass

        # launch through radical process wrapper
        # cmdline += " radical-process-wrapper -- "

        # task executable and arguments
        cmdline += " %s " % task.executable
        if task.arguments is not None:
            for arg in task.arguments:
                cmdline += " %s " % arg

        proc = _Process(args=cmdline,
                        cwd=workdir,
                        stdout_file=stdout_file,
                        stderr_file=stderr_file)

        self.logger.info(message="[Worker for node '%s'] launched task '%s'" % 
                        (self.nodename, cmdline),
                        suffix="exec")

        # tag some info to the process
        proc.cmdline = cmdline
        proc.task_uid = task.uid
        proc.nodename = self.nodename
        proc.launch_method = self.launch_method

        # return the process object
        return proc


#-----------------------------------------------------------------------------
#
class _TaskExecutorWorker(multiprocessing.Process):

    #-------------------------------------------------------------------------
    #
    def __init__(self, working_directory, exec_environment, hostname, cores, 
                 tasks_per_node, task_queue, launch_method, 
                 terminate_on_emtpy_queue=False, 
                 event_callback=None, result_callback=None, logger=None, 
                 ):
        ''' le constructeur
        '''
        self.working_directory = working_directory
        self.exec_environment = exec_environment
        self.hostname = hostname
        self.task_queue = task_queue      # pointer to agent task queue
        self.terminate_on_emtpy_queue = terminate_on_emtpy_queue

        self.log = logger

        # event callback gets called on start stop and failed process execution
        self.event_callback = event_callback

        # result callback gets called once a task has terminated
        self.result_callback = result_callback

        # we override the launch method discovered in the execution 
        # environment if it is set to a value that is not 'AUTO':

        if launch_method.upper() != 'AUTO':

            if launch_method.upper() == 'MPIRUN':
                self.process_launcher = \
                    _ProcessLauncher.mpirun(nodename=hostname,
                                            mpirun=self.exec_environment.mpirun,
                                            event_callback=event_callback,
                                            logger=logger) 

            elif launch_method.upper() == 'APRUN':
                self.process_launcher = \
                    _ProcessLauncher.aprun(nodename=hostname,
                                           aprun=self.exec_environment.aprun,
                                           event_callback=event_callback,
                                           logger=logger) 

            elif launch_method.upper() == 'SSH':
                self.process_launcher = \
                    _ProcessLauncher.ssh  (nodename=hostname,
                                           ssh=self.exec_environment.ssh,
                                           event_callback=self.exec_environment.ssh,
                                           logger=logger)

            elif launch_method.upper() == 'FORK':
                # abort if we use 'FORK' but have more than one host
                if len(self.exec_environment.nodes) > 1:
                    message = "Execution mode is 'FORK', but more than one node detected. "

                    self.log.warning(message=message, suffix="exec")
                else:
                    self.process_launcher = \
                        _ProcessLauncher.local(nodename=hostname,
                                               event_callback=event_callback,
                                               logger=logger)

        else:

            if self.exec_environment.launch_method == LAUNCH_METHOD_MPIRUN:
                self.process_launcher = \
                    _ProcessLauncher.mpirun(nodename=hostname,
                                            mpirun=self.exec_environment.mpirun,
                                            event_callback=event_callback,
                                            logger=logger)

            elif self.exec_environment.launch_method == LAUNCH_METHOD_APRUN:
                self.process_launcher = \
                    _ProcessLauncher.aprun(nodename=hostname,
                                           aprun=self.exec_environment.aprun,
                                           event_callback=event_callback,
                                           logger=logger)

            elif self.exec_environment.launch_method == LAUNCH_METHOD_SSH:
                self.process_launcher = \
                    _ProcessLauncher.ssh  (nodename=hostname,
                                           event_callback=event_callback,
                                           logger=logger)

            elif self.exec_environment.launch_method == LAUNCH_METHOD_LOCAL:
                if len(self.exec_environment.nodes) > 1:
                    message = "Execution mode is 'FORK', but more than one node detected. Aborting. "

                    self.log.info(message=message, suffix="exec")
                    raise Exception(message)
                else:
                    self.process_launcher = \
                        _ProcessLauncher.local(nodename=hostname,
                                               event_callback=event_callback,
                                               logger=logger)
            else:
                raise Exception("No exeuction environment found")

        self.log.info(message="Created new '%s' task execution worker for node '%s'" % 
                     (self.process_launcher.launch_method, hostname),
                      suffix="exec")

        # cores available to this worker. if tasks_per_node is set, 
        # we use it in lieu of cores
        if tasks_per_node is not None:
            self.cores = int(tasks_per_node)
        else:
            self.cores = int(cores)

        # execution 'slots' are initialized with 'None'. Later on they
        # will point to the currently executing subrocesses.
        self.slots = [None]*self.cores

        # init superclass
        super(_TaskExecutorWorker, self).__init__()

    #-------------------------------------------------------------------------
    #
    def terminate(self, drain=False, timeout=0.0):
        if drain is True:
            self.drain_slots(timeout)
            #self.quit_node_monitor()
        super(_TaskExecutorWorker, self).terminate()

    #-------------------------------------------------------------------------
    #
    def drain_slots(self, timeout=0.0):
        ''' wait for all slots to finish processing or until timeout
        '''
        timeout_time = time.time() + timeout
        while True:
            busy_slots = 0
            for slot in range(len(self.slots)):
                if self.slots[slot] is not None:
                    busy_slots += 1
                    rc = self.slots[slot].poll()
                    if rc is not None:
                        # write result to queue notify task queue & reset slot
                        self.task_queue.task_done()

                        end_time = datetime.datetime.now().strftime("%y-%m-%d %X")
                        start_time = time.strptime(self.slots[slot].start_time, "%y-%m-%d %X")
                        runtime = time.mktime(time.strptime(end_time, "%y-%m-%d %X")) - time.mktime(start_time)

                        if self.event_callback is not None:
                            # call the event callback for finished task
                            self.event_callback(
                                origin_type="TASK",
                                origin_id=self.slots[slot].task_uid,
                                event='STATECHANGE',
                                value=DONE)

                        if self.result_callback is not None:
                            # call result callback for finished task
                            self.result_callback(
                                task_id=self.slots[slot].task_uid,
                                cmdline=self.slots[slot].cmdline,
                                exit_code=rc,
                                working_directory=self.slots[slot].workdir,
                                stdout=self.slots[slot].stdout_filename,
                                stderr=self.slots[slot].stderr_filename,
                                start_time=self.slots[slot].start_time,
                                stop_time=end_time,
                                runtime=runtime,
                                execution_locations=[self.slots[slot].nodename])

                        self.slots[slot].close_and_flush_filehandles()
                        self.slots[slot] = None

            # if no slots are busy anymore, we are done
            if busy_slots == 0:
                return
            # timeout has occured. exit the loop
            if (timeout > 0.0) and (time.time() >= timeout_time):
                return
            # sleep a bit before we iterate over slots again
            time.sleep(1)

    #-------------------------------------------------------------------------
    #
    def launch_node_monitor(self):
        ''' Fire up the node monitor process for this executor.
        '''

        self.nodemon_dir_name = "%s/nodemon-%s" % (self.working_directory, self.hostname)
        # create working directory in case it doesn't exist
        os.makedirs(self.nodemon_dir_name)

        nodemon_task = Task(
            uid='nodemon-%s' % self.hostname,
            executable='radical-node-monitor',
            arguments=['--workdir=%s' % self.nodemon_dir_name],
            numcores='1',
            stdout=None,
            stderr=None)

        self.nodemon = self.process_launcher.launch(
            task=nodemon_task,
            workdir=self.nodemon_dir_name,
            stdout_file="%s/STDOUT" % self.nodemon_dir_name,
            stderr_file="%s/STDERR" % self.nodemon_dir_name)

    #-------------------------------------------------------------------------
    #
    def quit_node_monitor(self):
        ''' Stop the node monitor.
        '''
        if self.log is not None:
            self.log.info(
                message="Terminated node monitor on node '%s'" % 
                    (self.hostname),
                suffix="exec")

        with open('%s/TERMINATE' % self.nodemon_dir_name, 'w') as outfile:
            outfile.write('TERMINATE\n')

        self.nodemon.terminate()

    #-------------------------------------------------------------------------
    #
    def run(self):
        ''' the main run loop. pulls tasks from the queues and executes them
        '''

        # First order of business: we fire up the node monitor. The node
        # monitor gathers resource and process data on the node this
        # executor is responsible for.
        # self.launch_node_monitor()

        while True:
            # terminate if no more items are in the queue and
            # terminate_on_emtpy_queue is set to 'True'
            if self.task_queue.empty() and \
               self.terminate_on_emtpy_queue is True:
                # queue is empty - drain remaining jobs and quit
                self.drain_slots()
                #self.quit_node_monitor()
                return

            # iterate over slots
            for slot in range(len(self.slots)):
                # populate slot if empty: start a new subprocess
                if self.slots[slot] is None:
                    # try to pull a task from the queue
                    try:
                        ######################
                        ## EXECUTE THE TASK ##
                        ######################
                        task = self.task_queue.get_nowait()

                        # create working directory
                        task_dir_name = "%s/task-%s" % (self.working_directory, task.uid)
                        # create working directory in case it doesn't exist
                        os.makedirs(task_dir_name)

                        self.slots[slot] = self.process_launcher.launch(
                            task=task,
                            workdir=task_dir_name,
                            stdout_file="%s/STDOUT" % task_dir_name,
                            stderr_file="%s/STDERR" % task_dir_name)

                        self.slots[slot].workdir = task_dir_name
                        self.slots[slot].start_time = datetime.datetime.now().strftime("%y-%m-%d %X")


                        if self.event_callback is not None:
                            self.event_callback(origin_type='TASK',
                                                origin_id=task.uid,
                                                event='ASSIGNMENT',
                                                value='%s' % self.slots[slot].nodename)

                            self.event_callback(origin_type='TASK',
                                                origin_id=task.uid,
                                                event='STATECHANGE',
                                                value=RUNNING)

                    except Queue.Empty:
                        # do nothing if we don't have any queued tasks
                        self.slots[slot] = None
                else:
                    rc = self.slots[slot].poll()
                    if rc is None:
                        # subprocess is still running
                        pass
                    else:
                        # write result to queue notify task queue & reset slot
                        self.task_queue.task_done()

                        end_time = datetime.datetime.now().strftime("%y-%m-%d %X")
                        start_time = time.strptime(self.slots[slot].start_time, "%y-%m-%d %X")
                        runtime = time.mktime(time.strptime(end_time, "%y-%m-%d %X")) - time.mktime(start_time)

                        if self.event_callback is not None:
                            self.event_callback(origin_type='TASK',
                                                origin_id=self.slots[slot].task_uid,
                                                event='STATECHANGE',
                                                value=DONE)

                        if self.result_callback is not None:
                            # call result callback for finished task
                            self.result_callback(
                                task_id=self.slots[slot].task_uid,
                                cmdline=self.slots[slot].cmdline,
                                exit_code=rc,
                                working_directory=self.slots[slot].workdir,
                                stdout=self.slots[slot].stdout_filename,
                                stderr=self.slots[slot].stderr_filename,
                                start_time=self.slots[slot].start_time,
                                stop_time=end_time,
                                runtime=runtime,
                                execution_locations=[self.slots[slot].nodename])

                        self.slots[slot].close_and_flush_filehandles()
                        self.slots[slot] = None

            # sleep a bit before we iterate over slots again
            time.sleep(1)


#-----------------------------------------------------------------------------
#
class TaskExecutor(object):

    #-------------------------------------------------------------------------
    #
    def __init__(self, working_directory, execution_environment, tasks_per_node,
                 launch_method, task_queue, terminate_on_emtpy_queue=False,
                 event_callback=None, result_callback=None, logger=None):

        self._event_callback = event_callback
        self._working_directory = working_directory
        self._execution_environment = execution_environment
        self._task_queue = task_queue
        self._workers = list()

        self.log = logger

        # create working directory in case it doesn't exist
        if not os.path.exists(self._working_directory):
            os.makedirs(self._working_directory)

        # we create one worker process per node. at some point we
        # might have to / want to change this
        for node in self._execution_environment.nodes:
            cores = self._execution_environment.nodes[node]['cores']
            worker = _TaskExecutorWorker(
                task_queue=self._task_queue,
                working_directory=self._working_directory,
                exec_environment=self._execution_environment,
                terminate_on_emtpy_queue=terminate_on_emtpy_queue,
                hostname=node,
                cores=cores,
                tasks_per_node=tasks_per_node,
                launch_method=launch_method,
                event_callback=self._event_callback,
                result_callback=result_callback,
                logger=self.log)
            self._workers.append(worker)

    #-------------------------------------------------------------------------
    #
    def join(self):
        ''' wait for all workers to finish
        '''
        for worker in self._workers:
            worker.join()

    #-------------------------------------------------------------------------
    #
    def terminate(self, drain=True):
        ''' terminate all workers
        '''
        for worker in self._workers:
            worker.terminate()

    #-------------------------------------------------------------------------
    #
    @property
    def num_workers(self):
        return len(self._workers)

    #-------------------------------------------------------------------------
    #
    def start(self):
        for worker in self._workers:
            worker.start()

            if self.log is not None:
                self.log.info(
                    message="Started task execution worker for node '%s': %s" % 
                        (worker.hostname, worker),
                    suffix="exec")
