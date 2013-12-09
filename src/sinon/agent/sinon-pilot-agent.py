#!/usr/bin/env python

"""An agent for saga-pilot (sinon).
"""

__author__    = "Ole Weidner"
__email__     = "ole.weidner@rutgers.edu"
__copyright__ = "Copyright 2013, The RADICAL Project at Rutgers"
__license__   = "MIT"

import os
import sys
import time
import Queue
import logging
import pymongo
import StringIO
import datetime
import hostlist
import optparse
import threading 
import subprocess
import multiprocessing

from bson.objectid import ObjectId

# ----------------------------------------------------------------------------
# CONSTANTS
FREE             = None # just an alias
MAX_EXEC_WORKERS = 8    # max number of worker processes

#-----------------------------------------------------------------------------
#
def which(program):
    """Finds the location of an executable.
    Taken from: http://stackoverflow.com/questions/377017/test-if-executable-exists-in-python
    """
    #-------------------------------------------------------------------------
    #
    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file
    return None

#-----------------------------------------------------------------------------
#
class ExecutionEnvironment(object):
    """DOC
    """
    #-------------------------------------------------------------------------
    #
    @classmethod
    def discover(cls, logger):
        """Factory method creates a new execution environment.
        """
        eenv = cls(logger)
        # detect nodes, cores and memory available
        eenv._detect_nodes()
        eenv._detect_cores_and_memory()

        # check for 'mpirun'
        eenv._mpirun_location = which('mpirun')
        eenv._aprun_location  = which('aprun')
        eenv._ssh_location    = which('ssh')

        # suggest a launch method. the current precendce is 
        # aprun, mpirun, ssh, fork. this can be overrdden 
        # by passing the '--launch-method' parameter to the agent.

        if eenv._aprun_location is not None:
            eenv._launch_method = "APRUN"
        elif eenv._mpirun_location is not None:
            eenv._launch_method = "MPIRUN"
        elif eenv._ssh_location is not None:
            eenv._launch_method = "SSH"
        else:
            eenv._launch_method = "LOCAL"

        # create node dictionary
        for rn in eenv._raw_nodes:
            if rn not in eenv._nodes:
                eenv._nodes[rn] = {#'_count': 1,
                                   'cores': eenv._cores_per_node,
                                   'memory': eenv._memory_per_node}
            #else:
            #    eenv._nodes[rn]['_count'] += 1

        logger.info("Discovered execution environment: %s" % eenv._nodes)

        return eenv

    #-------------------------------------------------------------------------
    #
    def __init__(self, logger=None):
        '''le constructeur
        '''
        self._log = logger

        self._nodes = dict()
        self._raw_nodes = list()
        self._cores_per_node = 0
        self._memory_per_node = 0
        self._launch_method = None

        self._aprun_location  = None
        self._mpirun_location = None
        self._ssh_location    = None

    #-------------------------------------------------------------------------
    #
    @property
    def raw_nodes(self):
        return self._raw_nodes

    #-------------------------------------------------------------------------
    #
    @property
    def nodes(self):
        return self._nodes

    #-------------------------------------------------------------------------
    #
    @property
    def launch_method(self):
        return self._launch_method

    #-------------------------------------------------------------------------
    #
    @property
    def mpirun(self):
        if self._mpirun_location is None:
            return 'mpirun'
        else:
            return self._mpirun_location

    #-------------------------------------------------------------------------
    #
    @property
    def aprun(self): 
        if self._aprun_location is None:
            return 'aprun'
        else:
            return self._aprun_location

    #-------------------------------------------------------------------------
    #
    @property
    def ssh(self):
        if self._ssh_location is None:
            return 'ssh'
        else:
            return self._ssh_location

    #-------------------------------------------------------------------------
    #
    def _detect_cores_and_memory(self):
        self._cores_per_node = multiprocessing.cpu_count() #psutil.NUM_CPUS
        #mem_in_megabyte = int(psutil.virtual_memory().total/1024/1024)
        #self._memory_per_node = mem_in_megabyte

    #-------------------------------------------------------------------------
    #
    def _detect_nodes(self):
        # see if we have a PBS_NODEFILE
        pbs_nodefile = os.environ.get('PBS_NODEFILE')
        slurm_nodelist = os.environ.get('SLURM_NODELIST')

        if pbs_nodefile is not None:
            # parse PBS the nodefile
            self._raw_nodes = [line.strip() for line in open(pbs_nodefile)]
            self._log.info("Found PBS_NODEFILE %s: %s" % (pbs_nodefile, self._raw_nodes))

        elif slurm_nodelist is not None:
            # parse SLURM nodefile
            self._raw_nodes = hostlist.expand_hostlist(slurm_nodelist)
            self._log.info("Found SLURM_NODELIST %s. Expanded to: %s" % (slurm_nodelist, self._raw_nodes))

        else:
            self._raw_nodes = ['localhost']
            self._log.info("No PBS_NODEFILE or SLURM_NODELIST found. Using hosts: %s" % (self._raw_nodes))


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
    @property
    def started(self):
        return self._start_time

    # ------------------------------------------------------------------------
    #
    @property
    def finished(self):
        return self._end_time

    # ------------------------------------------------------------------------
    #
    @property
    def state(self):
        return self._state

    # ------------------------------------------------------------------------
    #
    @property
    def exit_code(self):
        return self._exit_code

    # ------------------------------------------------------------------------
    #
    @property
    def exec_loc(self):
        return self._exec_loc

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
            self._exec_loc = exec_loc

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
    def __init__(self, logger, database_info, task_queue, 
                 hosts, cores_per_host, launch_method):
        """Le Constructeur creates a new ExecWorker instance.
        """
        multiprocessing.Process.__init__(self)
        self.daemon      = True
        self._terminate  = False

        self._log = logger

        self._database_info  = database_info
        self._task_queue     = task_queue
        
        self._launch_method  = launch_method

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

        # Initialize a database connection. Pymongo takes care of connection 
        # pooling, etc. so we don't really have to worry about when and where
        # we open new connections. 
        self._client = pymongo.MongoClient(database_info['url'])
        self._db     = self._client[database_info['dbname']]
        self._w      = self._db["%s.w"  % database_info['session']]

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

            # we iterate over all slots. if slots are emtpy, we try
            # to run a new process. if they are occupied, we try to 
            # update the state             
            for host, slots in self._slots.iteritems():
                
                # we update tasks in 'bulk' after each iteration. 
                # all tasks that require DB updates are in update_tasks
                update_tasks = []

                for slot in range(len(slots)):

                    # check if slot is free. if so, launch a new task
                    if self._slots[host][slot] is FREE:

                        try:
                            task = self._task_queue.get_nowait()

                            # create working directory in case it
                            # doesn't exist
                            os.makedirs(task.workdir)

                            # RUN THE TASK
                            self._slots[host][slot] = _Process(task=task, host=host,
                                launch_method=self._launch_method)
                            self._slots[host][slot].task.update_state(
                                start_time=datetime.datetime.now(),
                                exec_loc=host,
                                state='Running'
                            )
                            update_tasks.append(self._slots[host][slot].task)

                        except Queue.Empty:
                            # do nothing if we don't have any queued tasks
                            self._slots[host][slot] = None

                    else:
                        rc = self._slots[host][slot].poll()
                        if rc is None:
                            # subprocess is still running
                            pass
                        else:
                            self._slots[host][slot].close_and_flush_filehandles()

                            # update database, set task state and notify.
                            if rc != 0:
                                state = 'Failed'
                            else:
                                state = 'Done'

                            self._slots[host][slot].task.update_state(
                                end_time=datetime.datetime.now(),
                                exit_code=rc,
                                state=state
                            )
                            update_tasks.append(self._slots[host][slot].task)

                            # mark slot as available
                            self._slots[host][slot] = FREE

                # update all the tasks that are marked for update.
                self._update_tasks(update_tasks)

                self._log.debug("Slot status:\n%s", self._slot_status(self._slots))

            time.sleep(1)

    # ------------------------------------------------------------------------
    #
    def _slot_status(self, slots):
        """Returns a multiline string corresponding to slot status.
        """
        slot_matrix = ""
        for host, slots in slots.iteritems():
            slot_vector = ""
            for slot in slots:
                if slot is FREE:
                    slot_vector += " - "
                else:
                    slot_vector += " X "
            slot_matrix += "%s: %s\n" % (host.ljust(24), slot_vector)
        return slot_matrix

    # ------------------------------------------------------------------------
    #

    def _update_tasks(self, tasks):
        """Updates the database entries for one or more tasks, inlcuding 
        task state, log, etc.
        """
        for task in tasks:
            self._w.update({"_id": ObjectId(task.uid)}, 
            {"$set": {"info.state"     : task.state,
                      "info.started"   : task.started,
                      "info.finished"  : task.finished,
                      "info.exec_loc"  : task.exec_loc,
                      "info.exit_code" : task.exit_code}})


# ----------------------------------------------------------------------------
#
class Agent(threading.Thread):

    # ------------------------------------------------------------------------
    #
    def __init__(self, logger, exec_env, launch_method, workdir, database_info):
        """Le Constructeur creates a new Agent instance.
        """
        threading.Thread.__init__(self)
        self.daemon      = True
        self.lock        = threading.Lock()
        self._terminate  = threading.Event()

        self._log            = logger

        self._workdir        = workdir
        self._pilot_id       = database_info['pilot']

        # launch method is determined by the execution environment,
        # but can be overridden if the 'launch_method' flag is set 
        if launch_method.lower() == "auto":
            self._launch_method = exec_env.launch_method
        else:
            self._launch_method = launch_method

        # Try to establish a database connection
        self._client = pymongo.MongoClient(database_info['url'])
        self._db     = self._client[database_info['dbname']]
        self._p      = self._db["%s.p"  % database_info['session']]
        self._w      = self._db["%s.w"  % database_info['session']]

        # the task queue holds the tasks that are pulled from the MongoDB 
        # server. The ExecWorkers compete for the tasks in the queue. 
        self._task_queue = multiprocessing.Queue()

        # we devide up the host list into maximum MAX_EXEC_WORKERS host 
        # partitions and assign them to the exec workers. asignment is 
        # round robin
        self._host_partitions = []
        partition_idx = 0
        for host in exec_env.nodes:
            if partition_idx >= MAX_EXEC_WORKERS:
                partition_idx = 0
            if len(self._host_partitions) <= partition_idx:
                self._host_partitions.append([host])
            else:
                self._host_partitions[partition_idx].append(host)
            partition_idx += 1

        # we assign each host partition to a task execution worker
        self._exec_workers = []
        for hp in self._host_partitions:
            exec_worker = ExecWorker(
                logger         = self._log,
                task_queue     = self._task_queue,
                database_info  = database_info,
                launch_method  = self._launch_method,
                hosts          = hp,
                cores_per_host = 8
            )
            exec_worker.start()
            self._log.info("Started up %s serving hosts %s", 
                exec_worker, hp)
            self._exec_workers.append(exec_worker)


    # ------------------------------------------------------------------------
    #
    def stop(self):
        """Terminate the agent main loop.
        """
        # First, we need to shut down all the workers
        for ew in self._exec_workers:
            ew.terminate()

        # Next, we set our own termination signal
        self._terminate.set()

    # ------------------------------------------------------------------------
    #
    def run(self):
        """Starts the thread when Thread.start() is called.
        """
        # first order of business: set the start time and state of the pilot
        self._log.info("Agent started. Database updated.")
        self._p.update(
            {"_id": ObjectId(self._pilot_id)}, 
            {"$set": {"info.state"     : "RUNNING",
                      "info.started"   : datetime.datetime.now()}})

        while not self._terminate.isSet():

            # Check the workers periodically. If they have died, we 
            # exit as well. this can happen, e.g., if the worker 
            # process has caught a ctrl+C
            for ew in self._exec_workers:
                if ew.is_alive() is False:
                    self.stop()

            # try to get new tasks from the database. for this, we check the 
            # wu_queue of the pilot. if there are new entries, we get them,
            # get the actual pilot entries for them and remove them from 
            # the wu_queue. 
            try: 
                p_cursor = self._p.find({"_id": ObjectId(self._pilot_id)})

                # Check if there's a command waiting
                command = p_cursor[0]['command']
                if command is not None:
                    self._log.info("Received new command: %s" % command)
                    if command.lower() == "cancel":
                        # if we receive 'cancel', we terminate the loop.
                        break

                # Check the pilot's workunit queue
                new_wu_ids = p_cursor[0]['wu_queue']

                # There are new work units in the wu_queue on the database.
                # Get the corresponding wu entries
                if len(new_wu_ids) > 0:
                    self._log.info("Found new tasks in pilot queue: %s", new_wu_ids)
                    wu_cursor = self._w.find({"_id": {"$in": new_wu_ids}})
                    for wu in wu_cursor:
                        # Create new task objects and put them into the 
                        # task queue
                        task_dir_name = "%s/task-%s" % (self._workdir, str(wu["_id"]))
                        task = Task(uid=str(wu["_id"]), 
                                    executable=wu["description"]["Executable"], 
                                    arguments=wu["description"]["Arguments"], 
                                    workdir=task_dir_name, 
                                    stdout=task_dir_name+'/STDOUT', 
                                    stderr=task_dir_name+'/STDERR')
                        self._task_queue.put(task)

                    # now we can remove the entries from the pilot's wu_queue
                    # PRINT TODO
                    self._p.update({"_id": ObjectId(self._pilot_id)}, 
                                   {"$pullAll": { "wu_queue": new_wu_ids}})

            except Exception, ex:
                self._log.error("MongoDB error while checking for new work units: %s" % ex)
                break

            time.sleep(1)

        # last order of business: set the start time and state of the pilot
        self._p.update(
            {"_id": ObjectId(self._pilot_id)}, 
            {"$set": {"info.state"     : "DONE",
                      "info.finished"   : datetime.datetime.now()}})
        self._log.info("Agent stopped. Database updated.")



#-----------------------------------------------------------------------------
#
class _Process(subprocess.Popen):

    #-------------------------------------------------------------------------
    #
    def __init__(self, task, host, launch_method):

        self._task = task

        # Assemble command line
        cmdline = str()

        # Based on the launch method we use different, well, launch methods
        # to launch the task. just on the shell, via mpirun, ssh or aprun
        if launch_method.lower() == "local":
            pass

        # task executable and arguments
        cmdline += " %s " % task.executable
        if task.arguments is not None:
            for arg in task.arguments:
                cmdline += " %s " % arg

        self.stdout_filename = task.stdout
        self._stdout_file_h  = open(self.stdout_filename, "w")

        self.stderr_filename = task.stderr
        self._stderr_file_h  = open(self.stderr_filename, "w")

        super(_Process, self).__init__(args=cmdline,
                                       bufsize=0,
                                       executable=None,
                                       stdin=None,
                                       stdout=self._stdout_file_h,
                                       stderr=self._stderr_file_h,
                                       preexec_fn=None,
                                       close_fds=True,
                                       shell=True,
                                       cwd=task.workdir,
                                       env=None,
                                       universal_newlines=False,
                                       startupinfo=None,
                                       creationflags=0)

    #-------------------------------------------------------------------------
    #
    @property
    def task(self):
        """Returns the task object associated with the process.
        """
        return self._task

    #-------------------------------------------------------------------------
    #
    def close_and_flush_filehandles(self):
        self._stdout_file_h.flush()
        self._stderr_file_h.flush()
        self._stdout_file_h.close()
        self._stderr_file_h.close()


#-----------------------------------------------------------------------------
#
def parse_commandline():

    parser = optparse.OptionParser()

    parser.add_option('-d', '--mongodb-url',
                      metavar='URL',
                      dest='mongodb_url',
                      help='Specifies the MongoDB Url.')

    parser.add_option('-n', '--database-name',
                      metavar='URL',
                      dest='database_name',
                      help='Specifies the MongoDB database name.')

    parser.add_option('-s', '--session-id',
                      metavar='SID',
                      dest='session_id',
                      help='Specifies the Session ID.')

    parser.add_option('-p', '--pilot-id',
                      metavar='PID',
                      dest='pilot_id',
                      help='Specifies the Pilot ID.')

    parser.add_option('-w', '--workdir',
                      metavar='DIRECTORY',
                      dest='workdir',
                      help='Specifies the base (working) directory for the agent. [default: %default]',
                      default='.')

    parser.add_option('-m', '--launch-method', 
                      metavar='METHOD',
                      dest='launch_method',
                      help='Enforce a specific launch method (AUTO, FORK, SSH, MPIRUN, APRUN). [default: %default]',
                      default='AUTO')

    # parse the whole shebang
    (options, args) = parser.parse_args()

    if options.mongodb_url is None:
        parser.error("You must define MongoDB URL (-d/--mongodb-url). Try --help for help.")
    elif options.database_name is None:
        parser.error("You must define a database name (-n/--database-name). Try --help for help.")
    elif options.session_id is None:
        parser.error("You must define a session id (-s/--session-id). Try --help for help.")
    elif options.pilot_id is None:
        parser.error("You must define a pilot id (-p/--pilot-id). Try --help for help.")

    if options.launch_method is not None: 
        valid_options = ['AUTO', 'FORK', 'SSH', 'MPIRUN', 'APRUN']
        if options.launch_method.upper() not in valid_options:
            parser.error("--launch-method must be one of these: %s" % valid_options)

    return options

#-----------------------------------------------------------------------------
#
if __name__ == "__main__":

    # parse command line options
    options = parse_commandline()

    # configure the agent logger
    logger = logging.getLogger('sinon.agent')
    logger.setLevel(logging.INFO)
    ch = logging.FileHandler("AGENT.LOG")
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # discover environment, mpirun, cores, etc.
    exec_env = ExecutionEnvironment.discover(logger)

    agent = Agent(logger        = logger,
                  exec_env      = exec_env,
                  workdir       = options.workdir, 
                  launch_method = options.launch_method, 
                  database_info = {
                    'url':     options.mongodb_url, 
                    'dbname':  options.database_name, 
                    'session': options.session_id,
                    'pilot':   options.pilot_id
                })
    try:
        agent.start()
        agent.join()

    except (KeyboardInterrupt, SystemExit):
        print "INTERRUPPTTTTTTTTTTTTTTTTT"
        agent.stop()


