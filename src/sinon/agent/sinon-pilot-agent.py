#!/usr/bin/env python

"""An agent for SAGA-Pilot.
"""

__author__    = "Ole Weidner"
__email__     = "ole.weidner@rutgers.edu"
__copyright__ = "Copyright 2013-2014, The RADICAL Project at Rutgers"
__license__   = "MIT"

import os
import sys
import time
import errno
import Queue
import signal
import logging
import pymongo
import datetime
import hostlist
import optparse
import threading 
import subprocess
import multiprocessing

from bson.objectid import ObjectId


# ----------------------------------------------------------------------------
# CONSTANTS
FREE                 = None # just an alias
MAX_EXEC_WORKERS     = 8    # max number of worker processes

LAUNCH_METHOD_SSH    = 'SSH'
LAUNCH_METHOD_AUTO   = 'AUTO'
LAUNCH_METHOD_APRUN  = 'APRUN'
LAUNCH_METHOD_LOCAL  = 'LOCAL'
LAUNCH_METHOD_MPIRUN = 'MPIRUN'

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

#---------------------------------------------------------------------------
#
def pilot_FAILED(mongo_p, pilot_uid, message):
    """Updates the state of one or more pilots.
    """
    mongo_p.update({"_id": ObjectId(pilot_uid)},
        {"$push": {"info.log" : message}})
                  
    mongo_p.update({"_id": ObjectId(pilot_uid)}, 
        {"$set": {"info.state" : 'Failed'}})

#---------------------------------------------------------------------------
#
def pilot_CANCELED(mongo_p, pilot_uid, message):
    """Updates the state of one or more pilots.
    """
    mongo_p.update({"_id": ObjectId(pilot_uid)},
        {"$push": {"info.log" : message}})
                  
    mongo_p.update({"_id": ObjectId(pilot_uid)}, 
        {"$set": {"info.state" : 'Canceled'}})

#---------------------------------------------------------------------------
#
def pilot_DONE(mongo_p, pilot_uid):
    """Updates the state of one or more pilots.
    """
    mongo_p.update({"_id": ObjectId(pilot_uid)}, 
        {"$set": {"info.state" : 'Done'}})

#-----------------------------------------------------------------------------
#
class ExecutionEnvironment(object):
    """DOC
    """
    #-------------------------------------------------------------------------
    #
    @classmethod
    def discover(cls, logger, launch_method):
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

        if launch_method == LAUNCH_METHOD_AUTO:
            # Try to autodetect launch method
            if eenv._aprun_location is not None:
                eenv._launch_method = LAUNCH_METHOD_APRUN
                eenv._launch_command = eenv._aprun_location
            elif eenv._mpirun_location is not None:
                eenv._launch_method = LAUNCH_METHOD_MPIRUN
                eenv._launch_command = eenv._mpirun_location
            elif eenv._ssh_location is not None:
                eenv._launch_method = LAUNCH_METHOD_SSH
                eenv._launch_command = eenv._ssh_location
            else:
                eenv._launch_method = LAUNCH_METHOD_LOCAL
                eenv._launch_command = None

        elif launch_method == LAUNCH_METHOD_SSH:
            if eenv._ssh_location is None:
                logger.error("Launch method set to %s but 'ssh' not found in path." % launch_method)
                return None
            else:
                eenv._launch_method = LAUNCH_METHOD_SSH
                eenv._launch_command = eenv._ssh_location   

        elif launch_method == LAUNCH_METHOD_MPIRUN:
            if eenv._mpirun_location is None:
                logger.error("Launch method set to %s but 'mpirun' not found in path." % launch_method)
                return None
            else:
                eenv._launch_method = LAUNCH_METHOD_MPIRUN
                eenv._launch_command = eenv._mpirun_location       

        elif launch_method == LAUNCH_METHOD_APRUN:
            if eenv._aprun_location is None:
                logger.error("Launch method set to %s but 'aprun' not found in path." % launch_method)
                return None
            else:
                eenv._launch_method = LAUNCH_METHOD_APRUN
                eenv._launch_command = eenv._aprun_location      

        elif launch_method == LAUNCH_METHOD_LOCAL:
                eenv._launch_method = LAUNCH_METHOD_LOCAL
                eenv._launch_command = None

        # create node dictionary
        for rn in eenv._raw_nodes:
            if rn not in eenv._nodes:
                eenv._nodes[rn] = {#'_count': 1,
                                   'cores': eenv._cores_per_node,
                                   'memory': eenv._memory_per_node}
            #else:
            #    eenv._nodes[rn]['_count'] += 1

        logger.info("Discovered execution environment: %s" % eenv._nodes)
        logger.info("Dicsovered launch method: %s (%s)" % (eenv._launch_method, eenv._launch_command))

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

        self._launch_method  = None
        self._launch_command = None

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
    def cores_per_node(self):
        return self._cores_per_node

    #-------------------------------------------------------------------------
    #
    @property
    def launch_method(self):
        return self._launch_method

    #-------------------------------------------------------------------------
    #
    @property
    def launch_command(self):
        return self._launch_command

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
        self._uid            = uid
        self._executable     = executable
        self._arguments      = arguments
        self._workdir        = workdir
        self._stdout         = stdout
        self._stderr         = stdout
        self._numcores       = 1


        # dynamic task properties
        self._start_time     = None
        self._end_time       = None

        self._state          = None
        self._exit_code      = None
        self._exec_locs      = None

        self._log            = []

    # ------------------------------------------------------------------------
    #
    @property
    def uid(self):
        return self._uid

    # ------------------------------------------------------------------------
    #
    @property
    def numcores(self):
        return self._numcores

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
    def exec_locs(self):
        return self._exec_locs

    # ------------------------------------------------------------------------
    #
    def update_state(self, start_time=None, end_time=None, state=None, 
                     exit_code=None, exec_locs=None):
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

        if exec_locs is None:
            exec_locs = self._exec_locs
        else:
            self._exec_locs = exec_locs

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
    def __init__(self, logger, mongo_w, task_queue, 
                 hosts, cores_per_host, launch_method, launch_command):
        """Le Constructeur creates a new ExecWorker instance.
        """
        multiprocessing.Process.__init__(self)
        self.daemon      = True
        self._terminate  = False

        self._log = logger
        self._w   = mongo_w
        
        self._task_queue     = task_queue
        
        self._launch_method  = launch_method
        self._launch_command = launch_command

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
                            try :
                                os.makedirs(task.workdir)
                            except OSError as e :
                                # ignore failure on existing directory
                                if  e.errno == errno.EEXIST and os.path.isdir (task.workdir) :
                                    pass
                                else : 
                                    raise

                            # RUN THE TASK
                            self._slots[host][slot] = _Process(
                                task=task, 
                                host=host,
                                launch_method=self._launch_method,
                                launch_command=self._launch_command,
                                logger=self._log)

                            exec_locs = ["%s:%s" % (host, slot)]

                            self._slots[host][slot].task.update_state(
                                start_time=datetime.datetime.utcnow(),
                                exec_locs = exec_locs ,
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
                                end_time=datetime.datetime.utcnow(),
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
            {"$set": {"info.state"         : task.state,
                      "info.started"       : task.started,
                      "info.finished"      : task.finished,
                      "info.exec_locs"     : task.exec_locs,
                      "info.exit_code"     : task.exit_code}})


# ----------------------------------------------------------------------------
#
class Agent(threading.Thread):

    # ------------------------------------------------------------------------
    #
    def __init__(self, logger, exec_env, launch_method, workdir, 
        pilot_id, pilot_collection, workunit_collection):
        """Le Constructeur creates a new Agent instance.
        """
        threading.Thread.__init__(self)
        self.daemon      = True
        self.lock        = threading.Lock()
        self._terminate  = threading.Event()

        self._log        = logger

        self._workdir    = workdir
        self._pilot_id   = pilot_id

        self._exec_env   = exec_env

        # launch method is determined by the execution environment,
        # but can be overridden if the 'launch_method' flag is set 
        if launch_method.lower() == "auto":
            self._launch_method = exec_env.launch_method
        else:
            self._launch_method = launch_method

        self._p      = pilot_collection
        self._w      = workunit_collection

        # the task queue holds the tasks that are pulled from the MongoDB 
        # server. The ExecWorkers compete for the tasks in the queue. 
        self._task_queue = multiprocessing.Queue()

        # we devide up the host list into maximum MAX_EXEC_WORKERS host 
        # partitions and assign them to the exec workers. asignment is 
        # round robin
        self._host_partitions = []
        partition_idx = 0
        for host in self._exec_env.nodes:
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
                mongo_w        = self._w,
                launch_method  = self._exec_env.launch_method,
                launch_command = self._exec_env.launch_command,
                hosts          = hp,
                cores_per_host = self._exec_env.cores_per_node
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
            {"$set": {"info.state"          : "RUNNING",
                      "info.nodes"          : self._exec_env.nodes.keys(),
                      "info.cores_per_node" : self._exec_env.cores_per_node,
                      "info.started"        : datetime.datetime.utcnow()}})

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

                        # WorkingDirectoryPriv is defined, we override the 
                        # standard working directory schema. 
                        # NOTE: this is not a good idea and just implemented
                        #       to support some last minute TROY experiments.
                        if wu["description"]["WorkingDirectoryPriv"] is not None:
                            task_dir_name = wu["description"]["WorkingDirectoryPriv"]
                        else:
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
                      "info.finished"   : datetime.datetime.utcnow()}})
        self._log.info("Agent stopped. Database updated.")

#-----------------------------------------------------------------------------
#
class _Process(subprocess.Popen):

    #-------------------------------------------------------------------------
    #
    def __init__(self, task, host, launch_method, launch_command, logger):

        self._task = task
        self._log  = logger

        # Assemble command line
        cmdline = str()

        # Based on the launch method we use different, well, launch methods
        # to launch the task. just on the shell, via mpirun, ssh or aprun
        if launch_method == LAUNCH_METHOD_LOCAL:
            pass

        if launch_method == LAUNCH_METHOD_MPIRUN:
            cmdline =  launch_command
            cmdline += " -np %s -host %s" % (str(task.numcores), host)

        elif launch_method == launch_command:
            cmdline =  launch_command
            cmdline += " -n %s " % str(task.numcores)
            
        elif launch_method == LAUNCH_METHOD_SSH:
            cmdline = launch_command
            cmdline += " %s " % host

        # task executable and arguments
        cmdline += " %s " % task.executable
        if task.arguments is not None:
            for arg in task.arguments:
                cmdline += " %s " % arg

        self.stdout_filename = task.stdout
        self._stdout_file_h  = open(self.stdout_filename, "w")

        self.stderr_filename = task.stderr
        self._stderr_file_h  = open(self.stderr_filename, "w")

        self._log.info("Launching task %s via %s" % (task.uid, cmdline))

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

    parser.add_option('-l', '--launch-method', 
                      metavar='METHOD',
                      dest='launch_method',
                      help='Enforce a specific launch method (AUTO, LOCAL, SSH, MPIRUN, APRUN). [default: %default]',
                      default=LAUNCH_METHOD_AUTO)

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
        valid_options = [LAUNCH_METHOD_AUTO, LAUNCH_METHOD_LOCAL, LAUNCH_METHOD_SSH, LAUNCH_METHOD_MPIRUN, LAUNCH_METHOD_APRUN]
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

    #--------------------------------------------------------------------------
    # Establish database connection
    try:
        mongo_client = pymongo.MongoClient(options.mongodb_url)
        mongo_db     = mongo_client[options.database_name]
        mongo_p      = mongo_db["%s.p"  % options.session_id]
        mongo_w      = mongo_db["%s.w"  % options.session_id]

    except Exception, ex:
        logger.error("Can't establish database connection: %s" % str(ex))
        sys.exit(1)

    #--------------------------------------------------------------------------
    # Some singal handling magic 
    def sigint_handler(signal, frame):
        msg = 'Caught SIGINT. EXITING.'
        pilot_CANCELED(mongo_p, options.pilot_id, msg)
        logger.warning(msg)
        sys.exit(0)
    signal.signal(signal.SIGINT, sigint_handler)

    def sigalarm_handler(signal, frame):
        msg = 'Caught SIGALRM (Walltime limit reached?). EXITING'
        pilot_CANCELED(mongo_p, options.pilot_id, msg)
        logger.warning(msg)
        sys.exit(0)
    signal.signal(signal.SIGALRM, sigalarm_handler)

    #def sigkill_handler(signal, frame):
    #    msg = 'Caught SIGKILL. EXITING'
    #    pilot_CANCELED(mongo_p, options.pilot_id, msg)
    #    logger.warning(msg)
    #    sys.exit(0)
    #signal.signal(signal.SIGKILL, sigkill_handler)

    def sigterm_handler(signal, frame):
        msg = 'Caught SIGTERM. EXITING'
        pilot_CANCELED(mongo_p, options.pilot_id, msg)
        logger.warning(msg)
        sys.exit(0)
    signal.signal(signal.SIGTERM, sigterm_handler)

    #--------------------------------------------------------------------------
    # Discover environment, mpirun, cores, etc.
    try:
        exec_env = ExecutionEnvironment.discover(
            logger=logger,
            launch_method=options.launch_method
        )
        if exec_env is None:
            msg = "Couldn't set up execution environment."
            logger.error(msg)
            pilot_FAILED(mongo_p, options.pilot_id, msg)
            sys.exit(1)

    except Exception, ex:
        msg = "Error setting up execution environment: %s" % str(ex)
        logger.error(msg)
        pilot_FAILED(mongo_p, options.pilot_id, msg)
        sys.exit(1)

    #--------------------------------------------------------------------------
    # Launch the agent thread
    try:
        agent = Agent(logger              = logger,
                      exec_env            = exec_env,
                      workdir             = options.workdir, 
                      launch_method       = options.launch_method, 
                      pilot_id            = options.pilot_id,
                      pilot_collection    = mongo_p,
                      workunit_collection = mongo_w
        )

        agent.start()
        agent.join()

    except Exception, ex:
        msg = "Error running agent: %s" % str(ex)
        logger.error(msg)
        pilot_FAILED(mongo_p, options.pilot_id, msg)
        agent.stop()
        sys.exit(1)

    except SystemExit:

        logger.error("Caught keyboard interrupt. EXITING")
        agent.stop()

