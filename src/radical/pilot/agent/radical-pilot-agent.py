#!/usr/bin/env python

"""
.. module:: radical.pilot.agent
   :platform: Unix
   :synopsis: An agent for RADICAL-Pilot.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import ast
import sys
import time
import errno
import pipes
import Queue
import signal
import gridfs
import pymongo
import optparse
import logging
import datetime
import hostlist
import traceback
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
def pilot_FAILED(mongo_p, pilot_uid, logger, message):
    """Updates the state of one or more pilots.
    """
    logger.error(message)      
    ts = datetime.datetime.utcnow()

    mongo_p.update({"_id": ObjectId(pilot_uid)}, 
        {"$push": {"log" : message,
                   "statehistory": {"state": 'Failed', "timestamp": ts}},
         "$set":  {"state": 'Failed',
                   "finished": ts}

        })

#---------------------------------------------------------------------------
#
def pilot_CANCELED(mongo_p, pilot_uid, logger, message):
    """Updates the state of one or more pilots.
    """
    logger.warning(message)
    ts = datetime.datetime.utcnow()

    mongo_p.update({"_id": ObjectId(pilot_uid)}, 
        {"$push": {"log" : message,
                   "statehistory": {"state": 'Canceled', "timestamp": ts}},
         "$set":  {"state": 'Canceled',
                   "finished": ts}
        })

#---------------------------------------------------------------------------
#
def pilot_DONE(mongo_p, pilot_uid):
    """Updates the state of one or more pilots.
    """
    ts = datetime.datetime.utcnow()

    mongo_p.update({"_id": ObjectId(pilot_uid)}, 
        {"$push": {"statehistory": {"state": 'Done', "timestamp": ts}},
         "$set": {"state": 'Done',
                  "finished": ts}

        })

#---------------------------------------------------------------------------
#
def pilot_PENDING_OUTPUT_TRANSFER(mongo_p, pilot_uid):
    """Updates the state of one or more pilots.
    """
    mongo_p.update({"_id": ObjectId(pilot_uid)}, 
        {"$set": {"state": 'PendingOutputTransfer',
                  "finished": datetime.datetime.utcnow()
        }})


#-----------------------------------------------------------------------------
#
class ExecutionEnvironment(object):
    """DOC
    """
    #-------------------------------------------------------------------------
    #
    @classmethod
    def discover(cls, logger, launch_method, requested_cores):
        """Factory method creates a new execution environment.
        """
        eenv = cls(logger)
        # detect nodes, cores and memory available
        eenv.detect_nodes()
        eenv.detect_cores_and_memory()

        # check for 'mpirun'
        eenv.mpirun_location = which('mpirun')
        eenv.aprun_location  = which('aprun')
        eenv.ssh_location    = which('ssh')

        # suggest a launch method. the current precendce is 
        # aprun, mpirun, ssh, fork. this can be overrdden 
        # by passing the '--launch-method' parameter to the agent.

        if launch_method == LAUNCH_METHOD_AUTO:
            # Try to autodetect launch method
            if eenv.aprun_location is not None:
                eenv.launch_method = LAUNCH_METHOD_APRUN
                eenv.launch_command = eenv.aprun_location
            elif eenv.mpirun_location is not None:
                eenv.launch_method = LAUNCH_METHOD_MPIRUN
                eenv.launch_command = eenv.mpirun_location
            elif eenv.ssh_location is not None:
                eenv.launch_method = LAUNCH_METHOD_SSH
                eenv.launch_command = eenv.ssh_location
            else:
                eenv.launch_method = LAUNCH_METHOD_LOCAL
                eenv.launch_command = None

        elif launch_method == LAUNCH_METHOD_SSH:
            if eenv.ssh_location is None:
                raise Exception("Launch method set to %s but 'ssh' not found in path." % launch_method)
            else:
                eenv.launch_method = LAUNCH_METHOD_SSH
                eenv.launch_command = eenv.ssh_location   

        elif launch_method == LAUNCH_METHOD_MPIRUN:
            if eenv.mpirun_location is None:
                raise Exception("Launch method set to %s but 'mpirun' not found in path." % launch_method)
            else:
                eenv.launch_method = LAUNCH_METHOD_MPIRUN
                eenv.launch_command = eenv.mpirun_location       

        elif launch_method == LAUNCH_METHOD_APRUN:
            if eenv.aprun_location is None:
                raise Exception("Launch method set to %s but 'aprun' not found in path." % launch_method)
            else:
                eenv.launch_method = LAUNCH_METHOD_APRUN
                eenv.launch_command = eenv.aprun_location      

        elif launch_method == LAUNCH_METHOD_LOCAL:
            eenv.launch_method = LAUNCH_METHOD_LOCAL
            eenv.launch_command = None

        # create node dictionary
        for rn in eenv.raw_nodes:
            if rn not in eenv.nodes:
                eenv.nodes[rn] = {#'_count': 1,
                                   'cores': eenv.cores_per_node,
                                   'memory': eenv.memory_per_node}
            #else:
            #    eenv.nodes[rn]['_count'] += 1

        logger.info("Discovered execution environment: %s" % eenv.nodes)
        logger.info("Discovered launch method: %s (%s)" % (eenv.launch_method, eenv.launch_command))

        cores_avail = len(eenv.nodes) * int(eenv.cores_per_node)
        if cores_avail < int(requested_cores):
            raise Exception("Not enought cores available (%s) to satisfy allocation request (%s)." % (str(cores_avail), str(requested_cores)))

        if launch_method == LAUNCH_METHOD_LOCAL:
            # make sure that we don't hog all cores with a local
            # pilot but only the number of cores that were allocated 
            eenv.cores_per_node = int(requested_cores)

        return eenv

    #-------------------------------------------------------------------------
    #
    def __init__(self, logger=None):
        '''le constructeur
        '''
        self.log = logger

        self.nodes = dict()
        self.raw_nodes = list()
        self.cores_per_node = 0
        self.memory_per_node = 0

        self.launch_method  = None
        self.launch_command = None

        self.aprun  = 'aprun'
        self.mpirun = 'mpirun'
        self.ssh    = 'ssh'

    #-------------------------------------------------------------------------
    #
    def detect_cores_and_memory(self):
        self.cores_per_node = multiprocessing.cpu_count() #psutil.NUM_CPUS
        #mem_in_megabyte = int(psutil.virtual_memory().total/1024/1024)
        #self._memory_per_node = mem_in_megabyte

    #-------------------------------------------------------------------------
    #
    def detect_nodes(self):
        # see if we have a PBS_NODEFILE
        pbs_nodefile = os.environ.get('PBS_NODEFILE')
        slurm_nodelist = os.environ.get('SLURM_NODELIST')

        if pbs_nodefile is not None:
            # parse PBS the nodefile
            self.raw_nodes = [line.strip() for line in open(pbs_nodefile)]
            self.log.info("Found PBS_NODEFILE %s: %s" % (pbs_nodefile, self.raw_nodes))

        elif slurm_nodelist is not None:
            # parse SLURM nodefile
            self.raw_nodes = hostlist.expand_hostlist(slurm_nodelist)
            self.log.info("Found SLURM_NODELIST %s. Expanded to: %s" % (slurm_nodelist, self.raw_nodes))

        else:
            self.raw_nodes = ['localhost']
            self.log.info("No PBS_NODEFILE or SLURM_NODELIST found. Using hosts: %s" % (self.raw_nodes))

# ----------------------------------------------------------------------------
#
class Task(object):

    # ------------------------------------------------------------------------
    #
    def __init__(self, uid, executable, arguments, environment, workdir, stdout, stderr, output_data):

        self._log         = None
        self._description = None

        # static task properties
        self.uid            = uid
        self.environment    = environment
        self.executable     = executable
        self.arguments      = arguments
        self.workdir        = workdir
        self.stdout         = stdout
        self.stderr         = stderr
        self.output_data    = output_data
        self.numcores       = 1

        # dynamic task properties
        self.started        = None
        self.finished       = None

        self.state          = None
        self.exit_code      = None
        self.exec_locs      = None

        self.stdout_id      = None
        self.stderr_id      = None

        self._log            = []


# ----------------------------------------------------------------------------
#
class ExecWorker(multiprocessing.Process):
    """An ExecWorker competes for the execution of tasks in a task queue
    and writes the results back to MongoDB.
    """

    # ------------------------------------------------------------------------
    #
    def __init__(self, logger, task_queue, hosts, cores_per_host, 
                 launch_method, launch_command, mongodb_url, mongodb_name,
                 pilot_id, session_id, unitmanager_id):

        """Le Constructeur creates a new ExecWorker instance.
        """
        multiprocessing.Process.__init__(self)
        self.daemon      = True
        self._terminate  = False

        self._log = logger

        self._unitmanager_id = None
        self._pilot_id = pilot_id

        mongo_client = pymongo.MongoClient(mongodb_url)
        self._mongo_db = mongo_client[mongodb_name]
        self._p = mongo_db["%s.p"  % session_id]
        self._w = mongo_db["%s.w"  % session_id]
        self._wm = mongo_db["%s.wm" % session_id]

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
        try:
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

                                self._slots[host][slot].task.started=datetime.datetime.utcnow()
                                self._slots[host][slot].task.exec_locs=exec_locs
                                self._slots[host][slot].task.state='Running'

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
                                uid = self._slots[host][slot].task.uid
                                self._log.info("Task %s terminated with return code %s." % (uid, rc))

                                if rc != 0:
                                    state = 'Failed'
                                else:
                                    if self._slots[host][slot].task.output_data is not None:
                                        state = 'PendingOutputTransfer'
                                    else:
                                        state = 'Done'

                                # upload stdout and stderr to GridFS
                                workdir = self._slots[host][slot].task.workdir
                                task_id = self._slots[host][slot].task.uid

                                stdout_id = None
                                stderr_id = None

                                stdout = "%s/STDOUT" % workdir
                                if os.path.isfile(stdout):
                                    fs = gridfs.GridFS(self._mongo_db)
                                    with open(stdout, 'r') as stdout_f:
                                        stdout_id = fs.put(stdout_f.read(), filename=stdout)
                                        self._log.info("Uploaded %s to MongoDB as %s." % (stdout, str(stdout_id)))

                                stderr = "%s/STDERR" % workdir
                                if os.path.isfile(stderr):
                                    fs = gridfs.GridFS(self._mongo_db)
                                    with open(stderr, 'r') as stderr_f:
                                        stderr_id = fs.put(stderr_f.read(), filename=stderr)
                                        self._log.info("Uploaded %s to MongoDB as %s." % (stderr, str(stderr_id)))

                                self._slots[host][slot].task.finished=datetime.datetime.utcnow()
                                self._slots[host][slot].task.exit_code=rc
                                self._slots[host][slot].task.state=state
                                self._slots[host][slot].task.stdout_id=stdout_id
                                self._slots[host][slot].task.stderr_id=stderr_id

                                update_tasks.append(self._slots[host][slot].task)

                                # mark slot as available
                                self._slots[host][slot] = FREE

                    # update all the tasks that are marked for update.
                    self._update_tasks(update_tasks)
                    self._log.debug("Slot status:\n%s", self._slot_status(self._slots))

                time.sleep(1)

        except Exception, ex:
            self._log.error("Error in ExecWorker loop: %s", traceback.format_exc())
            raise


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
        ts = datetime.datetime.utcnow()
        # We need to know which unit manager we are working with. We can pull
        # this informattion here:

        if self._unitmanager_id is None:
            cursor_p = self._p.find({"_id": ObjectId(self._pilot_id)},
                                    {"unitmanager": 1})
            self._unitmanager_id = cursor_p[0]["unitmanager"]

        for task in tasks:
            self._w.update({"_id": ObjectId(task.uid)}, 
            {"$set": {"state"         : task.state,
                      "started"       : task.started,
                      "finished"      : task.finished,
                      "exec_locs"     : task.exec_locs,
                      "exit_code"     : task.exit_code,
                      "stdout_id"     : task.stdout_id,
                      "stderr_id"     : task.stderr_id},
             "$push": {"statehistory": {"state": task.state, "timestamp": ts}}

                      })


# ----------------------------------------------------------------------------
#
class Agent(threading.Thread):

    # ------------------------------------------------------------------------
    #
    def __init__(self, logger, exec_env, workdir, runtime, launch_method, 
                 mongodb_url, mongodb_name, pilot_id, session_id, unitmanager_id):
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

        self._runtime    = runtime
        self._starttime  = None

        mongo_client = pymongo.MongoClient(mongodb_url)
        mongo_db = mongo_client[mongodb_name]
        self._p = mongo_db["%s.p"  % session_id]
        self._w = mongo_db["%s.w"  % session_id]
        self._wm = mongo_db["%s.wm" % session_id]

        # launch method is determined by the execution environment,
        # but can be overridden if the 'launch_method' flag is set 
        if launch_method.lower() == "auto":
            self._launch_method = exec_env.launch_method
        else:
            self._launch_method = launch_method

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
                logger          = self._log,
                task_queue      = self._task_queue,
                hosts           = hp,
                cores_per_host  = self._exec_env.cores_per_node,
                launch_method   = self._exec_env.launch_method,
                launch_command  = self._exec_env.launch_command,
                mongodb_url     = mongodb_url,
                mongodb_name    = mongodb_name,
                pilot_id        = pilot_id,
                session_id      = session_id,
                unitmanager_id = unitmanager_id
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
        ts = datetime.datetime.utcnow()
        self._p.update(
            {"_id": ObjectId(self._pilot_id)}, 
            {"$set": {"state"          : "Running",
                      "nodes"          : self._exec_env.nodes.keys(),
                      "cores_per_node" : self._exec_env.cores_per_node,
                      "started"        : ts},
             "$push": {"statehistory": {"state": 'Running', "timestamp": ts}}
            })

        self._starttime = time.time()

        while True:

            try:

                # Check the workers periodically. If they have died, we 
                # exit as well. this can happen, e.g., if the worker 
                # process has caught a ctrl+C
                exit = False
                for ew in self._exec_workers:
                    if ew.is_alive() is False:
                        pilot_FAILED(self._p, self._pilot_id, self._log, "Execution worker %s died." % str(ew))
                        exit = True
                if exit:
                    break

                # Exit the main loop if terminate is set. 
                if self._terminate.isSet():
                    pilot_CANCELED(self._p, self._pilot_id, self._log, "Terminated (_terminate set.")
                    break

                # Make sure that we haven't exceeded the agent runtime. if 
                # we have, terminate. 
                if time.time() >= self._starttime + (int(self._runtime) * 60):
                    self._log.info("Agent has reached runtime limit of %s seconds." % str(int(self._runtime)*60))
                    pilot_DONE(self._p, self._pilot_id)
                    break

                # Try to get new tasks from the database. for this, we check the 
                # wu_queue of the pilot. if there are new entries, we get them,
                # get the actual pilot entries for them and remove them from 
                # the wu_queue.
                try:
                    p_cursor = self._p.find({"_id": ObjectId(self._pilot_id)})

                    #if p_cursor.count() != 1:
                    #    self._log.info("Pilot entry %s has disappeared from the database." % self._pilot_id)
                    #    pilot_FAILED(self._p, self._pilot_id)
                    #    break
                    if False:
                        pass

                    else:
                        # Check if there's a command waiting
                        command = p_cursor[0]['command']
                        if command is not None:
                            self._log.info("Received new command: %s" % command)
                            if command.lower() == "cancel":
                                pilot_CANCELED(self._p, self._pilot_id, self._log, "CANCEL received. Terminating.")
                                break

                        # Check the pilot's workunit queue
                        #new_wu_ids = p_cursor[0]['wu_queue']

                        # Check if there are work units waiting for execution
                        ts = datetime.datetime.utcnow()

                        wu_cursor = self._w.find_and_modify(
                        query={"pilot" : self._pilot_id,
                               "state" : "PendingExecution"},
                        update={"$set" : {"state": "Running"},
                        "$push": {"statehistory": {"state": "RunningX", "timestamp": ts}}}#,
                        #limit=BULK_LIMIT
                        )

                        # There are new work units in the wu_queue on the database.
                        # Get the corresponding wu entries
                        if wu_cursor is not None:
                        #    self._log.info("Found new tasks in pilot queue: %s", new_wu_ids)
                        #    wu_cursor = self._w.find({"_id": {"$in": new_wu_ids}})
                            if not isinstance(wu_cursor, list):
                                wu_cursor = [wu_cursor]

                            for wu in wu_cursor:
                                logger.error("about to process: %s" % wu)
                                # Create new task objects and put them into the 
                                # task queue

                                # WorkingDirectoryPriv is defined, we override the 
                                # standard working directory schema. 
                                # NOTE: this is not a good idea and just implemented
                                #       to support some last minute TROY experiments.
                                if wu["description"]["working_directory_priv"] is not None:
                                    task_dir_name = wu["description"]["working_directory_priv"]
                                else:
                                    task_dir_name = "%s/unit-%s" % (self._workdir, str(wu["_id"]))

                                task = Task(uid         = str(wu["_id"]), 
                                            executable  = wu["description"]["executable"], 
                                            arguments   = wu["description"]["arguments"],
                                            environment = wu["description"]["environment"],
                                            workdir     = task_dir_name, 
                                            stdout      = task_dir_name+'/STDOUT', 
                                            stderr      = task_dir_name+'/STDERR',
                                            output_data = wu["description"]["output_data"])

                                self._task_queue.put(task)

                except Exception, ex:
                    raise

                time.sleep(1)

            except Exception, ex:
                # If we arrive here, there was an exception in the main loop.
                pilot_FAILED(self._p, self._pilot_id, self._log, 
                    "ERROR in agent main loop: %s. %s" % (str(ex), traceback.format_exc()))

        # MAIN LOOP TERMINATED
        return

#-----------------------------------------------------------------------------
#
class _Process(subprocess.Popen):

    #-------------------------------------------------------------------------
    #
    def __init__(self, task, host, launch_method, launch_command, logger):

        self._task = task
        self._log  = logger

        cmdline = str()

        # Based on the launch method we use different, well, launch methods
        # to launch the task. just on the shell, via mpirun, ssh or aprun
        if launch_method == LAUNCH_METHOD_LOCAL:
            pass

        if launch_method == LAUNCH_METHOD_MPIRUN:
            cmdline = launch_command
            cmdline += " -np %s -host %s" % (str(task.numcores), host)

        elif launch_method == launch_command:
            cmdline =  launch_command
            cmdline += " -n %s " % str(task.numcores)
            
        elif launch_method == LAUNCH_METHOD_SSH:
            cmdline = launch_command
            cmdline += " -o StrictHostKeyChecking=no %s " % host

        # task executable and arguments
        payload = str(" cd %s && " % task.workdir)
        payload += " %s " % task.executable
        if task.arguments is not None:
            for arg in task.arguments:
                payload += " %s " % arg
        
        cmdline += "%s" % payload

        self.stdout_filename = task.stdout
        self._stdout_file_h  = open(self.stdout_filename, "w")

        self.stderr_filename = task.stderr
        self._stderr_file_h  = open(self.stderr_filename, "w")

        self._log.info("Launching task %s via %s (env: %s) in %s" % (task.uid, cmdline, task.environment, task.workdir))

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
                                       env=task.environment,
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

    parser.add_option('-u', '--unitmanager-id',
                      metavar='UMID',
                      dest='unitmanager_id',
                      help='Specifies the UnitManager ID.')

    parser.add_option('-w', '--workdir',
                      metavar='DIRECTORY',
                      dest='workdir',
                      help='Specifies the base (working) directory for the agent. [default: %default]',
                      default='.')

    parser.add_option('-c', '--cores',
                      metavar='CORES',
                      dest='cores',
                      help='Specifies the number of cores to allocate.')

    parser.add_option('-t', '--runtime',
                      metavar='RUNTIME',
                      dest='runtime',
                      help='Specifies the agent runtime in minutes.')

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
    elif options.cores is None:
        parser.error("You must define the number of cores (-c/--cores). Try --help for help.")
    elif options.runtime is None:
        parser.error("You must define the agent runtime (-t/--runtime). Try --help for help.")

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
    logger = logging.getLogger('radical.pilot.agent')
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
        mongo_wm     = mongo_db["%s.wm" % options.session_id]

    except Exception, ex:
        logger.error("Couldn't establish database connection: %s" % str(ex))
        sys.exit(1)

    #--------------------------------------------------------------------------
    # Some singal handling magic 
    def sigint_handler(signal, frame):
        msg = 'Caught SIGINT. EXITING.'
        pilot_CANCELED(mongo_p, options.pilot_id, logger, msg)
        sys.exit(0)
    signal.signal(signal.SIGINT, sigint_handler)

    def sigalarm_handler(signal, frame):
        msg = 'Caught SIGALRM (Walltime limit reached?). EXITING'
        pilot_CANCELED(mongo_p, options.pilot_id, logger, msg)
        sys.exit(0)
    signal.signal(signal.SIGALRM, sigalarm_handler)

    #--------------------------------------------------------------------------
    # Discover environment, mpirun, cores, etc.
    try:
        exec_env = ExecutionEnvironment.discover(
            logger=logger,
            launch_method=options.launch_method,
            requested_cores=options.cores
        )
        if exec_env is None:
            msg = "Couldn't set up execution environment."
            logger.error(msg)
            pilot_FAILED(mongo_p, options.pilot_id, logger, msg)
            sys.exit(1)

    except Exception, ex:
        msg = "Error setting up execution environment: %s" % str(ex)
        logger.error(msg)
        pilot_FAILED(mongo_p, options.pilot_id, logger, msg)
        sys.exit(1)

    #--------------------------------------------------------------------------
    # Launch the agent thread
    try:
        if options.workdir is '.':
            workdir = os.getcwd()
        else:
            workdir = options.workdir

        agent = Agent(logger=logger,
                      exec_env=exec_env,
                      workdir=workdir,
                      runtime=options.runtime,
                      launch_method=options.launch_method,
                      mongodb_url=options.mongodb_url,
                      mongodb_name=options.database_name,
                      pilot_id=options.pilot_id,
                      session_id=options.session_id,
                      unitmanager_id=options.unitmanager_id)

        agent.start()
        agent.join()

    except Exception, ex:
        msg = "Error running agent: %s" % str(ex)
        logger.error(msg)
        pilot_FAILED(mongo_p, options.pilot_id, logger, msg)
        agent.stop()
        sys.exit(1)

    except SystemExit:

        logger.error("Caught keyboard interrupt. EXITING")
        agent.stop()
