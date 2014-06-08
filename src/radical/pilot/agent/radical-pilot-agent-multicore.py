#!/usr/bin/env python

"""
.. module:: radical.pilot.agent
   :platform: Unix
   :synopsis: A multi-core agent for RADICAL-Pilot.

.. moduleauthor:: Mark Santcroos <mark.santcroos@rutgers.edu>
"""

__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys
import time
import errno
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
FREE                 = 'Free'
BUSY                 = 'Busy'

LAUNCH_METHOD_SSH    = 'SSH'
LAUNCH_METHOD_AUTO   = 'AUTO'
LAUNCH_METHOD_APRUN  = 'APRUN'
LAUNCH_METHOD_LOCAL  = 'LOCAL'
LAUNCH_METHOD_MPIRUN = 'MPIRUN'
LAUNCH_METHOD_IBRUN = 'IBRUN'

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

#-----------------------------------------------------------------------------
#
class ExecutionEnvironment(object):
    """DOC
    """
    #-------------------------------------------------------------------------
    #
    @classmethod
    def discover(cls, logger, requested_cores):
        """Factory method creates a new execution environment.
        """
        eenv = cls(logger)

        # Discover nodes and number of cores available
        eenv.discover_nodes()
        eenv.discover_cores()

        # Discover task launch methods
        eenv.discovered_task_launch_methods = {}
        # TODO: Can we come up with a test / config whether local exec is allowed?
        #       Maybe at a list of "allowed launch methods" in the resource config?
        eenv.discovered_task_launch_methods[LAUNCH_METHOD_LOCAL] = \
            {'launch_command': None}
        ssh_location    = which('ssh')
        if ssh_location is not None:
            eenv.discovered_task_launch_methods[LAUNCH_METHOD_SSH] = \
                {'launch_command': ssh_location}
        mpirun_location = which('mpirun')
        if mpirun_location is not None:
            eenv.discovered_task_launch_methods[LAUNCH_METHOD_MPIRUN] = \
                {'launch_command': mpirun_location}
        ibrun_location  = which('ibrun')
        if ibrun_location is not None:
            eenv.discovered_task_launch_methods[LAUNCH_METHOD_IBRUN] = \
                {'launch_command': ibrun_location}
        aprun_location  = which('aprun')
        if aprun_location is not None:
            eenv.discovered_task_launch_methods[LAUNCH_METHOD_APRUN] = \
                {'launch_command': aprun_location}

        logger.info("Discovered task launch methods: %s." % [lm for lm in eenv.discovered_task_launch_methods])

        # create node dictionary
        for rn in eenv.raw_nodes:
            if rn not in eenv.nodes:
                eenv.nodes[rn] = {'cores': eenv.cores_per_node}
        logger.info("Discovered execution environment: %s" % eenv.nodes)

        # For now assume that all nodes have equal amount of cores
        cores_avail = len(eenv.nodes) * int(eenv.cores_per_node)
        if cores_avail < int(requested_cores):
            raise Exception("Not enough cores available (%s) to satisfy allocation request (%s)." % (str(cores_avail), str(requested_cores)))

        return eenv

    #-------------------------------------------------------------------------
    #
    def __init__(self, logger=None):
        '''le constructeur
        '''
        self.log = logger

        self.nodes = {}

    #-------------------------------------------------------------------------
    #
    def discover_cores(self):
        sge_hostfile = os.environ.get('PE_HOSTFILE')

        # SGE core configuration might be different than what multiprocessing announces
        # Alternative: "qconf -sq all.q|awk '/^slots *[0-9]+$/{print $2}'"
        if sge_hostfile is not None:
            # parse SGE hostfile
            cores_count_list = [int(line.split()[1]) for line in open(sge_hostfile)]
            core_counts = list(set(cores_count_list))
            self.cores_per_node = min(core_counts)
            self.log.info("Found unique core counts: %s Using: %d" % (core_counts, self.cores_per_node))
        else:
            self.cores_per_node = multiprocessing.cpu_count()

    #-------------------------------------------------------------------------
    #
    def discover_nodes(self):
        # see if we have a PBS_NODEFILE
        pbs_nodefile = os.environ.get('PBS_NODEFILE')
        slurm_nodelist = os.environ.get('SLURM_NODELIST')
        sge_hostfile = os.environ.get('PE_HOSTFILE')

        if pbs_nodefile is not None:
            # parse PBS the nodefile
            self.raw_nodes = [line.strip() for line in open(pbs_nodefile)]
            self.log.info("Found PBS_NODEFILE %s: %s" % (pbs_nodefile, self.raw_nodes))

        elif slurm_nodelist is not None:
            # parse SLURM nodefile
            self.raw_nodes = hostlist.expand_hostlist(slurm_nodelist)
            self.log.info("Found SLURM_NODELIST %s. Expanded to: %s" % (slurm_nodelist, self.raw_nodes))

        elif sge_hostfile is not None:
            # parse SGE hostfile
            self.raw_nodes = [line.split()[0] for line in open(sge_hostfile)]
            self.log.info("Found PE_HOSTFILE %s. Expanded to: %s" % (sge_hostfile, self.raw_nodes))

        else:
            self.raw_nodes = ['localhost']
            self.log.info("No PBS_NODEFILE, SLURM_NODELIST or PE_HOSTFILE found. Using hosts: %s" % (self.raw_nodes))

# ----------------------------------------------------------------------------
#
class Task(object):

    # ------------------------------------------------------------------------
    #
    def __init__(self, uid, executable, arguments, environment, numcores, mpi, pre_exec, workdir, stdout, stderr, output_data):

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
        self.numcores       = numcores
        self.mpi            = mpi
        self.pre_exec       = pre_exec

        # Location
        self.slots          = None

        # dynamic task properties
        self.started        = None
        self.finished       = None

        self.state          = None
        self.exit_code      = None

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
                 launch_methods, mongodb_url, mongodb_name,
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

        # Queued tasks by the Agent
        self._task_queue     = task_queue

        # Launched tasks by this ExecWorker
        self._running_tasks = []

        # Slots represents the internal process management structure.
        # The structure is as follows:
        # [
        #    {'hostname': 'node1', 'cores': [p_1, p_2, p_3, ... , p_cores_per_host]},
        #    {'hostname': 'node2', 'cores': [p_1, p_2, p_3. ... , p_cores_per_host]
        # ]
        #
        self._slots = []
        for host in hosts:
            self._slots.append({
                'host': host,
                'cores': [FREE for _ in range(0, cores_per_host)]
            })
        self._cores_per_host = cores_per_host

        # The available launch methods
        self._available_launch_methods = launch_methods

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

                idle = True

                self._log.debug("Slot status:\n%s", self._slot_status())

                # Loop over tasks instead of slots!
                try:
                    task = self._task_queue.get_nowait()
                    idle = False

                    if task.mpi:
                        # Find an appropriate method to execute MPI on this resource
                        if LAUNCH_METHOD_IBRUN in self._available_launch_methods:
                            launch_method = LAUNCH_METHOD_IBRUN
                            launch_command = self._available_launch_methods[launch_method]['launch_command']
                        elif LAUNCH_METHOD_APRUN in self._available_launch_methods:
                            launch_method = LAUNCH_METHOD_APRUN
                            launch_command = self._available_launch_methods[launch_method]['launch_command']
                        elif LAUNCH_METHOD_MPIRUN in self._available_launch_methods:
                            launch_method = LAUNCH_METHOD_MPIRUN
                            launch_command = self._available_launch_methods[launch_method]['launch_command']
                        else:
                            raise Exception("No task launch method available to execute MPI tasks")
                    else:
                        # For "regular" tasks either use SSH or none
                        # TODO: Find a switch to go for fork() if we run on localhost
                        if LAUNCH_METHOD_SSH in self._available_launch_methods:
                            launch_method = LAUNCH_METHOD_SSH
                            launch_command = self._available_launch_methods[launch_method]['launch_command']
                        else:
                            launch_method = self._available_launch_methods[LAUNCH_METHOD_LOCAL]
                            launch_command = self._available_launch_methods[launch_method]['launch_command']

                    self._log.debug("Launching task with %s (%s)." % (launch_method, launch_command))

                    # IBRUN (e.g. Stampede) requires continuous slots for multi core execution
                    # TODO: Dont have scattered scheduler yet, so test disabled.
                    if True: # launch_method in [LAUNCH_METHOD_IBRUN]:
                        req_cont = True
                    else:
                        req_cont = False

                    # First try to find all cores on a single host
                    host_index, offset = self._acquire_slots(task.numcores, single_host=True, continuous=req_cont)

                    # If that failed, and our launch method supports multiple hosts, try that
                    if host_index is None and launch_method in [LAUNCH_METHOD_IBRUN, LAUNCH_METHOD_MPIRUN]:
                        host_index, offset = self._acquire_slots(task.numcores, single_host=False, continuous=req_cont)

                    # Check if we got results
                    if host_index is None:
                        # No resources free, put back in queue
                        self._task_queue.put(task)
                        idle = True
                    else:
                        # We got an allocation go off and launch the process
                        task.host_index = host_index
                        task.offset = offset
                        task_slots = self._index_and_offset_to_slotlist(host_index, offset, task.numcores)
                        self._launch_task(task, task_slots, launch_method, launch_command)

                except Queue.Empty:
                    # do nothing if we don't have any queued tasks
                    pass

                idle &= self._check_running()

                # Check if something happened in this cycle, if not, zzzzz for a bit
                if idle:
                    time.sleep(1)

        except Exception, ex:
            self._log.error("Error in ExecWorker loop: %s", traceback.format_exc())
            raise


    # ------------------------------------------------------------------------
    #
    def _slot_status(self):
        """Returns a multi-line string corresponding to slot status.
        """
        slot_matrix = ""
        for slot in self._slots:
            slot_vector = ""
            for core in slot['cores']:
                if core is FREE:
                    slot_vector += " - "
                else:
                    slot_vector += " X "
            slot_matrix += "%s: %s\n" % (slot['host'].ljust(24), slot_vector)
        return slot_matrix

    # ------------------------------------------------------------------------
    #
    def _acquire_slots(self, numcores, single_host, continuous):

        #
        # Find a needle (continuous sub-list) in a haystack (list)
        #
        def find_sublist(haystack, needle):
            n = len(needle)
            # Find all matches (returns list of False and True for every position)
            hits = [(needle == haystack[i:i+n]) for i in xrange(len(haystack)-n+1)]
            try:
                # Grab the first occurrence
                index = hits.index(True)
            except ValueError:
                index = None

            return index

        #
        # Transform the number of cores into a continuous list of "status"es,
        # and use that to find a sub-list.
        #
        def find_cores_cont(cores, count, status):
            return find_sublist(cores, [status for _ in range(count)])

        #
        # Find an available continuous slot within host boundaries.
        #
        def find_slots_single_cont(count):

            for slot in self._slots:
                cores = slot['cores']
                offset = find_cores_cont(cores, count, FREE)
                if offset is not None:
                    self._log.info('Host %s satisfies %d at offset %d' % (slot['host'], count, offset))
                    return (self._slots.index(slot), offset)

            return None, None

        #
        # Find an available continuous slot across host boundaries.
        #
        def find_slots_multi_cont(count):
            # Glue core lists together
            cores = [core for host in [host['cores'] for host in self._slots] for core in host]

            ppn = self._cores_per_host

            # Find the start of the first available region
            cores_index = find_cores_cont(cores, count, FREE)
            if cores_index is None:
                return None, None

            # Determine the host in the hostlist
            host_index = cores_index/ppn
            # And the offset within that host
            offset = cores_index%ppn

            return (host_index, offset)

        #################################################################################
        #  End of inline functions, _acquire_slots() code begins here

        #
        # Switch between searching for continuous or scattered slots
        #
        if continuous:
            # Switch between searching for single or multi-host
            if single_host:
                host_index, offset = find_slots_single_cont(numcores)
            else:
                host_index, offset = find_slots_multi_cont(numcores)

            if host_index is not None:
                self._mark_continuous(host_index, offset, numcores, BUSY)
        else:
            if single_host:
                raise NotImplementedError('No scattered scheduler implemented yet.')
            else:
                raise NotImplementedError('No scattered scheduler implemented yet.')

            if host_index is not None:
                raise NotImplementedError('No scattered marker implemented yet.')

        return host_index, offset

    #
    # Convert an index, offset and core count into an expanded slot list
    # TODO: This needs work!
    #
    def _index_and_offset_to_slotlist(self, first_host_index, first_host_offset, core_count):

        slot_list = []

        ppn = self._cores_per_host

        last_core = (first_host_index * ppn) + first_host_offset + core_count - 1
        # We substract one here, because counting starts at zero;
        # Imagine a zero offset and a count of 1, the only core used would be core 0.
        #print 'Last core:', last_core

        last_host_index = (last_core) / ppn
        last_host = self._slots[last_host_index]['host']
        last_host_offset = last_core % ppn

        first_host = self._slots[first_host_index]['host']

        for host_index in range(first_host_index, last_host_index+1):
            # (within range() +1 because the ceiling is exclusive)

            first_core = 0
            last_core = ppn - 1

            if host_index == first_host_index:
                first_core = first_host_offset

            if host_index == last_host_index:
                last_core = last_host_offset

            for core in range(first_core, last_core+1):
                slot_list.append('%s:%d' % (self._slots[host_index]['host'], core))

        return slot_list

    #
    # For continuous allocations, this can be used to set a state
    #
    def _mark_continuous(self, first_host_index, first_host_offset, core_count, state):

        # Calculate the last core as a base for other calculations
        last_core = (first_host_index * self._cores_per_host) + \
                    first_host_offset + core_count - 1
        # We substract one here, because counting starts at zero;
        # Imagine a zero offset and a count of 1, the only core used would be core 0.

        # Benefit from some integer rounding here
        last_host_index = (last_core) / self._cores_per_host
        last_host_offset = last_core % self._cores_per_host

        # Get the hostnames for the host indexes
        last_host = self._slots[last_host_index]['host']
        first_host = self._slots[first_host_index]['host']

        self._log.debug('First host: %s offset: %d' % (first_host, first_host_offset))
        self._log.debug('Last host: %s offset: %d' % (last_host, last_host_offset))

        # Iterate over all hosts involved
        for host_index in range(first_host_index, last_host_index+1):
            # (within range() +1 because the ceiling is exclusive)

            # Values if a full host is involved
            first_core = 0
            last_core = self._cores_per_host - 1

            # If this is the first host, start at the offset
            if host_index == first_host_index:
                first_core = first_host_offset

            # If this is the last host, stop at the offset
            if host_index == last_host_index:
                last_core = last_host_offset

            self._log.debug('Host: %s (%d-%d)' % (self._slots[host_index]['host'], first_core, last_core))

            # Use first and last core values above to set cores state
            for core in range(first_core, last_core+1):
                self._slots[host_index]['cores'][core] = state

    # ------------------------------------------------------------------------
    #
    def _launch_task(self, task, slots, launch_method, launch_command):

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
        proc = _Process(
            task=task,
            slots=slots,
            launch_method=launch_method,
            launch_command=launch_command,
            logger=self._log)

        task.started=datetime.datetime.utcnow()
        task.slots=slots
        task.state='Executing'

        self._running_tasks.append(proc)


    # ------------------------------------------------------------------------
    #
    def _check_running(self):

        idle = True

        # we update tasks in 'bulk' after each iteration.
        # all tasks that require DB updates are in update_tasks
        update_tasks = []
        finished_tasks = []

        for proc in self._running_tasks:

            rc = proc.poll()
            if rc is None:
                # subprocess is still running
                continue

            finished_tasks.append(proc)

            # Make sure all stuff reached the spindles
            proc.close_and_flush_filehandles()

            # Convenience shortcut
            task = proc.task

            uid = task.uid
            self._log.info("Task %s terminated with return code %s." % (uid, rc))

            if rc != 0:
                state = 'Failed'
            else:
                if task.output_data is not None:
                    state = 'PendingOutputTransfer'
                else:
                    state = 'Done'

            # upload stdout and stderr to GridFS
            workdir = task.workdir
            task_id = task.uid

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

            task.finished=datetime.datetime.utcnow()
            task.exit_code=rc
            task.state=state
            task.stdout_id=stdout_id
            task.stderr_id=stderr_id

            update_tasks.append(task)

            self._mark_continuous(task.host_index, task.offset, task.numcores, FREE)

        # update all the tasks that are marked for update.
        self._update_tasks(update_tasks)

        for e in finished_tasks:
            self._running_tasks.remove(e)

        return idle

    # ------------------------------------------------------------------------
    #
    def _update_tasks(self, tasks):
        """Updates the database entries for one or more tasks, including
        task state, log, etc.
        """
        ts = datetime.datetime.utcnow()
        # We need to know which unit manager we are working with. We can pull
        # this information here:

        if self._unitmanager_id is None:
            cursor_p = self._p.find({"_id": ObjectId(self._pilot_id)},
                                    {"unitmanager": 1})
            self._unitmanager_id = cursor_p[0]["unitmanager"]

        for task in tasks:
            self._w.update({"_id": ObjectId(task.uid)}, 
            {"$set": {"state"         : task.state,
                      "started"       : task.started,
                      "finished"      : task.finished,
                      "slots"         : task.slots,
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
    def __init__(self, logger, exec_env, workdir, runtime,
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

        # the task queue holds the tasks that are pulled from the MongoDB
        # server. The ExecWorkers compete for the tasks in the queue. 
        self._task_queue = multiprocessing.Queue()

        # we assign each host partition to a task execution worker
        self._exec_worker = ExecWorker(
            logger          = self._log,
            task_queue      = self._task_queue,
            hosts           = self._exec_env.nodes,
            cores_per_host  = self._exec_env.cores_per_node,
            launch_methods  = self._exec_env.discovered_task_launch_methods,
            mongodb_url     = mongodb_url,
            mongodb_name    = mongodb_name,
            pilot_id        = pilot_id,
            session_id      = session_id,
            unitmanager_id = unitmanager_id
        )
        self._exec_worker.start()
        self._log.info("Started up %s serving hosts %s", self._exec_worker, self._exec_env.nodes)

    # ------------------------------------------------------------------------
    #
    def stop(self):
        """Terminate the agent main loop.
        """
        # First, we need to shut down all the workers
        self._exec_worker.terminate()

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
            {"$set": {"state"          : "Active",
                      "nodes"          : self._exec_env.nodes.keys(),
                      "cores_per_node" : self._exec_env.cores_per_node,
                      "started"        : ts},
             "$push": {"statehistory": {"state": 'Active', "timestamp": ts}}
            })

        self._starttime = time.time()

        while True:

            try:

                # Check the workers periodically. If they have died, we 
                # exit as well. this can happen, e.g., if the worker 
                # process has caught a ctrl+C
                exit = False
                if self._exec_worker.is_alive() is False:
                    pilot_FAILED(self._p, self._pilot_id, self._log, "Execution worker %s died." % str(self._exec_worker))
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
                        update={"$set" : {"state": "Executing"},
                        "$push": {"statehistory": {"state": "PulledByAgent", "timestamp": ts}}}#,
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
                                # Create new task objects and put them into the 
                                # task queue

                                task_dir_name = "%s/unit-%s" % (self._workdir, str(wu["_id"]))

                                task = Task(uid         = str(wu["_id"]), 
                                            executable  = wu["description"]["executable"], 
                                            arguments   = wu["description"]["arguments"],
                                            environment = wu["description"]["environment"],
                                            numcores    = wu["description"]["cores"],
                                            mpi         = wu["description"]["mpi"],
                                            pre_exec    = wu["description"]["pre_exec"],
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
    def __init__(self, task, slots, launch_method, launch_command, logger):

        self._task = task
        self._log  = logger

        # Before the Big Bang there was nothing
        pre_exec = task.pre_exec
        pre_exec_string = ''
        if pre_exec:
            if not isinstance(pre_exec, list):
                pre_exec = [pre_exec]
            for bb in pre_exec:
                pre_exec_string += "%s && " % bb

        # executable and arguments
        if task.executable is not None:
            task_exec_string = task.executable
        else:
            task_exec_string = ''
        if task.arguments is not None:
            for arg in task.arguments:
                task_exec_string += " %s" % arg

        # Based on the launch method we use different, well, launch methods
        # to launch the task. just on the shell, via mpirun, ssh, ibrun or aprun
        if launch_method == LAUNCH_METHOD_LOCAL:
            cmdline = ''

        elif launch_method == LAUNCH_METHOD_MPIRUN:
            hosts_string = ''
            for slot in slots:
                host = slot.split(':')[0]
                hosts_string += '%s,' % host

            mpirun_command = "%s -x PATH -np %s -host %s" % (launch_command, task.numcores, hosts_string)
            cmdline = "/bin/bash -l -c '%scd %s && %s %s'" % (pre_exec_string, task.workdir, mpirun_command, task_exec_string)

        elif launch_method == LAUNCH_METHOD_APRUN:
            cmdline = launch_command
            # APRUN MAGIC

        elif launch_method == LAUNCH_METHOD_IBRUN:
            # NOTE: Don't think that with IBRUN it is possible to have
            # processes != cores ...
            # TODO: this hardcoded 16 obviously needs to go away,
            #       the information is available though, just not to this class.
            ibrun_offset = task.host_index * 16 + task.offset
            ibrun_command = "%s -n %s -o %d" % (launch_command, task.numcores, ibrun_offset)
            cmdline = "/bin/bash -l -c '%scd %s && %s %s'" % (pre_exec_string, task.workdir, ibrun_command, task_exec_string)

        elif launch_method == LAUNCH_METHOD_SSH:
            host = slots[0].split(':')[0]
            cmdline = " %s -o StrictHostKeyChecking=no %s \"/bin/bash -l -c '%scd %s && %s'\"" % \
                      (launch_command, host, pre_exec_string, task.workdir, task_exec_string)

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
                                       cwd=task.workdir, # TODO: This doesn't always make sense if it runs remotely
                                       env=task.environment, # TODO: Idem
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

    parser.add_option('-V', '--version',
                      metavar='VERSION ',
                      dest='package_version',
                      help='The RADICAL-Pilot package version.')

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
    elif options.package_version is None:
        parser.error("You must pass the RADICAL-Pilot package version (-v/--version). Try --help for help.")

    return options

#-----------------------------------------------------------------------------
#
if __name__ == "__main__":

    # parse command line options
    options = parse_commandline()

    # configure the agent logger
    logger = logging.getLogger('radical.pilot.agent')
    logger.setLevel(logging.DEBUG)
    ch = logging.FileHandler("AGENT.LOG")
    #ch.setLevel(logging.DEBUG) # TODO: redundant if you have just one file?
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.info("RADICAL-Pilot multi-core agent for package/API version %s" % options.package_version)

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
    # Discover environment, nodes, cores, mpi, etc.
    try:
        exec_env = ExecutionEnvironment.discover(
            logger=logger,
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
