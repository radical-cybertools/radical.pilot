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
import stat
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
import tempfile
import traceback
import threading 
import subprocess
import multiprocessing

from bson.objectid import ObjectId


# ----------------------------------------------------------------------------
# CONSTANTS
FREE                 = 'Free'
BUSY                 = 'Busy'

LAUNCH_METHOD_SSH     = 'SSH'
LAUNCH_METHOD_APRUN   = 'APRUN'
LAUNCH_METHOD_LOCAL   = 'LOCAL'
LAUNCH_METHOD_MPIRUN  = 'MPIRUN'
LAUNCH_METHOD_MPIEXEC = 'MPIEXEC'
LAUNCH_METHOD_POE     = 'POE'
LAUNCH_METHOD_IBRUN   = 'IBRUN'

MULTI_NODE_LAUNCH_METHODS =  [LAUNCH_METHOD_IBRUN,
                              LAUNCH_METHOD_MPIRUN,
                              LAUNCH_METHOD_POE,
                              LAUNCH_METHOD_APRUN,
                              LAUNCH_METHOD_MPIEXEC]

LRMS_TORQUE = 'TORQUE'
LRMS_PBSPRO = 'PBSPRO'
LRMS_SLURM  = 'SLURM'
LRMS_SGE    = 'SGE'
LRMS_LSF    = 'LSF'
LRMS_LOADL  = 'LOADL'
LRMS_FORK   = 'FORK'

COMMAND_CANCEL_PILOT        = "Cancel_Pilot"
COMMAND_CANCEL_COMPUTE_UNIT = "Cancel_Compute_Unit"
COMMAND_KEEP_ALIVE          = "Keep_Alive"
COMMAND_FIELD               = "commands"
COMMAND_TYPE                = "type"
COMMAND_ARG                 = "arg"

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
                   "capability" : 0,
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
                   "capability" : 0,
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
                   "capability" : 0,
                  "finished": ts}

        })

#-----------------------------------------------------------------------------
#
class ExecutionEnvironment(object):
    """DOC
    """
    #-------------------------------------------------------------------------
    #
    def __init__(self, logger, lrms, requested_cores, task_launch_method, mpi_launch_method):
        self.log = logger

        self.requested_cores = requested_cores
        self.node_list = None # TODO: Need to think about a structure that works for all machines
        self.cores_per_node = None # Work with one value for now

        # Configure nodes and number of cores available
        self._configure(lrms)

        task_launch_command = None
        mpi_launch_command = None

        # Regular tasks
        if task_launch_method == LAUNCH_METHOD_LOCAL:
            task_launch_command = None

        elif task_launch_method == LAUNCH_METHOD_SSH:
            # Find ssh command
            command = self._find_ssh()
            if command is not None:
                task_launch_command = command

        elif task_launch_method == LAUNCH_METHOD_APRUN:
            # aprun: job launcher for Cray systems
            command = self._which('aprun')
            if command is not None:
                task_launch_command = command
        else:
            raise Exception("Task launch method not set or unknown: %s!" % task_launch_method)

        # MPI tasks
        if mpi_launch_method == LAUNCH_METHOD_MPIRUN:
            command = self._find_executable(['mpirun',           # General case
                                             'mpirun-openmpi-mp' # Mac OSX MacPorts
                                            ])
            if command is not None:
                mpi_launch_command = command

        elif mpi_launch_method == LAUNCH_METHOD_MPIEXEC:
            # mpiexec (e.g. on SuperMUC)
            command = self._which('mpiexec')
            if command is not None:
                mpi_launch_command = command

        elif mpi_launch_method == LAUNCH_METHOD_APRUN:
            # aprun: job launcher for Cray systems
            command = self._which('aprun')
            if command is not None:
                mpi_launch_command = command

        elif mpi_launch_method == LAUNCH_METHOD_IBRUN:
            # ibrun: wrapper for mpirun at TACC
            command = self._which('ibrun')
            if command is not None:
                mpi_launch_command = command

        elif mpi_launch_method == LAUNCH_METHOD_POE:
            # poe: LSF specific wrapper for MPI (e.g. yellowstone)
            command = self._which('poe')
            if command is not None:
                mpi_launch_command = command

        else:
            raise Exception("MPI launch method not set or unknown: %s!" % mpi_launch_method)

        if not mpi_launch_command:
            self.log.warning("No MPI launch command found for launch method: %s." % mpi_launch_method)

        self.discovered_launch_methods = {
            'task_launch_method': task_launch_method,
            'task_launch_command': task_launch_command,
            'mpi_launch_method': mpi_launch_method,
            'mpi_launch_command': mpi_launch_command
        }

        logger.info("Discovered task launch command: '%s' and MPI launch command: '%s'." % \
                    (task_launch_command, mpi_launch_command))

        logger.info("Discovered execution environment: %s" % self.node_list)

        # For now assume that all nodes have equal amount of cores
        cores_avail = len(self.node_list) * self.cores_per_node
        if cores_avail < int(requested_cores):
            raise Exception("Not enough cores available (%s) to satisfy allocation request (%s)." % (str(cores_avail), str(requested_cores)))


    def _find_ssh(self):

        command = self._which('ssh')

        if command is not None:

            # Some MPI environments (e.g. SGE) put a link to rsh as "ssh" into the path.
            # We try to detect that and then use different arguments.
            if os.path.islink(command):

                target = os.path.realpath(command)

                if os.path.basename(target) == 'rsh':
                    self.log.info('Detected that "ssh" is a link to "rsh".')
                    return target

            return '%s -o StrictHostKeyChecking=no' % command


    #-----------------------------------------------------------------------------
    #
    def _find_executable(self, names):
        """Takes a (list of) name(s) and looks for an executable in the path.
        """

        if not isinstance(names, list):
            names = [names]

        for name in names:
            ret = self._which(name)
            if ret is not None:
                return ret

        return None


    #-----------------------------------------------------------------------------
    #
    def _which(self, program):
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

    #-------------------------------------------------------------------------
    #
    def _configure_torque(self):
        self.log.info("Configured to run on system with %s." % LRMS_TORQUE)

        torque_nodefile = os.environ.get('PBS_NODEFILE')
        if torque_nodefile is None:
            msg = "$PBS_NODEFILE not set!"
            self.log.error(msg)
            raise Exception(msg)

        # Parse PBS the nodefile
        torque_nodes = [line.strip() for line in open(torque_nodefile)]
        self.log.info("Found Torque PBS_NODEFILE %s: %s" % (torque_nodefile, torque_nodes))

        # Number of nodes involved in allocation
        val = os.environ.get('PBS_NUM_NODES')
        if val:
            torque_num_nodes = int(val)
        else:
            msg = "$PBS_NUM_NODES not set! (old Torque version?)"
            torque_num_nodes = None
            self.log.warning(msg)

        # Number of cores (processors) per node
        val = os.environ.get('PBS_NUM_PPN')
        if val:
            torque_cores_per_node = int(val)
        else:
            msg = "$PBS_NUM_PPN not set! (old Torque version?)"
            torque_cores_per_node = None
            self.log.warning(msg)

        # Number of entries in nodefile should be PBS_NUM_NODES * PBS_NUM_PPN
        torque_nodes_length = len(torque_nodes)
        if torque_num_nodes and torque_cores_per_node and \
            torque_nodes_length != torque_num_nodes * torque_cores_per_node:
            msg = "Number of entries in $PBS_NODEFILE (%s) does not match with $PBS_NUM_NODES*$PBS_NUM_PPN (%s*%s)" % \
                  (torque_nodes_length, torque_nodes, torque_cores_per_node)
            raise Exception(msg)

        # only unique node names
        torque_node_list = list(set(torque_nodes))
        torque_node_list_length = len(torque_node_list)
        self.log.debug("Node list: %s(%d)" % (torque_node_list, torque_node_list_length))

        if torque_num_nodes and torque_cores_per_node:
            # Modern style Torque
            self.cores_per_node = torque_cores_per_node
        else:
            # Old style Torque (Should we just use this for all versions?)
            self.cores_per_node = torque_nodes_length / torque_node_list_length
        self.node_list = torque_node_list


    #-------------------------------------------------------------------------
    #
    def _parse_pbspro_vnodes(self):

        # PBS Job ID
        val = os.environ.get('PBS_JOBID')
        if val:
            pbspro_jobid = val
        else:
            msg = "$PBS_JOBID not set!"
            self.log.error(msg)
            raise Exception(msg)

        # Get the output of qstat -f for this job
        output = subprocess.check_output(["qstat", "-f", pbspro_jobid])

        # Get the (multiline) 'exec_vnode' entry
        vnodes_str = ''
        for line in output.splitlines():
            # Detect start of entry
            if 'exec_vnode = ' in line:
                vnodes_str += line.strip()
            elif vnodes_str:
                # Find continuing lines
                if " = " not in line:
                    vnodes_str += line.strip()
                else:
                    break

        # Get the RHS of the entry
        input = vnodes_str.split('=',1)[1].strip()
        self.log.debug("input: %s" % input)

        nodes_list = []
        # Break up the individual node partitions into vnode slices
        while True:
            idx = input.find(')+(')

            node_str = input[1:idx]
            nodes_list.append(node_str)
            input = input[idx+2:]

            if idx < 0:
                break

        vnodes_list = []
        cpus_list = []
        # Split out the slices into vnode name and cpu count
        for node_str in nodes_list:
            slices = node_str.split('+')
            for slice in slices:
                vnode, cpus = slice.split(':')
                cpus = int(cpus.split('=')[1])
                self.log.debug('vnode: %s cpus: %s' % (vnode, cpus))
                vnodes_list.append(vnode)
                cpus_list.append(cpus)

        self.log.debug("vnodes: %s" % vnodes_list)
        self.log.debug("cpus: %s" % cpus_list)

        cpus_list = list(set(cpus_list))
        min_cpus = int(min(cpus_list))

        if len(cpus_list) > 1:
            self.log.debug("Detected vnodes of different sizes: %s, the minimal is: %d." % (cpus_list, min_cpus))

        node_list = []
        for vnode in vnodes_list:
            # strip the last _0 of the vnodes to get the node name
            node_list.append(vnode.rsplit('_', 1)[0])

        # only unique node names
        node_list = list(set(node_list))
        self.log.debug("Node list: %s" % node_list)

        # Return the list of node names
        return node_list


    #-------------------------------------------------------------------------
    #
    def _configure_pbspro(self):
        self.log.info("Configured to run on system with %s." % LRMS_PBSPRO)
        # TODO: $NCPUS?!?! = 1 on archer

        pbspro_nodefile = os.environ.get('PBS_NODEFILE')

        if pbspro_nodefile is None:
            msg = "$PBS_NODEFILE not set!"
            self.log.error(msg)
            raise Exception(msg)

        self.log.info("Found PBSPro $PBS_NODEFILE %s." % pbspro_nodefile)

        # Dont need to parse the content of nodefile for PBSPRO,
        # only the length is interesting, as there are only duplicate entries in it.
        pbspro_nodes_length = len([line.strip() for line in open(pbspro_nodefile)])

        # Number of Processors per Node
        val = os.environ.get('NUM_PPN')
        if val:
            pbspro_num_ppn = int(val)
        else:
            msg = "$NUM_PPN not set!"
            self.log.error(msg)
            raise Exception(msg)

        # Number of Nodes allocated
        val = os.environ.get('NODE_COUNT')
        if val:
            pbspro_node_count = int(val)
        else:
            msg = "$NODE_COUNT not set!"
            self.log.error(msg)
            raise Exception(msg)

        # Number of Parallel Environments
        val = os.environ.get('NUM_PES')
        if val:
            pbspro_num_pes = int(val)
        else:
            msg = "$NUM_PES not set!"
            self.log.error(msg)
            raise Exception(msg)

        pbspro_vnodes = self._parse_pbspro_vnodes()

        # Verify that $NUM_PES == $NODE_COUNT * $NUM_PPN == len($PBS_NODEFILE)
        if not (pbspro_node_count * pbspro_num_ppn == pbspro_num_pes == pbspro_nodes_length):
            self.log.warning("NUM_PES != NODE_COUNT * NUM_PPN != len($PBS_NODEFILE)")

        self.cores_per_node = pbspro_num_ppn
        self.node_list = pbspro_vnodes

    #-------------------------------------------------------------------------
    #
    def _configure_slurm(self):

        self.log.info("Configured to run on system with %s." % LRMS_SLURM)

        slurm_nodelist = os.environ.get('SLURM_NODELIST')
        if slurm_nodelist is None:
            msg = "$SLURM_NODELIST not set!"
            self.log.error(msg)
            raise Exception(msg)

        # Parse SLURM nodefile environment variable
        slurm_nodes = hostlist.expand_hostlist(slurm_nodelist)
        self.log.info("Found SLURM_NODELIST %s. Expanded to: %s" % (slurm_nodelist, slurm_nodes))

        # $SLURM_NPROCS = Total number of processes in the current job
        slurm_nprocs_str = os.environ.get('SLURM_NPROCS')
        if slurm_nprocs_str is None:
            msg = "$SLURM_NPROCS not set!"
            self.log.error(msg)
            raise Exception(msg)
        else:
            slurm_nprocs = int(slurm_nprocs_str)

        # $SLURM_NNODES = Total number of nodes in the job's resource allocation
        slurm_nnodes_str = os.environ.get('SLURM_NNODES')
        if slurm_nnodes_str is None:
            msg = "$SLURM_NNODES not set!"
            self.log.error(msg)
            raise Exception(msg)
        else:
            slurm_nnodes = int(slurm_nnodes_str)

        # $SLURM_CPUS_ON_NODE = Count of processors available to the job on this node.
        slurm_cpus_on_node_str = os.environ.get('SLURM_CPUS_ON_NODE')
        if slurm_cpus_on_node_str is None:
            msg = "$SLURM_NNODES not set!"
            self.log.error(msg)
            raise Exception(msg)
        else:
            slurm_cpus_on_node = int(slurm_cpus_on_node_str)

        # Verify that $SLURM_NPROCS == $SLURM_NNODES * $SLURM_CPUS_ON_NODE
        if slurm_nnodes * slurm_cpus_on_node != slurm_nprocs:
            self.log.error("$SLURM_NPROCS(%d) != $SLURM_NNODES(%d) * $SLURM_CPUS_ON_NODE(%d)" % \
                           (slurm_nnodes, slurm_cpus_on_node, slurm_nprocs))

        # Verify that $SLURM_NNODES == len($SLURM_NODELIST)
        if slurm_nnodes != len(slurm_nodes):
            self.log.error("$SLURM_NNODES(%d) != len($SLURM_NODELIST)(%d)" % \
                           (slurm_nnodes, len(slurm_nodes)))

        self.cores_per_node = slurm_cpus_on_node
        self.node_list = slurm_nodes

    #-------------------------------------------------------------------------
    #
    def _configure_sge(self):

        sge_hostfile = os.environ.get('PE_HOSTFILE')
        if sge_hostfile is None:
            msg = "$PE_HOSTFILE not set!"
            self.log.error(msg)
            raise Exception(msg)

        # SGE core configuration might be different than what multiprocessing announces
        # Alternative: "qconf -sq all.q|awk '/^slots *[0-9]+$/{print $2}'"

        # Parse SGE hostfile for nodes
        sge_node_list = [line.split()[0] for line in open(sge_hostfile)]
        # Keep only unique nodes
        sge_nodes = list(set(sge_node_list))
        self.log.info("Found PE_HOSTFILE %s. Expanded to: %s" % (sge_hostfile, sge_nodes))

        # Parse SGE hostfile for cores
        sge_cores_count_list = [int(line.split()[1]) for line in open(sge_hostfile)]
        sge_core_counts = list(set(sge_cores_count_list))
        sge_cores_per_node = min(sge_core_counts)
        self.log.info("Found unique core counts: %s Using: %d" % (sge_core_counts, sge_cores_per_node))

        self.node_list = sge_nodes
        self.cores_per_node = sge_cores_per_node


    #-------------------------------------------------------------------------
    #
    def _configure_lsf(self):

        self.log.info("Configured to run on system with %s." % LRMS_LSF)

        lsf_hostfile = os.environ.get('LSB_DJOB_HOSTFILE')
        if lsf_hostfile is None:
            msg = "$LSB_DJOB_HOSTFILE not set!"
            self.log.error(msg)
            raise Exception(msg)

        lsb_mcpu_hosts = os.environ.get('LSB_MCPU_HOSTS')
        if lsb_mcpu_hosts is None:
            msg = "$LSB_MCPU_HOSTS not set!"
            self.log.error(msg)
            raise Exception(msg)

        # parse LSF hostfile
        # format:
        # <hostnameX>
        # <hostnameX>
        # <hostnameY>
        # <hostnameY>
        #
        # There are in total "-n" entries (number of tasks) and "-R" entries per host (tasks per host).
        # (That results in "-n" / "-R" unique hosts)
        #
        lsf_nodes = [line.strip() for line in open(lsf_hostfile)]
        self.log.info("Found LSB_DJOB_HOSTFILE %s. Expanded to: %s" % (lsf_hostfile, lsf_nodes))
        lsf_node_list = list(set(lsf_nodes))

        # Grab the core (slot) count from the environment
        # Format: hostX N hostY N hostZ N
        lsf_cores_count_list = map(int, lsb_mcpu_hosts.split()[1::2])
        lsf_core_counts = list(set(lsf_cores_count_list))
        lsf_cores_per_node = min(lsf_core_counts)
        self.log.info("Found unique core counts: %s Using: %d" % (lsf_core_counts, lsf_cores_per_node))

        self.node_list = lsf_node_list
        self.cores_per_node = lsf_cores_per_node

    #-------------------------------------------------------------------------
    #
    def _configure_loadl(self):

        self.log.info("Configured to run on system with %s." % LRMS_LOADL)

        #LOADL_HOSTFILE
        loadl_hostfile = os.environ.get('LOADL_HOSTFILE')
        if loadl_hostfile is None:
            msg = "$LOADL_HOSTFILE not set!"
            self.log.error(msg)
            raise Exception(msg)

        #LOADL_TOTAL_TASKS
        loadl_total_tasks_str = os.environ.get('LOADL_TOTAL_TASKS')
        if loadl_total_tasks_str is None:
            msg = "$LOADL_TOTAL_TASKS not set!"
            self.log.error(msg)
            raise Exception(msg)
        else:
            loadl_total_tasks = int(loadl_total_tasks_str)

        loadl_nodes = [line.strip() for line in open(loadl_hostfile)]
        self.log.info("Found LOADL_HOSTFILE %s. Expanded to: %s" % (loadl_hostfile, loadl_nodes))
        loadl_node_list = list(set(loadl_nodes))

        # Assume: cores_per_node = lenght(nodefile) / len(unique_nodes_in_nodefile)
        loadl_cpus_per_node = len(loadl_nodes) / len(loadl_node_list)

        # Verify that $LLOAD_TOTAL_TASKS == len($LOADL_HOSTFILE)
        if loadl_total_tasks != len(loadl_nodes):
            self.log.error("$LLOAD_TOTAL_TASKS(%d) != len($LOADL_HOSTFILE)(%d)" % \
                           (loadl_total_tasks, len(loadl_nodes)))

        self.node_list = loadl_node_list
        self.cores_per_node = loadl_cpus_per_node

    #-------------------------------------------------------------------------
    #
    def _configure_fork(self):

        self.log.info("Using fork on localhost.")

        detected_cpus = multiprocessing.cpu_count()
        selected_cpus = min(detected_cpus, self.requested_cores)

        self.log.info("Detected %d cores on localhost, using %d." % (detected_cpus, selected_cpus))

        self.node_list = ["localhost"]
        self.cores_per_node = selected_cpus


    #-------------------------------------------------------------------------
    #
    def _configure(self, lrms):
        # TODO: These dont have to be the same number for all hosts.

        # TODO: We might not have reserved the whole node.

        # TODO: Given that the Agent can determine the real core count, in principle we
        #       could just ignore the config and use as many as we have to our availability
        #       (taken into account that we might not have the full node reserved of course)
        #       Answer: at least on Yellowstone this doesnt work for MPI,
        #               as you can't spawn more tasks then the number of slots.


        if lrms == LRMS_FORK:
            # Fork on localhost
            self._configure_fork()

        elif lrms == LRMS_TORQUE:
            # TORQUE/PBS (e.g. India)
            self._configure_torque()

        elif lrms == LRMS_PBSPRO:
            # PBSPro (e.g. Archer)
            self._configure_pbspro()

        elif lrms == LRMS_SLURM:
            # SLURM (e.g. Stampede)
            self._configure_slurm()

        elif lrms == LRMS_SGE:
            # SGE (e.g. DAS4)
            self._configure_sge()

        elif lrms == LRMS_LSF:
            # LSF (e.g. Yellowstone)
            self._configure_lsf()

        elif lrms == LRMS_LOADL:
            # LoadLeveler (e.g. SuperMUC)
            self._configure_loadl()

        else:
            msg = "Unknown lrms type %s." % lrms
            self.log.error(msg)
            raise Exception(msg)

# ----------------------------------------------------------------------------
#
class Task(object):

    #
    def __init__(self, uid, executable, arguments, environment, numcores, mpi,
                 pre_exec, post_exec, workdir, stdout, stderr, output_data):

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
        self.post_exec      = post_exec

        # Location
        self.slots          = None

        # dynamic task properties
        self.started        = None
        self.finished       = None

        self.state          = None
        self.exit_code      = None

        self.stdout_id      = None
        self.stderr_id      = None

        self._log           = []
        self._proc          = None


# ----------------------------------------------------------------------------
#
class ExecWorker(multiprocessing.Process):
    """An ExecWorker competes for the execution of tasks in a task queue
    and writes the results back to MongoDB.
    """

    # ------------------------------------------------------------------------
    #
    def __init__(self, logger, task_queue, command_queue, node_list, cores_per_node,
                 launch_methods, mongodb_url, mongodb_name,
                 pilot_id, session_id, benchmark):

        """Le Constructeur creates a new ExecWorker instance.
        """
        multiprocessing.Process.__init__(self)
        self.daemon      = True
        self._terminate  = False

        self._log = logger

        self._pilot_id  = pilot_id
        self._benchmark = benchmark

        mongo_client = pymongo.MongoClient(mongodb_url)
        self._mongo_db = mongo_client[mongodb_name]
        self._p = mongo_db["%s.p"  % session_id]
        self._w = mongo_db["%s.w"  % session_id]
        self._wm = mongo_db["%s.wm" % session_id]

        # Queued tasks by the Agent
        self._task_queue     = task_queue

        # Queued commands by the Agent
        self._command_queue = command_queue

        # Launched tasks by this ExecWorker
        self._running_tasks = []
        self._cuids_to_cancel = []

        # Slots represents the internal process management structure.
        # The structure is as follows:
        # [
        #    {'node': 'node1', 'cores': [p_1, p_2, p_3, ... , p_cores_per_node]},
        #    {'node': 'node2', 'cores': [p_1, p_2, p_3. ... , p_cores_per_node]
        # ]
        #
        # We put it in a list because we care about (and make use of) the order.
        #
        self._slots = []
        for node in node_list:
            self._slots.append({
                'node': node,
                # TODO: Maybe use the real core numbers in the case of non-exclusive host reservations?
                'cores': [FREE for _ in range(0, cores_per_node)]
            })
        self._cores_per_node = cores_per_node

        #self._capability = self._slots2caps(self._slots)
        self._capability = self._slots2free(self._slots)

        # keep a slot allocation history (short status), start with presumably
        # empty state now
        self._slot_history = list()
        self._slot_history.append (self._slot_status (short=True))


        # The available launch methods
        self._available_launch_methods = launch_methods

        self._p.update(
            {"_id": ObjectId(self._pilot_id)},
            {"$set": {"slothistory" : self._slot_history,
                      "capability"  : 0,
                      "slots"       : self._slots}}
            )

    # ------------------------------------------------------------------------
    #
    def _slots2free(self, slots):
        """Convert slots structure into a free core count
        """

        free_cores = 0
        for node in slots:
            free_cores += node['cores'].count(FREE)

        return free_cores


    # ------------------------------------------------------------------------
    #
    def _slots2caps(self, slots):
        """Convert slots structure into a capability structure.
        """

        all_caps_tuples = {}
        for node in slots:
            free_cores = node['cores'].count(FREE)
            # (Free_cores, Continuous, Single_Node) = Count
            cap_tuple = (free_cores, False, True)

            if cap_tuple in all_caps_tuples:
                all_caps_tuples[cap_tuple] += 1
            else:
                all_caps_tuples[cap_tuple] = 1


        # Convert to please the gods of json and mongodb
        all_caps_dict = []
        for caps_tuple in all_caps_tuples:
            free_cores, cont, single = cap_tuple
            count = all_caps_tuples[cap_tuple]
            cap_dict = {'free_cores': free_cores, 'continuous': cont, 'single_node': single, 'count': count}
            all_caps_dict.append(cap_dict)

        return all_caps_dict

    # ------------------------------------------------------------------------
    #
    def stop(self):
        """Terminates the process' main loop.
        """
        # AM: Why does this call exist?  It is never called....
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

                # See if there are commands for the worker!
                try:
                    command = self._command_queue.get_nowait()
                    if command[COMMAND_TYPE] == COMMAND_CANCEL_COMPUTE_UNIT:
                        self._cuids_to_cancel.append(command[COMMAND_ARG])
                    else:
                        raise Exception("Command %s not applicable in this context." % command[COMMAND_TYPE])
                except Queue.Empty:
                    # do nothing if we don't have any queued commands
                    pass

                try:
                    task = self._task_queue.get_nowait()

                    if task.mpi:
                        launch_method = self._available_launch_methods['mpi_launch_method']
                        launch_command = self._available_launch_methods['mpi_launch_command']
                        if not launch_command:
                            raise Exception("Can't launch MPI tasks without MPI launcher.")
                    else:
                        launch_method = self._available_launch_methods['task_launch_method']
                        launch_command = self._available_launch_methods['task_launch_command']

                    self._log.debug("Launching task with %s (%s)." % (
                        launch_method, launch_command))

                    # IBRUN (e.g. Stampede) requires continuous slots for multi core execution
                    # TODO: Dont have scattered scheduler yet, so test disabled.
                    if True: # launch_method in [LAUNCH_METHOD_IBRUN]:
                        req_cont = True
                    else:
                        req_cont = False

                    # First try to find all cores on a single node
                    task_slots = self._acquire_slots(task.numcores, single_node=True, continuous=req_cont)

                    # If that failed, and our launch method supports multiple nodes, try that
                    if task_slots is None and launch_method in MULTI_NODE_LAUNCH_METHODS:
                        task_slots = self._acquire_slots(task.numcores, single_node=False, continuous=req_cont)

                    # Check if we got results
                    if task_slots is None:
                        # No resources free, put back in queue
                        self._task_queue.put(task)
                    else:
                        idle = False
                        # We got an allocation go off and launch the process
                        task.slots = task_slots
                        self._launch_task(task, launch_method, launch_command)

                except Queue.Empty:
                    # do nothing if we don't have any queued tasks
                    pass

                # AM: idle seems always true here.  But somehow the logic causes
                # the pilot to spin very busily...  So, disable this idle check
                # for now...
                # idle &= self._check_running()
                self._check_running()

                # Check if something happened in this cycle, if not, zzzzz for a bit
                if idle:
                    self._log.debug("sleep now")
                    time.sleep(1)
                else :
                    self._log.debug("sleep not")

            # AM: we are done -- push slot history 
            # FIXME: this is never called, self._terminate is a farce :(
            # if  self._benchmark :
            #     self._p.update(
            #         {"_id": ObjectId(self._pilot_id)},
            #         {"$set": {"slothistory" : self._slot_history, 
            #                   "capability"  : 0,
            #                   "slots"       : self._slots}}
            #         )


        except Exception, ex:
            msg = ("Error in ExecWorker loop: %s", traceback.format_exc())
            pilot_FAILED(self._p, self._pilot_id, self._log, msg)
            return


    # ------------------------------------------------------------------------
    #
    def _slot_status(self, short=False):
        """Returns a multi-line string corresponding to slot status.
        """

        if short:
            slot_matrix = ""
            for slot in self._slots:
                slot_matrix += "|"
                for core in slot['cores']:
                    if core is FREE:
                        slot_matrix += "-"
                    else:
                        slot_matrix += "+"
            slot_matrix += "|"
            ts = datetime.datetime.utcnow()
            return {'timestamp' : ts, 'slotstate' : slot_matrix}

        else :
            slot_matrix = ""
            for slot in self._slots:
                slot_vector  = ""
                for core in slot['cores']:
                    if core is FREE:
                        slot_vector += " - "
                    else:
                        slot_vector += " X "
                slot_matrix += "%s: %s\n" % (slot['node'].ljust(24), slot_vector)
            return slot_matrix


    # ------------------------------------------------------------------------
    #
    # Returns a data structure in the form of:
    #
    #
    def _acquire_slots(self, cores_requested, single_node, continuous):

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
        def find_cores_cont(slot_cores, cores_requested, status):
            return find_sublist(slot_cores, [status for _ in range(cores_requested)])

        #
        # Find an available continuous slot within node boundaries.
        #
        def find_slots_single_cont(cores_requested):

            for slot in self._slots:
                slot_node = slot['node']
                slot_cores = slot['cores']

                slot_cores_offset = find_cores_cont(slot_cores, cores_requested, FREE)

                if slot_cores_offset is not None:
                    self._log.info('Node %s satisfies %d cores at offset %d' % (slot_node, cores_requested, slot_cores_offset))
                    return ['%s:%d' % (slot_node, core) for core in range(slot_cores_offset, slot_cores_offset + cores_requested)]

            return None

        #
        # Find an available continuous slot across node boundaries.
        #
        def find_slots_multi_cont(cores_requested):

            # Convenience aliases
            cores_per_node = self._cores_per_node
            all_slots = self._slots

            # Glue all slot core lists together
            all_slot_cores = [core for node in [node['cores'] for node in all_slots] for core in node]
          # self._log.debug("all_slot_cores: %s" % all_slot_cores)

            # Find the start of the first available region
            all_slots_first_core_offset = find_cores_cont(all_slot_cores, cores_requested, FREE)
            self._log.debug("all_slots_first_core_offset: %s" % all_slots_first_core_offset)
            if all_slots_first_core_offset is None:
                return None

            # Determine the first slot in the slot list
            first_slot_index = all_slots_first_core_offset / cores_per_node
            self._log.debug("first_slot_index: %s" % first_slot_index)
            # And the core offset within that node
            first_slot_core_offset = all_slots_first_core_offset % cores_per_node
            self._log.debug("first_slot_core_offset: %s" % first_slot_core_offset)

            # Note: We subtract one here, because counting starts at zero;
            #       Imagine a zero offset and a count of 1, the only core used would be core 0.
            #       TODO: Verify this claim :-)
            all_slots_last_core_offset = (first_slot_index * cores_per_node) + first_slot_core_offset + cores_requested - 1
            self._log.debug("all_slots_last_core_offset: %s" % all_slots_last_core_offset)
            last_slot_index = (all_slots_last_core_offset) / cores_per_node
            self._log.debug("last_slot_index: %s" % last_slot_index)
            last_slot_core_offset = all_slots_last_core_offset % cores_per_node
            self._log.debug("last_slot_core_offset: %s" % last_slot_core_offset)

            # Convenience aliases
            last_slot = self._slots[last_slot_index]
            self._log.debug("last_slot: %s" % last_slot)
            last_node = last_slot['node']
            self._log.debug("last_node: %s" % last_node)
            first_slot = self._slots[first_slot_index]
            self._log.debug("first_slot: %s" % first_slot)
            first_node = first_slot['node']
            self._log.debug("first_node: %s" % first_node)

            # Collect all node:core slots here
            task_slots = []

            # Add cores from first slot for this task
            # As this is a multi-node search, we can safely assume that we go from the offset all the way to the last core
            task_slots.extend(['%s:%d' % (first_node, core) for core in range(first_slot_core_offset, cores_per_node)])

            # Add all cores from "middle" slots
            for slot_index in range(first_slot_index+1, last_slot_index):
                slot_node = all_slots[slot_index]['node']
                task_slots.extend(['%s:%d' % (slot_node, core) for core in range(0, cores_per_node)])

            # Add the cores of the last slot
            task_slots.extend(['%s:%d' % (last_node, core) for core in range(0, last_slot_core_offset+1)])

            return task_slots

        #  End of inline functions, _acquire_slots() code begins after this
        #################################################################################

        #
        # Switch between searching for continuous or scattered slots
        #
        # Switch between searching for single or multi-node
        if single_node:
            if continuous:
                task_slots = find_slots_single_cont(cores_requested)
            else:
                raise NotImplementedError('No scattered single node scheduler implemented yet.')
        else:
            if continuous:
                task_slots = find_slots_multi_cont(cores_requested)
            else:
                raise NotImplementedError('No scattered multi node scheduler implemented yet.')

        if task_slots is not None:
            self._change_slot_states(task_slots, BUSY)

        return task_slots

    #
    # Change the reserved state of slots (FREE or BUSY)
    #
    # task_slots in the shape of:
    #
    #
    def _change_slot_states(self, task_slots, new_state):

        # Convenience alias
        all_slots = self._slots

      # logger.debug("change_slot_states: task slots: %s" % task_slots)

        for slot in task_slots:
          # logger.debug("change_slot_states: slot content: %s" % slot)
            # Get the node and the core part
            [slot_node, slot_core] = slot.split(':')
            # Find the entry in the the all_slots list
            slot_entry = (slot for slot in all_slots if slot["node"] == slot_node).next()
            # Change the state of the slot
            slot_entry['cores'][int(slot_core)] = new_state

        # something changed - write history!
        # AM: mongodb entries MUST NOT grow larger than 16MB, or chaos will
        # ensue.  We thus limit the slot history size to 4MB, to keep suffient
        # space for the actual operational data
        if  len(str(self._slot_history)) < 4 * 1024 * 1024 :
            self._slot_history.append (self._slot_status (short=True))
        else :
            # just replace the last entry with the current one.
            self._slot_history[-1]  =  self._slot_status (short=True)


    # ------------------------------------------------------------------------
    #
    def _launch_task(self, task, launch_method, launch_command):

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

        # Start a new subprocess to launch the task
        proc = _Process(
            task=task,
            all_slots=self._slots,
            cores_per_node=self._cores_per_node,
            launch_method=launch_method,
            launch_command=launch_command,
            logger=self._log)

        task.started=datetime.datetime.utcnow()
        task.state='Executing'
        task._proc = proc

        # Add to the list of monitored tasks
        self._running_tasks.append(task) # add task here?

        # Update to mongodb
        #
        # AM: FIXME: this mongodb update is effectively a (or rather multiple)
        # synchronous remote operation(s) in the exec worker main loop.  Even if
        # spanning multiple exec workers, we would still share the mongodb
        # channel, which would still need serialization.  This is rather
        # inefficient.  We should consider to use a async communication scheme.
        # For example, we could collect all messages for a second (but not
        # longer) and send those updates in a bulk.
        self._update_tasks(task)


    # ------------------------------------------------------------------------
    #
    def _check_running(self):

        idle = True

        # we update tasks in 'bulk' after each iteration.
        # all tasks that require DB updates are in update_tasks
        update_tasks = []
        finished_tasks = []

        for task in self._running_tasks:

            proc = task._proc
            rc = proc.poll()
            if rc is None:
                # subprocess is still running

                if task.uid in self._cuids_to_cancel:
                    proc.kill()
                    state = 'Canceled'
                    finished_tasks.append(task)
                else:
                    continue

            else:

                finished_tasks.append(task)

                # Make sure all stuff reached the spindles
                proc.close_and_flush_filehandles()

                # Convenience shortcut
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

                task.stdout_id=stdout_id
                task.stderr_id=stderr_id
                task.exit_code=rc

            task.finished=datetime.datetime.utcnow()
            task.state=state

            update_tasks.append(task)

            self._change_slot_states(task.slots, FREE)

        # update all the tasks that are marked for update.
        self._update_tasks(update_tasks)

        for e in finished_tasks:
            self._running_tasks.remove(e)

        # AM: why is idle always True?  Whats the point here?  Not to run too
        # fast? :P
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

        # Update capabilities
        #self._capability = self._slots2caps(self._slots)
        self._capability = self._slots2free(self._slots)

        # AM: FIXME: this at the moment pushes slot history whenever a task
        # state is updated...  This needs only to be done on ExecWorker
        # shutdown.  Well, alas, there is currently no way for it to find out
        # when it is shut down... Some quick and  superficial measurements 
        # though show no negative impact on agent performance.
        # AM: the capability publication cannot be delayed until shutdown
        # though...
        if  self._benchmark :
            self._p.update(
                {"_id": ObjectId(self._pilot_id)},
                {"$set": {"slothistory" : self._slot_history,
                          #"slots"       : self._slots,
                          "capability"  : self._capability
                         }
                }
                )

        if not isinstance(tasks, list):
            tasks = [tasks]
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
                 mongodb_url, mongodb_name, pilot_id, session_id, 
                 benchmark):
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

        self._benchmark  = benchmark

        mongo_client = pymongo.MongoClient(mongodb_url)
        mongo_db = mongo_client[mongodb_name]
        self._p = mongo_db["%s.p"  % session_id]
        self._w = mongo_db["%s.w"  % session_id]
        self._wm = mongo_db["%s.wm" % session_id]

        # the task queue holds the tasks that are pulled from the MongoDB
        # server. The ExecWorkers compete for the tasks in the queue. 
        self._task_queue = multiprocessing.Queue()

        # Channel for the Agent to communicate commands with the ExecWorker
        self._command_queue = multiprocessing.Queue()

        # we assign each node partition to a task execution worker
        self._exec_worker = ExecWorker(
            logger          = self._log,
            task_queue      = self._task_queue,
            command_queue   = self._command_queue,
            node_list       = self._exec_env.node_list,
            cores_per_node  = self._exec_env.cores_per_node,
            launch_methods  = self._exec_env.discovered_launch_methods,
            mongodb_url     = mongodb_url,
            mongodb_name    = mongodb_name,
            pilot_id        = pilot_id,
            session_id      = session_id,
            benchmark       = benchmark
        )
        self._exec_worker.start()
        self._log.info("Started up %s serving nodes %s", self._exec_worker, self._exec_env.node_list)

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
                      "nodes"          : self._exec_env.node_list,
                      "cores_per_node" : self._exec_env.cores_per_node,
                      "started"        : ts,
                      "capability"     : 0},
             "$push": {"statehistory": {"state": 'Active', "timestamp": ts}}
            })

        self._starttime = time.time()

        while True:

            try:

                # Check the workers periodically. If they have died, we 
                # exit as well. this can happen, e.g., if the worker 
                # process has caught a ctrl+C
                if self._exec_worker.is_alive() is False:
                    pilot_FAILED(self._p, self._pilot_id, self._log, "Execution worker %s died." % str(self._exec_worker))
                    return

                # Exit the main loop if terminate is set. 
                if self._terminate.isSet():
                    pilot_CANCELED(self._p, self._pilot_id, self._log, "Terminated (_terminate set).")
                    return

                # Make sure that we haven't exceeded the agent runtime. if 
                # we have, terminate. 
                if time.time() >= self._starttime + (int(self._runtime) * 60):
                    self._log.info("Agent has reached runtime limit of %s seconds." % str(int(self._runtime)*60))
                    pilot_DONE(self._p, self._pilot_id)
                    return

                # Try to get new tasks from the database. for this, we check the 
                # wu_queue of the pilot. if there are new entries, we get them,
                # get the actual pilot entries for them and remove them from 
                # the wu_queue.
                try:

                    # Check if there's a command waiting
                    retdoc = self._p.find_and_modify(
                                query={"_id":ObjectId(self._pilot_id)},
                                update={"$set":{COMMAND_FIELD: []}}, # Wipe content of array
                                fields=[COMMAND_FIELD]
                    )
                    self._log.info("retdoc: %s" % retdoc)

                    if retdoc:
                        commands = retdoc['commands']
                    else:
                        commands = []

                    for command in commands:
                        if command[COMMAND_TYPE] == COMMAND_CANCEL_PILOT:
                            self._log.info("Received Cancel Pilot command.")
                            pilot_CANCELED(self._p, self._pilot_id, self._log, "CANCEL received. Terminating.")
                            return
                        elif command[COMMAND_TYPE] == COMMAND_CANCEL_COMPUTE_UNIT:
                            self._log.info("Received Cancel Compute Unit command for: %s" % command[COMMAND_ARG])
                            # Put it on the command queue of the ExecWorker
                            self._command_queue.put(command)
                        elif command[COMMAND_TYPE] == COMMAND_KEEP_ALIVE:
                            self._log.info("Received KeepAlive command.")
                        else:
                            raise Exception("Received unknown command: %s with arg: %s." % (command[COMMAND_TYPE], command[COMMAND_ARG]))

                    # Check if there are work units waiting for execution,
                    # and log that we pulled it.
                    ts = datetime.datetime.utcnow()
                    wu_cursor = self._w.find_and_modify(
                        query={"pilot" : self._pilot_id,
                               "state" : "PendingExecution"},
                        update={"$set" : {"state": "Scheduling"},
                                "$push": {"statehistory": {"state": "Scheduling", "timestamp": ts}}}
                    )

                    # There are new work units in the wu_queue on the database.
                    # Get the corresponding wu entries.
                    if wu_cursor is not None:
                        if not isinstance(wu_cursor, list):
                            wu_cursor = [wu_cursor]

                        for wu in wu_cursor:
                            # Create new task objects and put them into the task queue
                            w_uid = str(wu["_id"])
                            self._log.info("Found new tasks in pilot queue: %s" % w_uid)

                            task_dir_name = "%s/unit-%s" % (self._workdir, str(wu["_id"]))

                            task = Task(uid         = w_uid,
                                        executable  = wu["description"]["executable"],
                                        arguments   = wu["description"]["arguments"],
                                        environment = wu["description"]["environment"],
                                        numcores    = wu["description"]["cores"],
                                        mpi         = wu["description"]["mpi"],
                                        pre_exec    = wu["description"]["pre_exec"],
                                        post_exec   = wu["description"]["post_exec"],
                                        workdir     = task_dir_name,
                                        stdout      = task_dir_name+'/STDOUT',
                                        stderr      = task_dir_name+'/STDERR',
                                        output_data = wu["description"]["output_data"])

                            task.state = 'Scheduling'
                            self._task_queue.put(task)

                except Exception, ex:
                    raise

                time.sleep(1)

            except Exception, ex:
                # If we arrive here, there was an exception in the main loop.
                pilot_FAILED(self._p, self._pilot_id, self._log, 
                    "ERROR in agent main loop: %s. %s" % (str(ex), traceback.format_exc()))
                return

        # MAIN LOOP TERMINATED
        return

#-----------------------------------------------------------------------------
#
class _Process(subprocess.Popen):

    #-------------------------------------------------------------------------
    #
    def __init__(self, task, all_slots, cores_per_node, launch_method,
                 launch_command, logger):

        self._task = task
        self._log  = logger

        launch_script = tempfile.NamedTemporaryFile(prefix='radical_pilot_cu_launch_script-', dir=task.workdir, suffix=".sh", delete=False)
        self._log.debug('Created launch_script: %s' % launch_script.name)
        st = os.stat(launch_script.name)
        os.chmod(launch_script.name, st.st_mode | stat.S_IEXEC)
        launch_script.write('#!/bin/bash -l\n')
        launch_script.write('cd %s\n' % task.workdir)

        # Before the Big Bang there was nothing
        pre_exec = task.pre_exec
        pre_exec_string = ''
        if pre_exec:
            if not isinstance(pre_exec, list):
                pre_exec = [pre_exec]
            for bb in pre_exec:
                pre_exec_string += "%s\n" % bb

        # After the universe dies the infrared death, there will be nothing
        post_exec = task.post_exec
        post_exec_string = ''
        if post_exec:
            if not isinstance(post_exec, list):
                post_exec = [post_exec]
            for bb in post_exec:
                post_exec_string += "%s\n" % bb

        # executable and arguments
        if task.executable is not None:
            task_exec_string = task.executable # TODO: Do we allow $ENV/bin/program constructs here?
        else:
            raise Exception("No executable specified!") # TODO: This should be catched earlier problaby
        if task.arguments is not None:
            for arg in task.arguments:
                arg = arg.replace ('"', '\\"') # Escape all double quotes
                if  arg[0] == arg[-1] == "'" : # If a string is between outer single quotes,
                    task_exec_string += ' %s' % arg # ... pass it as is.
                else :
                    task_exec_string += ' "%s"' % arg # Otherwise return between double quotes.

        # Create string for environment variable setting
        env_string = ''
        if task.environment is not None and len(task.environment.keys()):
            env_string += 'export'
            for key in task.environment:
                env_string += ' %s=%s' % (key, task.environment[key])


        # Based on the launch method we use different, well, launch methods
        # to launch the task. just on the shell, via mpirun, ssh, ibrun or aprun
        if launch_method == LAUNCH_METHOD_LOCAL:
            launch_script.write('%s\n'    % pre_exec_string)
            launch_script.write('%s\n'    % env_string)
            launch_script.write('%s\n'    % task_exec_string)
            launch_script.write('%s\n'    % post_exec_string)

            cmdline = launch_script.name

        elif launch_method == LAUNCH_METHOD_MPIRUN:
            # Construct the hosts_string
            hosts_string = ''
            for slot in task.slots:
                host = slot.split(':')[0]
                hosts_string += '%s,' % host

            mpirun_command = "%s -np %s -host %s" % (launch_command,
                                                     task.numcores, hosts_string)

            launch_script.write('%s\n'    % pre_exec_string)
            launch_script.write('%s\n'    % env_string)
            launch_script.write('%s %s\n' % (mpirun_command, task_exec_string))
            launch_script.write('%s\n'    % post_exec_string)

            cmdline = launch_script.name

        elif launch_method == LAUNCH_METHOD_MPIEXEC:
            # Construct the hosts_string
            hosts_string = ''
            for slot in task.slots:
                host = slot.split(':')[0]
                hosts_string += '%s,' % host

            mpiexec_command = "%s -n %s -hosts %s" % (launch_command, task.numcores, hosts_string)

            launch_script.write('%s\n'    % pre_exec_string)
            launch_script.write('%s\n'    % env_string)
            launch_script.write('%s %s\n' % (mpiexec_command, task_exec_string))
            launch_script.write('%s\n'    % post_exec_string)

            cmdline = launch_script.name

        elif launch_method == LAUNCH_METHOD_APRUN:
            
            aprun_command = "%s -n %s" % (launch_command, task.numcores)

            launch_script.write('%s\n'    % pre_exec_string)
            launch_script.write('%s\n'    % env_string)
            launch_script.write('%s %s\n' % (aprun_command, task_exec_string))
            launch_script.write('%s\n' % post_exec_string)

            cmdline = launch_script.name

        elif launch_method == LAUNCH_METHOD_IBRUN:
            # NOTE: Don't think that with IBRUN it is possible to have
            # processes != cores ...

            first_slot = task.slots[0]
            # Get the host and the core part
            [first_slot_host, first_slot_core] = first_slot.split(':')
            # Find the entry in the the all_slots list based on the host
            slot_entry = (slot for slot in all_slots if slot["node"] == first_slot_host).next()
            # Transform it into an index in to the all_slots list
            all_slots_slot_index = all_slots.index(slot_entry)

            # TODO: This assumes all hosts have the same number of cores
            ibrun_offset = all_slots_slot_index * cores_per_node + int(first_slot_core)
            ibrun_command = "%s -n %s -o %d" % \
                            (launch_command, task.numcores,
                             ibrun_offset)

            # Build launch script
            launch_script.write('%s\n'    % pre_exec_string)
            launch_script.write('%s\n'    % env_string)
            launch_script.write('%s %s\n' % (ibrun_command, task_exec_string))
            launch_script.write('%s\n'    % post_exec_string)

            cmdline = launch_script.name

        elif launch_method == LAUNCH_METHOD_POE:

            # Count slots per host in provided slots description.
            hosts = {}
            for slot in task.slots:
                host = slot.split(':')[0]
                if host not in hosts:
                    hosts[host] = 1
                else:
                    hosts[host] += 1

            # Create string with format: "hostX N host
            hosts_string = ''
            for host in hosts:
                hosts_string += '%s %d ' % (host, hosts[host])

            # Override the LSB_MCPU_HOSTS env variable as this is set by
            # default to the size of the whole pilot.
            poe_command = 'LSB_MCPU_HOSTS="%s" %s' % (
                hosts_string, launch_command)

            # Continue to build launch script
            launch_script.write('%s\n'    % pre_exec_string)
            launch_script.write('%s\n'    % env_string)
            launch_script.write('%s %s\n' % (poe_command, task_exec_string))
            launch_script.write('%s\n'    % post_exec_string)

            # Command line to execute launch script
            cmdline = launch_script.name

        elif launch_method == LAUNCH_METHOD_SSH:
            host = task.slots[0].split(':')[0] # Get the host of the first entry in the acquired slot

            # Continue to build launch script
            launch_script.write('%s\n'    % pre_exec_string)
            launch_script.write('%s\n'    % env_string)
            launch_script.write('%s\n'    % task_exec_string)
            launch_script.write('%s\n'    % post_exec_string)

            # Command line to execute launch script
            cmdline = '%s %s %s' % (launch_command, host, launch_script.name)

        else:
            raise NotImplementedError("Launch method %s not implemented in executor!" % launch_method)

        # We are done writing to the launch script, its ready for execution now.
        launch_script.close()

        self.stdout_filename = task.stdout
        self._stdout_file_h  = open(self.stdout_filename, "w")

        self.stderr_filename = task.stderr
        self._stderr_file_h  = open(self.stderr_filename, "w")

        self._log.info("Launching task %s via %s in %s" % (task.uid, cmdline, task.workdir))

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

    parser.add_option('-b', '--benchmark',
                      metavar='BENCHMARK',
                      type='int',
                      dest='benchmark',
                      help='Enables timing for benchmarking purposes.')

    parser.add_option('-c', '--cores',
                      metavar='CORES',
                      dest='cores',
                      type='int',
                      help='Specifies the number of cores to allocate.')

    parser.add_option('-d', '--debug',
                      metavar='DEBUG',
                      dest='debug_level',
                      type='int',
                      help='The DEBUG level for the agent.')

    parser.add_option('-j', '--task-launch-method',
                      metavar='METHOD',
                      dest='task_launch_method',
                      help='Specifies the task launch method.')

    parser.add_option('-k', '--mpi-launch-method',
                      metavar='METHOD',
                      dest='mpi_launch_method',
                      help='Specifies the MPI launch method.')

    parser.add_option('-l', '--lrms',
                      metavar='LRMS',
                      dest='lrms',
                      help='Specifies the LRMS type.')

    parser.add_option('-m', '--mongodb-url',
                      metavar='URL',
                      dest='mongodb_url',
                      help='Specifies the MongoDB Url.')

    parser.add_option('-n', '--database-name',
                      metavar='URL',
                      dest='database_name',
                      help='Specifies the MongoDB database name.')

    parser.add_option('-p', '--pilot-id',
                      metavar='PID',
                      dest='pilot_id',
                      help='Specifies the Pilot ID.')

    parser.add_option('-s', '--session-id',
                      metavar='SID',
                      dest='session_id',
                      help='Specifies the Session ID.')

    parser.add_option('-t', '--runtime',
                      metavar='RUNTIME',
                      dest='runtime',
                      help='Specifies the agent runtime in minutes.')

    parser.add_option('-v', '--version',
                      metavar='VERSION ',
                      dest='package_version',
                      help='The RADICAL-Pilot package version.')

    parser.add_option('-w', '--workdir',
                      metavar='DIRECTORY',
                      dest='workdir',
                      help='Specifies the base (working) directory for the agent. [default: %default]',
                      default='.')

    # parse the whole shebang
    (options, args) = parser.parse_args()

    if options.mongodb_url is None:
        parser.error("You must define MongoDB URL (-m/--mongodb-url). Try --help for help.")
    if options.database_name is None:
        parser.error("You must define a database name (-n/--database-name). Try --help for help.")
    if options.session_id is None:
        parser.error("You must define a session id (-s/--session-id). Try --help for help.")
    if options.pilot_id is None:
        parser.error("You must define a pilot id (-p/--pilot-id). Try --help for help.")
    if options.cores is None:
        parser.error("You must define the number of cores (-c/--cores). Try --help for help.")
    if options.runtime is None:
        parser.error("You must define the agent runtime (-t/--runtime). Try --help for help.")
    if options.package_version is None:
        parser.error("You must pass the RADICAL-Pilot package version (-v/--version). Try --help for help.")
    if options.debug_level is None:
        parser.error("You must pass the DEBUG level (-d/--debug). Try --help for help.")
    if options.lrms is None:
        parser.error("You must pass the LRMS (-l/--lrms). Try --help for help.")

    return options

#-----------------------------------------------------------------------------
#
if __name__ == "__main__":

    # parse command line options
    options = parse_commandline()

    # configure the agent logger
    logger = logging.getLogger('radical.pilot.agent')
    logger.setLevel(options.debug_level)
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
        mongo_w      = mongo_db["%s.w"  % options.session_id]  # AM: never used
        mongo_wm     = mongo_db["%s.wm" % options.session_id]  # AM: never used

    except Exception, ex:
        logger.error("Couldn't establish database connection: %s" % str(ex))
        sys.exit(1)

    #--------------------------------------------------------------------------
    # Some signal handling magic
    def sigint_handler(signal, frame):
        msg = 'Caught SIGINT. EXITING.'
        pilot_FAILED(mongo_p, options.pilot_id, logger, msg)
        sys.exit (1)
    signal.signal(signal.SIGINT, sigint_handler)

    def sigalarm_handler(signal, frame):
        msg = 'Caught SIGALRM (Walltime limit reached?). EXITING'
        pilot_FAILED(mongo_p, options.pilot_id, logger, msg)
        sys.exit (1)
    signal.signal(signal.SIGALRM, sigalarm_handler)

    #--------------------------------------------------------------------------
    # Discover environment, nodes, cores, mpi, etc.
    try:
        exec_env = ExecutionEnvironment(
            logger=logger,
            lrms=options.lrms,
            requested_cores=options.cores,
            task_launch_method=options.task_launch_method,
            mpi_launch_method=options.mpi_launch_method
        )
        if exec_env is None:
            msg = "Couldn't set up execution environment."
            logger.error(msg)
            pilot_FAILED(mongo_p, options.pilot_id, logger, msg)
            sys.exit (1)

    except Exception, ex:
        msg = "Error setting up execution environment: %s" % str(ex)
        logger.error(msg)
        pilot_FAILED(mongo_p, options.pilot_id, logger, msg)
        sys.exit (1)

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
                      benchmark=options.benchmark)

        # AM: why is this done in a thread?  This thread blocks anyway, so it
        # could just *do* the things.  That would avoid those global vars and
        # would allow for cleaner shutdown.
        agent.start()
        agent.join()

    except Exception, ex:
        msg = "Error running agent: %s" % str(ex)
        logger.error(msg)
        pilot_FAILED(mongo_p, options.pilot_id, logger, msg)
        agent.stop()
        sys.exit (1)

    except SystemExit:

        logger.error("Caught keyboard interrupt. EXITING")
        agent.stop()

