
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import stat
import math
import time
import copy
import queue
import signal
import atexit
import threading as mt
import subprocess

import radical.utils as ru

from ...  import utils     as rpu
from ...  import states    as rps
from ...  import constants as rpc

from ..   import LaunchMethod

from .base           import AgentExecutingComponent


# ------------------------------------------------------------------------------
# ensure tasks are killed on termination
_pids = list()


def _kill():
    for pid in _pids:
        try   : os.killpg(pid, signal.SIGTERM)
        except: pass


atexit.register(_kill)
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
#
class MPIFUNCS(AgentExecutingComponent) :

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        AgentExecutingComponent.__init__ (self, cfg, session)

        self._collector = None
        self._terminate = mt.Event()


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        self._pwd = os.getcwd()
        self.gtod = "%s/gtod" % self._pwd

        self.register_input(rps.AGENT_EXECUTING_PENDING,
                            rpc.AGENT_EXECUTING_QUEUE, self.work)

        self.register_output(rps.AGENT_STAGING_OUTPUT_PENDING,
                             rpc.AGENT_STAGING_OUTPUT_QUEUE)

        self.register_publisher (rpc.AGENT_UNSCHEDULE_PUBSUB)
        self.register_subscriber(rpc.CONTROL_PUBSUB, self.command_cb)

        self.req_cfg = ru.read_json('funcs_req_queue.cfg')
        self.res_cfg = ru.read_json('funcs_res_queue.cfg')

        self._req_queue = ru.zmq.Putter('funcs_req_queue', self.req_cfg['put'])
        self._res_queue = ru.zmq.Getter('funcs_res_queue', self.res_cfg['get'])

        self._cancel_lock     = ru.RLock()
        self._tasks_to_cancel = list()
        self._tasks_to_watch  = list()
        self._watch_queue     = queue.Queue ()

        self._pid = self._cfg['pid']

        # run watcher thread
        self._collector = mt.Thread(target=self._collect)
        self._collector.daemon = True
        self._collector.start()

        # we need to launch the executors on all nodes, and use the
        # agent_launcher for that
        self._launcher = LaunchMethod.create(
                name    = self._cfg.get('agent_launch_method'),
                cfg     = self._cfg,
                session = self._session)

        # get address of control pubsub
        fname   = '%s/%s.cfg' % (self._cfg.path, rpc.CONTROL_PUBSUB)
        self.ctl_cfg = ru.read_json(fname)

        # now schedule the executors and start
        self._schedule_and_start_mpi_executor()
        # Since we know that every task is a multinode, we take half of the nodes and
        # spawn executros on them, then the rest of the nodes (the other half) 
        # will be utilized by the mpi_workers inside every executor.
        # So the mpi worker will see 2 nodes for every task and occupy it


    # --------------------------------------------------------------------------
    #
    def command_cb(self, topic, msg):

        self._log.info('command_cb [%s]: %s', topic, msg)

        cmd = msg['cmd']
        arg = msg['arg']

        if cmd == 'cancel_tasks':

            self._log.info("cancel_tasks command (%s)" % arg)
            with self._cancel_lock:
                self._tasks_to_cancel.extend(arg['uids'])

        return True


    # --------------------------------------------------------------------------
    #
    def _spawn(self, launcher, funcs):

        # NOTE: see documentation of funcs['sandbox'] semantics in the Task
        #       class definition.
        sandbox = '%s/%s'     % (self._pwd, funcs['uid'])
        fname   = '%s/%s.sh'  % (sandbox,   funcs['uid'])
        cfgname = '%s/%s.cfg' % (sandbox,   funcs['uid'])
        descr   = funcs['description']

        rpu.rec_makedir(sandbox)
        ru.write_json(funcs.get('cfg'), cfgname)

        launch_cmd, hop_cmd = launcher.construct_command(funcs, fname)

        if hop_cmd : cmdline = hop_cmd
        else       : cmdline = fname

        with open(fname, "w") as fout:

            fout.write('#!/bin/sh\n\n')

            # Create string for environment variable setting
            fout.write('export RP_SESSION_ID="%s"\n'      % self._cfg['sid'])
            fout.write('export RP_PILOT_ID="%s"\n'        % self._cfg['pid'])
            fout.write('export RP_AGENT_ID="%s"\n'        % self._cfg['aid'])
            fout.write('export RP_SPAWNER_ID="%s"\n'      % self.uid)
            fout.write('export RP_FUNCS_ID="%s"\n'        % funcs['uid'])
            fout.write('export RP_GTOD="%s"\n'            % self.gtod)
            fout.write('export RP_TMP="%s"\n'             % self._task_tmp)
            fout.write('export RP_PILOT_SANDBOX="%s"\n'   % self._pwd)
            fout.write('export RP_PILOT_STAGING="%s"\n'   % self._pwd)

            if self._cfg['resource'].startswith('local'):
                fout.write('export PILOT_SCHEMA="%s"\n'       % 'LOCAL')
            else:
                fout.write('export PILOT_SCHEMA="%s"\n'       % 'REMOTE')
                fout.write('export SLURM_NODELIST="%s"\n'     % os.environ['SLURM_NODELIST'])
                fout.write('export SLURM_CPUS_ON_NODE="%s"\n' % os.environ['SLURM_CPUS_ON_NODE'])

            if self._prof.enabled:
                fout.write('export RP_PROF="%s/%s.prof"\n' % (sandbox, funcs['uid']))

            else:
                fout.write('unset  RP_PROF\n')

            # also add any env vars requested in the task description
            if descr.get('environment', []):
                for key,val in descr['environment'].items():
                    fout.write('export "%s=%s"\n' % (key, val))

            fout.write('\n%s\n\n' % launch_cmd)
            fout.write('RETVAL=$?\n')
            fout.write("exit $RETVAL\n")

        # done writing to launch script, get it ready for execution.
        st = os.stat(fname)
        os.chmod(fname, st.st_mode | stat.S_IEXEC)

        fout = open('%s/%s.out' % (sandbox, funcs['uid']), "w")
        ferr = open('%s/%s.err' % (sandbox, funcs['uid']), "w")

        self._prof.prof('exec_start', uid=funcs['uid'])
        # we really want to use preexec_fn:
        # pylint: disable=W1509
        funcs['proc'] = subprocess.Popen(args       = cmdline,
                                         executable = None,
                                         stdin      = None,
                                         stdout     = fout,
                                         stderr     = ferr,
                                         preexec_fn = os.setsid,
                                         close_fds  = True,
                                         shell      = True,
                                         cwd        = sandbox)

        self._prof.prof('exec_ok', uid=funcs['uid'])
        _pids.append(funcs['proc'].pid)


    # ---------------------------------------------------------------------------
    #
    def _schedule_and_start_mpi_executor(self):

        '''
        This function suppose to make a decision regarding how many mpi_executors to
        start based on a few information like:

        cores_per_task: number of cores required for every MPI task
        '''
        rp_cfg_cpn = 0
        slurm_cpn  = 0
        if self._cfg['resource'].startswith('local'):
            rp_cfg_cpn = self._cfg['cores_per_node']
        else:
            slurm_cpn  = int(os.environ['SLURM_CPUS_ON_NODE'])

        node_list            = copy.deepcopy(self._cfg['rm_info']['node_list'])
        cores_per_node       = slurm_cpn if rp_cfg_cpn == 0 else rp_cfg_cpn
        cores_per_pilot      = self._cfg['cores']
        cores_per_executor   = self._cfg['max_task_cores']

        ve  = os.environ.get('VIRTUAL_ENV',  '')


        def _get_node_maps(cores, threads_per_proc):

            '''
            For a given set of cores and gpus, chunk them into sub-sets so that each
            sub-set can host one application process and all threads of that
            process. 

            example:
                cores : 8 and we convert it to ==> [1, 2, 3, 4, 5, 6, 7, 8]
                tpp   : 4
                result: [[1, 2, 3, 4], [5, 6, 7, 8]]]
            For more details, see top level comment of `agent/scheduler/base.py`.
            '''
            cores    = list(range(cores))
            core_map = list()

            # make sure the core sets can host the requested number of threads
            assert(not len(cores) % threads_per_proc)
            n_procs =  int(len(cores) / threads_per_proc)

            idx = 0
            for _ in range(n_procs):
                p_map = list()
                for _ in range(threads_per_proc):
                    p_map.append(cores[idx])
                    idx += 1
                core_map.append(p_map)

            assert(idx == len(cores)), \
                ('%s -- %s -- %s -- %s' % idx, len(cores), cores, n_procs)

            return core_map

        def _start_mpi_executor(cores_per_executor, slots, executors_to_start_id):

            exe = ru.which('radical-pilot-agent-funcs2-mpi')
            if not exe:
                exe = '%s/rp_install/bin/radical-pilot-agent-funcs2-mpi' % self._pwd

            uid   = 'func_exec.%04d' % executors_to_start_id
            pwd   = '%s/%s' % (self._pwd, uid)
            mpi_executor = {'uid'        : uid,
                            'description': {'executable'   : exe,
                                            'arguments'    : [pwd, ve],
                                            'cpu_processes': cores_per_executor,
                                            'environment'  : [],
                                            },
                            'task_sandbox_path'            : pwd,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
                            'slots'      : slots,
                            'cfg'        : {'req_get'      : self.req_cfg['get'],
                                            'res_put'      : self.res_cfg['put'],
                                            'ctrl'         : self.ctl_cfg['sub']
                                            }
                            }
            self._spawn(self._launcher, mpi_executor)

        def _find_slots(cores_per_node, cores_per_executor):

            slots    = {}
            nodes    = []
            core_map = _get_node_maps(cores_per_node, 1)

            # 1 slot (slot = node) per executor
            if cores_per_executor == cores_per_node:

                slot = {'name'     : node_list[0][0],
                        'uid'      : node_list[0][1],
                        'core_map' : core_map,
                        'gpus'     : []}
                nodes.append(slot)
                slots['nodes'] = nodes
                node_list.pop(0)

            # If this is true then we can fit more than one executor per node
            if cores_per_executor < cores_per_node:

                # Calculate how many executors to fit in a single node and limit it to 2 per node
                # 
                executors_per_node = math.floor(cores_per_node / cores_per_executor)

                if executors_per_node > 2:
                    executors_per_node = 2
                else:
                    pass

                for executor in range(executors_per_node):

                    slots = {'nodes':[{'name'     : node_list[0][0],
                                       'uid'      : node_list[0][1],
                                       'core_map' : core_map[:len(core_map) // executors_per_node],
                                       'gpus'     : []
                                       }]}
                self._log.debug(node_list)
                node_list.pop(0)
                self._log.debug(node_list)

            # more than 1 slot (node) per executor
            if cores_per_executor > cores_per_node:

                nodes_per_executor = math.ceil(cores_per_executor / cores_per_node)
                if nodes_per_executor % len(node_list) == 0:

                    for i in range(nodes_per_executor):
                        slot = {'name' : node_list[i][0],
                                'uid'  : node_list[i][1],
                                'core_map' : core_map,
                                'gpus'     : []}
                        nodes.append(slot)
                    slots['nodes'] = nodes
                else:
                    for i in range(math.floor(len(node_list) / nodes_per_executor)):

                        slot = {'name' : node_list[i][0],
                                'uid'  : node_list[i][1],
                                'core_map' : core_map,
                                'gpus'     : []}
                        nodes.append(slot)
                        node_list.pop(0)
                    slots['nodes'] = nodes
            self._log.debug(slots)
            return slots

        
        # Case 1 (if the mpi executor requires only 1 core, 
        # then simply it does not make sense to use MPI acort!)
        if cores_per_executor <= 1:
            raise ValueError('can not start mpi executor with size %d cores' % (cores_per_executor))

        # Case 2 (if the pilot cores are less than the required by the executor (core_per_executor))
        if cores_per_executor > cores_per_pilot:
            raise ValueError('mpi executor of size %d cores can not'
                             'fit in %d avilable cores' % (cores_per_executor, cores_per_pilot))

        # Case 3 (If we can fit a single executor per node, then do it for every node!)
        if cores_per_executor == cores_per_node:
            for executors_to_start_id in range(len(node_list)):
                self._prof.prof('exec_warmup_start',
                                uid = 'func_exec.%04d' % executors_to_start_id)
                slots = _find_slots(cores_per_node, cores_per_executor)
                _start_mpi_executor(cores_per_executor, slots, executors_to_start_id)
                self._prof.prof('exec_warmup_stop',
                                uid = 'func_exec.%04d' % executors_to_start_id)

        # Case 4 fit more than one executor (limit is 2) per node!
        if cores_per_executor < cores_per_node:
            executors_per_node = math.floor(cores_per_node / cores_per_executor)
            if executors_per_node > 2:
                executors_per_node = 2
            else:
                pass

            for node in range(len(node_list)):
                slots = _find_slots(cores_per_node, cores_per_executor)
                # We limit the number of executors per node to 2
                for executors_to_start_id in range(executors_per_node):
                    self._prof.prof('exec_warmup_start',
                                    uid = 'func_exec.%04d' % executors_to_start_id)
                    _start_mpi_executor(cores_per_executor, slots, executors_to_start_id)
                    self._prof.prof('exec_warmup_stop',
                                    uid = 'func_exec.%04d' % executors_to_start_id)

        # Case 5 (If we can not fit one executor per node, 
        # then we need more than one node per executor)
        if cores_per_executor > cores_per_node:
            # Case 5.1 Let's find out how many nodes our executor requires
            nodes_per_executor = math.ceil(cores_per_executor / cores_per_node)
            if nodes_per_executor % len(node_list) == 0:
                executors_to_start = len(node_list) // nodes_per_executor
                for executors_to_start_id in range(executors_to_start):
                    self._prof.prof('exec_warmup_start',
                                    uid = 'func_exec.%04d' % executors_to_start_id)
                    slots = _find_slots(cores_per_node, cores_per_executor)
                    _start_mpi_executor(cores_per_executor, slots, executors_to_start_id)
                    self._prof.prof('exec_warmup_stop',
                                    uid = 'func_exec.%04d' % executors_to_start_id)
            else:
                executors_to_start = math.floor(len(node_list) / nodes_per_executor)
                for executors_to_start_id in range(executors_to_start):
                    self._prof.prof('exec_warmup_start',
                                    uid = 'func_exec.%04d' % executors_to_start_id)
                    slots = _find_slots(cores_per_node, cores_per_executor)
                    _start_mpi_executor(cores_per_executor, slots, executors_to_start_id)
                    self._prof.prof('exec_warmup_stop',
                                    uid = 'func_exec.%04d' % executors_to_start_id)

    # --------------------------------------------------------------------------
    #
    def work(self, tasks):

        if not isinstance(tasks, list):
            tasks = [tasks]

        self.advance(tasks, rps.AGENT_EXECUTING, publish=True, push=False)

        for task in tasks:
            assert(task['description']['cpu_process_type'] == 'MPI_FUNC')
            self._req_queue.put(task)


    # --------------------------------------------------------------------------
    #
    def _collect(self):

        while not self._terminate.is_set():

            # pull tasks from "funcs_out_queue"
            tasks = self._res_queue.get_nowait(1000)

            if tasks:
                for task in tasks:
                    task['target_state']      = task['state']
                    task['pilot']             = self._pid

                    self._log.debug('got %s [%s] [%s] [%s]',
                                   task['uid'],    task['state'],
                                   task['stdout'], task['stderr'])

                self.advance(tasks, rps.AGENT_STAGING_OUTPUT_PENDING,
                             publish=True, push=True)
            else:
                time.sleep(0.1)


# ------------------------------------------------------------------------------

