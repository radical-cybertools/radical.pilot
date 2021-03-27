
__copyright__ = 'Copyright 2020-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import collections
import math
import os
import signal
import time

import subprocess    as mp
import threading     as mt

import radical.utils as ru

from .base import LaunchMethod

PTL_MAX_MSG_SIZE = 1024 * 1024 * 1024 * 1

DVM_URI_FILE_TPL   = '%(base_path)s/prrte.%(dvm_id)03d.uri'
DVM_HOSTS_FILE_TPL = '%(base_path)s/prrte.%(dvm_id)03d.hosts'


# ------------------------------------------------------------------------------
#
class PRTE2(LaunchMethod):

    """
    PMIx Reference Run Time Environment v2
    (https://github.com/openpmix/prrte)
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, cfg, session):

        LaunchMethod.__init__(self, name, cfg, session)

        # We remove all PRUN related environment variables from the launcher
        # environment, so that we can use PRUN for both launch of the
        # (sub-)agent and Task execution.
        self.env_removables.extend(['OMPI_', 'OPAL_', 'PMIX_'])

        self._verbose = bool(os.environ.get('RADICAL_PILOT_PRUN_VERBOSE'))

    # --------------------------------------------------------------------------
    #
    def get_rank_cmd(self):

        return "echo $PMIX_RANK"

    # --------------------------------------------------------------------------
    #
    def _configure(self):

        if not ru.which('prun'):
            raise Exception('prun command not found')

        self.launch_command = 'prun'

    # --------------------------------------------------------------------------
    #
    @classmethod
    def rm_config_hook(cls, name, cfg, rm, log, profiler):

        prte_cmd = ru.which('prte')
        if not prte_cmd:
            raise Exception('prte command not found')

        prte_prefix = os.environ.get('UMS_OMPIX_PRRTE_DIR') or \
                      os.environ.get('PRRTE_DIR', '')

        prte_info = {}
        # get OpenRTE/PRTE version
        out, _, _ = ru.sh_callout('prte_info | grep "RTE"', shell=True)
        for line in out.split('\n'):

            line = line.strip()

            if not prte_info.get('name'):
                if 'Open RTE' in line:
                    prte_info['name'] = 'Open RTE'
                elif 'PRTE' in line:
                    prte_info['name'] = 'PRTE'

            if 'RTE:' in line:
                prte_info['version'] = line.split(':')[1].strip()
            elif 'RTE repo revision:' in line:
                prte_info['version_detail'] = line.split(':')[1].strip()

        if prte_info.get('name'):
            log.info('version of %s: %s [%s]',
                     prte_info['name'],
                     prte_info.get('version'),
                     prte_info.get('version_detail'))
        else:
            log.info('version of OpenRTE/PRTE not determined')

        # get `dvm_count` from resource config
        # FIXME: another option is to introduce `nodes_per_dvm` in config
        #        dvm_count = math.ceil(len(rm.node_list) / nodes_per_dvm)
        dvm_count     = cfg['resource_cfg'].get('dvm_count') or 1
        nodes_per_dvm = math.ceil(len(rm.node_list) / dvm_count)

        # additional info to form prrte related files (uri-/hosts-file)
        dvm_file_info = {'base_path': os.getcwd()}

        # ----------------------------------------------------------------------
        def _watch_dvm(dvm_process, dvm_id, dvm_ready_flag):

            log.info('prte-%s watcher started', dvm_id)

            retval = dvm_process.poll()
            while retval is None:
                line = dvm_process.stdout.readline().strip()
                if line:
                    log.debug('prte-%s output: %s', dvm_id, line)

                    # final confirmation that DVM has started successfully
                    if not dvm_ready_flag.is_set() and 'DVM ready' in str(line):
                        dvm_ready_flag.set()
                        log.info('prte-%s is ready', dvm_id)
                        profiler.prof(event='dvm_ready',
                                      uid=cfg['pid'],
                                      msg='dvm_id=%s' % dvm_id)

                else:
                    time.sleep(1.)

            if retval != 0:
                # send a kill signal to the main thread.
                # We know that Python and threading are likely not to play well
                # with signals - but this is an exceptional case, and not part
                # of the standard termination sequence. If the signal is
                # swallowed, the next prun-call will trigger termination anyway.
                os.kill(os.getpid(), signal.SIGKILL)
                raise RuntimeError('PRTE DVM died (dvm_id=%s)' % dvm_id)

            log.info('prte-%s watcher stopped', dvm_id)

        # ----------------------------------------------------------------------
        def _start_dvm(dvm_id, dvm_size, dvm_ready_flag):

            file_info = dict(dvm_file_info)
            file_info['dvm_id'] = dvm_id

            prte  = '%s'               % prte_cmd
            prte += ' --prefix %s'     % prte_prefix
            prte += ' --report-uri %s' % DVM_URI_FILE_TPL   % file_info
            prte += ' --hostfile %s'   % DVM_HOSTS_FILE_TPL % file_info

            # large tasks imply large message sizes, we need to account for that
            # FIXME: need to derive the message size from DVM size - smaller
            #        DVMs will never need large messages, as they can't run
            #        large tasks
            prte += ' --pmixmca ptl_base_max_msg_size %d'  % PTL_MAX_MSG_SIZE
            prte += ' --prtemca routed_radix %d'           % dvm_size
            # 2 tweaks on Summit, which should not be needed in the long run:
            # - ensure 1 ssh per dvm
            prte += ' --prtemca plm_rsh_num_concurrent %d' % dvm_size
            # - avoid 64 node limit (ssh connection limit)
            prte += ' --prtemca plm_rsh_no_tree_spawn 1'

            # to select a set of hardware threads that should be used for
            # processing the following option should set hwthread IDs:
            #   --prtemca hwloc_default_cpu_list "0-83,88-171"

            # Use (g)stdbuf to disable buffering.  We need this to get the
            # "DVM ready" message to ensure DVM startup completion
            #
            # The command seems to be generally available on Cray's,
            # if not, we can code some home-cooked pty stuff (TODO)
            stdbuf_cmd = ru.which(['stdbuf', 'gstdbuf'])
            if not stdbuf_cmd:
                raise Exception('(g)stdbuf command not found')
            stdbuf_arg = '-oL'

            # base command = (g)stdbuf <args> + prte <args>
            cmd = '%s %s %s ' % (stdbuf_cmd, stdbuf_arg, prte)

            # additional (debug) arguments to prte
            verbose = bool(os.environ.get('RADICAL_PILOT_PRUN_VERBOSE'))
            if verbose:
                cmd += ' '.join(['--prtemca plm_base_verbose 5'])

            cmd = cmd.strip()

            log.info('start prte-%s on %d nodes [%s]', dvm_id, dvm_size, cmd)
            profiler.prof(event='dvm_start',
                          uid=cfg['pid'],
                          msg='dvm_id=%s' % dvm_id)

            dvm_process = mp.Popen(cmd.split(),
                                   stdout=mp.PIPE,
                                   stderr=mp.STDOUT)
            dvm_watcher = mt.Thread(target=_watch_dvm,
                                    args=(dvm_process, dvm_id, dvm_ready_flag),
                                    daemon=True)
            dvm_watcher.start()

            dvm_uri = None
            for _ in range(30):
                time.sleep(.5)

                try:
                    with open(DVM_URI_FILE_TPL % file_info, 'r') as fin:
                        for line in fin.readlines():
                            if '://' in line:
                                dvm_uri = line.strip()
                                break
                except Exception as e:
                    log.debug('DVM URI file missing: %s...', str(e)[:24])

                if dvm_uri:
                    break

            if not dvm_uri:
                raise Exception('DVM URI not found')

            log.info('prte-%s startup successful: [%s]', dvm_id, dvm_uri)
            profiler.prof(event='dvm_uri',
                          uid=cfg['pid'],
                          msg='dvm_id=%s' % dvm_id)

            return dvm_uri

        # ----------------------------------------------------------------------

        # format: {<id>: {'nodes': [...], 'dvm_uri': <uri>}}
        # `nodes` will be moved to RM, and the rest is LM details per partition
        partitions = {}

        # go through every dvm instance
        for _dvm_id in range(dvm_count):

            node_list = rm.node_list[_dvm_id       * nodes_per_dvm:
                                     (_dvm_id + 1) * nodes_per_dvm]
            dvm_file_info.update({'dvm_id': _dvm_id})
            # write hosts file
            with open(DVM_HOSTS_FILE_TPL % dvm_file_info, 'w') as fout:
                num_slots = rm.cores_per_node * rm.smt
                for node in node_list:
                    fout.write('%s slots=%d\n' % (node[0], num_slots))

            _dvm_size  = len(node_list)
            _dvm_ready = mt.Event()
            _dvm_uri   = _start_dvm(_dvm_id, _dvm_size, _dvm_ready)

            partition_id = str(_dvm_id)
            partitions[partition_id] = {
                'nodes'  : [node[1] for node in node_list],
                'dvm_uri': _dvm_uri}

            # extra time to confirm that "DVM ready" was just delayed
            if _dvm_ready.wait(timeout=15.):
                continue

            log.info('prte-%s to be re-started', _dvm_id)

            # terminate DVM (if it is still running)
            try   : ru.sh_callout('pterm --dvm-uri "%s"' % _dvm_uri)
            except: pass

            # re-start DVM
            partitions[partition_id]['dvm_uri'] = \
                _start_dvm(_dvm_id, _dvm_size, _dvm_ready)

            # FIXME: with the current approach there is only one attempt to
            #        restart DVM(s). If a failure during the start process
            #        will keep appears then need to consider re-assignment
            #        of nodes from failed DVM(s) + add time for re-started
            #        DVM to stabilize: `time.sleep(10.)`

        lm_info = {'partitions'  : partitions,
                   'version_info': prte_info,
                   'cvd_id_mode' : 'physical'}

        # we need to inform the actual LaunchMethod instance about partitions
        # (`partition_id` per task and `dvm_uri` per partition). List of nodes
        # will be stored in ResourceManager and the rest details about
        # partitions will be kept in `lm_info`, which will be passed as part of
        # the slots via the scheduler.
        return lm_info

    # --------------------------------------------------------------------------
    #
    @classmethod
    def rm_shutdown_hook(cls, name, cfg, rm, lm_info, log, profiler):
        """
        This hook is symmetric to the hook `rm_config_hook`, it is called
        during shutdown sequence, for the sake of freeing allocated resources.
        """
        cmd = ru.which('pterm')
        if not cmd:
            raise Exception('termination command not found')

        for p_id, p_data in lm_info.get('partitions', {}).items():
            dvm_uri = p_data['dvm_uri']
            try:
                log.info('terminating prte-%s (%s)', p_id, dvm_uri)
                _, err, _ = ru.sh_callout('%s --dvm-uri "%s"' % (cmd, dvm_uri))
                log.debug('termination status: %s', err.strip('\n') or '-')
                profiler.prof(event='dvm_stop',
                              uid=cfg['pid'],
                              msg='dvm_id=%s' % p_id)

            except Exception as e:
                # use the same event name as for runtime failures - those are
                # not distinguishable at the moment from termination failures
                log.debug('prte-%s termination failed (%s)', p_id, dvm_uri)
                profiler.prof(event='dvm_fail',
                              uid=cfg['pid'],
                              msg='dvm_id=%s | error: %s' % (p_id, e))

    # --------------------------------------------------------------------------
    #
    def construct_command(self, t, launch_script_hop):

        # latest implementation of PRRTE is able to handle bulk submissions
        # of prun-commands, but in case errors will come out then the delay
        # should be set: `time.sleep(.1)`

        slots     = t['slots']
        td        = t['description']

        task_exec = td['executable']
        task_args = td.get('arguments') or []

        n_procs   = td.get('cpu_processes') or 1
        n_threads = td.get('cpu_threads')   or 1

        if not slots.get('lm_info'):
            raise RuntimeError('lm_info not set (%s): %s' % (self.name, slots))

        if not slots['lm_info'].get('partitions'):
            raise RuntimeError('no partitions (%s): %s' % (self.name, slots))

        partitions = slots['lm_info']['partitions']
        # `partition_id` should be set in a scheduler
        partition_id = slots.get('partition_id') or partitions.keys()[0]
        dvm_uri = '--dvm-uri "%s"' % partitions[partition_id]['dvm_uri']

        if n_threads == 1: map_to_object = 'hwthread'
        else             : map_to_object = 'node:HWTCPUS'

        flags  = ' --np %d'                         % n_procs
        flags += ' --map-by %s:PE=%d:OVERSUBSCRIBE' % (map_to_object, n_threads)
        flags += ' --bind-to hwthread:overload-allowed'
        if self._verbose:
            flags += ':REPORT'

        if 'nodes' not in slots:
            # this task is unscheduled - we leave it to PRRTE/PMI-X
            # to correctly place the task
            pass
        else:
            hosts = collections.defaultdict(int)
            for node in slots['nodes']:
                hosts[node['name']] += 1
            flags += ' --host ' + ','.join(['%s:%s' % x for x in hosts.items()])

        flags += ' --pmixmca ptl_base_max_msg_size %d' % PTL_MAX_MSG_SIZE
        flags += ' --verbose'  # needed to get prte profile events

        task_args_str = self._create_arg_string(task_args)
        if task_args_str:
            task_exec += ' %s' % task_args_str

        cmd = '%s %s %s %s' % (self.launch_command, dvm_uri, flags, task_exec)

        return cmd, None

# ------------------------------------------------------------------------------
