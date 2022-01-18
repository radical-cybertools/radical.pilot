
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

DVM_URI_FILE_TPL   = '%(base_path)s/prrte.%(dvm_id)04d.uri'
DVM_HOSTS_FILE_TPL = '%(base_path)s/prrte.%(dvm_id)04d.hosts'


# ------------------------------------------------------------------------------
#
class PRTE(LaunchMethod):
    """
    PMIx Reference Run Time Environment v2
    (https://github.com/openpmix/prrte)
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, lm_cfg, rm_info, log, prof):

        self._verbose = bool(os.environ.get('RADICAL_PILOT_PRUN_VERBOSE'))

        LaunchMethod.__init__(self, name, lm_cfg, rm_info, log, prof)

    # --------------------------------------------------------------------------
    #
    def __del__(self):

        self._terminate()

    # --------------------------------------------------------------------------
    #
    def _configure(self):

        prte_cmd = ru.which('prte')
        if not prte_cmd:
            raise Exception('prte command not found')

        prte_prefix = os.environ.get('UMS_OMPIX_PRRTE_DIR')

        prte_info = {}  # get OpenRTE/PRTE version
        out = ru.sh_callout('prte_info | grep "RTE"', shell=True)[0]
        for line in out.split('\n'):

            line = line.strip()

            if not prte_info.get('name'):
                if   'Open RTE' in line: prte_info['name'] = 'Open RTE'
                elif 'PRTE'     in line: prte_info['name'] = 'PRTE'

            if 'RTE:' in line:
                prte_info['version'] = line.split(':')[1].strip()
            elif 'RTE repo revision:' in line:
                prte_info['version_detail'] = line.split(':')[1].strip()

        if prte_info.get('name'):
            self._log.info('version of %s: %s [%s]',
                           prte_info['name'],
                           prte_info.get('version'),
                           prte_info.get('version_detail'))
        else:
            self._log.info('version of OpenRTE/PRTE not determined')

        # get `dvm_count` from resource config (LM config section)
        # FIXME: another option is to introduce `nodes_per_dvm` in LM config
        #        dvm_count = math.ceil(len(rm_info.node_list) / nodes_per_dvm)
        dvm_count     = self._lm_cfg.get('dvm_count') or 1
        nodes_per_dvm = math.ceil(len(self._rm_info['node_list']) / dvm_count)

        # additional info to form prrte related files (uri-/hosts-file)
        dvm_file_info = {'base_path': os.getcwd()}

        # ----------------------------------------------------------------------
        def _watch_dvm(dvm_process, dvm_id, dvm_ready_flag):

            self._log.info('prte-%s watcher started', dvm_id)

            retval = dvm_process.poll()
            while retval is None:
                line = dvm_process.stdout.readline().strip()
                if line:
                    self._log.debug('prte-%s output: %s', dvm_id, line)

                    # final confirmation that DVM has started successfully
                    if not dvm_ready_flag.is_set() and 'DVM ready' in str(line):
                        dvm_ready_flag.set()
                        self._log.info('prte-%s is ready', dvm_id)
                        self._prof.prof(event='dvm_ready',
                                        uid=self._lm_cfg['pid'],
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

            self._log.info('prte-%s watcher stopped', dvm_id)

        # ----------------------------------------------------------------------
        def _start_dvm(dvm_id, dvm_size, dvm_ready_flag):

            file_info = dict(dvm_file_info)
            file_info['dvm_id'] = dvm_id

            prte  = '%s' % prte_cmd
            if prte_prefix: prte += ' --prefix %s' % prte_prefix
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

            self._log.info('start prte-%s on %d nodes [%s]',
                           dvm_id, dvm_size, cmd)
            self._prof.prof(event='dvm_start',
                            uid=self._lm_cfg['pid'],
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
                    with ru.ru_open(DVM_URI_FILE_TPL % file_info, 'r') as fin:
                        for line in fin.readlines():
                            if '://' in line:
                                dvm_uri = line.strip()
                                break
                except Exception as e:
                    self._log.debug('DVM URI file missing: %s...', str(e)[:24])

                if dvm_uri:
                    break

            if not dvm_uri:
                raise Exception('DVM URI not found')

            self._log.info('prte-%s startup successful: [%s]', dvm_id, dvm_uri)
            self._prof.prof(event='dvm_uri',
                            uid=self._lm_cfg['pid'],
                            msg='dvm_id=%s' % dvm_id)

            return dvm_uri

        # ----------------------------------------------------------------------

        # format: {<id>: {'nodes': [...], 'dvm_uri': <uri>}}
        dvm_list = {}

        # go through every dvm instance
        for _dvm_id in range(dvm_count):

            node_list = self._rm_info['node_list'][_dvm_id      * nodes_per_dvm:
                                                  (_dvm_id + 1) * nodes_per_dvm]
            dvm_file_info.update({'dvm_id': _dvm_id})
            # write hosts file
            with ru.ru_open(DVM_HOSTS_FILE_TPL % dvm_file_info, 'w') as fout:
                num_slots = self._rm_info['cores_per_node'] * \
                            self._rm_info['threads_per_core']
                for node in node_list:
                    fout.write('%s slots=%d\n' % (node['node_name'], num_slots))

            _dvm_size  = len(node_list)
            _dvm_ready = mt.Event()
            _dvm_uri   = _start_dvm(_dvm_id, _dvm_size, _dvm_ready)

            dvm_list[str(_dvm_id)] = {
                'nodes'  : [node['node_id'] for node in node_list],
                'dvm_uri': _dvm_uri}

            # extra time to confirm that "DVM ready" was just delayed
            if _dvm_ready.wait(timeout=15.):
                continue

            self._log.info('prte-%s to be re-started', _dvm_id)

            # terminate DVM (if it is still running)
            try   : ru.sh_callout('pterm --dvm-uri "%s"' % _dvm_uri)
            except: pass

            # re-start DVM
            dvm_list[str(_dvm_id)]['dvm_uri'] = \
                _start_dvm(_dvm_id, _dvm_size, _dvm_ready)

            # FIXME: with the current approach there is only one attempt to
            #        restart DVM(s). If a failure during the start process
            #        will keep appears then need to consider re-assignment
            #        of nodes from failed DVM(s) + add time for re-started
            #        DVM to stabilize: `time.sleep(10.)`

        lm_details = {'dvm_list'    : dvm_list,
                      'version_info': prte_info,
                      'cvd_id_mode' : 'physical'}

        return lm_details


    # --------------------------------------------------------------------------
    #
    def get_partitions(self):

        partitions = dict()

        for k, v in self._details['dvm_list'].items():
            partitions[str(k)] = v['nodes']

        return partitions


    # --------------------------------------------------------------------------
    #
    def _terminate(self):
        """
        This hook is symmetric to the hook `rm_config_hook`, it is called
        during shutdown sequence, for the sake of freeing allocated resources.
        """
        cmd = ru.which('pterm')
        if not cmd:
            raise Exception('termination command not found')

        for p_id, p_data in self._details.get('dvm_list', {}).items():
            dvm_uri = p_data['dvm_uri']
            try:
                self._log.info('terminating prte-%s (%s)', p_id, dvm_uri)
                _, err, _ = ru.sh_callout('%s --dvm-uri "%s"' % (cmd, dvm_uri))
                self._log.debug('termination state: %s', err.strip('\n') or '-')
                self._prof.prof(event='dvm_stop', uid=self._lm_cfg['pid'],
                                msg='dvm_id=%s' % p_id)

            except Exception as e:
                # use the same event name as for runtime failures - those are
                # not distinguishable at the moment from termination failures
                self._log.debug('prte-%s termination failed (%s)', p_id, dvm_uri)
                self._prof.prof(event='dvm_fail', uid=self._lm_cfg['pid'],
                                msg='dvm_id=%s | error: %s' % (p_id, e))

    # --------------------------------------------------------------------------
    #
    def _init_from_scratch(self, env, env_sh):

        lm_info = {'env'    : env,
                   'env_sh' : env_sh,
                   'command': ru.which('prun'),
                   'details': self._configure()}

        return lm_info

    # --------------------------------------------------------------------------
    #
    def _init_from_info(self, lm_info):

        self._env     = lm_info['env']
        self._env_sh  = lm_info['env_sh']
        self._command = lm_info['command']
        self._details = lm_info['details']

        assert self._command

    # --------------------------------------------------------------------------
    #
    def finalize(self):

        self._terminate()

    # --------------------------------------------------------------------------
    #
    def can_launch(self, task):

        if not task['description']['executable']:
            return False, 'no executable'

        return True, ''


    # --------------------------------------------------------------------------
    #
    def get_launcher_env(self):

        return ['. $RP_PILOT_SANDBOX/%s' % self._env_sh]

    # --------------------------------------------------------------------------
    #
    def get_launch_cmds(self, task, exec_path):

        # latest implementation of PRRTE is able to handle bulk submissions
        # of prun-commands, but in case errors will come out then the delay
        # should be set: `time.sleep(.1)`

        slots     = task['slots']
        td        = task['description']

        n_procs   = td['cpu_processes']
        n_threads = td['cpu_threads']
        n_gpus    = td['gpu_processes']

        if not self._details.get('dvm_list'):
            raise RuntimeError('details with dvm_list not set (%s)' % self.name)

        dvm_list  = self._details['dvm_list']
        # `partition_id` should be set in a scheduler
        dvm_id    = slots.get('partition_id') or list(dvm_list.keys())[0]
        dvm_uri   = '--dvm-uri "%s"' % dvm_list[dvm_id]['dvm_uri']

        flags = ''
        if n_gpus:
            # input data is edited here to keep PRUN setup within LM
            td['environment'] \
                ['PMIX_MCA_pmdl_ompi_include_envars'] = 'OMPI_*,CUDA_*'
            flags += '--personality ompi '

        flags += '--np %d '                                   % n_procs
        flags += '--map-by node:HWTCPUS:PE=%d:OVERSUBSCRIBE ' % n_threads
        flags += '--bind-to hwthread:overload-allowed'

        if self._verbose:
            flags += ':REPORT'

        if 'ranks' not in slots:
            # this task is unscheduled - we leave it to PRRTE/PMI-X
            # to correctly place the task
            pass
        else:
            ranks = collections.defaultdict(int)
            for rank in slots['ranks']:
                ranks[rank['node_name']] += 1
            flags += ' --host ' + ','.join(['%s:%s' % x for x in ranks.items()])

        flags += ' --pmixmca ptl_base_max_msg_size %d' % PTL_MAX_MSG_SIZE
        flags += ' --verbose'  # needed to get prte profile events

        cmd = '%s %s %s %s' % (self._command, dvm_uri, flags, exec_path)

        return cmd.strip()

    # --------------------------------------------------------------------------
    #
    def get_rank_cmd(self):

        return 'export RP_RANK=$PMIX_RANK\n'

    # --------------------------------------------------------------------------
    #
    def get_rank_exec(self, task, rank_id, rank):

        td           = task['description']
        task_exec    = td['executable']
        task_args    = td['arguments']
        task_argstr  = self._create_arg_string(task_args)
        command      = '%s %s' % (task_exec, task_argstr)

        return command.rstrip()

# ------------------------------------------------------------------------------

