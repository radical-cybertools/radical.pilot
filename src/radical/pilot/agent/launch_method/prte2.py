
__author__    = 'RADICAL-Cybertools Team'
__email__     = 'info@radical-cybertools.org'
__copyright__ = 'Copyright 2020, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import logging
import os
import signal
import time

import subprocess    as mp
import threading     as mt

import radical.utils as ru

from .base import LaunchMethod


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
        # (sub-)agent and CU execution.
        self.env_removables.extend(['OMPI_', 'OPAL_', 'PMIX_'])

        self._verbose = bool(os.environ.get('RADICAL_PILOT_PRUN_VERBOSE'))

    # --------------------------------------------------------------------------
    #
    def _configure(self):

        if ru.which('prun'):
            self.launch_command = 'prun'
        else:
            raise Exception('prun not found')

    # --------------------------------------------------------------------------
    #
    @classmethod
    def rm_config_hook(cls, name, cfg, rm, log, profiler):

        prte = ru.which('prte')
        if not prte:
            raise Exception('prte not found')

        # get OpenRTE/PRTE version
        prte_info = {}
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

        # write hosts file
        furi    = '%s/prrte.uri'   % os.getcwd()
        fhosts  = '%s/prrte.hosts' % os.getcwd()
        vm_size = len(rm.node_list)

        with open(fhosts, 'w') as f:
            for node in rm.node_list:
                f.write('%s slots=%d\n' % (node[0], rm.cores_per_node * rm.smt))

        prte += ' --prefix %s'     % os.environ.get('PRRTE_PREFIX', '')
        prte += ' --report-uri %s' % furi
        prte += ' --hostfile %s'   % fhosts

        if profiler.enabled:  # prte profiling
            # version 1 vs 2
            # prte += ' --pmca orte_state_base_verbose 1'
            prte += ' --mca orte_state_base_verbose 1'

        # large tasks imply large message sizes and we need to account for that
        _base_max_msg_size = 1024 * 1024 * 1024 * 1
        # FIXME: we should derive the message size from DVM size - smaller DVMs
        #        will never need large messages, as they can't run large tasks

        # version 1 vs 2
        # prte += ' --pmca ptl_base_max_msg_size %d' % _base_max_msg_size
        # # prte += ' --pmca rmaps_base_verbose 5'
        prte += ' --mca ptl_base_max_msg_size %d' % _base_max_msg_size

        # debug mapper problems for large tasks
        if log.isEnabledFor(logging.DEBUG):
            # version 1 vs 2
            # prte += ' -pmca orte_rmaps_base_verbose 100'
            prte += ' --mca orte_rmaps_base_verbose 100'

        # 2 temporary tweaks on Summit (not needed for the long run)
        # (1) avoid 64 node limit (ssh connection limit)
        # prte += ' --pmca plm_rsh_no_tree_spawn 1'
        # (2) ensure 1 ssh per dvm
        # prte += ' --pmca plm_rsh_num_concurrent %d' % vm_size

        # version 2
        prte += ' --mca plm_rsh_no_tree_spawn 1'
        prte += ' --mca plm_rsh_num_concurrent %d' % vm_size
        # FIXME: these 2 options above might not be needed
        #        (corresponding experiments should be conducted)

        # Use (g)stdbuf to disable buffering.  We need this to get the
        # "DVM ready" message to ensure DVM startup completion
        #
        # The command seems to be generally available on Cray's,
        # if not, we can code some home-cooked pty stuff (TODO)
        stdbuf_cmd = ru.which(['stdbuf', 'gstdbuf'])
        if not stdbuf_cmd:
            raise Exception('(g)stdbuf not found')
        stdbuf_arg = '-oL'

        # base command = (g)stdbuf <args> + prte + prte_args + debug_args
        cmd = '%s %s %s ' % (stdbuf_cmd, stdbuf_arg, prte)

        # additional (debug) arguments to prte
        verbose = bool(os.environ.get('RADICAL_PILOT_PRUN_VERBOSE'))
        if verbose:
            cmd += ' '.join(['--debug-devel',
                             '--mca odls_base_verbose 100',
                             '--mca rml_base_verbose 100'])

        cmd = cmd.strip()

        log.info('start prte on %d nodes [%s]', vm_size, cmd)
        profiler.prof(event='dvm_start', uid=cfg['pid'])

        dvm_process = mp.Popen(cmd.split(), stdout=mp.PIPE, stderr=mp.STDOUT)

        # ----------------------------------------------------------------------
        def _watch_dvm():

            log.info('starting prte watcher')

            retval = dvm_process.poll()
            while retval is None:
                line = dvm_process.stdout.readline().strip()
                if line:
                    log.debug('prte output: %s', line)
                else:
                    time.sleep(1.0)

            if retval != 0:
                # send a kill signal to the main thread.
                # We know that Python and threading are likely not to play well
                # with signals - but this is an exceptional case, and not part
                # of the standard termination sequence.  If the signal is
                # swallowed, the next `prun` call will trigger
                # termination anyway.
                os.kill(os.getpid(), signal.SIGKILL)
                raise RuntimeError('PRTE DVM died')

            log.info('prte stopped (%d)', dvm_process.returncode)
        # ----------------------------------------------------------------------

        dvm_watcher = mt.Thread(target=_watch_dvm)
        dvm_watcher.daemon = True
        dvm_watcher.start()

        dvm_uri = None
        for _ in range(100):

            time.sleep(0.5)
            try:
                with open(furi, 'r') as fin:
                    for line in fin.readlines():
                        if '://' in line:
                            dvm_uri = line.strip()
                            break

            except Exception as e:
                log.debug('DVM check - uri file missing: %s...', str(e)[:24])
                time.sleep(0.5)

            if dvm_uri:
                break

        if not dvm_uri:
            raise Exception('DVMURI not found')

        log.info('prte startup successful: [%s]', dvm_uri)

        # in some cases, the DVM seems to need some additional time to settle.
        # FIXME: this should not be needed
        time.sleep(10)
        profiler.prof(event='dvm_ok', uid=cfg['pid'])

        lm_info = {'dvm_uri'     : dvm_uri,
                   'version_info': prte_info,
                   'cvd_id_mode' : 'physical'}

        # we need to inform the actual LaunchMethod instance about the prte URI.
        # So we pass it back to the ResourceManager which will keep it in an
        # 'lm_info', which will then be passed as part of the slots via the
        # scheduler
        return lm_info

    # --------------------------------------------------------------------------
    #
    @classmethod
    def rm_shutdown_hook(cls, name, cfg, rm, lm_info, log, profiler):
        """
        This hook is symmetric to the hook `rm_config_hook`, it is called
        during shutdown sequence, for the sake of freeing allocated resources.
        """
        if lm_info.get('dvm_uri'):
            try:
                log.info('terminating prte')
                prun = ru.which('prun')
                if not prun:
                    raise Exception('prun not found')
                ru.sh_callout('%s --dvm-uri "%s" --terminate' %
                              (prun, lm_info['dvm_uri']))
                profiler.prof(event='dvm_stop', uid=cfg['pid'])

            except Exception as e:
                # use the same event name as for runtime failures - those are
                # not distinguishable at the moment from termination failures
                profiler.prof(event='dvm_fail', uid=cfg['pid'], msg=e)
                log.exception('prte termination failed')

    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        time.sleep(0.1)

        slots     = cu['slots']
        cud       = cu['description']

        task_exec = cud['executable']
        task_args = cud.get('arguments') or []
        task_env  = cud.get('environment') or {}

        n_procs   = cud.get('cpu_processes') or 1
        n_threads = cud.get('cpu_threads') or 1

        if not slots.get('lm_info'):
            raise RuntimeError('lm_info not set (%s): %s' % (self.name, slots))

        if not slots['lm_info'].get('dvm_uri'):
            raise RuntimeError('dvm_uri not set (%s): %s' % (self.name, slots))

        dvm_uri = '--dvm-uri "%s"' % slots['lm_info']['dvm_uri']

        # version 1 vs 2
        # flags  = ' -np %d --cpus-per-proc %d' % (n_procs, n_threads)
        # flags += ' --bind-to hwthread:overload-allowed --use-hwthread-cpus'
        # flags += ' --oversubscribe'
        flags  = ' -np %d --map-by :PE=%d' % (n_procs, n_threads)
        flags += ' --bind-to hwthread:overload-allowed'

        # DVM startup
        _base_max_msg_size = 1024 * 1024 * 1024 * 1

        # version 1 vs 2
        # flags += ' --pmca ptl_base_max_msg_size %d' % _base_max_msg_size
        # # flags += ' --pmca rmaps_base_verbose 5'
        flags += ' --mca ptl_base_max_msg_size %d' % _base_max_msg_size

        if 'nodes' not in slots:
            # this task is unscheduled - we leave it to PRRTE/PMI-X
            # to correctly place the task
            pass

        else:
            # FIXME: ensure correct binding for procs and threads via slotfile

            # enact the scheduler's host placement.  For now, we leave socket,
            # core and thread placement to the prted, and just add all process
            # slots to the host list
            hosts = ','.join([node['name'] for node in slots['nodes']])
            flags += ' --host %s' % hosts

        # additional (debug) arguments to prun
        debug_flags = '--verbose'  # needed to get prte profile events
        if self._verbose:
            debug_flags += ' ' + ' '.join(['--report-bindings'])
                                         # '--debug-devel',
                                         # '--display-devel-map',
                                         # '--display-allocation'])

        env_list = self.EXPORT_ENV_VARIABLES + list(task_env.keys())
        if env_list:
            envs = ' '.join(['-x "%s"' % k for k in env_list])
        else:
            envs = ''

        cmd = '%s %s %s %s %s %s %s' % (
            self.launch_command, dvm_uri, flags, debug_flags, envs,
            task_exec, self._create_arg_string(task_args))

        return cmd, None

# ------------------------------------------------------------------------------
