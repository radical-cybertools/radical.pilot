
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

import logging
import math
import os
import signal
import time

import threading     as mt
import subprocess    as mp

import radical.utils as ru

from .base import LaunchMethod

NUM_NODES_PER_DVM = 10  # e.g., if 50 nodes -> 5 DVMs
NUM_UNITS_PER_DVM = 20  #       and each DVM handles 20 tasks (max 100 tasks)

# with `True` value it skips the lists of hosts assigned to each task and gets
# corresponding host from file `prrte.<dvm_id>.hosts`; parameters that are used:
# task id (uid), numbers of nodes and tasks (units) per dvm from above.
SKIP_SCHEDULER = False


# ------------------------------------------------------------------------------
#
class PRTE(LaunchMethod):


    # --------------------------------------------------------------------------
    #
    def __init__(self, name, cfg, session):

        LaunchMethod.__init__(self, name, cfg, session)

        # We remove all PRUN related environment variables from the launcher
        # environment, so that we can use PRUN for both launch of the
        # (sub-)agent and CU execution.
        self.env_removables.extend(["OMPI_", "OPAL_", "PMIX_"])

        self._verbose = bool(os.environ.get('RADICAL_PILOT_PRUN_VERBOSE'))


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def _watch_dvm(dvm_process, dvm_id, log):

        log.info('start prte-%s watcher', dvm_id)

        retval = dvm_process.poll()
        while retval is None:
            line = dvm_process.stdout.readline().strip()
            if line:
                log.debug('prte-%s output: %s', dvm_id, line)
            else:
                time.sleep(1.)

        if retval != 0:
            # send a kill signal to the main thread.
            # We know that Python and threading are likely not to play well
            # with signals - but this is an exceptional case, and not part
            # of the standard termination sequence.  If the signal is
            # swallowed, the next `prun` call will trigger
            # termination anyway.
            os.kill(os.getpid(), signal.SIGKILL)
            raise RuntimeError('PRTE DVM died')

        log.info('prte-%s watcher stopped (%d)', dvm_id, dvm_process.returncode)


    # --------------------------------------------------------------------------
    #
    @classmethod
    def rm_config_hook(cls, name, cfg, rm, log, profiler):

        prte_cmd = ru.which('prte')
        if not prte_cmd:
            raise Exception("Couldn't find prte")

        # Now that we found the prte, get PRUN version
        out, _, _ = ru.sh_callout('prte_info | grep "Open RTE"', shell=True)
        prte_info = dict()
        for line in out.split('\n'):

            line = line.strip()

            if 'Open RTE:' in line:
                prte_info['version'] = line.split(':')[1].strip()

            elif  'Open RTE repo revision:' in line:
                prte_info['version_detail'] = line.split(':')[1].strip()

        log.info("Found Open RTE: %s [%s]",
                 prte_info.get('version'),
                 prte_info.get('version_detail'))

        # start creating multiple DVMs
        rm_node_list = list(rm.node_list)
        num_dvm = math.ceil(len(rm_node_list) / NUM_NODES_PER_DVM)
        dvm_uri_list = []

        # go through every dvm instance
        for dvm_id in range(num_dvm):
            node_list = rm_node_list[ dvm_id      * NUM_NODES_PER_DVM:
                                     (dvm_id + 1) * NUM_NODES_PER_DVM]
            vm_size   = len(node_list)
            furi      = '%s/prrte.%s.uri'   % (os.getcwd(), dvm_id)
            fhosts    = '%s/prrte.%s.hosts' % (os.getcwd(), dvm_id)

            with open(fhosts, 'w') as fout:
                for node in node_list:
                    fout.write('%s slots=%d\n' % (node[0],
                                                  rm.cores_per_node * rm.smt))

            prte  = '%s'               % prte_cmd
            prte += ' --prefix %s'     % os.environ['PRRTE_PREFIX']
            prte += ' --report-uri %s' % furi
            prte += ' --hostfile %s'   % fhosts

            if profiler.enabled:
                prte += ' --pmca orte_state_base_verbose 1'  # prte profiling

            # large tasks imply large message sizes, and we need to account for that
            # FIXME: we should derive the message size from DVM size - smaller DVMs
            #        will never need large messages, as they can't run large tasks)
            prte += ' --pmca ptl_base_max_msg_size %d' % (1024 * 1024 * 1024 * 1)
          # prte += ' --pmca rmaps_base_verbose 5'

            # debug mapper problems for large tasks
            if log.isEnabledFor(logging.DEBUG):
                prte += ' -pmca orte_rmaps_base_verbose 100'

            # we apply two temporary tweaks on Summit which should not be needed in
            # the long run:
            #
            # avoid 64 node limit (ssh connection limit)
            prte += ' --pmca plm_rsh_no_tree_spawn 1'

            # ensure 1 ssh per dvm
            prte += ' --pmca plm_rsh_num_concurrent %d' % vm_size

            # Use (g)stdbuf to disable buffering.  We need this to get the
            # "DVM ready" message to ensure DVM startup completion
            #
            # The command seems to be generally available on our Cray's,
            # if not, we can code some home-coooked pty stuff (TODO)
            stdbuf_cmd = ru.which(['stdbuf', 'gstdbuf'])
            if not stdbuf_cmd:
                raise Exception("Couldn't find (g)stdbuf")
            stdbuf_arg = "-oL"

            # Additional (debug) arguments to prte
            verbose = bool(os.environ.get('RADICAL_PILOT_PRUN_VERBOSE'))
            if verbose:
                debug_strings = [
                                 '--debug-devel',
                                 '--pmca odls_base_verbose 100',
                                 '--pmca rml_base_verbose 100',
                                ]
            else:
                debug_strings = []

            # Base command = (g)stdbuf <args> + prte + prte-args + debug_args
            cmdline  = '%s %s %s ' % (stdbuf_cmd, stdbuf_arg, prte)
            cmdline += ' '.join(debug_strings)
            cmdline  = cmdline.strip()

            log.info("start prte-%s on %d nodes [%s]", dvm_id, vm_size, cmdline)
            profiler.prof(event='dvm_start',
                          uid=cfg['pid'],
                          msg='dvm_id=%s' % dvm_id)

            dvm_uri     = None
            dvm_process = mp.Popen(cmdline.split(),
                                   stdout=mp.PIPE,
                                   stderr=mp.STDOUT)
            dvm_watcher = mt.Thread(target=cls._watch_dvm,
                                    args=[dvm_process, dvm_id, log])
            dvm_watcher.daemon = True
            dvm_watcher.start()

            for _ in range(100):
                time.sleep(.5)

                try:
                    with open(furi, 'r') as fin:
                        for line in fin.readlines():
                            if '://' in line:
                                dvm_uri = line.strip()
                                break
                except Exception as e:
                    log.debug('DVM check: uri file missing: %s...', str(e)[:24])
                    time.sleep(.5)

                if dvm_uri:
                    break

            if not dvm_uri:
                raise Exception("DVM_URI not found!")

            log.info("prte-%s startup successful: [%s]", dvm_id, dvm_uri)

            # in some cases, DVM seems to need some additional time to settle.
            # FIXME: this should not be needed, really
            time.sleep(10)
            profiler.prof(event='dvm_ok',
                          uid=cfg['pid'],
                          msg='dvm_id=%s' % dvm_id)

            dvm_uri_list.append(dvm_uri)

        lm_info = {'dvm_uri'     : dvm_uri_list,
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
        This hook is symmetric to the config hook above, and is called during
        shutdown sequence, for the sake of freeing allocated resources.
        """
        prun = ru.which('prun')
        if not prun:
            raise Exception("Couldn't find prun")

        for dvm_id, dvm_uri in enumerate(lm_info.get('dvm_uri', [])):
            try:
                log.info('terminating prte-%s (%s)', dvm_id, dvm_uri)
                ru.sh_callout('%s --hnp %s --terminate' % (prun, dvm_uri))
                profiler.prof(event='dvm_stop',
                              uid=cfg['pid'],
                              msg='dvm_id=%s' % dvm_id)
            except Exception as e:
                # use the same event name as for runtime failures - those are
                # not distinguishable at the moment from termination failures
                profiler.prof(event='dvm_fail',
                              uid=cfg['pid'],
                              msg='dvm_id=%s | error: %s' % (dvm_id, e))
                log.debug('prte-%s termination failed (%s)', dvm_id, dvm_uri)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        # ensure that `prun` is in the path (`which` will raise otherwise)
        ru.which('prun')
        self.launch_command = 'prun'


    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        ### --- test for 1 task executed by JSRUN ------------------------------

        ## NOTE: this is applicable for task without GPU, since for that need to
        ##       set lm_info = {'cvd_id_mode': 'logical'}, which is NOT per task

        # if int(cu['uid'].split('unit.')[1]) == 0:
        #     from ... import agent as rpa
        #     launcher = rpa.LaunchMethod.create(name='JSRUN',
        #                                        cfg=self._cfg,
        #                                        session=self._session)
        #     return launcher.construct_command(cu, launch_script_hop)
        ### --------------------------------------------------------------------

        time.sleep(0.1)

        slots        = cu['slots']
        cud          = cu['description']
        task_exec    = cud['executable']
        task_env     = cud.get('environment') or dict()
        task_args    = cud.get('arguments')   or list()
        task_argstr  = self._create_arg_string(task_args)

        n_procs      = cu['description'].get('cpu_processes') or 1
        n_threads    = cu['description'].get('cpu_threads')   or 1

        cu_id        = int(cu['uid'].split('unit.')[1])
        dvm_id       = int(cu_id / NUM_UNITS_PER_DVM)
        cu_order_idx = int(cu_id % NUM_UNITS_PER_DVM)  # inside dvm
        num_units_per_node = math.ceil(NUM_UNITS_PER_DVM / NUM_NODES_PER_DVM)

        self._log.debug('prep %s', cu['uid'])

        if 'lm_info' not in slots:
            raise RuntimeError('No lm_info to launch via %s: %s'
                               % (self.name, slots))

        if not slots['lm_info']:
            raise RuntimeError('lm_info missing for %s: %s'
                               % (self.name, slots))

        if 'dvm_uri' not in slots['lm_info']:
            raise RuntimeError('dvm_uri not in lm_info for %s: %s'
                               % (self.name, slots))

        if len(slots['lm_info']['dvm_uri']) <= dvm_id:
            raise RuntimeError('not enough DVMs (lm_info %s: %s; dvm_id: %s)'
                               % (self.name, slots, dvm_id))

        dvm_uri = slots['lm_info']['dvm_uri'][dvm_id]

        if task_argstr: task_command = "%s %s" % (task_exec, task_argstr)
        else          : task_command = task_exec

        env_string = ''
        env_list   = self.EXPORT_ENV_VARIABLES + list(task_env.keys())
        if env_list:
            for var in env_list:
                env_string += '-x "%s" ' % var

        map_flag  = ' -np %d --cpus-per-proc %d' % (n_procs, n_threads)
        map_flag += ' --bind-to hwthread:overload-allowed --use-hwthread-cpus'
        map_flag += ' --oversubscribe'

        # see DVM startup
        map_flag += ' --pmca ptl_base_max_msg_size %d' % (1024 * 1024 * 1024 * 1)
      # map_flag += ' --pmca rmaps_base_verbose 5'

        if SKIP_SCHEDULER:
            fhosts = '%s/../prrte.%s.hosts' % (cu['unit_sandbox_path'], dvm_id)
            with open(fhosts) as fd:
                hosts = [x.split()[0] for x in fd.readlines() if x.split()]
            map_flag += ' -host %s:%s' % (
                hosts[int(cu_order_idx/num_units_per_node)], n_procs)
        else:
            if 'nodes' not in slots:
                # this task is unscheduled - we leave it to PRRTE/PMI-X to
                # correctly place the task
                pass

            else:
                # FIXME: ensure correct binding for procs and threads via slotfile

                # enact the scheduler's host placement.  For now, we leave socket,
                # core and thread placement to the prted, and just add all process
                # slots to the host list.
                hosts = ''

                for node in slots['nodes']:
                    hosts += '%s,' % node['name']

                # remove trailing ','
                map_flag += ' -host %s' % hosts.rstrip(',')

        # Additional (debug) arguments to prun
        if self._verbose:
            debug_string = ' '.join([
                                        '--verbose',
                                      # '--debug-devel',
                                      # '-display-devel-map',
                                      # '-display-allocation',
                                        '--report-bindings',
                                     ])
        else:
            debug_string = '--verbose'  # needed to get prte profile events

      # env_string = ''  # FIXME
        command = '%s --hnp "%s" %s %s %s %s' % (self.launch_command,
                  dvm_uri, map_flag, debug_string, env_string, task_command)

        return command, None


# ------------------------------------------------------------------------------

