
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import time
import subprocess    as mp
import radical.utils as ru

from .base import LaunchMethod


# ==============================================================================
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
    @classmethod
    def lrms_config_hook(cls, name, cfg, lrms, logger, profiler):

        prte = ru.which('prte')
        if not prte:
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

        logger.info("Found Open RTE: %s [%s]",
                    prte_info.get('version'), prte_info.get('version_detail'))


        # write hosts file
        furi   = '%s/prrte.uri'   % os.getcwd()
        fhosts = '%s/prrte.hosts' % os.getcwd()

        with open(fhosts, 'w') as fout:
            for node in lrms.node_list:
                fout.write('%s slots=%d\n' % (node[0], lrms.cores_per_node * lrms.smt))

        pre   = os.environ['PRRTE_PREFIX']
        prte += ' --prefix %s'     % pre
        prte += ' --report-uri %s' % furi
        prte += ' --hostfile %s'   % fhosts
      # prte += ' --mca plm_rsh_no_tree_spawn 1'

        # Use (g)stdbuf to disable buffering.
        # We need this to get the "DVM ready",
        # without waiting for prte to complete.
        # The command seems to be generally available on our Cray's,
        # if not, we can code some home-coooked pty stuff (TODO)
        stdbuf_cmd =  ru.which(['stdbuf', 'gstdbuf'])
        if not stdbuf_cmd:
            raise Exception("Couldn't find (g)stdbuf")
        stdbuf_arg = "-oL"

        # Base command = (g)stdbuf <args> + prte + prte-args + debug_args
        cmdline   = '%s %s %s ' % (stdbuf_cmd, stdbuf_arg, prte)
      # cmdline   = prte

        # Additional (debug) arguments to prte
        verbose = bool(os.environ.get('RADICAL_PILOT_PRUN_VERBOSE'))
        if verbose:
            debug_strings = [
                             '--debug-devel',
                             '--mca odls_base_verbose 100',
                             '--mca rml_base_verbose 100'
                            ]
        else:
            debug_strings = []

        # Split up the debug strings into args and add them to the cmdline
        cmdline += ' '.join(debug_strings)
        cmdline  = cmdline.strip()

        vm_size = len(lrms.node_list)
        logger.info("Start prte on %d nodes [%s]", vm_size, cmdline)
        profiler.prof(event='dvm_start', uid=cfg['pilot_id'])

        dvm_uri     = None
        dvm_process = mp.Popen(cmdline.split(), stdout=mp.PIPE,
                               stderr=mp.STDOUT)

        for _ in range(100):

            time.sleep(0.1)
            try:
                with open(furi, 'r') as fin:
                    for line in fin.readlines():
                        if '://' in line:
                            dvm_uri = line.strip()
                            break

            except Exception as e:
                logger.debug('DVM check: %s' % e)
                time.sleep(0.5)

            if dvm_uri:
                break

        if not dvm_uri:
            raise Exception("VMURI not found!")

        logger.info("prte startup successful: [%s]", dvm_uri)
        profiler.prof(event='dvm_ok', uid=cfg['pilot_id'])


        # ----------------------------------------------------------------------
        def _watch_dvm():

            logger.info('starting prte watcher')

            retval = dvm_process.poll()
            while retval is None:
                line = dvm_process.stdout.readline().strip()
                if line:
                    logger.debug('prte output: %s', line)
                else:
                    time.sleep(1.0)

            if retval != 0:
                # send a kill signal to the main thread.
                # We know that Python and threading are likely not to play well
                # with signals - but this is an exceptional case, and not part
                # of the stadard termination sequence.  If the signal is
                # swallowed, the next `prun` call will trigger
                # termination anyway.
                os.kill(os.getpid())
                raise RuntimeError('PRTE DVM died')

            logger.info('prte stopped (%d)' % dvm_process.returncode)
        # ----------------------------------------------------------------------

        dvm_watcher = ru.Thread(target=_watch_dvm, name="DVMWatcher")
        dvm_watcher.start()

        lm_info = {'dvm_uri'     : dvm_uri,
                   'version_info': prte_info}

        # we need to inform the actual LM instance about the prte URI.  So we
        # pass it back to the LRMS which will keep it in an 'lm_info', which
        # will then be passed as part of the slots via the scheduler
        return lm_info


    # --------------------------------------------------------------------------
    #
    @classmethod
    def lrms_shutdown_hook(cls, name, cfg, lrms, lm_info, logger, profiler):
        """
        This hook is symmetric to the config hook above, and is called during
        shutdown sequence, for the sake of freeing allocated resources.
        """

        if 'dvm_uri' in lm_info:
            try:
                logger.info('terminating prte')
                prun = ru.which('prun')
                if not prun:
                    raise Exception("Couldn't find prun")
                ru.sh_callout('%s --hnp %s --terminate'
                             % (prun, lm_info['dvm_uri']))
                profiler.prof(event='dvm_stop', uid=cfg['pilot_id'])

            except Exception as e:
                # use the same event name as for runtime failures - those are
                # not distinguishable at the moment from termination failures
                profiler.prof(event='dvm_fail', uid=cfg['pilot_id'], msg=e)
                logger.exception('prte termination failed')


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        # ensure that `prun` is in the path (`which` will raise otherwise)
        ru.which('prun')
        self.launch_command = 'prun'


    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        time.sleep(0.1)

        slots        = cu['slots']
        cud          = cu['description']
        task_exec    = cud['executable']
        task_env     = cud.get('environment') or dict()
        task_args    = cud.get('arguments')   or list()
        task_argstr  = self._create_arg_string(task_args)

        n_threads = cu['description'].get('cpu_threads',   1)
        n_procs   = cu['description'].get('cpu_processes', 0) \
                  + cu['description'].get('gpu_processes', 0)

        if not n_procs  : n_procs   = 1
        if not n_threads: n_threads = 1

      # import pprint
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

        dvm_uri = slots['lm_info']['dvm_uri']

        if task_argstr: task_command = "%s %s" % (task_exec, task_argstr)
        else          : task_command = task_exec

        env_string = ''
        env_list   = self.EXPORT_ENV_VARIABLES + task_env.keys()
        if env_list:
            for var in env_list:
                env_string += '-x "%s" ' % var

        map_flag  = ' -np %d --cpus-per-proc %d' % (n_procs, n_threads)
      # map_flag += ' --bind-to hwthread:overload-allowed --use-hwthread-cpus'
      # map_flag += ' --oversubscribe'

        if 'nodes' not in slots:
            # this task is unscheduled - we leave it to PRRTE/PMI-X to
            # correctly place the task
            pass

        else:
            # FIXME: ensure correct binding for procs and threads via slotfile

            # enact the scheduler's host placement.  For now, we leave socket,
            # core and thread placement to the prted, and just add all cpu and
            # gpu process slots to the host list.
            hosts = ''

            for node in slots['nodes']:

                # for each cpu and gpu slot, add the respective node name
                for _ in node['core_map']: hosts += '%s,' % node['name']
                for _ in node['gpu_map' ]: hosts += '%s,' % node['name']

            # remove trailing ','
            map_flag += ' -host %s' % hosts.rstrip(',')

        # Additional (debug) arguments to prun
        debug_string = ''
        if self._verbose:
            debug_string = ' '.join([
                                     # '-display-devel-map',
                                     # '-display-allocation',
                                     # '--debug-devel',
                                       '--report-bindings',
                                    ])

        env_string = ''  # FIXME
        command = '%s --hnp "%s" %s %s %s %s' % (self.launch_command,
                  dvm_uri, map_flag, debug_string, env_string, task_command)

        return command, None


# ------------------------------------------------------------------------------

