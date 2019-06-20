
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
        out, err, ret = ru.sh_callout('prte_info | grep "Open RTE"', shell=True)
        prte_info = dict()
        for line in out.split('\n'):

            line = line.strip()

            if 'Open RTE:' in line:
                prte_info['version'] = line.split(':')[1].strip()

            elif  'Open RTE repo revision:' in line:
                prte_info['version_detail'] = line.split(':')[1].strip()

        logger.info("Found Open RTE: %s [%s]",
                    prte_info.get('version'), prte_info.get('version_detail'))

        # Use (g)stdbuf to disable buffering.
        # We need this to get the "DVM ready",
        # without waiting for prte to complete.
        # The command seems to be generally available on our Cray's,
        # if not, we can code some home-coooked pty stuff.
        stdbuf_cmd =  ru.which(['stdbuf', 'gstdbuf'])
        if not stdbuf_cmd:
            raise Exception("Couldn't find (g)stdbuf")
        stdbuf_arg = "-oL"

        # Base command = (g)stdbuf <args> + prte + prte-args + debug_args
        prte_args = '--report-uri -'
        cmdline   = '%s %s %s %s ' % (stdbuf_cmd, stdbuf_arg, prte, prte_args)

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

        vm_size = len(lrms.node_list)
        logger.info("Start prte on %d nodes ['%s']", vm_size, ' '.join(cmdline))
        profiler.prof(event='dvm_start', uid=cfg['pilot_id'])

        dvm_uri     = None
        dvm_process = mp.Popen(cmdline.split(), stdout=mp.PIPE, stderr=mp.STDOUT)

        while True:

            line = dvm_process.stdout.readline()

            if '://' in line:

                dvm_uri = line.strip()
                logger.info("prte uri: %s" % dvm_uri)

            elif 'DVM ready' in line:
                if not dvm_uri:
                    raise Exception("VMURI not found!")

                logger.info("prte startup successful!")
                profiler.prof(event='dvm_ok', uid=cfg['pilot_id'])
                break

            else:

                # Check if the process is still around,
                # and log output in debug mode.
                if dvm_process.poll() is None:
                    logger.debug("PRUN: %s", line)
                else:
                    # Process is gone: fatal!
                    raise Exception("prun process disappeared")
                    profiler.prof(event='dvm_fail', uid=cfg['pilot_id'])


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

        self.launch_command = ru.which('prun')


    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

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
      # self._log.debug('=== prep %s', pprint.pformat(cu))
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

        if 'nodes' not in slots:
            # this task is unscheduled - we leave it to PRRTE/PMI-X to
            # correctly place the task.  Just count procs and threads.

            map_flag  = ' --bind-to none -use-hwthread-cpus --oversubscribe'
            map_flag += ' -n %d --cpus-per-proc %d' % (n_procs, n_threads)

        else:
            # enact the scheduler's placement
            hosts_string = ''
            depths       = set()
            for node in slots['nodes']:
                self._log.debug('=== %s' % node)

                # add all cpu and gpu process slots to the node list.
                for cpu_slot in node[2]: hosts_string += '%s,' % node[0]
                for gpu_slot in node[3]: hosts_string += '%s,' % node[0]
                for cpu_slot in node[2]: depths.add(len(cpu_slot))

            # remove trailing ','
            hosts_string = hosts_string.rstrip(',')

            # FIXME: ensure correct binding for procs and threads
          # assert(len(depths) == 1), depths
          # depth = depths.pop()
          # if depth > 1: map_flag = '--bind-to none --map-by ppr:%d:core' % depth
          # else        : map_flag = '--bind-to none'
            map_flag  = ' --bind-to none -use-hwthread-cpus --oversubscribe'
            map_flag += ' -n %d --cpus-per-proc %d' % (n_procs, n_threads)
            map_flag += ' -host %s' % hosts_string


        # Additional (debug) arguments to prun
        if self._verbose:
            debug_strings = ['-display-devel-map',
                             '-display-allocation',
                             '--debug-devel',
                            ]
        else:
            debug_strings = []
        debug_string = ' '.join(debug_strings)

        command = '%s %s --hnp "%s" %s %s %s' % (self.launch_command,
                  debug_string, dvm_uri, map_flag, env_string, task_command)

        return command, None


# ------------------------------------------------------------------------------

