
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import time
import threading
import subprocess    as mp
import radical.utils as ru

from .base import LaunchMethod


# ==============================================================================
#
# NOTE: This requires a development version of Open MPI available.
#
class ORTELib(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        LaunchMethod.__init__(self, cfg, session)

        # We remove all ORTE related environment variables from the launcher
        # environment, so that we can use ORTE for both launch of the
        # (sub-)agent and CU execution.
        self.env_removables.extend(["OMPI_", "OPAL_", "PMIX_"])


    # --------------------------------------------------------------------------
    #
    @classmethod
    def lrms_config_hook(cls, name, cfg, lrms, logger, profiler):
        """
        FIXME: this config hook will manipulate the LRMS nodelist.  Not a nice
               thing to do, but hey... :P
               What really should be happening is that the LRMS digs information
               on node reservation out of the config and configures the node
               list accordingly.  This config hook should be limited to starting
               the DVM.
        """

        dvm_command = ru.which('orte-dvm')
        if not dvm_command:
            raise Exception("Couldn't find orte-dvm")

        # Now that we found the orte-dvm, get ORTE version
        out, err, ret = ru.sh_callout('orte-info | grep "Open RTE"', shell=True)
        orte_info = dict()
        for line in out.split('\n'):

            line = line.strip()
            if not line:
                continue

            key, val = line.split(':', 1)
            if 'Open RTE' == key.strip():
                orte_info['version'] = val.strip()
            elif  'Open RTE repo revision' == key.strip():
                orte_info['version_detail'] = val.strip()

        assert(orte_info.get('version'))
        logger.info("Found Open RTE: %s / %s",
                    orte_info['version'], orte_info.get('version_detail'))

        # Use (g)stdbuf to disable buffering.
        # We need this to get the "DVM ready",
        # without waiting for orte-dvm to complete.
        # The command seems to be generally available on our Cray's,
        # if not, we can code some home-coooked pty stuff.
        stdbuf_cmd =  ru.which(['stdbuf', 'gstdbuf'])
        if not stdbuf_cmd:
            raise Exception("Couldn't find (g)stdbuf")
        stdbuf_arg = "-oL"

        # Base command = (g)stdbuf <args> + orte-dvm + debug_args
        dvm_args = [stdbuf_cmd, stdbuf_arg, dvm_command]

        # Additional (debug) arguments to orte-dvm
        if os.environ.get('RADICAL_PILOT_ORTE_VERBOSE'):
            debug_strings = [
                             '--debug-devel',
                             '--mca odls_base_verbose 100',
                             '--mca rml_base_verbose 100'
                            ]
        else:
            debug_strings = []

        # Split up the debug strings into args and add them to the dvm_args
        [dvm_args.extend(ds.split()) for ds in debug_strings]

        vm_size = len(lrms.node_list)
        logger.info("Start DVM on %d nodes ['%s']", vm_size, ' '.join(dvm_args))
        profiler.prof(event='orte_dvm_start', uid=cfg['pilot_id'])

        dvm_uri     = None
        dvm_process = mp.Popen(dvm_args, stdout=mp.PIPE, stderr=mp.STDOUT)

        while True:

            line = dvm_process.stdout.readline().strip()

            if line.startswith('VMURI:'):

                if len(line.split(' ')) != 2:
                    raise Exception("Unknown VMURI format: %s" % line)

                label, dvm_uri = line.split(' ', 1)

                if label != 'VMURI:':
                    raise Exception("Unknown VMURI format: %s" % line)

                logger.info("ORTE DVM URI: %s" % dvm_uri)

            elif line == 'DVM ready':

                if not dvm_uri:
                    raise Exception("VMURI not found!")

                logger.info("ORTE DVM startup successful!")
                profiler.prof(event='orte_dvm_ok', uid=cfg['pilot_id'])
                break

            else:

                # Check if the process is still around,
                # and log output in debug mode.
                if dvm_process.poll() is None:
                    logger.debug("ORTE: %s", line)
                else:
                    # Process is gone: fatal!
                    raise Exception("ORTE DVM process disappeared")
                    profiler.prof(event='orte_dvm_fail', uid=cfg['pilot_id'])


        # ----------------------------------------------------------------------
        def _watch_dvm():

            logger.info('starting DVM watcher')

            retval = dvm_process.poll()
            while retval is None:
                line = dvm_process.stdout.readline().strip()
                if line:
                    logger.debug('dvm output: %s', line)
                else:
                    time.sleep(1.0)

            if retval != 0:
                # send a kill signal to the main thread.
                # We know that Python and threading are likely not to play well
                # with signals - but this is an exceptional case, and not part
                # of the stadard termination sequence.  If the signal is
                # swallowed, the next `orte-submit` call will trigger
                # termination anyway.
                os.kill(os.getpid())

            logger.info('DVM stopped (%d)' % dvm_process.returncode)
        # ----------------------------------------------------------------------

        dvm_watcher = ru.Thread(target=_watch_dvm, name="DVMWatcher")
        dvm_watcher.start()

        lm_info = {'dvm_uri'     : dvm_uri,
                   'version_info': {name: orte_info}}

        # we need to inform the actual LM instance about the DVM URI.  So we
        # pass it back to the LRMS which will keep it in an 'lm_info', which
        # will then be passed as part of the slots via the scheduler
        return lm_info


    # --------------------------------------------------------------------------
    #
    # NOTE: ORTE_LIB LM relies on the ORTE LaunchMethod's lrms_config_hook and
    # lrms_shutdown_hook. These are "always" called, as even in the ORTE_LIB
    # case we use ORTE for the sub-agent launch.
    #
    @classmethod
    def lrms_shutdown_hook(cls, name, cfg, lrms, lm_info, logger, profiler):
        """
        This hook is symmetric to the config hook above, and is called during
        shutdown sequence, for the sake of freeing allocated resources.
        """

        if 'dvm_uri' in lm_info:
            try:
                logger.info('terminating dvm')
                orterun = ru.which('orterun')
                if not orterun:
                    raise Exception("Couldn't find orterun")
                ru.sh_callout('%s --hnp %s --terminate' 
                             % (orterun, lm_info['dvm_uri']))
                profiler.prof(event='orte_dvm_stop', uid=cfg['pilot_id'])

            except Exception as e:
                # use the same event name as for runtime failures - those are
                # not distinguishable at the moment from termination failures
                profiler.prof(event='orte_dvm_fail', uid=cfg['pilot_id'], msg=e)
                logger.exception('dvm termination failed')


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        self.launch_command = ru.which('orterun')

        # Request to create a background asynchronous event loop
        os.putenv("OMPI_MCA_ess_tool_async_progress", "enabled")


    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        slots        = cu['slots']
        cud          = cu['description']
        task_exec    = cud['executable']
        task_mpi     = bool('mpi' in cud.get('cpu_process_type', '').lower())
        task_cores   = cud.get('cpu_processes', 0) + cud.get('gpu_processes', 0)
        task_env     = cud.get('environment') or dict()
        task_args    = cud.get('arguments')   or list()
        task_argstr  = self._create_arg_string(task_args)

     #  import pprint
     #  self._log.debug('prep %s', pprint.pformat(cu))
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

        # Construct the hosts_string, env vars
        hosts_string = ''
        depths       = set()
        for node in slots['nodes']:

            # On some Crays, like on ARCHER, the hostname is "archer_N".  In
            # that case we strip off the part upto and including the underscore.
            #
            # TODO: If this ever becomes a problem, i.e. we encounter "real"
            #       hostnames with underscores in it, or other hostname 
            #       mangling, we need to turn this into a system specific 
            #       regexp or so.
            
            #node_id = node[1].rsplit('_', 1)[-1] 
            node_id = node['uid'].rsplit('_', 1)[-1] 
            # add all cpu and gpu process slots to the node list.
            for cpu_slot in node['core_map']: hosts_string += '%s,' % node_id
            for gpu_slot in node['gpu_map']: hosts_string += '%s,' % node_id
            for cpu_slot in node['core_map']: depths.add(len(cpu_slot))

        assert(len(depths) == 1), depths
        depth = list(depths)[0]

        # FIXME: is this binding correct?
      # if depth > 1: map_flag = '--bind-to none --map-by ppr:%d:core' % depth
      # else        : map_flag = '--bind-to none'
        map_flag = '--bind-to none'

        # remove trailing ','
        hosts_string = hosts_string.rstrip(',')

        # Additional (debug) arguments to orterun
        if os.environ.get('RADICAL_PILOT_ORTE_VERBOSE'):
            debug_strings = ['-display-devel-map', 
                             '-display-allocation', 
                             '--debug-devel',
                             '--mca oob_base_verbose 100',
                             '--mca rml_base_verbose 100'
                            ]
        else:
            debug_strings = []
        debug_string = ' '.join(debug_strings)

        if task_mpi: np_flag = '-np %s' % task_cores
        else       : np_flag = '-np 1'

        command = '%s %s %s %s -host %s %s' \
                % (self.launch_command, debug_string, 
                   np_flag, map_flag, hosts_string, env_string)

        return command, task_command


# ------------------------------------------------------------------------------

