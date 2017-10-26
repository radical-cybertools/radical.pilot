
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import time
import threading
import subprocess

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
    def lrms_config_hook(cls, name, cfg, lrms, logger):
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
        orte_info = {}
        oi_output = subprocess.check_output(['orte-info|grep "Open RTE"'], shell=True)
        oi_lines = oi_output.split('\n')
        for line in oi_lines:
            if not line:
                continue
            key, val = line.split(':')
            if 'Open RTE' == key.strip():
                orte_info['version'] = val.strip()
            elif  'Open RTE repo revision' == key.strip():
                orte_info['version_detail'] = val.strip()
        logger.info("Found Open RTE: %s / %s",
                    orte_info['version'], orte_info['version_detail'])

        # Use (g)stdbuf to disable buffering.
        # We need this to get the "DVM ready",
        # without waiting for orte-dvm to complete.
        # The command seems to be generally available on our Cray's,
        # if not, we can code some home-coooked pty stuff.
        stdbuf_cmd =  cls._find_executable(['stdbuf', 'gstdbuf'])
        if not stdbuf_cmd:
            raise Exception("Couldn't find (g)stdbuf")
        stdbuf_arg = "-oL"

        # Base command = (g)stdbuf <args> + orte-dvm + debug_args
        dvm_args = [stdbuf_cmd, stdbuf_arg, dvm_command]

        # Additional (debug) arguments to orte-dvm
        if os.environ.get('RADICAL_PILOT_ORTE_VERBOSE'):
            debug_strings = [ # '--debug-devel',
                              # '--mca odls_base_verbose 100',
                              # '--mca rml_base_verbose 100'
                            ]
        else:
            debug_strings = []

        # Split up the debug strings into args and add them to the dvm_args
        [dvm_args.extend(ds.split()) for ds in debug_strings]

        vm_size = len(lrms.node_list)
        logger.info("Starting ORTE DVM on %d nodes with '%s' ...", vm_size, ' '.join(dvm_args))
        dvm_process = subprocess.Popen(dvm_args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        dvm_uri = None
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
                break

            else:

                # Check if the process is still around,
                # and log output in debug mode.
                if None == dvm_process.poll():
                    logger.debug("ORTE: %s" % line)
                else:
                    # Process is gone: fatal!
                    raise Exception("ORTE DVM process disappeared")


        # ----------------------------------------------------------------------
        def _watch_dvm():

            logger.info('starting DVM watcher')

            retval = dvm_process.poll()
            while retval is None:
                line = dvm_process.stdout.readline().strip()
                if line:
                    logger.debug('dvm output: %s' % line)
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
        # will then be passed as part of the opaque_slots via the scheduler
        return lm_info


    # --------------------------------------------------------------------------
    #
    # NOTE: ORTE_LIB LM relies on the ORTE LaunchMethod's lrms_config_hook and
    # lrms_shutdown_hook. These are "always" called, as even in the ORTE_LIB
    # case we use ORTE for the sub-agent launch.
    #
    @classmethod
    def lrms_shutdown_hook(cls, name, cfg, lrms, lm_info, logger):
        """
        This hook is symmetric to the config hook above, and is called during
        shutdown sequence, for the sake of freeing allocated resources.
        """

        if 'dvm_uri' in lm_info:
            try:
                logger.info('terminating dvm')
                orte_submit = ru.which('orterun')
                if not orte_submit:
                    raise Exception("Couldn't find orterun")
                subprocess.Popen([orte_submit, "--hnp", lm_info['dvm_uri'], "--terminate"])

            except Exception as e:
                logger.exception('dmv termination failed')


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        self.launch_command = ru.which('orterun')

        # Request to create a background asynchronous event loop
        os.putenv("OMPI_MCA_ess_tool_async_progress", "enabled")


    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        opaque_slots = cu['opaque_slots']
        cud          = cu['description']
        task_exec    = cud['executable']
        task_cores   = cud['cores']
        task_mpi     = cud.get('mpi')       or False
        task_args    = cud.get('arguments') or []
        task_argstr  = self._create_arg_string(task_args)

        if 'task_slots' not in opaque_slots:
            raise RuntimeError('No task_slots to launch via %s: %s' \
                               % (self.name, opaque_slots))

        task_slots = opaque_slots['task_slots']

        if task_argstr:
            task_command = "%s %s" % (task_exec, task_argstr)
        else:
            task_command = task_exec

        # Construct the hosts_string, env vars
        # On some Crays, like on ARCHER, the hostname is "archer_N".
        # In that case we strip off the part upto and including the underscore.
        #
        # TODO: If this ever becomes a problem, i.e. we encounter "real" hostnames
        #       with underscores in it, or other hostname mangling, we need to turn
        #       this into a system specific regexp or so.
        #
        hosts_string = ",".join([slot.split(':')[0].rsplit('_', 1)[-1] for slot in task_slots])
        export_vars  = ' '.join(['-x ' + var for var in self.EXPORT_ENV_VARIABLES if var in os.environ])

        # Additional (debug) arguments to orterun
        if os.environ.get('RADICAL_PILOT_ORTE_VERBOSE'):
            debug_strings = [ #  '--debug-devel',
                              #  '--mca oob_base_verbose 100',
                              #  '--mca rml_base_verbose 100'
                            ]
        else:
            debug_strings = [ # '--debug-devel',
                              # '--mca oob_base_verbose 100',
                              # '--mca rml_base_verbose 100'
                            ]

        orte             # _command = '%s %s %s --bind-to none -np %d -host %s' % (
            self.launch_command, ' '.join(debug_strings), export_vars, 
                           task_cores if task_mpi else 1, hosts_string)

        return orte_command, task_command
