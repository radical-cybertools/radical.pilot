
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
class ORTE(LaunchMethod):

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
        debug_strings = [
            #'--debug-devel',
            #'--mca odls_base_verbose 100',
            #'--mca rml_base_verbose 100',
        ]
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
        # will then be passed as part of the slots via the scheduler
        return lm_info


    # --------------------------------------------------------------------------
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
                orterun = ru.which('orterun')
                if not orterun:
                    raise Exception("Couldn't find orterun")
                subprocess.Popen([orterun, "--hnp", lm_info['dvm_uri'], "--terminate"])
            except Exception as e:
                logger.exception('dmv termination failed')


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        self.launch_command = ru.which('orterun')


    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        slots        = cu['slots']
        cud          = cu['description']
        task_exec    = cud['executable']
        task_args    = cud.get('arguments', [])
        task_cores   = cud.get('cpu_processes', 0) + cud.get('gpu_processes', 0)
        task_mpi     = bool('mpi' in cud.get('cpu_process_type', '').lower())

     #  import pprint
     #  self._log.debug('prep %s', pprint.pformat(cu))
        self._log.debug('prep %s', cu['uid'])

        task_argstr  = self._create_arg_string(task_args)

        if 'dvm_uri' not in slots['lm_info']:
            raise RuntimeError('dvm_uri not in lm_info for %s: %s' \
                    % (self.name, slots))

        dvm_uri    = slots['lm_info']['dvm_uri']

        if task_argstr:
            task_command = "%s %s" % (task_exec, task_argstr)
        else:
            task_command = task_exec

        # Construct the hosts_string, env vars
        hosts_string = ''
        for node in slots['nodes']:

            # On some Crays, like on ARCHER, the hostname is "archer_N".  In
            # that case we strip off the part upto and including the underscore.
            #
            # TODO: If this ever becomes a problem, i.e. we encounter "real"
            #       hostnames with underscores in it, or other hostname 
            #       mangling, we need to turn this into a system specific 
            #       regexp or so.
            node_id = node[1].rsplit('_', 1)[-1] 

            # add all cpu and gpu process slots to the node list.
            for cpu_slot in node[2]: hosts_string += '%s,' % node_id
            for gpu_slot in node[3]: hosts_string += '%s,' % node_id

        # remove last ','
        hosts_string = hosts_string.rstrip(',')
        export_vars  = ' '.join(['-x ' + var \
                                 for var in self.EXPORT_ENV_VARIABLES \
                                 if  var in os.environ])

        # Additional (debug) arguments to orterun
        debug_strings = [
            #'--debug-devel',
            #'--mca oob_base_verbose 100',
            #'--mca rml_base_verbose 100'
        ]

        if task_mpi: np_flag = '-np %s' % task_cores
        else       : np_flag = '-np 1'

        orte_command = '%s %s --hnp "%s" %s --bind-to none %s -host %s %s' % \
                (self.launch_command, ' '.join(debug_strings), dvm_uri, 
                 export_vars, np_flag, hosts_string, task_command)

        return orte_command, None


# ------------------------------------------------------------------------------

