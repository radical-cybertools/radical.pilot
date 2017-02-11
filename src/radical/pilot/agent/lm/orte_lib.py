
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import time
import threading
import subprocess

from .base import LaunchMethod


# ==============================================================================
#
# NOTE: This requires a development version of Open MPI
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
    # NOTE: ORTE_LIB LM relies on the ORTE LaunchMethod's lrms_config_hook and
    # lrms_shutdown_hook. These are "always" called, as even in the ORTE_LIB
    # case we use ORTE for the sub-agent launch.
    #
    # --------------------------------------------------------------------------


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        self.launch_command = self._which('orterun')

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
        task_slots   = opaque_slots.get('task_slots')

        if not task_slots:
            raise RuntimeError('No task_slots to launch via %s: %s' \
                               % (self.name, opaque_slots))

        nodes = [slot.split(':')[0].rsplit('_', 1)[-1] for slot in task_slots]

        if task_argstr:
            task_command = "%s %s" % (task_exec, task_argstr)
        else:
            task_command = task_exec

        export_vars = ' '.join(['-x ' + var for var in self.EXPORT_ENV_VARIABLES if var in os.environ])

        # Construct the hosts_string
        #
        # On some Crays, like on ARCHER, the hostname is "archer_N".
        # In that case we strip off the part upto and including the underscore.
        #
        # TODO: If this ever becomes a problem, i.e. we encounter "real" hostnames
        #       with underscores in it, or other hostname mangling, we need to turn
        #       this into a system specific regexp or so.
        #
        if task_mpi:
            # we create one process per given task slot.  In this case we don't
            # care if the task slots are on the same node or not
            hosts_string = ",".join(nodes)
        else:
            # this is not an MPI task, so we only start *one* process on the
            # given set of slots, and expect the application itself to spread
            # onto the other slots.  For that to be possible we expect all task
            # slots to reside on the same node.
            if len(set(nodes)) != 1:
                self._log.error( 'non-MPI CUs cannot span nodes %s', task_slots)
                raise ValueError('non-MPI CUs cannot span nodes %s'% task_slots)
            hosts_string = nodes[0]

        # Additional (debug) arguments to orterun
        debug_strings = [
            #'--debug-devel',
            #'--mca oob_base_verbose 100',
            #'--mca rml_base_verbose 100'
        ]
        orte_command = '%s %s %s --bind-to none -np %d -host %s' % (
            self.launch_command, ' '.join(debug_strings), export_vars, task_cores if task_mpi else 1, hosts_string)

        return orte_command, task_command


# ------------------------------------------------------------------------------

