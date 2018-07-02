
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import radical.utils as ru

from .base import LaunchMethod


# ==============================================================================
#
class CCMRun(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        LaunchMethod.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):
        # ccmrun: Cluster Compatibility Mode (CCM) job launcher for Cray systems
        self.launch_command = ru.which('ccmrun')


    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        slots        = cu['slots']
        cud          = cu['description']
        task_exec    = cud['executable']
        task_cores   = cud['cpu_processes']  # FIXME: cpu_threads
        task_args    = cud.get('arguments') or []
        task_argstr  = self._create_arg_string(task_args)

        if task_argstr:
            task_command = "%s %s" % (task_exec, task_argstr)
        else:
            task_command = task_exec

        ccmrun_command = "%s -n %d %s" % (self.launch_command, task_cores, task_command)

        return ccmrun_command, None


# ------------------------------------------------------------------------------

