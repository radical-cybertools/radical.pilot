
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import radical.utils as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class RSH(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, logger):

        LaunchMethod.__init__(self, cfg, logger)

        # Instruct the ExecWorkers to unset this environment variable.
        # Otherwise this will break nested RSH with SHELL spawner, i.e. when
        # both the sub-agent and CUs are started using RSH.
        self.env_removables.extend(["RP_SPAWNER_HOP"])


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        # Find rsh command
        self.launch_command = ru.which('rsh')


    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        opaque_slots = cu['opaque_slots']
        cud          = cu['description']
        task_exec    = cud['executable']
        task_cores   = cud['cores']
        task_env     = cud.get('environment') or dict()
        task_args    = cud.get('arguments')   or list()
        task_argstr  = self._create_arg_string(task_args)

        if not 'task_slots' in opaque_slots:
            raise RuntimeError('insufficient information to launch via %s: %s' \
                    % (self.name, opaque_slots))

        task_slots = opaque_slots['task_slots']

        if not launch_script_hop:
            raise ValueError("RSH launch method needs launch_script_hop!")

        # Get the host of the first entry in the acquired slot
        host = task_slots[0].split(':')[0]

        if task_argstr: task_command = "%s %s" % (task_exec, task_argstr)
        else          : task_command = task_exec

        # Pass configured and available environment variables to the remote shell
        export_vars = ' '.join(['%s=%s' % (var, os.environ[var]) 
                                for var in self.EXPORT_ENV_VARIABLES 
                                 if var in os.environ])

        # Command line to execute launch script via rsh on host
        rsh_hop_cmd = "%s %s %s %s" % (self.launch_command, host, 
                                       export_vars, launch_script_hop)

        # Special case, return a tuple that overrides the default command line.
        return task_command, rsh_hop_cmd


# ------------------------------------------------------------------------------

