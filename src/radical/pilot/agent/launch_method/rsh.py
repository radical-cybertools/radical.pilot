
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
    def __init__(self, name, cfg, session):

        LaunchMethod.__init__(self, name, cfg, session)

        # Instruct the ExecWorkers to unset this environment variable.
        # Otherwise this will break nested RSH with SHELL spawner, i.e. when
        # both the sub-agent and CUs are started using RSH.
        self.env_removables.extend(["RP_SPAWNER_HOP"])


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        # Find rsh command
        self.launch_command = ru.which('rsh')

        if not self.launch_command:
            raise RuntimeError("rsh not found!")


    # --------------------------------------------------------------------------
    #
    def construct_command(self, t, launch_script_hop):

        slots        = t['slots']
        td          = t['description']
        task_exec    = td['executable']
        task_env     = td.get('environment') or dict()
        task_args    = td.get('arguments')   or list()
        task_argstr  = self._create_arg_string(task_args)

        if task_argstr: task_command = "%s %s" % (task_exec, task_argstr)
        else          : task_command = task_exec

        if not launch_script_hop:
            raise ValueError("RSH launch method needs launch_script_hop!")

        if 'nodes' not in slots:
            raise RuntimeError('insufficient information to launch via %s: %s'
                              % (self.name, slots))

        if len(slots['nodes']) > 1:
            raise RuntimeError('rsh cannot run multinode tasks')

        host = slots['nodes'][0]['name']

        # Pass configured and available environment variables to the remote shell
        export_vars  = ' '.join(['%s=%s' % (var, os.environ[var])
                                 for var in self.EXPORT_ENV_VARIABLES
                                  if var in os.environ])
        export_vars += ' '.join(['%s=%s' % (var, task_env[var])
                                 for var in task_env])

        # Command line to execute launch script via rsh on host
        rsh_hop_cmd = "%s %s %s %s" % (self.launch_command, host,
                                       export_vars, launch_script_hop)

        return task_command, rsh_hop_cmd


# ------------------------------------------------------------------------------

