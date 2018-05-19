
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os

import radical.utils as ru

from .base import LaunchMethod


# ==============================================================================
#
class SSH(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        LaunchMethod.__init__(self, cfg, session)

        # Instruct the ExecWorkers to unset this environment variable.
        # Otherwise this will break nested SSH with SHELL spawner, i.e. when
        # both the sub-agent and CUs are started using SSH.
        self.env_removables.extend(["RP_SPAWNER_HOP"])


    # --------------------------------------------------------------------------
    #
    def _configure(self):
        # Find ssh command
        command = ru.which('ssh')

        if command is not None:

            # Some MPI environments (e.g. SGE) put a link to rsh as "ssh" into
            # the path.  We try to detect that and then use different arguments.
            if os.path.islink(command):

                target = os.path.realpath(command)

                if os.path.basename(target) == 'rsh':
                    self._log.info('Detected that "ssh" is a link to "rsh".')
                    return target

            command = '%s -o StrictHostKeyChecking=no -o ControlMaster=auto' % command

        self.launch_command = command


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

        if not launch_script_hop :
            raise ValueError ("LaunchMethodSSH.construct_command needs launch_script_hop!")

        # Get the host of the first entry in the acquired slot
        host = task_slots[0].split(':')[0]

        if task_argstr:
            task_command = "%s %s" % (task_exec, task_argstr)
        else:
            task_command = task_exec


        env_string = ''
        env_list   = self.EXPORT_ENV_VARIABLES + task_env.keys()
        if env_list:
            # this is a crude version of env transplanting where we prep the
            # shell command line.  We likely won't survive any complicated vars
            # (multiline, quotes, etc)
            env_string = ' '
            for var in task_env:
                env_string += '%s="$%s" ' % (var, var)


        # Command line to execute launch script via ssh on host
        ssh_hop_cmd = "%s %s %s %s" % (self.launch_command, host, env_string,
                launch_script_hop)

        # Special case, return a tuple that overrides the default command line.
        return task_command, ssh_hop_cmd


# ------------------------------------------------------------------------------

