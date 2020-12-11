
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os

import radical.utils as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class SSH(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, cfg, session):

        LaunchMethod.__init__(self, name, cfg, session)

        # Instruct the ExecWorkers to unset this environment variable.
        # Otherwise this will break nested SSH with SHELL spawner, i.e. when
        # both the sub-agent and CUs are started using SSH.
        self.env_removables.extend(["RP_SPAWNER_HOP"])


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        command = ru.which('ssh')

        if not command:
            raise RuntimeError("ssh not found!")

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
    def construct_command(self, t, launch_script_hop):

        slots        = t['slots']
        td          = t['description']
        task_exec    = td['executable']
        task_env     = td.get('environment') or dict()
        task_args    = td.get('arguments')   or list()
        task_argstr  = self._create_arg_string(task_args)

        if task_argstr: task_command = "%s %s" % (task_exec, task_argstr)
        else          : task_command = task_exec

        if not launch_script_hop :
            raise ValueError ("LMSSH.construct_command needs launch_script_hop!")

        if 'nodes' not in slots:
            raise RuntimeError('insufficient information to launch via %s: %s'
                              % (self.name, slots))

        if len(slots['nodes']) > 1:
            raise RuntimeError('ssh cannot run multinode tasks')

        host = slots['nodes'][0]['name']

        # Pass configured and available environment variables.
        # This is a crude version of env transplanting where we prep the
        # shell command line.  We likely won't survive any complicated vars
        # (multiline, quotes, etc)
        env_string  = ' '.join(['%s=%s' % (var, os.environ[var])
                                for var in self.EXPORT_ENV_VARIABLES
                                if  var in os.environ])
        env_string += ' '.join(['%s=%s' % (var, task_env[var])
                                for var in task_env])

        ssh_hop_cmd = "%s %s %s %s" % (self.launch_command, host, env_string,
                                       launch_script_hop)

        return task_command, ssh_hop_cmd


# ------------------------------------------------------------------------------

