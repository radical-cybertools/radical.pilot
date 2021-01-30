
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
    def construct_command(self, cu, launch_script_hop):

        slots        = cu['slots']
        cud          = cu['description']
        task_exec    = cud['executable']
        task_env     = cud.get('environment') or dict()
        task_args    = cud.get('arguments')   or list()
        task_argstr  = self._create_arg_string(task_args)

        if task_argstr: task_command = "%s %s" % (task_exec, task_argstr)
        else          : task_command = task_exec

        if not launch_script_hop :
            raise ValueError ("LMSSH.construct_command needs launch_script_hop!")

        if 'ranks' not in slots:
            raise RuntimeError('insufficient information to launch via %s: %s'
                              % (self.name, slots))

        if len(slots['ranks']) > 1:
            raise RuntimeError('ssh cannot run multinode units')

        host = slots['ranks'][0]['node']

        ssh_hop_cmd = "%s %s %s" % (self.launch_command, host, launch_script_hop)

        return task_command, ssh_hop_cmd


# ------------------------------------------------------------------------------

