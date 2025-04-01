
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
    def __init__(self, name, lm_cfg, rm_info, log, prof):

        LaunchMethod.__init__(self, name, lm_cfg, rm_info, log, prof)


    # --------------------------------------------------------------------------
    #
    def init_from_scratch(self, env, env_sh):

        command = ru.which('ssh')

        if not command:
            raise RuntimeError("ssh not found!")

        # Some MPI environments (e.g. SGE) put a link to rsh as "ssh" into
        # the path.  We try to detect that and then use different arguments.
        is_rsh = False
        if os.path.islink(command):

            command = os.path.realpath(command)

            if os.path.basename(command) == 'rsh':
                self._log.info('Detected that "ssh" is a link to "rsh".')
                is_rsh = True

        if not is_rsh:
            command += ' -o StrictHostKeyChecking=no -o ControlMaster=auto'

        lm_info = {'env'    : env,
                   'env_sh' : env_sh,
                   'command': command}

        return lm_info


    # --------------------------------------------------------------------------
    #
    def init_from_info(self, lm_info):

        self._env     = lm_info['env']
        self._env_sh  = lm_info['env_sh']
        self._command = lm_info['command']

        assert self._command


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        pass


    # --------------------------------------------------------------------------
    #
    def can_launch(self, task):

        # ensure single rank
        if len(task['slots']) > 1:
            return False, 'more than one rank'

        # ensure non-MPI
        if task['description'].get('use_mpi'):
            return False, 'cannot launch MPI tasks'

        if not task['description']['executable']:
            return False, 'no executable'

        return True, ''


    # --------------------------------------------------------------------------
    #
    def get_launcher_env(self):

        return ['. $RP_PILOT_SANDBOX/%s' % self._env_sh]


    # --------------------------------------------------------------------------
    #
    def get_launch_cmds(self, task, exec_path):

        slots = task['slots']

        if len(slots) != 1:
            raise RuntimeError('ssh cannot run multi-rank tasks')

        host = slots[0]['node_name']
        cmd  = '%s %s %s' % (self._command, host, exec_path)
        return cmd.rstrip()


    # --------------------------------------------------------------------------
    #
    def get_rank_cmd(self):

        return 'export RP_RANK=0\n'


# ------------------------------------------------------------------------------

