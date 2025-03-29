
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import radical.utils as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class Fork(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, lm_cfg, rm_info, log, prof):

        self.node_name: str = ru.get_hostname() or ''

        LaunchMethod.__init__(self, name, lm_cfg, rm_info, log, prof)


    # --------------------------------------------------------------------------
    #
    def init_from_scratch(self, env, env_sh):

        lm_info = {'env'   : env,
                   'env_sh': env_sh}

        return lm_info


    # --------------------------------------------------------------------------
    #
    def init_from_info(self, lm_info):

        self._env    = lm_info['env']
        self._env_sh = lm_info['env_sh']


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        pass


    # --------------------------------------------------------------------------
    #
    def can_launch(self, task):

        if len(task['slots']) > 1:
            return False, 'more than one rank'

        node = task['slots'][0]['node_name']
        if node not in ['localhost', self.node_name]:
            return False, 'not on localhost'

        if task['description']['use_mpi']:
            return False, 'cannot launch MPI tasks'

        if task['description']['ranks'] > 1:
            return False, 'needs MPI'

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

        return exec_path


    # --------------------------------------------------------------------------
    #
    def get_rank_cmd(self):

        return 'export RP_RANK=0\n'


# ------------------------------------------------------------------------------

