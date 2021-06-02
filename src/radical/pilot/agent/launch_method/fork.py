
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import radical.utils as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class Fork(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, lm_cfg, cfg, log, prof):

        self.node_name: str = ru.get_hostname() or ''

        LaunchMethod.__init__(self, name, lm_cfg, cfg, log, prof)


    # --------------------------------------------------------------------------
    #
    def _init_from_scratch(self, lm_cfg, env, env_sh):

        lm_info = {'env'   : env,
                   'env_sh': env_sh}

        return lm_info


    # --------------------------------------------------------------------------
    #
    def _init_from_info(self, lm_info, lm_cfg):

        self._env    = lm_info['env']
        self._env_sh = lm_info['env_sh']


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        pass


    # --------------------------------------------------------------------------
    #
    def can_launch(self, task):

        # ensure single rank on localhost
        if len(task['slots']['ranks']) > 1:
            return False

        node = task['slots']['ranks'][0]['node']
        if node not in ['localhost', self.node_name]:
            return False

        return True


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

        return 'export RP_RANK=0'


    # --------------------------------------------------------------------------
    #
    def get_rank_exec(self, task, rank_id, rank):

        td          = task['description']
        task_exec   = td['executable']
        task_args   = td.get('arguments')
        task_argstr = self._create_arg_string(task_args)
        command     = "%s %s" % (task_exec, task_argstr)

        return command.rstrip()


# ------------------------------------------------------------------------------

