
__copyright__ = 'Copyright 2016-2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'


import radical.utils as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class JSRUN(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, lm_cfg, rm_info, log, prof):

        self._command: str = ''

        LaunchMethod.__init__(self, name, lm_cfg, rm_info, log, prof)


    # --------------------------------------------------------------------------
    #
    def _init_from_scratch(self, env, env_sh):

        lm_info = {'env'    : env,
                   'env_sh' : env_sh,
                   'command': ru.which('jsrun')}

        return lm_info


    # --------------------------------------------------------------------------
    #
    def _init_from_info(self, lm_info):

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

        uid = task['uid']
        td  = task['description']

        self._log.debug('prep %s', uid)

        cmd_options = '–brs -n%(ranks)d -a1 '   \
                      '-c%(cores_per_rank)d' % td

        if td['gpus_per_rank']:

            cmd_options += ' -g%(gpus_per_rank)d' % td

            # from https://www.olcf.ornl.gov/ \
            #             wp-content/uploads/2018/11/multi-gpu-workshop.pdf
            #
            # CUDA with    MPI, use jsrun --smpiargs="-gpu"
            # CUDA without MPI, use jsrun --smpiargs="off"
            #
            # This is set for CUDA tasks only
            if 'cuda' in td.get('gpu_type', '').lower():
                if td['ranks'] > 1:
                    # MPI is enabled
                    cmd_options += ' --smpiargs="-gpu"'
                else:
                    cmd_options += ' --smpiargs="off"'

        cmd = '%s %s %s' % (self._command, cmd_options, exec_path)
        return cmd.rstrip()


    # --------------------------------------------------------------------------
    #
    def get_rank_cmd(self):

        # FIXME: does JSRUN set a rank env?
        ret  = 'test -z "$MPI_RANK"  || export RP_RANK=$MPI_RANK\n'
        ret += 'test -z "$PMIX_RANK" || export RP_RANK=$PMIX_RANK\n'

        return ret


    # --------------------------------------------------------------------------
    #
    def get_rank_exec(self, task, rank_id, rank):

        td          = task['description']
        task_exec   = td['executable']
        task_args   = td.get('arguments')
        task_argstr = self._create_arg_string(task_args)
        command     = '%s %s' % (task_exec, task_argstr)

        return command.rstrip()


# ------------------------------------------------------------------------------

