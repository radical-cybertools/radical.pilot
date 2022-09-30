
__copyright__ = 'Copyright 2016-2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

import radical.utils as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
# aprun: job launcher for Cray systems (alps-run)
# TODO : ensure that only one concurrent aprun per node is executed!
#
class APRun(LaunchMethod):

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
                   'command': ru.which('aprun')}

        return lm_info


    # --------------------------------------------------------------------------
    #
    def _init_from_info(self, lm_info):

        self._env         = lm_info['env']
        self._env_sh      = lm_info['env_sh']
        self._command     = lm_info['command']

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

        td             = task['description']

        n_tasks        = td['ranks']
        n_task_threads = td.get('cores_per_rank', 1)

        # aprun options
        # –  Number of MPI ranks per node:                –N <n_ranks_per_node>
        # –  Total number of MPI ranks:                   –n <n_ranks_total>
        # –  Number of hyperthreads per MPI rank (depth): –d <n_rank_threads>
        # –  Number of hyperthreads per core:             –j <n_hwthreads>
        # –  MPI rank and thread placement:               --cc depth
        # –  Environment variables:                       -e <env_var>
        # –  Core specialization:                         -r <n_threads>

        rpn = os.environ.get('SAGA_PPN') or n_tasks
        rpn = min(n_tasks, int(rpn))

        cmd_options = '-N %s ' % rpn + \
                      '-n %s ' % n_tasks + \
                      '-d %s'  % n_task_threads

        # CPU affinity binding
        # - use –d and --cc depth to let ALPS control affinity
        # - use --cc none if you want to use OpenMP (or KMP) env. variables
        #   to specify affinity: --cc none -e KMP_AFFINITY=<affinity>
        #   (*) turn off thread affinity: export KMP_AFFINITY=none
        #
        # saga_smt = os.environ.get('RADICAL_SAGA_SMT')
        # if saga_smt:
        #     cmd_options += ' -j %s' % saga_smt
        #     cmd_options += ' --cc depth'

        # `share` mode access restricts the application specific cpuset
        # contents to only the application reserved cores and memory on NUMA
        # node boundaries, meaning the application will not have access to
        # cores and memory on other NUMA nodes on that compute node.
        #
        # slots = task['slots']
        # nodes = set([rank['node_name'] for rank in slots['ranks']])
        # if len(nodes) < 2:
        #     cmd_options += ' -F share'  # default is `exclusive`
        # cmd_options += ' -L %s ' % ','.join(nodes)

        # task_env = td['environment']
        # cmd_options += ''.join([' -e %s=%s' % x for x in task_env.items()])
        # if td['cores_per_rank'] > 1 and 'OMP_NUM_THREADS' not in task_env:
        #     cmd_options += ' -e OMP_NUM_THREADS=%(cores_per_rank)s' % td

        cmd = '%s %s %s' % (self._command, cmd_options, exec_path)
        return cmd.rstrip()


    # --------------------------------------------------------------------------
    #
    def get_rank_cmd(self):

        ret  = 'test -z "$MPI_RANK"    || export RP_RANK=$MPI_RANK\n'
        ret += 'test -z "$PMIX_RANK"   || export RP_RANK=$PMIX_RANK\n'
        ret += 'test -z "$ALPS_APP_PE" || export RP_RANK=$ALPS_APP_PE\n'

        return ret


    # --------------------------------------------------------------------------
    #
    def get_rank_exec(self, task, rank_id, rank):

        td           = task['description']
        task_exec    = td['executable']
        task_args    = td['arguments']
        task_argstr  = self._create_arg_string(task_args)
        command      = '%s %s' % (task_exec, task_argstr)

        return command.rstrip()


# ------------------------------------------------------------------------------

