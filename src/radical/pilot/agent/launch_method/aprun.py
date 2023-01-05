
__copyright__ = 'Copyright 2016-2023, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

import radical.utils as ru

from .base import LaunchMethod, LaunchMethodOptions


# ------------------------------------------------------------------------------
#
class APRunOptions(LaunchMethodOptions):

    _mapping = {
        'ranks_per_node'  : '-N',  # number of MPI ranks per node
        'ranks'           : '-n',  # total number of MPI ranks
        'cores_per_rank'  : '-d',  # number of hyperthreads per MPI rank (depth)
        'threads_per_core': '–j',  # number of hyperthreads per core
        'reserved_cores'  : '-r'   # core specialization
    }

# options to be considered:
# --cc depth    - MPI rank and thread placement
# -e <env_var>  - environment variables


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
        ranks          = td['ranks']
        cores_per_rank = td.get('cores_per_rank', 1)

        ranks_per_node = os.environ.get('SAGA_PPN') or ranks
        ranks_per_node = min(ranks, int(ranks_per_node))

        options = APRunOptions({'ranks_per_node': ranks_per_node,
                                'ranks'         : ranks,
                                'cores_per_rank': cores_per_rank})

        # get configurable options
        cfg_options = self._lm_cfg.get('options', {})
        options.reserved_cores = cfg_options.get('reserved_cores')

        # CPU affinity binding
        # - use –d and --cc depth to let ALPS control affinity
        # - use --cc none if you want to use OpenMP (or KMP) env. variables
        #   to specify affinity: --cc none -e KMP_AFFINITY=<affinity>
        #   (*) turn off thread affinity: export KMP_AFFINITY=none
        #
        # smt = os.environ.get('RADICAL_SMT')
        # if smt:
        #     options.threads_per_core = int(smt)
        #     # update `self._schema` first for attribute "depth"
        #     #   options.depth = 'depth'

        # `share` mode access restricts the application specific cpuset
        # contents to only the application reserved cores and memory on NUMA
        # node boundaries, meaning the application will not have access to
        # cores and memory on other NUMA nodes on that compute node.
        #
        # slots = task['slots']
        # nodes = set([rank['node_name'] for rank in slots['ranks']])
        # if len(nodes) < 2:
        #     # update `self._schema` first for attribute "share"
        #     #   options.share = 'share' -> "-F share" (default is `exclusive`)
        # # update `self._schema` first for attribute "nodelist"
        # #   options.nodelist = nodes -> "-L <node0>,<node1>"

        # task_env = td['environment']
        # cmd_options += ''.join([' -e %s=%s' % x for x in task_env.items()])
        # if td['cores_per_rank'] > 1 and 'OMP_NUM_THREADS' not in task_env:
        #     cmd_options += ' -e OMP_NUM_THREADS=%(cores_per_rank)s' % td

        cmd = '%s %s %s' % (self._command, options, exec_path)
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
    def get_exec(self, task):

        td           = task['description']
        task_exec    = td['executable']
        task_args    = td['arguments']
        task_argstr  = self._create_arg_string(task_args)
        command      = '%s %s' % (task_exec, task_argstr)

        return command.rstrip()


# ------------------------------------------------------------------------------

