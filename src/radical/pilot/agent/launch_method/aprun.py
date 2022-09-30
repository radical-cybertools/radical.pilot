
__copyright__ = 'Copyright 2016-2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

import radical.utils as ru

from .base import LaunchMethod


# ----- TO BE MOVED TO THE LM BASE MODULE vvv-----------------------------------
#
class LMOptionsBaseMeta(type):

    # --------------------------------------------------------------------------
    #
    def __new__(mcs, name, bases, namespace):

        if name != 'LMOptions':

            base_schema = {}
            for _cls in bases:
                _cls_v = getattr(_cls, '_schema', None)
                if _cls_v is not None:
                    base_schema.update(_cls_v)

            mapping = namespace.get('_mapping')
            if not mapping:
                raise Exception('mapping not provided')

            namespace['_schema'].clear()
            for k in list(mapping):
                if k not in base_schema:
                    del mapping[k]
                namespace['_schema'][k] = base_schema[k]

        return super().__new__(mcs, name, bases, namespace)


# ------------------------------------------------------------------------------
#
class LMOptionsMeta(ru.TypedDictMeta, LMOptionsBaseMeta):
    pass


# ------------------------------------------------------------------------------
#
class LMOptions(ru.TypedDict, metaclass=LMOptionsMeta):

    _delimiter = ' '      # could be set as '='
    _mapping   = {}
    _schema    = {        # provides all possible options
        'ranks'           : int,
        'ranks_per_node'  : int,
        'threads_per_rank': int,
        'threads_per_core': int,
        'reserved_cores'  : int
    }

    # --------------------------------------------------------------------------
    #
    def __init__(self, from_dict=None, options=None):

        self.__dict__['_valid_options'] = set(options or list(self._schema))

        super().__init__(from_dict=from_dict)

    # --------------------------------------------------------------------------
    #
    def __str__(self):

        options = []
        for k, o in self._mapping.items():

            if k not in self._valid_options:
                continue

            v = getattr(self, k)
            if v is None or v is False:
                continue
            elif v is True:
                options.append('%s' % o)
            else:
                if isinstance(v, (list, tuple)):
                    v = ','.join(v)
                options.append('%s%s%s' % (o, self._delimiter, v))

        return ' '.join(options)

# ----- TO BE MOVED TO THE LM BASE MODULE ^^^-----------------------------------


# ------------------------------------------------------------------------------
#
class APRunOptions(LMOptions):

    _mapping = {
        'ranks_per_node'  : '-N',  # number of MPI ranks per node
        'ranks'           : '-n',  # total number of MPI ranks
        'threads_per_rank': '-d',  # number of hyperthreads per MPI rank (depth)
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

        td      = task['description']
        n_ranks = td['ranks']
        n_cores = td.get('cores_per_rank', 1)

        ranks_per_node = os.environ.get('SAGA_PPN') or n_ranks
        ranks_per_node = min(n_ranks, int(ranks_per_node))

        options = APRunOptions({'ranks_per_node'  : ranks_per_node,
                                'ranks'           : n_ranks,
                                'threads_per_rank': n_cores})

        # get configurable options
        cfg_options = self._lm_cfg.get('options', {})
        options.reserved_cores = cfg_options.get('reserved_cores')

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

        cmd = '%s %s %s' % (self._command, str(options), exec_path)
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

