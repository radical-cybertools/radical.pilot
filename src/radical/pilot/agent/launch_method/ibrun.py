
__copyright__ = 'Copyright 2016-2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'


import radical.utils as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
# ibrun: TACC's wrapper around system specific launchers just as srun, mpirun...
#
class IBRun(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, lm_cfg, rm_info, log, prof):

        self._command: str  = ''

        LaunchMethod.__init__(self, name, lm_cfg, rm_info, log, prof)


    # --------------------------------------------------------------------------
    #
    def init_from_scratch(self, env, env_sh):

        lm_info = {'env'    : env,
                   'env_sh' : env_sh,
                   'command': ru.which('ibrun')}

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
        assert slots, 'task.slots is not set or empty'

        td                 = task['description']
        n_ranks            = td['ranks']
        n_threads_per_rank = td['cores_per_rank']

        # Usage of env variable IBRUN_TASKS_PER_NODE is purely for MPI tasks,
        # threads are not considered (info provided by TACC support)
        tasks_per_node = self._lm_cfg.get('options', {}).get('tasks_per_node')
        if not tasks_per_node:
            tasks_per_node = self._rm_info['cores_per_node'] // \
                             (n_ranks * n_threads_per_rank) or 1
        # IBRUN_TASKS_PER_NODE is used to set the actual number of running
        # tasks, if not set, then `ibrun` script will use the default Slurm
        # setting for the number of tasks per node to build the hostlist
        # (for TACC machines)
        #
        # NOTE: in case of performance issue: reconsider this parameter
        launch_env = 'IBRUN_TASKS_PER_NODE=%d' % tasks_per_node

        rank_node_idxs = set([slot['node_index'] for slot in slots])
        tasks_offset   = 0
        ibrun_offset   = 0

        for node in self._rm_info.node_list:

            if node['index'] not in rank_node_idxs:
                tasks_offset += tasks_per_node
                continue

            # cores contains core ids for each thread,
            # but threads are ignored for offset
            core_id_min = min([slot['cores'][0]['index']
                               for slot in slots
                               if  slot['node_index'] == node['index']])
            # offset into processor (cpus) hostlist
            ibrun_offset = tasks_offset + (core_id_min // n_threads_per_rank)
            break

        cmd = '%s %s -n %s -o %d %s' % (launch_env, self._command, n_ranks,
                                        ibrun_offset, exec_path)
        return cmd.rstrip()


    # --------------------------------------------------------------------------
    #
    def get_rank_cmd(self):

        ret  = 'test -z "$SLURM_PROCID" || export RP_RANK=$SLURM_PROCID\n'
        ret += 'test -z "$MPI_RANK"     || export RP_RANK=$MPI_RANK\n'
        ret += 'test -z "$PMIX_RANK"    || export RP_RANK=$PMIX_RANK\n'

        return ret


# ------------------------------------------------------------------------------

