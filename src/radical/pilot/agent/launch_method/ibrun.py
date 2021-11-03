
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


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

        self._command  : str  = ''
        self._node_list: list = self._rm_info.node_list or []

        LaunchMethod.__init__(self, name, lm_cfg, rm_info, log, prof)


    # --------------------------------------------------------------------------
    #
    def _init_from_scratch(self, env, env_sh):

        lm_info = {'env'    : env,
                   'env_sh' : env_sh,
                   'command': ru.which('ibrun')}

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

        slots   = task['slots']
        td      = task['description']
        n_tasks = td['cpu_processes']

        # Usage of env variable TACC_TASKS_PER_NODE is purely for MPI tasks,
        # threads are not considered (info provided by TACC support)
        n_node_tasks = int(td['environment'].get('TACC_TASKS_PER_NODE') or
                           self._rm_info.get('cores_per_node', 1))

        assert slots['ranks'], 'task.slots.ranks is not set'

        # TACC_TASKS_PER_NODE is used to set the actual number of running tasks,
        # if not set, then ibrun script will use the default slurm setting for
        # the number of tasks per node to build the hostlist (for TACC machines)
        #
        # NOTE: in case of performance issue: reconsider this parameter
        ibrun_offset = 0
        offsets      = list()
        node_id      = 0

        for node in self._node_list:
            for rank in slots['ranks']:
                if rank['node_id'] == node[0]:
                    for core_map in rank['core_map']:
                        assert core_map, 'core_map is not set'
                        # core_map contains core ids for each thread,
                        # but threads are ignored for offsets
                        offsets.append(node_id + (core_map[0] // len(core_map)))
            node_id += n_node_tasks

        if offsets:
            ibrun_offset = min(offsets)

        cmd = '%s -n %s -o %d %s' % (self._command, n_tasks, ibrun_offset,
                                     exec_path)
        return cmd.rstrip()


    # --------------------------------------------------------------------------
    #
    def get_rank_cmd(self):

        # FIXME: does IBRUN set a rank env?
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

