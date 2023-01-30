
__copyright__ = 'Copyright 2016-2023, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import math

import radical.utils as ru

from ...   import constants as rpc
from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class JSRUN(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, lm_cfg, rm_info, log, prof):

        self._erf    : bool = False
        self._command: str  = ''

        LaunchMethod.__init__(self, name, lm_cfg, rm_info, log, prof)


    # --------------------------------------------------------------------------
    #
    def _init_from_scratch(self, env, env_sh):

        lm_info = {'env'    : env,
                   'env_sh' : env_sh,
                   'command': ru.which('jsrun'),
                   'erf'    : False}

        if '_erf' in self.name.lower():
            lm_info['erf'] = True

        return lm_info


    # --------------------------------------------------------------------------
    #
    def _init_from_info(self, lm_info):

        self._env     = lm_info['env']
        self._env_sh  = lm_info['env_sh']
        self._command = lm_info['command']

        assert self._command

        self._erf     = lm_info['erf']


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
    def _create_resource_set_file(self, slots, uid, sandbox):
        '''
        This method takes as input a Task slots and creates the necessary
        resource set file (Explicit Resource File, ERF). This resource set
        file is then used by jsrun to place and execute tasks on nodes.

        An example of a resource file is:

        * Task 1: 2 MPI procs, 2 threads per process and 2 gpus per process*

            rank 0 : {host: 1; cpu:  {0, 1}; gpu: {0,1}}
            rank 1 : {host: 1; cpu: {22,23}; gpu: {3,4}}

        * Task 2: 2 MPI procs, 1 thread per process and 1 gpus per process*

            rank 0 : {host: 2; cpu:  7; gpu: 2}
            rank 1 : {host: 2; cpu: 30; gpu: 5}

        Parameters
        ----------
        slots : List of dictionaries.

            The slots that the task will be placed. A slot has the following
            format:

            {"ranks"         : [{"node_name" : "a",
                                 "node_id"   : 1,
                                 "core_map"  : [[0]],
                                 "gpu_map"   : [],
                                 "lfs"       : 0,
                                 "mem"       : 0
                                }]
            }

        uid     : task ID (string)
        sandbox : task sandbox (string)
        '''

        # https://github.com/olcf-tutorials/ERF-CPU-Indexing
        # `cpu_index_using: physical` causes the following issue
        # "error in ptssup_mkcltsock_afunix()"
        rs_str  = 'cpu_index_using: logical\n'

        rank_id = 0
        for rank in slots['ranks']:

            gpu_maps = list(rank['gpu_map'])
            for map_set in rank['core_map']:
                cores = ','.join(str(core) for core in map_set)
                rs_str += 'rank: %d: {' % rank_id
                rs_str += ' host: %s;'  % str(rank['node_id'])
                rs_str += ' cpu: {%s}'  % cores
                if gpu_maps:
                    gpus = [str(gpu_map[0]) for gpu_map in gpu_maps]
                    rs_str += '; gpu: {%s}' % ','.join(gpus)
                rs_str  += ' }\n'
                rank_id += 1

        rs_name = '%s/%s.rs' % (sandbox, uid)
        with ru.ru_open(rs_name, 'w') as fout:
            fout.write(rs_str)

        return rs_name


    # --------------------------------------------------------------------------
    #
    def get_launch_cmds(self, task, exec_path):

        uid = task['uid']
        td  = task['description']

        if self._erf:

            sbox  = task['task_sandbox_path']
            slots = task['slots']

            assert slots['ranks'], 'task.slots.ranks not defined'

            cmd_options = '--erf_input %s' % \
                self._create_resource_set_file(slots, uid, sbox)

        else:
            # for OpenMP threads a corresponding parameter should be provided
            # in task description - `td.threading_type = rp.OpenMP`,
            # and RP will set `export OMP_NUM_THREADS=<cores_per_rank>`
            cores_per_rs = math.ceil(
                td['cores_per_rank'] / self._rm_info['threads_per_core'])
            # JSRun uses resource sets (RS) to configure a node representation
            # for a job/task: https://docs.olcf.ornl.gov/systems/\
            #                 summit_user_guide.html#resource-sets
            # -b: bind to RS
            # -n: number of RS
            # -a: number of MPI tasks (ranks) per RS
            # -c: number of CPUs (physical cores) per RS
            cmd_options = '-b rs -n%d -a1 -c%d' % (td['ranks'], cores_per_rs)

        if td['gpus_per_rank']:

            if not self._erf:
                # -g: number of GPUs per RS
                # -r: number of RS per host (node)
                cmd_options += ' -g%d -r%d' % (
                    td['gpus_per_rank'],
                    self._rm_info['gpus_per_node'] // td['gpus_per_rank'])

            # from https://www.olcf.ornl.gov/ \
            #             wp-content/uploads/2018/11/multi-gpu-workshop.pdf
            #
            # CUDA with    MPI, use jsrun --smpiargs="-gpu"
            # CUDA without MPI, use jsrun --smpiargs="off"
            #
            # This is set for CUDA tasks only
            if td['gpu_type'] == rpc.CUDA:
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
    def get_exec(self, task):

        td          = task['description']
        task_exec   = td['executable']
        task_args   = td.get('arguments')
        task_argstr = self._create_arg_string(task_args)
        command     = '%s %s' % (task_exec, task_argstr)

        return command.rstrip()


# ------------------------------------------------------------------------------

