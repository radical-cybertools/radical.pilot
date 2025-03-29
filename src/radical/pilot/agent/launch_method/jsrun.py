
__copyright__ = 'Copyright 2016-2023, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import math

import radical.utils as ru

from ...      import constants as rpc
from .base    import LaunchMethod
from ...utils import convert_slots_to_old


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
    def init_from_scratch(self, env, env_sh):

        lm_info = {'env'    : env,
                   'env_sh' : env_sh,
                   'command': ru.which('jsrun'),
                   'erf'    : False}

        if '_erf' in self.name.lower():
            lm_info['erf'] = True

        return lm_info


    # --------------------------------------------------------------------------
    #
    def init_from_info(self, lm_info):

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

            rank 0 : { host: 1; cpu:  {0, 1}; gpu: {0,1} }
            rank 1 : { host: 1; cpu: {22,23}; gpu: {3,4} }

        * Task 2: 2 MPI procs, 1 thread per process and 1 gpus per process*

            rank 0 : { host: 2; cpu:  {7}; gpu: {2} }
            rank 1 : { host: 2; cpu: {30}; gpu: {5} }

        Parameters
        ----------
        slots : List of dictionaries.

            The slots that the task will be placed. A slot has the following
            format:

            {
               "node_name" : "a",
               "node_index": "1",
               "cores"     : [[0, 1]],
               "gpus"      : [[0]],
               "lfs"       : 0,
               "mem"       : 0
            }

        uid     : task ID (string)
        sandbox : task sandbox (string)
        '''

        # https://github.com/olcf-tutorials/ERF-CPU-Indexing
        # `cpu_index_using: physical` causes the following issue
        # "error in ptssup_mkcltsock_afunix()"
        rs_str  = 'cpu_index_using: logical\n'

        # NOTE: our tests feed us the new slot structure, the jsrun scheduler
        #       provides the old one.  We need to detect that and convert.
        if self._in_pytest:
            slots = convert_slots_to_old(slots)

        base_id = 0
        for slot_ranks in slots:

            ranks_per_rs  = len(slot_ranks['cores'])
            rank_ids      = [str(r + base_id) for r in range(ranks_per_rs)]
            base_id      += ranks_per_rs

            core_id_sets = []
            for core_map in slot_ranks['cores']:
                core_ids = [str(cid) for cid in core_map]
                core_id_sets.append('{%s}' % ','.join(core_ids))

            rs_str += 'rank: %s : {'    % ','.join(rank_ids)
            rs_str += ' host: %s;'      % str(slot_ranks['node_index'])
            rs_str += ' cpu: %s'        % ','.join(core_id_sets)
            if slot_ranks['gpus'] and slot_ranks['gpus'][0]:
                # check the first element, since it is the same for RS ranks
                slot_gpus = slot_ranks['gpus'][0]
                if not self._in_pytest:
                    assert slot_ranks['gpus'].count(slot_gpus) == ranks_per_rs
                rs_str += '; gpu: {%s}' % ','.join([str(g) for g in slot_gpus])
            rs_str += ' }\n'

        rs_name = '%s/%s.rs' % (sandbox, uid)
        with ru.ru_open(rs_name, 'w') as fout:
            fout.write(rs_str)

        return rs_name


    # --------------------------------------------------------------------------
    #
    def get_launch_cmds(self, task, exec_path):

        uid   = task['uid']
        td    = task['description']
        slots = task['slots']

        assert slots, 'task.slots not defined'

        # NOTE: our tests feed us the new slot structure, the jsrun scheduler
        #       provides the old one.  We need to detect that and convert.
        if self._in_pytest:
            slots = convert_slots_to_old(slots)

        if self._erf:

            cmd_options = '--erf_input %s' % self._create_resource_set_file(
                slots, uid, task['task_sandbox_path'])

        else:

            # JSRun uses resource sets (RS) to configure a node representation
            # for a job/task: https://docs.olcf.ornl.gov/systems/\
            #                 summit_user_guide.html#resource-sets

            rs             = len(slots)
            slot_ranks     = slots[0]
            # physical cores per rank
            cores_per_rank = math.ceil(len(slot_ranks['cores'][0]) /
                             self._rm_info['threads_per_core'])
            ranks_per_rs   = len(slot_ranks['cores'])
            cores_per_rs   = cores_per_rank * ranks_per_rs

            gpus_per_rs  = 0
            if slot_ranks['gpus']:
                slot_gpus = slot_ranks['gpus'][0]
                if not self._in_pytest:
                    assert slot_ranks['gpus'].count(slot_gpus) == ranks_per_rs
                gpus_per_rs = len(slot_gpus)

            # -n: number of RS
            # -a: number of MPI tasks (ranks)     per RS
            # -c: number of CPUs (physical cores) per RS
            # -g: number of GPUs                  per RS
            cmd_options = '-n%d -a%d -c%d -g%d' % (
                rs, ranks_per_rs, cores_per_rs, gpus_per_rs)

            if gpus_per_rs:
                # -r: number of RS per host (node)
                max_rs_per_node = self._rm_info['gpus_per_node'] // gpus_per_rs
                if rs > max_rs_per_node:
                    # number of nodes that specified must evenly divide the
                    # number of resource sets, otherwise an error is returned
                    max_rs_per_node = math.gcd(rs, max_rs_per_node)
                else:
                    max_rs_per_node = min(rs, max_rs_per_node)
                cmd_options += ' -r%d' % max_rs_per_node

            # -b: binding of tasks within a resource set (none, rs, or packed:N)
            if ranks_per_rs > 1:
                if td['threading_type'] == rpc.OpenMP:
                    # for OpenMP threads RP will set:
                    #    export OMP_NUM_THREADS=<threads_per_rank>
                    cmd_options += ' -b packed:%d' % cores_per_rank
            else:
                cmd_options += ' -b rs'

        if td['gpus_per_rank'] and td['gpu_type'] == rpc.CUDA:
            # from https://www.olcf.ornl.gov/ \
            #             wp-content/uploads/2018/11/multi-gpu-workshop.pdf
            #
            # CUDA with    MPI, use jsrun --smpiargs="-gpu"
            # CUDA without MPI, use jsrun --smpiargs="off"
            #
            # This is set for CUDA tasks only
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

        ret = 'test -z "$PMIX_RANK" || export RP_RANK=$PMIX_RANK\n'

        return ret


# ------------------------------------------------------------------------------

