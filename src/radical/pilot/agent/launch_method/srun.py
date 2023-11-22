
__copyright__ = 'Copyright 2016-2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import math

import radical.utils as ru

from .base import LaunchMethod

MIN_NNODES_IN_LIST = 42
MIN_VSLURM_IN_LIST = 18


# ------------------------------------------------------------------------------
#
class Srun(LaunchMethod):
    '''
    This launch method uses `srun` to place tasks into a slurm allocation.

    Srun has severe limitations compared to other launch methods, in that it
    does not allow to place a task on a specific set of nodes and cores, at
    least not in the general case.  It is possible to select nodes as long as
    the task uses (a part of) a single node, or the task is using multiple nodes
    uniformly.  Core pinning is only available on tasks which use exactly one
    full node (and in that case becomes useless for our purposes).

    We use srun in the following way:

        IF    task <= nodesize
        OR    task is uniformel
        THEN  enforce node placement
        ELSE  leave *all* placement to slurm
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, lm_cfg, rm_info, log, prof):

        self._command : str  = ''
        self._traverse: bool = bool('princeton.traverse' in lm_cfg['resource'])
        self._exact   : bool = rm_info.details.get('exact', False)

        LaunchMethod.__init__(self, name, lm_cfg, rm_info, log, prof)


    # --------------------------------------------------------------------------
    #
    def _init_from_scratch(self, env, env_sh):

        command = ru.which('srun')
        out, err, ret = ru.sh_callout('%s -V' % command)
        if ret:
            raise RuntimeError('cannot use srun [%s] [%s]' % (out, err))

        version = out.split()[-1]
        vmajor  = int(version.split('.')[0])
        self._log.debug('using srun from %s [v.%s]', command, version)

        lm_info = {'env'     : env,
                   'env_sh'  : env_sh,
                   'command' : command,
                   'version' : version,
                   'vmajor'  : vmajor}

        return lm_info


    # --------------------------------------------------------------------------
    #
    def _init_from_info(self, lm_info):

        self._env     = lm_info['env']
        self._env_sh  = lm_info['env_sh']
        self._command = lm_info['command']
        self._version = lm_info['version']
        self._vmajor  = lm_info['vmajor']

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

        return ['export SLURM_CPU_BIND=verbose',  # debug mapping
                '. $RP_PILOT_SANDBOX/%s' % self._env_sh]


    # --------------------------------------------------------------------------
    #
    def get_launch_cmds(self, task, exec_path):

        uid            = task['uid']
        slots          = task['slots']
        td             = task['description']
        sbox           = task['task_sandbox_path']

        n_tasks        = td['ranks']
        n_task_threads = td.get('cores_per_rank', 1)
        gpus_per_task  = td.get('gpus_per_rank', 0.)

        # Alas, exact rank-to-core mapping seems only be available in Slurm when
        # tasks use full nodes - which in RP is rarely the case.  We thus are
        # limited to specifying the list of nodes we want the processes to be
        # placed on, and otherwise have to rely on the `--exclusive` flag to get
        # a decent auto mapping.  In cases where the scheduler did not place
        # the task we leave the node placement to srun as well.

        nodefile = None
        nodelist = list()

        if not slots:
            n_nodes = int(math.ceil(float(n_tasks) /
                                    self._rm_info.get('cores_per_node', 1)))
        else:
            # the scheduler did place tasks - we can't honor the core and gpu
            # mapping (see above), but we at least honor the nodelist.
            nodelist = set([str(rank['node_name']) for rank in slots['ranks']])
            n_nodes  = len(nodelist)

            # older slurm versions don't accept option `--nodefile`
            # 42 node is the upper limit to switch from `--nodelist`
            # to `--nodefile`
            if self._vmajor > MIN_VSLURM_IN_LIST:
                if n_nodes > MIN_NNODES_IN_LIST:
                    nodefile = '%s/%s.nodes' % (sbox, uid)
                    with ru.ru_open(nodefile, 'w') as fout:
                        fout.write(','.join(nodelist) + '\n')

            if slots['ranks'][0]['gpu_map']:
                gpus_per_task = len(slots['ranks'][0]['gpu_map'][0])

        mapping = ''
        if self._exact:
            mapping += '--exact '

        if self._traverse:
            mapping += '--ntasks=%d '        % n_tasks \
                    +  '--cpus-per-task=%d ' % n_task_threads \
                    +  '--ntasks-per-core=1 --distribution="arbitrary"'
        else:
            mapping += '--nodes %d ' % n_nodes \
                    +  '--ntasks %d' % n_tasks

            if n_task_threads:
                mapping += ' --cpus-per-task %d' % n_task_threads

        if self._rm_info['threads_per_core'] > 1:
            mapping += ' --threads-per-core %d' % \
                       self._rm_info['threads_per_core']

        if td.get('mem_per_rank'):
            mapping += ' --mem %s' % td.get('mem_per_rank')
        else:
            # allow access to full node memory by default
            mapping += ' --mem 0'

        # check that gpus were requested to be allocated
        if self._rm_info.get('requested_gpus') and gpus_per_task:
            if self._traverse:
                mapping += ' --gpus-per-task=%d' % gpus_per_task
            else:
                mapping += ' --gpus-per-task %d' % gpus_per_task + \
                           ' --gpu-bind closest'

        if nodefile:
            mapping += ' --nodefile=%s' % nodefile

        elif nodelist:
            mapping += ' --nodelist=%s' % ','.join(nodelist)

        cmd = '%s %s %s' % (self._command, mapping, exec_path)
        return cmd.rstrip()


    # --------------------------------------------------------------------------
    #
    def get_rank_cmd(self):

        ret  = 'test -z "$SLURM_PROCID" || export RP_RANK=$SLURM_PROCID\n'
        ret += 'test -z "$MPI_RANK"     || export RP_RANK=$MPI_RANK\n'
        ret += 'test -z "$PMIX_RANK"    || export RP_RANK=$PMIX_RANK\n'

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

