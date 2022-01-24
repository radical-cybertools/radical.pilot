
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

import math

import radical.utils as ru

from .base import LaunchMethod


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

        self._command: str = ''

        LaunchMethod.__init__(self, name, lm_cfg, rm_info, log, prof)


    # --------------------------------------------------------------------------
    #
    def _init_from_scratch(self, env, env_sh):

        command = ru.which('srun')

        out, err, ret = ru.sh_callout('%s -V' % command)
        if ret:
            raise RuntimeError('cannot use srun [%s] [%s]' % (out, err))

        self._version = out.split()[-1]
        self._log.debug('using srun from %s [%s]',
                        command, self._version)

        lm_info = {'env'    : env,
                   'env_sh' : env_sh,
                   'command': command}

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


    # -------------------------------------------------------------------------
    #
    def get_slurm_ver(self):

        major_version = int(self._version.split('.')[0].split()[-1])
        return major_version


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

        n_tasks        = td['cpu_processes']
        n_task_threads = td.get('cpu_threads', 1)
        n_gpus         = td.get('gpu_processes', 1)

        # Alas, exact rank-to-core mapping seems only be available in Slurm when
        # tasks use full nodes - which in RP is rarely the case.  We thus are
        # limited to specifying the list of nodes we want the processes to be
        # placed on, and otherwise have to rely on the `--exclusive` flag to get
        # a decent auto mapping.  In cases where the scheduler did not place
        # the task we leave the node placement to srun as well.

        if not slots:
            nodefile = None
            n_nodes  = int(math.ceil(float(n_tasks) /
                                     self._rm_info.get('cores_per_node', 1)))
        else:
            # the scheduler did place tasks - we can't honor the core and gpu
            # mapping (see above), but we at least honor the nodelist.
            nodelist = [rank['node_name'] for rank in slots['ranks']]
            nodefile = '%s/%s.nodes' % (sbox, uid)
            with ru.ru_open(nodefile, 'w') as fout:
                fout.write(','.join(nodelist))
                fout.write('\n')

            n_nodes = len(set(nodelist))

        # use `--exclusive` to ensure all tasks get individual resources.
        # do not use core binding: it triggers warnings on some installations
        # FIXME: warnings are triggered anyway :-(
        mapping = '--exclusive --cpu-bind=none ' \
                + '--nodes %d '        % n_nodes \
                + '--ntasks %d '       % n_tasks \
                + '--gpus %d '         % (n_gpus * n_tasks) \
                + '--cpus-per-task %d' % n_task_threads

        # check that gpus were requested to be allocated
        if self._rm_info.get('gpus'):
            mapping += ' --gpus-per-task %d' % n_gpus

        if nodefile:
            if self.get_slurm_ver() <= 18:
                mapping += ' --nodelist=%s' % ','.join(str(n) for n in nodelist)
            else:
                mapping += ' --nodefile=%s' % nodefile

        cmd = '%s %s %s' % (self._command, mapping, exec_path)
        return cmd.rstrip()


    # --------------------------------------------------------------------------
    #
    def get_rank_cmd(self):

        # FIXME: does SRUN set a rank env?
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

