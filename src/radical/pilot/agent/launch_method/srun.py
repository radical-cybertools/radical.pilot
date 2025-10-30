
__copyright__ = 'Copyright 2016-2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os
import math
import time
import signal

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
        self._verbose : bool = False

        if (os.environ.get('RADICAL_PILOT_SRUN_VERBOSE', '').lower() in
                ['true', 'yes', '1']):
            self._verbose = True

        LaunchMethod.__init__(self, name, lm_cfg, rm_info, log, prof)


    # --------------------------------------------------------------------------
    #
    def init_from_scratch(self, env, env_sh):

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
    def init_from_info(self, lm_info):

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
    def cancel_task(self, task, pid):
        '''
        This method cancels the task, in the default case by killing the task's
        launch process identified by its process ID.
        '''

        # according to OLCF, SIGINT should be sent twice for effective rank
        # cancellation
        try:
            self._log.debug('killing task %s (%d)', task['uid'], pid)
            os.killpg(pid, signal.SIGINT)

            try:
                time.sleep(0.1)
                os.killpg(pid, signal.SIGINT)
            except OSError:
                pass

            # also send a SIGKILL to drive the message home.
            # NOTE: the `sleep` will limit the cancel throughput!
            try:
                time.sleep(0.1)
                os.killpg(pid, signal.SIGKILL)
            except OSError:
                pass

        except OSError:
            # lost race: task is already gone, we ignore this
            self._log.debug('task already gone: %s', task['uid'])


    # --------------------------------------------------------------------------
    #
    def can_launch(self, task):

        if not task['description']['executable']:
            return False, 'no executable'

        return True, ''


    # --------------------------------------------------------------------------
    #
    def get_launcher_env(self):

        launcher_env = []

        if self._verbose:
            launcher_env.extend([
                'export SLURM_DEBUG=1',  # enable verbosity
                'export SLURM_CPU_BIND=verbose'  # debug mapping
            ])

        launcher_env.append('. $RP_PILOT_SANDBOX/%s' % self._env_sh)
        return launcher_env


    # --------------------------------------------------------------------------
    #
    def get_launch_cmds(self, task, exec_path):

        uid            = task['uid']
        slots          = task['slots']
        td             = task['description']
        sbox           = task['task_sandbox_path']

        n_tasks        = len(slots)
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
            n_tasks = td['ranks']
            n_nodes = int(math.ceil(float(n_tasks) /
                                    self._rm_info.get('cores_per_node', 1)))
        else:
            # the scheduler did place tasks - we can't honor the core and gpu
            # mapping (see above), but we at least honor the nodelist.
            nodelist = set([str(slot['node_name']) for slot in slots])
            n_nodes  = len(nodelist)

            # older slurm versions don't accept option `--nodefile`
            # 42 node is the upper limit to switch from `--nodelist`
            # to `--nodefile`
            if self._vmajor > MIN_VSLURM_IN_LIST:
                if n_nodes > MIN_NNODES_IN_LIST:
                    nodefile = '%s/%s.nodes' % (sbox, uid)
                    with ru.ru_open(nodefile, 'w') as fout:
                        fout.write(','.join(nodelist) + '\n')

            if slots[0]['gpus']:
                gpus_per_task = len(slots[0]['gpus'])

        mapping = ''
        if n_tasks > 1:
            if td['use_mpi'] is False:
                mapping += '-K0 '  # '--kill-on-bad-exit=0 '
            else:
                # ensure that all ranks are killed if one rank fails
                mapping += '-K1 '  # '--kill-on-bad-exit=1 '
                # allow step cancellation with single SIGINT
                mapping += '--quit-on-interrupt '

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
        if not td.get('metadata', {}).get('lm_skip_gpus', False):
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

        env = '--export=ALL'

        cmd = '%s %s %s %s' % (self._command, env, mapping, exec_path)
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
    def get_env_blacklist(self):
        '''
        extend the base blacklist by SLURM specific environment variables
        '''

        ret = super().get_env_blacklist()

        ret.extend([
                    'SLURM_*',
                   ])

        return ret


# ------------------------------------------------------------------------------

